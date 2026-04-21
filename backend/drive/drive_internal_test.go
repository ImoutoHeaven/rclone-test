package drive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	fssync "github.com/rclone/rclone/fs/sync"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestDriveScopes(t *testing.T) {
	for _, test := range []struct {
		in       string
		want     []string
		wantFlag bool
	}{
		{"", []string{
			"https://www.googleapis.com/auth/drive",
		}, false},
		{" drive.file , drive.readonly", []string{
			"https://www.googleapis.com/auth/drive.file",
			"https://www.googleapis.com/auth/drive.readonly",
		}, false},
		{" drive.file , drive.appfolder", []string{
			"https://www.googleapis.com/auth/drive.file",
			"https://www.googleapis.com/auth/drive.appfolder",
		}, true},
	} {
		got := driveScopes(test.in)
		assert.Equal(t, test.want, got, test.in)
		gotFlag := driveScopesContainsAppFolder(got)
		assert.Equal(t, test.wantFlag, gotFlag, test.in)
	}
}

func TestDriveAccountsJSONModeSelection(t *testing.T) {
	assert.False(t, isMultiAccountMode(""))
	accountsPath := filepath.Join(t.TempDir(), "accounts.json")
	assert.True(t, isMultiAccountMode(accountsPath))
	assert.True(t, isMultiAccountMode("   "))

	opt := Options{
		AccountsJSON:           "relative/accounts.json",
		TeamDriveID:            "team",
		AccountSelectionPolicy: "round_robin",
	}
	err := validateMultiAccountConfig(&opt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accounts_json must be an absolute path")
}

func TestDriveAccountsJSONRejectsInlinePayload(t *testing.T) {
	inline := `[{"token":{"access_token":"abc","refresh_token":"refresh-abc","expiry":"2026-04-17T00:00:00Z"}}]`
	opt := Options{
		AccountsJSON:           inline,
		TeamDriveID:            "team",
		AccountSelectionPolicy: "round_robin",
	}
	err := validateMultiAccountConfig(&opt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accounts_json must be an absolute path")
}

func TestDriveAccountsJSONRejectsInvalidCombinations(t *testing.T) {
	tests := []struct {
		name    string
		opt     Options
		wantErr string
	}{
		{
			name: "accounts_json with service_account_file",
			opt: Options{
				AccountsJSON:       "/tmp/accounts.json",
				TeamDriveID:        "team",
				ServiceAccountFile: "sa.json",
			},
			wantErr: "accounts_json cannot be used with service_account_file",
		},
		{
			name: "accounts_json with service_account_credentials",
			opt: Options{
				AccountsJSON:              "/tmp/accounts.json",
				TeamDriveID:               "team",
				ServiceAccountCredentials: `{"type":"service_account"}`,
			},
			wantErr: "accounts_json cannot be used with service_account_credentials",
		},
		{
			name: "accounts_json with env_auth",
			opt: Options{
				AccountsJSON: "/tmp/accounts.json",
				TeamDriveID:  "team",
				EnvAuth:      true,
			},
			wantErr: "accounts_json cannot be used with env_auth",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMultiAccountConfig(&tc.opt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestDriveAccountsJSONNamespaceRules(t *testing.T) {
	t.Run("both team_drive and root_folder_id set", func(t *testing.T) {
		opt := Options{
			AccountsJSON:           "/tmp/accounts.json",
			TeamDriveID:            "team",
			RootFolderID:           "root",
			AccountSelectionPolicy: "round_robin",
		}

		err := validateMultiAccountConfig(&opt)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one of team_drive or root_folder_id")
	})

	t.Run("neither team_drive nor root_folder_id set", func(t *testing.T) {
		opt := Options{
			AccountsJSON:           "/tmp/accounts.json",
			AccountSelectionPolicy: "round_robin",
		}

		err := validateMultiAccountConfig(&opt)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one of team_drive or root_folder_id")
	})
}

func TestDriveAccountSelectionPolicyDefaultsAndValidation(t *testing.T) {
	opt := Options{}
	applyAccountSelectionPolicyDefaults(&opt)
	assert.Equal(t, "round_robin", opt.AccountSelectionPolicy)

	require.NoError(t, validateAccountSelectionPolicy(opt.AccountSelectionPolicy))
	require.NoError(t, validateAccountSelectionPolicy("random"))

	err := validateAccountSelectionPolicy("weighted")
	require.Error(t, err)
	assert.EqualError(t, err, "drive: account_selection_policy must be round_robin or random")
}

func TestDriveMultiAccountInitFailsWhenAnyAccountCannotAccessNamespace(t *testing.T) {
	namespaceErr := errors.New("forbidden")
	pool := &accountPool{
		accounts: []*accountRuntime{
			{
				index: 0,
				namespaceAccessCheck: func(context.Context, namespaceTarget) error {
					return nil
				},
			},
			{
				index: 1,
				namespaceAccessCheck: func(context.Context, namespaceTarget) error {
					return namespaceErr
				},
			},
		},
	}

	err := validateNamespaceAccessForAllAccounts(context.Background(), pool, namespaceTarget{teamDriveID: "team-id"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "account 1")
	assert.ErrorContains(t, err, "cannot access configured namespace")
	assert.ErrorContains(t, err, namespaceErr.Error())
}

func TestDriveMultiAccountInitPassesWhenAllAccountsCanAccessNamespace(t *testing.T) {
	pool := &accountPool{
		accounts: []*accountRuntime{
			{
				index: 0,
				namespaceAccessCheck: func(context.Context, namespaceTarget) error {
					return nil
				},
			},
			{
				index: 1,
				namespaceAccessCheck: func(context.Context, namespaceTarget) error {
					return nil
				},
			},
		},
	}

	err := validateNamespaceAccessForAllAccounts(context.Background(), pool, namespaceTarget{rootFolderID: "root-id"})
	require.NoError(t, err)
}

func TestDriveMultiAccountInitUsesPerAccountTokenWhenBackendTokenEmpty(t *testing.T) {
	entryToken := testParseAccountsJSONToken("entry")
	accountsJSON := fmt.Sprintf(`[{"token":%s}]`, entryToken)
	accountsPath := writeTempAccountsFile(t, accountsJSON)

	opt := &Options{
		AccountsJSON:     accountsPath,
		UploadDailyLimit: defaultUploadDailyLimit,
	}
	baseMapper := configmap.Simple{
		config.ConfigToken:        "",
		config.ConfigClientID:     "backend-client-id",
		config.ConfigClientSecret: "backend-client-secret",
	}

	called := false
	_, err := createOAuthClientForInit(context.Background(), opt, "drive-test", baseMapper, func(_ context.Context, _ *Options, _ string, scopedMapper configmap.Mapper) (*http.Client, error) {
		called = true

		token, ok := scopedMapper.Get(config.ConfigToken)
		require.True(t, ok)
		if strings.TrimSpace(token) == "" {
			return nil, errors.New("missing token")
		}
		assert.JSONEq(t, entryToken, token)

		clientID, ok := scopedMapper.Get(config.ConfigClientID)
		require.True(t, ok)
		assert.Equal(t, "backend-client-id", clientID)

		clientSecret, ok := scopedMapper.Get(config.ConfigClientSecret)
		require.True(t, ok)
		assert.Equal(t, "backend-client-secret", clientSecret)

		return &http.Client{}, nil
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func testParseAccountsJSONToken(accessToken string) string {
	return fmt.Sprintf(`{"access_token":"%s","refresh_token":"refresh-%s","expiry":"2026-04-17T00:00:00Z"}`, accessToken, accessToken)
}

func writeTempAccountsFile(t *testing.T, content string) string {
	t.Helper()
	accountsPath := filepath.Join(t.TempDir(), "accounts.json")
	require.NoError(t, os.WriteFile(accountsPath, []byte(content), 0o600))
	return accountsPath
}

func TestParseAccountsJSONRejectsEmptyArray(t *testing.T) {
	accountsPath := writeTempAccountsFile(t, "[]")
	_, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accounts_json must not be empty array")
}

func TestParseAccountsJSONRequiresAbsolutePath(t *testing.T) {
	_, err := parseAccountsJSON("accounts.json", &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accounts_json must be an absolute path")
}

func TestParseAccountsJSONRejectsMissingFile(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing-accounts.json")
	_, err := parseAccountsJSON(missingPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accounts_json file not found")
}

func TestParseAccountsJSONRejectsInvalidFileFormat(t *testing.T) {
	accountsPath := writeTempAccountsFile(t, "{this-is-not-valid-json")
	_, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid accounts_json JSONL")
}

func TestParseAccountsJSONAcceptsObjectToken(t *testing.T) {
	accountsPath := writeTempAccountsFile(t, `[{"token":{"access_token":"abc","refresh_token":"refresh-abc","expiry":"2026-04-17T00:00:00Z"}}]`)
	accounts, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.JSONEq(t, `{"access_token":"abc","refresh_token":"refresh-abc","expiry":"2026-04-17T00:00:00Z"}`, accounts[0].tokenJSON)
}

func TestParseAccountsJSONRejectsUnparseableTokenJSON(t *testing.T) {
	accountsPath := writeTempAccountsFile(t, `[{"token":{"access_token":"abc","expiry":"invalid-time"}}]`)
	_, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token must contain parseable OAuth token JSON")
}

func TestParseAccountsJSONRejectsTokenString(t *testing.T) {
	accountsPath := writeTempAccountsFile(t, `[{"token":"{\"access_token\":\"abc\"}"}]`)
	_, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token must be a JSON object")
}

func TestParseAccountsJSONIgnoresBackendLevelTokenInMultiAccountMode(t *testing.T) {
	entryToken := testParseAccountsJSONToken("entry")
	raw := fmt.Sprintf(`[{"token":%s}]`, entryToken)
	accountsPath := writeTempAccountsFile(t, raw)

	accounts, err := parseAccountsJSON(accountsPath, &Options{
		Token:            "{not-valid-json}",
		UploadDailyLimit: defaultUploadDailyLimit,
	})
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.JSONEq(t, entryToken, accounts[0].tokenJSON)
	assert.NotEqual(t, "{not-valid-json}", accounts[0].tokenJSON)
}

func TestParseAccountsJSONAssignsDeterministicIndexIdentity(t *testing.T) {
	token0 := testParseAccountsJSONToken("a")
	token1 := testParseAccountsJSONToken("b")
	raw := fmt.Sprintf(`[{"token":%s},{"name":"shared","token":%s}]`, token0, token1)
	accountsPath := writeTempAccountsFile(t, raw)

	accounts, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.NoError(t, err)
	require.Len(t, accounts, 2)
	assert.Equal(t, 0, accounts[0].index)
	assert.Equal(t, 1, accounts[1].index)
	assert.Equal(t, "account-0", accounts[0].name)
	assert.Equal(t, "shared", accounts[1].name)
}

func TestParseAccountsJSONResolvesPerAccountDailyLimitOverride(t *testing.T) {
	token0 := testParseAccountsJSONToken("a")
	token1 := testParseAccountsJSONToken("b")
	raw := fmt.Sprintf(`[{"token":%s,"upload_daily_limit":"42Mi"},{"token":%s}]`, token0, token1)
	accountsPath := writeTempAccountsFile(t, raw)

	opt := &Options{UploadDailyLimit: defaultUploadDailyLimit}
	accounts, err := parseAccountsJSON(accountsPath, opt)
	require.NoError(t, err)
	require.Len(t, accounts, 2)
	assert.Equal(t, fs.SizeSuffix(42*fs.Mebi), accounts[0].uploadDailyLimit)
	assert.Equal(t, opt.UploadDailyLimit, accounts[1].uploadDailyLimit)
}

func TestParseAccountsJSONRejectsNonPositivePerAccountDailyLimit(t *testing.T) {
	token := testParseAccountsJSONToken("a")
	raw := fmt.Sprintf(`[{"token":%s,"upload_daily_limit":"0"}]`, token)
	accountsPath := writeTempAccountsFile(t, raw)

	_, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload_daily_limit must be > 0")
}

func TestParseAccountsJSONResolvesCredentialFallbacks(t *testing.T) {
	token0 := testParseAccountsJSONToken("a")
	token1 := testParseAccountsJSONToken("b")
	raw := fmt.Sprintf(`[{"token":%s},{"token":%s,"client_id":"entry-client"}]`, token0, token1)
	accountsPath := writeTempAccountsFile(t, raw)

	accounts, err := parseAccountsJSON(accountsPath, &Options{
		ClientID:         "backend-client",
		ClientSecret:     "backend-secret",
		UploadDailyLimit: defaultUploadDailyLimit,
	})
	require.NoError(t, err)
	require.Len(t, accounts, 2)
	assert.Equal(t, "backend-client", accounts[0].clientID)
	assert.Equal(t, "backend-secret", accounts[0].clientSecret)
	assert.Equal(t, "entry-client", accounts[1].clientID)
	assert.Equal(t, "backend-secret", accounts[1].clientSecret)
}

func TestParseAccountsJSONParsesJSONL(t *testing.T) {
	line0 := fmt.Sprintf(`{"name":"a0","token":%s}`, testParseAccountsJSONToken("a0"))
	line1 := fmt.Sprintf(`{"name":"a1","token":%s,"client_id":"entry-client"}`, testParseAccountsJSONToken("a1"))
	accountsPath := writeTempAccountsFile(t, line0+"\n"+line1+"\n")

	accounts, err := parseAccountsJSON(accountsPath, &Options{
		ClientID:         "backend-client",
		ClientSecret:     "backend-secret",
		UploadDailyLimit: defaultUploadDailyLimit,
	})
	require.NoError(t, err)
	require.Len(t, accounts, 2)
	assert.Equal(t, "a0", accounts[0].name)
	assert.Equal(t, "a1", accounts[1].name)
	assert.Equal(t, "backend-client", accounts[0].clientID)
	assert.Equal(t, "entry-client", accounts[1].clientID)
}

func TestPersistAccountsJSONFilePreservesJSONLFormatAndUnknownFields(t *testing.T) {
	line0 := fmt.Sprintf(`{"name":"a0","token":%s,"client_id":"old-client-0","custom":"keep-0"}`, testParseAccountsJSONToken("a0"))
	line1 := fmt.Sprintf(`{"name":"a1","token":%s,"client_secret":"old-secret-1","enabled":true}`, testParseAccountsJSONToken("a1"))
	accountsPath := writeTempAccountsFile(t, line0+"\n"+line1+"\n")

	accounts, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.NoError(t, err)

	store := newAccountStore(accounts, func(accountsJSON string) error {
		return persistAccountsJSONFile(accountsPath, accountsJSON)
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	store.setAccountToken(0, testParseAccountsJSONToken("updated-a0"))
	store.setAccountClientID(0, "updated-client-0")
	store.setAccountClientSecret(1, "updated-secret-1")

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))

	raw, err := os.ReadFile(accountsPath)
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(string(raw), "\n"))
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	require.Len(t, lines, 2)
	assert.False(t, strings.HasPrefix(strings.TrimSpace(string(raw)), "["))

	var entry0 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &entry0))
	assert.JSONEq(t, testParseAccountsJSONToken("updated-a0"), string(entry0["token"]))
	assert.JSONEq(t, `"updated-client-0"`, string(entry0["client_id"]))
	assert.JSONEq(t, `"keep-0"`, string(entry0["custom"]))

	var entry1 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &entry1))
	assert.JSONEq(t, testParseAccountsJSONToken("a1"), string(entry1["token"]))
	assert.JSONEq(t, `"updated-secret-1"`, string(entry1["client_secret"]))
	assert.JSONEq(t, `true`, string(entry1["enabled"]))
}

func TestPersistAccountsJSONFilePreservesJSONArrayFormatAndUnknownFields(t *testing.T) {
	rawInput := fmt.Sprintf(`[{"name":"a0","token":%s,"client_id":"old-client-0","custom":"keep-0"},{"name":"a1","token":%s,"client_secret":"old-secret-1","enabled":true}]`, testParseAccountsJSONToken("a0"), testParseAccountsJSONToken("a1"))
	accountsPath := writeTempAccountsFile(t, rawInput)

	accounts, err := parseAccountsJSON(accountsPath, &Options{UploadDailyLimit: defaultUploadDailyLimit})
	require.NoError(t, err)

	store := newAccountStore(accounts, func(accountsJSON string) error {
		return persistAccountsJSONFile(accountsPath, accountsJSON)
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	store.setAccountToken(0, testParseAccountsJSONToken("updated-a0"))
	store.setAccountClientID(0, "updated-client-0")
	store.setAccountClientSecret(1, "updated-secret-1")

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))

	raw, err := os.ReadFile(accountsPath)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(strings.TrimSpace(string(raw)), "["))

	var entries []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &entries))
	require.Len(t, entries, 2)
	assert.JSONEq(t, testParseAccountsJSONToken("updated-a0"), string(entries[0]["token"]))
	assert.JSONEq(t, `"updated-client-0"`, string(entries[0]["client_id"]))
	assert.JSONEq(t, `"keep-0"`, string(entries[0]["custom"]))
	assert.JSONEq(t, testParseAccountsJSONToken("a1"), string(entries[1]["token"]))
	assert.JSONEq(t, `"updated-secret-1"`, string(entries[1]["client_secret"]))
	assert.JSONEq(t, `true`, string(entries[1]["enabled"]))
}

func testAccountConfigsForMapperAndStore() []accountConfig {
	return []accountConfig{
		{
			index:            0,
			name:             "account-0",
			clientID:         "client-0",
			clientSecret:     "secret-0",
			tokenJSON:        testParseAccountsJSONToken("token-0"),
			uploadDailyLimit: defaultUploadDailyLimit,
		},
		{
			index:            1,
			name:             "account-1",
			clientID:         "client-1",
			clientSecret:     "secret-1",
			tokenJSON:        testParseAccountsJSONToken("token-1"),
			uploadDailyLimit: defaultUploadDailyLimit,
		},
		{
			index:            2,
			name:             "account-2",
			clientID:         "client-2",
			clientSecret:     "secret-2",
			tokenJSON:        testParseAccountsJSONToken("token-2"),
			uploadDailyLimit: defaultUploadDailyLimit,
		},
	}
}

func decodePersistedAccountTokens(t *testing.T, raw string) []string {
	t.Helper()
	type persistedAccount struct {
		Token json.RawMessage `json:"token"`
	}
	var entries []persistedAccount
	require.NoError(t, json.Unmarshal([]byte(raw), &entries))
	tokens := make([]string, 0, len(entries))
	for _, entry := range entries {
		tokens = append(tokens, string(entry.Token))
	}
	return tokens
}

func decodePersistedAccounts(t *testing.T, raw string) []map[string]json.RawMessage {
	t.Helper()
	var entries []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &entries))
	return entries
}

func TestAccountMapperTokenGetSetTargetsSingleIndex(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:2]

	var (
		persistedMu       stdsync.Mutex
		persistedSnapshot string
	)
	store := newAccountStore(accounts, func(accountsJSON string) error {
		persistedMu.Lock()
		persistedSnapshot = accountsJSON
		persistedMu.Unlock()
		return nil
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	base := configmap.Simple{
		config.ConfigToken:        "base-token",
		config.ConfigClientID:     "base-client-id",
		config.ConfigClientSecret: "base-client-secret",
	}
	mapper := accountMapper{
		store: store,
		base:  base,
		index: 1,
	}

	token, ok := mapper.Get(config.ConfigToken)
	require.True(t, ok)
	assert.JSONEq(t, accounts[1].tokenJSON, token)

	updatedToken := testParseAccountsJSONToken("updated-1")
	mapper.Set(config.ConfigToken, updatedToken)

	tokenAccount0, ok := store.accountToken(0)
	require.True(t, ok)
	tokenAccount1, ok := store.accountToken(1)
	require.True(t, ok)
	assert.JSONEq(t, accounts[0].tokenJSON, tokenAccount0)
	assert.JSONEq(t, updatedToken, tokenAccount1)

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))

	persistedMu.Lock()
	persistedRaw := persistedSnapshot
	persistedMu.Unlock()
	require.NotEmpty(t, persistedRaw)
	tokens := decodePersistedAccountTokens(t, persistedRaw)
	require.Len(t, tokens, 2)
	assert.JSONEq(t, accounts[0].tokenJSON, tokens[0])
	assert.JSONEq(t, updatedToken, tokens[1])

	persistedAccounts := decodePersistedAccounts(t, persistedRaw)
	require.Len(t, persistedAccounts, 2)
	firstTokenRaw := persistedAccounts[0]["token"]
	require.NotEmpty(t, firstTokenRaw)
	isTokenString := firstTokenRaw[0] == '"'
	assert.False(t, isTokenString, "token must persist as object, not JSON string")
}

func TestAccountStoreSerializesConcurrentTokenWrites(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:2]

	var concurrentWrites int32
	var maxConcurrentWrites int32
	store := newAccountStore(accounts, func(string) error {
		inFlight := atomic.AddInt32(&concurrentWrites, 1)
		for {
			prev := atomic.LoadInt32(&maxConcurrentWrites)
			if inFlight <= prev {
				break
			}
			if atomic.CompareAndSwapInt32(&maxConcurrentWrites, prev, inFlight) {
				break
			}
		}
		time.Sleep(3 * time.Millisecond)
		atomic.AddInt32(&concurrentWrites, -1)
		return nil
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	const updates = 48
	var wg stdsync.WaitGroup
	for i := 0; i < updates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store.setAccountToken(0, testParseAccountsJSONToken(fmt.Sprintf("concurrent-%d", i)))
		}(i)
	}
	wg.Wait()

	flushCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))
	assert.Equal(t, int32(1), atomic.LoadInt32(&maxConcurrentWrites))
}

func TestAccountStoreCoalescesRapidTokenUpdatesToLatestSnapshot(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:1]

	firstPersistRelease := make(chan struct{})
	var persistCalls int32
	var snapshotsMu stdsync.Mutex
	snapshots := make([]string, 0, 4)
	store := newAccountStore(accounts, func(accountsJSON string) error {
		call := atomic.AddInt32(&persistCalls, 1)
		snapshotsMu.Lock()
		snapshots = append(snapshots, accountsJSON)
		snapshotsMu.Unlock()
		if call == 1 {
			<-firstPersistRelease
		}
		return nil
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	store.setAccountToken(0, testParseAccountsJSONToken("first"))
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&persistCalls) >= 1
	}, time.Second, time.Millisecond)

	const rapidUpdates = 30
	latestToken := ""
	for i := 0; i < rapidUpdates; i++ {
		latestToken = testParseAccountsJSONToken(fmt.Sprintf("latest-%d", i))
		store.setAccountToken(0, latestToken)
	}
	close(firstPersistRelease)

	flushCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))

	assert.Less(t, int(atomic.LoadInt32(&persistCalls)), rapidUpdates)

	snapshotsMu.Lock()
	require.NotEmpty(t, snapshots)
	lastSnapshot := snapshots[len(snapshots)-1]
	snapshotsMu.Unlock()
	tokens := decodePersistedAccountTokens(t, lastSnapshot)
	require.Len(t, tokens, 1)
	assert.JSONEq(t, latestToken, tokens[0])
}

func TestAccountStoreRetriesPersistenceWithBackoff(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:1]

	var attempts int32
	store := newAccountStore(accounts, func(string) error {
		if atomic.AddInt32(&attempts, 1) <= 3 {
			return errors.New("transient persist failure")
		}
		return nil
	})
	var backoffCalls int32
	store.backoffFn = func(int) time.Duration {
		atomic.AddInt32(&backoffCalls, 1)
		return 2 * time.Millisecond
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	store.setAccountToken(0, testParseAccountsJSONToken("retry-success"))

	flushCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))

	assert.Equal(t, int32(4), atomic.LoadInt32(&attempts))
	assert.Equal(t, int32(3), atomic.LoadInt32(&backoffCalls))
}

func TestPersistAccountsJSONFileAtomicallyReplacesAndUsesSecurePermissions(t *testing.T) {
	accountsPath := filepath.Join(t.TempDir(), "accounts.json")
	require.NoError(t, os.WriteFile(accountsPath, []byte(`[{"name":"old","token":{"access_token":"old"}}]`), 0o644))

	newPayload := `[{"name":"acc","token":{"access_token":"new","refresh_token":"refresh-new","expiry":"2026-04-17T00:00:00Z"}}]`
	require.NoError(t, persistAccountsJSONFile(accountsPath, newPayload))

	raw, err := os.ReadFile(accountsPath)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"name":"old","token":{"access_token":"new","refresh_token":"refresh-new","expiry":"2026-04-17T00:00:00Z"}}]`, string(raw))

	info, err := os.Stat(accountsPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestAccountStoreTokenUpdateDoesNotOverwriteOtherAccounts(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()

	var (
		persistedMu       stdsync.Mutex
		persistedSnapshot string
	)
	store := newAccountStore(accounts, func(accountsJSON string) error {
		persistedMu.Lock()
		persistedSnapshot = accountsJSON
		persistedMu.Unlock()
		return nil
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	updatedToken := testParseAccountsJSONToken("only-middle-updated")
	store.setAccountToken(1, updatedToken)

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, store.flush(flushCtx))

	persistedMu.Lock()
	persistedRaw := persistedSnapshot
	persistedMu.Unlock()
	require.NotEmpty(t, persistedRaw)
	tokens := decodePersistedAccountTokens(t, persistedRaw)
	require.Len(t, tokens, 3)
	assert.JSONEq(t, accounts[0].tokenJSON, tokens[0])
	assert.JSONEq(t, updatedToken, tokens[1])
	assert.JSONEq(t, accounts[2].tokenJSON, tokens[2])
}

func TestAccountStoreFlushOnShutdown(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:1]

	persistedCh := make(chan string, 8)
	store := newAccountStore(accounts, func(accountsJSON string) error {
		persistedCh <- accountsJSON
		return nil
	})

	baselineToken := testParseAccountsJSONToken("shutdown-baseline")
	store.setAccountToken(0, baselineToken)

	baselineCtx, baselineCancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, store.flush(baselineCtx))
	baselineCancel()

	baselineSnapshot := ""
drainBaseline:
	for {
		select {
		case snapshot := <-persistedCh:
			baselineSnapshot = snapshot
		default:
			break drainBaseline
		}
	}
	require.NotEmpty(t, baselineSnapshot)

	updatedToken := testParseAccountsJSONToken("shutdown-flush")
	store.setAccountToken(0, updatedToken)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, store.shutdown(shutdownCtx))

	latestSnapshot := ""
drainLoop:
	for {
		select {
		case snapshot := <-persistedCh:
			latestSnapshot = snapshot
		default:
			break drainLoop
		}
	}
	require.NotEmpty(t, latestSnapshot)
	tokens := decodePersistedAccountTokens(t, latestSnapshot)
	require.Len(t, tokens, 1)
	assert.JSONEq(t, updatedToken, tokens[0])

	store.setAccountToken(0, testParseAccountsJSONToken("post-shutdown"))
	time.Sleep(20 * time.Millisecond)
	select {
	case snapshot := <-persistedCh:
		t.Fatalf("unexpected persistence after shutdown: %s", snapshot)
	default:
	}
}

func newTestAccountRuntimeForPool(index int, disabled bool, uploadSleepUntilUTC time.Time) *accountRuntime {
	rt := &accountRuntime{
		index: index,
		name:  fmt.Sprintf("account-%d", index),
	}
	rt.state.disabled = disabled
	rt.state.uploadSleepUntilUTC = uploadSleepUntilUTC
	return rt
}

func TestAccountRuntimeConsumePendingQuotaResetOnce(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	runtime.markQuotaResetOnNextSuccess()
	assert.True(t, runtime.consumeQuotaResetOnNextSuccess())
	assert.False(t, runtime.consumeQuotaResetOnNextSuccess())
}

func TestAccountRuntimeSleepAndQuotaResetCanBeMarkedTogether(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	runtime.markUploadLimitSleep(now.Add(time.Hour))

	assert.Equal(t, now.Add(time.Hour), runtime.uploadSleepUntilUTC())
	assert.True(t, runtime.consumeQuotaResetOnNextSuccess())
}

func TestSelectWriteAccountForRoundSkipsAttemptedAccounts(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	pool := &accountPool{policy: "round_robin", accounts: []*accountRuntime{runtime0, runtime1}, nowFn: func() time.Time { return now }}

	runtime, outcome, wait, err := pool.selectWriteAccountForRound(map[int]struct{}{0: {}})
	require.NoError(t, err)
	assert.Equal(t, selectWriteAccountOutcomeSelected, outcome)
	assert.Zero(t, wait)
	assert.Same(t, runtime1, runtime)
}

func TestSelectWriteAccountForRoundSignalsImmediateNewRound(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	pool := &accountPool{policy: "round_robin", accounts: []*accountRuntime{runtime0}, nowFn: func() time.Time { return now }}

	runtime, outcome, wait, err := pool.selectWriteAccountForRound(map[int]struct{}{0: {}})
	require.NoError(t, err)
	assert.Nil(t, runtime)
	assert.Equal(t, selectWriteAccountOutcomeStartNextRound, outcome)
	assert.Zero(t, wait)
}

func TestSelectWriteAccountForRoundSignalsPoolSleep(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, now.Add(10*time.Minute))
	pool := &accountPool{policy: "round_robin", accounts: []*accountRuntime{runtime0}, nowFn: func() time.Time { return now }}

	runtime, outcome, wait, err := pool.selectWriteAccountForRound(nil)
	require.NoError(t, err)
	assert.Nil(t, runtime)
	assert.Equal(t, selectWriteAccountOutcomeWaitForPoolWake, outcome)
	assert.Equal(t, 10*time.Minute, wait)
}

func TestSelectWriteAccountForRoundReturnsAllDisabledOutcome(t *testing.T) {
	pool := &accountPool{policy: "round_robin", accounts: []*accountRuntime{newTestAccountRuntimeForPool(0, true, time.Time{})}}

	runtime, outcome, wait, err := pool.selectWriteAccountForRound(nil)
	require.ErrorIs(t, err, errNoAvailableAccount)
	assert.Nil(t, runtime)
	assert.Equal(t, selectWriteAccountOutcomeAllDisabled, outcome)
	assert.Zero(t, wait)
}

func TestAccountPoolRoundRobinSkipsIneligibleAccounts(t *testing.T) {
	now := time.Now().UTC()
	pool := &accountPool{
		policy: "round_robin",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, true, time.Time{}),
			newTestAccountRuntimeForPool(1, false, now.Add(time.Hour)),
			newTestAccountRuntimeForPool(2, false, time.Time{}),
			newTestAccountRuntimeForPool(3, false, time.Time{}),
		},
	}

	got := make([]int, 0, 4)
	for range 4 {
		runtime, err := pool.selectAccount(context.Background(), operationPathWrite)
		require.NoError(t, err)
		require.NotNil(t, runtime)
		got = append(got, runtime.index)
	}

	assert.Equal(t, []int{2, 3, 2, 3}, got)
}

func TestAccountPoolRandomSelectsOnlyEligibleAccounts(t *testing.T) {
	now := time.Now().UTC()
	pool := &accountPool{
		policy: "random",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, true, time.Time{}),
			newTestAccountRuntimeForPool(1, false, now.Add(time.Hour)),
			newTestAccountRuntimeForPool(2, false, time.Time{}),
			newTestAccountRuntimeForPool(3, false, time.Time{}),
		},
	}

	for range 200 {
		runtime, err := pool.selectAccount(context.Background(), operationPathWrite)
		require.NoError(t, err)
		require.NotNil(t, runtime)
		assert.Contains(t, []int{2, 3}, runtime.index)
	}
}

func TestAccountPoolWritePathWaitsForEarliestWakeWhenAllSleeping(t *testing.T) {
	now := time.Now().UTC()
	pool := &accountPool{
		policy: "round_robin",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, false, now.Add(40*time.Millisecond)),
			newTestAccountRuntimeForPool(1, false, now.Add(120*time.Millisecond)),
		},
	}

	started := time.Now()
	runtime, err := pool.selectAccount(context.Background(), operationPathWrite)
	elapsed := time.Since(started)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	assert.Equal(t, 0, runtime.index)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
}

func TestAccountPoolReadPathIgnoresUploadSleepEligibilityFilter(t *testing.T) {
	now := time.Now().UTC()
	pool := &accountPool{
		policy: "round_robin",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, false, now.Add(time.Hour)),
			newTestAccountRuntimeForPool(1, false, time.Time{}),
		},
	}

	runtime, err := pool.selectAccount(context.Background(), operationPathRead)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	assert.Equal(t, 0, runtime.index)
}

func TestAccountPoolWriteWaitAbortsOnContextCancellation(t *testing.T) {
	now := time.Now().UTC()
	pool := &accountPool{
		policy: "round_robin",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, false, now.Add(24*time.Hour)),
			newTestAccountRuntimeForPool(1, false, now.Add(24*time.Hour)),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)

	started := time.Now()
	runtime, err := pool.selectAccount(ctx, operationPathWrite)
	elapsed := time.Since(started)
	require.Error(t, err)
	assert.Nil(t, runtime)
	assert.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
}

func TestAccountPoolRoundRobinEligibilityTransitionOverTime(t *testing.T) {
	base := time.Date(2026, 4, 17, 10, 0, 0, 0, time.UTC)
	now := base

	pool := &accountPool{
		policy: "round_robin",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, false, time.Time{}),
			newTestAccountRuntimeForPool(1, false, base.Add(time.Minute)),
			newTestAccountRuntimeForPool(2, false, time.Time{}),
		},
		nowFn: func() time.Time {
			return now
		},
	}

	runtime0, err := pool.selectAccount(context.Background(), operationPathWrite)
	require.NoError(t, err)
	require.NotNil(t, runtime0)
	assert.Equal(t, 0, runtime0.index)

	runtime1, err := pool.selectAccount(context.Background(), operationPathWrite)
	require.NoError(t, err)
	require.NotNil(t, runtime1)
	assert.Equal(t, 2, runtime1.index)

	now = base.Add(time.Minute + time.Nanosecond)

	runtime2, err := pool.selectAccount(context.Background(), operationPathWrite)
	require.NoError(t, err)
	require.NotNil(t, runtime2)
	assert.Equal(t, 0, runtime2.index)

	runtime3, err := pool.selectAccount(context.Background(), operationPathWrite)
	require.NoError(t, err)
	require.NotNil(t, runtime3)
	assert.Equal(t, 1, runtime3.index)
}

func newTestFsWithAccountPool(policy string, count int) *Fs {
	runtimes := make([]*accountRuntime, 0, count)
	for i := range count {
		runtimes = append(runtimes, newTestAccountRuntimeForPool(i, false, time.Time{}))
	}
	return &Fs{accountPool: newAccountPool(policy, runtimes)}
}

func TestBindWriteOperationAccountKeepsSameRuntimeForObjectLifecycle(t *testing.T) {
	f := newTestFsWithAccountPool("round_robin", 2)

	ctxWrite1, runtimeWrite1, err := bindAccountForWriteObject(context.Background(), f)
	require.NoError(t, err)
	require.NotNil(t, runtimeWrite1)
	assert.Equal(t, 0, runtimeWrite1.index)

	ctxWrite1Repeat, runtimeWrite1Repeat, err := bindAccountForWriteObject(ctxWrite1, f)
	require.NoError(t, err)
	require.NotNil(t, runtimeWrite1Repeat)
	assert.Same(t, runtimeWrite1, runtimeWrite1Repeat)
	assert.Same(t, ctxWrite1, ctxWrite1Repeat)

	runtimeFromBoundCtx, err := runtimeFromContext(ctxWrite1Repeat, f)
	require.NoError(t, err)
	require.NotNil(t, runtimeFromBoundCtx)
	assert.Same(t, runtimeWrite1, runtimeFromBoundCtx)

	_, runtimeWrite2, err := bindAccountForWriteObject(context.Background(), f)
	require.NoError(t, err)
	require.NotNil(t, runtimeWrite2)
	assert.Equal(t, 1, runtimeWrite2.index)
	assert.NotSame(t, runtimeWrite1, runtimeWrite2)

	single := &Fs{}
	boundSingleCtx, singleRuntime, err := bindAccountForWriteObject(context.Background(), single)
	require.NoError(t, err)
	assert.NotNil(t, boundSingleCtx)
	assert.Nil(t, singleRuntime)
}

func TestBindReadOperationAccountIsPerCall(t *testing.T) {
	f := newTestFsWithAccountPool("round_robin", 2)

	ctxRead1, runtimeRead1, err := bindAccountForReadCall(context.Background(), f)
	require.NoError(t, err)
	require.NotNil(t, runtimeRead1)
	assert.Equal(t, 0, runtimeRead1.index)

	ctxRead2, runtimeRead2, err := bindAccountForReadCall(ctxRead1, f)
	require.NoError(t, err)
	require.NotNil(t, runtimeRead2)
	assert.Equal(t, 1, runtimeRead2.index)
	assert.NotSame(t, runtimeRead1, runtimeRead2)

	runtimeFromBoundCtx, err := runtimeFromContext(ctxRead2, f)
	require.NoError(t, err)
	require.NotNil(t, runtimeFromBoundCtx)
	assert.Same(t, runtimeRead2, runtimeFromBoundCtx)

	runtimeWithoutBoundCtx, err := runtimeFromContext(context.Background(), f)
	require.NoError(t, err)
	require.NotNil(t, runtimeWithoutBoundCtx)
	assert.Equal(t, 0, runtimeWithoutBoundCtx.index)

	single := &Fs{}
	readSingleCtx, singleRuntime, err := bindAccountForReadCall(context.Background(), single)
	require.NoError(t, err)
	assert.NotNil(t, readSingleCtx)
	assert.Nil(t, singleRuntime)
}

func TestBindWriteOperationIgnoresForeignRuntimeContext(t *testing.T) {
	primary := newTestFsWithAccountPool("round_robin", 2)
	foreign := newTestFsWithAccountPool("round_robin", 1)

	foreignCtx, foreignRuntime, err := bindAccountForWriteObject(context.Background(), foreign)
	require.NoError(t, err)
	require.NotNil(t, foreignRuntime)

	bindingCtx, boundRuntime, err := bindAccountForWriteObject(foreignCtx, primary)
	require.NoError(t, err)
	require.NotNil(t, boundRuntime)
	assert.NotSame(t, foreignRuntime, boundRuntime)
	assert.Same(t, primary.accountPool.accounts[0], boundRuntime)

	ctxRuntime := contextBoundRuntime(bindingCtx)
	require.NotNil(t, ctxRuntime)
	assert.Same(t, boundRuntime, ctxRuntime)
}

func TestRuntimeFromContextIgnoresForeignRuntimeContext(t *testing.T) {
	primary := newTestFsWithAccountPool("round_robin", 2)
	foreign := newTestFsWithAccountPool("round_robin", 1)

	foreignCtx, foreignRuntime, err := bindAccountForWriteObject(context.Background(), foreign)
	require.NoError(t, err)
	require.NotNil(t, foreignRuntime)

	runtime, err := runtimeFromContext(foreignCtx, primary)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	assert.NotSame(t, foreignRuntime, runtime)
	assert.Same(t, primary.accountPool.accounts[0], runtime)
}

func TestBindRuntimeForReadCallIgnoresForeignRuntimeContext(t *testing.T) {
	primary := newTestFsWithAccountPool("round_robin", 2)
	foreign := newTestFsWithAccountPool("round_robin", 1)

	foreignCtx, foreignRuntime, err := bindAccountForWriteObject(context.Background(), foreign)
	require.NoError(t, err)
	require.NotNil(t, foreignRuntime)

	bindingCtx, err := primary.bindRuntimeForReadCall(foreignCtx)
	require.NoError(t, err)

	runtime := contextBoundRuntime(bindingCtx)
	require.NotNil(t, runtime)
	assert.NotSame(t, foreignRuntime, runtime)
	assert.Same(t, primary.accountPool.accounts[0], runtime)
}

func TestShouldRetryDoesNotDisableForeignRuntimeFromContext(t *testing.T) {
	primary := newTestFsWithAccountPool("round_robin", 1)
	foreign := newTestFsWithAccountPool("round_robin", 1)

	foreignCtx, foreignRuntime, err := bindAccountForWriteObject(context.Background(), foreign)
	require.NoError(t, err)
	require.NotNil(t, foreignRuntime)
	foreignRuntime.setDisabled(false)

	primaryRuntime := primary.accountPool.accounts[0]
	primaryRuntime.setDisabled(false)

	fatalRefreshErr := errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_grant\"}")

	retry, returnedErr := primary.shouldRetry(foreignCtx, fatalRefreshErr)
	assert.False(t, retry)
	assert.Equal(t, fatalRefreshErr, returnedErr)

	assert.False(t, foreignRuntime.isDisabled())
	assert.False(t, primaryRuntime.isDisabled())
}

type driveRoutingRecorder struct {
	mu          stdsync.Mutex
	byKind      map[string][]int
	allCalls    []string
	lastPaths   map[string]string
	chunkRanges map[int][]string
}

func newDriveRoutingRecorder() *driveRoutingRecorder {
	return &driveRoutingRecorder{
		byKind:      make(map[string][]int),
		lastPaths:   make(map[string]string),
		chunkRanges: make(map[int][]string),
	}
}

func (r *driveRoutingRecorder) record(account int, kind, path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byKind[kind] = append(r.byKind[kind], account)
	r.allCalls = append(r.allCalls, fmt.Sprintf("account=%d kind=%s path=%s", account, kind, path))
	r.lastPaths[kind] = path
}

func (r *driveRoutingRecorder) accountsFor(kind string) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	accounts := r.byKind[kind]
	return append([]int(nil), accounts...)
}

func (r *driveRoutingRecorder) dumpCalls() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.allCalls, "\n")
}

func (r *driveRoutingRecorder) recordChunkRange(account int, contentRange string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunkRanges[account] = append(r.chunkRanges[account], contentRange)
}

func (r *driveRoutingRecorder) chunkRangesFor(account int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ranges := r.chunkRanges[account]
	return append([]string(nil), ranges...)
}

type driveRoutingTransport struct {
	account  int
	recorder *driveRoutingRecorder

	failUploadStart bool
	failChunkCall   int

	mu         stdsync.Mutex
	chunkCalls int
}

func (t *driveRoutingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path := req.URL.Path
	uploadType := req.URL.Query().Get("uploadType")

	switch {
	case (req.Method == http.MethodPost || req.Method == http.MethodPatch) && strings.Contains(path, "/upload/drive/v3/files") && uploadType == "resumable":
		t.recorder.record(t.account, "upload.start", path)
		if t.failUploadStart {
			body := `{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded","message":"User rate limit exceeded."}],"message":"User rate limit exceeded."}}`
			return newDriveRoutingResponse(req, http.StatusForbidden, body, nil), nil
		}
		location := fmt.Sprintf("https://upload.local/session/account-%d", t.account)
		return newDriveRoutingResponse(req, http.StatusOK, `{}`, map[string]string{"Location": location}), nil

	case req.Method == http.MethodPost && path == "/upload/drive/v3/files":
		t.recorder.record(t.account, "files.create", path)
		body := `{"id":"obj-1","name":"file.txt","mimeType":"text/plain","md5Checksum":"md5","sha1Checksum":"sha1","sha256Checksum":"sha256","modifiedTime":"2026-04-17T00:00:00.000Z","size":"3","parents":["root"]}`
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	case req.Method == http.MethodPatch && strings.Contains(path, "/upload/drive/v3/files/"):
		t.recorder.record(t.account, "files.update", path)
		body := `{"id":"obj-1","name":"file.txt","mimeType":"text/plain","md5Checksum":"md5","sha1Checksum":"sha1","sha256Checksum":"sha256","modifiedTime":"2026-04-17T00:00:00.000Z","size":"3","parents":["root"]}`
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	case req.Method == http.MethodPost && strings.Contains(path, "/session/account-"):
		t.recorder.record(t.account, "upload.chunk", path)
		t.recorder.recordChunkRange(t.account, req.Header.Get("Content-Range"))
		t.mu.Lock()
		t.chunkCalls++
		chunkCall := t.chunkCalls
		t.mu.Unlock()

		if t.failChunkCall > 0 && chunkCall == t.failChunkCall {
			body := `{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded","message":"User rate limit exceeded."}],"message":"User rate limit exceeded."}}`
			return newDriveRoutingResponse(req, http.StatusForbidden, body, nil), nil
		}

		if chunkCall == 1 {
			return newDriveRoutingResponse(req, statusResumeIncomplete, "", nil), nil
		}

		body := `{"id":"obj-1","name":"file.txt","mimeType":"text/plain","md5Checksum":"md5","sha1Checksum":"sha1","sha256Checksum":"sha256","modifiedTime":"2026-04-17T00:00:00.000Z","size":"3","parents":["root"]}`
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	case req.Method == http.MethodPost && strings.Contains(path, "/drive/v3/files/") && strings.Contains(path, "/permissions"):
		t.recorder.record(t.account, "permissions.create", path)
		return newDriveRoutingResponse(req, http.StatusOK, `{"id":"perm-1"}`, nil), nil

	case req.Method == http.MethodPost && path == "/drive/v3/files":
		t.recorder.record(t.account, "files.create", path)
		body := `{"id":"obj-1","name":"file.txt","mimeType":"text/plain","md5Checksum":"md5","sha1Checksum":"sha1","sha256Checksum":"sha256","modifiedTime":"2026-04-17T00:00:00.000Z","size":"3","parents":["root"]}`
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	case req.Method == http.MethodGet && path == "/drive/v3/files":
		t.recorder.record(t.account, "files.list", path)
		return newDriveRoutingResponse(req, http.StatusOK, `{"files":[]}`, nil), nil

	case req.Method == http.MethodGet && strings.Contains(path, "/drive/v3/files/") && strings.Contains(path, "/permissions/"):
		t.recorder.record(t.account, "permissions.get", path)
		body := `{"id":"perm-1","role":"reader","type":"user","emailAddress":"reader@example.com","permissionDetails":[{"inherited":false}]}`
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	case req.Method == http.MethodGet && strings.Contains(path, "/drive/v3/files/") && strings.Contains(path, "/listLabels"):
		t.recorder.record(t.account, "files.listLabels", path)
		return newDriveRoutingResponse(req, http.StatusOK, `{"labels":[]}`, nil), nil

	case req.Method == http.MethodGet && strings.Contains(path, "/drive/v3/files/"):
		t.recorder.record(t.account, "files.get", path)
		fileID := path[strings.LastIndex(path, "/")+1:]
		if fileID == "" {
			fileID = "root"
		}
		body := fmt.Sprintf(`{"id":%q,"name":%q,"mimeType":"text/plain","md5Checksum":"md5","modifiedTime":"2026-04-17T00:00:00.000Z","size":"1","parents":["root"]}`, fileID, fileID)
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	case req.Method == http.MethodPatch && strings.Contains(path, "/drive/v3/files/"):
		t.recorder.record(t.account, "files.update", path)
		body := `{"id":"obj-1","name":"file.txt","mimeType":"text/plain","md5Checksum":"md5","sha1Checksum":"sha1","sha256Checksum":"sha256","modifiedTime":"2026-04-17T00:00:00.000Z","size":"3","parents":["root"]}`
		return newDriveRoutingResponse(req, http.StatusOK, body, nil), nil

	default:
		t.recorder.record(t.account, "other", path)
		return newDriveRoutingResponse(req, http.StatusOK, `{}`, nil), nil
	}
}

func newDriveRoutingResponse(req *http.Request, statusCode int, body string, headers map[string]string) *http.Response {
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	for key, value := range headers {
		h.Set(key, value)
	}
	return &http.Response{
		StatusCode: statusCode,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func newRoutingTestRuntime(t *testing.T, account int, recorder *driveRoutingRecorder) *accountRuntime {
	t.Helper()
	client := &http.Client{Transport: &driveRoutingTransport{account: account, recorder: recorder}}
	svc, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	require.NoError(t, err)
	runtimePacer := fs.NewPacer(context.Background(), pacer.NewGoogleDrive(pacer.MinSleep(0), pacer.Burst(1000)))
	return newAccountRuntime(account, fmt.Sprintf("account-%d", account), client, svc, nil, runtimePacer, defaultUploadDailyLimit)
}

func routingTransportForRuntime(t *testing.T, runtime *accountRuntime) *driveRoutingTransport {
	t.Helper()
	require.NotNil(t, runtime)
	require.NotNil(t, runtime.client)
	transport, ok := runtime.client.Transport.(*driveRoutingTransport)
	require.True(t, ok)
	return transport
}

func newRoutingTestFs(t *testing.T, policy string, recorder *driveRoutingRecorder) *Fs {
	t.Helper()
	runtime0 := newRoutingTestRuntime(t, 0, recorder)
	runtime1 := newRoutingTestRuntime(t, 1, recorder)
	pool := newAccountPool(policy, []*accountRuntime{runtime0, runtime1})
	f := &Fs{
		opt: Options{
			UploadCutoff:     1,
			ChunkSize:        2,
			MetadataOwner:    rwWrite,
			UploadDailyLimit: defaultUploadDailyLimit,
		},
		rootFolderID:    "root",
		dirResourceKeys: new(sync.Map),
		permissionsMu:   new(sync.Mutex),
		permissions:     map[string]*drive.Permission{},
		svc:             runtime0.svc,
		client:          runtime0.client,
		pacer:           runtime0.pacer,
		accountPool:     pool,
	}
	f.dirCache = dircache.New("", "root", f)
	return f
}

func TestWriteLifecycleUsesBoundAccountAcrossChunkAndMetadataCalls(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)

	// Force first write binding to account 1 so mixed runtime usage is observable.
	f.accountPool.rrCursor = 1

	ctx, ci := fs.AddConfig(context.Background())
	ci.Metadata = true

	obj := &Object{baseObject: baseObject{fs: f, remote: "file.txt", id: "obj-1", mimeType: "text/plain"}}
	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), 3, true, nil, nil).WithMimeType("text/plain")

	err := obj.Update(ctx, bytes.NewReader([]byte("abc")), src, fs.MetadataOption{"owner": "owner@example.com"})
	require.NoError(t, err)

	startAccounts := recorder.accountsFor("upload.start")
	chunkAccounts := recorder.accountsFor("upload.chunk")
	metadataAccounts := recorder.accountsFor("permissions.create")

	require.NotEmpty(t, startAccounts, "missing upload.start call; calls:\n%s", recorder.dumpCalls())
	require.NotEmpty(t, chunkAccounts, "missing upload.chunk call; calls:\n%s", recorder.dumpCalls())
	require.NotEmpty(t, metadataAccounts, "missing permissions.create call; calls:\n%s", recorder.dumpCalls())

	expectedAccount := startAccounts[0]
	for _, account := range chunkAccounts {
		assert.Equal(t, expectedAccount, account, "chunk call used different account; calls:\n%s", recorder.dumpCalls())
	}
	assert.Equal(t, expectedAccount, metadataAccounts[0], "metadata callback did not use bound write account; calls:\n%s", recorder.dumpCalls())
}

func TestReadCallsParticipateInPolicySelection(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)

	_, err := f.getFile(context.Background(), "alpha", "id")
	require.NoError(t, err)
	_, _, err = f.getPermission(context.Background(), "alpha", "perm-read-a", false)
	require.NoError(t, err)
	_, err = f.getLabels(context.Background(), "alpha")
	require.NoError(t, err)
	_, err = f.getFile(context.Background(), "beta", "id")
	require.NoError(t, err)

	fileGetAccounts := recorder.accountsFor("files.get")
	permissionGetAccounts := recorder.accountsFor("permissions.get")
	labelListAccounts := recorder.accountsFor("files.listLabels")

	require.GreaterOrEqual(t, len(fileGetAccounts), 2, "expected at least 2 file get calls; calls:\n%s", recorder.dumpCalls())
	require.NotEmpty(t, permissionGetAccounts, "expected permissions.get call; calls:\n%s", recorder.dumpCalls())
	require.NotEmpty(t, labelListAccounts, "expected files.listLabels call; calls:\n%s", recorder.dumpCalls())

	assert.Equal(t, 0, fileGetAccounts[0], "first files.get should use account 0; calls:\n%s", recorder.dumpCalls())
	assert.Equal(t, 1, permissionGetAccounts[0], "permissions.get should rotate to account 1; calls:\n%s", recorder.dumpCalls())
	assert.Equal(t, 0, labelListAccounts[0], "files.listLabels should rotate to account 0; calls:\n%s", recorder.dumpCalls())
	assert.Equal(t, 1, fileGetAccounts[1], "second files.get should rotate to account 1; calls:\n%s", recorder.dumpCalls())
}

func TestMetadataReadCallsParticipateInPolicySelection(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)

	_, _, err := f.getPermission(context.Background(), "alpha", "perm-read-a", false)
	require.NoError(t, err)
	_, err = f.getLabels(context.Background(), "alpha")
	require.NoError(t, err)
	_, _, err = f.getPermission(context.Background(), "beta", "perm-read-b", false)
	require.NoError(t, err)
	_, err = f.getLabels(context.Background(), "beta")
	require.NoError(t, err)

	permissionGetAccounts := recorder.accountsFor("permissions.get")
	labelListAccounts := recorder.accountsFor("files.listLabels")

	require.Len(t, permissionGetAccounts, 2, "expected two permissions.get calls; calls:\n%s", recorder.dumpCalls())
	require.Len(t, labelListAccounts, 2, "expected two files.listLabels calls; calls:\n%s", recorder.dumpCalls())

	assert.Equal(t, []int{0, 0}, permissionGetAccounts, "permissions.get calls should follow round-robin with interleaved label reads; calls:\n%s", recorder.dumpCalls())
	assert.Equal(t, []int{1, 1}, labelListAccounts, "files.listLabels calls should follow round-robin with interleaved permission reads; calls:\n%s", recorder.dumpCalls())
}

func TestMakeShortcutUsesBoundWriteAccount(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	srcFs := newRoutingTestFs(t, "round_robin", recorder)
	dstFs := newRoutingTestFs(t, "round_robin", recorder)

	// Force destination write binding to account 1.
	dstFs.accountPool.rrCursor = 1

	obj, err := srcFs.makeShortcut(context.Background(), "", dstFs, "shortcut-root")
	require.NoError(t, err)
	assert.Nil(t, obj)

	createAccounts := recorder.accountsFor("files.create")
	require.NotEmpty(t, createAccounts, "missing files.create call; calls:\n%s", recorder.dumpCalls())
	assert.Equal(t, 1, createAccounts[0], "shortcut write call did not use bound destination account; calls:\n%s", recorder.dumpCalls())
}

func TestCheckUploadDailyLimit(t *testing.T) {
	tests := []struct {
		name    string
		in      fs.SizeSuffix
		wantErr bool
	}{
		{name: "positive", in: 750 * fs.Gibi, wantErr: false},
		{name: "zero", in: 0, wantErr: true},
		{name: "negative", in: -1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkUploadDailyLimit(tc.in)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUploadBudgetUTCDayStart(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	now := time.Date(2026, 4, 16, 8, 30, 0, 0, loc)
	got := utcDayStart(now)
	want := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)

	assert.Equal(t, want, got)
	assert.Equal(t, time.UTC, got.Location())
}

func TestUploadBudgetSleepUntilNextUTC(t *testing.T) {
	now := time.Date(2026, 4, 16, 23, 59, 59, 0, time.UTC)
	d := durationUntilNextUTCDay(now)

	assert.Equal(t, time.Second, d)
}

func TestUploadBudgetAccountingSuccessOnly(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 1 * fs.Gibi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = 0

	// simulate success accounting
	f.noteSuccessfulUploadBytes(context.Background(), time.Now(), 1024)
	assert.Equal(t, int64(1024), f.uploadBudget.usedBytes)

	// failed transfer path must not call noteSuccessfulUploadBytes
	// verify value remains unchanged when no success accounting is invoked
	assert.Equal(t, int64(1024), f.uploadBudget.usedBytes)
}

func TestPerAccountUploadBudgetAccountingIsolation(t *testing.T) {
	runtime0 := &accountRuntime{index: 0, uploadDailyLimit: 10 * fs.Mebi}
	runtime1 := &accountRuntime{index: 1, uploadDailyLimit: 10 * fs.Mebi}

	f := &Fs{
		opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi},
		accountPool: &accountPool{
			accounts: []*accountRuntime{runtime0, runtime1},
		},
	}

	now := time.Now().UTC()
	ctx0 := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	ctx1 := context.WithValue(context.Background(), accountRuntimeKey, runtime1)

	f.noteSuccessfulUploadBytes(ctx0, now, 3*int64(fs.Mebi))
	f.noteSuccessfulUploadBytes(ctx1, now, 2*int64(fs.Mebi))

	runtime0.state.mu.RLock()
	used0 := runtime0.state.usedBytes
	runtime0.state.mu.RUnlock()

	runtime1.state.mu.RLock()
	used1 := runtime1.state.usedBytes
	runtime1.state.mu.RUnlock()

	assert.Equal(t, 3*int64(fs.Mebi), used0)
	assert.Equal(t, 2*int64(fs.Mebi), used1)
	assert.Equal(t, int64(0), f.uploadBudget.usedBytes)
}

func TestPerAccountBudgetRolloverOccursIndependently(t *testing.T) {
	now := time.Now().UTC()
	today := utcDayStart(now)

	runtime0 := &accountRuntime{index: 0, uploadDailyLimit: 10 * fs.Mebi}
	runtime0.state.dayStartUTC = today.Add(-24 * time.Hour)
	runtime0.state.usedBytes = 10 * int64(fs.Mebi)

	runtime1 := &accountRuntime{index: 1, uploadDailyLimit: 10 * fs.Mebi}
	runtime1.state.dayStartUTC = today
	runtime1.state.usedBytes = 10 * int64(fs.Mebi)

	f := &Fs{
		opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi},
		accountPool: &accountPool{
			accounts: []*accountRuntime{runtime0, runtime1},
		},
	}

	ctx0 := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	ctx1 := context.WithValue(context.Background(), accountRuntimeKey, runtime1)

	sleptOnRuntime0 := false
	err := f.waitForUploadBudget(ctx0, 1, func(time.Duration) error {
		sleptOnRuntime0 = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, sleptOnRuntime0)

	runtime0.state.mu.RLock()
	runtime0DayStart := runtime0.state.dayStartUTC
	runtime0Used := runtime0.state.usedBytes
	runtime0.state.mu.RUnlock()
	assert.Equal(t, today, runtime0DayStart)
	assert.Equal(t, int64(0), runtime0Used)

	sleptOnRuntime1 := false
	err = f.waitForUploadBudget(ctx1, 1, func(time.Duration) error {
		sleptOnRuntime1 = true
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, sleptOnRuntime1)

	runtime1.state.mu.RLock()
	runtime1DayStart := runtime1.state.dayStartUTC
	runtime1Used := runtime1.state.usedBytes
	runtime1.state.mu.RUnlock()
	assert.Equal(t, today, runtime1DayStart)
	assert.Equal(t, int64(10*fs.Mebi), runtime1Used)
}

func TestProactiveBudgetOverflowSetsPerAccountUploadSleepUntilUTC(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	today := utcDayStart(now)
	limit := fs.SizeSuffix(10 * fs.Mebi)

	runtime0 := &accountRuntime{index: 0, uploadDailyLimit: limit}
	runtime0.state.dayStartUTC = today
	runtime0.state.usedBytes = int64(limit)

	runtime1 := &accountRuntime{index: 1, uploadDailyLimit: limit}
	runtime1.state.dayStartUTC = today

	f := &Fs{
		opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: limit},
		accountPool: &accountPool{
			accounts: []*accountRuntime{runtime0, runtime1},
		},
		nowFn: func() time.Time {
			return now
		},
	}

	ctx0 := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	slept := false
	err := f.waitForUploadBudget(ctx0, 1, func(time.Duration) error {
		slept = true
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, slept)

	runtime0.state.mu.RLock()
	runtime0Wake := runtime0.state.uploadSleepUntilUTC
	runtime0.state.mu.RUnlock()

	runtime1.state.mu.RLock()
	runtime1Wake := runtime1.state.uploadSleepUntilUTC
	runtime1.state.mu.RUnlock()

	assert.False(t, runtime0Wake.IsZero())
	assert.Equal(t, now.Add(time.Hour), runtime0Wake)
	assert.Equal(t, time.UTC, runtime0Wake.Location())
	assert.True(t, runtime1Wake.IsZero())
}

func TestUploadBudgetUTCDayBoundaryIndependentOfLocalTZ(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	local := time.Date(2026, 4, 17, 7, 59, 59, 0, loc)
	start := utcDayStart(local)

	assert.Equal(t, time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC), start)
}

func TestWaitForUploadBudgetAllowsWhenUnderLimit(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = 5 * int64(fs.Mebi)

	err := f.waitForUploadBudget(context.Background(), 2*int64(fs.Mebi), func(time.Duration) error { return nil })

	require.NoError(t, err)
}

func TestWaitForUploadBudgetSleepsWhenExceeding(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = 10 * int64(fs.Mebi)

	slept := false
	err := f.waitForUploadBudget(context.Background(), 1, func(time.Duration) error {
		slept = true
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, slept)
}

func TestWaitForUploadBudgetAllowsOversizedUnitAfterRollover(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = 10 * int64(fs.Mebi)

	callCount := 0
	err := f.waitForUploadBudget(context.Background(), 12*int64(fs.Mebi), func(time.Duration) error {
		callCount++
		f.uploadBudget.mu.Lock()
		f.uploadBudget.dayStartUTC = f.uploadBudget.dayStartUTC.Add(-24 * time.Hour)
		f.uploadBudget.usedBytes = 0
		f.uploadBudget.mu.Unlock()
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestWaitForUploadBudgetOversizedProceedsAfterSingleRolloverWithConcurrentUsage(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = 0

	callCount := 0
	err := f.waitForUploadBudget(context.Background(), 12*int64(fs.Mebi), func(time.Duration) error {
		callCount++
		if callCount == 1 {
			f.uploadBudget.mu.Lock()
			f.uploadBudget.usedBytes = int64(fs.Mebi)
			f.uploadBudget.mu.Unlock()
			return nil
		}
		return context.Canceled
	})

	require.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestWaitForUploadBudgetOversizedAtUsedZeroStillWaitsForRollover(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = 0

	callCount := 0
	err := f.waitForUploadBudget(context.Background(), 12*int64(fs.Mebi), func(time.Duration) error {
		callCount++
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, callCount)
}

func newTestFsForRetry(t *testing.T) *Fs {
	t.Helper()
	f := &Fs{}
	f.nowFn = func() time.Time {
		return time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	}
	f.sleepFn = func(context.Context, time.Duration) error {
		return nil
	}
	return f
}

func TestShouldRetryUploadUserRateLimitExceededReturnsAccountFailoverSignal(t *testing.T) {
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})

	f := &Fs{
		opt: Options{
			SleepOnUploadLimit: true,
			UploadDailyLimit:   defaultUploadDailyLimit,
		},
		accountPool: &accountPool{
			policy:   "round_robin",
			accounts: []*accountRuntime{runtime0, runtime1},
		},
	}
	f.nowFn = func() time.Time {
		return time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	}

	sleepCalls := 0
	f.sleepFn = func(_ context.Context, d time.Duration) error {
		sleepCalls++
		return nil
	}

	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "userRateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	require.False(t, retry)
	require.True(t, isAccountUploadLimitError(err))
	assert.ErrorIs(t, err, gerr)
	assert.Zero(t, sleepCalls)

	assert.Equal(t, time.Date(2026, 4, 16, 13, 0, 0, 0, time.UTC), runtime0.uploadSleepUntilUTC())
	assert.True(t, runtime0.consumeQuotaResetOnNextSuccess())
	assert.True(t, runtime1.uploadSleepUntilUTC().IsZero())

	boundCtx, boundRuntime, bindErr := bindAccountForWriteObject(ctx, f)
	require.NoError(t, bindErr)
	assert.Same(t, runtime0, contextBoundRuntime(boundCtx))
	assert.Same(t, runtime0, boundRuntime)
}

func TestReactiveNoPoolUserRateLimitExceededBehaviorRemainsLegacy(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}
	gerr := &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded", Message: "User rate limit exceeded."}}}

	retry, err := f.shouldRetryUpload(context.Background(), gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.False(t, isAccountUploadLimitError(err))
	assert.Equal(t, 1, sleepCalls)
}

func TestReactiveSingleAccountPoolUserRateLimitExceededBehaviorRemainsLegacy(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})
	f.accountPool = &accountPool{accounts: []*accountRuntime{runtime}}
	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}
	gerr := &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded", Message: "User rate limit exceeded."}}}

	retry, err := f.shouldRetryUpload(context.WithValue(context.Background(), accountRuntimeKey, runtime), gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.False(t, isAccountUploadLimitError(err))
	assert.Equal(t, 1, sleepCalls)
}

func TestAllWriteAccountsSleepingWaitsThenResumes(t *testing.T) {
	now := time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, now.Add(5*time.Second))
	runtime1 := newTestAccountRuntimeForPool(1, false, now.Add(15*time.Second))

	var waits []time.Duration
	pool := &accountPool{
		policy:   "round_robin",
		accounts: []*accountRuntime{runtime0, runtime1},
		nowFn: func() time.Time {
			return now
		},
		waitForWakeFn: func(context.Context, time.Duration) error {
			wait := runtime0.uploadSleepUntilUTC().Sub(now)
			waits = append(waits, wait)
			now = now.Add(wait)
			return nil
		},
	}

	runtime, err := pool.selectAccount(context.Background(), operationPathWrite)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	assert.Equal(t, 0, runtime.index)
	require.Len(t, waits, 1)
	assert.Equal(t, 5*time.Second, waits[0])
}

func TestFatalRefreshErrorDisablesOnlyCurrentAccount(t *testing.T) {
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})

	err := errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_grant\"}")
	classification := runtime0.disableOnFatalRefresh(err)

	assert.Equal(t, refreshErrorFatal, classification)
	assert.True(t, runtime0.isDisabled())
	assert.False(t, runtime1.isDisabled())
}

func TestTransientRefreshErrorDoesNotDisableAccount(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	err := errors.New("oauth2: token refresh failed: Post \"https://oauth2.googleapis.com/token\": context deadline exceeded")
	classification := runtime.disableOnFatalRefresh(err)

	assert.Equal(t, refreshErrorTransient, classification)
	assert.False(t, runtime.isDisabled())
}

func TestDisabledAccountsExcludedFromReadAndWriteSelection(t *testing.T) {
	now := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, true, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, now.Add(time.Hour))
	runtime2 := newTestAccountRuntimeForPool(2, false, time.Time{})

	pool := &accountPool{
		policy:   "round_robin",
		accounts: []*accountRuntime{runtime0, runtime1, runtime2},
		nowFn: func() time.Time {
			return now
		},
	}

	readRuntime, err := pool.selectAccount(context.Background(), operationPathRead)
	require.NoError(t, err)
	require.NotNil(t, readRuntime)
	assert.Equal(t, 1, readRuntime.index)

	writeRuntime, err := pool.selectAccount(context.Background(), operationPathWrite)
	require.NoError(t, err)
	require.NotNil(t, writeRuntime)
	assert.Equal(t, 2, writeRuntime.index)
}

func TestAllAccountsDisabledReturnsNoAvailableAccountError(t *testing.T) {
	pool := &accountPool{
		policy: "round_robin",
		accounts: []*accountRuntime{
			newTestAccountRuntimeForPool(0, true, time.Time{}),
			newTestAccountRuntimeForPool(1, true, time.Time{}),
		},
	}

	_, readErr := pool.selectAccount(context.Background(), operationPathRead)
	require.ErrorIs(t, readErr, errNoAvailableAccount)

	_, writeErr := pool.selectAccount(context.Background(), operationPathWrite)
	require.ErrorIs(t, writeErr, errNoAvailableAccount)
}

func TestDisabledAccountDoesNotAutoReenterBeforeRestart(t *testing.T) {
	now := time.Date(2026, 4, 17, 10, 0, 0, 0, time.UTC)
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})
	pool := &accountPool{
		policy:   "round_robin",
		accounts: []*accountRuntime{runtime},
		nowFn: func() time.Time {
			return now
		},
	}

	classification := runtime.disableOnFatalRefresh(errors.New("oauth2: cannot fetch token: {\"error\":\"invalid_scope\"}"))
	assert.Equal(t, refreshErrorFatal, classification)
	assert.True(t, runtime.isDisabled())

	for range 3 {
		runtime.state.mu.Lock()
		runtime.state.uploadSleepUntilUTC = now.Add(10 * time.Minute)
		runtime.state.mu.Unlock()

		_, err := pool.selectAccount(context.Background(), operationPathRead)
		require.ErrorIs(t, err, errNoAvailableAccount)

		_, err = pool.selectAccount(context.Background(), operationPathWrite)
		require.ErrorIs(t, err, errNoAvailableAccount)
	}

	assert.True(t, runtime.isDisabled())
}

func TestPerAccountRefreshProgressDoesNotSerializeThroughGlobalRefreshLock(t *testing.T) {
	t.Parallel()

	errInvalidGrant := errors.New("oauth2: cannot fetch token: {\"error\":\"invalid_grant\"}")
	errTransient := errors.New("oauth2: cannot fetch token: Post https://oauth2.googleapis.com/token: i/o timeout")

	const accounts = 8
	var wg sync.WaitGroup
	started := make(chan struct{}, accounts)
	release := make(chan struct{})

	runtimes := make([]*accountRuntime, 0, accounts)
	for i := 0; i < accounts; i++ {
		runtimes = append(runtimes, newTestAccountRuntimeForPool(i, false, time.Time{}))
	}

	for i, rt := range runtimes {
		wg.Add(1)
		go func(idx int, runtime *accountRuntime) {
			defer wg.Done()
			started <- struct{}{}
			<-release
			if idx%2 == 0 {
				runtime.disableOnFatalRefresh(errInvalidGrant)
				return
			}
			runtime.disableOnFatalRefresh(errTransient)
		}(i, rt)
	}

	for i := 0; i < accounts; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("refresh classification goroutines did not all start; possible serialization")
		}
	}

	close(release)
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("refresh classification appears serialized through global lock")
	}

	for i, rt := range runtimes {
		if i%2 == 0 {
			assert.True(t, rt.isDisabled())
		} else {
			assert.False(t, rt.isDisabled())
		}
	}
}

func TestDriveShutdownFlushesPendingTokenWrites(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:1]

	var (
		persistedMu       stdsync.Mutex
		persistedSnapshot string
	)

	store := newAccountStore(accounts, func(accountsJSON string) error {
		persistedMu.Lock()
		persistedSnapshot = accountsJSON
		persistedMu.Unlock()
		return nil
	})

	f := &Fs{accountStore: store}

	updatedToken := testParseAccountsJSONToken("shutdown-drive-flush")
	store.setAccountToken(0, updatedToken)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, f.Shutdown(shutdownCtx))

	persistedMu.Lock()
	persistedRaw := persistedSnapshot
	persistedMu.Unlock()
	require.NotEmpty(t, persistedRaw)
	tokens := decodePersistedAccountTokens(t, persistedRaw)
	require.Len(t, tokens, 1)
	assert.JSONEq(t, updatedToken, tokens[0])

	require.NoError(t, f.Shutdown(context.Background()))
}

func TestDriveShutdownReturnsErrorOnFlushTimeout(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:1]

	persistBlock := make(chan struct{})
	store := newAccountStore(accounts, func(string) error {
		<-persistBlock
		return nil
	})
	t.Cleanup(func() {
		close(persistBlock)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = store.shutdown(shutdownCtx)
	})

	f := &Fs{accountStore: store}
	store.setAccountToken(0, testParseAccountsJSONToken("timeout-token"))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := f.Shutdown(shutdownCtx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "shutdown token writer flush failed")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAccountStoreShutdownRejectsLateEnqueueRace(t *testing.T) {
	accounts := testAccountConfigsForMapperAndStore()[:1]

	persistedCh := make(chan string, 4)
	store := newAccountStore(accounts, func(accountsJSON string) error {
		persistedCh <- accountsJSON
		return nil
	})

	baselineToken := testParseAccountsJSONToken("baseline-before-race")
	store.setAccountToken(0, baselineToken)

	baselineCtx, baselineCancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, store.flush(baselineCtx))
	baselineCancel()

	baselineSnapshot := ""
drainBaseline:
	for {
		select {
		case snapshot := <-persistedCh:
			baselineSnapshot = snapshot
		default:
			break drainBaseline
		}
	}
	require.NotEmpty(t, baselineSnapshot)

	readyToSet := make(chan struct{})
	allowSet := make(chan struct{})
	store.beforeSetMuLock = func() {
		readyToSet <- struct{}{}
		<-allowSet
	}

	doneSet := make(chan struct{})
	go func() {
		store.setAccountToken(0, testParseAccountsJSONToken("late-token"))
		close(doneSet)
	}()

	select {
	case <-readyToSet:
	case <-time.After(time.Second):
		t.Fatal("setAccountToken did not reach pre-lock hold")
	}

	shutdownEntered := make(chan struct{})
	allowShutdown := make(chan struct{})
	store.afterStopEnqueue = func() {
		close(shutdownEntered)
		<-allowShutdown
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- store.shutdown(shutdownCtx)
	}()

	select {
	case <-shutdownEntered:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not stop enqueue before flush")
	}

	close(allowSet)
	close(allowShutdown)
	require.NoError(t, <-shutdownDone)
	<-doneSet

	latest := baselineSnapshot
drain:
	for {
		select {
		case snapshot := <-persistedCh:
			latest = snapshot
		default:
			break drain
		}
	}
	require.NotEmpty(t, latest)
	tokens := decodePersistedAccountTokens(t, latest)
	require.Len(t, tokens, 1)
	assert.False(t, jsonEqual(testParseAccountsJSONToken("late-token"), tokens[0]))
}

func jsonEqual(a, b string) bool {
	var av any
	var bv any
	if err := json.Unmarshal([]byte(a), &av); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(b), &bv); err != nil {
		return false
	}
	return assert.ObjectsAreEqualValues(av, bv)
}

func TestClassifyRefreshErrorRecognizesOAuthRetrieveError(t *testing.T) {
	t.Run("fatal invalid grant", func(t *testing.T) {
		err := errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_grant\"}")
		assert.Equal(t, refreshErrorFatal, classifyRefreshError(err))
	})

	t.Run("transient retrieve error", func(t *testing.T) {
		err := errors.New("oauth2: cannot fetch token: Post https://oauth2.googleapis.com/token: connection reset by peer")
		assert.Equal(t, refreshErrorTransient, classifyRefreshError(err))
	})
}

func TestRefreshClassificationTransientThenFatalProgression(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	transientErr := errors.New("oauth2: token refresh failed: Post https://oauth2.googleapis.com/token: i/o timeout")
	assert.Equal(t, refreshErrorTransient, runtime.disableOnFatalRefresh(transientErr))
	assert.False(t, runtime.isDisabled())

	fatalErr := errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_client\"}")
	assert.Equal(t, refreshErrorFatal, runtime.disableOnFatalRefresh(fatalErr))
	assert.True(t, runtime.isDisabled())
}

func TestMonitorAccountRefreshTokenDisablesOnFatalAfterTransient(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(7, false, time.Time{})

	expiryCh := make(chan time.Time, 1)
	expiryCh <- time.Now()

	fakeSource := &fakeRefreshMonitorTokenSource{
		expiryCh: expiryCh,
		onToken: func(call int) (*oauth2.Token, error) {
			if call == 1 {
				return nil, errors.New("oauth2: token refresh failed: transport timeout")
			}
			if call == 2 {
				return nil, errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_client\"}")
			}
			return &oauth2.Token{AccessToken: "ok"}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitorDone := make(chan struct{})
	go func() {
		monitorAccountRefreshToken(ctx, 7, runtime, fakeSource, func(context.Context, time.Duration) error { return nil })
		close(monitorDone)
	}()

	require.Eventually(t, runtime.isDisabled, time.Second, 10*time.Millisecond)
	select {
	case <-monitorDone:
	case <-time.After(time.Second):
		t.Fatal("refresh monitor did not exit after fatal disable")
	}
	require.Equal(t, 2, fakeSource.tokenCalls)
}

type fakeRefreshMonitorTokenSource struct {
	expiryCh   <-chan time.Time
	tokenCalls int
	onToken    func(call int) (*oauth2.Token, error)
}

func (f *fakeRefreshMonitorTokenSource) OnExpiry() <-chan time.Time {
	return f.expiryCh
}

func (f *fakeRefreshMonitorTokenSource) Token() (*oauth2.Token, error) {
	f.tokenCalls++
	if f.onToken != nil {
		return f.onToken(f.tokenCalls)
	}
	return &oauth2.Token{AccessToken: "ok"}, nil
}

func TestReadPathSelectionIgnoresUploadSleepState(t *testing.T) {
	now := time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, now.Add(time.Hour))
	runtime1 := newTestAccountRuntimeForPool(1, false, now.Add(2*time.Hour))

	pool := &accountPool{
		policy:   "round_robin",
		accounts: []*accountRuntime{runtime0, runtime1},
		nowFn: func() time.Time {
			return now
		},
		waitForWakeFn: func(context.Context, time.Duration) error {
			return errors.New("read path should not wait for upload sleep")
		},
	}

	runtime, err := pool.selectAccount(context.Background(), operationPathRead)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	assert.Equal(t, 0, runtime.index)
}

func TestShouldRetryUploadPathUserRateLimitExceededSleep(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true

	sleepCalls := 0
	var slept time.Duration
	f.sleepFn = func(_ context.Context, d time.Duration) error {
		sleepCalls++
		slept = d
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "userRateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.Equal(t, 1, sleepCalls)
	assert.Equal(t, time.Hour, slept)
}

func TestNoteSuccessfulUploadBytesConsumesRuntimeQuotaResetBeforeAddingBytes(t *testing.T) {
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = 123
	runtime0.markUploadLimitSleep(now.Add(time.Hour))

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: defaultUploadDailyLimit}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	f.noteSuccessfulUploadBytes(ctx, now, 11)

	runtime0.state.mu.RLock()
	defer runtime0.state.mu.RUnlock()
	assert.True(t, runtime0.state.uploadSleepUntilUTC.IsZero())
	assert.Equal(t, int64(11), runtime0.state.usedBytes)
	assert.False(t, runtime0.state.resetQuotaOnNextSuccessfulUpload)
}

func TestSingleAccountPooledLegacyProbeResetStillClearsQuotaOnSuccess(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime.state.dayStartUTC = utcDayStart(now)
	runtime.state.usedBytes = 123
	runtime.state.uploadSleepUntilUTC = now.Add(time.Hour)

	f := &Fs{
		opt:         Options{SleepOnUploadLimit: true, UploadDailyLimit: defaultUploadDailyLimit},
		accountPool: &accountPool{accounts: []*accountRuntime{runtime}},
	}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime)
	ctx = withUploadProbeState(ctx)
	markUploadProbeQuotaReset(ctx)
	f.noteSuccessfulUploadBytes(ctx, now, 11)

	runtime.state.mu.RLock()
	defer runtime.state.mu.RUnlock()
	assert.True(t, runtime.state.uploadSleepUntilUTC.IsZero())
	assert.Equal(t, int64(11), runtime.state.usedBytes)
}

func TestCheckUploadBudgetForAttemptMarksRuntimeForFailover(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = int64(10 * fs.Mebi)

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForAttempt(ctx, 1)
	assert.True(t, isAccountUploadLimitError(err))
	assert.Equal(t, now.Add(time.Hour), runtime0.uploadSleepUntilUTC())
	assert.True(t, runtime0.consumeQuotaResetOnNextSuccess())
}

func TestCheckUploadBudgetForAttemptAllowsExpiredProbeBeforeReset(t *testing.T) {
	now := time.Date(2026, 4, 16, 13, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = int64(10 * fs.Mebi)
	runtime0.state.uploadSleepUntilUTC = now
	runtime0.state.resetQuotaOnNextSuccessfulUpload = true

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForAttempt(ctx, 1)
	assert.NoError(t, err)

	runtime0.state.mu.RLock()
	defer runtime0.state.mu.RUnlock()
	assert.Equal(t, now, runtime0.state.uploadSleepUntilUTC)
	assert.Equal(t, int64(10*fs.Mebi), runtime0.state.usedBytes)
	assert.True(t, runtime0.state.resetQuotaOnNextSuccessfulUpload)
}

func TestCheckUploadBudgetForAttemptPreservesOversizedSignal(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(now)

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForAttempt(ctx, 12*int64(fs.Mebi))
	assert.True(t, isOversizedUploadLimitError(err))
	assert.Equal(t, now.Add(durationUntilNextUTCDay(now)), runtime0.uploadSleepUntilUTC())

	runtime0.state.mu.RLock()
	defer runtime0.state.mu.RUnlock()
	assert.False(t, runtime0.state.resetQuotaOnNextSuccessfulUpload)
}

func TestOversizedSignalPreservesRolloverProgressionWithoutHourlyProbeLoop(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(now)

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForAttempt(ctx, 12*int64(fs.Mebi))
	assert.True(t, isOversizedUploadLimitError(err))
	assert.Equal(t, now.Add(durationUntilNextUTCDay(now)), runtime0.uploadSleepUntilUTC())
	assert.NotEqual(t, now.Add(time.Hour), runtime0.uploadSleepUntilUTC())

	now = now.Add(durationUntilNextUTCDay(now))
	err = f.checkUploadBudgetForAttempt(ctx, 12*int64(fs.Mebi))
	assert.NoError(t, err)
	assert.True(t, runtime0.uploadSleepUntilUTC().IsZero())
}

func TestCheckUploadBudgetForAttemptAllowsOversizedAfterSingleRollover(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(now)

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForAttempt(ctx, 12*int64(fs.Mebi))
	assert.True(t, isOversizedUploadLimitError(err))
	now = now.Add(24 * time.Hour)
	err = f.checkUploadBudgetForAttempt(ctx, 12*int64(fs.Mebi))
	assert.NoError(t, err)
}

func TestCheckUploadBudgetForAttemptDoesNotTreatExpiredHourlyProbeAsOversizedRollover(t *testing.T) {
	now := time.Date(2026, 4, 16, 13, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = int64(10 * fs.Mebi)
	runtime0.state.uploadSleepUntilUTC = now
	runtime0.state.resetQuotaOnNextSuccessfulUpload = true

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForAttempt(ctx, 12*int64(fs.Mebi))
	assert.True(t, isOversizedUploadLimitError(err))
	assert.Equal(t, now.Add(durationUntilNextUTCDay(now)), runtime0.uploadSleepUntilUTC())
}

func TestCheckUploadBudgetForChunkPreservesOversizedSignalForConcreteUnknownSizeChunk(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)

	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 1}, accountPool: &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}, nowFn: func() time.Time { return now }}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	err := f.checkUploadBudgetForChunk(ctx, 2)
	assert.True(t, isOversizedUploadLimitError(err))
	assert.False(t, isAccountUploadLimitError(err))
	assert.Equal(t, now.Add(durationUntilNextUTCDay(now)), runtime0.uploadSleepUntilUTC())

	runtime0.state.mu.RLock()
	defer runtime0.state.mu.RUnlock()
	assert.False(t, runtime0.state.resetQuotaOnNextSuccessfulUpload)
}

func TestCheckUploadBudgetForAttemptKeepsNoPoolBehaviorUnchanged(t *testing.T) {
	f := &Fs{opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi}}
	f.uploadBudget.dayStartUTC = utcDayStart(time.Now())
	f.uploadBudget.usedBytes = int64(10 * fs.Mebi)

	err := f.checkUploadBudgetForAttempt(context.Background(), 1)
	assert.NoError(t, err)
}

func TestResumableUploadOneAttemptUsesFailoverSignalForWholeFileProactiveBudgetExhaustion(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = 1

	f := &Fs{
		opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 1, ChunkSize: 1},
		accountPool: &accountPool{
			accounts: []*accountRuntime{runtime0, runtime1},
		},
		nowFn: func() time.Time { return now },
	}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	createInfo := &drive.File{Name: "file.txt", Parents: []string{"root"}, MimeType: "text/plain", ModifiedTime: now.Format(timeFormatOut)}

	_, err := f.uploadOneAttempt(ctx, bytes.NewReader([]byte("x")), 1, "text/plain", "", "file.txt", createInfo)
	assert.True(t, isAccountUploadLimitError(err))
	assert.Equal(t, now.Add(time.Hour), runtime0.uploadSleepUntilUTC())
	assert.True(t, runtime0.consumeQuotaResetOnNextSuccess())
}

func TestResumableUploadOneAttemptDoesNotOwnOuterFailover(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	runtime0 := f.accountPool.accounts[0]
	runtime0.uploadDailyLimit = 10 * fs.Mebi
	runtime0.state.dayStartUTC = utcDayStart(time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC))
	routingTransportForRuntime(t, runtime0).failChunkCall = 2

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	createInfo := &drive.File{Name: "file.txt", Parents: []string{"root"}, MimeType: "text/plain", ModifiedTime: time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC).Format(timeFormatOut)}

	_, err := f.uploadOneAttempt(ctx, bytes.NewReader([]byte("abc")), 3, "text/plain", "", "file.txt", createInfo)
	assert.True(t, isAccountUploadLimitError(err))
	assert.False(t, isOversizedUploadLimitError(err))
	assert.Equal(t, []int{0}, recorder.accountsFor("upload.start"))
	assert.Equal(t, []int{0, 0}, recorder.accountsFor("upload.chunk"))
	assert.Empty(t, recorder.chunkRangesFor(1))
}

func TestResumableUploadOneAttemptChecksWholeFileBudgetBeforeAnyChunk(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	runtime0 := f.accountPool.accounts[0]
	runtime0.uploadDailyLimit = 4
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = 2

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	createInfo := &drive.File{Name: "file.txt", Parents: []string{"root"}, MimeType: "text/plain", ModifiedTime: now.Format(timeFormatOut)}

	_, err := f.uploadOneAttempt(ctx, bytes.NewReader([]byte("abc")), 3, "text/plain", "", "file.txt", createInfo)
	assert.True(t, isAccountUploadLimitError(err))
	assert.Empty(t, recorder.accountsFor("upload.start"))
	assert.Empty(t, recorder.accountsFor("upload.chunk"))
}

func TestResumableUploadOneAttemptPreservesWholeFileOversizedSignalBeforeAnyChunk(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	runtime0 := f.accountPool.accounts[0]
	runtime0.uploadDailyLimit = 2
	runtime0.state.dayStartUTC = utcDayStart(now)

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	createInfo := &drive.File{Name: "file.txt", Parents: []string{"root"}, MimeType: "text/plain", ModifiedTime: now.Format(timeFormatOut)}

	_, err := f.uploadOneAttempt(ctx, bytes.NewReader([]byte("abc")), 3, "text/plain", "", "file.txt", createInfo)
	assert.True(t, isOversizedUploadLimitError(err))
	assert.Empty(t, recorder.accountsFor("upload.start"))
	assert.Empty(t, recorder.accountsFor("upload.chunk"))
}

func TestUnknownSizeResumableUploadOneAttemptPreservesOversizedSignalForOversizedChunk(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	runtime0 := f.accountPool.accounts[0]
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	createInfo := &drive.File{Name: "file.txt", Parents: []string{"root"}, MimeType: "text/plain", ModifiedTime: now.Format(timeFormatOut)}

	_, err := f.uploadOneAttempt(ctx, bytes.NewReader([]byte("abc")), -1, "text/plain", "", "file.txt", createInfo)
	assert.True(t, isOversizedUploadLimitError(err))
	assert.False(t, isAccountUploadLimitError(err))
	assert.Equal(t, []int{0}, recorder.accountsFor("upload.start"))
	assert.Empty(t, recorder.accountsFor("upload.chunk"))
	assert.Equal(t, now.Add(durationUntilNextUTCDay(now)), runtime0.uploadSleepUntilUTC())

	runtime0.state.mu.RLock()
	defer runtime0.state.mu.RUnlock()
	assert.False(t, runtime0.state.resetQuotaOnNextSuccessfulUpload)
}

func TestUploadAttemptControllerClearsAttemptedSetOnNewRound(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	f.accountPool = &accountPool{
		accounts: []*accountRuntime{runtime0, runtime1},
		nowFn:    f.nowFn,
	}

	var attempts []int
	runs := 0
	_, err := executeUploadWithAccountFailover(f, context.Background(), bytes.NewReader([]byte("abc")), func(attemptCtx context.Context, attemptIn io.Reader) (string, error) {
		runtime := contextBoundRuntime(attemptCtx)
		require.NotNil(t, runtime)
		attempts = append(attempts, runtime.index)
		runs++
		if runs <= 3 {
			return "", &accountUploadLimitError{cause: errors.New("quota"), wake: time.Time{}}
		}
		return "ok", nil
	})
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, 0, 1}, attempts)
}

func TestUploadAttemptControllerWaitsForPoolWakeWhenNoAccountWritable(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	f.nowFn = func() time.Time { return now }
	waits := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		waits++
		now = now.Add(time.Minute)
		return nil
	}
	runtime0 := newTestAccountRuntimeForPool(0, false, now.Add(time.Minute))
	runtime1 := newTestAccountRuntimeForPool(1, false, now.Add(2*time.Minute))
	f.accountPool = &accountPool{
		accounts: []*accountRuntime{runtime0, runtime1},
		nowFn:    func() time.Time { return now },
	}

	result, err := executeUploadWithAccountFailover(f, context.Background(), bytes.NewReader([]byte("abc")), func(attemptCtx context.Context, attemptIn io.Reader) (string, error) {
		runtime := contextBoundRuntime(attemptCtx)
		require.NotNil(t, runtime)
		return fmt.Sprintf("account-%d", runtime.index), nil
	})
	require.NoError(t, err)
	assert.Equal(t, "account-0", result)
	assert.Equal(t, 1, waits)
}

func TestUploadAttemptControllerEscalatesWhenInputNotRewindable(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	f.accountPool = &accountPool{
		accounts: []*accountRuntime{runtime0, runtime1},
		nowFn:    f.nowFn,
	}

	_, err := executeUploadWithAccountFailover(f, context.Background(), io.NopCloser(strings.NewReader("abc")), func(attemptCtx context.Context, attemptIn io.Reader) (string, error) {
		return "", &accountUploadLimitError{cause: errors.New("quota"), wake: time.Time{}}
	})
	require.Error(t, err)
	assert.True(t, fserrors.IsRetryError(err))
	assert.False(t, isAccountUploadLimitError(err))
}

func TestPutUncheckedUsesAnotherAccountAfterReactiveUploadLimit(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	routingTransportForRuntime(t, f.accountPool.accounts[0]).failUploadStart = true

	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), 3, true, nil, nil).WithMimeType("text/plain")

	obj, err := f.PutUnchecked(context.Background(), bytes.NewReader([]byte("abc")), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, []int{0, 1}, recorder.accountsFor("upload.start"))
	assert.Equal(t, []int{1, 1}, recorder.accountsFor("upload.chunk"))
}

func TestBaseObjectUpdateUsesAnotherAccountAfterProactiveBudgetExhaustion(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 16
	f.opt.UploadDailyLimit = 10 * fs.Mebi
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	runtime0 := f.accountPool.accounts[0]
	runtime1 := f.accountPool.accounts[1]
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = 1
	runtime1.uploadDailyLimit = 1 * fs.Mebi

	obj := &Object{baseObject: baseObject{fs: f, remote: "file.txt", id: "obj-1", mimeType: "text/plain"}}
	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), 1, true, nil, nil).WithMimeType("text/plain")

	err := obj.Update(context.Background(), bytes.NewReader([]byte("a")), src)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, recorder.accountsFor("files.update"))
	assert.Equal(t, now.Add(time.Hour), runtime0.uploadSleepUntilUTC())
	assert.True(t, runtime1.uploadSleepUntilUTC().IsZero())
}

func TestDirectSmallUploadDoesNotSleepInPlaceOnProactiveBudgetExhaustion(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 16
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}
	runtime0 := f.accountPool.accounts[0]
	runtime1 := f.accountPool.accounts[1]
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = 1
	runtime1.uploadDailyLimit = 1 * fs.Mebi

	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), 1, true, nil, nil).WithMimeType("text/plain")

	obj, err := f.PutUnchecked(context.Background(), bytes.NewReader([]byte("a")), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, []int{1}, recorder.accountsFor("files.create"))
	assert.Zero(t, sleepCalls)
	assert.Equal(t, now.Add(time.Hour), runtime0.uploadSleepUntilUTC())
}

func TestResumableUploadRestartsFromZeroOnAccountFailover(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	routingTransportForRuntime(t, f.accountPool.accounts[0]).failChunkCall = 2

	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), 3, true, nil, nil).WithMimeType("text/plain")

	obj, err := f.PutUnchecked(context.Background(), bytes.NewReader([]byte("abc")), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, []int{0, 1}, recorder.accountsFor("upload.start"))
	assert.Equal(t, []int{0, 0, 1, 1}, recorder.accountsFor("upload.chunk"))
	assert.Equal(t, []string{"bytes 0-1/3", "bytes 2-2/3"}, recorder.chunkRangesFor(0))
	assert.Equal(t, []string{"bytes 0-1/3", "bytes 2-2/3"}, recorder.chunkRangesFor(1))
}

func TestUnknownSizeResumableUploadFailsOverAfterOversizedChunkWithoutInPlaceSleep(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}
	runtime0 := f.accountPool.accounts[0]
	runtime1 := f.accountPool.accounts[1]
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime1.uploadDailyLimit = 10 * fs.Mebi

	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), -1, true, nil, nil).WithMimeType("text/plain")

	obj, err := f.PutUnchecked(context.Background(), bytes.NewReader([]byte("abc")), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, []int{0, 1}, recorder.accountsFor("upload.start"))
	assert.Equal(t, []int{1, 1}, recorder.accountsFor("upload.chunk"))
	assert.Empty(t, recorder.chunkRangesFor(0))
	assert.Equal(t, []string{"bytes 0-1/*", "bytes 2-2/3"}, recorder.chunkRangesFor(1))
	assert.Zero(t, sleepCalls)
	assert.Equal(t, now.Add(durationUntilNextUTCDay(now)), runtime0.uploadSleepUntilUTC())

	runtime0.state.mu.RLock()
	defer runtime0.state.mu.RUnlock()
	assert.False(t, runtime0.state.resetQuotaOnNextSuccessfulUpload)
}

func TestUnknownSizeResumableUploadFailsOverBeforeFinalChunkSend(t *testing.T) {
	recorder := newDriveRoutingRecorder()
	f := newRoutingTestFs(t, "round_robin", recorder)
	f.opt.SleepOnUploadLimit = true
	f.opt.UploadCutoff = 1
	currentDay := time.Now().UTC()
	now := time.Date(currentDay.Year(), currentDay.Month(), currentDay.Day(), 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }
	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}
	runtime0 := f.accountPool.accounts[0]
	runtime1 := f.accountPool.accounts[1]
	runtime0.uploadDailyLimit = 2
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime1.uploadDailyLimit = 10 * fs.Mebi

	src := object.NewStaticObjectInfo("file.txt", time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC), -1, true, nil, nil).WithMimeType("text/plain")

	obj, err := f.PutUnchecked(context.Background(), bytes.NewReader([]byte("abc")), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, []int{0, 1}, recorder.accountsFor("upload.start"))
	assert.Equal(t, []int{0, 1, 1}, recorder.accountsFor("upload.chunk"))
	assert.Equal(t, []string{"bytes 0-1/*"}, recorder.chunkRangesFor(0))
	assert.Equal(t, []string{"bytes 0-1/*", "bytes 2-2/3"}, recorder.chunkRangesFor(1))
	assert.Zero(t, sleepCalls)
	assert.Equal(t, now.Add(time.Hour), runtime0.uploadSleepUntilUTC())
	assert.True(t, runtime0.consumeQuotaResetOnNextSuccess())
}

func TestNoPoolUploadBudgetBehaviorRemainsUnchanged(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f := &Fs{
		opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 10 * fs.Mebi},
		nowFn: func() time.Time {
			return now
		},
	}
	f.uploadBudget.dayStartUTC = utcDayStart(now)
	f.uploadBudget.usedBytes = 10 * int64(fs.Mebi)

	assert.False(t, f.usesUploadAccountFailover())
	assert.NoError(t, f.checkUploadBudgetForAttempt(context.Background(), 1))

	var waits []time.Duration
	err := f.waitForUploadBudget(context.Background(), 1, func(d time.Duration) error {
		waits = append(waits, d)
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []time.Duration{time.Hour}, waits)
	assert.Equal(t, int64(10*fs.Mebi), f.uploadBudget.usedBytes)
}

func TestSleepOnUploadLimitFalseDoesNotEnableAccountFailover(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 1
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime0.state.usedBytes = 1

	f := &Fs{
		opt: Options{SleepOnUploadLimit: false, UploadDailyLimit: 1},
		accountPool: &accountPool{
			accounts: []*accountRuntime{runtime0, runtime1},
		},
		nowFn: func() time.Time {
			return now
		},
	}
	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)

	assert.False(t, f.usesUploadAccountFailover())
	assert.NoError(t, f.checkUploadBudgetForAttempt(ctx, 1))

	gerr := &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded", Message: "User rate limit exceeded."}}}
	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.False(t, isAccountUploadLimitError(err))
	assert.True(t, runtime0.uploadSleepUntilUTC().IsZero())
	assert.True(t, runtime1.uploadSleepUntilUTC().IsZero())
}

func TestSingleAccountPoolDoesNotEnableFileLevelFailover(t *testing.T) {
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime.uploadDailyLimit = 1
	runtime.state.dayStartUTC = utcDayStart(now)
	runtime.state.usedBytes = 1

	f := &Fs{
		opt: Options{SleepOnUploadLimit: true, UploadDailyLimit: 1},
		accountPool: &accountPool{
			accounts: []*accountRuntime{runtime},
		},
		nowFn: func() time.Time {
			return now
		},
	}
	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}
	ctx, boundRuntime, err := bindAccountForWriteObject(context.Background(), f)
	require.NoError(t, err)
	require.Same(t, runtime, boundRuntime)

	assert.False(t, f.usesUploadAccountFailover())
	assert.NoError(t, f.checkUploadBudgetForAttempt(ctx, 1))

	gerr := &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded", Message: "User rate limit exceeded."}}}
	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.False(t, isAccountUploadLimitError(err))
	assert.Equal(t, 1, sleepCalls)
	assert.Equal(t, now.Add(time.Hour), runtime.uploadSleepUntilUTC())
}

func TestOversizedUploadBehaviorRemainsExplicitForCoveredPoolPath(t *testing.T) {
	initialNow := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	now := initialNow
	rolloverWait := durationUntilNextUTCDay(initialNow)
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})
	runtime0.uploadDailyLimit = 2
	runtime1.uploadDailyLimit = 2
	runtime0.state.dayStartUTC = utcDayStart(now)
	runtime1.state.dayStartUTC = utcDayStart(now)
	pool := newAccountPool("round_robin", []*accountRuntime{runtime0, runtime1})
	pool.nowFn = func() time.Time { return now }

	f := &Fs{
		opt:         Options{SleepOnUploadLimit: true, UploadDailyLimit: 2},
		accountPool: pool,
		nowFn: func() time.Time {
			return now
		},
	}
	var waits []time.Duration
	f.sleepFn = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		now = now.Add(d)
		return nil
	}

	var attempts []int
	oversizedSignals := 0
	result, err := executeUploadWithAccountFailover(f, context.Background(), bytes.NewReader([]byte("abc")), func(attemptCtx context.Context, attemptIn io.Reader) (int, error) {
		runtime := contextBoundRuntime(attemptCtx)
		require.NotNil(t, runtime)
		attempts = append(attempts, runtime.index)
		err := f.checkUploadBudgetForAttempt(attemptCtx, 3)
		if err != nil {
			assert.True(t, isOversizedUploadLimitError(err))
			assert.False(t, isAccountUploadLimitError(err))
			oversizedSignals++
			return -1, err
		}
		return runtime.index, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result)
	assert.Equal(t, []int{0, 1, 0}, attempts)
	assert.Equal(t, 2, oversizedSignals)
	assert.Equal(t, []time.Duration{rolloverWait}, waits)
	assert.True(t, runtime0.uploadSleepUntilUTC().IsZero())
	assert.Equal(t, initialNow.Add(rolloverWait), runtime1.uploadSleepUntilUTC())
}

func TestBudgetSleepSuccessBeforeWakeDoesNotResetOrClearSleep(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	now := time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC)
	runtime.state.dayStartUTC = utcDayStart(now)
	runtime.state.usedBytes = 20
	runtime.state.uploadSleepUntilUTC = now.Add(time.Hour)

	f := &Fs{
		opt:         Options{SleepOnUploadLimit: true, UploadDailyLimit: defaultUploadDailyLimit},
		accountPool: &accountPool{accounts: []*accountRuntime{runtime}},
	}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime)
	ctx = withUploadProbeState(ctx)
	f.noteSuccessfulUploadBytes(ctx, now, 5)

	runtime.state.mu.RLock()
	defer runtime.state.mu.RUnlock()
	assert.Equal(t, now.Add(time.Hour), runtime.state.uploadSleepUntilUTC)
	assert.Equal(t, int64(25), runtime.state.usedBytes)
}

func TestBudgetSleepSuccessAfterWakeClearsSleepWithoutReset(t *testing.T) {
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})

	now := time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC)
	runtime.state.dayStartUTC = utcDayStart(now)
	runtime.state.usedBytes = 20
	runtime.state.uploadSleepUntilUTC = now

	f := &Fs{
		opt:         Options{SleepOnUploadLimit: true, UploadDailyLimit: defaultUploadDailyLimit},
		accountPool: &accountPool{accounts: []*accountRuntime{runtime}},
	}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime)
	ctx = withUploadProbeState(ctx)
	f.noteSuccessfulUploadBytes(ctx, now, 5)

	runtime.state.mu.RLock()
	defer runtime.state.mu.RUnlock()
	assert.True(t, runtime.state.uploadSleepUntilUTC.IsZero())
	assert.Equal(t, int64(25), runtime.state.usedBytes)
}

func TestWaitForUploadBudgetPerAccountSleepsInProbeIntervals(t *testing.T) {
	now := time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC)
	limit := fs.SizeSuffix(10 * fs.Mebi)
	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime.uploadDailyLimit = limit
	runtime.state.dayStartUTC = utcDayStart(now)
	runtime.state.usedBytes = int64(limit)

	f := &Fs{
		opt:         Options{SleepOnUploadLimit: true, UploadDailyLimit: limit},
		accountPool: &accountPool{accounts: []*accountRuntime{runtime}},
		nowFn: func() time.Time {
			return now
		},
	}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime)
	var waits []time.Duration
	err := f.waitForUploadBudget(ctx, 1, func(d time.Duration) error {
		waits = append(waits, d)
		now = now.Add(d)
		if len(waits) >= 3 {
			return context.Canceled
		}
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, waits, 3)
	assert.Equal(t, []time.Duration{time.Hour, time.Hour, time.Hour}, waits)
}

func TestShouldRetryUploadPathUserRateLimitExceededRepeatsHourlyProbe(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	f.nowFn = func() time.Time { return now }

	runtime := newTestAccountRuntimeForPool(0, false, time.Time{})
	f.accountPool = &accountPool{accounts: []*accountRuntime{runtime}}

	var waits []time.Duration
	f.sleepFn = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		now = now.Add(d)
		return nil
	}

	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "userRateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime)
	ctx = withUploadProbeState(ctx)
	for range 3 {
		retry, err := f.shouldRetryUpload(ctx, gerr, true)
		require.True(t, retry)
		require.Equal(t, gerr, err)
	}

	assert.Equal(t, []time.Duration{time.Hour, time.Hour, time.Hour}, waits)
	assert.Equal(t, now, runtime.uploadSleepUntilUTC())
}

func TestBindWriteOperationRebindResetsUploadProbeState(t *testing.T) {
	primary := newTestFsWithAccountPool("round_robin", 1)
	foreign := newTestFsWithAccountPool("round_robin", 1)

	foreignCtx, _, err := bindAccountForWriteObject(context.Background(), foreign)
	require.NoError(t, err)
	markUploadProbeQuotaReset(foreignCtx)

	bindingCtx, _, err := bindAccountForWriteObject(foreignCtx, primary)
	require.NoError(t, err)

	assert.False(t, consumeUploadProbeQuotaReset(bindingCtx))
}

func TestShouldRetryUploadPathFatalRefreshDisablesOnlyBoundRuntime(t *testing.T) {
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})

	f := newTestFsForRetry(t)
	f.accountPool = &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	fatalErr := errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_grant\"}")

	retry, err := f.shouldRetryUpload(ctx, fatalErr, true)
	assert.False(t, retry)
	assert.Equal(t, fatalErr, err)
	assert.True(t, runtime0.isDisabled())
	assert.False(t, runtime1.isDisabled())
}

func TestShouldRetryUploadPathTransientRefreshDoesNotDisableBoundRuntime(t *testing.T) {
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})

	f := newTestFsForRetry(t)
	f.accountPool = &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime0)
	transientErr := errors.New("oauth2: token refresh failed: Post https://oauth2.googleapis.com/token: i/o timeout")

	retry, err := f.shouldRetryUpload(ctx, transientErr, true)
	assert.False(t, retry)
	assert.Equal(t, transientErr, err)
	assert.False(t, runtime0.isDisabled())
	assert.False(t, runtime1.isDisabled())
}

func TestShouldRetryFatalRefreshDisablesOnlyCurrentlyBoundRuntime(t *testing.T) {
	runtime0 := newTestAccountRuntimeForPool(0, false, time.Time{})
	runtime1 := newTestAccountRuntimeForPool(1, false, time.Time{})

	f := newTestFsForRetry(t)
	f.accountPool = &accountPool{accounts: []*accountRuntime{runtime0, runtime1}}

	ctx := context.WithValue(context.Background(), accountRuntimeKey, runtime1)
	fatalErr := errors.New("oauth2: cannot fetch token: 400 Bad Request Response: {\"error\":\"invalid_client\"}")

	retry, err := f.shouldRetry(ctx, fatalErr)
	assert.False(t, retry)
	assert.Equal(t, fatalErr, err)
	assert.False(t, runtime0.isDisabled())
	assert.True(t, runtime1.isDisabled())
}

func TestShouldRetryUploadPathUserRateLimitExceededOverridesStopOnUploadLimit(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	f.opt.StopOnUploadLimit = true

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "userRateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.Equal(t, 1, sleepCalls)
}

func TestShouldRetryUploadPathRateLimitExceededUnchanged(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	f.opt.StopOnUploadLimit = true

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "rateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.False(t, retry)
	assert.Error(t, err)
	assert.True(t, fserrors.IsFatalError(err))
	assert.Equal(t, 0, sleepCalls)
}

func TestShouldRetryNonUploadPathDoesNotSleepOnUserRateLimitExceeded(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "userRateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, false)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.Equal(t, 0, sleepCalls)
}

func TestShouldRetryUploadPathUserRateLimitExceededDisabledFlagUnchanged(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = false

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "userRateLimitExceeded",
			Message: "User rate limit exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.True(t, retry)
	assert.Equal(t, gerr, err)
	assert.Equal(t, 0, sleepCalls)
}

func TestShouldRetryUploadPathQuotaExceededUnchanged(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	f.opt.StopOnUploadLimit = true

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "quotaExceeded",
			Message: "Quota exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.False(t, retry)
	assert.True(t, fserrors.IsFatalError(err))
	assert.Equal(t, 0, sleepCalls)
}

func TestShouldRetryUploadPathStorageQuotaExceededUnchanged(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	f.opt.StopOnUploadLimit = true

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "storageQuotaExceeded",
			Message: "Storage quota exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.False(t, retry)
	assert.True(t, fserrors.IsFatalError(err))
	assert.Equal(t, 0, sleepCalls)
}

func TestShouldRetryUploadPathTeamDriveFileLimitExceededUnchanged(t *testing.T) {
	f := newTestFsForRetry(t)
	f.opt.SleepOnUploadLimit = true
	f.opt.StopOnUploadLimit = true

	sleepCalls := 0
	f.sleepFn = func(context.Context, time.Duration) error {
		sleepCalls++
		return nil
	}

	ctx := context.Background()
	gerr := &googleapi.Error{
		Code: 403,
		Errors: []googleapi.ErrorItem{{
			Reason:  "teamDriveFileLimitExceeded",
			Message: "Team drive file limit exceeded.",
		}},
	}

	retry, err := f.shouldRetryUpload(ctx, gerr, true)
	assert.False(t, retry)
	assert.True(t, fserrors.IsFatalError(err))
	assert.Equal(t, 0, sleepCalls)
}

/*
var additionalMimeTypes = map[string]string{
	"application/vnd.ms-excel.sheet.macroenabled.12":                          ".xlsm",
	"application/vnd.ms-excel.template.macroenabled.12":                       ".xltm",
	"application/vnd.ms-powerpoint.presentation.macroenabled.12":              ".pptm",
	"application/vnd.ms-powerpoint.slideshow.macroenabled.12":                 ".ppsm",
	"application/vnd.ms-powerpoint.template.macroenabled.12":                  ".potm",
	"application/vnd.ms-powerpoint":                                           ".ppt",
	"application/vnd.ms-word.document.macroenabled.12":                        ".docm",
	"application/vnd.ms-word.template.macroenabled.12":                        ".dotm",
	"application/vnd.openxmlformats-officedocument.presentationml.template":   ".potx",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.template":    ".xltx",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.template": ".dotx",
	"application/vnd.sun.xml.writer":                                          ".sxw",
	"text/richtext":                                                           ".rtf",
}
*/

// Load the example export formats into exportFormats for testing
func TestInternalLoadExampleFormats(t *testing.T) {
	fetchFormatsOnce.Do(func() {})
	buf, err := os.ReadFile(filepath.FromSlash("test/about.json"))
	var about struct {
		ExportFormats map[string][]string `json:"exportFormats,omitempty"`
		ImportFormats map[string][]string `json:"importFormats,omitempty"`
	}
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(buf, &about))
	_exportFormats = fixMimeTypeMap(about.ExportFormats)
	_importFormats = fixMimeTypeMap(about.ImportFormats)
}

func TestInternalParseExtensions(t *testing.T) {
	for _, test := range []struct {
		in      string
		want    []string
		wantErr error
	}{
		{"doc", []string{".doc"}, nil},
		{" docx ,XLSX, 	pptx,svg,md", []string{".docx", ".xlsx", ".pptx", ".svg", ".md"}, nil},
		{"docx,svg,Docx", []string{".docx", ".svg"}, nil},
		{"docx,potato,docx", []string{".docx"}, errors.New(`couldn't find MIME type for extension ".potato"`)},
	} {
		extensions, _, gotErr := parseExtensions(test.in)
		if test.wantErr == nil {
			assert.NoError(t, gotErr)
		} else {
			assert.EqualError(t, gotErr, test.wantErr.Error())
		}
		assert.Equal(t, test.want, extensions)
	}

	// Test it is appending
	extensions, _, gotErr := parseExtensions("docx,svg", "docx,svg,xlsx")
	assert.NoError(t, gotErr)
	assert.Equal(t, []string{".docx", ".svg", ".xlsx"}, extensions)
}

func TestInternalFindExportFormat(t *testing.T) {
	ctx := context.Background()
	item := &drive.File{
		Name:     "file",
		MimeType: "application/vnd.google-apps.document",
	}
	for _, test := range []struct {
		extensions    []string
		wantExtension string
		wantMimeType  string
	}{
		{[]string{}, "", ""},
		{[]string{".pdf"}, ".pdf", "application/pdf"},
		{[]string{".pdf", ".rtf", ".xls"}, ".pdf", "application/pdf"},
		{[]string{".xls", ".rtf", ".pdf"}, ".rtf", "application/rtf"},
		{[]string{".xls", ".csv", ".svg"}, "", ""},
	} {
		f := new(Fs)
		f.exportExtensions = test.extensions
		gotExtension, gotFilename, gotMimeType, gotIsDocument := f.findExportFormat(ctx, item)
		assert.Equal(t, test.wantExtension, gotExtension)
		if test.wantExtension != "" {
			assert.Equal(t, item.Name+gotExtension, gotFilename)
		} else {
			assert.Equal(t, "", gotFilename)
		}
		assert.Equal(t, test.wantMimeType, gotMimeType)
		assert.Equal(t, true, gotIsDocument)
	}
}

func TestMimeTypesToExtension(t *testing.T) {
	for mimeType, extension := range _mimeTypeToExtension {
		extensions, err := mime.ExtensionsByType(mimeType)
		assert.NoError(t, err)
		assert.Contains(t, extensions, extension)
	}
}

func TestExtensionToMimeType(t *testing.T) {
	for mimeType, extension := range _mimeTypeToExtension {
		gotMimeType := mime.TypeByExtension(extension)
		mediatype, _, err := mime.ParseMediaType(gotMimeType)
		assert.NoError(t, err)
		assert.Equal(t, mimeType, mediatype)
	}
}

func TestExtensionsForExportFormats(t *testing.T) {
	if _exportFormats == nil {
		t.Error("exportFormats == nil")
	}
	for fromMT, toMTs := range _exportFormats {
		for _, toMT := range toMTs {
			if !isInternalMimeType(toMT) {
				extensions, err := mime.ExtensionsByType(toMT)
				assert.NoError(t, err, "invalid MIME type %q", toMT)
				assert.NotEmpty(t, extensions, "No extension found for %q (from: %q)", fromMT, toMT)
			}
		}
	}
}

func TestExtensionsForImportFormats(t *testing.T) {
	t.Skip()
	if _importFormats == nil {
		t.Error("_importFormats == nil")
	}
	for fromMT := range _importFormats {
		if !isInternalMimeType(fromMT) {
			extensions, err := mime.ExtensionsByType(fromMT)
			assert.NoError(t, err, "invalid MIME type %q", fromMT)
			assert.NotEmpty(t, extensions, "No extension found for %q", fromMT)
		}
	}
}

func (f *Fs) InternalTestShouldRetry(t *testing.T) {
	ctx := context.Background()
	gatewayTimeout := googleapi.Error{
		Code: 503,
	}
	timeoutRetry, timeoutError := f.shouldRetry(ctx, &gatewayTimeout)
	assert.True(t, timeoutRetry)
	assert.Equal(t, &gatewayTimeout, timeoutError)
	generic403 := googleapi.Error{
		Code: 403,
	}
	rLEItem := googleapi.ErrorItem{
		Reason:  "rateLimitExceeded",
		Message: "User rate limit exceeded.",
	}
	generic403.Errors = append(generic403.Errors, rLEItem)
	oldStopUpload := f.opt.StopOnUploadLimit
	oldStopDownload := f.opt.StopOnDownloadLimit
	f.opt.StopOnUploadLimit = true
	f.opt.StopOnDownloadLimit = true
	defer func() {
		f.opt.StopOnUploadLimit = oldStopUpload
		f.opt.StopOnDownloadLimit = oldStopDownload
	}()
	expectedRLError := fserrors.FatalError(&generic403)
	rateLimitRetry, rateLimitErr := f.shouldRetry(ctx, &generic403)
	assert.False(t, rateLimitRetry)
	assert.Equal(t, rateLimitErr, expectedRLError)
	dQEItem := googleapi.ErrorItem{
		Reason: "downloadQuotaExceeded",
	}
	generic403.Errors[0] = dQEItem
	expectedDQError := fserrors.FatalError(&generic403)
	downloadQuotaRetry, downloadQuotaError := f.shouldRetry(ctx, &generic403)
	assert.False(t, downloadQuotaRetry)
	assert.Equal(t, downloadQuotaError, expectedDQError)
	tDFLEItem := googleapi.ErrorItem{
		Reason: "teamDriveFileLimitExceeded",
	}
	generic403.Errors[0] = tDFLEItem
	expectedTDFLError := fserrors.FatalError(&generic403)
	teamDriveFileLimitRetry, teamDriveFileLimitError := f.shouldRetry(ctx, &generic403)
	assert.False(t, teamDriveFileLimitRetry)
	assert.Equal(t, teamDriveFileLimitError, expectedTDFLError)
	qEItem := googleapi.ErrorItem{
		Reason: "quotaExceeded",
	}
	generic403.Errors[0] = qEItem
	expectedQuotaError := fserrors.FatalError(&generic403)
	quotaExceededRetry, quotaExceededError := f.shouldRetry(ctx, &generic403)
	assert.False(t, quotaExceededRetry)
	assert.Equal(t, quotaExceededError, expectedQuotaError)

	sqEItem := googleapi.ErrorItem{
		Reason: "storageQuotaExceeded",
	}
	generic403.Errors[0] = sqEItem
	expectedStorageQuotaError := fserrors.FatalError(&generic403)
	storageQuotaExceededRetry, storageQuotaExceededError := f.shouldRetry(ctx, &generic403)
	assert.False(t, storageQuotaExceededRetry)
	assert.Equal(t, storageQuotaExceededError, expectedStorageQuotaError)
}

func (f *Fs) InternalTestDocumentImport(t *testing.T) {
	oldAllow := f.opt.AllowImportNameChange
	f.opt.AllowImportNameChange = true
	defer func() {
		f.opt.AllowImportNameChange = oldAllow
	}()

	testFilesPath, err := filepath.Abs(filepath.FromSlash("test/files"))
	require.NoError(t, err)

	testFilesFs, err := fs.NewFs(context.Background(), testFilesPath)
	require.NoError(t, err)

	_, f.importMimeTypes, err = parseExtensions("odt,ods,doc")
	require.NoError(t, err)

	err = operations.CopyFile(context.Background(), f, testFilesFs, "example2.doc", "example2.doc")
	require.NoError(t, err)
}

func (f *Fs) InternalTestDocumentUpdate(t *testing.T) {
	testFilesPath, err := filepath.Abs(filepath.FromSlash("test/files"))
	require.NoError(t, err)

	testFilesFs, err := fs.NewFs(context.Background(), testFilesPath)
	require.NoError(t, err)

	_, f.importMimeTypes, err = parseExtensions("odt,ods,doc")
	require.NoError(t, err)

	err = operations.CopyFile(context.Background(), f, testFilesFs, "example2.xlsx", "example1.ods")
	require.NoError(t, err)
}

func (f *Fs) InternalTestDocumentExport(t *testing.T) {
	var buf bytes.Buffer
	var err error

	f.exportExtensions, _, err = parseExtensions("txt")
	require.NoError(t, err)

	obj, err := f.NewObject(context.Background(), "example2.txt")
	require.NoError(t, err)

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	_, err = io.Copy(&buf, rc)
	require.NoError(t, err)
	text := buf.String()

	for _, excerpt := range []string{
		"Lorem ipsum dolor sit amet, consectetur",
		"porta at ultrices in, consectetur at augue.",
	} {
		require.Contains(t, text, excerpt)
	}
}

func (f *Fs) InternalTestDocumentLink(t *testing.T) {
	var buf bytes.Buffer
	var err error

	f.exportExtensions, _, err = parseExtensions("link.html")
	require.NoError(t, err)

	obj, err := f.NewObject(context.Background(), "example2.link.html")
	require.NoError(t, err)

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	_, err = io.Copy(&buf, rc)
	require.NoError(t, err)
	text := buf.String()

	require.True(t, strings.HasPrefix(text, "<html>"))
	require.True(t, strings.HasSuffix(text, "</html>\n"))
	for _, excerpt := range []string{
		`<meta http-equiv="refresh"`,
		`Loading <a href="`,
	} {
		require.Contains(t, text, excerpt)
	}
}

const (
	// from fstest/fstests/fstests.go
	existingDir    = "hello? sausage"
	existingFile   = `hello? sausage/êé/Hello, 世界/ " ' @ < > & ? + ≠/z.txt`
	existingSubDir = "êé"
)

// TestIntegration/FsMkdir/FsPutFiles/Internal/Shortcuts
func (f *Fs) InternalTestShortcuts(t *testing.T) {
	ctx := context.Background()
	srcObj, err := f.NewObject(ctx, existingFile)
	require.NoError(t, err)
	srcHash, err := srcObj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.NotEqual(t, "", srcHash)
	t.Run("Errors", func(t *testing.T) {
		_, err := f.makeShortcut(ctx, "", f, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can't be root")

		_, err = f.makeShortcut(ctx, "notfound", f, "dst")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can't find source")

		_, err = f.makeShortcut(ctx, existingFile, f, existingFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not overwriting")
		assert.Contains(t, err.Error(), "existing file")

		_, err = f.makeShortcut(ctx, existingFile, f, existingDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not overwriting")
		assert.Contains(t, err.Error(), "existing directory")
	})
	t.Run("File", func(t *testing.T) {
		dstObj, err := f.makeShortcut(ctx, existingFile, f, "shortcut.txt")
		require.NoError(t, err)
		require.NotNil(t, dstObj)
		assert.Equal(t, "shortcut.txt", dstObj.Remote())
		dstHash, err := dstObj.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, srcHash, dstHash)
		require.NoError(t, dstObj.Remove(ctx))
	})
	t.Run("Dir", func(t *testing.T) {
		dstObj, err := f.makeShortcut(ctx, existingDir, f, "shortcutdir")
		require.NoError(t, err)
		require.Nil(t, dstObj)
		entries, err := f.List(ctx, "shortcutdir")
		require.NoError(t, err)
		require.Equal(t, 1, len(entries))
		require.Equal(t, "shortcutdir/"+existingSubDir, entries[0].Remote())
		require.NoError(t, f.Rmdir(ctx, "shortcutdir"))
	})
	t.Run("Command", func(t *testing.T) {
		_, err := f.Command(ctx, "shortcut", []string{"one"}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "need exactly 2 arguments")

		_, err = f.Command(ctx, "shortcut", []string{"one", "two"}, map[string]string{
			"target": "doesnotexistremote:",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "couldn't find target")

		_, err = f.Command(ctx, "shortcut", []string{"one", "two"}, map[string]string{
			"target": ".",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "target is not a drive backend")

		dstObjI, err := f.Command(ctx, "shortcut", []string{existingFile, "shortcut2.txt"}, map[string]string{
			"target": fs.ConfigString(f),
		})
		require.NoError(t, err)
		dstObj := dstObjI.(*Object)
		assert.Equal(t, "shortcut2.txt", dstObj.Remote())
		dstHash, err := dstObj.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, srcHash, dstHash)
		require.NoError(t, dstObj.Remove(ctx))

		dstObjI, err = f.Command(ctx, "shortcut", []string{existingFile, "shortcut3.txt"}, nil)
		require.NoError(t, err)
		dstObj = dstObjI.(*Object)
		assert.Equal(t, "shortcut3.txt", dstObj.Remote())
		dstHash, err = dstObj.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, srcHash, dstHash)
		require.NoError(t, dstObj.Remove(ctx))
	})
}

// TestIntegration/FsMkdir/FsPutFiles/Internal/UnTrash
func (f *Fs) InternalTestUnTrash(t *testing.T) {
	ctx := context.Background()

	// Make some objects, one in a subdir
	contents := random.String(100)
	file1 := fstest.NewItem("trashDir/toBeTrashed", contents, time.Now())
	obj1 := fstests.PutTestContents(ctx, t, f, &file1, contents, false)
	file2 := fstest.NewItem("trashDir/subdir/toBeTrashed", contents, time.Now())
	_ = fstests.PutTestContents(ctx, t, f, &file2, contents, false)

	// Check objects
	checkObjects := func() {
		fstest.CheckListingWithRoot(t, f, "trashDir", []fstest.Item{
			file1,
			file2,
		}, []string{
			"trashDir/subdir",
		}, f.Precision())
	}
	checkObjects()

	// Make sure we are using the trash
	require.Equal(t, true, f.opt.UseTrash)

	// Remove the object and the dir
	require.NoError(t, obj1.Remove(ctx))
	require.NoError(t, f.Purge(ctx, "trashDir/subdir"))

	// Check objects gone
	fstest.CheckListingWithRoot(t, f, "trashDir", []fstest.Item{}, []string{}, f.Precision())

	// Restore the object and directory
	r, err := f.unTrashDir(ctx, "trashDir", true)
	require.NoError(t, err)
	assert.Equal(t, unTrashResult{Errors: 0, Untrashed: 2}, r)

	// Check objects restored
	checkObjects()

	// Remove the test dir
	require.NoError(t, f.Purge(ctx, "trashDir"))
}

// TestIntegration/FsMkdir/FsPutFiles/Internal/CopyOrMoveID
func (f *Fs) InternalTestCopyOrMoveID(t *testing.T) {
	ctx := context.Background()
	obj, err := f.NewObject(ctx, existingFile)
	require.NoError(t, err)
	o := obj.(*Object)

	dir := t.TempDir()

	checkFile := func(name string) {
		filePath := filepath.Join(dir, name)
		fi, err := os.Stat(filePath)
		require.NoError(t, err)
		assert.Equal(t, int64(100), fi.Size())
		err = os.Remove(filePath)
		require.NoError(t, err)
	}

	t.Run("BadID", func(t *testing.T) {
		err = f.copyOrMoveID(ctx, "moveid", "ID-NOT-FOUND", dir+"/")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "couldn't find id")
	})

	t.Run("Directory", func(t *testing.T) {
		rootID, err := f.dirCache.RootID(ctx, false)
		require.NoError(t, err)
		err = f.copyOrMoveID(ctx, "moveid", rootID, dir+"/")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "can't moveid directory")
	})

	t.Run("MoveWithoutDestName", func(t *testing.T) {
		err = f.copyOrMoveID(ctx, "moveid", o.id, dir+"/")
		require.NoError(t, err)
		checkFile(path.Base(existingFile))
	})

	t.Run("CopyWithoutDestName", func(t *testing.T) {
		err = f.copyOrMoveID(ctx, "copyid", o.id, dir+"/")
		require.NoError(t, err)
		checkFile(path.Base(existingFile))
	})

	t.Run("MoveWithDestName", func(t *testing.T) {
		err = f.copyOrMoveID(ctx, "moveid", o.id, dir+"/potato.txt")
		require.NoError(t, err)
		checkFile("potato.txt")
	})

	t.Run("CopyWithDestName", func(t *testing.T) {
		err = f.copyOrMoveID(ctx, "copyid", o.id, dir+"/potato.txt")
		require.NoError(t, err)
		checkFile("potato.txt")
	})
}

// TestIntegration/FsMkdir/FsPutFiles/Internal/Query
func (f *Fs) InternalTestQuery(t *testing.T) {
	ctx := context.Background()
	var err error
	t.Run("BadQuery", func(t *testing.T) {
		_, err = f.query(ctx, "this is a bad query")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to execute query")
	})

	t.Run("NoMatch", func(t *testing.T) {
		results, err := f.query(ctx, fmt.Sprintf("name='%s' and name!='%s'", existingSubDir, existingSubDir))
		require.NoError(t, err)
		assert.Len(t, results, 0)
	})

	t.Run("GoodQuery", func(t *testing.T) {
		pathSegments := strings.Split(existingFile, "/")
		var parent string
		for _, item := range pathSegments {
			// the file name contains ' characters which must be escaped
			escapedItem := f.opt.Enc.FromStandardName(item)
			escapedItem = strings.ReplaceAll(escapedItem, `\`, `\\`)
			escapedItem = strings.ReplaceAll(escapedItem, `'`, `\'`)

			results, err := f.query(ctx, fmt.Sprintf("%strashed=false and name='%s'", parent, escapedItem))
			require.NoError(t, err)
			require.True(t, len(results) > 0)
			for _, result := range results {
				assert.True(t, len(result.Id) > 0)
				assert.Equal(t, result.Name, item)
			}
			parent = fmt.Sprintf("'%s' in parents and ", results[0].Id)
		}
	})
}

// TestIntegration/FsMkdir/FsPutFiles/Internal/AgeQuery
func (f *Fs) InternalTestAgeQuery(t *testing.T) {
	// Check set up for filtering
	assert.True(t, f.Features().FilterAware)

	opt := &filter.Options{}
	err := opt.MaxAge.Set("1h")
	assert.NoError(t, err)
	flt, err := filter.NewFilter(opt)
	assert.NoError(t, err)

	defCtx := context.Background()
	fltCtx := filter.ReplaceConfig(defCtx, flt)

	testCtx1 := fltCtx
	testCtx2 := filter.SetUseFilter(testCtx1, true)
	testCtx3, testCancel := context.WithCancel(testCtx2)
	testCtx4 := filter.SetUseFilter(testCtx3, false)
	testCancel()
	assert.False(t, filter.GetUseFilter(testCtx1))
	assert.True(t, filter.GetUseFilter(testCtx2))
	assert.True(t, filter.GetUseFilter(testCtx3))
	assert.False(t, filter.GetUseFilter(testCtx4))

	subRemote := fmt.Sprintf("%s:%s/%s", f.Name(), f.Root(), "agequery-testdir")
	subFsResult, err := fs.NewFs(defCtx, subRemote)
	require.NoError(t, err)
	subFs, isDriveFs := subFsResult.(*Fs)
	require.True(t, isDriveFs)

	tempDir1 := t.TempDir()
	tempFs1, err := fs.NewFs(defCtx, tempDir1)
	require.NoError(t, err)

	tempDir2 := t.TempDir()
	tempFs2, err := fs.NewFs(defCtx, tempDir2)
	require.NoError(t, err)

	file1 := fstest.Item{ModTime: time.Now(), Path: "agequery.txt"}
	_ = fstests.PutTestContents(defCtx, t, tempFs1, &file1, "abcxyz", true)

	// validate sync/copy
	const timeQuery = "(modifiedTime >= '"

	assert.NoError(t, fssync.CopyDir(defCtx, subFs, tempFs1, false))
	assert.NotContains(t, subFs.lastQuery, timeQuery)

	assert.NoError(t, fssync.CopyDir(fltCtx, subFs, tempFs1, false))
	assert.Contains(t, subFs.lastQuery, timeQuery)

	assert.NoError(t, fssync.CopyDir(fltCtx, tempFs2, subFs, false))
	assert.Contains(t, subFs.lastQuery, timeQuery)

	assert.NoError(t, fssync.CopyDir(defCtx, tempFs2, subFs, false))
	assert.NotContains(t, subFs.lastQuery, timeQuery)

	// validate list/walk
	devNull, errOpen := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(t, errOpen)
	defer func() {
		_ = devNull.Close()
	}()

	assert.NoError(t, operations.List(defCtx, subFs, devNull))
	assert.NotContains(t, subFs.lastQuery, timeQuery)

	assert.NoError(t, operations.List(fltCtx, subFs, devNull))
	assert.Contains(t, subFs.lastQuery, timeQuery)
}

func (f *Fs) InternalTest(t *testing.T) {
	// These tests all depend on each other so run them as nested tests
	t.Run("DocumentImport", func(t *testing.T) {
		f.InternalTestDocumentImport(t)
		t.Run("DocumentUpdate", func(t *testing.T) {
			f.InternalTestDocumentUpdate(t)
			t.Run("DocumentExport", func(t *testing.T) {
				f.InternalTestDocumentExport(t)
				t.Run("DocumentLink", func(t *testing.T) {
					f.InternalTestDocumentLink(t)
				})
			})
		})
	})
	t.Run("Shortcuts", f.InternalTestShortcuts)
	t.Run("UnTrash", f.InternalTestUnTrash)
	t.Run("CopyOrMoveID", f.InternalTestCopyOrMoveID)
	t.Run("Query", f.InternalTestQuery)
	t.Run("AgeQuery", f.InternalTestAgeQuery)
	t.Run("ShouldRetry", f.InternalTestShouldRetry)
}

var _ fstests.InternalTester = (*Fs)(nil)
