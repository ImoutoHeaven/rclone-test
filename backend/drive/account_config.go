package drive

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"golang.org/x/oauth2"
)

type accountConfig struct {
	index            int
	name             string
	clientID         string
	clientSecret     string
	tokenJSON        string
	uploadDailyLimit fs.SizeSuffix
}

type accountConfigJSON struct {
	Name             string          `json:"name"`
	ClientID         string          `json:"client_id"`
	ClientSecret     string          `json:"client_secret"`
	Token            json.RawMessage `json:"token"`
	UploadDailyLimit *string         `json:"upload_daily_limit"`
}

func parseAccountsJSON(accountsPath string, opt *Options) ([]accountConfig, error) {
	if opt == nil {
		opt = &Options{}
	}

	normalizedPath := strings.TrimSpace(accountsPath)
	if !filepath.IsAbs(normalizedPath) {
		return nil, fmt.Errorf("drive: accounts_json must be an absolute path to a local file")
	}

	contents, err := os.ReadFile(normalizedPath)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return nil, fmt.Errorf("drive: accounts_json file not found: %w", err)
		case errors.Is(err, os.ErrPermission):
			return nil, fmt.Errorf("drive: accounts_json file is not readable: %w", err)
		default:
			return nil, fmt.Errorf("drive: failed to read accounts_json file: %w", err)
		}
	}

	jsonEntries, err := decodeAccountsFileContents(contents)
	if err != nil {
		return nil, err
	}

	entries := make([]accountConfig, len(jsonEntries))
	for i, account := range jsonEntries {
		tokenJSON, err := normalizeOAuthTokenJSON(string(account.Token))
		if err != nil {
			return nil, fmt.Errorf("drive: account %d %w", i, err)
		}

		effectiveLimit := opt.UploadDailyLimit
		if account.UploadDailyLimit != nil {
			parsedLimit := fs.SizeSuffix(0)
			if err := parsedLimit.Set(strings.TrimSpace(*account.UploadDailyLimit)); err != nil {
				return nil, fmt.Errorf("drive: account %d upload_daily_limit is invalid: %w", i, err)
			}
			if err := checkUploadDailyLimit(parsedLimit); err != nil {
				return nil, fmt.Errorf("drive: account %d upload_daily_limit must be > 0: %w", i, err)
			}
			effectiveLimit = parsedLimit
		}

		name := strings.TrimSpace(account.Name)
		if name == "" {
			name = fmt.Sprintf("account-%d", i)
		}

		entries[i] = accountConfig{
			index:            i,
			name:             name,
			clientID:         firstNonEmpty(strings.TrimSpace(account.ClientID), strings.TrimSpace(opt.ClientID)),
			clientSecret:     firstNonEmpty(strings.TrimSpace(account.ClientSecret), strings.TrimSpace(opt.ClientSecret)),
			tokenJSON:        tokenJSON,
			uploadDailyLimit: effectiveLimit,
		}
	}

	return entries, nil
}

func decodeAccountsFileContents(contents []byte) ([]accountConfigJSON, error) {
	trimmed := strings.TrimSpace(string(contents))
	if trimmed == "" {
		return nil, fmt.Errorf("drive: accounts_json file is empty")
	}

	if strings.HasPrefix(trimmed, "[") {
		var entries []accountConfigJSON
		if err := json.Unmarshal([]byte(trimmed), &entries); err != nil {
			return nil, fmt.Errorf("drive: invalid accounts_json JSON array: %w", err)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("drive: accounts_json must not be empty array")
		}
		return entries, nil
	}

	lines := strings.Split(trimmed, "\n")
	entries := make([]accountConfigJSON, 0, len(lines))
	for lineNo, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry accountConfigJSON
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("drive: invalid accounts_json JSONL at line %d: %w", lineNo+1, err)
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("drive: accounts_json file is empty")
	}

	return entries, nil
}

func normalizeOAuthTokenJSON(tokenJSON string) (string, error) {
	tokenJSON = strings.TrimSpace(tokenJSON)
	if tokenJSON == "" {
		return "", fmt.Errorf("token is required")
	}

	if !strings.HasPrefix(tokenJSON, "{") {
		return "", fmt.Errorf("token must be a JSON object")
	}

	tokenFields := make(map[string]json.RawMessage)
	if err := json.Unmarshal([]byte(tokenJSON), &tokenFields); err != nil {
		return "", fmt.Errorf("token must contain parseable OAuth token JSON: %w", err)
	}
	if len(tokenFields) == 0 {
		return "", fmt.Errorf("token must be a non-empty OAuth token JSON object")
	}

	normalizedToken, err := json.Marshal(tokenFields)
	if err != nil {
		return "", fmt.Errorf("token must contain parseable OAuth token JSON: %w", err)
	}

	token := new(oauth2.Token)
	if err := json.Unmarshal(normalizedToken, token); err != nil {
		return "", fmt.Errorf("token must contain parseable OAuth token JSON: %w", err)
	}
	if token.AccessToken == "" && token.RefreshToken == "" && token.TokenType == "" && token.Expiry.IsZero() {
		return "", fmt.Errorf("token must be a non-empty OAuth token JSON object")
	}

	return string(normalizedToken), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
