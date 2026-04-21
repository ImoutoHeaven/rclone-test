package drive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type accountStore struct {
	mu           sync.RWMutex
	accounts     []accountConfig
	positions    map[int]int
	rev          uint64
	persistedRev uint64

	beforeSetMuLock  func()
	afterStopEnqueue func()

	persistFn func(string) error
	backoffFn func(int) time.Duration

	signalCh chan struct{}
	flushCh  chan accountStoreFlushRequest
	stopCh   chan struct{}
	doneCh   chan struct{}

	accepting           atomic.Bool
	stopOnce            sync.Once
	cancelSignalPersist context.CancelFunc
}

type accountStoreFlushRequest struct {
	ctx  context.Context
	done chan error
}

type persistedAccountJSON struct {
	Name             string          `json:"name,omitempty"`
	ClientID         string          `json:"client_id,omitempty"`
	ClientSecret     string          `json:"client_secret,omitempty"`
	Token            json.RawMessage `json:"token"`
	UploadDailyLimit string          `json:"upload_daily_limit,omitempty"`
}

type accountsJSONFormat int

const (
	accountsJSONFormatJSONArray accountsJSONFormat = iota
	accountsJSONFormatJSONL
)

var persistedCredentialFields = []string{"token", "client_id", "client_secret"}

func newAccountStore(initial []accountConfig, persistFn func(string) error) *accountStore {
	if persistFn == nil {
		persistFn = func(string) error { return nil }
	}

	accounts := make([]accountConfig, len(initial))
	copy(accounts, initial)

	positions := make(map[int]int, len(accounts))
	for i, account := range accounts {
		positions[account.index] = i
	}

	signalCtx, cancel := context.WithCancel(context.Background())
	s := &accountStore{
		accounts:            accounts,
		positions:           positions,
		persistFn:           persistFn,
		backoffFn:           defaultAccountStoreBackoff,
		signalCh:            make(chan struct{}, 1),
		flushCh:             make(chan accountStoreFlushRequest),
		stopCh:              make(chan struct{}),
		doneCh:              make(chan struct{}),
		cancelSignalPersist: cancel,
	}
	s.accepting.Store(true)

	go s.writerLoop(signalCtx)

	return s
}

func defaultAccountStoreBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<(attempt-1)) * 100 * time.Millisecond
}

func (s *accountStore) writerLoop(signalCtx context.Context) {
	defer close(s.doneCh)

	for {
		select {
		case <-s.signalCh:
			_ = s.persistUntilStable(signalCtx)
		case req := <-s.flushCh:
			req.done <- s.persistUntilStable(req.ctx)
		case <-s.stopCh:
			return
		}
	}
}

func (s *accountStore) notifyWriter() {
	select {
	case s.signalCh <- struct{}{}:
	default:
	}
}

func (s *accountStore) setAccountToken(index int, tokenJSON string) {
	tokenJSON, err := normalizeOAuthTokenJSON(tokenJSON)
	if err != nil {
		return
	}
	if hook := s.beforeSetMuLock; hook != nil {
		hook()
	}

	s.mu.Lock()
	if !s.accepting.Load() {
		s.mu.Unlock()
		return
	}
	position, ok := s.positions[index]
	if !ok {
		s.mu.Unlock()
		return
	}
	if s.accounts[position].tokenJSON == tokenJSON {
		s.mu.Unlock()
		return
	}
	s.accounts[position].tokenJSON = tokenJSON
	s.rev++
	s.mu.Unlock()

	s.notifyWriter()
}

func (s *accountStore) setAccountClientID(index int, clientID string) {
	clientID = strings.TrimSpace(clientID)
	if hook := s.beforeSetMuLock; hook != nil {
		hook()
	}

	s.mu.Lock()
	if !s.accepting.Load() {
		s.mu.Unlock()
		return
	}
	position, ok := s.positions[index]
	if !ok {
		s.mu.Unlock()
		return
	}
	if s.accounts[position].clientID == clientID {
		s.mu.Unlock()
		return
	}
	s.accounts[position].clientID = clientID
	s.rev++
	s.mu.Unlock()

	s.notifyWriter()
}

func (s *accountStore) setAccountClientSecret(index int, clientSecret string) {
	clientSecret = strings.TrimSpace(clientSecret)
	if hook := s.beforeSetMuLock; hook != nil {
		hook()
	}

	s.mu.Lock()
	if !s.accepting.Load() {
		s.mu.Unlock()
		return
	}
	position, ok := s.positions[index]
	if !ok {
		s.mu.Unlock()
		return
	}
	if s.accounts[position].clientSecret == clientSecret {
		s.mu.Unlock()
		return
	}
	s.accounts[position].clientSecret = clientSecret
	s.rev++
	s.mu.Unlock()

	s.notifyWriter()
}

func (s *accountStore) accountToken(index int) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	position, ok := s.positions[index]
	if !ok {
		return "", false
	}
	return s.accounts[position].tokenJSON, true
}

func (s *accountStore) accountClientID(index int) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	position, ok := s.positions[index]
	if !ok {
		return "", false
	}
	return s.accounts[position].clientID, true
}

func (s *accountStore) accountClientSecret(index int) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	position, ok := s.positions[index]
	if !ok {
		return "", false
	}
	return s.accounts[position].clientSecret, true
}

func (s *accountStore) flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	req := accountStoreFlushRequest{
		ctx:  ctx,
		done: make(chan error, 1),
	}

	select {
	case <-s.doneCh:
		return nil
	case s.flushCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *accountStore) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	wasAccepting := s.accepting.Swap(false)
	s.mu.Unlock()

	if wasAccepting {
		s.cancelSignalPersist()
	}
	if hook := s.afterStopEnqueue; hook != nil {
		hook()
	}

	flushErr := s.flush(ctx)

	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	select {
	case <-s.doneCh:
	case <-ctx.Done():
		if flushErr == nil {
			return ctx.Err()
		}
	}

	if flushErr != nil {
		return flushErr
	}
	return nil
}

func (s *accountStore) persistUntilStable(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		snapshot, rev, err := s.snapshotAccountsJSON()
		if err != nil {
			return err
		}
		if rev <= s.getPersistedRev() {
			return nil
		}

		attempt := 0
		for {
			if err := ctx.Err(); err != nil {
				return err
			}

			err := s.persistFn(snapshot)
			if err == nil {
				s.setPersistedRev(rev)
				break
			}

			attempt++
			if err := sleepContext(ctx, s.backoffFn(attempt)); err != nil {
				return err
			}

			latestSnapshot, latestRev, snapshotErr := s.snapshotAccountsJSON()
			if snapshotErr != nil {
				return snapshotErr
			}
			if latestRev != rev {
				snapshot = latestSnapshot
				rev = latestRev
				attempt = 0
			}
		}
	}
}

func (s *accountStore) snapshotAccountsJSON() (string, uint64, error) {
	s.mu.RLock()
	accounts := make([]accountConfig, len(s.accounts))
	copy(accounts, s.accounts)
	rev := s.rev
	s.mu.RUnlock()

	encoded, err := encodeAccountsJSON(accounts)
	if err != nil {
		return "", 0, err
	}
	return encoded, rev, nil
}

func encodeAccountsJSON(accounts []accountConfig) (string, error) {
	entries := make([]persistedAccountJSON, len(accounts))
	for i, account := range accounts {
		entries[i] = persistedAccountJSON{
			Name:             strings.TrimSpace(account.name),
			ClientID:         strings.TrimSpace(account.clientID),
			ClientSecret:     strings.TrimSpace(account.clientSecret),
			Token:            json.RawMessage(account.tokenJSON),
			UploadDailyLimit: account.uploadDailyLimit.String(),
		}
	}

	buf, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *accountStore) getPersistedRev() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.persistedRev
}

func (s *accountStore) setPersistedRev(rev uint64) {
	s.mu.Lock()
	if rev > s.persistedRev {
		s.persistedRev = rev
	}
	s.mu.Unlock()
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeAccountsJSONObjects(contents []byte) ([]map[string]json.RawMessage, accountsJSONFormat, bool, error) {
	raw := string(contents)
	hasTrailingNewline := strings.HasSuffix(raw, "\n")
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, accountsJSONFormatJSONArray, hasTrailingNewline, fmt.Errorf("drive: accounts_json file is empty")
	}

	if strings.HasPrefix(trimmed, "[") {
		var entries []map[string]json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &entries); err != nil {
			return nil, accountsJSONFormatJSONArray, hasTrailingNewline, fmt.Errorf("drive: invalid accounts_json JSON array: %w", err)
		}
		if len(entries) == 0 {
			return nil, accountsJSONFormatJSONArray, hasTrailingNewline, fmt.Errorf("drive: accounts_json must not be empty array")
		}
		return entries, accountsJSONFormatJSONArray, hasTrailingNewline, nil
	}

	lines := strings.Split(trimmed, "\n")
	entries := make([]map[string]json.RawMessage, 0, len(lines))
	for lineNo, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, accountsJSONFormatJSONL, hasTrailingNewline, fmt.Errorf("drive: invalid accounts_json JSONL at line %d: %w", lineNo+1, err)
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, accountsJSONFormatJSONL, hasTrailingNewline, fmt.Errorf("drive: accounts_json file is empty")
	}
	return entries, accountsJSONFormatJSONL, hasTrailingNewline, nil
}

func encodeAccountsJSONObjects(entries []map[string]json.RawMessage, format accountsJSONFormat, hasTrailingNewline bool) (string, error) {
	switch format {
	case accountsJSONFormatJSONArray:
		buf, err := json.Marshal(entries)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	case accountsJSONFormatJSONL:
		lines := make([]string, len(entries))
		for i, entry := range entries {
			buf, err := json.Marshal(entry)
			if err != nil {
				return "", err
			}
			lines[i] = string(buf)
		}
		out := strings.Join(lines, "\n")
		if hasTrailingNewline {
			out += "\n"
		}
		return out, nil
	default:
		return "", fmt.Errorf("drive: unknown accounts_json format %d", format)
	}
}

func mergePersistedCredentialFields(existing, updated map[string]json.RawMessage) {
	if existing == nil {
		return
	}
	for _, field := range persistedCredentialFields {
		value, ok := updated[field]
		if !ok {
			delete(existing, field)
			continue
		}
		existing[field] = append(json.RawMessage(nil), value...)
	}
}

func mergePersistedAccountsJSON(existingContents []byte, updatedContents string) (string, error) {
	existingEntries, format, hasTrailingNewline, err := decodeAccountsJSONObjects(existingContents)
	if err != nil {
		return "", err
	}
	updatedEntries, _, _, err := decodeAccountsJSONObjects([]byte(updatedContents))
	if err != nil {
		return "", err
	}
	if len(existingEntries) != len(updatedEntries) {
		return "", fmt.Errorf("drive: accounts_json entry count changed from %d to %d", len(existingEntries), len(updatedEntries))
	}

	for i := range existingEntries {
		if existingEntries[i] == nil {
			existingEntries[i] = make(map[string]json.RawMessage)
		}
		mergePersistedCredentialFields(existingEntries[i], updatedEntries[i])
	}

	return encodeAccountsJSONObjects(existingEntries, format, hasTrailingNewline)
}

func persistAccountsJSONFile(accountsPath, accountsJSON string) error {
	normalizedPath := strings.TrimSpace(accountsPath)
	if !filepath.IsAbs(normalizedPath) {
		return fmt.Errorf("drive: accounts_json must be an absolute path to a local file")
	}

	existingContents, err := os.ReadFile(normalizedPath)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("drive: accounts_json file not found: %w", err)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("drive: accounts_json file is not readable: %w", err)
		default:
			return fmt.Errorf("drive: failed to access accounts_json file: %w", err)
		}
	}

	mergedAccountsJSON, err := mergePersistedAccountsJSON(existingContents, accountsJSON)
	if err != nil {
		return fmt.Errorf("drive: failed to merge accounts_json updates: %w", err)
	}

	dir := filepath.Dir(normalizedPath)
	tmpFile, err := os.CreateTemp(dir, ".rclone-drive-accounts-*.tmp")
	if err != nil {
		return fmt.Errorf("drive: failed to create temporary accounts_json file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmpFile.Chmod(0o600); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("drive: failed to set secure permissions on temporary accounts_json file: %w", err)
	}

	if _, err := tmpFile.WriteString(mergedAccountsJSON); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("drive: failed to write temporary accounts_json file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("drive: failed to sync temporary accounts_json file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("drive: failed to close temporary accounts_json file: %w", err)
	}

	if err := os.Rename(tmpPath, normalizedPath); err != nil {
		return fmt.Errorf("drive: failed to replace accounts_json file atomically: %w", err)
	}

	cleanupTmp = false
	return nil
}
