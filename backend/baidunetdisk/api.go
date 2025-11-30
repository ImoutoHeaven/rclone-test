package baidunetdisk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
)

const (
	defaultUploadAPI                   = "https://d.pcs.baidu.com"
	defaultUploadThread                = 3
	defaultUploadTimeout               = 60 * time.Second
	defaultRetryCount                  = 10
	defaultServersideMD5Override       = true
	defaultRetryInitialWait            = time.Second
	defaultRetryMaxWait                = 5 * time.Second
	defaultOnlineAPIAddress            = "https://api.oplist.org/baiduyun/renewapi"
	maxUploadThread                    = 64
	minUploadThread                    = 1
	maxSliceNum                        = 2048
	defaultDynamicUploadRotate         = 256
	defaultDynamicUploadRandom         = false
	defaultSliceSize             int64 = 4 * 1024 * 1024
	vipSliceSize                 int64 = 16 * 1024 * 1024
	svipSliceSize                int64 = 32 * 1024 * 1024
	sliceStep                    int64 = 1 * 1024 * 1024
)

// errnoError wraps baidu errno so callers can inspect it.
type errnoError struct {
	code int
}

func (e errnoError) Error() string {
	return fmt.Sprintf("baidunetdisk api error: errno=%d", e.code)
}

// Options defines backend configuration.
type Options struct {
	RefreshToken                string        `config:"refresh_token"`
	ClientID                    string        `config:"client_id"`
	ClientSecret                string        `config:"client_secret"`
	UseOnlineAPI                bool          `config:"use_online_api"`
	APIAddress                  string        `config:"api_url_address"`
	UploadThread                int           `config:"upload_thread"`
	UploadTimeout               time.Duration `config:"upload_timeout"`
	UploadAPI                   string        `config:"upload_api"`
	UseDynamicUploadAPI         bool          `config:"use_dynamic_upload_api"`
	DynamicUploadAPIRotate      int           `config:"dynamic_upload_api_rotate"`
	DynamicUploadAPIRandom      bool          `config:"dynamic_upload_api_random_pick"`
	DynamicUploadAPISliceRandom bool          `config:"dynamic_upload_api_slice_random_pick"`
	CustomUploadPartSize        int64         `config:"custom_upload_part_size"`
	LowBandwidthUploadMode      bool          `config:"low_bandwith_upload_mode"` //nolint:misspell // keep tag aligned with upstream naming
	UploadRetryCount            int           `config:"upload_retry_count"`
	UploadRetryWait             time.Duration `config:"upload_retry_initial_wait"`
	UploadRetryMaxWait          time.Duration `config:"upload_retry_max_wait"`
	AccessToken                 string        `config:"access_token"`
	OrderBy                     string        `config:"order_by"`
	OrderDirection              string        `config:"order_direction"`
	ServersideMD5Override       bool          `config:"serverside_md5_override"`
}

// uploadProgressStore persists upload progress on disk (per content-md5 + access_token).
type uploadProgressStore struct {
	mu       sync.Mutex
	basePath string
}

func newUploadProgressStore(remoteName string) *uploadProgressStore {
	dir := filepath.Join(config.GetCacheDir(), "baidunetdisk", remoteName)
	_ = os.MkdirAll(dir, 0o755)
	return &uploadProgressStore{basePath: dir}
}

func (s *uploadProgressStore) pathForKey(key string) string {
	safe := url.PathEscape(key)
	return filepath.Join(s.basePath, safe+".json")
}

func (s *uploadProgressStore) Load(key string) (*PrecreateResp, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.pathForKey(key))
	if err != nil {
		return nil, false
	}
	var resp PrecreateResp
	if json.Unmarshal(data, &resp) != nil {
		return nil, false
	}
	return &resp, true
}

func (s *uploadProgressStore) Save(key string, resp *PrecreateResp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.pathForKey(key)
	if resp == nil {
		_ = os.Remove(path)
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o644)
}

// config options for backend registration
var configOptions = []fs.Option{{
	Name:     "refresh_token",
	Help:     "Baidu Netdisk refresh token.",
	Required: true,
}, {
	Name:     "client_id",
	Help:     "Client ID for local OAuth refresh.",
	Advanced: true,
}, {
	Name:     "client_secret",
	Help:     "Client Secret for local OAuth refresh.",
	Advanced: true,
}, {
	Name:     "use_online_api",
	Help:     "Use online API to refresh token.",
	Default:  true,
	Advanced: true,
}, {
	Name:     "api_url_address",
	Help:     "Online API address for token refresh.",
	Default:  defaultOnlineAPIAddress,
	Advanced: true,
}, {
	Name:     "upload_thread",
	Help:     "Concurrent upload threads (1-64).",
	Default:  defaultUploadThread,
	Advanced: true,
}, {
	Name:     "upload_timeout",
	Help:     "Per-slice upload timeout in seconds.",
	Default:  fs.Duration(defaultUploadTimeout),
	Advanced: true,
}, {
	Name:     "upload_api",
	Help:     "Fixed upload api endpoint.",
	Default:  defaultUploadAPI,
	Advanced: true,
}, {
	Name:     "use_dynamic_upload_api",
	Help:     "Use locateupload to resolve upload domain dynamically; falls back to upload_api on failure.",
	Default:  true,
	Advanced: true,
}, {
	Name:     "dynamic_upload_api_rotate",
	Help:     "When dynamic upload api is enabled, re-resolve upload domain every N parts (0 to disable rotation).",
	Default:  defaultDynamicUploadRotate,
	Advanced: true,
}, {
	Name:     "dynamic_upload_api_random_pick",
	Help:     "When dynamic upload api is enabled, randomly pick from https servers list instead of always using the first one.",
	Default:  defaultDynamicUploadRandom,
	Advanced: true,
}, {
	Name:     "dynamic_upload_api_slice_random_pick",
	Help:     "When dynamic upload api is enabled, randomly pick upload host for each slice and retry using cached https servers.",
	Default:  false,
	Advanced: true,
}, {
	Name:     "custom_upload_part_size",
	Help:     "Custom upload part size in bytes (0 for auto, limited by VIP type).",
	Default:  int64(0),
	Advanced: true,
}, {
	Name:     "low_bandwith_upload_mode", //nolint:misspell // keep option name for compatibility
	Help:     "Enable low bandwidth mode (increase slice size gradually to keep part count <= 2048).",
	Default:  false,
	Advanced: true,
}, {
	Name:     "upload_retry_count",
	Help:     "Max retries per slice.",
	Default:  defaultRetryCount,
	Advanced: true,
}, {
	Name:     "upload_retry_initial_wait",
	Help:     "Initial backoff for slice retry.",
	Default:  fs.Duration(defaultRetryInitialWait),
	Advanced: true,
}, {
	Name:     "upload_retry_max_wait",
	Help:     "Max backoff for slice retry.",
	Default:  fs.Duration(defaultRetryMaxWait),
	Advanced: true,
}, {
	Name:     "order_by",
	Help:     "List ordering field (name|time|size).",
	Default:  "name",
	Advanced: true,
}, {
	Name:     "order_direction",
	Help:     "List ordering direction (asc|desc).",
	Default:  "asc",
	Advanced: true,
}, {
	Name:     "serverside_md5_override",
	Help:     "When true, use per-slice MD5 returned by Baidu server to build block_list for create; when false, use locally computed MD5 list.",
	Default:  defaultServersideMD5Override,
	Advanced: true,
}}

// setDefaults normalises options and fills defaults.
func (o *Options) setDefaults() {
	if o.UploadThread == 0 {
		o.UploadThread = defaultUploadThread
	}
	if o.UploadThread < minUploadThread {
		o.UploadThread = minUploadThread
	}
	if o.UploadThread > maxUploadThread {
		o.UploadThread = maxUploadThread
	}
	if o.UploadTimeout == 0 {
		o.UploadTimeout = defaultUploadTimeout
	}
	if o.UploadAPI == "" {
		o.UploadAPI = defaultUploadAPI
	}
	if o.APIAddress == "" {
		o.APIAddress = defaultOnlineAPIAddress
	}
	if o.DynamicUploadAPIRotate < 0 {
		o.DynamicUploadAPIRotate = 0
	}
	if !o.UseDynamicUploadAPI {
		// When dynamic upload api is disabled, random/rotate flags are ignored.
		o.DynamicUploadAPIRandom = false
		o.DynamicUploadAPISliceRandom = false
	}
	if o.UploadRetryCount == 0 {
		o.UploadRetryCount = defaultRetryCount
	}
	if o.UploadRetryWait == 0 {
		o.UploadRetryWait = defaultRetryInitialWait
	}
	if o.UploadRetryMaxWait == 0 {
		o.UploadRetryMaxWait = defaultRetryMaxWait
	}
	if o.OrderBy == "" {
		o.OrderBy = "name"
	}
	switch o.OrderBy {
	case "name", "time", "size":
	default:
		o.OrderBy = "name"
	}
	if o.OrderDirection == "" {
		o.OrderDirection = "asc"
	}
	if o.OrderDirection != "asc" && o.OrderDirection != "desc" {
		o.OrderDirection = "asc"
	}
}

// parse config into options.
func getOptions(m configmap.Mapper) (*Options, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	opt.setDefaults()
	return opt, nil
}

// refreshToken refreshes access + refresh token with retry on empty token.
func (f *Fs) refreshToken(ctx context.Context) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	return f.refreshTokenLocked(ctx)
}

// refreshTokenLocked performs single token refresh with caller holding tokenMu.
func (f *Fs) refreshTokenLocked(ctx context.Context) error {
	err := f.refreshTokenOnce(ctx)
	if err != nil && errors.Is(err, errEmptyToken) {
		return f.refreshTokenOnce(ctx)
	}
	return err
}

// refreshTokenOnce performs single token refresh. Caller must hold tokenMu.
func (f *Fs) refreshTokenOnce(ctx context.Context) error {
	if f.opt.UseOnlineAPI && f.opt.APIAddress != "" {
		u, _ := url.Parse(f.opt.APIAddress)
		q := u.Query()
		q.Set("refresh_ui", f.opt.RefreshToken)
		q.Set("server_use", "true")
		q.Set("driver_txt", "baiduyun_go")
		u.RawQuery = q.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		resp, err := f.client.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		var out struct {
			RefreshToken string `json:"refresh_token"`
			AccessToken  string `json:"access_token"`
			ErrorMessage string `json:"text"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return err
		}
		if out.RefreshToken == "" || out.AccessToken == "" {
			if out.ErrorMessage != "" {
				return fmt.Errorf("token refresh failed: %s", out.ErrorMessage)
			}
			return errEmptyToken
		}
		f.accessToken = out.AccessToken
		f.opt.RefreshToken = out.RefreshToken
		if err := config.SetValueAndSave(f.name, "refresh_token", f.opt.RefreshToken); err != nil {
			return err
		}
		if err := config.SetValueAndSave(f.name, "access_token", f.accessToken); err != nil {
			return err
		}
		return nil
	}

	if f.opt.ClientID == "" || f.opt.ClientSecret == "" {
		return fmt.Errorf("client_id and client_secret are required when use_online_api=false")
	}
	values := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {f.opt.RefreshToken},
		"client_id":     {f.opt.ClientID},
		"client_secret": {f.opt.ClientSecret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://openapi.baidu.com/oauth/2.0/token?"+values.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	var token struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	var tokenErr tokenErrResp
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		// try decode error
		_ = json.NewDecoder(resp.Body).Decode(&tokenErr)
		if tokenErr.Error != "" {
			return fmt.Errorf("%s : %s", tokenErr.Error, tokenErr.ErrorDescription)
		}
		return err
	}
	if token.RefreshToken == "" {
		return errEmptyToken
	}
	f.accessToken = token.AccessToken
	f.opt.RefreshToken = token.RefreshToken
	if err := config.SetValueAndSave(f.name, "refresh_token", f.opt.RefreshToken); err != nil {
		return err
	}
	if err := config.SetValueAndSave(f.name, "access_token", f.accessToken); err != nil {
		return err
	}
	return nil
}

// apiRequest executes baidu api with retry + errno handling.
func (f *Fs) apiRequest(ctx context.Context, method, fullURL string, params url.Values, body io.Reader, contentType string, out interface{}) ([]byte, error) {
	if err := f.ensureAccessToken(ctx); err != nil {
		return nil, err
	}
	var bodyBuf []byte
	if body != nil {
		var err error
		bodyBuf, err = io.ReadAll(body)
		if err != nil {
			return nil, err
		}
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		var reqBody io.Reader
		if bodyBuf != nil {
			reqBody = bytes.NewReader(bodyBuf)
		}
		req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		q := req.URL.Query()
		for k, v := range params {
			for _, item := range v {
				q.Add(k, item)
			}
		}
		q.Set("access_token", f.getAccessToken())
		req.URL.RawQuery = q.Encode()
		resp, err := f.client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second << attempt)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Second << attempt)
			continue
		}
		var errno struct {
			Errno int `json:"errno"`
		}
		_ = json.Unmarshal(data, &errno)
		if errno.Errno != 0 {
			if errno.Errno == 111 || errno.Errno == -6 {
				if err := f.refreshToken(ctx); err != nil {
					return nil, err
				}
				lastErr = fmt.Errorf("errno %d", errno.Errno)
				time.Sleep(time.Second << attempt)
				continue
			}
			return nil, errnoError{code: errno.Errno}
		}
		if out != nil {
			if err := json.Unmarshal(data, out); err != nil {
				return nil, err
			}
		}
		return data, nil
	}
	return nil, lastErr
}

func (f *Fs) apiGet(ctx context.Context, path string, params url.Values, out interface{}) ([]byte, error) {
	return f.apiRequest(ctx, http.MethodGet, "https://pan.baidu.com/rest/2.0"+path, params, nil, "", out)
}

func (f *Fs) apiPostForm(ctx context.Context, path string, params url.Values, form url.Values, out interface{}) ([]byte, error) {
	body := strings.NewReader(form.Encode())
	return f.apiRequest(ctx, http.MethodPost, "https://pan.baidu.com/rest/2.0"+path, params, body, "application/x-www-form-urlencoded", out)
}

// getVIPType fetches uinfo to determine vip type.
func (f *Fs) getVIPType(ctx context.Context) (int, error) {
	data, err := f.apiGet(ctx, "/xpan/nas", url.Values{"method": {"uinfo"}}, nil)
	if err != nil {
		return 0, err
	}
	var vip struct {
		VipType int `json:"vip_type"`
	}
	_ = json.Unmarshal(data, &vip)
	return vip.VipType, nil
}

// getSliceSize matches OpenList calculation.
func (f *Fs) getSliceSize(filesize int64) int64 {
	vipType := f.vipType
	// non vip fixed 4MB
	if vipType == 0 {
		if f.opt.CustomUploadPartSize != 0 && f.opt.CustomUploadPartSize < defaultSliceSize {
			fs.Logf(f, "custom upload part size not supported for non vip, using default slice size")
		}
		if filesize > maxSliceNum*defaultSliceSize {
			fs.Logf(f, "file size %d exceeds recommended limit, may fail", filesize)
		}
		return defaultSliceSize
	}

	if f.opt.CustomUploadPartSize != 0 {
		part := f.opt.CustomUploadPartSize
		max := vipSliceSize
		if vipType == 2 {
			max = svipSliceSize
		}
		if part < defaultSliceSize {
			fs.Logf(f, "custom part size %d smaller than default, using default", part)
			return defaultSliceSize
		}
		if part > max {
			fs.Logf(f, "custom part size %d larger than vip max %d, clamping", part, max)
			return max
		}
		return part
	}

	maxSlice := defaultSliceSize
	switch vipType {
	case 1:
		maxSlice = vipSliceSize
	case 2:
		maxSlice = svipSliceSize
	}

	if f.opt.LowBandwidthUploadMode {
		size := defaultSliceSize
		for size <= maxSlice {
			if filesize <= maxSliceNum*size {
				return size
			}
			size += sliceStep
		}
	}

	if filesize > maxSliceNum*maxSlice {
		fs.Logf(f, "file size %d exceeds recommended limit, may fail", filesize)
	}

	return maxSlice
}

// apiCreate wraps create call.
func (f *Fs) apiCreate(ctx context.Context, fullPath string, size int64, isDir int, uploadID, blockList string, ctime, mtime int64) (*File, error) {
	params := url.Values{"method": {"create"}}
	form := url.Values{
		"path":  {fullPath},
		"size":  {strconv.FormatInt(size, 10)},
		"isdir": {strconv.Itoa(isDir)},
		"rtype": {"3"},
	}
	if ctime != 0 && mtime != 0 {
		form.Set("local_mtime", strconv.FormatInt(mtime, 10))
		form.Set("local_ctime", strconv.FormatInt(ctime, 10))
	}
	if uploadID != "" {
		form.Set("uploadid", uploadID)
	}
	if blockList != "" {
		form.Set("block_list", blockList)
	}

	var resp File
	// 针对 errno=10（百度侧分片校验/时序问题）做有限次退避重试：
	// 间隔 30s 和 60s，共 3 次尝试。
	delays := []time.Duration{30 * time.Second, 60 * time.Second}
	maxAttempts := len(delays) + 1

	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, err := f.apiPostForm(ctx, "/xpan/file", params, form, &resp)
		if err == nil {
			return &resp, nil
		}

		var ee errnoError
		if !errors.As(err, &ee) || ee.code != 10 {
			// 非 errno=10 的错误不在这里重试，直接返回
			return nil, err
		}

		// errno=10 视为可能的服务端时序问题，做有限次延迟重试
		if attempt >= len(delays) {
			// 已达到最大重试次数，仍然 errno=10
			return nil, err
		}

		wait := delays[attempt]
		fs.Debugf(f, "create errno=10, retrying in %v (attempt %d/%d) path=%s size=%d", wait, attempt+1, maxAttempts, fullPath, size)

		// 等待时尊重 context 取消
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}

	return nil, errnoError{code: 10}
}

// apiPrecreate wraps precreate call. content/slice md5 included only when provided.
func (f *Fs) apiPrecreate(ctx context.Context, fullPath string, size int64, blockListStr, contentMd5, sliceMd5 string, ctime, mtime int64) (*PrecreateResp, error) {
	params := url.Values{"method": {"precreate"}}
	form := url.Values{
		"path":       {fullPath},
		"size":       {strconv.FormatInt(size, 10)},
		"isdir":      {"0"},
		"autoinit":   {"1"},
		"rtype":      {"3"},
		"block_list": {blockListStr},
	}
	if contentMd5 != "" && sliceMd5 != "" {
		form.Set("content-md5", contentMd5)
		form.Set("slice-md5", sliceMd5)
	}
	if ctime != 0 && mtime != 0 {
		form.Set("local_mtime", strconv.FormatInt(mtime, 10))
		form.Set("local_ctime", strconv.FormatInt(ctime, 10))
	}
	var resp PrecreateResp
	if _, err := f.apiPostForm(ctx, "/xpan/file", params, form, &resp); err != nil {
		return nil, err
	}
	if resp.ReturnType == 2 {
		resp.File.Ctime = ctime
		resp.File.Mtime = mtime
	}
	return &resp, nil
}

// getUploadURL resolves upload api endpoint, optionally using locateupload.
func (f *Fs) getUploadURL(ctx context.Context, fullPath, uploadID string) string {
	// Dynamic resolution disabled: always use configured upload api.
	if !f.opt.UseDynamicUploadAPI || fullPath == "" || uploadID == "" {
		return f.opt.UploadAPI
	}
	u, err := f.locateUpload(ctx, fullPath, uploadID)
	if err != nil {
		fs.Debugf(f, "locateupload failed for path=%s uploadid=%s: %v, fallback to %s", fullPath, uploadID, err, f.opt.UploadAPI)
		return f.opt.UploadAPI
	}
	return u
}

// locateUpload calls Baidu locateupload api to obtain preferred upload domain.
func (f *Fs) locateUpload(ctx context.Context, fullPath, uploadID string) (string, error) {
	base := defaultUploadAPI
	u, err := url.Parse(base + "/rest/2.0/pcs/file")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("method", "locateupload")
	q.Set("appid", "250528")
	q.Set("path", fullPath)
	q.Set("uploadid", uploadID)
	q.Set("upload_version", "2.0")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, maxDownloadErrorBody))
		return "", fmt.Errorf("locateupload status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}

	var payload UploadServerResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if payload.ErrorCode != 0 {
		return "", fmt.Errorf("locateupload error_code=%d msg=%s", payload.ErrorCode, payload.ErrorMsg)
	}

	// Prefer https servers; optionally random pick when enabled.
	pickRandom := f.opt.DynamicUploadAPIRandom

	httpsFromList := func(list []struct {
		Server string `json:"server"`
	}) []string { out := make([]string, 0, len(list)); for _, s := range list {
		if strings.HasPrefix(s.Server, "https://") {
			out = append(out, s.Server)
		}
	}; return out }

	serversHTTPS := httpsFromList(payload.Servers)
	if len(serversHTTPS) > 0 {
		host := serversHTTPS[0]
		if pickRandom && len(serversHTTPS) > 1 {
			idx := rand.Intn(len(serversHTTPS))
			host = serversHTTPS[idx]
		}
		fs.Debugf(f, "locateupload resolved server=%s path=%s uploadid=%s random=%v candidates=%d", host, fullPath, uploadID, pickRandom, len(serversHTTPS))
		return host, nil
	}

	bakHTTPS := httpsFromList(payload.BakServers)
	if len(bakHTTPS) > 0 {
		host := bakHTTPS[0]
		if pickRandom && len(bakHTTPS) > 1 {
			idx := rand.Intn(len(bakHTTPS))
			host = bakHTTPS[idx]
		}
		fs.Debugf(f, "locateupload resolved bak_server=%s path=%s uploadid=%s random=%v candidates=%d", host, fullPath, uploadID, pickRandom, len(bakHTTPS))
		return host, nil
	}

	// Cache https hosts list for per-slice random pick.
	allHTTPS := append(serversHTTPS, bakHTTPS...)
	if len(allHTTPS) > 0 {
		f.uploadHostsMu.Lock()
		f.uploadHosts[uploadID] = allHTTPS
		f.uploadHostsMu.Unlock()
	}

	if payload.Host != "" {
		host := payload.Host
		if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
			host = "https://" + host
		}
		fs.Debugf(f, "locateupload resolved host=%s path=%s uploadid=%s", host, fullPath, uploadID)
		return host, nil
	}
	return "", fmt.Errorf("locateupload: no upload server in response for path=%s uploadid=%s", fullPath, uploadID)
}

// ensureAccessToken ensures we have an access token.
func (f *Fs) ensureAccessToken(ctx context.Context) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	if f.accessToken != "" {
		return nil
	}
	if f.opt.AccessToken != "" {
		f.accessToken = f.opt.AccessToken
		return nil
	}
	return f.refreshTokenLocked(ctx)
}

// newHTTPClient creates configured http client.
func newHTTPClient(ctx context.Context) *http.Client {
	return fshttp.NewClient(ctx)
}

// getAccessToken returns current access token under lock.
func (f *Fs) getAccessToken() string {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()
	return f.accessToken
}
