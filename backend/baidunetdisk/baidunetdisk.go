// Package baidunetdisk implements Baidu Netdisk backend based on OpenList baidu-upload-mod logic.
package baidunetdisk

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
)

// Fs represents Baidu Netdisk backend.
type Fs struct {
	name        string
	root        string
	opt         *Options
	features    *fs.Features
	client      *http.Client
	accessToken string
	vipType     int

	progressStore *uploadProgressStore
}

// Object represents a Baidu Netdisk object.
type Object struct {
	fs     *Fs
	remote string
	info   objectInfo
}

var (
	_ fs.Fs          = (*Fs)(nil)
	_ fs.PutStreamer = (*Fs)(nil)
	_ fs.Object      = (*Object)(nil)
	_ fs.Copier      = (*Fs)(nil)
	_ fs.Mover       = (*Fs)(nil)
	_ fs.DirMover    = (*Fs)(nil)
	_ fs.Purger      = (*Fs)(nil)
	_ fs.UserInfoer  = (*Fs)(nil)
)

const (
	baiduErrAccessTokenInvalid = 31045
	baiduErrAntiHotlink        = 31326
	baiduErrSignature          = 31362
	baiduErrDlinkExpired       = 31360

	maxDownloadErrorBody = 32 * 1024
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "baidunetdisk",
		Description: "Baidu Netdisk",
		NewFs:       NewFs,
		Options:     configOptions,
	})
}

// NewFs constructs Fs.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt, err := getOptions(m)
	if err != nil {
		return nil, err
	}
	root = path.Clean("/" + strings.Trim(root, "/"))

	f := &Fs{
		name:          name,
		root:          root,
		opt:           opt,
		client:        newHTTPClient(ctx),
		progressStore: newUploadProgressStore(name),
	}
	if err := f.ensureAccessToken(ctx); err != nil {
		return nil, err
	}
	vip, err := f.getVIPType(ctx)
	if err != nil {
		return nil, err
	}
	f.vipType = vip
	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)
	if err := f.adjustRoot(ctx); err != nil {
		if errors.Is(err, fs.ErrorIsFile) {
			return f, fs.ErrorIsFile
		}
		return nil, err
	}
	return f, nil
}

// Name of the remote
func (f *Fs) Name() string { return f.name }

// Root of the remote
func (f *Fs) Root() string { return strings.TrimPrefix(f.root, "/") }

// String returns description.
func (f *Fs) String() string { return fmt.Sprintf("Baidu Netdisk root '%s'", f.root) }

// Precision returns modtime precision. Baidu Netdisk does not provide a
// reliable file modification time, so advertise modtime as unsupported and
// let rclone fall back to size-only comparisons.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns supported hash types.
// Baidu's reported MD5 for superfile2 uploads is not a reliable end-to-end
// checksum (see OpenList notes), so we deliberately disable hash support to
// avoid false "corrupted on transfer" results.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.None) }

// Features returns optional features.
func (f *Fs) Features() *fs.Features { return f.features }

// UserInfo returns quota info.
func (f *Fs) UserInfo(ctx context.Context) (map[string]string, error) {
	var quota QuotaResp
	if _, err := f.apiRequest(ctx, http.MethodGet, "https://pan.baidu.com/api/quota", url.Values{}, nil, "", &quota); err != nil {
		return nil, err
	}
	return map[string]string{
		"total": strconv.FormatUint(quota.Total, 10),
		"used":  strconv.FormatUint(quota.Used, 10),
	}, nil
}

// fullPath with root.
func (f *Fs) fullPath(remote string) string {
	if remote == "" {
		return f.root
	}
	return path.Clean(path.Join(f.root, remote))
}

// NewObject finds object by remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	parent := path.Dir(remote)
	if parent == "." {
		parent = ""
	}
	entries, err := f.List(ctx, parent)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if obj, ok := entry.(*Object); ok && obj.Remote() == remote {
			return obj, nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// List directories/files.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	full := f.fullPath(dir)
	start := 0
	limit := 200
	for {
		params := url.Values{
			"method": {"list"},
			"dir":    {full},
			"web":    {"web"},
			"start":  {strconv.Itoa(start)},
			"limit":  {strconv.Itoa(limit)},
		}
		if f.opt.OrderBy != "" {
			params.Set("order", f.opt.OrderBy)
			if f.opt.OrderDirection == "desc" {
				params.Set("desc", "1")
			}
		}
		var resp ListResp
		if _, err = f.apiGet(ctx, "/xpan/file", params, &resp); err != nil {
			var ee errnoError
			if errors.As(err, &ee) && ee.code == -9 {
				return entries, nil
			}
			return nil, err
		}
		if len(resp.List) == 0 {
			break
		}
		for _, item := range resp.List {
			info := item.toObjectInfo(f.root)
			if info.isDir {
				entries = append(entries, fs.NewDir(info.remote, info.modTime))
			} else {
				entries = append(entries, &Object{
					fs:     f,
					remote: info.remote,
					info:   info,
				})
			}
		}
		start += limit
	}
	return entries, nil
}

// adjustRoot detects whether the configured root points to an existing file.
// If so, it rewinds root to parent and signals fs.ErrorIsFile per rclone convention.
func (f *Fs) adjustRoot(ctx context.Context) error {
	if f.root == "/" {
		return nil
	}
	cleanRoot := f.root
	parent := path.Dir(cleanRoot)
	if parent == "." {
		parent = "/"
	}
	leaf := path.Base(cleanRoot)

	start := 0
	limit := 200
	for {
		params := url.Values{
			"method": {"list"},
			"dir":    {parent},
			"web":    {"web"},
			"start":  {strconv.Itoa(start)},
			"limit":  {strconv.Itoa(limit)},
		}
		var resp ListResp
		if _, err := f.apiGet(ctx, "/xpan/file", params, &resp); err != nil {
			var ee errnoError
			if errors.As(err, &ee) && ee.code == -9 {
				// parent not found; treat as new path, keep current root
				return nil
			}
			return err
		}
		if len(resp.List) == 0 {
			return nil
		}
		for _, item := range resp.List {
			if item.ServerFilename == leaf || path.Base(item.Path) == leaf {
				if item.Isdir == 0 {
					f.root = parent
					return fs.ErrorIsFile
				}
				// leaf is directory; keep original root
				f.root = cleanRoot
				return nil
			}
		}
		start += limit
	}
}

// Mkdir is noop (Baidu create happens during upload/create).
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.apiCreate(ctx, f.fullPath(dir), 0, 1, "", "", 0, 0)
	return err
}

// Rmdir removes an empty directory.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.deletePath(ctx, f.fullPath(dir))
}

// Purge deletes all files under dir (server side delete).
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.deletePath(ctx, f.fullPath(dir))
}

// Copy performs server-side copy using filemanager copy.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	srcPath := f.fullPath(srcObj.Remote())
	dstFull := f.fullPath(remote)
	dstDir := path.Dir(dstFull)
	newName := path.Base(dstFull)
	err := f.manage(ctx, "copy", []map[string]string{{
		"path":    srcPath,
		"dest":    dstDir,
		"newname": newName,
	}})
	if err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

// Move performs server-side move.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	srcPath := f.fullPath(srcObj.Remote())
	dstFull := f.fullPath(remote)
	dstDir := path.Dir(dstFull)
	newName := path.Base(dstFull)
	err := f.manage(ctx, "move", []map[string]string{{
		"path":    srcPath,
		"dest":    dstDir,
		"newname": newName,
	}})
	if err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

// DirMove moves a directory server-side.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	if src.Name() != f.Name() {
		return fs.ErrorCantDirMove
	}
	srcPath := f.fullPath(srcRemote)
	dstFull := f.fullPath(dstRemote)
	dstDir := path.Dir(dstFull)
	newName := path.Base(dstFull)
	return f.manage(ctx, "move", []map[string]string{{
		"path":    srcPath,
		"dest":    dstDir,
		"newname": newName,
	}})
}

// Put uploads object.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.put(ctx, in, src, options)
}

// PutStream uploads stream.
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.put(ctx, in, src, options)
}

func (f *Fs) put(ctx context.Context, in io.Reader, src fs.ObjectInfo, _ []fs.OpenOption) (fs.Object, error) {
	if src.Size() < 1 {
		return nil, errBaiduEmptyFilesNotAllowed
	}
	full := f.fullPath(src.Remote())
	size := src.Size()
	modTime := src.ModTime(ctx)
	fs.Debugf(f, "upload start path=%s size=%d", full, size)
	cacheFile, cleanup, err := f.ensureCacheFile(ctx, in, size)
	if err != nil {
		return nil, err
	}
	if cleanup {
		defer func() {
			_ = cacheFile.Close()
			_ = os.Remove(cacheFile.Name())
		}()
	}
	sliceSize := f.getSliceSize(size)
	contentMd5, sliceMd5, blockList, err := computeHashes(cacheFile, size, sliceSize)
	if err != nil {
		return nil, err
	}
	fs.Debugf(f, "upload hashes path=%s content-md5=%s slice-md5=%s blocks=%d sliceSize=%d", full, contentMd5, sliceMd5, len(blockList), sliceSize)
	blockListStr, _ := json.Marshal(blockList)

	// rapid upload
	if obj, err := f.putRapid(ctx, full, size, contentMd5, modTime, blockListStr); err == nil {
		fs.Debugf(f, "rapid upload hit path=%s size=%d", full, size)
		return obj, nil
	}

	ctime := modTime.Unix()
	mtime := modTime.Unix()
	key := contentMd5 + "_" + f.accessToken
	precreate, ok := f.progressStore.Load(key)
	if !ok {
		precreate, err = f.apiPrecreate(ctx, full, size, string(blockListStr), contentMd5, sliceMd5, ctime, mtime)
		if err != nil {
			return nil, err
		}
		fs.Debugf(f, "precreate new path=%s uploadid=%s returnType=%d blocks=%d", full, precreate.UploadID, precreate.ReturnType, len(precreate.BlockList))
		if precreate.ReturnType == 2 {
			info := precreate.File.toObjectInfo(f.root)
			return f.newObject(info), nil
		}
	} else {
		fs.Debugf(f, "resume upload from cache path=%s uploadid=%s pending=%d", full, precreate.UploadID, countPending(precreate.BlockList))
	}
	if precreate.UploadURL == "" {
		precreate.UploadURL = f.getUploadURL(full, precreate.UploadID)
	}

	for retry := 0; retry < 2; retry++ {
		fs.Debugf(f, "upload parts path=%s uploadid=%s attempt=%d pending=%d", full, precreate.UploadID, retry, countPending(precreate.BlockList))
		err = f.uploadParts(ctx, precreate, cacheFile, full, path.Base(full), size, sliceSize)
		if err == nil {
			break
		}
		filtered := filterUnfinished(precreate.BlockList)
		precreate.BlockList = filtered
		f.progressStore.Save(key, precreate)
		if errors.Is(err, errUploadIDExpired) {
			newPre, err2 := f.apiPrecreate(ctx, full, size, string(blockListStr), "", "", ctime, mtime)
			if err2 != nil {
				return nil, err2
			}
			if newPre.ReturnType == 2 {
				info := newPre.File.toObjectInfo(f.root)
				f.progressStore.Save(key, nil)
				return f.newObject(info), nil
			}
			precreate = newPre
			precreate.UploadURL = f.getUploadURL(full, precreate.UploadID)
			continue
		}
		fs.Debugf(f, "upload parts failed path=%s uploadid=%s err=%v", full, precreate.UploadID, err)
		return nil, err
	}
	f.progressStore.Save(key, nil)

	fs.Debugf(f, "calling create path=%s uploadid=%s blocks=%d", full, precreate.UploadID, len(blockList))
	fileInfo, err := f.apiCreate(ctx, full, size, 0, precreate.UploadID, string(blockListStr), ctime, mtime)
	if err != nil {
		fs.Debugf(f, "create failed path=%s uploadid=%s err=%v", full, precreate.UploadID, err)
		return nil, err
	}
	fs.Debugf(f, "create ok path=%s fsid=%d size=%d", full, fileInfo.FsID, fileInfo.Size)
	info := fileInfo.toObjectInfo(f.root)
	// Baidu's create API returns \"now\" as mtime even when local_mtime is
	// set; mirror OpenList by overriding the returned timestamps so rclone's
	// change detection doesn't treat freshly uploaded files as changed.
	info.modTime = modTime
	info.ctime = modTime
	return f.newObject(info), nil
}

func (f *Fs) putRapid(ctx context.Context, fullPath string, size int64, contentMd5 string, modTime time.Time, blockList []byte) (fs.Object, error) {
	if len(contentMd5) != 32 {
		return nil, fmt.Errorf("invalid content-md5")
	}
	ctime := modTime.Unix()
	mtime := modTime.Unix()
	file, err := f.apiCreate(ctx, fullPath, size, 0, "", string(blockList), ctime, mtime)
	if err != nil {
		return nil, err
	}
	info := file.toObjectInfo(f.root)
	info.modTime = modTime
	info.ctime = modTime
	return f.newObject(info), nil
}

// uploadParts uploads all pending parts concurrently with per-slice retry.
func (f *Fs) uploadParts(ctx context.Context, pre *PrecreateResp, file *os.File, fullPath, fileName string, totalSize, sliceSize int64) error {
	totalParts := len(pre.BlockList)
	if totalParts == 0 {
		return nil
	}
	lastBlockSize := totalSize % sliceSize
	if lastBlockSize == 0 {
		lastBlockSize = sliceSize
	}

	sem := make(chan struct{}, f.opt.UploadThread)
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	for idx, seq := range pre.BlockList {
		if seq < 0 {
			continue
		}
		idx := idx
		partSeq := seq
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			offset := int64(partSeq) * sliceSize
			partSize := sliceSize
			if partSeq+1 == totalParts {
				partSize = lastBlockSize
			}
			var attemptErr error
			wait := f.opt.UploadRetryWait
			for attempt := 0; attempt <= f.opt.UploadRetryCount; attempt++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				attemptErr = f.uploadSlice(ctx, pre.UploadURL, fullPath, pre.UploadID, fileName, partSeq, file, offset, partSize)
				if attemptErr == nil {
					mu.Lock()
					pre.BlockList[idx] = -1
					mu.Unlock()
					return nil
				}
				if errors.Is(attemptErr, errUploadIDExpired) {
					return attemptErr
				}
				if attempt < f.opt.UploadRetryCount {
					time.Sleep(wait)
					wait *= 2
					if wait > f.opt.UploadRetryMaxWait {
						wait = f.opt.UploadRetryMaxWait
					}
				}
			}
			return attemptErr
		})
	}
	return g.Wait()
}

// uploadSlice performs single slice upload.
func (f *Fs) uploadSlice(ctx context.Context, uploadURL, fullPath, uploadID, fileName string, partSeq int, file *os.File, offset, size int64) error {
	// Build multipart head/tail to avoid chunked transfer (Baidu rejects chunked).
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_, err := mw.CreateFormFile("file", fileName)
	if err != nil {
		return err
	}
	headLen := buf.Len()
	if err := mw.Close(); err != nil {
		return err
	}
	bufBytes := buf.Bytes()
	head := bytes.NewReader(bufBytes[:headLen])
	tail := bytes.NewReader(bufBytes[headLen:])
	section := io.NewSectionReader(file, offset, size)
	body := io.MultiReader(head, section, tail)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL+"/rest/2.0/pcs/superfile2", body)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Set("method", "upload")
	q.Set("type", "tmpfile")
	q.Set("path", fullPath)
	q.Set("uploadid", uploadID)
	q.Set("partseq", strconv.Itoa(partSeq))
	q.Set("access_token", f.accessToken)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.ContentLength = int64(head.Len()) + size + int64(tail.Len())
	client := *f.client
	if f.opt.UploadTimeout > 0 {
		client.Timeout = f.opt.UploadTimeout
	} else {
		client.Timeout = defaultUploadTimeout
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	lower := strings.ToLower(string(respBody))
	if strings.Contains(lower, "uploadid") &&
		(strings.Contains(lower, "invalid") || strings.Contains(lower, "expired") || strings.Contains(lower, "not found")) {
		return errUploadIDExpired
	}
	var errno struct {
		Errno     int `json:"errno"`
		ErrorCode int `json:"error_code"`
	}
	_ = json.Unmarshal(respBody, &errno)
	if errno.Errno != 0 || errno.ErrorCode != 0 {
		return fmt.Errorf("upload slice failed: errno=%d code=%d resp=%s", errno.Errno, errno.ErrorCode, string(respBody))
	}
	return nil
}

// computeHashes calculates file md5, first 256k md5 and block md5 list.
func computeHashes(file *os.File, size, sliceSize int64) (string, string, []string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", "", nil, err
	}
	fileMd5 := md5.New()
	first256 := md5.New()
	var firstRemaining int64 = 256 * 1024
	count := int((size + sliceSize - 1) / sliceSize)
	blockList := make([]string, 0, count)
	buf := make([]byte, 4*1024*1024)
	var offset int64
	for part := 0; part < count; part++ {
		partSize := sliceSize
		if offset+partSize > size {
			partSize = size - offset
		}
		blockMd5 := md5.New()
		remaining := partSize
		for remaining > 0 {
			readSize := int64(len(buf))
			if readSize > remaining {
				readSize = remaining
			}
			n, err := file.Read(buf[:readSize])
			if err != nil && err != io.EOF {
				return "", "", nil, err
			}
			if n == 0 {
				break
			}
			data := buf[:n]
			fileMd5.Write(data)
			blockMd5.Write(data)
			if firstRemaining > 0 {
				toWrite := int64(n)
				if toWrite > firstRemaining {
					toWrite = firstRemaining
				}
				first256.Write(data[:toWrite])
				firstRemaining -= toWrite
			}
			remaining -= int64(n)
			offset += int64(n)
		}
		blockList = append(blockList, hex.EncodeToString(blockMd5.Sum(nil)))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", "", nil, err
	}
	return hex.EncodeToString(fileMd5.Sum(nil)), hex.EncodeToString(first256.Sum(nil)), blockList, nil
}

// ensureCacheFile ensures we have a seekable file for upload.
func (f *Fs) ensureCacheFile(ctx context.Context, in io.Reader, size int64) (*os.File, bool, error) {
	if file, ok := in.(*os.File); ok {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, false, err
		}
		return file, false, nil
	}
	tmp, err := os.CreateTemp("", "baidunetdisk-*")
	if err != nil {
		return nil, false, err
	}
	written, err := io.Copy(tmp, in)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, false, err
	}
	if written != size {
		fs.Logf(f, "warning: written size mismatch, expected %d got %d", size, written)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, false, err
	}
	return tmp, true, nil
}

// delete path using filemanager delete.
func (f *Fs) deletePath(ctx context.Context, fullPath string) error {
	return f.manage(ctx, "delete", []string{fullPath})
}

// manage wraps filemanager opera.
func (f *Fs) manage(ctx context.Context, opera string, filelist any) error {
	params := url.Values{"method": {"filemanager"}, "opera": {opera}}
	listStr, err := json.Marshal(filelist)
	if err != nil {
		return err
	}
	form := url.Values{
		"async":    {"0"},
		"filelist": {string(listStr)},
		"ondup":    {"fail"},
	}
	_, err = f.apiPostForm(ctx, "/xpan/file", params, form, nil)
	return err
}

// newObject builds Object.
func (f *Fs) newObject(info objectInfo) *Object {
	return &Object{
		fs:     f,
		remote: info.remote,
		info:   info,
	}
}

// filterUnfinished keeps non -1 part seqs.
func filterUnfinished(parts []int) []int {
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if p >= 0 {
			out = append(out, p)
		}
	}
	return out
}

// countPending returns number of parts not marked as finished (-1).
func countPending(parts []int) int {
	count := 0
	for _, p := range parts {
		if p >= 0 {
			count++
		}
	}
	return count
}

// Fs returns the parent Fs.
func (o *Object) Fs() fs.Info { return o.fs }

// Remote path.
func (o *Object) Remote() string { return o.remote }

// String representation.
func (o *Object) String() string { return o.Remote() }

// Hash is not supported because Baidu's MD5 is not an authoritative checksum.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns object size.
func (o *Object) Size() int64 { return o.info.size }

// ModTime returns modification time.
func (o *Object) ModTime(ctx context.Context) time.Time { return o.info.modTime }

// SetModTime not supported.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error { return fs.ErrorCantSetModTime }

// Storable indicates can store.
func (o *Object) Storable() bool { return true }

type baiduErrorBody struct {
	Errno     int    `json:"errno"`
	ErrorCode int    `json:"error_code"`
	ErrMsg    string `json:"errmsg"`
	ErrorMsg  string `json:"error_msg"`
	RequestID string `json:"request_id"`
}

type baiduDownloadError struct {
	statusCode int
	errno      int
	message    string
	requestID  string
}

func (e baiduDownloadError) Error() string {
	parts := make([]string, 0, 3)
	if e.errno != 0 {
		parts = append(parts, fmt.Sprintf("errno=%d", e.errno))
	}
	if e.statusCode != 0 {
		parts = append(parts, fmt.Sprintf("status=%d", e.statusCode))
	}
	if e.message != "" {
		parts = append(parts, e.message)
	}
	if len(parts) == 0 {
		return "baidunetdisk download error"
	}
	return strings.Join(parts, ": ")
}

func (e baiduDownloadError) needsTokenRefresh() bool {
	return e.errno == baiduErrAccessTokenInvalid
}

func (e baiduDownloadError) needsDlinkRefresh() bool {
	return e.errno == baiduErrDlinkExpired || e.errno == baiduErrSignature
}

func (e baiduDownloadError) isAntiHotlink() bool {
	return e.errno == baiduErrAntiHotlink
}

func parseBaiduDownloadError(resp *http.Response) baiduDownloadError {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxDownloadErrorBody))
	var payload baiduErrorBody
	_ = json.Unmarshal(data, &payload)
	errno := payload.Errno
	if errno == 0 {
		errno = payload.ErrorCode
	}
	message := payload.ErrMsg
	if message == "" {
		message = payload.ErrorMsg
	}
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	requestID := payload.RequestID
	if requestID == "" {
		requestID = resp.Header.Get("X-Bce-Request-Id")
	}
	return baiduDownloadError{
		statusCode: resp.StatusCode,
		errno:      errno,
		message:    message,
		requestID:  requestID,
	}
}

func isAccessTokenError(err error) bool {
	var ee errnoError
	if errors.As(err, &ee) && ee.code == baiduErrAccessTokenInvalid {
		return true
	}
	var de baiduDownloadError
	return errors.As(err, &de) && de.errno == baiduErrAccessTokenInvalid
}

func (o *Object) fetchDownloadLink(ctx context.Context) (string, error) {
	var meta DownloadResp
	params := url.Values{
		"method": {"filemetas"},
		"fsids":  {fmt.Sprintf("[%d]", o.info.fsID)},
		"dlink":  {"1"},
	}
	if _, err := o.fs.apiGet(ctx, "/xpan/multimedia", params, &meta); err != nil {
		return "", err
	}
	if len(meta.List) == 0 {
		return "", fs.ErrorObjectNotFound
	}
	if meta.List[0].Dlink == "" {
		return "", errors.New("baidunetdisk: empty dlink returned")
	}
	dl := fmt.Sprintf("%s&access_token=%s", meta.List[0].Dlink, o.fs.accessToken)
	return dl, nil
}

func (o *Object) resolveDownloadLocation(ctx context.Context, dlink string) (string, error) {
	client := *o.fs.client
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, dlink, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return "", parseBaiduDownloadError(resp)
	}
	location := resp.Header.Get("Location")
	if location == "" {
		location = dlink
	}
	return location, nil
}

func (o *Object) doDownload(ctx context.Context, location string, headers map[string]string) (io.ReadCloser, error) {
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		getReq.Header.Set(k, v)
	}
	resp, err := o.fs.client.Do(getReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		defer resp.Body.Close()
		return nil, parseBaiduDownloadError(resp)
	}
	return resp.Body, nil
}

// Open downloads object with Range/error handling aligned with rclone expectations.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.Size())
	headers := fs.OpenOptionHeaders(options)
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["User-Agent"] = "pan.baidu.com"

	var (
		dlink            string
		needDlink        = true
		needTokenRefresh bool
		lastErr          error
	)

	for attempt := 0; attempt < 3; attempt++ {
		if needTokenRefresh {
			if err := o.fs.refreshToken(ctx); err != nil {
				return nil, fmt.Errorf("refresh access token: %w", err)
			}
			needTokenRefresh = false
			needDlink = true
		}

		if needDlink {
			link, err := o.fetchDownloadLink(ctx)
			if err != nil {
				if isAccessTokenError(err) && !needTokenRefresh {
					fs.Debugf(o, "filemetas errno=%d, refreshing token and retrying download", baiduErrAccessTokenInvalid)
					needTokenRefresh = true
					lastErr = err
					continue
				}
				return nil, err
			}
			dlink = link
			needDlink = false
		}

		location, err := o.resolveDownloadLocation(ctx, dlink)
		if err != nil {
			if de, ok := err.(baiduDownloadError); ok {
				fs.Debugf(o, "download HEAD error: status=%d errno=%d request_id=%s msg=%s", de.statusCode, de.errno, de.requestID, de.message)
				if de.needsTokenRefresh() && !needTokenRefresh {
					needTokenRefresh = true
					lastErr = err
					continue
				}
				if de.needsDlinkRefresh() && !needDlink {
					needDlink = true
					lastErr = err
					continue
				}
				if de.isAntiHotlink() {
					return nil, err
				}
			}
			return nil, err
		}

		body, err := o.doDownload(ctx, location, headers)
		if err == nil {
			return body, nil
		}
		if de, ok := err.(baiduDownloadError); ok {
			fs.Debugf(o, "download GET error: status=%d errno=%d request_id=%s msg=%s", de.statusCode, de.errno, de.requestID, de.message)
			if de.needsTokenRefresh() && !needTokenRefresh {
				needTokenRefresh = true
				lastErr = err
				continue
			}
			if de.needsDlinkRefresh() && !needDlink {
				needDlink = true
				lastErr = err
				continue
			}
			if de.isAntiHotlink() {
				return nil, err
			}
		}
		return nil, err
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("baidunetdisk: failed to open object for download")
}

// Update re-uploads.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	obj, err := o.fs.put(ctx, in, src, options)
	if err != nil {
		return err
	}
	*o = *(obj.(*Object))
	return nil
}

// Remove deletes object.
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deletePath(ctx, o.fs.fullPath(o.Remote()))
}
