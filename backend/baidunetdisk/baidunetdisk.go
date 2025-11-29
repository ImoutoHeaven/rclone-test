// Package baidunetdisk implements Baidu Netdisk backend based on OpenList baidu-upload-mod logic.
package baidunetdisk

import (
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
	return f, nil
}

// Name of the remote
func (f *Fs) Name() string { return f.name }

// Root of the remote
func (f *Fs) Root() string { return strings.TrimPrefix(f.root, "/") }

// String returns description.
func (f *Fs) String() string { return fmt.Sprintf("Baidu Netdisk root '%s'", f.root) }

// Precision returns modtime precision
func (f *Fs) Precision() time.Duration { return time.Second }

// Hashes returns supported hash types.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.MD5) }

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
	modTime := src.ModTime(ctx)
	contentMd5, sliceMd5, blockList, err := computeHashes(cacheFile, size, sliceSize)
	if err != nil {
		return nil, err
	}
	blockListStr, _ := json.Marshal(blockList)

	// rapid upload
	if obj, err := f.putRapid(ctx, full, size, contentMd5, modTime, blockListStr); err == nil {
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
		if precreate.ReturnType == 2 {
			info := precreate.File.toObjectInfo(f.root)
			return f.newObject(info), nil
		}
	}
	if precreate.UploadURL == "" {
		precreate.UploadURL = f.getUploadURL(full, precreate.UploadID)
	}

	for retry := 0; retry < 2; retry++ {
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
		return nil, err
	}
	f.progressStore.Save(key, nil)

	fileInfo, err := f.apiCreate(ctx, full, size, 0, precreate.UploadID, string(blockListStr), ctime, mtime)
	if err != nil {
		return nil, err
	}
	info := fileInfo.toObjectInfo(f.root)
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
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		defer func() {
			_ = pw.Close()
		}()
		part, err := mw.CreateFormFile("file", fileName)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		section := io.NewSectionReader(file, offset, size)
		if _, err := io.Copy(part, section); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = mw.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL+"/rest/2.0/pcs/superfile2", pr)
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	lower := strings.ToLower(string(body))
	if strings.Contains(lower, "uploadid") &&
		(strings.Contains(lower, "invalid") || strings.Contains(lower, "expired") || strings.Contains(lower, "not found")) {
		return errUploadIDExpired
	}
	var errno struct {
		Errno     int `json:"errno"`
		ErrorCode int `json:"error_code"`
	}
	_ = json.Unmarshal(body, &errno)
	if errno.Errno != 0 || errno.ErrorCode != 0 {
		return fmt.Errorf("upload slice failed: errno=%d code=%d resp=%s", errno.Errno, errno.ErrorCode, string(body))
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

// Fs returns the parent Fs.
func (o *Object) Fs() fs.Info { return o.fs }

// Remote path.
func (o *Object) Remote() string { return o.remote }

// String representation.
func (o *Object) String() string { return o.Remote() }

// Hash returns md5 when requested.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t == hash.MD5 {
		return o.info.md5, nil
	}
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

// Open downloads object.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var meta DownloadResp
	params := url.Values{
		"method": {"filemetas"},
		"fsids":  {fmt.Sprintf("[%d]", o.info.fsID)},
		"dlink":  {"1"},
	}
	if _, err := o.fs.apiGet(ctx, "/xpan/multimedia", params, &meta); err != nil {
		return nil, err
	}
	if len(meta.List) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	dl := fmt.Sprintf("%s&access_token=%s", meta.List[0].Dlink, o.fs.accessToken)
	client := *o.fs.client
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, dl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()
	location := resp.Header.Get("Location")
	if location == "" {
		location = dl
	}
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	getReq.Header.Set("User-Agent", "pan.baidu.com")
	resp, err = o.fs.client.Do(getReq)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
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
