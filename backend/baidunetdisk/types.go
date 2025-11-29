package baidunetdisk

import (
	"encoding/hex"
	"errors"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var (
	errBaiduEmptyFilesNotAllowed = errors.New("baidunetdisk: empty files are not allowed")
	errUploadIDExpired           = errors.New("baidunetdisk: uploadid expired")
	errEmptyToken                = errors.New("baidunetdisk: empty token returned from api")
)

// token error structure when using official oauth refresh
type tokenErrResp struct {
	ErrorDescription string `json:"error_description"`
	Error            string `json:"error"`
}

// File is the baidu api file info payload.
type File struct {
	Category       int    `json:"category"`
	FsID           int64  `json:"fs_id"`
	Size           int64  `json:"size"`
	Path           string `json:"path"`
	ServerFilename string `json:"server_filename"`
	Md5            string `json:"md5"`
	Isdir          int    `json:"isdir"`
	ServerCtime    int64  `json:"server_ctime"`
	ServerMtime    int64  `json:"server_mtime"`
	LocalMtime     int64  `json:"local_mtime"`
	LocalCtime     int64  `json:"local_ctime"`
	Ctime          int64  `json:"ctime"`
	Mtime          int64  `json:"mtime"`
	Thumbs         struct {
		URL3 string `json:"url3"`
	} `json:"thumbs"`
}

// ListResp is response for list api.
type ListResp struct {
	Errno int    `json:"errno"`
	List  []File `json:"list"`
}

// DownloadResp represents xpan/multimedia?method=filemetas
type DownloadResp struct {
	Errno int `json:"errno"`
	List  []struct {
		Dlink string `json:"dlink"`
	} `json:"list"`
}

// PrecreateResp is precreate response.
type PrecreateResp struct {
	Errno      int    `json:"errno"`
	ReturnType int    `json:"return_type"`
	Path       string `json:"path"`
	UploadID   string `json:"uploadid"`
	BlockList  []int  `json:"block_list"`
	File       File   `json:"info"`
	UploadURL  string `json:"-"` // cached upload domain for resume
}

// QuotaResp is response for quota endpoint.
type QuotaResp struct {
	Errno int    `json:"errno"`
	Total uint64 `json:"total"`
	Used  uint64 `json:"used"`
}

func (f File) ensureTimes() File {
	if f.ServerCtime == 0 {
		f.ServerCtime = f.Ctime
	}
	if f.ServerMtime == 0 {
		f.ServerMtime = f.Mtime
	}
	return f
}

// toObjectInfo converts File to ObjectInfo
func (f File) toObjectInfo(root string) objectInfo {
	f = f.ensureTimes()
	name := f.ServerFilename
	if name == "" {
		name = path.Base(f.Path)
	}
	remote := strings.TrimPrefix(f.Path, root)
	remote = strings.TrimPrefix(remote, "/")
	return objectInfo{
		fsID:     f.FsID,
		path:     f.Path,
		remote:   remote,
		name:     name,
		size:     f.Size,
		modTime:  time.Unix(f.ServerMtime, 0),
		ctime:    time.Unix(f.ServerCtime, 0),
		isDir:    f.Isdir == 1,
		md5:      decryptMd5(f.Md5),
		thumbURL: f.Thumbs.URL3,
	}
}

// decryptMd5 converts baidu md5 (possibly encrypted) to plain md5.
func decryptMd5(encryptMd5 string) string {
	if _, err := hex.DecodeString(encryptMd5); err == nil && len(encryptMd5) == 32 {
		return encryptMd5
	}
	var out strings.Builder
	out.Grow(len(encryptMd5))
	for i, n := 0, int64(0); i < len(encryptMd5); i++ {
		if i == 9 {
			n = int64(unicode.ToLower(rune(encryptMd5[i])) - 'g')
		} else {
			n, _ = strconv.ParseInt(encryptMd5[i:i+1], 16, 64)
		}
		out.WriteString(strconv.FormatInt(n^int64(15&i), 16))
	}
	encryptMd5 = out.String()
	return encryptMd5[8:16] + encryptMd5[:8] + encryptMd5[24:32] + encryptMd5[16:24]
}

// objectInfo carries file metadata mapped into rclone expectations.
type objectInfo struct {
	fsID     int64
	path     string
	remote   string
	name     string
	size     int64
	modTime  time.Time
	ctime    time.Time
	isDir    bool
	md5      string
	thumbURL string
}
