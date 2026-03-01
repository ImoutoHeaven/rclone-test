package myrient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestFs(t *testing.T, serverURL string) *Fs {
	t.Helper()

	fsi, err := NewFs(context.Background(), "test", "", configmap.Simple{"url": serverURL + "/"})
	require.NoError(t, err)

	f, ok := fsi.(*Fs)
	require.True(t, ok)
	return f
}

func TestBackendRegistrationAndReadOnly(t *testing.T) {
	regInfo, err := fs.Find("myrient")
	require.NoError(t, err)
	require.NotNil(t, regInfo)

	var urlOpt *fs.Option
	for i := range regInfo.Options {
		if regInfo.Options[i].Name == "url" {
			urlOpt = &regInfo.Options[i]
			break
		}
	}
	require.NotNil(t, urlOpt)
	require.Equal(t, "https://myrient.erista.me/files/", urlOpt.Default)
	require.False(t, urlOpt.Required)

	f := &Fs{}
	require.ErrorIs(t, f.Mkdir(context.Background(), ""), errReadOnly)
	require.ErrorIs(t, f.Rmdir(context.Background(), ""), errReadOnly)
	o, err := f.Put(context.Background(), strings.NewReader("test"), nil)
	require.Nil(t, o)
	require.ErrorIs(t, err, errReadOnly)
}

func TestNewFsUsesDefaultURL(t *testing.T) {
	fsi, err := NewFs(context.Background(), "test", "", configmap.Simple{})
	require.NoError(t, err)

	f, ok := fsi.(*Fs)
	require.True(t, ok)
	require.Equal(t, "https://myrient.erista.me/files/", f.opt.URL)
}

func TestParseIndexRows_FiltersDotEntries(t *testing.T) {
	htmlPage := `
	<table id="list"><tbody>
	<tr><td class="link"><a href="../">Parent directory/</a></td><td class="size">-</td><td class="date">-</td></tr>
	<tr><td class="link"><a href="./">./</a></td><td class="size">-</td><td class="date">02-Dec-2025 13:43</td></tr>
	<tr><td class="link"><a href="../">../</a></td><td class="size">-</td><td class="date">02-Dec-2025 13:43</td></tr>
	<tr><td class="link"><a href=".">.</a></td><td class="size">-</td><td class="date">02-Dec-2025 13:43</td></tr>
	<tr><td class="link"><a href="..">..</a></td><td class="size">-</td><td class="date">02-Dec-2025 13:43</td></tr>
	<tr><td class="link"><a href="SpiderZSoft%20locked%20bootlegs/">SpiderZSoft locked bootlegs/</a></td><td class="size">-</td><td class="date">02-Dec-2025 13:43</td></tr>
	<tr><td class="link"><a href="Crossy%20Road.zip">Crossy Road.zip</a></td><td class="size">2.0 MiB</td><td class="date">01-Dec-2025 01:15</td></tr>
	</tbody></table>`

	rows, err := parseIndexRows(strings.NewReader(htmlPage))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "SpiderZSoft%20locked%20bootlegs/", rows[0].href)
	assert.Equal(t, "SpiderZSoft locked bootlegs/", rows[0].name)
	assert.True(t, rows[0].isDir)
	assert.Equal(t, "-", rows[0].sizeText)
	assert.Equal(t, "02-Dec-2025 13:43", rows[0].dateText)
	assert.Equal(t, "Crossy%20Road.zip", rows[1].href)
	assert.Equal(t, "Crossy Road.zip", rows[1].name)
	assert.False(t, rows[1].isDir)
	assert.Equal(t, "2.0 MiB", rows[1].sizeText)
	assert.Equal(t, "01-Dec-2025 01:15", rows[1].dateText)
}

func TestParseIndexRows_ReturnsErrorWhenListTableMissing(t *testing.T) {
	_, err := parseIndexRows(strings.NewReader(`<table id="other"><tbody><tr><td class="link"><a href="file">file</a></td></tr></tbody></table>`))
	require.ErrorIs(t, err, errIndexTableNotFound)
}

func TestParseIndexRows_ReturnsErrorWhenListTableBodyMissing(t *testing.T) {
	_, err := parseIndexRows(strings.NewReader(`<table id="list"></table>`))
	require.ErrorIs(t, err, errIndexTableBodyNotFound)
}

func TestResolveMetadata_HeadAndRangeFallback(t *testing.T) {
	ctx := context.Background()
	headModTime := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	rangeModTime := time.Date(2026, time.January, 4, 5, 6, 7, 0, time.UTC)
	indexModTime := time.Date(2025, time.December, 1, 1, 15, 0, 0, time.UTC)

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/head-complete.bin":
			require.Equal(t, http.MethodHead, r.Method)
			w.Header().Set("Content-Length", "41189392")
			w.Header().Set("Last-Modified", headModTime.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case "/range-size.bin":
			switch r.Method {
			case http.MethodHead:
				w.Header().Set("Last-Modified", headModTime.Format(http.TimeFormat))
				w.WriteHeader(http.StatusOK)
			case http.MethodGet:
				require.Equal(t, "bytes=0-0", r.Header.Get("Range"))
				w.Header().Set("Content-Range", "bytes 0-0/12345")
				w.WriteHeader(http.StatusPartialContent)
			default:
				t.Fatalf("unexpected method for range-size.bin: %s", r.Method)
			}
		case "/index-mtime.bin":
			require.Equal(t, http.MethodHead, r.Method)
			w.Header().Set("Content-Length", "777")
			w.WriteHeader(http.StatusOK)
		case "/range-invalid.bin":
			switch r.Method {
			case http.MethodHead:
				w.WriteHeader(http.StatusOK)
			case http.MethodGet:
				require.Equal(t, "bytes=0-0", r.Header.Get("Range"))
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Content-Range", "bytes 0-0/*")
				w.WriteHeader(http.StatusPartialContent)
			default:
				t.Fatalf("unexpected method for range-invalid.bin: %s", r.Method)
			}
		case "/head-fails-range.bin":
			switch r.Method {
			case http.MethodHead:
				w.WriteHeader(http.StatusMethodNotAllowed)
			case http.MethodGet:
				require.Equal(t, "bytes=0-0", r.Header.Get("Range"))
				w.Header().Set("Content-Range", "bytes 0-0/54321")
				w.Header().Set("Last-Modified", rangeModTime.Format(http.TimeFormat))
				w.WriteHeader(http.StatusPartialContent)
			default:
				t.Fatalf("unexpected method for head-fails-range.bin: %s", r.Method)
			}
		case "/missing-metadata.bin":
			switch r.Method {
			case http.MethodHead:
				w.WriteHeader(http.StatusOK)
			case http.MethodGet:
				require.Equal(t, "bytes=0-0", r.Header.Get("Range"))
				w.WriteHeader(http.StatusPartialContent)
			default:
				t.Fatalf("unexpected method for missing-metadata.bin: %s", r.Method)
			}
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
	}))
	defer testServer.Close()

	f := &Fs{
		opt: Options{URL: testServer.URL + "/"},
	}

	t.Run("head metadata is authoritative", func(t *testing.T) {
		o := &Object{
			fs:           f,
			remote:       "head-complete.bin",
			size:         -1,
			indexSize:    -1,
			indexModTime: indexModTime,
		}

		err := o.resolveMetadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(41189392), o.Size())
		assert.Equal(t, headModTime, o.ModTime(ctx))
	})

	t.Run("range probe fills missing size", func(t *testing.T) {
		o := &Object{
			fs:           f,
			remote:       "range-size.bin",
			size:         -1,
			indexSize:    -1,
			indexModTime: indexModTime,
		}

		err := o.resolveMetadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(12345), o.Size())
		assert.Equal(t, headModTime, o.ModTime(ctx))
	})

	t.Run("index date is mtime fallback", func(t *testing.T) {
		o := &Object{
			fs:           f,
			remote:       "index-mtime.bin",
			size:         -1,
			indexSize:    -1,
			indexModTime: indexModTime,
		}

		err := o.resolveMetadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(777), o.Size())
		assert.Equal(t, indexModTime, o.ModTime(ctx))
	})

	t.Run("range probe failure falls back to index metadata", func(t *testing.T) {
		o := &Object{
			fs:           f,
			remote:       "range-invalid.bin",
			size:         -1,
			indexSize:    4242,
			indexModTime: indexModTime,
		}

		err := o.resolveMetadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(4242), o.Size())
		assert.Equal(t, indexModTime, o.ModTime(ctx))
	})

	t.Run("head failure still allows range metadata fallback", func(t *testing.T) {
		o := &Object{
			fs:           f,
			remote:       "head-fails-range.bin",
			size:         -1,
			indexSize:    -1,
			indexModTime: indexModTime,
		}

		err := o.resolveMetadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(54321), o.Size())
		assert.Equal(t, rangeModTime, o.ModTime(ctx))
	})

	t.Run("fully missing metadata keeps unknown defaults", func(t *testing.T) {
		o := &Object{
			fs:        f,
			remote:    "missing-metadata.bin",
			size:      -1,
			indexSize: -1,
		}

		err := o.resolveMetadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), o.Size())
		assert.True(t, o.ModTime(ctx).IsZero())
	})
}

func TestListAndOpen_EndToEnd(t *testing.T) {
	ctx := context.Background()
	dirIndexTime := time.Date(2025, time.December, 2, 13, 43, 0, 0, time.UTC)
	fileHeadTime := time.Date(2026, time.January, 5, 6, 7, 8, 0, time.UTC)
	var rangeReachedDownload atomic.Bool

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			require.Equal(t, http.MethodGet, r.Method)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, err := io.WriteString(w, `<table id="list"><tbody>
			<tr><td class="link"><a href="../">Parent directory/</a></td><td class="size">-</td><td class="date">-</td></tr>
			<tr><td class="link"><a href="./">./</a></td><td class="size">-</td><td class="date">-</td></tr>
			<tr><td class="link"><a href="Arcade/">Arcade/</a></td><td class="size">-</td><td class="date">02-Dec-2025 13:43</td></tr>
			<tr><td class="link"><a href="Crossy%20Road.zip">Crossy Road.zip</a></td><td class="size">2.0 MiB</td><td class="date">01-Dec-2025 01:15</td></tr>
			</tbody></table>`)
			require.NoError(t, err)
		case "/Crossy Road.zip":
			http.Redirect(w, r, "/download/Crossy%20Road.zip", http.StatusFound)
		case "/download/Crossy Road.zip":
			switch r.Method {
			case http.MethodHead:
				w.Header().Set("Content-Length", "2146454")
				w.Header().Set("Last-Modified", fileHeadTime.Format(http.TimeFormat))
				w.WriteHeader(http.StatusOK)
			case http.MethodGet:
				require.Equal(t, "bytes=0-0", r.Header.Get("Range"))
				rangeReachedDownload.Store(true)
				w.Header().Set("Content-Range", "bytes 0-0/2146454")
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Last-Modified", fileHeadTime.Format(http.TimeFormat))
				w.WriteHeader(http.StatusPartialContent)
				_, err := w.Write([]byte("x"))
				require.NoError(t, err)
			default:
				t.Fatalf("unexpected method for download path: %s", r.Method)
			}
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
	}))
	defer testServer.Close()

	f := newTestFs(t, testServer.URL)

	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	var gotDir fs.Directory
	var gotFile fs.Object
	for _, entry := range entries {
		switch entry.Remote() {
		case "Arcade":
			gotDir, _ = entry.(fs.Directory)
		case "Crossy Road.zip":
			gotFile, _ = entry.(fs.Object)
		}
	}

	require.NotNil(t, gotDir)
	assert.Equal(t, dirIndexTime, gotDir.ModTime(ctx))
	require.NotNil(t, gotFile)
	assert.Equal(t, int64(2146454), gotFile.Size())

	obj, err := f.NewObject(ctx, "Crossy Road.zip")
	require.NoError(t, err)
	assert.Equal(t, int64(2146454), obj.Size())

	r, err := obj.Open(ctx, &fs.RangeOption{Start: 0, End: 0})
	require.NoError(t, err)
	defer func() {
		_ = r.Close()
	}()

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Len(t, b, 1)
	assert.True(t, rangeReachedDownload.Load())
	assert.Equal(t, int64(2146454), obj.Size())
	assert.Equal(t, fileHeadTime, obj.ModTime(ctx))
}

func TestParseContentRangeTotal(t *testing.T) {
	n, err := parseContentRangeTotal("bytes 0-0/41189392")
	require.NoError(t, err)
	assert.Equal(t, int64(41189392), n)

	_, err = parseContentRangeTotal("items 0-0/99")
	require.Error(t, err)

	_, err = parseContentRangeTotal("bytes 0-0/*")
	require.Error(t, err)

	_, err = parseContentRangeTotal("invalid")
	require.Error(t, err)
}
