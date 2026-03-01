// Package myrient provides a read-only filesystem for Myrient indexes.
package myrient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/net/html"
)

var (
	errReadOnly               = errors.New("myrient backend is read only")
	errIndexTableNotFound     = errors.New("myrient index table not found")
	errIndexTableBodyNotFound = errors.New("myrient index table body not found")
)

const defaultURL = "https://myrient.erista.me/files/"

const myrientIndexDateLayout = "02-Jan-2006 15:04"

// Options defines the configuration for this backend.
type Options struct {
	URL string `config:"url"`
}

// Fs stores the interface to the remote Myrient files.
type Fs struct {
	name       string
	root       string
	opt        Options
	features   *fs.Features
	baseURL    *url.URL
	httpClient *http.Client
}

// Object describes a remote file and its resolved metadata.
type Object struct {
	fs           *Fs
	remote       string
	size         int64
	modTime      time.Time
	indexSize    int64
	indexModTime time.Time
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "myrient",
		Description: "Myrient (read-only)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:    "url",
			Help:    "Base URL for Myrient index",
			Default: defaultURL,
		}},
	})
}

// NewFs creates a new Fs object from the name and root.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.URL == "" {
		opt.URL = defaultURL
	}
	if !strings.HasSuffix(opt.URL, "/") {
		opt.URL += "/"
	}
	baseURL, err := url.Parse(opt.URL)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:       name,
		root:       root,
		opt:        *opt,
		baseURL:    baseURL,
		httpClient: fshttp.NewClient(ctx),
	}
	f.features = (&fs.Features{CanHaveEmptyDirectories: true}).Fill(ctx, f)
	return f, nil
}

// Name returns the configured name of the file system.
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root for the filesystem.
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the filesystem.
func (f *Fs) String() string {
	return "Myrient root '" + f.root + "'"
}

// Features returns the optional features of this Fs.
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision is the remote file system's modtime precision.
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns hash.HashNone to indicate remote hashing is unavailable.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Size returns the size in bytes of the remote file.
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the remote file.
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Fs returns the parent filesystem for this object.
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns a printable identifier for this object.
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the object path relative to fs root.
func (o *Object) Remote() string {
	return o.remote
}

// Hash reports that hashing is unsupported for this backend.
func (o *Object) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (f *Fs) resolvedBaseURL() (*url.URL, error) {
	if f.baseURL != nil {
		return f.baseURL, nil
	}
	rawURL := f.opt.URL
	if rawURL == "" {
		rawURL = defaultURL
	}
	if !strings.HasSuffix(rawURL, "/") {
		rawURL += "/"
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	return parsedURL, nil
}

func (o *Object) url() (string, error) {
	if o.fs == nil {
		return "", errors.New("missing filesystem reference")
	}
	baseURL, err := o.fs.resolvedBaseURL()
	if err != nil {
		return "", err
	}
	remotePath := path.Join(o.fs.root, o.remote)
	if remotePath == "." {
		remotePath = ""
	}
	joined, err := rest.URLJoin(baseURL, rest.URLPathEscape(remotePath))
	if err != nil {
		return "", err
	}
	return joined.String(), nil
}

func (f *Fs) dirURL(dir string) (string, error) {
	baseURL, err := f.resolvedBaseURL()
	if err != nil {
		return "", err
	}
	remotePath := path.Join(f.root, dir)
	if remotePath == "." {
		remotePath = ""
	}
	if remotePath != "" && !strings.HasSuffix(remotePath, "/") {
		remotePath += "/"
	}
	joined, err := rest.URLJoin(baseURL, rest.URLPathEscape(remotePath))
	if err != nil {
		return "", err
	}
	return joined.String(), nil
}

func (f *Fs) readDirRows(ctx context.Context, dir string) ([]indexRow, error) {
	dirURL, err := f.dirURL(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to build directory URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dirURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create list request: %w", err)
	}
	client := f.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list %q: %w", dir, err)
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode == http.StatusNotFound {
		return nil, fs.ErrorDirNotFound
	}
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("failed to list %q: %s", dir, res.Status)
	}
	rows, err := parseIndexRows(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse directory listing at %q: %w", dirURL, err)
	}
	for i := range rows {
		rows[i].parsedSize = -1

		size, err := parseIndexSize(rows[i].sizeText)
		if err != nil {
			fs.Debugf(f, "failed to parse index size %q for %q: %v", rows[i].sizeText, rows[i].name, err)
		} else {
			rows[i].parsedSize = size
		}

		date, err := parseIndexDate(rows[i].dateText)
		if err != nil {
			fs.Debugf(f, "failed to parse index mtime %q for %q: %v", rows[i].dateText, rows[i].name, err)
		} else {
			rows[i].parsedDate = date
		}
	}
	return rows, nil
}

func parseIndexSize(sizeText string) (int64, error) {
	sizeText = strings.TrimSpace(sizeText)
	if sizeText == "" || sizeText == "-" {
		return -1, nil
	}
	if n, err := strconv.ParseInt(strings.ReplaceAll(sizeText, ",", ""), 10, 64); err == nil {
		if n < 0 {
			return -1, fmt.Errorf("size can't be negative %q", sizeText)
		}
		return n, nil
	}
	compact := strings.ReplaceAll(sizeText, " ", "")
	var parsed fs.SizeSuffix
	if err := parsed.Set(compact); err != nil {
		return -1, err
	}
	return int64(parsed), nil
}

func parseIndexDate(dateText string) (time.Time, error) {
	dateText = strings.TrimSpace(dateText)
	if dateText == "" || dateText == "-" {
		return time.Time{}, nil
	}
	t, err := time.ParseInLocation(myrientIndexDateLayout, dateText, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func (o *Object) resolveMetadata(ctx context.Context) error {
	if err := o.head(ctx); err != nil {
		fs.Debugf(o, "failed to resolve metadata via HEAD: %v", err)
	}

	if o.size < 0 {
		n, err := o.sizeFromRangeProbe(ctx)
		if err == nil {
			o.size = n
		} else {
			fs.Debugf(o, "failed to resolve metadata via range probe: %v", err)
		}
	}

	if o.modTime.IsZero() && !o.indexModTime.IsZero() {
		o.modTime = o.indexModTime
	}
	if o.size < 0 && o.indexSize >= 0 {
		o.size = o.indexSize
	}
	return nil
}

func (o *Object) head(ctx context.Context) error {
	objectURL, err := o.url()
	if err != nil {
		return fmt.Errorf("failed to build object URL for HEAD: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, objectURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create HEAD request: %w", err)
	}
	res, err := o.client().Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		if res.StatusCode == http.StatusNotFound {
			return fs.ErrorObjectNotFound
		}
		return fmt.Errorf("HEAD failed: %s", res.Status)
	}
	o.decodeMetadataFromHeaders(res.Header)
	return nil
}

func (o *Object) sizeFromRangeProbe(ctx context.Context) (int64, error) {
	objectURL, err := o.url()
	if err != nil {
		return -1, fmt.Errorf("failed to build object URL for range probe: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
	if err != nil {
		return -1, fmt.Errorf("failed to create range probe request: %w", err)
	}
	req.Header.Set("Range", "bytes=0-0")
	res, err := o.client().Do(req)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return -1, fmt.Errorf("range probe failed: %s", res.Status)
	}
	if lastModified := res.Header.Get("Last-Modified"); lastModified != "" && o.modTime.IsZero() {
		modTime, err := http.ParseTime(lastModified)
		if err == nil {
			o.modTime = modTime
		}
	}
	return parseContentRangeTotal(res.Header.Get("Content-Range"))
}

func (o *Object) decodeMetadataFromHeaders(headers http.Header) {
	if size, err := parseContentRangeTotal(headers.Get("Content-Range")); err == nil {
		o.size = size
	} else if contentLength := headers.Get("Content-Length"); contentLength != "" {
		size, err := strconv.ParseInt(contentLength, 10, 64)
		if err == nil {
			o.size = size
		}
	}
	if lastModified := headers.Get("Last-Modified"); lastModified != "" {
		modTime, err := http.ParseTime(lastModified)
		if err == nil {
			o.modTime = modTime
		}
	}
}

func (o *Object) client() *http.Client {
	if o.fs != nil && o.fs.httpClient != nil {
		return o.fs.httpClient
	}
	return http.DefaultClient
}

func parseContentRangeTotal(v string) (int64, error) {
	v = strings.TrimSpace(v)
	parts := strings.Split(v, "/")
	if len(parts) != 2 {
		return -1, fmt.Errorf("invalid content-range %q", v)
	}
	prefix := strings.TrimSpace(parts[0])
	if !strings.HasPrefix(strings.ToLower(prefix), "bytes ") {
		return -1, fmt.Errorf("invalid content-range unit %q", v)
	}
	if strings.TrimSpace(prefix[len("bytes "):]) == "" {
		return -1, fmt.Errorf("invalid content-range range %q", v)
	}
	total := strings.TrimSpace(parts[1])
	if total == "" || total == "*" {
		return -1, fmt.Errorf("invalid content-range total %q", v)
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("invalid content-range total %q: %w", v, err)
	}
	if n < 0 {
		return -1, fmt.Errorf("invalid content-range total %q", v)
	}
	return n, nil
}

type indexRow struct {
	href       string
	name       string
	isDir      bool
	sizeText   string
	dateText   string
	parsedSize int64
	parsedDate time.Time
}

func shouldSkipRow(href, name string) bool {
	name = strings.TrimSpace(name)
	href = strings.TrimSpace(href)
	if name == "Parent directory/" || name == "./" || name == "../" || name == "." || name == ".." {
		return true
	}
	return href == "./" || href == "../" || href == "." || href == ".."
}

func parseIndexRows(in io.Reader) ([]indexRow, error) {
	doc, err := html.Parse(in)
	if err != nil {
		return nil, err
	}
	table := findListTable(doc)
	if table == nil {
		return nil, errIndexTableNotFound
	}
	tbody := firstChildElement(table, "tbody")
	if tbody == nil {
		return nil, errIndexTableBodyNotFound
	}
	rows := make([]indexRow, 0)
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		if tr.Type != html.ElementNode || tr.Data != "tr" {
			continue
		}
		row, ok := parseIndexRow(tr)
		if !ok || shouldSkipRow(row.href, row.name) {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseIndexRow(tr *html.Node) (indexRow, bool) {
	link := firstDescendantElement(tr, "a")
	if link == nil {
		return indexRow{}, false
	}
	href := strings.TrimSpace(attrValue(link, "href"))
	name := strings.TrimSpace(nodeText(link))
	if href == "" && name == "" {
		return indexRow{}, false
	}
	row := indexRow{
		href: href,
		name: name,
	}
	if sizeNode := childElementByClass(tr, "td", "size"); sizeNode != nil {
		row.sizeText = strings.TrimSpace(nodeText(sizeNode))
	}
	if dateNode := childElementByClass(tr, "td", "date"); dateNode != nil {
		row.dateText = strings.TrimSpace(nodeText(dateNode))
	}
	row.isDir = strings.HasSuffix(row.href, "/") || strings.HasSuffix(row.name, "/")
	return row, true
}

func findListTable(n *html.Node) *html.Node {
	if n == nil {
		return nil
	}
	if n.Type == html.ElementNode && n.Data == "table" && attrValue(n, "id") == "list" {
		return n
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if got := findListTable(c); got != nil {
			return got
		}
	}
	return nil
}

func firstChildElement(n *html.Node, tag string) *html.Node {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == tag {
			return c
		}
	}
	return nil
}

func childElementByClass(n *html.Node, tag, class string) *html.Node {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type != html.ElementNode || c.Data != tag {
			continue
		}
		if hasClass(c, class) {
			return c
		}
	}
	return nil
}

func hasClass(n *html.Node, want string) bool {
	for _, a := range n.Attr {
		if a.Key != "class" {
			continue
		}
		for _, cls := range strings.Fields(a.Val) {
			if cls == want {
				return true
			}
		}
	}
	return false
}

func firstDescendantElement(n *html.Node, tag string) *html.Node {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == tag {
			return c
		}
		if got := firstDescendantElement(c, tag); got != nil {
			return got
		}
	}
	return nil
}

func attrValue(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func nodeText(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(cur *html.Node) {
		if cur.Type == html.TextNode {
			b.WriteString(cur.Data)
		}
		for c := cur.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return b.String()
}

func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	rows, err := f.readDirRows(ctx, dir)
	if err != nil {
		return nil, err
	}
	entries := make(fs.DirEntries, 0, len(rows))
	for _, row := range rows {
		name := strings.TrimSuffix(row.name, "/")
		remote := path.Join(dir, name)
		if row.isDir {
			entries = append(entries, fs.NewDir(remote, row.parsedDate))
			continue
		}
		o := &Object{
			fs:           f,
			remote:       remote,
			size:         -1,
			indexSize:    row.parsedSize,
			indexModTime: row.parsedDate,
		}
		if err := o.resolveMetadata(ctx); err != nil {
			fs.Debugf(o, "failed to resolve metadata while listing: %v", err)
		}
		entries = append(entries, o)
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return nil, fs.ErrorObjectNotFound
	}
	dir, base := path.Split(remote)
	dir = strings.TrimSuffix(dir, "/")
	if base == "" {
		return nil, fs.ErrorObjectNotFound
	}
	rows, err := f.readDirRows(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	for _, row := range rows {
		if row.isDir {
			continue
		}
		name := strings.TrimSuffix(row.name, "/")
		if name != base {
			continue
		}
		o := &Object{
			fs:           f,
			remote:       path.Join(dir, name),
			size:         -1,
			indexSize:    row.parsedSize,
			indexModTime: row.parsedDate,
		}
		if err := o.resolveMetadata(ctx); err != nil {
			fs.Debugf(o, "failed to resolve metadata for object lookup: %v", err)
		}
		return o, nil
	}
	return nil, fs.ErrorObjectNotFound
}

// SetModTime sets the modification time for this object.
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return errReadOnly
}

// Storable indicates this object can be copied.
func (o *Object) Storable() bool {
	return true
}

// Open opens this object for reading.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	objectURL, err := o.url()
	if err != nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
	if err != nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}
	for key, value := range fs.OpenOptionHeaders(options) {
		req.Header.Add(key, value)
	}
	res, err := o.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		_ = res.Body.Close()
		if res.StatusCode == http.StatusNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, fmt.Errorf("open failed: %s", res.Status)
	}
	o.decodeMetadataFromHeaders(res.Header)
	return res.Body, nil
}

// Remove deletes this object.
func (o *Object) Remove(ctx context.Context) error {
	return errReadOnly
}

// Update writes to this object.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errReadOnly
}

// Put in to the remote path with the modTime given of the given size.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errReadOnly
}

// Mkdir makes the root directory of the Fs object.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return errReadOnly
}

// Rmdir removes the root directory of the Fs object.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errReadOnly
}

// Check the interfaces are satisfied.
var _ fs.Fs = &Fs{}
var _ fs.Object = &Object{}
