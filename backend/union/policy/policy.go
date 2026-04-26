package policy

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
)

var policies = make(map[string]Policy)

// Policy is the interface of a set of defined behavior choosing
// the upstream Fs to operate on
type Policy interface {
	// Action category policy, governing the modification of files and directories
	Action(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error)

	// Create category policy, governing the creation of files and directories
	Create(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error)

	// Search category policy, governing the access to files and directories
	Search(ctx context.Context, upstreams []*upstream.Fs, path string) (*upstream.Fs, error)

	// ActionEntries is ACTION category policy but receiving a set of candidate entries
	ActionEntries(entries ...upstream.Entry) ([]upstream.Entry, error)

	// CreateEntries is CREATE category policy but receiving a set of candidate entries
	CreateEntries(entries ...upstream.Entry) ([]upstream.Entry, error)

	// SearchEntries is SEARCH category policy but receiving a set of candidate entries
	SearchEntries(entries ...upstream.Entry) (upstream.Entry, error)
}

func registerPolicy(name string, p Policy) {
	policies[strings.ToLower(name)] = p
}

// Get a Policy from the list
func Get(name string) (Policy, error) {
	p, ok := policies[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("didn't find policy called %q", name)
	}
	return p, nil
}

func filterRO(ufs []*upstream.Fs) (wufs []*upstream.Fs) {
	for _, u := range ufs {
		if u.IsWritable() {
			wufs = append(wufs, u)
		}
	}
	return wufs
}

func filterROEntries(ue []upstream.Entry) (wue []upstream.Entry) {
	for _, e := range ue {
		if e.UpstreamFs().IsWritable() {
			wue = append(wue, e)
		}
	}
	return wue
}

func filterNC(ufs []*upstream.Fs) (wufs []*upstream.Fs) {
	for _, u := range ufs {
		if u.IsCreatable() {
			wufs = append(wufs, u)
		}
	}
	return wufs
}

func filterNCEntries(ue []upstream.Entry) (wue []upstream.Entry) {
	for _, e := range ue {
		if e.UpstreamFs().IsCreatable() {
			wue = append(wue, e)
		}
	}
	return wue
}

func parentDir(absPath string) string {
	parent := path.Dir(strings.TrimRight(absPath, "/"))
	if parent == "." {
		parent = ""
	}
	return parent
}

func clean(absPath string) string {
	cleanPath := path.Clean(absPath)
	if cleanPath == "." {
		cleanPath = ""
	}
	return cleanPath
}

type probeResult struct {
	entry fs.DirEntry
	found bool
}

func shouldFailClosedOnProbeError(u *upstream.Fs, err error) bool {
	return err != nil && u != nil && u.Opt != nil && u.Opt.StrictReads
}

func findEntry(ctx context.Context, f fs.Fs, remote string) (probeResult, error) {
	remote = clean(remote)
	dir := parentDir(remote)
	entries, err := f.List(ctx, dir)
	if remote == dir {
		if err != nil {
			if len(entries) == 0 && errors.Is(err, fs.ErrorDirNotFound) {
				return probeResult{}, nil
			}
			return probeResult{}, err
		}
		return probeResult{entry: fs.NewDir("", time.Time{}), found: true}, nil
	}
	var result probeResult
	for _, e := range entries {
		eRemote := e.Remote()
		found := false
		if f.Features().CaseInsensitive {
			found = strings.EqualFold(remote, eRemote)
		} else {
			found = (remote == eRemote)
		}
		if found {
			result = probeResult{entry: e, found: true}
			break
		}
	}
	if err != nil {
		if len(entries) == 0 && !result.found && errors.Is(err, fs.ErrorDirNotFound) {
			return probeResult{}, nil
		}
		return result, err
	}
	return result, nil
}
