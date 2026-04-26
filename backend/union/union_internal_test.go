package union

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/union/common"
	"github.com/rclone/rclone/backend/union/policy"
	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/rclone/rclone/lib/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MakeTestDirs makes directories in /tmp for testing
func MakeTestDirs(t *testing.T, n int) (dirs []string) {
	for i := 1; i <= n; i++ {
		dir := t.TempDir()
		dirs = append(dirs, dir)
	}
	return dirs
}

type scriptedListResult struct {
	entries fs.DirEntries
	err     error
}

type scriptedObjectResult struct {
	obj fs.Object
	err error
}

type scriptedListRResult struct {
	tranches []fs.DirEntries
	err      error
}

// scriptedFs keeps strict-read regressions focused on union aggregation behavior.
type scriptedFs struct {
	*mockfs.Fs
	features      *fs.Features
	listResults   map[string]scriptedListResult
	listHook      func(context.Context, string) (fs.DirEntries, error)
	objectResults map[string]scriptedObjectResult
	listRResults  map[string]scriptedListRResult
}

func newScriptedFs(t *testing.T, name string) *scriptedFs {
	t.Helper()

	base, err := mockfs.NewFs(context.Background(), name, "", nil)
	require.NoError(t, err)

	f := &scriptedFs{
		Fs:            base.(*mockfs.Fs),
		listResults:   make(map[string]scriptedListResult),
		objectResults: make(map[string]scriptedObjectResult),
		listRResults:  make(map[string]scriptedListRResult),
	}
	f.features = (&fs.Features{}).Fill(context.Background(), f)
	return f
}

func (f *scriptedFs) Features() *fs.Features {
	return f.features
}

func (f *scriptedFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if f.listHook != nil {
		return f.listHook(ctx, dir)
	}
	if result, ok := f.listResults[dir]; ok {
		return result.entries, result.err
	}
	return f.Fs.List(ctx, dir)
}

func (f *scriptedFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if result, ok := f.objectResults[remote]; ok {
		return result.obj, result.err
	}
	return f.Fs.NewObject(ctx, remote)
}

func (f *scriptedFs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	result, ok := f.listRResults[dir]
	if !ok {
		return fs.ErrorDirNotFound
	}
	for _, tranche := range result.tranches {
		if err := callback(tranche); err != nil {
			return err
		}
	}
	return result.err
}

func newStrictReadTestUpstreams(t *testing.T, strictReads bool, remotes ...*scriptedFs) []*upstream.Fs {
	t.Helper()

	opt := &common.Options{
		ActionPolicy: "epall",
		CreatePolicy: "epmfs",
		SearchPolicy: "ff",
		StrictReads:  strictReads,
	}

	upstreams := make([]*upstream.Fs, len(remotes))
	for i, remote := range remotes {
		u, err := upstream.New(context.Background(), t.TempDir(), "", opt)
		require.NoError(t, err)
		u.Fs = remote
		u.RootFs = remote
		u.RootPath = ""
		upstreams[i] = u
	}

	return upstreams
}

func newStrictReadTestUnion(t *testing.T, strictReads bool, remotes ...*scriptedFs) *Fs {
	t.Helper()

	opt := common.Options{
		ActionPolicy: "epall",
		CreatePolicy: "epmfs",
		SearchPolicy: "ff",
		StrictReads:  strictReads,
	}
	searchPolicy, err := policy.Get(opt.SearchPolicy)
	require.NoError(t, err)

	upstreams := newStrictReadTestUpstreams(t, strictReads, remotes...)

	return &Fs{
		name:         "test-union",
		opt:          opt,
		features:     &fs.Features{},
		upstreams:    upstreams,
		searchPolicy: searchPolicy,
	}
}

func newTestObject(remote string) fs.Object {
	return mockobject.New(remote).WithContent([]byte("test-data"), mockobject.SeekModeNone)
}

func collectListR(ctx context.Context, f *Fs, dir string) (fs.DirEntries, error) {
	var entries fs.DirEntries
	err := f.ListR(ctx, dir, func(tranche fs.DirEntries) error {
		entries = append(entries, tranche...)
		return nil
	})
	return entries, err
}

func TestStrictReadsPolicyProbeDoesNotTreatFailureAsAbsent(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Unix(1_700_000_000, 0)

	type policyInvoke func(context.Context, policy.Policy, []*upstream.Fs, string) ([]*upstream.Fs, error)

	searchInvoke := func(ctx context.Context, p policy.Policy, upstreams []*upstream.Fs, remote string) ([]*upstream.Fs, error) {
		u, err := p.Search(ctx, upstreams, remote)
		if u == nil {
			return nil, err
		}
		return []*upstream.Fs{u}, err
	}

	rows := []struct {
		name       string
		policyName string
		remote     string
		invoke     policyInvoke
		setup      func(t *testing.T) ([]*scriptedFs, *scriptedFs, error)
	}{
		{
			name:       "search first-found treats probe failure as absent only when non-strict",
			policyName: "epff",
			remote:     "target.txt",
			invoke:     searchInvoke,
			setup: func(t *testing.T) ([]*scriptedFs, *scriptedFs, error) {
				boom := errors.New("search probe failed")
				absent := newScriptedFs(t, "search-absent")
				absent.listResults[""] = scriptedListResult{entries: fs.DirEntries{}}

				failing := newScriptedFs(t, "search-failing")
				failing.listResults[""] = scriptedListResult{err: boom}

				found := newScriptedFs(t, "search-found")
				found.listResults[""] = scriptedListResult{entries: fs.DirEntries{newTestObject("target.txt")}}

				return []*scriptedFs{absent, failing, found}, found, boom
			},
		},
		{
			name:       "action existing-path does not route through failed probe in strict mode",
			policyName: "epall",
			remote:     "target.txt",
			invoke: func(ctx context.Context, p policy.Policy, upstreams []*upstream.Fs, remote string) ([]*upstream.Fs, error) {
				return p.Action(ctx, upstreams, remote)
			},
			setup: func(t *testing.T) ([]*scriptedFs, *scriptedFs, error) {
				boom := errors.New("action probe failed")
				absent := newScriptedFs(t, "action-absent")
				absent.listResults[""] = scriptedListResult{entries: fs.DirEntries{}}

				failing := newScriptedFs(t, "action-failing")
				failing.listResults[""] = scriptedListResult{err: boom}

				found := newScriptedFs(t, "action-found")
				found.listResults[""] = scriptedListResult{entries: fs.DirEntries{newTestObject("target.txt")}}

				return []*scriptedFs{absent, failing, found}, found, boom
			},
		},
		{
			name:       "create parent-path probe does not collapse failure into absence in strict mode",
			policyName: "epff",
			remote:     "parent/child.txt",
			invoke: func(ctx context.Context, p policy.Policy, upstreams []*upstream.Fs, remote string) ([]*upstream.Fs, error) {
				return p.Create(ctx, upstreams, remote)
			},
			setup: func(t *testing.T) ([]*scriptedFs, *scriptedFs, error) {
				boom := errors.New("parent probe failed")
				absent := newScriptedFs(t, "parent-absent")
				absent.listResults[""] = scriptedListResult{entries: fs.DirEntries{}}

				failing := newScriptedFs(t, "parent-failing")
				failing.listResults[""] = scriptedListResult{err: boom}

				found := newScriptedFs(t, "parent-found")
				found.listResults[""] = scriptedListResult{entries: fs.DirEntries{fs.NewDir("parent", baseTime)}}

				return []*scriptedFs{absent, failing, found}, found, boom
			},
		},
		{
			name:       "newest selection does not treat failed probe as absent in strict mode",
			policyName: "newest",
			remote:     "target.txt",
			invoke:     searchInvoke,
			setup: func(t *testing.T) ([]*scriptedFs, *scriptedFs, error) {
				boom := errors.New("newest probe failed")
				absent := newScriptedFs(t, "newest-absent")
				absent.listResults[""] = scriptedListResult{entries: fs.DirEntries{}}

				failing := newScriptedFs(t, "newest-failing")
				failing.listResults[""] = scriptedListResult{err: boom}

				older := newScriptedFs(t, "newest-older")
				older.listResults[""] = scriptedListResult{entries: fs.DirEntries{fs.NewDir("target.txt", baseTime)}}

				newestFound := newScriptedFs(t, "newest-found")
				newestFound.listResults[""] = scriptedListResult{entries: fs.DirEntries{fs.NewDir("target.txt", baseTime.Add(time.Minute))}}

				return []*scriptedFs{absent, failing, older, newestFound}, newestFound, boom
			},
		},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			p, err := policy.Get(tc.policyName)
			require.NoError(t, err)

			remotes, expected, boom := tc.setup(t)

			selectedNonStrict, errNonStrict := tc.invoke(ctx, p, newStrictReadTestUpstreams(t, false, remotes...), tc.remote)
			require.NoError(t, errNonStrict)
			require.Len(t, selectedNonStrict, 1)
			assert.Same(t, expected, selectedNonStrict[0].RootFs)

			_, errStrict := tc.invoke(ctx, p, newStrictReadTestUpstreams(t, true, remotes...), tc.remote)
			require.Error(t, errStrict)
			assert.ErrorIs(t, errStrict, boom)
			assert.False(t, errors.Is(errStrict, fs.ErrorObjectNotFound))
		})
	}

	t.Run("search first-found preserves original probe failure over cancellation artifact", func(t *testing.T) {
		const attempts = 256
		const cancelAwareProbeCount = 64

		originalMaxProcs := runtime.GOMAXPROCS(0)
		if originalMaxProcs < 4 {
			runtime.GOMAXPROCS(4)
			defer runtime.GOMAXPROCS(originalMaxProcs)
		}

		p, err := policy.Get("epff")
		require.NoError(t, err)

		for attempt := 0; attempt < attempts; attempt++ {
			boom := errors.New("search probe failed with context-aware sibling")
			var started sync.WaitGroup
			started.Add(cancelAwareProbeCount)

			remotes := make([]*scriptedFs, 0, cancelAwareProbeCount+1)

			failing := newScriptedFs(t, fmt.Sprintf("search-failing-%d", attempt))
			failing.listHook = func(ctx context.Context, dir string) (fs.DirEntries, error) {
				started.Wait()
				return nil, boom
			}
			remotes = append(remotes, failing)

			for i := 0; i < cancelAwareProbeCount; i++ {
				cancelAware := newScriptedFs(t, fmt.Sprintf("search-cancel-aware-%d-%d", attempt, i))
				cancelAware.listHook = func(ctx context.Context, dir string) (fs.DirEntries, error) {
					started.Done()
					<-ctx.Done()
					return nil, ctx.Err()
				}
				remotes = append(remotes, cancelAware)
			}

			_, errStrict := searchInvoke(ctx, p, newStrictReadTestUpstreams(t, true, remotes...), "target.txt")
			require.Error(t, errStrict)
			if errors.Is(errStrict, context.Canceled) {
				t.Fatalf("strict epff returned cancellation artifact on attempt %d: %v", attempt, errStrict)
			}
			assert.ErrorIs(t, errStrict, boom)
		}
	})
}

func TestStrictReadsPolicyProbeTreatsCleanDirNotFoundAsAbsence(t *testing.T) {
	ctx := context.Background()

	t.Run("search clean dir-not-found absence stays non-fatal in strict mode", func(t *testing.T) {
		p, err := policy.Get("epff")
		require.NoError(t, err)

		missingA := newScriptedFs(t, "search-missing-a")
		missingA.listResults[""] = scriptedListResult{err: fs.ErrorDirNotFound}

		missingB := newScriptedFs(t, "search-missing-b")
		missingB.listResults[""] = scriptedListResult{err: fmt.Errorf("wrapped miss: %w", fs.ErrorDirNotFound)}

		_, errNonStrict := p.Search(ctx, newStrictReadTestUpstreams(t, false, missingA, missingB), "target.txt")
		require.Error(t, errNonStrict)
		assert.ErrorIs(t, errNonStrict, fs.ErrorObjectNotFound)
		assert.False(t, errors.Is(errNonStrict, fs.ErrorDirNotFound))

		_, errStrict := p.Search(ctx, newStrictReadTestUpstreams(t, true, missingA, missingB), "target.txt")
		require.Error(t, errStrict)
		assert.ErrorIs(t, errStrict, fs.ErrorObjectNotFound)
		assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
	})

	t.Run("create parent probe clean dir-not-found absence stays non-fatal in strict mode", func(t *testing.T) {
		p, err := policy.Get("epff")
		require.NoError(t, err)

		missingA := newScriptedFs(t, "create-missing-a")
		missingA.listResults[""] = scriptedListResult{err: fs.ErrorDirNotFound}

		missingB := newScriptedFs(t, "create-missing-b")
		missingB.listResults[""] = scriptedListResult{err: fmt.Errorf("wrapped miss: %w", fs.ErrorDirNotFound)}

		_, errNonStrict := p.Create(ctx, newStrictReadTestUpstreams(t, false, missingA, missingB), "child.txt")
		require.Error(t, errNonStrict)
		assert.ErrorIs(t, errNonStrict, fs.ErrorObjectNotFound)
		assert.False(t, errors.Is(errNonStrict, fs.ErrorDirNotFound))

		_, errStrict := p.Create(ctx, newStrictReadTestUpstreams(t, true, missingA, missingB), "child.txt")
		require.Error(t, errStrict)
		assert.ErrorIs(t, errStrict, fs.ErrorObjectNotFound)
		assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
	})
}

func TestStrictReadsListFailsClosedOnPartialError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("listing failed")

	successful := newScriptedFs(t, "healthy")
	successful.listResults[""] = scriptedListResult{entries: fs.DirEntries{newTestObject("healthy.txt")}}

	failing := newScriptedFs(t, "failing")
	failing.listResults[""] = scriptedListResult{err: boom}

	entriesNonStrict, errNonStrict := newStrictReadTestUnion(t, false, successful, failing).List(ctx, "")
	assert.NoError(t, errNonStrict)
	assert.Len(t, entriesNonStrict, 1)

	entriesStrict, errStrict := newStrictReadTestUnion(t, true, successful, failing).List(ctx, "")
	require.Error(t, errStrict)
	assert.Nil(t, entriesStrict)
	assert.ErrorIs(t, errStrict, boom)
	assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
}

func TestStrictReadsListDoesNotClassifyMixedFailureAsDirNotFound(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("listing failed")

	missing := newScriptedFs(t, "missing")
	missing.listResults[""] = scriptedListResult{err: fs.ErrorDirNotFound}

	failing := newScriptedFs(t, "failing")
	failing.listResults[""] = scriptedListResult{err: boom}

	_, errNonStrict := newStrictReadTestUnion(t, false, missing, failing).List(ctx, "")
	require.Error(t, errNonStrict)
	assert.ErrorIs(t, errNonStrict, boom)

	entriesStrict, errStrict := newStrictReadTestUnion(t, true, missing, failing).List(ctx, "")
	require.Error(t, errStrict)
	assert.Nil(t, entriesStrict)
	assert.ErrorIs(t, errStrict, boom)
	assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
}

func TestStrictReadsNewObjectDoesNotCollapseErrorIntoNotFound(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("lookup failed")

	missing := newScriptedFs(t, "missing")
	missing.objectResults["target.txt"] = scriptedObjectResult{err: fs.ErrorObjectNotFound}

	failing := newScriptedFs(t, "failing")
	failing.objectResults["target.txt"] = scriptedObjectResult{err: boom}

	objNonStrict, errNonStrict := newStrictReadTestUnion(t, false, missing, failing).NewObject(ctx, "target.txt")
	assert.Nil(t, objNonStrict)
	assert.True(t, errors.Is(errNonStrict, fs.ErrorObjectNotFound))

	objStrict, errStrict := newStrictReadTestUnion(t, true, missing, failing).NewObject(ctx, "target.txt")
	assert.Nil(t, objStrict)
	require.Error(t, errStrict)
	assert.False(t, errors.Is(errStrict, fs.ErrorObjectNotFound))
	assert.ErrorIs(t, errStrict, boom)
}

func TestStrictReadsNonTraverseLookupFailsClosed(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("lookup failed")

	missing := newScriptedFs(t, "missing")
	missing.objectResults["target.txt"] = scriptedObjectResult{err: fs.ErrorObjectNotFound}

	failing := newScriptedFs(t, "failing")
	failing.objectResults["target.txt"] = scriptedObjectResult{err: boom}

	obj, err := newStrictReadTestUnion(t, true, missing, failing).NewObject(ctx, "target.txt")
	assert.Nil(t, obj)
	require.Error(t, err)
	assert.False(t, errors.Is(err, fs.ErrorObjectNotFound))
	assert.ErrorIs(t, err, boom)
}

func TestStrictReadsNewObjectDoesNotTreatWrappedNotFoundAsFailure(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("lookup failed")

	missing := newScriptedFs(t, "missing")
	missing.objectResults["target.txt"] = scriptedObjectResult{err: fmt.Errorf("wrapped miss: %w", fs.ErrorObjectNotFound)}

	failing := newScriptedFs(t, "failing")
	failing.objectResults["target.txt"] = scriptedObjectResult{err: boom}

	objNonStrict, errNonStrict := newStrictReadTestUnion(t, false, missing, failing).NewObject(ctx, "target.txt")
	assert.Nil(t, objNonStrict)
	assert.True(t, errors.Is(errNonStrict, fs.ErrorObjectNotFound))

	objStrict, errStrict := newStrictReadTestUnion(t, true, missing, failing).NewObject(ctx, "target.txt")
	assert.Nil(t, objStrict)
	require.Error(t, errStrict)
	assert.False(t, errors.Is(errStrict, fs.ErrorObjectNotFound))
	assert.ErrorIs(t, errStrict, boom)
}

func TestStrictReadsNewObjectRejectsPartialTrustedObject(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("lookup failed")

	successful := newScriptedFs(t, "healthy")
	successful.objectResults["target.txt"] = scriptedObjectResult{obj: newTestObject("target.txt")}

	failing := newScriptedFs(t, "failing")
	failing.objectResults["target.txt"] = scriptedObjectResult{err: boom}

	objNonStrict, errNonStrict := newStrictReadTestUnion(t, false, successful, failing).NewObject(ctx, "target.txt")
	require.Error(t, errNonStrict)
	require.NotNil(t, objNonStrict)
	assert.False(t, errors.Is(errNonStrict, fs.ErrorObjectNotFound))
	assert.ErrorIs(t, errNonStrict, boom)

	objStrict, errStrict := newStrictReadTestUnion(t, true, successful, failing).NewObject(ctx, "target.txt")
	require.Error(t, errStrict)
	assert.Nil(t, objStrict)
	assert.ErrorIs(t, errStrict, boom)
}

func TestStrictReadsListRStopsPartialRecursiveSuccess(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("recursive listing failed")

	successful := newScriptedFs(t, "healthy")
	successful.listRResults[""] = scriptedListRResult{
		tranches: []fs.DirEntries{{newTestObject("healthy.txt")}},
	}

	failing := newScriptedFs(t, "failing")
	failing.listRResults[""] = scriptedListRResult{
		tranches: []fs.DirEntries{{newTestObject("partial.txt")}},
		err:      boom,
	}

	finalCallbackEntriesNonStrict, errNonStrict := collectListR(ctx, newStrictReadTestUnion(t, false, successful, failing), "")
	require.NoError(t, errNonStrict)
	assert.NotEmpty(t, finalCallbackEntriesNonStrict)

	finalCallbackEntries, errStrict := collectListR(ctx, newStrictReadTestUnion(t, true, successful, failing), "")
	require.Error(t, errStrict)
	assert.Empty(t, finalCallbackEntries)
	assert.ErrorIs(t, errStrict, boom)
	assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
}

func TestStrictReadsListRRejectsPartialDirNotFound(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "dir not found", err: fs.ErrorDirNotFound},
		{name: "wrapped dir not found", err: fmt.Errorf("wrapped miss: %w", fs.ErrorDirNotFound)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			successful := newScriptedFs(t, "healthy")
			successful.listRResults[""] = scriptedListRResult{
				tranches: []fs.DirEntries{{newTestObject("healthy.txt")}},
			}

			failing := newScriptedFs(t, "failing")
			failing.listRResults[""] = scriptedListRResult{
				tranches: []fs.DirEntries{{newTestObject("partial.txt")}},
				err:      tc.err,
			}

			finalCallbackEntriesNonStrict, errNonStrict := collectListR(ctx, newStrictReadTestUnion(t, false, successful, failing), "")
			require.NoError(t, errNonStrict)
			assert.NotEmpty(t, finalCallbackEntriesNonStrict)

			finalCallbackEntries, errStrict := collectListR(ctx, newStrictReadTestUnion(t, true, successful, failing), "")
			require.Error(t, errStrict)
			assert.Empty(t, finalCallbackEntries)
			assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
		})
	}
}

func TestStrictReadsListRDoesNotClassifyMixedFailureAsDirNotFound(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("recursive listing failed")

	missing := newScriptedFs(t, "missing")
	missing.listRResults[""] = scriptedListRResult{err: fs.ErrorDirNotFound}

	failing := newScriptedFs(t, "failing")
	failing.listRResults[""] = scriptedListRResult{err: boom}

	_, errNonStrict := collectListR(ctx, newStrictReadTestUnion(t, false, missing, failing), "")
	require.Error(t, errNonStrict)
	assert.ErrorIs(t, errNonStrict, boom)

	finalCallbackEntries, errStrict := collectListR(ctx, newStrictReadTestUnion(t, true, missing, failing), "")
	require.Error(t, errStrict)
	assert.Empty(t, finalCallbackEntries)
	assert.ErrorIs(t, errStrict, boom)
	assert.False(t, errors.Is(errStrict, fs.ErrorDirNotFound))
}

func (f *Fs) TestInternalReadOnly(t *testing.T) {
	if f.name != "TestUnionRO" {
		t.Skip("Only on RO union")
	}
	dir := "TestInternalReadOnly"
	ctx := context.Background()
	rofs := f.upstreams[len(f.upstreams)-1]
	assert.False(t, rofs.IsWritable())

	// Put a file onto the read only fs
	contents := random.String(50)
	file1 := fstest.NewItem(dir+"/file.txt", contents, time.Now())
	obj1 := fstests.PutTestContents(ctx, t, rofs, &file1, contents, true)

	// Check read from readonly fs via union
	o, err := f.NewObject(ctx, file1.Path)
	require.NoError(t, err)
	assert.Equal(t, int64(50), o.Size())

	// Now call Update on the union Object with new data
	contents2 := random.String(100)
	file2 := fstest.NewItem(dir+"/file.txt", contents2, time.Now())
	in := bytes.NewBufferString(contents2)
	src := object.NewStaticObjectInfo(file2.Path, file2.ModTime, file2.Size, true, nil, nil)
	err = o.Update(ctx, in, src)
	require.NoError(t, err)
	assert.Equal(t, int64(100), o.Size())

	// Check we read the new object via the union
	o, err = f.NewObject(ctx, file1.Path)
	require.NoError(t, err)
	assert.Equal(t, int64(100), o.Size())

	// Remove the object
	assert.NoError(t, o.Remove(ctx))

	// Check we read the old object in the read only layer now
	o, err = f.NewObject(ctx, file1.Path)
	require.NoError(t, err)
	assert.Equal(t, int64(50), o.Size())

	// Remove file and dir from read only fs
	assert.NoError(t, obj1.Remove(ctx))
	assert.NoError(t, rofs.Rmdir(ctx, dir))
}

func (f *Fs) InternalTest(t *testing.T) {
	t.Run("ReadOnly", f.TestInternalReadOnly)
}

var _ fstests.InternalTester = (*Fs)(nil)

// This specifically tests a union of local which can Move but not
// Copy and :memory: which can Copy but not Move to makes sure that
// the resulting union can Move
func TestMoveCopy(t *testing.T) {
	if *fstest.RemoteName != "" {
		t.Skip("Skipping as -remote set")
	}
	ctx := context.Background()
	dirs := MakeTestDirs(t, 1)
	fsString := fmt.Sprintf(":union,upstreams='%s :memory:bucket':", dirs[0])
	f, err := fs.NewFs(ctx, fsString)
	require.NoError(t, err)

	unionFs := f.(*Fs)
	fLocal := unionFs.upstreams[0].Fs
	fMemory := unionFs.upstreams[1].Fs

	if runtime.GOOS == "darwin" {
		// need to disable as this test specifically tests a local that can't Copy
		f.Features().Disable("Copy")
		fLocal.Features().Disable("Copy")
	}

	t.Run("Features", func(t *testing.T) {
		assert.NotNil(t, f.Features().Move)
		assert.Nil(t, f.Features().Copy)

		// Check underlying are as we are expect
		assert.NotNil(t, fLocal.Features().Move)
		assert.Nil(t, fLocal.Features().Copy)
		assert.Nil(t, fMemory.Features().Move)
		assert.NotNil(t, fMemory.Features().Copy)
	})

	// Put a file onto the local fs
	contentsLocal := random.String(50)
	fileLocal := fstest.NewItem("local.txt", contentsLocal, time.Now())
	_ = fstests.PutTestContents(ctx, t, fLocal, &fileLocal, contentsLocal, true)
	objLocal, err := f.NewObject(ctx, fileLocal.Path)
	require.NoError(t, err)

	// Put a file onto the memory fs
	contentsMemory := random.String(60)
	fileMemory := fstest.NewItem("memory.txt", contentsMemory, time.Now())
	_ = fstests.PutTestContents(ctx, t, fMemory, &fileMemory, contentsMemory, true)
	objMemory, err := f.NewObject(ctx, fileMemory.Path)
	require.NoError(t, err)

	fstest.CheckListing(t, f, []fstest.Item{fileLocal, fileMemory})

	t.Run("MoveLocal", func(t *testing.T) {
		fileLocal.Path = "local-renamed.txt"
		_, err := operations.Move(ctx, f, nil, fileLocal.Path, objLocal)
		require.NoError(t, err)
		fstest.CheckListing(t, f, []fstest.Item{fileLocal, fileMemory})

		// Check can retrieve object from union
		obj, err := f.NewObject(ctx, fileLocal.Path)
		require.NoError(t, err)
		assert.Equal(t, fileLocal.Size, obj.Size())

		// Check can retrieve object from underlying
		obj, err = fLocal.NewObject(ctx, fileLocal.Path)
		require.NoError(t, err)
		assert.Equal(t, fileLocal.Size, obj.Size())

		t.Run("MoveMemory", func(t *testing.T) {
			fileMemory.Path = "memory-renamed.txt"
			_, err := operations.Move(ctx, f, nil, fileMemory.Path, objMemory)
			require.NoError(t, err)
			fstest.CheckListing(t, f, []fstest.Item{fileLocal, fileMemory})

			// Check can retrieve object from union
			obj, err := f.NewObject(ctx, fileMemory.Path)
			require.NoError(t, err)
			assert.Equal(t, fileMemory.Size, obj.Size())

			// Check can retrieve object from underlying
			obj, err = fMemory.NewObject(ctx, fileMemory.Path)
			require.NoError(t, err)
			assert.Equal(t, fileMemory.Size, obj.Size())
		})
	})
}
