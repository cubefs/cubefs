// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/local"
)

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

// syncTestEnv pairs two local Backends (src + dst) rooted at independent
// temp directories. The returned roots are EvalSymlinks-resolved so tests
// can use them directly with the OS package.
type syncTestEnv struct {
	src     backend.Backend
	dst     backend.Backend
	srcRoot string
	dstRoot string
}

func newSyncTestEnv(t *testing.T) *syncTestEnv {
	t.Helper()
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	srcResolved, err := filepath.EvalSymlinks(srcDir)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", srcDir, err)
	}
	dstResolved, err := filepath.EvalSymlinks(dstDir)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", dstDir, err)
	}
	src, err := local.New(&local.Config{
		AllowedRoots:         []string{srcResolved},
		DefaultBufferSizeKiB: 256,
	})
	if err != nil {
		t.Fatalf("new src backend: %v", err)
	}
	dst, err := local.New(&local.Config{
		AllowedRoots:         []string{dstResolved},
		DefaultBufferSizeKiB: 256,
	})
	if err != nil {
		t.Fatalf("new dst backend: %v", err)
	}
	t.Cleanup(func() {
		_ = src.Close()
		_ = dst.Close()
	})
	return &syncTestEnv{src: src, dst: dst, srcRoot: srcResolved, dstRoot: dstResolved}
}

// writeSrcFile writes content under the src root at the given relative key.
// Uses the backend.Put path to keep test setup honest about the same code
// path the executor will hit.
func (env *syncTestEnv) writeSrcFile(t *testing.T, key string, content []byte) {
	t.Helper()
	_, err := env.src.Put(context.Background(), key,
		bytes.NewReader(content), int64(len(content)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("seed src %q: %v", key, err)
	}
}

// writeDstFile writes content under the dst root at the given relative key.
func (env *syncTestEnv) writeDstFile(t *testing.T, key string, content []byte) {
	t.Helper()
	_, err := env.dst.Put(context.Background(), key,
		bytes.NewReader(content), int64(len(content)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("seed dst %q: %v", key, err)
	}
}

// listDstKeys returns all object keys present under dst (recursive). Useful
// for asserting set-membership after a Run. Returned keys are RELATIVE to
// dstRoot (the absolute root prefix is stripped to match the rebaseKey
// semantics tested elsewhere).
func (env *syncTestEnv) listDstKeys(t *testing.T) []string {
	t.Helper()
	ch, err := env.dst.List(context.Background(), env.dstRoot, true)
	if err != nil {
		t.Fatalf("list dst: %v", err)
	}
	var keys []string
	prefix := strings.TrimRight(env.dstRoot, "/") + "/"
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("list dst entry err: %v", e.Err)
		}
		if e.IsDir {
			continue
		}
		k := e.Key
		if strings.HasPrefix(k, prefix) {
			k = strings.TrimPrefix(k, prefix)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// readDstFile fetches a key's bytes from dst.
func (env *syncTestEnv) readDstFile(t *testing.T, key string) []byte {
	t.Helper()
	rc, err := env.dst.Get(context.Background(), key, 0, 0)
	if err != nil {
		t.Fatalf("get dst %q: %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read dst %q: %v", key, err)
	}
	return b
}

// srcExists reports whether key still exists under the src root (used to
// verify the AfterCopy=verify_then_delete_src behaviour).
func (env *syncTestEnv) srcExists(t *testing.T, key string) bool {
	t.Helper()
	_, _, _, err := env.src.Head(context.Background(), key)
	if err == nil {
		return true
	}
	if errors.Is(err, backend.ErrKeyNotFound) {
		return false
	}
	t.Fatalf("head src %q: %v", key, err)
	return false
}

// newSyncTask builds a baseline Task for env. Tests tweak the returned
// pointer (Filter / Retention / AfterCopy) before passing to Executor.Run.
//
// SrcPath/DstPath are absolute paths because the local Backend rejects an
// empty key — the resolveSafe path needs something to walk under.
func newSyncTask(env *syncTestEnv, id string) *Task {
	return &Task{
		ID:          id,
		Type:        TaskTypeSync,
		Src:         env.src,
		Dst:         env.dst,
		SrcPath:     env.srcRoot,
		DstPath:     env.dstRoot,
		Parallelism: 2,
	}
}

// runSyncTask runs the task synchronously against a fresh Executor and
// returns the Result. ctx is the caller-supplied context.
func runSyncTask(ctx context.Context, t *testing.T, task *Task) Result {
	t.Helper()
	e := New(WithProgressInterval(20 * time.Millisecond))
	defer e.Close()
	return e.Run(ctx, task, NoopReporter{})
}

// -----------------------------------------------------------------------
// failingBackend wraps a real Backend but returns ErrKeyNotFound on Get
// for any key whose basename (last "/" segment) matches one of failKeys.
// Used for the "retention skipped after partial failure" test where the
// upstream lister emits absolute paths but the test only knows the
// basename.
type failingBackend struct {
	inner    backend.Backend
	failKeys map[string]bool
}

func (f *failingBackend) Kind() string { return f.inner.Kind() }
func (f *failingBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	return f.inner.List(ctx, prefix, recursive)
}
func (f *failingBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	base := key
	if i := strings.LastIndex(key, "/"); i >= 0 {
		base = key[i+1:]
	}
	if f.failKeys[base] {
		return nil, backend.ErrKeyNotFound
	}
	return f.inner.Get(ctx, key, off, size)
}
func (f *failingBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	return f.inner.Head(ctx, key)
}
func (f *failingBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (string, error) {
	return f.inner.Put(ctx, key, body, size, opts)
}
func (f *failingBackend) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}
func (f *failingBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	return f.inner.Rename(ctx, oldKey, newKey)
}
func (f *failingBackend) Capabilities() backend.Caps { return f.inner.Capabilities() }
func (f *failingBackend) Close() error               { return f.inner.Close() }

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

func TestRunSync_EmptySource(t *testing.T) {
	env := newSyncTestEnv(t)
	task := newSyncTask(env, "t-empty")
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if res.Progress.FilesDone != 0 {
		t.Errorf("FilesDone = %d, want 0", res.Progress.FilesDone)
	}
	if res.Progress.FilesFailed != 0 {
		t.Errorf("FilesFailed = %d, want 0", res.Progress.FilesFailed)
	}
}

func TestRunSync_ThreeFilesAllTransfer(t *testing.T) {
	env := newSyncTestEnv(t)
	files := map[string][]byte{
		"a.txt": []byte("alpha alpha alpha"),
		"b.txt": []byte("bravo bravo bravo bravo"),
		"c.txt": []byte("charlie"),
	}
	for k, v := range files {
		env.writeSrcFile(t, k, v)
	}

	task := newSyncTask(env, "t-three")
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesDone, int64(3); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}
	if got, want := res.Progress.FilesFailed, int64(0); got != want {
		t.Errorf("FilesFailed = %d, want %d", got, want)
	}
	dstKeys := env.listDstKeys(t)
	if len(dstKeys) != 3 {
		t.Fatalf("dst has %d files, want 3 (%v)", len(dstKeys), dstKeys)
	}
	for _, k := range dstKeys {
		got := env.readDstFile(t, k)
		want, ok := files[k]
		if !ok {
			t.Errorf("unexpected key %q on dst", k)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("dst[%q] bytes differ: got %d, want %d", k, len(got), len(want))
		}
	}
}

func TestRunSync_FilterExcludesOne(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "keep1.pt", []byte("model1"))
	env.writeSrcFile(t, "keep2.pt", []byte("model2"))
	env.writeSrcFile(t, "skip.tmp", []byte("temporary"))

	task := newSyncTask(env, "t-filter")
	task.Filter = Filter{Include: []string{"*.pt"}}
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesDone, int64(2); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}
	if got, want := res.Progress.FilesSkipped, int64(1); got != want {
		t.Errorf("FilesSkipped = %d, want %d", got, want)
	}
	keys := env.listDstKeys(t)
	if len(keys) != 2 {
		t.Fatalf("dst has %d files, want 2 (%v)", len(keys), keys)
	}
	for _, k := range keys {
		if strings.HasSuffix(k, ".tmp") {
			t.Errorf("dst should not contain %q", k)
		}
	}
}

func TestRunSync_IdempotentReRun(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "x.bin", []byte("xxxxxxx"))
	env.writeSrcFile(t, "y.bin", []byte("yyyyyyyyy"))

	// First run lands the files.
	res1 := runSyncTask(context.Background(), t, newSyncTask(env, "t-id-1"))
	if res1.Status != StatusDone {
		t.Fatalf("first Status = %s, want Done. err=%s", res1.Status, res1.Error)
	}
	if res1.Progress.FilesDone != 2 {
		t.Fatalf("first FilesDone = %d, want 2", res1.Progress.FilesDone)
	}

	// Second run should detect matching sizes and skip both files.
	res2 := runSyncTask(context.Background(), t, newSyncTask(env, "t-id-2"))
	if res2.Status != StatusDone {
		t.Fatalf("second Status = %s, want Done. err=%s", res2.Status, res2.Error)
	}
	if res2.Progress.FilesSkipped != 2 {
		t.Errorf("second FilesSkipped = %d, want 2", res2.Progress.FilesSkipped)
	}
	if res2.Progress.FilesDone != 0 {
		t.Errorf("second FilesDone = %d, want 0 (idempotent)", res2.Progress.FilesDone)
	}
}

func TestRunSync_AfterCopyVerifyThenDeleteSrc(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "p.pt", []byte("payload-1"))
	env.writeSrcFile(t, "q.pt", []byte("payload-2-larger"))

	task := newSyncTask(env, "t-acdel")
	task.AfterCopy = AfterCopyVerifyThenDeleteSrc
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	for _, k := range []string{"p.pt", "q.pt"} {
		if env.srcExists(t, k) {
			t.Errorf("src %q should have been deleted after verify", k)
		}
	}
	if got := env.listDstKeys(t); len(got) != 2 {
		t.Errorf("dst should have 2 files, got %d (%v)", len(got), got)
	}
}

func TestRunSync_RetentionKeepLast(t *testing.T) {
	env := newSyncTestEnv(t)

	// Pre-seed dst with 5 versioned files (only one is in src — the
	// retention pass should see all five regardless and prune to 2).
	for n := 1; n <= 5; n++ {
		env.writeDstFile(t, fmt.Sprintf("model-step-%d.pt", n),
			[]byte(fmt.Sprintf("v%d", n)))
	}
	env.writeSrcFile(t, "model-step-6.pt", []byte("v6"))

	task := newSyncTask(env, "t-ret")
	task.Retention = Retention{
		Pattern:  "model-step-{N}.pt",
		KeepLast: 2,
	}
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	keys := env.listDstKeys(t)
	if len(keys) != 2 {
		t.Fatalf("dst has %d files after retention, want 2 (%v)", len(keys), keys)
	}
	// Top two versions are 6 and 5.
	want := []string{"model-step-5.pt", "model-step-6.pt"}
	if !equalStringSlices(keys, want) {
		t.Errorf("dst keys = %v, want %v", keys, want)
	}
}

func TestRunSync_RetentionNotAppliedAfterFailure(t *testing.T) {
	env := newSyncTestEnv(t)

	// Seed dst with 5 versioned files; want to verify they stay put even
	// though retention.keepLast=2 would normally prune 3.
	for n := 1; n <= 5; n++ {
		env.writeDstFile(t, fmt.Sprintf("model-step-%d.pt", n),
			[]byte(fmt.Sprintf("v%d", n)))
	}
	// Add ONE src file that is guaranteed to fail (Get returns
	// ErrKeyNotFound via the failingBackend wrapper). The src filesystem
	// must have a corresponding entry for List to enumerate it.
	env.writeSrcFile(t, "model-step-9.pt", []byte("v9-bytes"))

	failSrc := &failingBackend{
		inner:    env.src,
		failKeys: map[string]bool{"model-step-9.pt": true},
	}

	task := &Task{
		ID:          "t-ret-fail",
		Type:        TaskTypeSync,
		Src:         failSrc,
		Dst:         env.dst,
		SrcPath:     env.srcRoot,
		DstPath:     env.dstRoot,
		Parallelism: 2,
		Retention: Retention{
			Pattern:  "model-step-{N}.pt",
			KeepLast: 2,
		},
	}
	e := New(WithProgressInterval(20 * time.Millisecond))
	defer e.Close()
	res := e.Run(context.Background(), task, NoopReporter{})

	if res.Status != StatusFailed {
		t.Fatalf("Status = %s, want Failed (transfer should have errored). err=%q",
			res.Status, res.Error)
	}
	if res.Progress.FilesFailed != 1 {
		t.Errorf("FilesFailed = %d, want 1", res.Progress.FilesFailed)
	}
	keys := env.listDstKeys(t)
	// All 5 pre-existing versioned files must still be present —
	// retention must NOT have run.
	if len(keys) != 5 {
		t.Errorf("dst has %d files, want 5 (retention should be skipped on failure): %v",
			len(keys), keys)
	}
}

func TestRunSync_Cancellation(t *testing.T) {
	env := newSyncTestEnv(t)
	// Seed enough files that some workers are still draining when we
	// cancel. Sizes are small so the test is fast — what we're proving is
	// that ctx-cancel propagates to a Cancelled status.
	for i := 0; i < 20; i++ {
		env.writeSrcFile(t, fmt.Sprintf("f%02d.dat", i),
			bytes.Repeat([]byte("z"), 1024))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Run even starts

	task := newSyncTask(env, "t-cancel")
	res := runSyncTask(ctx, t, task)

	if res.Status != StatusCancelled {
		t.Fatalf("Status = %s, want Cancelled. err=%s", res.Status, res.Error)
	}
}

// TestRunSync_ProgressBytesTotal verifies FilesTotal/BytesTotal climb as the
// producer enumerates entries — gives the rest of the suite confidence the
// atomic counters update correctly.
func TestRunSync_ProgressCounters(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "one.bin", []byte("12345"))
	env.writeSrcFile(t, "two.bin", []byte("1234567890"))

	res := runSyncTask(context.Background(), t, newSyncTask(env, "t-counters"))
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesTotal, int64(2); got != want {
		t.Errorf("FilesTotal = %d, want %d", got, want)
	}
	if got, want := res.Progress.FilesDone, int64(2); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}
	if got, want := res.Progress.BytesTotal, int64(15); got != want {
		t.Errorf("BytesTotal = %d, want %d", got, want)
	}
	if got, want := res.Progress.BytesDone, int64(15); got != want {
		t.Errorf("BytesDone = %d, want %d", got, want)
	}
}

// TestRunSync_AtomicCounterRace runs with -race; the worker goroutines all
// touch the same Progress struct, so any non-atomic access would surface
// here.
func TestRunSync_AtomicCounterRace(t *testing.T) {
	env := newSyncTestEnv(t)
	for i := 0; i < 50; i++ {
		env.writeSrcFile(t, fmt.Sprintf("r%03d.bin", i),
			bytes.Repeat([]byte("a"), 256))
	}
	task := newSyncTask(env, "t-race")
	task.Parallelism = 8
	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if got, want := atomic.LoadInt64(&res.Progress.FilesDone), int64(50); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}
}

// -----------------------------------------------------------------------
// rebaseKey unit test — kept here (rebaseKey lives in sync_task.go).
// -----------------------------------------------------------------------

func TestRebaseKey(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		src     string
		dst     string
		want    string
		wantErr bool
	}{
		{"prefix with trailing slash", "runs/a/b.pt", "runs/", "warm/", "warm/a/b.pt", false},
		{"prefix without slash", "runs/a/b.pt", "runs", "warm", "warm/a/b.pt", false},
		{"empty srcPath", "runs/a/b.pt", "", "warm/", "warm/runs/a/b.pt", false},
		{"empty dstPath", "runs/a/b.pt", "runs/", "", "a/b.pt", false},
		{"key not under src", "other/x.pt", "runs/", "warm/", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rebaseKey(tc.key, tc.src, tc.dst)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
