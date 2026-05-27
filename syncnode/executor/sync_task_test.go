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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
func (f *failingBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	return f.inner.Put(ctx, key, body, size, opts)
}
func (f *failingBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return f.inner.GetChecksum(ctx, key)
}
func (f *failingBackend) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}
func (f *failingBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	return f.inner.Rename(ctx, oldKey, newKey)
}
func (f *failingBackend) Capabilities() backend.Caps          { return f.inner.Capabilities() }
func (f *failingBackend) SameInstance(o backend.Backend) bool { return f.inner.SameInstance(o) }
func (f *failingBackend) Close() error                        { return f.inner.Close() }

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

// TestRunSync_PreservesSourceMtime verifies that the executor wires
// PutOptions.Mtime through to the destination backend so that dst.Mtime
// matches src.Mtime after a successful Run. This guards the integration
// between syncOneFile/transferOnce (which sets opts.Mtime from entry.Mtime)
// and the local backend (which honours PutOptions.Mtime via os.Chtimes).
func TestRunSync_PreservesSourceMtime(t *testing.T) {
	env := newSyncTestEnv(t)
	files := map[string][]byte{
		"alpha.bin":     []byte("alpha payload"),
		"sub/beta.bin":  []byte("beta payload with more bytes"),
		"sub/gamma.bin": []byte("gamma"),
	}
	for k, v := range files {
		env.writeSrcFile(t, k, v)
	}

	// Stamp distinct mtimes on each source file so a copy that drops mtime
	// (or uses wallclock) would visibly differ from the expectation.
	wantMtimes := map[string]time.Time{
		"alpha.bin":     time.Date(2021, 3, 14, 15, 9, 26, 0, time.UTC),
		"sub/beta.bin":  time.Date(2022, 6, 28, 18, 31, 53, 0, time.UTC),
		"sub/gamma.bin": time.Date(2023, 11, 5, 7, 22, 11, 0, time.UTC),
	}
	for rel, mt := range wantMtimes {
		abs := filepath.Join(env.srcRoot, rel)
		if err := os.Chtimes(abs, mt, mt); err != nil {
			t.Fatalf("chtimes src %q: %v", rel, err)
		}
	}

	task := newSyncTask(env, "t-mtime")
	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesDone, int64(len(files)); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}

	// Some filesystems coarsen mtime resolution; accept a small delta.
	const tolerance = time.Second
	for rel, want := range wantMtimes {
		_, _, got, err := env.dst.Head(context.Background(), rel)
		if err != nil {
			t.Fatalf("Head dst %q: %v", rel, err)
		}
		if diff := got.Sub(want); diff < -tolerance || diff > tolerance {
			t.Errorf("dst[%s].Mtime = %s, want %s (delta=%s tolerance=%s)", rel, got, want, diff, tolerance)
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

// TestRunSync_IdempotentReRun_NoETag verifies that local→local re-runs (no
// ETag on either side) always re-upload rather than silently skipping. This
// is the safe behaviour: without a content hash we cannot prove equality, so
// we must overwrite to avoid missing same-size mutations.
func TestRunSync_IdempotentReRun_NoETag(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "x.bin", []byte("xxxxxxx"))
	env.writeSrcFile(t, "y.bin", []byte("yyyyyyyyy"))

	res1 := runSyncTask(context.Background(), t, newSyncTask(env, "t-id-1"))
	if res1.Status != StatusDone {
		t.Fatalf("first Status = %s, want Done. err=%s", res1.Status, res1.Error)
	}
	if res1.Progress.FilesDone != 2 {
		t.Fatalf("first FilesDone = %d, want 2", res1.Progress.FilesDone)
	}

	// Second run: local backend returns no ETag, so we cannot skip.
	res2 := runSyncTask(context.Background(), t, newSyncTask(env, "t-id-2"))
	if res2.Status != StatusDone {
		t.Fatalf("second Status = %s, want Done. err=%s", res2.Status, res2.Error)
	}
	if res2.Progress.FilesDone != 2 {
		t.Errorf("second FilesDone = %d, want 2 (re-upload without ETag)", res2.Progress.FilesDone)
	}
	if res2.Progress.FilesSkipped != 0 {
		t.Errorf("second FilesSkipped = %d, want 0 (no ETag to verify equality)", res2.Progress.FilesSkipped)
	}
}

// etagBackend wraps a local Backend and injects synthetic ETags so that
// idempotency tests can exercise the skip-on-ETag-match path without
// requiring a real S3 server.
type etagBackend struct {
	inner backend.Backend
	root  string
	mu    sync.Mutex
	etags map[string]string // key → etag
}

func newETagBackend(t *testing.T) *etagBackend {
	t.Helper()
	dir := t.TempDir()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	b, err := local.New(&local.Config{
		AllowedRoots:         []string{resolved},
		DefaultBufferSizeKiB: 256,
	})
	if err != nil {
		t.Fatalf("new etagBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &etagBackend{inner: b, root: resolved, etags: make(map[string]string)}
}

func (e *etagBackend) Kind() string { return e.inner.Kind() }

// relKey strips the configured root + leading separators so the etags
// lookup is rooted on a stable, location-independent key (matches the
// scheme used in Put / Head below).
func (e *etagBackend) relKey(key string) string {
	rel := strings.TrimPrefix(key, e.root)
	return strings.TrimLeft(rel, "/")
}
func (e *etagBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch, err := e.inner.List(ctx, prefix, recursive)
	if err != nil {
		return nil, err
	}
	out := make(chan backend.Entry, 32)
	go func() {
		defer close(out)
		for entry := range ch {
			// Override the inner backend's ETag (local computes md5)
			// with our synthetic etag so the test's idempotency-skip
			// path compares against a single consistent ETag scheme.
			if !entry.IsDir {
				e.mu.Lock()
				entry.ETag = e.etags[e.relKey(entry.Key)]
				e.mu.Unlock()
			}
			out <- entry
		}
	}()
	return out, nil
}
func (e *etagBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	return e.inner.Get(ctx, key, off, size)
}
func (e *etagBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	sz, _, mt, err := e.inner.Head(ctx, key)
	if err != nil {
		return 0, "", time.Time{}, err
	}
	e.mu.Lock()
	etag := e.etags[e.relKey(key)]
	e.mu.Unlock()
	return sz, etag, mt, nil
}
func (e *etagBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	res, err := e.inner.Put(ctx, key, body, size, opts)
	if err != nil {
		return backend.PutResult{}, err
	}
	// Generate a stable synthetic ETag keyed on the relative path within
	// root, so two etagBackend instances rooted at different temp dirs
	// produce the same ETag for the same logical object — that's what
	// the idempotency-skip test needs.
	e.mu.Lock()
	rel := e.relKey(key)
	if e.etags[rel] == "" {
		e.etags[rel] = fmt.Sprintf("etag-%s", rel)
	}
	res.ETag = e.etags[rel]
	e.mu.Unlock()
	return res, nil
}
func (e *etagBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return e.inner.GetChecksum(ctx, key)
}
func (e *etagBackend) Delete(ctx context.Context, key string) error {
	e.mu.Lock()
	delete(e.etags, e.relKey(key))
	e.mu.Unlock()
	return e.inner.Delete(ctx, key)
}
func (e *etagBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	e.mu.Lock()
	oldRel := e.relKey(oldKey)
	newRel := e.relKey(newKey)
	e.etags[newRel] = e.etags[oldRel]
	delete(e.etags, oldRel)
	e.mu.Unlock()
	return e.inner.Rename(ctx, oldKey, newKey)
}
func (e *etagBackend) Capabilities() backend.Caps          { return e.inner.Capabilities() }
func (e *etagBackend) SameInstance(o backend.Backend) bool { return e.inner.SameInstance(o) }
func (e *etagBackend) Close() error                        { return e.inner.Close() }

// TestRunSync_IdempotentReRun_WithETag verifies that when both src and dst
// backends provide ETags (as S3 does), a re-run with unchanged content skips
// all files.
func TestRunSync_IdempotentReRun_WithETag(t *testing.T) {
	src := newETagBackend(t)
	dst := newETagBackend(t)

	payload := []byte("hello-etag-world")
	for _, key := range []string{"a.bin", "b.bin"} {
		if _, err := src.Put(context.Background(), key,
			bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{}); err != nil {
			t.Fatalf("seed src %q: %v", key, err)
		}
	}

	task := &Task{
		ID:               "t-etag-1",
		Type:             TaskTypeSync,
		Src:              src,
		Dst:              dst,
		SrcPath:          src.root,
		DstPath:          dst.root,
		AfterCopy:        AfterCopyKeep,
		DownloadStrategy: DownloadStrategyTempRename,
	}
	ex := New()
	res1 := ex.Run(context.Background(), task, NoopReporter{})
	if res1.Status != StatusDone {
		t.Fatalf("first Status = %s, err=%s", res1.Status, res1.Error)
	}
	if res1.Progress.FilesDone != 2 {
		t.Fatalf("first FilesDone = %d, want 2", res1.Progress.FilesDone)
	}

	task2 := *task
	task2.ID = "t-etag-2"
	res2 := ex.Run(context.Background(), &task2, NoopReporter{})
	if res2.Status != StatusDone {
		t.Fatalf("second Status = %s, err=%s", res2.Status, res2.Error)
	}
	if res2.Progress.FilesSkipped != 2 {
		t.Errorf("second FilesSkipped = %d, want 2 (ETag match)", res2.Progress.FilesSkipped)
	}
	if res2.Progress.FilesDone != 0 {
		t.Errorf("second FilesDone = %d, want 0", res2.Progress.FilesDone)
	}
}

func TestRunSync_AfterCopyVerifyThenDeleteSrc(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "p.pt", []byte("payload-1"))
	env.writeSrcFile(t, "q.pt", []byte("payload-2-larger"))

	task := newSyncTask(env, "t-acdel")
	task.AfterCopy = AfterCopyVerifyThenDeleteSrc
	// validateTask requires ChecksumMode=strong before allowing src
	// deletion — without it the task fails closed (see executor.go).
	task.ChecksumMode = "strong"
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

// TestRunSync_AfterCopyVerifyThenDeleteSrc_SkipPath — rclone-move 语义：
// 即使 dst 已经存在且与 src 强校验一致，本来会走 idempotent skip 分支，
// AfterCopy=verify_then_delete_src 仍必须删除 src，否则源端会残留文件，
// 用户视角等同于"move 没完成"。validateTask 已保证 strong checksum，
// 故跳过分支命中时端到端校验已经一致，删除是安全的。
func TestRunSync_AfterCopyVerifyThenDeleteSrc_SkipPath(t *testing.T) {
	env := newSyncTestEnv(t)
	// 两个文件 src/dst 内容一致，模拟"上次同步已完成、本次重跑"的场景。
	for k, payload := range map[string][]byte{
		"p.pt": []byte("identical-1"),
		"q.pt": []byte("identical-2-larger"),
	} {
		env.writeSrcFile(t, k, payload)
		env.writeDstFile(t, k, payload)
	}

	task := newSyncTask(env, "t-acdel-skip")
	task.AfterCopy = AfterCopyVerifyThenDeleteSrc
	task.ChecksumMode = "strong"
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if res.Progress.FilesSkipped != 2 {
		t.Errorf("FilesSkipped = %d, want 2 (skip-path must fire when dst already matches)", res.Progress.FilesSkipped)
	}
	if res.Progress.FilesDone != 0 {
		t.Errorf("FilesDone = %d, want 0 (no actual transfer expected on skip path)", res.Progress.FilesDone)
	}
	for _, k := range []string{"p.pt", "q.pt"} {
		if env.srcExists(t, k) {
			t.Errorf("src %q should have been deleted on skip-path (rclone-move semantics)", k)
		}
	}
	dstKeys := env.listDstKeys(t)
	if len(dstKeys) != 2 {
		t.Errorf("dst should still hold both files after skip, got %d (%v)", len(dstKeys), dstKeys)
	}
}

// TestRunSync_TaskTypeMove_HappyPath verifies that a bare TaskTypeMove
// (no AfterCopy / ChecksumMode set by the caller) runs through runSync,
// copies src to dst, and deletes src — i.e. validateTask locks the
// invariants and the data path produces rclone-move semantics end-to-end.
func TestRunSync_TaskTypeMove_HappyPath(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "p.pt", []byte("move-payload-1"))
	env.writeSrcFile(t, "q.pt", []byte("move-payload-2-larger"))

	task := newSyncTask(env, "t-move")
	task.Type = TaskTypeMove
	// 不设置 AfterCopy / ChecksumMode — validateTask 应自动锁定。
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	for _, k := range []string{"p.pt", "q.pt"} {
		if env.srcExists(t, k) {
			t.Errorf("src %q should have been deleted by TaskTypeMove", k)
		}
	}
	if got := env.listDstKeys(t); len(got) != 2 {
		t.Errorf("dst should hold 2 files after move, got %d (%v)", len(got), got)
	}
}

// TestRunSync_TaskTypeMove_SkipPath verifies that TaskTypeMove also enforces
// rclone-move semantics on the skip path: dst already matches src → skip
// transfer but still delete src.
func TestRunSync_TaskTypeMove_SkipPath(t *testing.T) {
	env := newSyncTestEnv(t)
	for k, payload := range map[string][]byte{
		"p.pt": []byte("identical-move-1"),
		"q.pt": []byte("identical-move-2-larger"),
	} {
		env.writeSrcFile(t, k, payload)
		env.writeDstFile(t, k, payload)
	}

	task := newSyncTask(env, "t-move-skip")
	task.Type = TaskTypeMove
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if res.Progress.FilesSkipped != 2 {
		t.Errorf("FilesSkipped = %d, want 2", res.Progress.FilesSkipped)
	}
	for _, k := range []string{"p.pt", "q.pt"} {
		if env.srcExists(t, k) {
			t.Errorf("src %q must be deleted on TaskTypeMove skip-path", k)
		}
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
// Data-integrity P0/P1/P2 tests
// -----------------------------------------------------------------------

// checksumMismatchBackend wraps a Backend so GetChecksum returns a fixed
// "wrong" sha256 value. Triggers the strong-mode mismatch path in
// syncOneFile without requiring a contrived two-machine race.
type checksumMismatchBackend struct {
	inner backend.Backend
}

func (b *checksumMismatchBackend) Kind() string { return b.inner.Kind() }
func (b *checksumMismatchBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	return b.inner.List(ctx, prefix, recursive)
}
func (b *checksumMismatchBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	return b.inner.Get(ctx, key, off, size)
}
func (b *checksumMismatchBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	return b.inner.Head(ctx, key)
}
func (b *checksumMismatchBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	return b.inner.Put(ctx, key, body, size, opts)
}
func (b *checksumMismatchBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	// Always return a predictable "different" sha256 value so the
	// equality check in checksumEqual() falls through to mismatch.
	return strings.Repeat("0", 64), backend.ChecksumAlgorithmSHA256, nil
}
func (b *checksumMismatchBackend) Delete(ctx context.Context, key string) error {
	return b.inner.Delete(ctx, key)
}
func (b *checksumMismatchBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	return b.inner.Rename(ctx, oldKey, newKey)
}
func (b *checksumMismatchBackend) Capabilities() backend.Caps          { return b.inner.Capabilities() }
func (b *checksumMismatchBackend) SameInstance(o backend.Backend) bool { return b.inner.SameInstance(o) }
func (b *checksumMismatchBackend) Close() error                        { return b.inner.Close() }

// TestSyncOneFile_StrongChecksum_Mismatch — P0 strong-mode rejects a copy
// when the backend-reported dst checksum disagrees with what we just
// uploaded. After MaxRetries failures the file is failed; dst is rolled
// back so a partially-written object doesn't linger.
func TestSyncOneFile_StrongChecksum_Mismatch(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "p0.bin", []byte("integrity-payload"))

	// Wrap dst so GetChecksum always returns a wrong sha256 — the verify
	// step in syncOneFile will see mismatch on every attempt.
	dst := &checksumMismatchBackend{inner: env.dst}

	task := &Task{
		ID:           "t-p0-mismatch",
		Type:         TaskTypeSync,
		Src:          env.src,
		Dst:          dst,
		SrcPath:      env.srcRoot,
		DstPath:      env.dstRoot,
		Parallelism:  1,
		ChecksumMode: "strong",
		MaxRetries:   2,
	}
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusFailed {
		t.Fatalf("Status = %s, want Failed (checksum mismatch). err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesFailed, int64(1); got != want {
		t.Errorf("FilesFailed = %d, want %d", got, want)
	}
	if !strings.Contains(res.Error, "checksum mismatch") {
		t.Errorf("error = %q, want it to mention 'checksum mismatch'", res.Error)
	}
	// Dst should have been deleted by the rollback in syncOneFile (the
	// final failed attempt ran t.Dst.Delete before returning).
	if _, _, _, err := env.dst.Head(context.Background(), filepath.Join(env.dstRoot, "p0.bin")); err == nil {
		t.Errorf("dst p0.bin still exists; rollback should have removed it")
	} else if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("Head dst err = %v, want ErrKeyNotFound", err)
	}
}

// TestValidateTask_VerifyDeleteRequiresStrong — P0 防呆: any task that
// sets AfterCopy=verify_then_delete_src without ChecksumMode=strong must
// be rejected before any side-effects run.
func TestValidateTask_VerifyDeleteRequiresStrong(t *testing.T) {
	env := newSyncTestEnv(t)

	// Empty mode → reject.
	task := newSyncTask(env, "t-vt-empty")
	task.AfterCopy = AfterCopyVerifyThenDeleteSrc
	if err := validateTask(task); err == nil {
		t.Errorf("validateTask returned nil; expected verify_then_delete_src/empty to be rejected")
	} else if !strings.Contains(err.Error(), "checksumMode=strong") {
		t.Errorf("validateTask err = %q, want it to mention checksumMode=strong", err)
	}

	// "loose" mode → still reject (only "strong" is allowed).
	task2 := newSyncTask(env, "t-vt-loose")
	task2.AfterCopy = AfterCopyVerifyThenDeleteSrc
	task2.ChecksumMode = "loose"
	if err := validateTask(task2); err == nil {
		t.Errorf("validateTask returned nil for loose mode; expected reject")
	}

	// "strong" mode → accept.
	task3 := newSyncTask(env, "t-vt-strong")
	task3.AfterCopy = AfterCopyVerifyThenDeleteSrc
	task3.ChecksumMode = "strong"
	if err := validateTask(task3); err != nil {
		t.Errorf("validateTask returned %v for strong mode; want nil", err)
	}
}

// mutatingBackend wraps a src Backend and skews the Mtime returned by the
// SECOND Head call only. After that the inner mtime is returned unchanged
// so the retry attempt sees a consistent pre/post pair.
//
// Call sequence in syncOneFile with retry-on-mutate:
//
//	call 1 (pre-Head, attempt 0)   → mt0
//	call 2 (post-Head, attempt 0)  → mt0 + 1m  ← mutation detected, retry
//	call 3 (post-Head, attempt 1)  → mt0       ← BUT we compare against the
//	                                            srcPre that moved forward
//	                                            to mt0+1m on retry, so this
//	                                            still mismatches.
//
// To make the retry succeed cleanly, we make ONLY call #2 skewed, and on
// every subsequent call return the inner mtime — but the retry loop also
// rolls srcPre forward to the most recently observed post-Head value
// (mt0+1m). So call #3 returning mt0 would still be a mismatch.
//
// Workaround: after the first skew, we permanently shift every subsequent
// reading by +1m so srcPre/srcPost stay equal across the retry.
type mutatingBackend struct {
	inner   backend.Backend
	calls   atomic.Int64
	shifted atomic.Bool
}

func (b *mutatingBackend) Kind() string { return b.inner.Kind() }
func (b *mutatingBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	return b.inner.List(ctx, prefix, recursive)
}
func (b *mutatingBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	return b.inner.Get(ctx, key, off, size)
}
func (b *mutatingBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	sz, etag, mt, err := b.inner.Head(ctx, key)
	if err != nil {
		return 0, "", time.Time{}, err
	}
	// Trigger the mutation on the SECOND call (the first attempt's post-
	// Head). On every subsequent call return the shifted mtime so the
	// retry attempt sees a stable pre/post pair.
	n := b.calls.Add(1)
	if n == 2 {
		b.shifted.Store(true)
	}
	if b.shifted.Load() {
		mt = mt.Add(time.Minute)
	}
	return sz, etag, mt, nil
}
func (b *mutatingBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	return b.inner.Put(ctx, key, body, size, opts)
}
func (b *mutatingBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return b.inner.GetChecksum(ctx, key)
}
func (b *mutatingBackend) Delete(ctx context.Context, key string) error {
	return b.inner.Delete(ctx, key)
}
func (b *mutatingBackend) Rename(ctx context.Context, oldKey, newKey string) error {
	return b.inner.Rename(ctx, oldKey, newKey)
}
func (b *mutatingBackend) Capabilities() backend.Caps          { return b.inner.Capabilities() }
func (b *mutatingBackend) SameInstance(o backend.Backend) bool { return b.inner.SameInstance(o) }
func (b *mutatingBackend) Close() error                        { return b.inner.Close() }

// TestSyncOneFile_OnSourceMutated_Retry — P1 OnSourceMutated="retry"
// detects mid-transfer src drift on the FIRST attempt, rolls dst back, and
// the SECOND attempt (where mutatingBackend's mtime is stable) succeeds.
func TestSyncOneFile_OnSourceMutated_Retry(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "p1.bin", []byte("payload-mutated-then-stable"))

	src := &mutatingBackend{inner: env.src}

	task := &Task{
		ID:              "t-p1-retry",
		Type:            TaskTypeSync,
		Src:             src,
		Dst:             env.dst,
		SrcPath:         env.srcRoot,
		DstPath:         env.dstRoot,
		Parallelism:     1,
		OnSourceMutated: "retry",
		MaxRetries:      3,
	}
	res := runSyncTask(context.Background(), t, task)

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done (retry should succeed). err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesDone, int64(1); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}
	if got, want := res.Progress.FilesFailed, int64(0); got != want {
		t.Errorf("FilesFailed = %d, want %d", got, want)
	}
}

// fakeStore is an in-memory InProgressStore used by the resume test. The
// real bolt store is exercised by syncnode/bolt's own tests.
type fakeStore struct {
	mu sync.Mutex
	bp map[string]*Breakpoint
}

func newFakeStore() *fakeStore { return &fakeStore{bp: map[string]*Breakpoint{}} }
func (s *fakeStore) Put(ctx context.Context, b *Breakpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *b
	s.bp[b.Key] = &cp
	return nil
}
func (s *fakeStore) Get(ctx context.Context, key string) (*Breakpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.bp[key]; ok {
		cp := *b
		return &cp, nil
	}
	return nil, nil
}
func (s *fakeStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.bp, key)
	return nil
}
func (s *fakeStore) snapshot() map[string]*Breakpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Breakpoint, len(s.bp))
	for k, v := range s.bp {
		cp := *v
		out[k] = &cp
	}
	return out
}

// TestSyncOneFile_Resume_FromBreakpoint — P2 ResumeEnabled clears the
// breakpoint after a successful transfer (the post-condition the resume
// loop guards). When syncOneFile completes without a partial failure, no
// breakpoint should remain in the store.
func TestSyncOneFile_Resume_FromBreakpoint(t *testing.T) {
	env := newSyncTestEnv(t)
	env.writeSrcFile(t, "p2.bin", []byte("resume-payload"))

	store := newFakeStore()
	// Pre-seed a breakpoint to verify it gets DELETED on success — the
	// resume loop must clean up after itself when the transfer completes.
	preKey := breakpointKey("t-p2-resume", filepath.Join(env.srcRoot, "p2.bin"))
	if err := store.Put(context.Background(), &Breakpoint{
		TaskID:    "t-p2-resume",
		Key:       preKey,
		BytesDone: 0,
	}); err != nil {
		t.Fatalf("seed breakpoint: %v", err)
	}

	task := &Task{
		ID:            "t-p2-resume",
		Type:          TaskTypeSync,
		Src:           env.src,
		Dst:           env.dst,
		SrcPath:       env.srcRoot,
		DstPath:       env.dstRoot,
		Parallelism:   1,
		ResumeEnabled: true,
	}
	e := New(WithProgressInterval(20*time.Millisecond), WithInProgressStore(store))
	defer e.Close()
	res := e.Run(context.Background(), task, NoopReporter{})

	if res.Status != StatusDone {
		t.Fatalf("Status = %s, want Done. err=%s", res.Status, res.Error)
	}
	if got, want := res.Progress.FilesDone, int64(1); got != want {
		t.Errorf("FilesDone = %d, want %d", got, want)
	}
	if remaining := store.snapshot(); len(remaining) != 0 {
		t.Errorf("breakpoint store has %d entries after success, want 0: %v",
			len(remaining), remaining)
	}
}

// -----------------------------------------------------------------------
// OnExisting strategy — direct dispatcher tests (truth table)
//
// These tests exercise shouldSkipExistingDstByStrategy in isolation, so
// the matrix is tight: 4 strategies × 4 dst states. We avoid running the
// full Executor pipeline here because the dispatcher's behaviour is
// independent of pipeline plumbing, and a focused table-test catches
// future regressions without depending on filter/retention/reporter wiring.
//
// The 4 dst states:
//   - "missing"          dst has no object at this key (HEAD would fail).
//                        The dispatcher is only ever called when dst exists,
//                        so we emulate this by NOT calling the dispatcher and
//                        instead asserting the upstream behaviour in a separate
//                        comment — covered by existing integration tests
//                        (TestRunSync_*). We keep three real dst states here.
//   - "different size"   dst size != src size; verify_then_skip must NOT skip.
//   - "same size, different checksum/etag" dst size == src size but signatures
//                        diverge; verify_then_skip must NOT skip.
//   - "full match"       dst size == src size and signatures equal.
// -----------------------------------------------------------------------

// onExistingFixture builds an in-memory src/dst pair where both keys exist
// and the caller controls dst's recorded size + ETag. The src always holds
// `payload` so size + ETag computation is deterministic.
type onExistingFixture struct {
	env     *syncTestEnv
	srcKey  string // logical key under srcRoot (relative)
	dstKey  string // absolute path under dstRoot (matches dispatcher arg)
	entry   backend.Entry
	dstSize int64
	dstETag string
}

func newOnExistingFixture(t *testing.T, payload []byte, dstPayload []byte) *onExistingFixture {
	t.Helper()
	env := newSyncTestEnv(t)
	const srcKey = "x.bin"
	env.writeSrcFile(t, srcKey, payload)
	// dstPayload may differ in length to simulate the "different size" state.
	env.writeDstFile(t, srcKey, dstPayload)

	srcAbs := filepath.Join(env.srcRoot, srcKey)
	dstAbs := filepath.Join(env.dstRoot, srcKey)

	srcSize, srcETag, srcMtime, err := env.src.Head(context.Background(), srcAbs)
	if err != nil {
		t.Fatalf("head src: %v", err)
	}
	dstSize, dstETag, _, err := env.dst.Head(context.Background(), dstAbs)
	if err != nil {
		t.Fatalf("head dst: %v", err)
	}
	return &onExistingFixture{
		env:    env,
		srcKey: srcAbs,
		dstKey: dstAbs,
		entry: backend.Entry{
			Key:   srcAbs,
			Size:  srcSize,
			ETag:  srcETag,
			Mtime: srcMtime,
		},
		dstSize: dstSize,
		dstETag: dstETag,
	}
}

// dispatch is a convenience wrapper that builds a minimal *Task with the
// requested OnExisting strategy and invokes the dispatcher against the
// fixture. dstMtime is supplied separately so newer_only tests can vary it
// without re-seeding the file.
//
// The local Backend's Head returns an empty ETag (POSIX has no native etag),
// so verify_then_skip's size+etag fast path can never succeed against two
// vanilla local backends. We compensate by setting ChecksumMode=strong on
// the task: shouldSkipExistingDst then falls through to GetChecksum on both
// sides — same payload → matching sha256 → skip. This keeps the dispatcher
// matrix honest without forcing every fixture row through an etagBackend
// wrapper.
func (f *onExistingFixture) dispatch(t *testing.T, strategy string, dstMtime time.Time) bool {
	t.Helper()
	task := newSyncTask(f.env, "t-on-existing")
	task.OnExisting = strategy
	task.ChecksumMode = "strong"
	return shouldSkipExistingDstByStrategy(
		context.Background(),
		task,
		f.entry,
		f.dstKey,
		f.dstSize,
		f.dstETag,
		dstMtime,
	)
}

// TestShouldSkipExistingDstByStrategy_TruthTable covers the 4 strategies ×
// 3 dst states (the "dst missing" state is not reachable via this dispatcher
// — caller only enters it on Head success — so the row is omitted; the
// upstream behaviour is exercised by TestRunSync_ThreeFilesAllTransfer).
//
// Truth table (✓ = skip, ✗ = upload):
//
//	dst state                | verify_then_skip | always_skip | newer_only | overwrite
//	-------------------------+------------------+-------------+------------+----------
//	different size           |        ✗         |      ✓      |     ✗      |    ✗
//	same size, diff etag     |        ✗         |      ✓      |     ✗      |    ✗
//	full match (same etag)   |        ✓         |      ✓      |     ✗      |    ✗
//
// The newer_only row uses dst.Mtime < src.Mtime so dst is "older" → no skip;
// a separate test below covers the dst-newer + tolerance branches.
func TestShouldSkipExistingDstByStrategy_TruthTable(t *testing.T) {
	srcPayload := []byte("payload-on-existing-strategy")

	// State 1: dst with a different size (smaller payload).
	diffSize := newOnExistingFixture(t, srcPayload, []byte("short"))

	// State 2: dst with the same size but a different byte (so etag differs).
	sameSizeDiffETag := newOnExistingFixture(t, srcPayload,
		bytes.Repeat([]byte("A"), len(srcPayload)))

	// State 3: dst is a byte-for-byte copy → md5 etag will match.
	fullMatch := newOnExistingFixture(t, srcPayload, srcPayload)

	// dstMtime in the past so newer_only never sees dst as "fresher".
	dstOlder := time.Now().Add(-time.Hour)

	cases := []struct {
		name     string
		strategy string
		fix      *onExistingFixture
		want     bool
	}{
		// verify_then_skip — legacy behaviour: skip iff size+etag both match.
		{"verify_then_skip/different_size", OnExistingVerifyThenSkip, diffSize, false},
		{"verify_then_skip/same_size_diff_etag", OnExistingVerifyThenSkip, sameSizeDiffETag, false},
		{"verify_then_skip/full_match", OnExistingVerifyThenSkip, fullMatch, true},

		// always_skip — rclone --ignore-existing: dst always wins.
		{"always_skip/different_size", OnExistingAlwaysSkip, diffSize, true},
		{"always_skip/same_size_diff_etag", OnExistingAlwaysSkip, sameSizeDiffETag, true},
		{"always_skip/full_match", OnExistingAlwaysSkip, fullMatch, true},

		// newer_only — dst.Mtime older than src ⇒ no skip on any state.
		{"newer_only/different_size", OnExistingNewerOnly, diffSize, false},
		{"newer_only/same_size_diff_etag", OnExistingNewerOnly, sameSizeDiffETag, false},
		{"newer_only/full_match", OnExistingNewerOnly, fullMatch, false},

		// overwrite — rclone --ignore-times: never skip.
		{"overwrite/different_size", OnExistingOverwrite, diffSize, false},
		{"overwrite/same_size_diff_etag", OnExistingOverwrite, sameSizeDiffETag, false},
		{"overwrite/full_match", OnExistingOverwrite, fullMatch, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.fix.dispatch(t, c.strategy, dstOlder)
			if got != c.want {
				t.Errorf("dispatch(%s) = %v, want %v", c.strategy, got, c.want)
			}
		})
	}
}

// TestShouldSkipExistingDstByStrategy_NewerOnly_Tolerance covers the
// branches around the 1s cross-backend mtime tolerance. The dispatcher
// formula is: skip iff !src.After(dst + 1s). Combined with the IsZero
// fail-safe, the matrix is:
//
//	dst.Mtime relative to src.Mtime         | skip?
//	-----------------------------------------+------
//	dst > src                                | yes
//	dst == src                               | yes
//	dst + 1s == src     (boundary, inclusive) | yes
//	dst + 1s + nanosecond < src              | no
//	dst far older than src                   | no
//	src.Mtime zero (missing)                 | no  (fail-safe)
//	dst.Mtime zero (missing)                 | no  (fail-safe)
func TestShouldSkipExistingDstByStrategy_NewerOnly_Tolerance(t *testing.T) {
	srcPayload := []byte("payload-newer-only-tolerance")
	fix := newOnExistingFixture(t, srcPayload, srcPayload)
	srcMtime := fix.entry.Mtime
	if srcMtime.IsZero() {
		t.Fatalf("test fixture has zero src mtime; cannot exercise tolerance branches")
	}

	cases := []struct {
		name     string
		dstMtime time.Time
		// override src.Mtime (used by the IsZero src case)
		srcMtime time.Time
		want     bool
	}{
		{"dst_strictly_newer", srcMtime.Add(time.Hour), srcMtime, true},
		{"dst_equal", srcMtime, srcMtime, true},
		{"dst_within_1s_tolerance", srcMtime.Add(-time.Second), srcMtime, true},
		{"dst_just_outside_tolerance", srcMtime.Add(-time.Second - time.Nanosecond), srcMtime, false},
		{"dst_far_older", srcMtime.Add(-24 * time.Hour), srcMtime, false},
		{"src_mtime_zero", srcMtime, time.Time{}, false},
		{"dst_mtime_zero", time.Time{}, srcMtime, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			entry := fix.entry
			entry.Mtime = c.srcMtime
			task := newSyncTask(fix.env, "t-newer-only")
			task.OnExisting = OnExistingNewerOnly
			got := shouldSkipExistingDstByStrategy(
				context.Background(),
				task,
				entry,
				fix.dstKey,
				fix.dstSize,
				fix.dstETag,
				c.dstMtime,
			)
			if got != c.want {
				t.Errorf("dispatch(newer_only, dst=%v, src=%v) = %v, want %v",
					c.dstMtime, c.srcMtime, got, c.want)
			}
		})
	}
}

// TestShouldSkipExistingDstByStrategy_UnknownFailsClosed — the dispatcher
// must never silently skip on an unknown strategy. validateTask should have
// rejected it earlier; the defence in depth is to upload (fail-closed) so
// the worst case is an unnecessary re-upload, never silent data loss.
func TestShouldSkipExistingDstByStrategy_UnknownFailsClosed(t *testing.T) {
	fix := newOnExistingFixture(t, []byte("payload"), []byte("payload"))
	task := newSyncTask(fix.env, "t-unknown")
	task.OnExisting = "this-is-not-a-valid-strategy"
	got := shouldSkipExistingDstByStrategy(
		context.Background(),
		task,
		fix.entry,
		fix.dstKey,
		fix.dstSize,
		fix.dstETag,
		time.Now(),
	)
	if got {
		t.Errorf("dispatch(unknown) = true, want false (fail-closed)")
	}
}

// -----------------------------------------------------------------------
// OnExisting strategy — validateTask tests
// -----------------------------------------------------------------------

// TestValidateTask_OnExisting_UnknownRejected covers the executor-side
// whitelist (double defence with syncnode/config.go). validateTask must
// reject any OnExisting value not in validOnExisting before any side
// effects.
func TestValidateTask_OnExisting_UnknownRejected(t *testing.T) {
	env := newSyncTestEnv(t)
	task := newSyncTask(env, "t-on-existing-unknown")
	task.OnExisting = "rclone-does-not-have-this"
	err := validateTask(task)
	if err == nil {
		t.Fatalf("validateTask returned nil for unknown OnExisting; want rejection")
	}
	if !strings.Contains(err.Error(), "OnExisting") {
		t.Errorf("err = %q, want it to mention OnExisting", err)
	}
}

// TestValidateTask_OnExisting_AllAcceptedForSync confirms every named
// strategy (and the empty back-compat alias) is accepted on a vanilla sync
// task and that empty is normalised to verify_then_skip.
func TestValidateTask_OnExisting_AllAcceptedForSync(t *testing.T) {
	env := newSyncTestEnv(t)
	cases := []struct {
		input string
		want  string // expected post-validation value
	}{
		{"", OnExistingVerifyThenSkip},
		{OnExistingVerifyThenSkip, OnExistingVerifyThenSkip},
		{OnExistingAlwaysSkip, OnExistingAlwaysSkip},
		{OnExistingNewerOnly, OnExistingNewerOnly},
		{OnExistingOverwrite, OnExistingOverwrite},
	}
	for _, c := range cases {
		t.Run("strategy="+c.input, func(t *testing.T) {
			task := newSyncTask(env, "t-on-existing-"+c.input)
			task.OnExisting = c.input
			if err := validateTask(task); err != nil {
				t.Fatalf("validateTask(%q): %v", c.input, err)
			}
			if task.OnExisting != c.want {
				t.Errorf("post-validateTask OnExisting = %q, want %q",
					task.OnExisting, c.want)
			}
		})
	}
}

// TestValidateTask_OnExisting_MoveExclusion covers the type=move互斥 rule.
// Move semantics already pair "verify_then_skip + verify_then_delete_src +
// strong" — every other strategy either leaves src undeleted or risks
// overwriting a newer dst, both of which would silently lose data.
//
// Verifies BOTH branches:
//   - any non-empty, non-verify_then_skip value → error with the canonical
//     message text;
//   - empty / verify_then_skip → accept, with the post-validation invariant
//     forcing OnExisting = verify_then_skip so the dispatcher hits the
//     legacy path.
func TestValidateTask_OnExisting_MoveExclusion(t *testing.T) {
	env := newSyncTestEnv(t)

	// Rejected strategies — every "non-safe" value the whitelist accepts
	// for sync but not for move.
	rejected := []string{
		OnExistingAlwaysSkip,
		OnExistingNewerOnly,
		OnExistingOverwrite,
	}
	for _, s := range rejected {
		t.Run("reject_"+s, func(t *testing.T) {
			task := newSyncTask(env, "t-move-"+s)
			task.Type = TaskTypeMove
			task.OnExisting = s
			err := validateTask(task)
			if err == nil {
				t.Fatalf("validateTask returned nil for type=move + OnExisting=%q; want reject", s)
			}
			// Canonical error text required by the spec — operators search
			// for this string in logs to distinguish the move互斥 path from
			// the generic whitelist rejection.
			if !strings.Contains(err.Error(), "type=move forbids onExisting") {
				t.Errorf("err = %q, want it to contain 'type=move forbids onExisting'", err)
			}
		})
	}

	// Accepted strategies — both must lock OnExisting to verify_then_skip
	// after validation (parity with AfterCopy/ChecksumMode auto-lock).
	accepted := []string{"", OnExistingVerifyThenSkip}
	for _, s := range accepted {
		t.Run("accept_"+s, func(t *testing.T) {
			task := newSyncTask(env, "t-move-accept-"+s)
			task.Type = TaskTypeMove
			task.OnExisting = s
			if err := validateTask(task); err != nil {
				t.Fatalf("validateTask returned %v for type=move + OnExisting=%q; want accept", err, s)
			}
			if task.OnExisting != OnExistingVerifyThenSkip {
				t.Errorf("post-validateTask OnExisting = %q, want %q (move lock)",
					task.OnExisting, OnExistingVerifyThenSkip)
			}
		})
	}
}

// TestRunSync_DryRun_DoesNotMutate is the executor-level invariant for
// rclone-gap 子项 2: when DryRun=true is set, neither src nor dst observes
// any mutation, regardless of how many files would have been copied.
//
// The Confirm flag is left false here — Confirm is only required for
// destructive tasks (Type=Move / AfterCopy=verify_then_delete_src). Plain
// Sync + DryRun is the workflow operators use to preview a fresh rule.
func TestRunSync_DryRun_DoesNotMutate(t *testing.T) {
	resetDryRunStats(t)

	env := newSyncTestEnv(t)
	srcKeys := []string{"a.txt", "b.txt", "c/d.txt"}
	for _, k := range srcKeys {
		env.writeSrcFile(t, k, []byte("payload-"+k))
	}

	task := newSyncTask(env, "t-dry-no-mutate")
	task.DryRun = true

	res := runSyncTask(context.Background(), t, task)
	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}

	// Src untouched.
	for _, k := range srcKeys {
		if !env.srcExists(t, k) {
			t.Errorf("src key %q missing after dry-run (must not delete)", k)
		}
	}
	// Dst empty.
	dstKeys := env.listDstKeys(t)
	if len(dstKeys) != 0 {
		t.Errorf("dst has %d keys after dry-run: %v; want 0", len(dstKeys), dstKeys)
	}
	// Stats: would_copy == src count, nothing else.
	snap := DryRunStats()
	if snap.WouldCopy != int64(len(srcKeys)) {
		t.Errorf("WouldCopy = %d, want %d", snap.WouldCopy, len(srcKeys))
	}
	if snap.WouldSkip != 0 || snap.WouldServerSideCopy != 0 || snap.WouldDeleteSrc != 0 {
		t.Errorf("unexpected non-zero counters: %+v", snap)
	}
}

// resetDryRunStats zeros the package-level dry-run counters so each test
// starts from a clean slate. Symmetric with resetServerSideCopyStats so
// dry_run_test.go can reuse it.
func resetDryRunStats(t *testing.T) {
	t.Helper()
	dryRunWouldCopy.Store(0)
	dryRunWouldSkip.Store(0)
	dryRunWouldServerSideCopy.Store(0)
	dryRunWouldDeleteSrc.Store(0)
	t.Cleanup(func() {
		dryRunWouldCopy.Store(0)
		dryRunWouldSkip.Store(0)
		dryRunWouldServerSideCopy.Store(0)
		dryRunWouldDeleteSrc.Store(0)
	})
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
