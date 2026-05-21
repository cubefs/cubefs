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
func (e *etagBackend) Capabilities() backend.Caps { return e.inner.Capabilities() }
func (e *etagBackend) Close() error               { return e.inner.Close() }

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
func (b *checksumMismatchBackend) Capabilities() backend.Caps { return b.inner.Capabilities() }
func (b *checksumMismatchBackend) Close() error               { return b.inner.Close() }

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
func (b *mutatingBackend) Capabilities() backend.Caps { return b.inner.Capabilities() }
func (b *mutatingBackend) Close() error               { return b.inner.Close() }

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
