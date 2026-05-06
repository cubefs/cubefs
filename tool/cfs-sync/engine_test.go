package main

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
	"testing"
	"time"

	"github.com/cubefs/cubefs/tool/cfs-sync/storage"
)

// ── mock storage ──────────────────────────────────────────────────────────────

type memStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
	putErr  error // if non-nil, Put returns this error
	getErr  error // if non-nil, Get returns this error
	delErr  error // if non-nil, Delete returns this error
	deleted []string
	label   string
}

func newMemStorage(label string, files map[string]string) *memStorage {
	m := &memStorage{objects: make(map[string][]byte), label: label}
	for k, v := range files {
		m.objects[k] = []byte(v)
	}
	return m
}

func (m *memStorage) String() string { return "mem:" + m.label }

func (m *memStorage) List(_ context.Context, _ string) (<-chan *storage.Object, <-chan error) {
	objs := make(chan *storage.Object, 64)
	errc := make(chan error, 1)

	m.mu.Lock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	m.mu.Unlock()

	sort.Strings(keys)
	go func() {
		defer close(objs)
		defer close(errc)
		for _, k := range keys {
			m.mu.Lock()
			data := m.objects[k]
			m.mu.Unlock()
			objs <- &storage.Object{
				Key:   k,
				Size:  int64(len(data)),
				Mtime: time.Unix(1000000, 0),
			}
		}
	}()
	return objs, errc
}

func (m *memStorage) Get(_ context.Context, key string, off, size int64) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.Lock()
	data, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}
	if off > 0 {
		data = data[off:]
	}
	if size > 0 && int64(len(data)) > size {
		data = data[:size]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memStorage) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	if m.putErr != nil {
		return m.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *memStorage) PutWithMtime(ctx context.Context, key string, r io.Reader, size int64, _ time.Time) error {
	return m.Put(ctx, key, r, size)
}

func (m *memStorage) Delete(_ context.Context, key string) error {
	if m.delErr != nil {
		return m.delErr
	}
	m.mu.Lock()
	delete(m.objects, key)
	m.deleted = append(m.deleted, key)
	m.mu.Unlock()
	return nil
}

func (m *memStorage) MkdirAll(_ context.Context, _ string) error { return nil }

func (m *memStorage) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ks := make([]string, 0, len(m.objects))
	for k := range m.objects {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// ── helper: collect tasks from mergeDiff ─────────────────────────────────────

func collectMergeDiff(syncer *Syncer, srcObjs, dstObjs []*storage.Object, srcErr, dstErr error) []Task {
	srcCh := make(chan *storage.Object, len(srcObjs)+1)
	dstCh := make(chan *storage.Object, len(dstObjs)+1)
	srcErrCh := make(chan error, 1)
	dstErrCh := make(chan error, 1)

	for _, o := range srcObjs {
		srcCh <- o
	}
	close(srcCh)
	for _, o := range dstObjs {
		dstCh <- o
	}
	close(dstCh)

	if srcErr != nil {
		srcErrCh <- srcErr
	}
	close(srcErrCh)
	if dstErr != nil {
		dstErrCh <- dstErr
	}
	close(dstErrCh)

	out := make(chan Task, 64)
	syncer.mergeDiff(context.Background(), srcCh, srcErrCh, dstCh, dstErrCh, out)
	close(out)

	var tasks []Task
	for t := range out {
		tasks = append(tasks, t)
	}
	return tasks
}

func defaultSyncer() *Syncer {
	opts := DefaultSyncOptions()
	opts.Transfers = 4
	opts.Checkers = 4
	return NewSyncer(newMemStorage("src", nil), newMemStorage("dst", nil), opts)
}

func obj(key string, size int64) *storage.Object {
	return &storage.Object{Key: key, Size: size, Mtime: time.Unix(1000000, 0)}
}

// ── mergeDiff tests ───────────────────────────────────────────────────────────

func TestMergeDiff_SrcOnly(t *testing.T) {
	s := defaultSyncer()
	tasks := collectMergeDiff(s,
		[]*storage.Object{obj("a.txt", 10), obj("b.txt", 20)},
		nil,
		nil, nil,
	)
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
	for _, task := range tasks {
		if task.Op != OpCopy {
			t.Errorf("task %q: want OpCopy, got %v", task.SrcKey, task.Op)
		}
	}
}

func TestMergeDiff_DstOnly_NoDelete(t *testing.T) {
	s := defaultSyncer()
	s.opts.Delete = false
	tasks := collectMergeDiff(s,
		nil,
		[]*storage.Object{obj("extra.txt", 10)},
		nil, nil,
	)
	if len(tasks) != 0 {
		t.Errorf("want 0 tasks without --delete, got %d", len(tasks))
	}
}

func TestMergeDiff_DstOnly_WithDelete(t *testing.T) {
	s := defaultSyncer()
	s.opts.Delete = true
	tasks := collectMergeDiff(s,
		nil,
		[]*storage.Object{obj("extra.txt", 10)},
		nil, nil,
	)
	if len(tasks) != 1 {
		t.Fatalf("want 1 delete task, got %d", len(tasks))
	}
	if tasks[0].Op != OpDelete {
		t.Errorf("want OpDelete, got %v", tasks[0].Op)
	}
	if tasks[0].DstKey != "extra.txt" {
		t.Errorf("DstKey = %q, want %q", tasks[0].DstKey, "extra.txt")
	}
}

func TestMergeDiff_BothSides(t *testing.T) {
	s := defaultSyncer()
	tasks := collectMergeDiff(s,
		[]*storage.Object{obj("a.txt", 10)},
		[]*storage.Object{obj("a.txt", 10)},
		nil, nil,
	)
	// Both sides have the file → checker gets to decide; mergeDiff emits a copy task.
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}
	if tasks[0].Op != OpCopy {
		t.Errorf("want OpCopy, got %v", tasks[0].Op)
	}
}

func TestMergeDiff_Mixed(t *testing.T) {
	s := defaultSyncer()
	s.opts.Delete = true
	// src: a.txt (copy), b.txt (both sides → copy)
	// dst: b.txt (both sides → copy), c.txt (dst only → delete)
	tasks := collectMergeDiff(s,
		[]*storage.Object{obj("a.txt", 10), obj("b.txt", 20)},
		[]*storage.Object{obj("b.txt", 20), obj("c.txt", 30)},
		nil, nil,
	)
	if len(tasks) != 3 {
		t.Fatalf("want 3 tasks, got %d %v", len(tasks), tasks)
	}
	ops := make(map[string]TaskOp)
	for _, t := range tasks {
		if t.Op == OpCopy {
			ops[t.SrcKey] = t.Op
		} else {
			ops[t.DstKey] = t.Op
		}
	}
	if ops["a.txt"] != OpCopy {
		t.Error("a.txt should be OpCopy")
	}
	if ops["b.txt"] != OpCopy {
		t.Error("b.txt should be OpCopy")
	}
	if ops["c.txt"] != OpDelete {
		t.Error("c.txt should be OpDelete")
	}
}

func TestMergeDiff_SkipsDirectories(t *testing.T) {
	s := defaultSyncer()
	dirObj := &storage.Object{Key: "subdir/", Size: 0, IsDir: true}
	tasks := collectMergeDiff(s,
		[]*storage.Object{dirObj, obj("subdir/file.txt", 5)},
		nil,
		nil, nil,
	)
	// Only the file should generate a task, not the directory.
	if len(tasks) != 1 {
		t.Fatalf("want 1 task (file only), got %d", len(tasks))
	}
	if tasks[0].SrcKey != "subdir/file.txt" {
		t.Errorf("unexpected task key: %q", tasks[0].SrcKey)
	}
}

func TestMergeDiff_FilteredFile(t *testing.T) {
	s := defaultSyncer()
	s.filter = &Filter{includes: []string{"*.pt"}}
	tasks := collectMergeDiff(s,
		[]*storage.Object{obj("model.pt", 100), obj("readme.txt", 10)},
		nil,
		nil, nil,
	)
	if len(tasks) != 1 {
		t.Fatalf("want 1 task (model.pt only), got %d", len(tasks))
	}
	if tasks[0].SrcKey != "model.pt" {
		t.Errorf("unexpected key: %q", tasks[0].SrcKey)
	}
}

func TestMergeDiff_ContextCancelled(t *testing.T) {
	s := defaultSyncer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	srcCh := make(chan *storage.Object, 1)
	dstCh := make(chan *storage.Object, 1)
	srcErrCh := make(chan error, 1)
	dstErrCh := make(chan error, 1)
	srcCh <- obj("a.txt", 10)
	close(srcCh)
	close(dstCh)
	close(srcErrCh)
	close(dstErrCh)

	out := make(chan Task, 64)
	s.mergeDiff(ctx, srcCh, srcErrCh, dstCh, dstErrCh, out)
	close(out)
	// Should not panic; result (0 or 1 task) is non-deterministic due to cancellation.
}

// ── end-to-end Syncer.Run tests ───────────────────────────────────────────────

func newSyncer(srcFiles, dstFiles map[string]string) (*Syncer, *memStorage, *memStorage) {
	src := newMemStorage("src", srcFiles)
	dst := newMemStorage("dst", dstFiles)
	opts := DefaultSyncOptions()
	opts.Transfers = 4
	opts.Checkers = 4
	opts.Retries = 0
	return NewSyncer(src, dst, opts), src, dst
}

func TestSyncer_CopyNewFiles(t *testing.T) {
	syncer, _, dst := newSyncer(
		map[string]string{"a.txt": "hello", "b.txt": "world"},
		nil,
	)
	failed := syncer.Run(context.Background())
	if failed != 0 {
		t.Fatalf("expected 0 failures, got %d", failed)
	}
	keys := dst.keys()
	if len(keys) != 2 {
		t.Fatalf("dst should have 2 files, got %v", keys)
	}
}

func TestSyncer_SkipsExistingWithIgnoreExisting(t *testing.T) {
	syncer, _, dst := newSyncer(
		map[string]string{"a.txt": "new-content"},
		map[string]string{"a.txt": "old-content"},
	)
	syncer.opts.IgnoreExisting = true

	syncer.Run(context.Background())

	dst.mu.Lock()
	content := string(dst.objects["a.txt"])
	dst.mu.Unlock()
	if content != "old-content" {
		t.Errorf("IgnoreExisting should preserve dst content, got %q", content)
	}
}

func TestSyncer_DeleteExtraAtDst(t *testing.T) {
	syncer, _, dst := newSyncer(
		map[string]string{"a.txt": "keep"},
		map[string]string{"a.txt": "keep", "extra.txt": "remove"},
	)
	syncer.opts.Delete = true

	syncer.Run(context.Background())

	dst.mu.Lock()
	_, extraExists := dst.objects["extra.txt"]
	dst.mu.Unlock()
	if extraExists {
		t.Error("extra.txt should have been deleted")
	}
}

func TestSyncer_NoDeleteByDefault(t *testing.T) {
	syncer, _, dst := newSyncer(
		map[string]string{"a.txt": "keep"},
		map[string]string{"a.txt": "keep", "extra.txt": "keep"},
	)
	// Delete is false by default

	syncer.Run(context.Background())

	dst.mu.Lock()
	_, extraExists := dst.objects["extra.txt"]
	dst.mu.Unlock()
	if !extraExists {
		t.Error("extra.txt should remain when --delete is not set")
	}
}

func TestSyncer_PropagatesSrcContent(t *testing.T) {
	syncer, _, dst := newSyncer(
		map[string]string{"data.txt": "source-data"},
		nil,
	)
	syncer.Run(context.Background())

	dst.mu.Lock()
	content := string(dst.objects["data.txt"])
	dst.mu.Unlock()
	if content != "source-data" {
		t.Errorf("content = %q, want %q", content, "source-data")
	}
}

func TestSyncer_DryRun(t *testing.T) {
	syncer, _, dst := newSyncer(
		map[string]string{"a.txt": "data"},
		nil,
	)
	syncer.opts.DryRun = true

	syncer.Run(context.Background())

	if len(dst.keys()) != 0 {
		t.Error("dry-run should not transfer any files")
	}
}

func TestSyncer_IgnoreErrors(t *testing.T) {
	src := newMemStorage("src", map[string]string{"a.txt": "a", "b.txt": "b"})
	dst := newMemStorage("dst", nil)
	dst.putErr = errors.New("disk full")

	opts := DefaultSyncOptions()
	opts.Transfers = 2
	opts.Checkers = 2
	opts.Retries = 0
	opts.IgnoreErrors = true

	syncer := NewSyncer(src, dst, opts)
	failed := syncer.Run(context.Background())
	if failed == 0 {
		t.Error("expected failures when Put returns error")
	}
}

func TestSyncer_RetryOnError(t *testing.T) {
	src := newMemStorage("src", map[string]string{"a.txt": "data"})
	dst := newMemStorage("dst", nil)

	// fail on first put, succeed on second
	// Use a custom storage that fails once then succeeds.
	flakyDst := &flakyStorage{inner: dst, failsRemaining: 1}

	opts := DefaultSyncOptions()
	opts.Transfers = 1
	opts.Checkers = 1
	opts.Retries = 2
	opts.RetriesSleep = 0

	syncer := NewSyncer(src, flakyDst, opts)
	failed := syncer.Run(context.Background())
	if failed != 0 {
		t.Errorf("expected 0 failures after retry, got %d", failed)
	}
	if !flakyDst.succeeded {
		t.Error("expected put to eventually succeed")
	}
}

// flakyStorage wraps a memStorage and fails the first N Put calls.
type flakyStorage struct {
	inner          *memStorage
	failsRemaining int
	mu             sync.Mutex
	succeeded      bool
}

func (f *flakyStorage) String() string { return f.inner.String() }
func (f *flakyStorage) List(ctx context.Context, p string) (<-chan *storage.Object, <-chan error) {
	return f.inner.List(ctx, p)
}
func (f *flakyStorage) Get(ctx context.Context, k string, off, size int64) (io.ReadCloser, error) {
	return f.inner.Get(ctx, k, off, size)
}
func (f *flakyStorage) Put(ctx context.Context, k string, r io.Reader, sz int64) error {
	f.mu.Lock()
	if f.failsRemaining > 0 {
		f.failsRemaining--
		f.mu.Unlock()
		return errors.New("transient error")
	}
	f.succeeded = true
	f.mu.Unlock()
	return f.inner.Put(ctx, k, r, sz)
}
func (f *flakyStorage) PutWithMtime(ctx context.Context, k string, r io.Reader, sz int64, _ time.Time) error {
	return f.Put(ctx, k, r, sz)
}
func (f *flakyStorage) Delete(ctx context.Context, k string) error  { return f.inner.Delete(ctx, k) }
func (f *flakyStorage) MkdirAll(ctx context.Context, k string) error { return f.inner.MkdirAll(ctx, k) }

func TestSyncer_FilesFrom(t *testing.T) {
	// Write a files-from list to a temp file.
	tmp, err := os.CreateTemp(t.TempDir(), "files-from-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(tmp, "# comment")
	fmt.Fprintln(tmp, "a.txt")
	fmt.Fprintln(tmp, "")
	fmt.Fprintln(tmp, "b.txt")
	tmp.Close()

	src := newMemStorage("src", map[string]string{"a.txt": "aaa", "b.txt": "bbb", "c.txt": "skip"})
	dst := newMemStorage("dst", nil)
	opts := DefaultSyncOptions()
	opts.Transfers = 2
	opts.Checkers = 2
	opts.Retries = 0
	opts.FilesFrom = tmp.Name()

	syncer := NewSyncer(src, dst, opts)
	failed := syncer.Run(context.Background())
	if failed != 0 {
		t.Fatalf("expected 0 failures, got %d", failed)
	}
	keys := dst.keys()
	sort.Strings(keys)
	if strings.Join(keys, ",") != "a.txt,b.txt" {
		t.Errorf("dst keys = %v, want [a.txt b.txt]", keys)
	}
}

func TestSyncer_StatsCounters(t *testing.T) {
	syncer, _, _ := newSyncer(
		map[string]string{"a.txt": "aa", "b.txt": "bbb"},
		nil,
	)
	syncer.Run(context.Background())

	if syncer.stats.FilesTransferred.Load() != 2 {
		t.Errorf("FilesTransferred = %d, want 2", syncer.stats.FilesTransferred.Load())
	}
	if syncer.stats.BytesTransferred.Load() != 5 {
		t.Errorf("BytesTransferred = %d, want 5", syncer.stats.BytesTransferred.Load())
	}
}

// ── opName tests ──────────────────────────────────────────────────────────────

func TestOpName(t *testing.T) {
	if opName(OpCopy) != "copy" {
		t.Error("OpCopy name wrong")
	}
	if opName(OpDelete) != "delete" {
		t.Error("OpDelete name wrong")
	}
	if opName(TaskOp(99)) != "unknown" {
		t.Error("unknown op name wrong")
	}
}

// ── local storage used as integration target ──────────────────────────────────

func TestSyncer_LocalToLocal(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Write source files.
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "world.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	src, err := newLocalStorage(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := newLocalStorage(dstDir)
	if err != nil {
		t.Fatal(err)
	}

	opts := DefaultSyncOptions()
	opts.Transfers = 2
	opts.Checkers = 2
	opts.Retries = 0

	syncer := NewSyncer(src, dst, opts)
	failed := syncer.Run(context.Background())
	if failed != 0 {
		t.Fatalf("expected 0 failures, got %d", failed)
	}

	for _, name := range []string{"hello.txt", "world.txt"} {
		data, err := os.ReadFile(filepath.Join(dstDir, name))
		if err != nil {
			t.Errorf("dst/%s not found: %v", name, err)
			continue
		}
		if !strings.Contains(string(data), name[:5]) {
			t.Errorf("dst/%s content %q unexpected", name, data)
		}
	}
}

// newLocalStorage is a thin wrapper so tests don't import storage directly.
func newLocalStorage(root string) (storage.Storage, error) {
	return storage.NewLocal(root)
}
