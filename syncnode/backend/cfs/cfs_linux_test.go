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

//go:build linux

package cfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/syncnode/backend"
)

// fakeFile is one inode/file in the fake meta+extent storage.
type fakeFile struct {
	ino   uint64
	mode  uint32
	data  []byte
	mtime time.Time
}

// fakeDir is one directory in the fake filesystem.
type fakeDir struct {
	ino      uint64
	children map[string]uint64 // name -> child inode
}

// fakeFS is a minimal in-memory filesystem implementing metaClient +
// extentAPI. It's NOT thread-safe for arbitrary mutations, but each test
// works on its own *fakeFS instance and ec.Write is protected by a mutex
// so concurrent Write goroutines from writeParallel are safe.
type fakeFS struct {
	mu        sync.Mutex
	nextIno   uint64
	dirs      map[uint64]*fakeDir // ino -> dir
	files     map[uint64]*fakeFile
	openInos  map[uint64]struct{} // currently-open streams

	// Instrumentation counters used by tests.
	writeCalls    atomic.Int64
	flushCalls    atomic.Int64
	closeCalls    atomic.Int64
	truncateCalls atomic.Int64

	// For parallel-write detection: count concurrent in-flight Writes.
	inFlightWrites atomic.Int64
	maxInFlight    atomic.Int64
	// Optional sleep injected into Write to widen the concurrency window
	// for the parallel-write test.
	writeDelay time.Duration
}

func newFakeFS() *fakeFS {
	fs := &fakeFS{
		nextIno:  100, // reserve 0..99 for fixed inodes
		dirs:     map[uint64]*fakeDir{},
		files:    map[uint64]*fakeFile{},
		openInos: map[uint64]struct{}{},
	}
	// Root directory is always inode 1 (proto.RootIno).
	fs.dirs[proto.RootIno] = &fakeDir{ino: proto.RootIno, children: map[string]uint64{}}
	return fs
}

func (fs *fakeFS) allocIno() uint64 {
	fs.nextIno++
	return fs.nextIno
}

// metaClient implementation -------------------------------------------------

func (fs *fakeFS) LookupPath(p string) (uint64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if p == "" || p == "/" {
		return proto.RootIno, nil
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	ino := proto.RootIno
	for _, part := range parts {
		dir, ok := fs.dirs[ino]
		if !ok {
			return 0, syscall.ENOENT
		}
		child, ok := dir.children[part]
		if !ok {
			return 0, syscall.ENOENT
		}
		ino = child
	}
	return ino, nil
}

func (fs *fakeFS) Lookup_ll(parentID uint64, name string) (uint64, uint32, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, ok := fs.dirs[parentID]
	if !ok {
		return 0, 0, syscall.ENOENT
	}
	child, ok := dir.children[name]
	if !ok {
		return 0, 0, syscall.ENOENT
	}
	if _, isDir := fs.dirs[child]; isDir {
		return child, modeDir, nil
	}
	if f, ok := fs.files[child]; ok {
		return child, f.mode, nil
	}
	return 0, 0, syscall.ENOENT
}

// modeDir is the directory mode bits as the SDK uses them: 0755 | os.ModeDir
// (Go's high-bit dir flag, which proto.IsDir checks against).
var modeDir = uint32(0o755) | uint32(os.ModeDir)

func (fs *fakeFS) Create_ll(parentID uint64, name string, mode, _, _ uint32, _ []byte, _ string, ignoreExist bool) (*proto.InodeInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, ok := fs.dirs[parentID]
	if !ok {
		return nil, syscall.ENOENT
	}
	if existing, ok := dir.children[name]; ok {
		if ignoreExist {
			if f, ok := fs.files[existing]; ok {
				return &proto.InodeInfo{Inode: existing, Mode: f.mode, Size: uint64(len(f.data)), ModifyTime: f.mtime}, nil
			}
			return &proto.InodeInfo{Inode: existing, Mode: modeDir}, nil
		}
		return nil, syscall.EEXIST
	}
	ino := fs.allocIno()
	if isDirMode(mode) {
		fs.dirs[ino] = &fakeDir{ino: ino, children: map[string]uint64{}}
		dir.children[name] = ino
		return &proto.InodeInfo{Inode: ino, Mode: modeDir}, nil
	}
	now := time.Now()
	fs.files[ino] = &fakeFile{ino: ino, mode: mode, mtime: now}
	dir.children[name] = ino
	return &proto.InodeInfo{Inode: ino, Mode: mode, ModifyTime: now}, nil
}

// isDirMode mirrors proto.IsDir over the raw uint32 mode bits.
func isDirMode(mode uint32) bool { return proto.IsDir(mode) }

func (fs *fakeFS) InodeGet_ll(ino uint64) (*proto.InodeInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if d, ok := fs.dirs[ino]; ok {
		_ = d
		return &proto.InodeInfo{Inode: ino, Mode: modeDir}, nil
	}
	if f, ok := fs.files[ino]; ok {
		return &proto.InodeInfo{Inode: ino, Mode: f.mode, Size: uint64(len(f.data)), ModifyTime: f.mtime}, nil
	}
	return nil, syscall.ENOENT
}

func (fs *fakeFS) Delete_ll(parentID uint64, name string, _ bool, _ string) (*proto.InodeInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, ok := fs.dirs[parentID]
	if !ok {
		return nil, syscall.ENOENT
	}
	child, ok := dir.children[name]
	if !ok {
		return nil, syscall.ENOENT
	}
	delete(dir.children, name)
	if _, ok := fs.files[child]; ok {
		delete(fs.files, child)
	}
	if _, ok := fs.dirs[child]; ok {
		delete(fs.dirs, child)
	}
	return &proto.InodeInfo{Inode: child}, nil
}

func (fs *fakeFS) Rename_ll(srcParent uint64, srcName string, dstParent uint64, dstName string, _, _ string, _ bool) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	sd, ok := fs.dirs[srcParent]
	if !ok {
		return syscall.ENOENT
	}
	dd, ok := fs.dirs[dstParent]
	if !ok {
		return syscall.ENOENT
	}
	child, ok := sd.children[srcName]
	if !ok {
		return syscall.ENOENT
	}
	delete(sd.children, srcName)
	dd.children[dstName] = child
	return nil
}

func (fs *fakeFS) ReadDir_ll(parent uint64) ([]proto.Dentry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, ok := fs.dirs[parent]
	if !ok {
		return nil, syscall.ENOENT
	}
	out := make([]proto.Dentry, 0, len(dir.children))
	for name, child := range dir.children {
		var typ uint32
		if _, ok := fs.dirs[child]; ok {
			typ = modeDir
		} else if f, ok := fs.files[child]; ok {
			typ = f.mode
		}
		out = append(out, proto.Dentry{Name: name, Inode: child, Type: typ})
	}
	return out, nil
}

func (fs *fakeFS) BatchInodeGet(inos []uint64) []*proto.InodeInfo {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var out []*proto.InodeInfo
	for _, ino := range inos {
		if d, ok := fs.dirs[ino]; ok {
			_ = d
			out = append(out, &proto.InodeInfo{Inode: ino, Mode: modeDir})
		} else if f, ok := fs.files[ino]; ok {
			out = append(out, &proto.InodeInfo{Inode: ino, Mode: f.mode, Size: uint64(len(f.data)), ModifyTime: f.mtime})
		}
	}
	return out
}

// Setattr matches metaClient — currently the backend doesn't call it, but
// the interface declares it so the fake must implement it.
func (fs *fakeFS) Setattr(_ uint64, _, _, _, _ uint32, _, _ int64) error {
	return nil
}

func (fs *fakeFS) Close() error { return nil }

// extentAPI implementation --------------------------------------------------

func (fs *fakeFS) OpenStream(ino uint64, _, _ bool, _ string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[ino]; !ok {
		return syscall.ENOENT
	}
	fs.openInos[ino] = struct{}{}
	return nil
}

func (fs *fakeFS) CloseStream(ino uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.openInos, ino)
	fs.closeCalls.Add(1)
	return nil
}

func (fs *fakeFS) Read(ino uint64, p []byte, off int, _ int, _ uint32, _ bool) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[ino]
	if !ok {
		return 0, syscall.ENOENT
	}
	if off >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (fs *fakeFS) Write(ino uint64, off int, data []byte, _ int, _ func() error, _ uint32, _ bool, _ bool) (int, error) {
	cur := fs.inFlightWrites.Add(1)
	defer fs.inFlightWrites.Add(-1)
	// Record peak concurrency.
	for {
		prev := fs.maxInFlight.Load()
		if cur <= prev || fs.maxInFlight.CompareAndSwap(prev, cur) {
			break
		}
	}
	if fs.writeDelay > 0 {
		time.Sleep(fs.writeDelay)
	}
	fs.writeCalls.Add(1)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[ino]
	if !ok {
		return 0, syscall.ENOENT
	}
	end := off + len(data)
	if cap(f.data) < end {
		grown := make([]byte, end)
		copy(grown, f.data)
		f.data = grown
	} else {
		f.data = f.data[:end]
	}
	copy(f.data[off:end], data)
	f.mtime = time.Now()
	return len(data), nil
}

func (fs *fakeFS) Flush(_ uint64) error {
	fs.flushCalls.Add(1)
	return nil
}

func (fs *fakeFS) Truncate(_ *meta.MetaWrapper, _, ino uint64, size int, _ string) error {
	fs.truncateCalls.Add(1)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[ino]
	if !ok {
		return syscall.ENOENT
	}
	if size == 0 {
		f.data = f.data[:0]
		return nil
	}
	if size < len(f.data) {
		f.data = f.data[:size]
	} else {
		grown := make([]byte, size)
		copy(grown, f.data)
		f.data = grown
	}
	return nil
}

// Compile-time check: fakeFS must satisfy both interfaces. Note: Setattr's
// real signature matches the interface; the SetattrFixed helper above
// exists only to document that the right shape is on Setattr.
var (
	_ metaClient = (*fakeFS)(nil)
	_ extentAPI  = (*fakeFS)(nil)
)

// newTestBackend builds a Backend with the fake fs injected.
func newTestBackend(t *testing.T) (*Backend, *fakeFS) {
	t.Helper()
	fs := newFakeFS()
	cfg := &Config{
		Masters:       []string{"unused"},
		Volume:        "test",
		WriteChunkMiB: 1, // small chunks so even modest test data goes parallel
		WriteParallel: 4,
	}
	b := newWithDeps(cfg, fs, fs, 0)
	t.Cleanup(func() { _ = b.Close() })
	return b, fs
}

// ---- Tests ----------------------------------------------------------------

func TestNew_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  interface{}
	}{
		{"nil", nil},
		{"wrong-type", &struct{ X int }{}},
		{"missing-masters", &Config{Volume: "v"}},
		{"missing-volume", &Config{Masters: []string{"m"}}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			b, err := New(c.cfg)
			if b != nil {
				t.Fatalf("expected nil backend, got %v", b)
			}
			if !errors.Is(err, backend.ErrConfigInvalid) {
				t.Errorf("expected ErrConfigInvalid, got %v", err)
			}
		})
	}
}

func TestCapabilities(t *testing.T) {
	b, _ := newTestBackend(t)
	caps := b.Capabilities()
	if !caps.RangeRead {
		t.Error("RangeRead should be true")
	}
	if caps.Multipart {
		t.Error("Multipart should be false")
	}
	if !caps.AtomicRename {
		t.Error("AtomicRename should be true")
	}
	if !caps.StrongConsistency {
		t.Error("StrongConsistency should be true")
	}
	if caps.ListMaxKeys != 0 {
		t.Errorf("ListMaxKeys = %d, want 0", caps.ListMaxKeys)
	}
}

func TestKind(t *testing.T) {
	b, _ := newTestBackend(t)
	if got := b.Kind(); got != "cfs" {
		t.Errorf("Kind = %q, want cfs", got)
	}
}

func TestPutGetSmall(t *testing.T) {
	b, fs := newTestBackend(t)
	ctx := context.Background()

	payload := []byte("hello cubefs")
	_, err := b.Put(ctx, "/dir/a.txt", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if fs.writeCalls.Load() == 0 {
		t.Error("expected at least one Write call")
	}
	if fs.flushCalls.Load() == 0 {
		t.Error("expected Flush to be called")
	}
	if fs.closeCalls.Load() == 0 {
		t.Error("expected CloseStream to be called")
	}

	r, err := b.Get(ctx, "/dir/a.txt", 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Get returned %q, want %q", got, payload)
	}
}

func TestPutOverwrite(t *testing.T) {
	b, fs := newTestBackend(t)
	ctx := context.Background()

	_, err := b.Put(ctx, "/file", bytes.NewReader([]byte("first")), 5, backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	_, err = b.Put(ctx, "/file", bytes.NewReader([]byte("second-longer")), 13, backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	// Truncate should have been called twice (once per Put).
	if got := fs.truncateCalls.Load(); got < 2 {
		t.Errorf("Truncate called %d times, want >= 2", got)
	}

	r, _ := b.Get(ctx, "/file", 0, 0)
	defer r.Close()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, []byte("second-longer")) {
		t.Errorf("overwrite did not replace content, got %q", got)
	}
}

func TestPutParallelWrites(t *testing.T) {
	// Acceptance: for a body > parallelWriteMinBytes (16 MiB) the Write
	// goroutines must actually overlap. We instrument fakeFS to record
	// peak in-flight Write calls; with the parallel path engaged the
	// peak should be > 1.
	b, fs := newTestBackend(t)
	fs.writeDelay = 5 * time.Millisecond // widen the concurrency window
	ctx := context.Background()

	// 20 MiB > 16 MiB threshold → parallel path.
	const size = 20 * 1024 * 1024
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	_, err := b.Put(ctx, "/big.bin", bytes.NewReader(payload), int64(size), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	peak := fs.maxInFlight.Load()
	if peak < 2 {
		t.Errorf("expected concurrent Write peak >= 2, got %d (writeCalls=%d)",
			peak, fs.writeCalls.Load())
	}

	// Verify the bytes round-trip.
	r, err := b.Get(ctx, "/big.bin", 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(payload))
	}
}

func TestPutSequentialBelowThreshold(t *testing.T) {
	// For small bodies the sequential path is used. The peak in-flight
	// count should be 1.
	b, fs := newTestBackend(t)
	fs.writeDelay = 2 * time.Millisecond
	ctx := context.Background()

	// 1 MiB << 16 MiB threshold.
	payload := make([]byte, 1*1024*1024)
	_, _ = rand.Read(payload)
	_, err := b.Put(ctx, "/small.bin", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	peak := fs.maxInFlight.Load()
	if peak > 1 {
		t.Errorf("expected sequential peak == 1, got %d", peak)
	}
}

func TestPutConcurrentMultiFile(t *testing.T) {
	// AC line: "同时 10 个文件并发写无错". We launch 10 parallel Puts on
	// 10 distinct keys and verify every byte round-trips.
	b, _ := newTestBackend(t)
	ctx := context.Background()

	const files = 10
	const each = 4 * 1024 * 1024
	payloads := make([][]byte, files)
	for i := range payloads {
		p := make([]byte, each)
		_, _ = rand.Read(p)
		payloads[i] = p
	}
	var wg sync.WaitGroup
	errs := make([]error, files)
	for i := 0; i < files; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := b.Put(ctx, fmt.Sprintf("/multi/f%d.bin", i),
				bytes.NewReader(payloads[i]), int64(each), backend.PutOptions{})
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Errorf("Put %d: %v", i, e)
		}
	}
	for i := 0; i < files; i++ {
		r, err := b.Get(ctx, fmt.Sprintf("/multi/f%d.bin", i), 0, 0)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatalf("ReadAll %d: %v", i, err)
		}
		if !bytes.Equal(got, payloads[i]) {
			t.Errorf("file %d mismatch", i)
		}
	}
}

func TestGetRange(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	payload := []byte("0123456789ABCDEFGHIJ")
	_, err := b.Put(ctx, "/r", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Read bytes [5, 10).
	r, err := b.Get(ctx, "/r", 5, 5)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if string(got) != "56789" {
		t.Errorf("range read got %q, want %q", got, "56789")
	}
}

func TestHead(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	payload := []byte("metadata-test")
	_, _ = b.Put(ctx, "/h", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{})

	size, etag, mtime, err := b.Head(ctx, "/h")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", size, len(payload))
	}
	if etag != "" {
		t.Errorf("etag must be empty for cfs, got %q", etag)
	}
	if mtime.IsZero() {
		t.Error("mtime must be set")
	}
}

func TestHead_NotFound(t *testing.T) {
	b, _ := newTestBackend(t)
	_, _, _, err := b.Head(context.Background(), "/missing")
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	b, fs := newTestBackend(t)
	ctx := context.Background()
	_, _ = b.Put(ctx, "/d", bytes.NewReader([]byte("x")), 1, backend.PutOptions{})

	if err := b.Delete(ctx, "/d"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Idempotent: second delete of same path is a no-op.
	if err := b.Delete(ctx, "/d"); err != nil {
		t.Errorf("Delete idempotent: %v", err)
	}
	// And of a never-existed path.
	if err := b.Delete(ctx, "/never"); err != nil {
		t.Errorf("Delete missing: %v", err)
	}
	// Confirm file is gone from fake storage too.
	if _, ok := fs.files[100+1]; ok {
		t.Error("file should be gone")
	}
}

func TestRename(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	_, _ = b.Put(ctx, "/src/file", bytes.NewReader([]byte("payload")), 7, backend.PutOptions{})

	if err := b.Rename(ctx, "/src/file", "/dst/new"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// Old path gone.
	_, _, _, err := b.Head(ctx, "/src/file")
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("old path should be missing, got %v", err)
	}
	// New path readable.
	r, err := b.Get(ctx, "/dst/new", 0, 0)
	if err != nil {
		t.Fatalf("Get new: %v", err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if string(got) != "payload" {
		t.Errorf("renamed file content = %q, want payload", got)
	}
}

func TestList(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	_, _ = b.Put(ctx, "/lst/a", bytes.NewReader([]byte("a")), 1, backend.PutOptions{})
	_, _ = b.Put(ctx, "/lst/b", bytes.NewReader([]byte("bb")), 2, backend.PutOptions{})
	_, _ = b.Put(ctx, "/lst/sub/c", bytes.NewReader([]byte("ccc")), 3, backend.PutOptions{})

	// Non-recursive: only immediate children of /lst.
	ch, err := b.List(ctx, "/lst", false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var names []string
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("List entry err: %v", e.Err)
		}
		names = append(names, e.Key)
	}
	if len(names) != 3 {
		t.Errorf("non-recursive returned %d entries, want 3 (a, b, sub): %v", len(names), names)
	}

	// Recursive: also walks into sub/.
	ch, err = b.List(ctx, "/lst", true)
	if err != nil {
		t.Fatalf("List recursive: %v", err)
	}
	var rec []string
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("List rec err: %v", e.Err)
		}
		rec = append(rec, e.Key)
	}
	if len(rec) != 4 {
		t.Errorf("recursive returned %d entries, want 4 (a, b, sub, sub/c): %v", len(rec), rec)
	}
}

func TestList_MissingPrefix(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	ch, err := b.List(ctx, "/nope", false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("missing prefix should yield no entries, got %d", count)
	}
}

func TestClose_Idempotent(t *testing.T) {
	b, _ := newTestBackend(t)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close should be no-op: %v", err)
	}
}

// Sanity check: make sure the package import of stream is still needed —
// the real constructor uses it via stream.NewExtentClient. The test
// doesn't exercise that code path (it uses newWithDeps) but the import
// MUST stay so cross-compilation linkage is right.
var _ = stream.ExtentConfig{}
