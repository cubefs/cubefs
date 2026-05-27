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
	ino    uint64
	mode   uint32
	uid    uint32
	gid    uint32
	data   []byte
	mtime  time.Time
	xattrs map[string][]byte
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

func (fs *fakeFS) Create_ll(parentID uint64, name string, mode, uid, gid uint32, _ []byte, _ string, ignoreExist bool) (*proto.InodeInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, ok := fs.dirs[parentID]
	if !ok {
		return nil, syscall.ENOENT
	}
	if existing, ok := dir.children[name]; ok {
		if ignoreExist {
			if f, ok := fs.files[existing]; ok {
				return &proto.InodeInfo{Inode: existing, Mode: f.mode, Uid: f.uid, Gid: f.gid, Size: uint64(len(f.data)), ModifyTime: f.mtime}, nil
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
	fs.files[ino] = &fakeFile{ino: ino, mode: mode, uid: uid, gid: gid, mtime: now}
	dir.children[name] = ino
	return &proto.InodeInfo{Inode: ino, Mode: mode, Uid: uid, Gid: gid, ModifyTime: now}, nil
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
		return &proto.InodeInfo{Inode: ino, Mode: f.mode, Uid: f.uid, Gid: f.gid, Size: uint64(len(f.data)), ModifyTime: f.mtime}, nil
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
			out = append(out, &proto.InodeInfo{Inode: ino, Mode: f.mode, Uid: f.uid, Gid: f.gid, Size: uint64(len(f.data)), ModifyTime: f.mtime})
		}
	}
	return out
}

// Setattr matches metaClient. Honors the per-bit AttrXxx flags so a single
// batched call (the production Put path) updates mode/uid/gid/mtime
// atomically on the addressed inode. Real metanode stores ModifyTime as
// unix-seconds — we do the same for fidelity.
func (fs *fakeFS) Setattr(ino uint64, valid, mode, uid, gid uint32, atime, mtime int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[ino]
	if !ok {
		return syscall.ENOENT
	}
	if valid&proto.AttrMode != 0 {
		f.mode = mode
	}
	if valid&proto.AttrUid != 0 {
		f.uid = uid
	}
	if valid&proto.AttrGid != 0 {
		f.gid = gid
	}
	if valid&proto.AttrModifyTime != 0 {
		f.mtime = time.Unix(mtime, 0)
	}
	_ = atime // atime not exercised by syncnode tests
	return nil
}

// XAttrSet_ll writes one xattr on the inode. Empty name is rejected the way
// real metanode rejects it (in production cfs.Put skips empty names defensively).
func (fs *fakeFS) XAttrSet_ll(ino uint64, name, value []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[ino]
	if !ok {
		return syscall.ENOENT
	}
	if len(name) == 0 {
		return syscall.EINVAL
	}
	if f.xattrs == nil {
		f.xattrs = make(map[string][]byte)
	}
	cpy := make([]byte, len(value))
	copy(cpy, value)
	f.xattrs[string(name)] = cpy
	return nil
}

// XAttrGetAll_ll returns every xattr defined on the inode. proto.XAttrInfo
// stores values as strings, so binary bytes are round-tripped through
// string() — the production Stat() converts them back to []byte (which is
// a pure cast that preserves all bytes, including non-UTF-8).
func (fs *fakeFS) XAttrGetAll_ll(ino uint64) (*proto.XAttrInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[ino]
	if !ok {
		return nil, syscall.ENOENT
	}
	out := &proto.XAttrInfo{Inode: ino, XAttrs: map[string]string{}}
	for k, v := range f.xattrs {
		out.XAttrs[k] = string(v)
	}
	return out, nil
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

// Compile-time check: fakeFS must satisfy both interfaces.
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

// TestPutPreservesMtime verifies that PutOptions.Mtime causes the backend to
// call Setattr(AttrModifyTime) and that a subsequent Head returns the same
// mtime (truncated to whole seconds because the metanode wire format stores
// ModifyTime as int64 unix-seconds).
func TestPutPreservesMtime(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()

	// Use a whole-second timestamp so the second-precision wire truncation
	// is exact rather than approximate.
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	payload := []byte("cfs mtime preservation payload")
	if _, err := b.Put(ctx, "/dir/mtime.bin", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{Mtime: &want}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, _, got, err := b.Head(ctx, "/dir/mtime.bin")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("Head mtime = %s, want %s", got, want)
	}
}

// TestPutMtimeSubSecondTruncation documents that CFS stores ModifyTime as
// whole unix-seconds on the wire, so sub-second precision from
// PutOptions.Mtime is lost (this is a real CFS constraint, not a fakeFS
// quirk — see metanode/partition_op_inode.go).
func TestPutMtimeSubSecondTruncation(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()

	want := time.Date(2024, 1, 2, 3, 4, 5, 987654321, time.UTC)
	payload := []byte("subsecond truncation payload")
	if _, err := b.Put(ctx, "/dir/sub.bin", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{Mtime: &want}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, _, got, err := b.Head(ctx, "/dir/sub.bin")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	wantTruncated := time.Unix(want.Unix(), 0)
	if !got.Equal(wantTruncated) {
		t.Errorf("Head mtime = %s, want %s (truncated to seconds)", got, wantTruncated)
	}
	if got.Equal(want) {
		t.Errorf("CFS unexpectedly preserved sub-second precision (got = want = %s) — backend contract change?", got)
	}
}

// statBackend asserts cfs.Backend implements backend.Stater and returns the
// narrowed interface for use in the metadata-round-trip tests.
func statBackend(t *testing.T, b *Backend) backend.Stater {
	t.Helper()
	s, ok := backend.Backend(b).(backend.Stater)
	if !ok {
		t.Fatalf("cfs.Backend must implement backend.Stater")
	}
	return s
}

// TestPutPreservesMode round-trips a few representative permission modes
// (including setuid) through Put → Stat to verify proto.AttrMode is honored.
func TestPutPreservesMode(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	stater := statBackend(t, b)

	cases := []uint32{0o600, 0o640, 0o644, 0o755, 0o4755}
	for _, mode := range cases {
		key := "/mode/" + modeToOctal(mode)
		body := []byte("mode-test " + key)
		modeIn := mode
		if _, err := b.Put(ctx, key, bytes.NewReader(body), int64(len(body)), backend.PutOptions{Mode: &modeIn}); err != nil {
			t.Fatalf("Put(%s, mode=%o): %v", key, mode, err)
		}
		st, err := stater.Stat(ctx, key)
		if err != nil {
			t.Fatalf("Stat(%s): %v", key, err)
		}
		if st.Mode == nil {
			t.Fatalf("Stat(%s): Mode is nil", key)
		}
		if got := *st.Mode & 0o7777; got != mode {
			t.Errorf("Stat(%s): mode = %o, want %o", key, got, mode)
		}
	}
}

// TestPutPreservesOwner verifies uid/gid round-trip through Setattr+Stat.
// CFS stores both as uint32 on the inode, so we exercise non-zero values
// that survive an explicit PutOptions{UID, GID}.
func TestPutPreservesOwner(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	stater := statBackend(t, b)

	uid := uint32(1000)
	gid := uint32(2000)
	body := []byte("owner-test")
	if _, err := b.Put(ctx, "/owner/file", bytes.NewReader(body), int64(len(body)), backend.PutOptions{UID: &uid, GID: &gid}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st, err := stater.Stat(ctx, "/owner/file")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.UID == nil || *st.UID != uid {
		t.Errorf("UID = %v, want %d", st.UID, uid)
	}
	if st.GID == nil || *st.GID != gid {
		t.Errorf("GID = %v, want %d", st.GID, gid)
	}
}

// TestPutPreservesXattr writes a few xattrs (including one with binary
// non-UTF-8 bytes and one with an empty value) and verifies they all
// round-trip through XAttrSet_ll + XAttrGetAll_ll.
func TestPutPreservesXattr(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	stater := statBackend(t, b)

	xattrs := map[string][]byte{
		"user.syncnode.text":   []byte("hello"),
		"user.syncnode.binary": {0x00, 0x01, 0xff, 0xfe, 0x80},
		"user.syncnode.empty":  {},
	}
	body := []byte("xattr-test")
	if _, err := b.Put(ctx, "/xattr/file", bytes.NewReader(body), int64(len(body)), backend.PutOptions{Xattrs: xattrs}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st, err := stater.Stat(ctx, "/xattr/file")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	for name, want := range xattrs {
		got, ok := st.Xattrs[name]
		if !ok {
			t.Errorf("xattr %q missing from Stat", name)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("xattr %q = %v, want %v", name, got, want)
		}
	}
}

// TestPutPreservesAll combines mode + owner + xattr + mtime in a single
// Put to catch ordering bugs — e.g. an out-of-order Setattr that chmod's
// after chown and silently drops a setuid bit, or a flush-before-Setattr
// race that writes mtime after metanode has already touched the inode.
func TestPutPreservesAll(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	stater := statBackend(t, b)

	mode := uint32(0o4755) // setuid + rwxr-xr-x
	uid := uint32(1234)
	gid := uint32(5678)
	mt := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	xattrs := map[string][]byte{
		"user.syncnode.kind": []byte("combined"),
		"user.syncnode.bin":  {0xde, 0xad, 0xbe, 0xef},
	}
	body := []byte("combined-test")
	if _, err := b.Put(ctx, "/all/file", bytes.NewReader(body), int64(len(body)), backend.PutOptions{
		Mode:   &mode,
		UID:    &uid,
		GID:    &gid,
		Mtime:  &mt,
		Xattrs: xattrs,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st, err := stater.Stat(ctx, "/all/file")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Mode == nil || (*st.Mode&0o7777) != mode {
		t.Errorf("mode mismatch: got %v want %o", st.Mode, mode)
	}
	if st.UID == nil || *st.UID != uid {
		t.Errorf("uid mismatch: got %v want %d", st.UID, uid)
	}
	if st.GID == nil || *st.GID != gid {
		t.Errorf("gid mismatch: got %v want %d", st.GID, gid)
	}
	if !st.Mtime.Equal(mt) {
		t.Errorf("mtime mismatch: got %s want %s", st.Mtime, mt)
	}
	for name, want := range xattrs {
		got, ok := st.Xattrs[name]
		if !ok || !bytes.Equal(got, want) {
			t.Errorf("xattr %q ok=%v got=%v want=%v", name, ok, got, want)
		}
	}
}

// TestStat_NotFound verifies the Stat path translates ENOENT to
// backend.ErrKeyNotFound so the executor's "src disappeared" branch can fire.
func TestStat_NotFound(t *testing.T) {
	b, _ := newTestBackend(t)
	stater := statBackend(t, b)
	if _, err := stater.Stat(context.Background(), "/missing/file"); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

// modeToOctal renders a permission mode for use in a CFS key. Lifted from
// the local-backend test helper so we can build deterministic key names
// without depending on fmt.Sprintf's order.
func modeToOctal(m uint32) string {
	const digits = "01234567"
	var buf [6]byte
	i := len(buf) - 1
	for m > 0 && i >= 0 {
		buf[i] = digits[m&7]
		m >>= 3
		i--
	}
	out := string(bytes.TrimLeft(buf[:], "\x00"))
	if out == "" {
		out = "0"
	}
	return out
}
