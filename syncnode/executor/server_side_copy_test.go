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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// serverSideCopyBackend is a test double that reports SameInstance==true
// against itself and Caps.ServerSideCopy==true; it records every
// ServerSideCopy / Get / Put call so the test can assert the executor took
// the fast path (or fell back) as expected.
//
// When `peer` is non-nil, ServerSideCopy writes the destination object
// into the peer's object map — simulating two backends pointing at the
// same realm but represented by distinct Go instances.
type serverSideCopyBackend struct {
	objects map[string][]byte
	peer    *serverSideCopyBackend // dst sink when set

	sscCalls   atomic.Int64
	getCalls   atomic.Int64
	putCalls   atomic.Int64
	failSSC    bool  // when true, ServerSideCopy returns ErrBackendUnsupported
	failSSCErr error // when set, ServerSideCopy returns this error (non-unsupported)
}

func newServerSideCopyBackend(initial map[string][]byte) *serverSideCopyBackend {
	objs := make(map[string][]byte, len(initial))
	for k, v := range initial {
		cp := make([]byte, len(v))
		copy(cp, v)
		objs[k] = cp
	}
	return &serverSideCopyBackend{objects: objs}
}

func (s *serverSideCopyBackend) Kind() string { return "s3" }

func (s *serverSideCopyBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry, len(s.objects)+1)
	go func() {
		defer close(ch)
		for k, body := range s.objects {
			select {
			case ch <- backend.Entry{Key: k, Size: int64(len(body)), Mtime: time.Unix(1700000000, 0)}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (s *serverSideCopyBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	s.getCalls.Add(1)
	body, ok := s.objects[key]
	if !ok {
		return nil, backend.ErrKeyNotFound
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (s *serverSideCopyBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	body, ok := s.objects[key]
	if !ok {
		return 0, "", time.Time{}, backend.ErrKeyNotFound
	}
	return int64(len(body)), "", time.Unix(1700000000, 0), nil
}

func (s *serverSideCopyBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	s.putCalls.Add(1)
	buf, err := io.ReadAll(body)
	if err != nil {
		return backend.PutResult{}, err
	}
	s.objects[key] = buf
	return backend.PutResult{BytesPut: int64(len(buf))}, nil
}

func (s *serverSideCopyBackend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return "", "", backend.ErrBackendUnsupported
}

func (s *serverSideCopyBackend) Delete(ctx context.Context, key string) error {
	delete(s.objects, key)
	return nil
}

func (s *serverSideCopyBackend) Rename(ctx context.Context, oldKey, newKey string) error { return nil }

func (s *serverSideCopyBackend) Capabilities() backend.Caps {
	return backend.Caps{
		RangeRead:         true,
		Multipart:         true,
		StrongConsistency: true,
		ServerSideCopy:    true,
	}
}

// SameInstance: this fake always claims "same instance" against any other
// instance of the same fake type so tests can pair src and dst.
func (s *serverSideCopyBackend) SameInstance(o backend.Backend) bool {
	_, ok := o.(*serverSideCopyBackend)
	return ok
}

func (s *serverSideCopyBackend) ServerSideCopy(ctx context.Context, srcKey, dstKey string, opts backend.PutOptions) (backend.PutResult, error) {
	s.sscCalls.Add(1)
	if s.failSSC {
		return backend.PutResult{}, backend.ErrBackendUnsupported
	}
	if s.failSSCErr != nil {
		return backend.PutResult{}, s.failSSCErr
	}
	body, ok := s.objects[srcKey]
	if !ok {
		return backend.PutResult{}, backend.ErrKeyNotFound
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	if s.peer != nil {
		s.peer.objects[dstKey] = cp
	} else {
		s.objects[dstKey] = cp
	}
	return backend.PutResult{BytesPut: int64(len(cp))}, nil
}

func (s *serverSideCopyBackend) Close() error { return nil }

// TestSyncOneFile_ServerSideCopy_FastPath: when both ends report
// SameInstance + ServerSideCopy and the task does not require strong
// checksum or mutation tracking, the executor MUST take the fast path
// (ServerSideCopy called, Get NOT called).
func TestSyncOneFile_ServerSideCopy_FastPath(t *testing.T) {
	resetServerSideCopyStats(t)

	src := newServerSideCopyBackend(map[string][]byte{
		"/data/foo.bin": []byte("payload-bytes-server-side"),
	})
	dst := newServerSideCopyBackend(nil)
	src.peer = dst

	task := &Task{
		ID:          "t-ssc",
		Type:        TaskTypeSync,
		Src:         src,
		Dst:         dst,
		SrcPath:     "/data",
		DstPath:     "/dest",
		Parallelism: 1,
	}

	e := New(WithProgressInterval(20 * time.Millisecond))
	defer e.Close()
	res := e.Run(context.Background(), task, NoopReporter{})

	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}
	if src.sscCalls.Load() != 1 {
		t.Errorf("ServerSideCopy calls = %d, want 1", src.sscCalls.Load())
	}
	if src.getCalls.Load() != 0 {
		t.Errorf("Get calls = %d, want 0 (fast path should skip Get)", src.getCalls.Load())
	}
	if dst.putCalls.Load() != 0 {
		t.Errorf("dst.Put calls = %d, want 0 (fast path should skip Put)", dst.putCalls.Load())
	}
	if got, want := dst.objects["/dest/foo.bin"], src.objects["/data/foo.bin"]; string(got) != string(want) {
		t.Errorf("dst payload = %q, want %q", got, want)
	}
	ok, fallback, errs := ServerSideCopyStats()
	if ok != 1 || fallback != 0 || errs != 0 {
		t.Errorf("stats = (ok=%d, fallback=%d, errs=%d), want (1,0,0)", ok, fallback, errs)
	}
}

// TestSyncOneFile_ServerSideCopy_FallbackOnUnsupported: when
// ServerSideCopy returns ErrBackendUnsupported, the executor MUST fall
// back to Get/Put and still complete the transfer.
func TestSyncOneFile_ServerSideCopy_FallbackOnUnsupported(t *testing.T) {
	resetServerSideCopyStats(t)

	src := newServerSideCopyBackend(map[string][]byte{
		"/data/bar.bin": []byte("fallback-payload"),
	})
	src.failSSC = true
	dst := newServerSideCopyBackend(nil)

	task := &Task{
		ID:          "t-ssc-fb",
		Type:        TaskTypeSync,
		Src:         src,
		Dst:         dst,
		SrcPath:     "/data",
		DstPath:     "/dest",
		Parallelism: 1,
	}

	e := New(WithProgressInterval(20 * time.Millisecond))
	defer e.Close()
	res := e.Run(context.Background(), task, NoopReporter{})

	if res.Status != StatusDone {
		t.Fatalf("Status=%v Error=%v", res.Status, res.Error)
	}
	if src.sscCalls.Load() != 1 {
		t.Errorf("ServerSideCopy calls = %d, want 1", src.sscCalls.Load())
	}
	if src.getCalls.Load() == 0 {
		t.Errorf("expected Get to be called as fallback, got 0")
	}
	if dst.putCalls.Load() == 0 {
		t.Errorf("expected Put to be called as fallback, got 0")
	}
	ok, fallback, errs := ServerSideCopyStats()
	if ok != 0 || fallback != 1 || errs != 0 {
		t.Errorf("stats = (ok=%d, fallback=%d, errs=%d), want (0,1,0)", ok, fallback, errs)
	}
}

// TestSyncOneFile_ServerSideCopy_SkippedInStrongMode: strong checksum
// mode cannot recompute sha256 server-side, so the fast path must be
// skipped entirely (ServerSideCopy NEVER called).
func TestSyncOneFile_ServerSideCopy_SkippedInStrongMode(t *testing.T) {
	resetServerSideCopyStats(t)

	src := newServerSideCopyBackend(map[string][]byte{
		"/data/baz.bin": []byte("strong-mode-payload"),
	})
	dst := newServerSideCopyBackend(nil)

	task := &Task{
		ID:           "t-ssc-strong",
		Type:         TaskTypeSync,
		Src:          src,
		Dst:          dst,
		SrcPath:      "/data",
		DstPath:      "/dest",
		Parallelism:  1,
		ChecksumMode: "strong",
	}

	e := New(WithProgressInterval(20 * time.Millisecond))
	defer e.Close()
	res := e.Run(context.Background(), task, NoopReporter{})

	// Strong mode requires GetChecksum on dst; our fake returns
	// ErrBackendUnsupported there, so the task will likely Fail — but the
	// invariant we care about is the fast path was NOT attempted.
	_ = res
	if src.sscCalls.Load() != 0 {
		t.Errorf("ServerSideCopy calls = %d, want 0 in strong mode", src.sscCalls.Load())
	}
}

// resetServerSideCopyStats zeroes the package-level counters so each test
// starts from a clean slate. Registered as a cleanup so a failing test
// doesn't poison the next one.
func resetServerSideCopyStats(t *testing.T) {
	t.Helper()
	serverSideCopyOK.Store(0)
	serverSideCopyFallback.Store(0)
	serverSideCopyErr.Store(0)
	t.Cleanup(func() {
		serverSideCopyOK.Store(0)
		serverSideCopyFallback.Store(0)
		serverSideCopyErr.Store(0)
	})
}

// ensure errors is referenced even if test bodies stop using it.
var _ = errors.Is
