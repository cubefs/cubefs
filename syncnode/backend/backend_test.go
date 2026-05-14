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

package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeBackend is a minimal Backend implementation used to exercise the
// registry + pool logic without needing any real storage. Each fake
// records how many times it was constructed.
type fakeBackend struct {
	kind   string
	closed atomic.Bool
}

type fakeConfig struct {
	Kind string
	Fail bool
}

var fakeConstructCount int64

func newFake(cfg interface{}) (Backend, error) {
	atomic.AddInt64(&fakeConstructCount, 1)
	c, ok := cfg.(*fakeConfig)
	if !ok {
		return nil, errors.New("fakeBackend: cfg must be *fakeConfig")
	}
	if c.Fail {
		return nil, errors.New("fakeBackend: forced failure")
	}
	return &fakeBackend{kind: c.Kind}, nil
}

func (f *fakeBackend) Kind() string { return f.kind }

func (f *fakeBackend) List(ctx context.Context, prefix string, recursive bool) (<-chan Entry, error) {
	ch := make(chan Entry)
	close(ch)
	return ch, nil
}

func (f *fakeBackend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	return nil, ErrKeyNotFound
}

func (f *fakeBackend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	return 0, "", time.Time{}, ErrKeyNotFound
}

func (f *fakeBackend) Put(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (string, error) {
	return "fake-etag", nil
}

func (f *fakeBackend) Delete(ctx context.Context, key string) error  { return nil }
func (f *fakeBackend) Rename(ctx context.Context, o, n string) error { return nil }
func (f *fakeBackend) Capabilities() Caps                            { return Caps{} }
func (f *fakeBackend) Close() error                                  { f.closed.Store(true); return nil }

// Register a couple of fake kinds for testing the registry + pool. Done
// inside an init() guarded against double-registration in case multiple
// test binaries link the same package.
var fakeRegisterOnce sync.Once

func registerFakes() {
	fakeRegisterOnce.Do(func() {
		Register("fake-a", newFake)
		Register("fake-b", newFake)
	})
}

func TestRegister_AndNew(t *testing.T) {
	registerFakes()
	kinds := RegisteredKinds()
	got := map[string]bool{}
	for _, k := range kinds {
		got[k] = true
	}
	if !got["fake-a"] || !got["fake-b"] {
		t.Errorf("RegisteredKinds missing fakes: %v", kinds)
	}

	b, err := New("fake-a", &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Kind() != "fake-a" {
		t.Errorf("Kind = %q, want fake-a", b.Kind())
	}
}

func TestRegister_DuplicatePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register("dup-test", newFake)
	Register("dup-test", newFake) // should panic
}

func TestNew_UnknownKind(t *testing.T) {
	_, err := New("nonexistent-kind", nil)
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestPool_ShareSameKey(t *testing.T) {
	registerFakes()
	atomic.StoreInt64(&fakeConstructCount, 0)

	p := NewPool()
	defer p.Close()

	key := PoolKey{Kind: "fake-a"}
	cfg := &fakeConfig{Kind: "fake-a"}

	// 100 goroutines acquire the same key; constructor must run exactly once.
	const N = 100
	var wg sync.WaitGroup
	results := make([]Backend, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, err := p.Acquire(key, cfg)
			if err != nil {
				t.Errorf("goroutine %d Acquire: %v", i, err)
				return
			}
			results[i] = b
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt64(&fakeConstructCount); got != 1 {
		t.Errorf("constructor ran %d times, want exactly 1", got)
	}
	first := results[0]
	for i := 1; i < N; i++ {
		if results[i] != first {
			t.Errorf("Acquire %d returned different instance from %d (interfaces must match)", i, 0)
		}
	}
	if p.Size() != 1 {
		t.Errorf("pool size = %d, want 1", p.Size())
	}
}

func TestPool_DifferentKeysDifferentInstances(t *testing.T) {
	registerFakes()
	p := NewPool()
	defer p.Close()

	bA, _ := p.Acquire(PoolKey{Kind: "fake-a", Endpoint: "e1"}, &fakeConfig{Kind: "fake-a"})
	bB, _ := p.Acquire(PoolKey{Kind: "fake-a", Endpoint: "e2"}, &fakeConfig{Kind: "fake-a"})
	if bA == bB {
		t.Error("different endpoints should produce different instances")
	}
	if p.Size() != 2 {
		t.Errorf("pool size = %d, want 2", p.Size())
	}
}

func TestPool_ConstructorFailureRetriable(t *testing.T) {
	registerFakes()
	p := NewPool()
	defer p.Close()

	// First call: forced failure.
	_, err := p.Acquire(PoolKey{Kind: "fake-a"}, &fakeConfig{Kind: "fake-a", Fail: true})
	if err == nil {
		t.Fatal("expected error on Fail config")
	}
	// Second call: same key but good cfg. Should succeed because failed
	// entries are evicted from the pool.
	b, err := p.Acquire(PoolKey{Kind: "fake-a"}, &fakeConfig{Kind: "fake-a"})
	if err != nil {
		t.Fatalf("retry Acquire: %v", err)
	}
	if b == nil {
		t.Fatal("expected non-nil backend on retry")
	}
}

func TestPool_CloseReleasesBackends(t *testing.T) {
	registerFakes()
	p := NewPool()

	b, _ := p.Acquire(PoolKey{Kind: "fake-a"}, &fakeConfig{Kind: "fake-a"})
	fb := b.(*fakeBackend)
	if fb.closed.Load() {
		t.Fatal("backend should not be closed before pool.Close")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !fb.closed.Load() {
		t.Error("backend should be closed after pool.Close")
	}
	// Second Close is a no-op.
	if err := p.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}
	// Acquire after Close should fail.
	if _, err := p.Acquire(PoolKey{Kind: "fake-a"}, &fakeConfig{Kind: "fake-a"}); err == nil {
		t.Error("Acquire after Close should return error")
	}
}

func TestPoolKey_String(t *testing.T) {
	k := PoolKey{Kind: "s3", Endpoint: "https://s3.amazonaws.com", Region: "us-east-1"}
	s := k.String()
	if s != "s3|https://s3.amazonaws.com|us-east-1|" {
		t.Errorf("String = %q", s)
	}
}

// Use fmt to silence "unused" warnings when only Sprintf is needed in
// future tests.
var _ = fmt.Sprintf
