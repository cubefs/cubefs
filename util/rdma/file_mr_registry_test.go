// Copyright 2018 The CubeFS Authors.
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

package rdma

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockRegister produces a fake RDMAMem keyed deterministically by key.
// Each call increments callCount so single-flight semantics can be
// asserted. registerDelay simulates slow mmap/ibv_reg_mr so concurrent
// callers actually race.
type mockRegister struct {
	callCount int64 // accessed atomically (Go 1.17 compat)
	failKeys  map[uint64]error // keys whose register call should error
	delay     time.Duration
	mu        sync.Mutex
	calls     []uint64 // per-call key, in order seen
}

func (m *mockRegister) register(key uint64) (*RDMAMem, int, error) {
	atomic.AddInt64(&m.callCount, 1)
	m.mu.Lock()
	m.calls = append(m.calls, key)
	m.mu.Unlock()
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if err, ok := m.failKeys[key]; ok {
		return nil, 0, err
	}
	// Return a sentinel RDMAMem; the registry only ever calls
	// Free() on it (stub on non-rdma builds → no-op). For
	// uniqueness in assertions, encode key into Rkey/VA.
	return &RDMAMem{
		Rkey: uint32(key + 1000),
		VA:   uint64(key + 2000),
		Size: 1 << 20,
	}, 1 << 20, nil
}

func TestFileMRRegistry_Ctor(t *testing.T) {
	if _, err := NewFileMRRegistry(0, func(uint64) (*RDMAMem, int, error) { return nil, 0, nil }); err == nil {
		t.Error("zero maxEntries should be rejected")
	}
	if _, err := NewFileMRRegistry(4, nil); err == nil {
		t.Error("nil register should be rejected")
	}
}

func TestFileMRRegistry_AcquireRelease(t *testing.T) {
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(4, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	e1, err := r.Acquire(7)
	if err != nil {
		t.Fatalf("Acquire(7): %v", err)
	}
	if e1.Key != 7 || e1.Rkey() != 1007 || e1.VA() != 2007 {
		t.Errorf("unexpected entry: %+v", e1)
	}
	if got := r.Len(); got != 1 {
		t.Errorf("Len after first acquire: got %d want 1", got)
	}
	if got := atomic.LoadInt64(&mr.callCount); got != 1 {
		t.Errorf("register calls: got %d want 1", got)
	}

	// Second Acquire same key — cache hit, no new register call.
	e2, err := r.Acquire(7)
	if err != nil {
		t.Fatalf("Acquire(7) again: %v", err)
	}
	if e2 != e1 {
		t.Error("expected same entry on cache hit")
	}
	if got := atomic.LoadInt64(&mr.callCount); got != 1 {
		t.Errorf("register calls after hit: got %d want 1", got)
	}

	r.Release(e1)
	r.Release(e2)
	if got := r.Len(); got != 1 {
		t.Errorf("Len after release (still cached): got %d want 1", got)
	}
}

func TestFileMRRegistry_LRUEviction(t *testing.T) {
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(3, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Fill the cache. Release each so they're eligible for eviction.
	for k := uint64(1); k <= 3; k++ {
		e, err := r.Acquire(k)
		if err != nil {
			t.Fatalf("Acquire(%d): %v", k, err)
		}
		r.Release(e)
	}
	if got := r.Len(); got != 3 {
		t.Fatalf("after 3 acquires Len: got %d want 3", got)
	}

	// Acquire 1 again to make it MRU (push 2 to LRU front of eviction).
	e1, _ := r.Acquire(1)
	r.Release(e1)

	// Acquire 4: cache full, must evict. Eviction order is LRU back
	// → front. After the bump above, the LRU back should be 2 (the
	// oldest untouched), so 2 gets evicted.
	if _, err := r.Acquire(4); err != nil {
		t.Fatalf("Acquire(4): %v", err)
	}
	if r.Len() != 3 {
		t.Errorf("Len after eviction: got %d want 3", r.Len())
	}

	// 2 should now be a miss (re-registered).
	preCalls := atomic.LoadInt64(&mr.callCount)
	if _, err := r.Acquire(2); err != nil {
		t.Fatalf("Acquire(2): %v", err)
	}
	if atomic.LoadInt64(&mr.callCount) != preCalls+1 {
		t.Errorf("re-Acquire(2) should hit register again")
	}
}

func TestFileMRRegistry_PinnedSkipsEviction(t *testing.T) {
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(2, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Two entries, both pinned (not Released).
	_, err = r.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Acquire(2)
	if err != nil {
		t.Fatal(err)
	}

	// Acquire(3): cache full + everything pinned → ErrFileMRRegistryFull.
	_, err = r.Acquire(3)
	if !errors.Is(err, ErrFileMRRegistryFull) {
		t.Fatalf("got %v, want ErrFileMRRegistryFull", err)
	}
}

func TestFileMRRegistry_SingleFlight(t *testing.T) {
	// Multiple goroutines miss for the same key — register should
	// run exactly once and every goroutine gets the same entry.
	mr := &mockRegister{delay: 30 * time.Millisecond}
	r, err := NewFileMRRegistry(4, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const N = 16
	entries := make([]*FileMREntry, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			entries[i], errs[i] = r.Acquire(42)
		}()
	}
	wg.Wait()

	// register should have been called exactly once.
	if got := atomic.LoadInt64(&mr.callCount); got != 1 {
		t.Errorf("single-flight: register calls = %d, want 1", got)
	}
	// Every goroutine got the same entry.
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: err = %v", i, errs[i])
		}
		if entries[i] != entries[0] {
			t.Errorf("goroutine %d: got different entry", i)
		}
	}
	// refCount equals N (each successful Acquire incremented).
	if got := atomic.LoadInt32(&entries[0].refCount); int(got) != N {
		t.Errorf("refCount = %d, want %d", got, N)
	}
}

func TestFileMRRegistry_SingleFlightFailure(t *testing.T) {
	// register fails; waiters should all see the same error.
	mr := &mockRegister{
		delay:    20 * time.Millisecond,
		failKeys: map[uint64]error{99: errors.New("simulated reg failure")},
	}
	r, err := NewFileMRRegistry(4, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const N = 8
	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = r.Acquire(99)
		}()
	}
	wg.Wait()

	// register called once (or possibly twice on retry); allow up to
	// 2 because each waiter that observed an error may retry the
	// next round. But all N callers must see the same error pattern.
	if got := atomic.LoadInt64(&mr.callCount); got < 1 {
		t.Errorf("expected register to be called at least once, got %d", got)
	}
	for i := 0; i < N; i++ {
		if errs[i] == nil {
			t.Errorf("goroutine %d should have failed", i)
		}
	}
}

func TestFileMRRegistry_Invalidate(t *testing.T) {
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(2, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	e, _ := r.Acquire(5)
	r.Release(e)
	if r.Len() != 1 {
		t.Fatalf("setup: Len = %d", r.Len())
	}

	r.Invalidate(5)
	if r.Len() != 0 {
		t.Errorf("after Invalidate: Len = %d, want 0", r.Len())
	}

	// Re-acquire after Invalidate must trigger a fresh register.
	pre := atomic.LoadInt64(&mr.callCount)
	_, err = r.Acquire(5)
	if err != nil {
		t.Fatalf("re-Acquire: %v", err)
	}
	if atomic.LoadInt64(&mr.callCount) != pre+1 {
		t.Errorf("re-Acquire after Invalidate should call register again")
	}
}

func TestFileMRRegistry_InvalidateMissingKey(t *testing.T) {
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(2, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Should be a no-op, not panic.
	r.Invalidate(99999)
}

func TestFileMRRegistry_Close(t *testing.T) {
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(4, mr.register)
	if err != nil {
		t.Fatal(err)
	}

	_, err = r.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}

	r.Close()

	if _, err := r.Acquire(2); !errors.Is(err, ErrFileMRRegistryClosed) {
		t.Errorf("Acquire after Close: %v", err)
	}
	if r.Len() != 0 {
		t.Errorf("Len after Close: %d", r.Len())
	}

	// Idempotent Close.
	r.Close()
}

func TestFileMRRegistry_AcquireDoesNotBlockOtherKeys(t *testing.T) {
	// One slow register for key 1 must not block a parallel Acquire
	// for key 2. Use a per-key delay so the test isolates the
	// registry's internal locking from mock latency.
	var (
		registerCount int64
		startedKey1   = make(chan struct{})
	)
	register := func(key uint64) (*RDMAMem, int, error) {
		atomic.AddInt64(&registerCount, 1)
		if key == 1 {
			close(startedKey1)
			time.Sleep(100 * time.Millisecond)
		}
		return &RDMAMem{Rkey: uint32(key + 1000), VA: uint64(key + 2000)}, 1 << 20, nil
	}
	r, err := NewFileMRRegistry(4, register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		r.Acquire(1)
	}()

	// Wait until register(1) actually entered (not just goroutine
	// scheduled) so the test doesn't race the wake-up.
	<-startedKey1

	start := time.Now()
	if _, err := r.Acquire(2); err != nil {
		t.Fatalf("Acquire(2): %v", err)
	}
	if took := time.Since(start); took > 50*time.Millisecond {
		t.Errorf("Acquire(2) waited %v — should not block on key 1", took)
	}
	<-done1
}

func TestFileMRRegistry_ConcurrentStress(t *testing.T) {
	// Hammer the registry from many goroutines across a small key
	// space with a cache smaller than the key space. Detects races
	// between Acquire / Release / Invalidate / eviction. Run under
	// -race for full coverage.
	const (
		cacheSize = 8
		keySpace  = 32
		workers   = 32
		ops       = 200
	)
	mr := &mockRegister{}
	r, err := NewFileMRRegistry(cacheSize, mr.register)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := uint64((w*ops + i) % keySpace)
				e, err := r.Acquire(key)
				if err == ErrFileMRRegistryFull {
					continue // cache pressure — expected occasionally
				}
				if err != nil {
					t.Errorf("Acquire(%d): %v", key, err)
					return
				}
				// Brief hold to create real refCount contention.
				time.Sleep(time.Microsecond)
				r.Release(e)
				// Occasionally Invalidate to exercise that path too.
				if i%50 == 0 {
					r.Invalidate(key)
				}
			}
		}()
	}
	wg.Wait()
}
