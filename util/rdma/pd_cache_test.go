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
	"unsafe"
)

// These tests exercise the PD cache without touching real RDMA
// hardware: the pdAllocator is injected and returns fake unsafe.Pointer
// values that just need to be distinguishable per ctxKey. Real
// ibv_alloc_pd behaviour is verified by integration tests on
// linux+rdma.

// fakePDAlloc returns a unique pointer per ctxKey by encoding the key
// into a non-nil address. Tests use ctxKey directly as the pointer
// so equality assertions can compare against a predictable value.
func fakePDAlloc(key uintptr) (unsafe.Pointer, error) {
	// uintptr → unsafe.Pointer is unsafe in general (no GC tracking)
	// but the test only uses the value for equality comparison —
	// it's never dereferenced.
	//nolint:govet,staticcheck
	return unsafe.Pointer(key + 0xDEAD0000), nil
}

func TestPDCache_SameKeyReturnsSamePD(t *testing.T) {
	resetPDCacheForTest()
	defer resetPDCacheForTest()

	key := uintptr(0x100)
	pd1, err := getOrAllocPDCached(key, fakePDAlloc)
	if err != nil {
		t.Fatalf("first alloc: %v", err)
	}
	pd2, err := getOrAllocPDCached(key, fakePDAlloc)
	if err != nil {
		t.Fatalf("second alloc: %v", err)
	}
	if pd1 != pd2 {
		t.Errorf("same key returned different PDs: pd1=%p pd2=%p", pd1, pd2)
	}
}

func TestPDCache_DistinctKeysReturnDistinctPDs(t *testing.T) {
	resetPDCacheForTest()
	defer resetPDCacheForTest()

	a, _ := getOrAllocPDCached(uintptr(0x100), fakePDAlloc)
	b, _ := getOrAllocPDCached(uintptr(0x200), fakePDAlloc)
	if a == b {
		t.Errorf("distinct keys returned same PD: a=%p b=%p", a, b)
	}
}

func TestPDCache_AllocOnlyCalledOncePerKey(t *testing.T) {
	resetPDCacheForTest()
	defer resetPDCacheForTest()

	var calls int64
	alloc := func(k uintptr) (unsafe.Pointer, error) {
		atomic.AddInt64(&calls, 1)
		return fakePDAlloc(k)
	}
	for i := 0; i < 5; i++ {
		if _, err := getOrAllocPDCached(uintptr(0x100), alloc); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("alloc invocations: got %d want 1", got)
	}
}

func TestPDCache_ConcurrentFirstTouchSingleAlloc(t *testing.T) {
	// N goroutines hit the same cold key. Exactly ONE alloc call
	// should happen; all N callers see the same PD. Catches the
	// double-alloc race a naive check-then-store would have.
	resetPDCacheForTest()
	defer resetPDCacheForTest()

	const N = 32
	var calls int64
	alloc := func(k uintptr) (unsafe.Pointer, error) {
		atomic.AddInt64(&calls, 1)
		return fakePDAlloc(k)
	}

	results := make([]unsafe.Pointer, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			pd, err := getOrAllocPDCached(uintptr(0xABCD), alloc)
			if err != nil {
				t.Errorf("g%d: %v", i, err)
				return
			}
			results[i] = pd
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("alloc invocations: got %d want 1 (concurrent first-touch must singleflight)", got)
	}
	for i := 1; i < N; i++ {
		if results[i] != results[0] {
			t.Errorf("g%d got different PD: %p vs %p", i, results[i], results[0])
		}
	}
}

func TestPDCache_AllocErrorNotCached(t *testing.T) {
	// A failed alloc must NOT leave a bogus entry in the cache —
	// the next call should retry the allocation.
	resetPDCacheForTest()
	defer resetPDCacheForTest()

	var calls int64
	failing := errors.New("simulated kernel OOM")
	alloc := func(k uintptr) (unsafe.Pointer, error) {
		n := atomic.AddInt64(&calls, 1)
		if n == 1 {
			return nil, failing
		}
		return fakePDAlloc(k)
	}

	if _, err := getOrAllocPDCached(uintptr(0x500), alloc); !errors.Is(err, failing) {
		t.Errorf("first call: got %v want %v", err, failing)
	}
	// Retry: should re-invoke alloc, succeed this time.
	pd, err := getOrAllocPDCached(uintptr(0x500), alloc)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if pd == nil {
		t.Fatal("expected non-nil PD on retry")
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Errorf("alloc invocations: got %d want 2", got)
	}
}

func TestPDCache_ResetClears(t *testing.T) {
	resetPDCacheForTest()

	var calls int64
	alloc := func(k uintptr) (unsafe.Pointer, error) {
		atomic.AddInt64(&calls, 1)
		return fakePDAlloc(k)
	}

	_, _ = getOrAllocPDCached(uintptr(0x100), alloc)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("setup calls: %d", got)
	}

	resetPDCacheForTest()

	_, _ = getOrAllocPDCached(uintptr(0x100), alloc)
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Errorf("after reset, alloc should re-invoke: got %d want 2", got)
	}
}
