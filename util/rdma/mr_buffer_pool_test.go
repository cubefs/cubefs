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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests cover the MRBufferPool bookkeeping (free list, blocking
// Acquire, TTL sweep, Close) without needing real RDMA hardware.
// Real MR registration happens in mr_buffer_pool_rdma.go and is
// validated by integration tests.

// makeTestPool builds an MRBufferPool with `n` placeholder MRBuffers.
// Rkey/VA/Data are zero-valued — fine because the pool only treats
// them as opaque payload.
func makeTestPool(n int, ttl time.Duration) *MRBufferPool {
	bufs := make([]*MRBuffer, n)
	for i := range bufs {
		bufs[i] = &MRBuffer{Size: 1024}
	}
	return NewMRBufferPool(bufs, ttl)
}

func TestMRBufferPool_AcquireRelease(t *testing.T) {
	p := makeTestPool(2, 0)
	defer p.Close()

	if got := p.Len(); got != 2 {
		t.Fatalf("Len: got %d want 2", got)
	}
	if got := p.Available(); got != 2 {
		t.Fatalf("initial Available: got %d want 2", got)
	}

	b1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	b2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}
	if b1 == nil || b2 == nil || b1 == b2 {
		t.Fatalf("got duplicate or nil buffers: %v %v", b1, b2)
	}
	if got := p.Available(); got != 0 {
		t.Fatalf("after 2 acquires Available: got %d want 0", got)
	}
	if b1.acquiredAtUnixNanos.Load() == 0 {
		t.Errorf("acquired buffer should have non-zero timestamp")
	}

	p.Release(b1)
	if got := p.Available(); got != 1 {
		t.Fatalf("after release Available: got %d want 1", got)
	}
	if b1.acquiredAtUnixNanos.Load() != 0 {
		t.Errorf("released buffer should have zero timestamp")
	}

	p.Release(b2)
	if got := p.Available(); got != 2 {
		t.Fatalf("after both released Available: got %d want 2", got)
	}
}

func TestMRBufferPool_AcquireBlocksUntilRelease(t *testing.T) {
	p := makeTestPool(1, 0)
	defer p.Close()

	first, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// Second acquire blocks; verify by racing it with a Release.
	got := make(chan *MRBuffer, 1)
	go func() {
		b, _ := p.Acquire(context.Background())
		got <- b
	}()

	// Give the waiter time to park on cond.Wait — anything between
	// 10 ms and a few hundred ms works. Then release and expect a
	// prompt wakeup.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("second Acquire returned before Release")
	default:
	}

	p.Release(first)
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("second Acquire did not wake within 1s")
	}
}

func TestMRBufferPool_AcquireContextCancel(t *testing.T) {
	p := makeTestPool(1, 0)
	defer p.Close()

	_, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("setup Acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = p.Acquire(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Acquire didn't honour cancel promptly: %v", elapsed)
	}
}

func TestMRBufferPool_TryAcquire(t *testing.T) {
	p := makeTestPool(1, 0)
	defer p.Close()

	b, ok := p.TryAcquire()
	if !ok || b == nil {
		t.Fatalf("first TryAcquire: ok=%v buf=%v", ok, b)
	}

	if _, ok := p.TryAcquire(); ok {
		t.Fatal("second TryAcquire should fail (pool exhausted)")
	}

	p.Release(b)
	if _, ok := p.TryAcquire(); !ok {
		t.Fatal("TryAcquire should succeed after Release")
	}
}

func TestMRBufferPool_TTLReclaims(t *testing.T) {
	const ttl = 100 * time.Millisecond
	p := makeTestPool(1, ttl)
	defer p.Close()

	b, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if b == nil {
		t.Fatal("nil buffer")
	}

	// Without calling Release, wait past ttl + sweep interval
	// (sweep ticks at ttl/4 with floor of 50ms).
	time.Sleep(ttl + 100*time.Millisecond)

	if got := p.Available(); got != 1 {
		t.Fatalf("after TTL Available: got %d want 1 (sweep should have reclaimed)", got)
	}
}

func TestMRBufferPool_DoubleReleaseIgnored(t *testing.T) {
	p := makeTestPool(2, 0)
	defer p.Close()

	b, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	p.Release(b)
	p.Release(b) // should be silently ignored

	if got := p.Available(); got != 2 {
		t.Fatalf("after double Release Available: got %d want 2", got)
	}
}

func TestMRBufferPool_NilReleaseIgnored(t *testing.T) {
	p := makeTestPool(1, 0)
	defer p.Close()
	// Should not panic.
	p.Release(nil)
}

func TestMRBufferPool_CloseUnblocksWaiters(t *testing.T) {
	p := makeTestPool(1, 0)

	_, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("setup Acquire: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Acquire(context.Background())
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)

	p.Close()
	select {
	case err := <-errCh:
		if err != ErrMRPoolClosed {
			t.Errorf("waiter got %v, want %v", err, ErrMRPoolClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake waiter within 1s")
	}
}

func TestMRBufferPool_AcquireAfterCloseFails(t *testing.T) {
	p := makeTestPool(1, 0)
	p.Close()
	if _, err := p.Acquire(context.Background()); err != ErrMRPoolClosed {
		t.Fatalf("got %v, want %v", err, ErrMRPoolClosed)
	}
	if _, ok := p.TryAcquire(); ok {
		t.Fatal("TryAcquire on closed pool should fail")
	}
}

func TestMRBufferPool_ConcurrentAcquireRelease(t *testing.T) {
	// Stress: many goroutines acquire/release concurrently on a small
	// pool. Detects races in the free-list bookkeeping. Combine with
	// `go test -race` for full coverage.
	const (
		poolSize    = 8
		workers     = 64
		opsPerWorker = 200
	)
	p := makeTestPool(poolSize, 0)
	defer p.Close()

	var acquireCount, releaseCount atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				b, err := p.Acquire(context.Background())
				if err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				acquireCount.Add(1)
				// Brief simulated work so contention happens.
				time.Sleep(50 * time.Microsecond)
				p.Release(b)
				releaseCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if acquireCount.Load() != releaseCount.Load() {
		t.Errorf("acquire/release mismatch: %d / %d", acquireCount.Load(), releaseCount.Load())
	}
	if got := p.Available(); got != poolSize {
		t.Errorf("final Available: got %d want %d", got, poolSize)
	}
}
