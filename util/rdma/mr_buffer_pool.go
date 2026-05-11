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
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// MRBufferPool serves one-sided RDMA Read requests by handing out
// pinned, MR-registered buffers from a bounded pool. The flow is:
//
//   1. Client sends OpReadMRLookup with (pid, ext, offset, size).
//   2. Server acquires a free MRBuffer from the pool, fills it via
//      store.Read, and replies with the buffer's (rkey, VA, CRC).
//   3. Client issues an RDMA Read against (rkey, VA) → data flows
//      directly NIC-to-NIC, server CPU not involved.
//   4. Client sends OpReadMRRelease so the server can hand the
//      buffer back to the pool. A background sweeper also reclaims
//      buffers stuck "in use" past TTL — defends against clients
//      crashing or networks blipping between read and release.
//
// The pool's bookkeeping (free list, condvar, TTL sweep) lives in
// this file with no build tag so it can be unit-tested on platforms
// without RDMA hardware. The actual MR registration is in
// mr_buffer_pool_rdma.go behind `//go:build linux && rdma`.

// ErrMRPoolClosed is returned by Acquire after Close has run.
var ErrMRPoolClosed = errors.New("rdma: MR buffer pool closed")

// MRBuffer is one pre-registered pinned region with its remote-access
// credentials. Returned by Acquire and given back via Release.
//
// Data is the local-process view of the same memory the peer reads
// via (Rkey, VA). It is only safe to write while the caller holds
// the buffer (i.e. between Acquire and Release).
type MRBuffer struct {
	Index int    // slot index in the pool; pool uses this on Release
	Rkey  uint32 // remote key the peer presents in its RDMA Read WR
	VA    uint64 // remote virtual address (= local uintptr to Data)
	Size  int    // capacity of Data, in bytes
	Data  []byte // local view; backed by the same pinned memory the peer reads

	// acquiredAtUnixNanos holds time.Now().UnixNano() while the
	// buffer is held by a caller; 0 when free. Used by the TTL
	// sweep to detect leaked acquisitions.
	acquiredAtUnixNanos atomic.Int64
}

// MRBufferPool is a thread-safe pool of MRBuffer. Acquire blocks when
// the pool is empty until a Release (or TTL eviction) frees a buffer.
type MRBufferPool struct {
	buffers []*MRBuffer

	mu      sync.Mutex
	cond    *sync.Cond
	freeIdx []int // indices into buffers; LIFO for cache locality

	ttl     time.Duration
	closed  bool
	closeCh chan struct{}
	sweepWg sync.WaitGroup

	// ownedMems tracks RDMAMem allocations made by
	// NewMRBufferPoolForPD; left nil for tests that hand in
	// pre-built MRBuffer slices. freeOwnedMems (defined in the
	// rdma-tagged file) consumes this on Close.
	ownedMems []*RDMAMem
}

// NewMRBufferPool wraps an already-registered slice of MRBuffer in
// a thread-safe pool. Caller retains ownership of the underlying
// memory (i.e., is responsible for freeing each buffer's MR
// registration after Close).
//
// ttl bounds how long a buffer may be held after Acquire before the
// background sweep returns it to the free list. Use ttl=0 to disable
// the sweep — useful for tests that drive the lifecycle manually.
func NewMRBufferPool(buffers []*MRBuffer, ttl time.Duration) *MRBufferPool {
	p := &MRBufferPool{
		buffers: buffers,
		ttl:     ttl,
		closeCh: make(chan struct{}),
		freeIdx: make([]int, len(buffers)),
	}
	p.cond = sync.NewCond(&p.mu)
	for i := range buffers {
		buffers[i].Index = i
		p.freeIdx[i] = i
	}
	if ttl > 0 {
		p.sweepWg.Add(1)
		go p.sweepLoop()
	}
	return p
}

// Acquire returns a free MRBuffer, blocking until one is available
// or ctx is cancelled / Close is called. The returned buffer's Data
// content is undefined — callers must fully overwrite the bytes they
// intend to expose before sending the rkey to the peer.
func (p *MRBufferPool) Acquire(ctx context.Context) (*MRBuffer, error) {
	// Context cancellation needs a wakeup path because cond.Wait
	// doesn't observe ctx. Spawn a single goroutine per Acquire only
	// when ctx has a Done channel; otherwise the unblocked-wait path
	// is allocation-free.
	if ctx != nil && ctx.Done() != nil {
		wakeStop := make(chan struct{})
		defer close(wakeStop)
		go func() {
			select {
			case <-ctx.Done():
				p.mu.Lock()
				p.cond.Broadcast()
				p.mu.Unlock()
			case <-wakeStop:
			}
		}()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return nil, ErrMRPoolClosed
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		if n := len(p.freeIdx); n > 0 {
			idx := p.freeIdx[n-1]
			p.freeIdx = p.freeIdx[:n-1]
			buf := p.buffers[idx]
			buf.acquiredAtUnixNanos.Store(time.Now().UnixNano())
			return buf, nil
		}
		p.cond.Wait()
	}
}

// TryAcquire returns a buffer if one is immediately available without
// blocking, or (nil, false) if all buffers are in use. Useful for
// best-effort fast paths that want to fall back to TCP rather than
// queue under load.
func (p *MRBufferPool) TryAcquire() (*MRBuffer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false
	}
	if n := len(p.freeIdx); n > 0 {
		idx := p.freeIdx[n-1]
		p.freeIdx = p.freeIdx[:n-1]
		buf := p.buffers[idx]
		buf.acquiredAtUnixNanos.Store(time.Now().UnixNano())
		return buf, true
	}
	return nil, false
}

// Release returns a buffer to the free list. Safe to call exactly
// once per Acquire; double-releases are silently ignored to keep the
// caller's defer ergonomics simple after error paths.
func (p *MRBufferPool) Release(b *MRBuffer) {
	if b == nil {
		return
	}
	// Guard against double-release: if the buffer was already freed
	// (e.g. via TTL sweep before the caller's defer ran), skip.
	if b.acquiredAtUnixNanos.Load() == 0 {
		return
	}
	b.acquiredAtUnixNanos.Store(0)

	p.mu.Lock()
	if !p.closed {
		p.freeIdx = append(p.freeIdx, b.Index)
		p.cond.Signal()
	}
	p.mu.Unlock()
}

// ReleaseByIndex is equivalent to Release(p.buffers[i]) but lets a
// caller release a buffer it identified by index — e.g. the server-
// side OpReadMRRelease handler, which receives the PoolIndex from
// the client on the wire. Out-of-range indices are silently ignored
// (the TTL sweep will reclaim any genuinely stuck buffer regardless).
func (p *MRBufferPool) ReleaseByIndex(idx int) {
	if idx < 0 || idx >= len(p.buffers) {
		return
	}
	p.Release(p.buffers[idx])
}

// Len reports the configured pool size. Useful for metrics export.
func (p *MRBufferPool) Len() int {
	return len(p.buffers)
}

// AvailableLocked exposes the current free-count; intended for tests
// and metrics. Takes the mutex; callers must not hold it.
func (p *MRBufferPool) Available() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.freeIdx)
}

// Close stops the sweep goroutine and unblocks any waiters. Buffers
// already held by callers are left as-is; their MR registrations
// will still be freed below since the underlying memory is owned
// by the pool when constructed via NewMRBufferPoolForPD.
func (p *MRBufferPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.closeCh)
	p.cond.Broadcast()
	p.mu.Unlock()
	p.sweepWg.Wait()
	p.freeOwnedMems()
}

// freeOwnedMems releases the underlying MR registrations + pinned
// memory created by NewMRBufferPoolForPD. No-op if no mems were
// attached (e.g. when the pool was built directly via
// NewMRBufferPool for tests).
func (p *MRBufferPool) freeOwnedMems() {
	for _, m := range p.ownedMems {
		m.Free()
	}
	p.ownedMems = nil
}

// attachMems hooks the registered RDMAMems into the pool so Close
// can deregister them. Keeping the mems behind a method (rather than
// a struct field exposed everywhere) lets the build-tag-free pool
// stay agnostic about real-vs-mock memory.
func (p *MRBufferPool) attachMems(mems []*RDMAMem) {
	p.ownedMems = mems
}

// sweepLoop runs while ttl > 0. Once a second it reclaims buffers
// whose acquiredAt is older than ttl — defends against peers that
// took an rkey, crashed, and never sent OpReadMRRelease.
//
// Tested via TestMRBufferPool_TTLReclaims with a tight ttl + sleep
// instead of mocking the clock; the sweep tick rate is fixed at
// max(ttl/4, 50ms) so even a 200ms-TTL test runs in <300ms.
func (p *MRBufferPool) sweepLoop() {
	defer p.sweepWg.Done()
	tickEvery := p.ttl / 4
	if tickEvery < 50*time.Millisecond {
		tickEvery = 50 * time.Millisecond
	}
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.sweepExpired()
		}
	}
}

func (p *MRBufferPool) sweepExpired() {
	deadlineNanos := time.Now().Add(-p.ttl).UnixNano()
	for _, b := range p.buffers {
		acq := b.acquiredAtUnixNanos.Load()
		if acq == 0 || acq > deadlineNanos {
			continue
		}
		// Atomically transition from "acquired" to "free" so a racing
		// Release does not double-add to the free list.
		if !b.acquiredAtUnixNanos.CompareAndSwap(acq, 0) {
			continue
		}
		p.mu.Lock()
		if !p.closed {
			p.freeIdx = append(p.freeIdx, b.Index)
			p.cond.Signal()
		}
		p.mu.Unlock()
	}
}
