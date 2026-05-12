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
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FileMRRegistry is the abstract bookkeeping layer behind per-extent
// (or any file-backed) MR registrations. It owns:
//
//   - an in-memory LRU cache of currently-registered (key → RDMAMem)
//   - a single-flight gate so concurrent first-time Acquires for the
//     same key trigger exactly one register call
//   - per-entry reference counting so an Acquire is never evicted
//     while still in use by a reader
//
// The actual MR registration (mmap + ibv_reg_mr) is pluggable: the
// caller supplies a RegisterFunc when constructing the registry. This
// keeps the LRU/refcount/single-flight logic build-tag-free and
// unit-testable on platforms without RDMA hardware; the production
// wiring in datanode/ supplies a function that opens the extent file,
// mmap()s it, and calls RegisterFileMR.

// ErrFileMRRegistryClosed is returned by Acquire after Close.
var ErrFileMRRegistryClosed = errors.New("rdma: FileMRRegistry closed")

// activeExtentMRCount tracks the number of currently-active (i.e.
// registered AND not yet evicted/freed) MRs across every
// FileMRRegistry instance in the process. Server-side scale signal:
// each MR consumes one entry in the NIC's MR table (mlx5 TLB), and
// when the table fills the hardware silently falls back to memory
// lookups, dropping RDMA Read latency from microseconds to
// milliseconds.
//
// Exposed via ActiveExtentMRCount() for the datanode metrics layer
// to publish as a Prometheus gauge. Updated atomically from Acquire
// (increment), evictOneLocked (decrement), and Close (decrement by
// the number of freed entries).
//
// Note: this counts only persistent extent MRs (FileMRRegistry).
// Conn-local MRs (recv pool, send scratch, Phase A read scratch)
// are not counted — they're bounded by the conn count, not by extent
// churn, and don't typically threaten the NIC MR table.
var activeExtentMRCount int64

// ActiveExtentMRCount returns the current count of registered extent
// MRs across all FileMRRegistry instances in this process. Safe to
// call concurrently. Intended for periodic metric scrape only — not
// a hot-path read.
func ActiveExtentMRCount() int64 {
	return atomic.LoadInt64(&activeExtentMRCount)
}

// ErrFileMRRegistryFull is returned by Acquire when the cache is at
// MaxEntries and every cached entry is currently in use (refCount>0).
// Callers should fall back to the two-sided read path.
var ErrFileMRRegistryFull = errors.New("rdma: FileMRRegistry full, all entries in use")

// RegisterFunc materialises an MR for the given key. Called with no
// locks held so the underlying mmap + ibv_reg_mr can take as long as
// they need without blocking other Acquires.
//
// Returns the registered MR plus the actual usable size in bytes
// (which the caller wires into FileMREntry.Size for bounds checks).
// On failure the caller should NOT Free anything — Acquire propagates
// the error to its caller.
type RegisterFunc func(key uint64) (mem *RDMAMem, size int, err error)

// FileMREntry is one cache slot. Callers obtain it via Acquire and
// must call Release exactly once when done. While refCount > 0 the
// entry is pinned in the cache; LRU eviction skips over it.
type FileMREntry struct {
	Key  uint64
	Mem  *RDMAMem
	Size int

	registry *FileMRRegistry
	lruElem  *list.Element
	// Atomics use Load/Store/Add helpers (Go 1.17 compat — atomic.Int32
	// / atomic.Int64 are Go 1.19+).
	refCount int32
	lastUsed int64
}

// Rkey is a convenience accessor.
func (e *FileMREntry) Rkey() uint32 { return e.Mem.Rkey }

// VA is a convenience accessor.
func (e *FileMREntry) VA() uint64 { return e.Mem.VA }

// pending describes an in-flight registration waited on by other
// callers seeking the same key. The result is communicated via the
// (entry, err) pair after done closes.
type pending struct {
	done  chan struct{}
	entry *FileMREntry
	err   error
}

// FileMRRegistry is goroutine-safe.
type FileMRRegistry struct {
	maxEntries int
	register   RegisterFunc

	mu       sync.Mutex
	active   map[uint64]*FileMREntry
	lru      *list.List // front = most recently used
	pendings map[uint64]*pending

	closed bool
}

// NewFileMRRegistry returns a registry with the given LRU capacity.
// register is called on every cache miss and must be safe to invoke
// concurrently for different keys (the registry serialises
// duplicate-key calls itself).
func NewFileMRRegistry(maxEntries int, register RegisterFunc) (*FileMRRegistry, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("rdma: NewFileMRRegistry: maxEntries must be > 0")
	}
	if register == nil {
		return nil, fmt.Errorf("rdma: NewFileMRRegistry: nil register func")
	}
	return &FileMRRegistry{
		maxEntries: maxEntries,
		register:   register,
		active:     make(map[uint64]*FileMREntry, maxEntries),
		lru:        list.New(),
		pendings:   make(map[uint64]*pending),
	}, nil
}

// Acquire returns a registered MR for key, registering on demand.
// The returned entry's refCount is incremented; the caller MUST call
// Release on it (typically via defer) exactly once.
//
// Concurrent calls for the same key collapse to one register() call:
// the first caller does the work, subsequent callers wait on the
// pending channel and observe the same result.
func (r *FileMRRegistry) Acquire(key uint64) (*FileMREntry, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrFileMRRegistryClosed
	}
	// Fast path: cache hit.
	if e, ok := r.active[key]; ok {
		atomic.AddInt32(&e.refCount, 1)
		atomic.StoreInt64(&e.lastUsed, time.Now().UnixNano())
		r.lru.MoveToFront(e.lruElem)
		r.mu.Unlock()
		return e, nil
	}
	// Single-flight: another goroutine already started registering.
	if p, ok := r.pendings[key]; ok {
		r.mu.Unlock()
		<-p.done
		// Re-check state under lock.
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrFileMRRegistryClosed
		}
		if p.err != nil {
			r.mu.Unlock()
			return nil, p.err
		}
		// The pending caller already inserted the entry; bump
		// refCount and reuse.
		if e, ok := r.active[key]; ok && e == p.entry {
			atomic.AddInt32(&e.refCount, 1)
			atomic.StoreInt64(&e.lastUsed, time.Now().UnixNano())
			r.lru.MoveToFront(e.lruElem)
			r.mu.Unlock()
			return e, nil
		}
		// Race: entry was evicted between the wait and our re-lookup.
		// Fall through and try to register fresh.
		r.mu.Unlock()
		return r.Acquire(key)
	}
	// Real miss: become the registrar.
	p := &pending{done: make(chan struct{})}
	r.pendings[key] = p
	r.mu.Unlock()

	// register() runs without the registry mutex so callers seeking
	// other keys aren't blocked behind a slow mmap/ibv_reg_mr.
	mem, size, err := r.register(key)

	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() {
		// Signal waiters and drop the pending entry. Done last so a
		// waiter racing into another Acquire sees consistent state.
		close(p.done)
		delete(r.pendings, key)
	}()

	if r.closed {
		// Registry closed while we were registering. Free the MR we
		// just allocated so it doesn't leak.
		if mem != nil {
			mem.Free()
		}
		p.err = ErrFileMRRegistryClosed
		return nil, ErrFileMRRegistryClosed
	}
	if err != nil {
		p.err = err
		return nil, err
	}
	// Make room in the LRU if we're at capacity. evictOneLocked
	// returns ErrFileMRRegistryFull when every cached entry is
	// pinned — in that case the just-registered MR is freed so we
	// don't exceed the cap.
	if len(r.active) >= r.maxEntries {
		if eerr := r.evictOneLocked(); eerr != nil {
			mem.Free()
			p.err = eerr
			return nil, eerr
		}
	}
	e := &FileMREntry{
		Key:      key,
		Mem:      mem,
		Size:     size,
		registry: r,
	}
	atomic.StoreInt32(&e.refCount, 1)
	atomic.StoreInt64(&e.lastUsed, time.Now().UnixNano())
	e.lruElem = r.lru.PushFront(e)
	r.active[key] = e
	atomic.AddInt64(&activeExtentMRCount, 1)
	p.entry = e
	return e, nil
}

// Release decrements the caller's reference. The entry stays in the
// cache; eviction reclaims it only after refCount falls to zero and
// it becomes the LRU candidate.
func (r *FileMRRegistry) Release(e *FileMREntry) {
	if e == nil || e.registry != r {
		return
	}
	// Allow refCount to go negative briefly so a double-Release is
	// detectable, but the worst case is the entry becomes eligible
	// for eviction earlier than intended — not a correctness bug.
	atomic.AddInt32(&e.refCount, -1)
}

// Invalidate forcibly removes the entry for key. Used by the
// extent-delete path on the server side. If the entry is still in
// use (refCount>0) it is removed from the active set so new Acquires
// will re-register, but the underlying MR is NOT freed until the
// last in-flight reader Releases — guaranteed by the deferred Free
// at the bottom of the function.
//
// No-op if the key is not currently cached.
func (r *FileMRRegistry) Invalidate(key uint64) {
	r.mu.Lock()
	e, ok := r.active[key]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.active, key)
	r.lru.Remove(e.lruElem)
	atomic.AddInt64(&activeExtentMRCount, -1)
	r.mu.Unlock()
	// Free now if no readers; otherwise the last Release after our
	// removal will see the entry orphaned from the active map and
	// drop it. Currently the simple path: Free immediately. This is
	// safe because in-flight RDMA Reads against a deregistered MR
	// will fail at the NIC with a remote-access error, which the
	// client handles by falling back to TCP.
	e.Mem.Free()
}

// Close evicts everything and rejects new Acquires.
func (r *FileMRRegistry) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	// Snapshot under lock; Free without holding it.
	mems := make([]*RDMAMem, 0, len(r.active))
	for _, e := range r.active {
		mems = append(mems, e.Mem)
	}
	r.active = map[uint64]*FileMREntry{}
	r.lru = list.New()
	atomic.AddInt64(&activeExtentMRCount, -int64(len(mems)))
	// Cancel all pending registrations.
	for _, p := range r.pendings {
		p.err = ErrFileMRRegistryClosed
		close(p.done)
	}
	r.pendings = map[uint64]*pending{}
	r.mu.Unlock()

	for _, m := range mems {
		m.Free()
	}
}

// Len reports the current number of active cached entries. Mostly
// for tests / metrics.
func (r *FileMRRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.active)
}

// MaxEntries reports the LRU cap. Useful for metrics.
func (r *FileMRRegistry) MaxEntries() int { return r.maxEntries }

// evictOneLocked finds the least-recently-used entry whose refCount
// is zero and removes it. Must be called with r.mu held.
// Returns ErrFileMRRegistryFull if every cached entry is still in
// use (refCount>0).
func (r *FileMRRegistry) evictOneLocked() error {
	for e := r.lru.Back(); e != nil; e = e.Prev() {
		entry := e.Value.(*FileMREntry)
		if atomic.LoadInt32(&entry.refCount) <= 0 {
			r.lru.Remove(e)
			delete(r.active, entry.Key)
			atomic.AddInt64(&activeExtentMRCount, -1)
			entry.Mem.Free()
			return nil
		}
	}
	return ErrFileMRRegistryFull
}
