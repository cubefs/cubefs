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
	"time"
)

// LeaseTracker is the server-side bookkeeping for OpExtentMRLookup
// grants. It bridges the FileMRRegistry (which knows the registered
// MRs and reference counts them) and the over-the-wire lease
// protocol (clients hold an opaque LeaseID and renew it before
// expiry):
//
//   Grant       Lookup handler → tracker.Grant(entry, ttl) returns
//                a LeaseID + Lease pinned by entry.refCount=1.
//   Renew       Renew handler  → tracker.Renew(id, ttl) extends
//                expiry. Unknown / expired IDs return error so the
//                client triggers a fresh Lookup.
//   Release     Optional client-driven release; tracker calls
//                registry.Release on the underlying entry.
//   Sweep       Background goroutine releases expired leases.
//   InvalidateKey  Extent-delete hook drops every lease for a key
//                so the registry can drop the MR immediately.
//
// All public methods are safe for concurrent callers. The internal
// mutex protects only the maps and the nextID counter; per-lease
// state (expiresAt) uses atomic ops so the sweep loop can read it
// without grabbing the big lock.

// ErrLeaseUnknown is returned by Renew / Release for a LeaseID that
// has already expired, been explicitly released, or never existed.
var ErrLeaseUnknown = errors.New("rdma: lease unknown or expired")

// ErrLeaseTrackerClosed is returned by Grant after Close.
var ErrLeaseTrackerClosed = errors.New("rdma: LeaseTracker closed")

// Lease is the server's view of a single client grant.
type Lease struct {
	ID    uint64
	Key   uint64 // mirrors entry.Key for log/diagnostic ease
	entry *FileMREntry

	expiresAtUnixNanos atomic.Int64
}

// ExpiresAt returns the absolute deadline for this lease.
func (l *Lease) ExpiresAt() time.Time {
	return time.Unix(0, l.expiresAtUnixNanos.Load())
}

// LeaseTracker is goroutine-safe.
type LeaseTracker struct {
	registry *FileMRRegistry
	maxTTL   time.Duration

	mu     sync.Mutex
	nextID uint64
	byID   map[uint64]*Lease
	byKey  map[uint64]map[uint64]*Lease // key → leaseID → lease

	closeCh chan struct{}
	sweepWg sync.WaitGroup
	closed  bool
}

// NewLeaseTracker spawns a background sweeper that releases expired
// leases. maxTTL caps any lease duration regardless of client hint.
// sweepInterval is how often the sweeper wakes; values smaller than
// maxTTL/8 are clamped up to that floor to avoid waste.
func NewLeaseTracker(registry *FileMRRegistry, maxTTL, sweepInterval time.Duration) (*LeaseTracker, error) {
	if registry == nil {
		return nil, errors.New("rdma: NewLeaseTracker: nil registry")
	}
	if maxTTL <= 0 {
		return nil, errors.New("rdma: NewLeaseTracker: maxTTL must be > 0")
	}
	if floor := maxTTL / 8; sweepInterval < floor {
		sweepInterval = floor
	}
	if sweepInterval < 100*time.Millisecond {
		sweepInterval = 100 * time.Millisecond
	}
	t := &LeaseTracker{
		registry: registry,
		maxTTL:   maxTTL,
		byID:     make(map[uint64]*Lease),
		byKey:    make(map[uint64]map[uint64]*Lease),
		closeCh:  make(chan struct{}),
	}
	t.sweepWg.Add(1)
	go t.sweepLoop(sweepInterval)
	return t, nil
}

// Grant pins entry with a new lease whose TTL is min(requested,
// maxTTL). The returned lease carries the assigned ID, the granted
// TTL (in seconds, mirroring the wire format), and the underlying
// MR entry which the caller can read .Rkey()/.VA() from.
//
// On success the entry's refCount is held by this lease; the caller
// MUST NOT separately Release the entry — that happens on lease
// expiry / Release / Close.
func (t *LeaseTracker) Grant(entry *FileMREntry, ttl time.Duration) (*Lease, uint32, error) {
	if entry == nil {
		return nil, 0, errors.New("rdma: Grant: nil entry")
	}
	granted := ttl
	if granted <= 0 || granted > t.maxTTL {
		granted = t.maxTTL
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		// The caller already holds entry's refCount via Acquire;
		// release it so the entry can be freed by LRU eviction.
		t.registry.Release(entry)
		return nil, 0, ErrLeaseTrackerClosed
	}
	t.nextID++
	id := t.nextID
	l := &Lease{
		ID:    id,
		Key:   entry.Key,
		entry: entry,
	}
	l.expiresAtUnixNanos.Store(time.Now().Add(granted).UnixNano())

	t.byID[id] = l
	leasesForKey, ok := t.byKey[entry.Key]
	if !ok {
		leasesForKey = make(map[uint64]*Lease)
		t.byKey[entry.Key] = leasesForKey
	}
	leasesForKey[id] = l
	t.mu.Unlock()

	return l, uint32(granted.Seconds()), nil
}

// Renew extends the lease's expiry to now+ttl (capped at maxTTL).
// Returns the granted seconds. Returns ErrLeaseUnknown if the lease
// has expired or never existed — clients must then issue a fresh
// Lookup.
func (t *LeaseTracker) Renew(id uint64, ttl time.Duration) (uint32, error) {
	if ttl <= 0 || ttl > t.maxTTL {
		ttl = t.maxTTL
	}
	t.mu.Lock()
	l, ok := t.byID[id]
	t.mu.Unlock()
	if !ok {
		return 0, ErrLeaseUnknown
	}
	// Atomically check current expiry — a sweep racing with us may
	// have already marked the lease expired. We refuse to extend an
	// already-expired lease so the client's view stays consistent
	// (it must re-Lookup).
	now := time.Now().UnixNano()
	for {
		cur := l.expiresAtUnixNanos.Load()
		if cur < now {
			return 0, ErrLeaseUnknown
		}
		newExp := now + int64(ttl)
		if l.expiresAtUnixNanos.CompareAndSwap(cur, newExp) {
			break
		}
	}
	return uint32(ttl.Seconds()), nil
}

// Release explicitly drops a lease ahead of its TTL. Idempotent: a
// double-release / release-after-expire returns ErrLeaseUnknown but
// has no other effect. Useful for the client-driven "I'm done"
// signal so server resources don't sit idle until TTL.
func (t *LeaseTracker) Release(id uint64) error {
	t.mu.Lock()
	l, ok := t.byID[id]
	if !ok {
		t.mu.Unlock()
		return ErrLeaseUnknown
	}
	t.deleteLeaseLocked(l)
	t.mu.Unlock()
	t.registry.Release(l.entry)
	return nil
}

// InvalidateKey drops every lease for the given key (e.g. extent
// delete). All in-flight readers of those leases will see their
// RDMA Read fail at the NIC layer once the MR is freed by the
// registry. Callers that need a softer behaviour should run their
// own grace period before calling.
func (t *LeaseTracker) InvalidateKey(key uint64) {
	t.mu.Lock()
	leases, ok := t.byKey[key]
	if !ok {
		t.mu.Unlock()
		return
	}
	// Snapshot under lock; Release outside.
	toRelease := make([]*Lease, 0, len(leases))
	for _, l := range leases {
		t.deleteLeaseLocked(l)
		toRelease = append(toRelease, l)
	}
	t.mu.Unlock()
	for _, l := range toRelease {
		t.registry.Release(l.entry)
	}
}

// LookupLease finds a lease by ID, for inspection. Returns nil if
// not found.
func (t *LeaseTracker) LookupLease(id uint64) *Lease {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.byID[id]
}

// ActiveCount returns the number of unexpired leases currently
// tracked. Useful for metrics / tests.
func (t *LeaseTracker) ActiveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.byID)
}

// Close stops the sweeper and releases every outstanding lease. The
// associated registry entries are released back so eviction can
// reclaim them. Safe to call multiple times.
func (t *LeaseTracker) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	close(t.closeCh)
	// Snapshot all leases under lock so we can Release without
	// holding the mutex (registry.Release is cheap but takes its
	// own mutex; nested locking is avoidable here).
	toRelease := make([]*Lease, 0, len(t.byID))
	for _, l := range t.byID {
		toRelease = append(toRelease, l)
	}
	t.byID = map[uint64]*Lease{}
	t.byKey = map[uint64]map[uint64]*Lease{}
	t.mu.Unlock()
	t.sweepWg.Wait()
	for _, l := range toRelease {
		t.registry.Release(l.entry)
	}
}

// deleteLeaseLocked removes the lease from both maps. Caller must
// hold t.mu and must call registry.Release(l.entry) afterwards.
func (t *LeaseTracker) deleteLeaseLocked(l *Lease) {
	delete(t.byID, l.ID)
	if forKey, ok := t.byKey[l.Key]; ok {
		delete(forKey, l.ID)
		if len(forKey) == 0 {
			delete(t.byKey, l.Key)
		}
	}
}

// sweepLoop runs until Close. It scans byID periodically and
// releases leases whose expiresAt has passed.
func (t *LeaseTracker) sweepLoop(interval time.Duration) {
	defer t.sweepWg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			t.sweepExpired()
		}
	}
}

func (t *LeaseTracker) sweepExpired() {
	nowNanos := time.Now().UnixNano()
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	var expired []*Lease
	for _, l := range t.byID {
		if l.expiresAtUnixNanos.Load() <= nowNanos {
			t.deleteLeaseLocked(l)
			expired = append(expired, l)
		}
	}
	t.mu.Unlock()
	for _, l := range expired {
		t.registry.Release(l.entry)
	}
}
