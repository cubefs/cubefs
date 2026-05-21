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

// leaseTrackerFixture builds a registry + tracker pair pre-populated
// with `n` Acquired entries so individual tests can focus on the
// tracker behaviour without re-implementing the registry plumbing.
func leaseTrackerFixture(t *testing.T, n int, maxTTL, sweep time.Duration) (*FileMRRegistry, *LeaseTracker, []*FileMREntry) {
	t.Helper()
	register := func(key uint64) (*RDMAMem, int, error) {
		return &RDMAMem{Rkey: uint32(key + 1000), VA: uint64(key + 2000)}, 1 << 20, nil
	}
	r, err := NewFileMRRegistry(n+4, register)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	tracker, err := NewLeaseTracker(r, maxTTL, sweep)
	if err != nil {
		r.Close()
		t.Fatalf("tracker: %v", err)
	}
	entries := make([]*FileMREntry, n)
	for i := 0; i < n; i++ {
		e, err := r.Acquire(uint64(i + 1))
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		entries[i] = e
	}
	return r, tracker, entries
}

func TestLeaseTracker_Ctor(t *testing.T) {
	if _, err := NewLeaseTracker(nil, time.Second, time.Second); err == nil {
		t.Error("nil registry should be rejected")
	}
	r, _ := NewFileMRRegistry(1, func(uint64) (*RDMAMem, int, error) { return nil, 0, errors.New("x") })
	defer r.Close()
	if _, err := NewLeaseTracker(r, 0, time.Second); err == nil {
		t.Error("zero maxTTL should be rejected")
	}
}

func TestLeaseTracker_GrantRenewRelease(t *testing.T) {
	r, tr, entries := leaseTrackerFixture(t, 1, 30*time.Second, time.Second)
	defer r.Close()
	defer tr.Close()

	lease, granted, err := tr.Grant(entries[0], 10*time.Second)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if granted != 10 {
		t.Errorf("granted: got %d want 10", granted)
	}
	if tr.ActiveCount() != 1 {
		t.Errorf("ActiveCount: got %d want 1", tr.ActiveCount())
	}
	if l := tr.LookupLease(lease.ID); l == nil || l.ID != lease.ID {
		t.Error("LookupLease: not found")
	}

	// Renew extends expiry.
	preExp := atomic.LoadInt64(&lease.expiresAtUnixNanos)
	time.Sleep(2 * time.Millisecond)
	newGranted, err := tr.Renew(lease.ID, 20*time.Second)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if newGranted != 20 {
		t.Errorf("Renew granted: got %d want 20", newGranted)
	}
	if atomic.LoadInt64(&lease.expiresAtUnixNanos) <= preExp {
		t.Error("Renew did not advance expiry")
	}

	// Release removes the lease and returns refcount to registry.
	if err := tr.Release(lease.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if tr.ActiveCount() != 0 {
		t.Errorf("ActiveCount after Release: %d", tr.ActiveCount())
	}
	if _, err := tr.Renew(lease.ID, time.Second); !errors.Is(err, ErrLeaseUnknown) {
		t.Errorf("Renew after Release: got %v, want ErrLeaseUnknown", err)
	}
}

func TestLeaseTracker_TTLCappedToMax(t *testing.T) {
	r, tr, entries := leaseTrackerFixture(t, 1, 5*time.Second, time.Second)
	defer r.Close()
	defer tr.Close()

	_, granted, err := tr.Grant(entries[0], 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if granted != 5 {
		t.Errorf("Grant capped: got %d want 5 (= maxTTL)", granted)
	}
}

func TestLeaseTracker_RenewExpiredFails(t *testing.T) {
	// Tight TTL + tight sweep so we can observe the expiry path.
	const maxTTL = 100 * time.Millisecond
	r, tr, entries := leaseTrackerFixture(t, 1, maxTTL, 30*time.Millisecond)
	defer r.Close()
	defer tr.Close()

	lease, _, err := tr.Grant(entries[0], maxTTL)
	if err != nil {
		t.Fatal(err)
	}

	// Wait past TTL + sweep cycle.
	time.Sleep(maxTTL + 80*time.Millisecond)

	if tr.ActiveCount() != 0 {
		t.Errorf("sweep should have reclaimed lease, ActiveCount=%d", tr.ActiveCount())
	}
	if _, err := tr.Renew(lease.ID, time.Second); !errors.Is(err, ErrLeaseUnknown) {
		t.Errorf("Renew after expiry: got %v, want ErrLeaseUnknown", err)
	}
}

func TestLeaseTracker_InvalidateKeyDropsAllLeases(t *testing.T) {
	r, tr, entries := leaseTrackerFixture(t, 1, 30*time.Second, time.Second)
	defer r.Close()
	defer tr.Close()

	// Grant several leases against the same entry. Acquire bumps
	// refcount each time so the registry pins it.
	const N = 5
	for i := 0; i < N; i++ {
		if _, err := r.Acquire(entries[0].Key); err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
	}
	leaseIDs := make([]uint64, 0, N+1)
	l0, _, _ := tr.Grant(entries[0], 30*time.Second)
	leaseIDs = append(leaseIDs, l0.ID)
	for i := 0; i < N; i++ {
		l, _, err := tr.Grant(entries[0], 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		leaseIDs = append(leaseIDs, l.ID)
	}
	if got := tr.ActiveCount(); got != N+1 {
		t.Errorf("ActiveCount before invalidate: %d want %d", got, N+1)
	}

	tr.InvalidateKey(entries[0].Key)
	if got := tr.ActiveCount(); got != 0 {
		t.Errorf("ActiveCount after Invalidate: %d", got)
	}
	for _, id := range leaseIDs {
		if l := tr.LookupLease(id); l != nil {
			t.Errorf("lease %d still tracked", id)
		}
	}
}

func TestLeaseTracker_DoubleReleaseIsErr(t *testing.T) {
	r, tr, entries := leaseTrackerFixture(t, 1, 30*time.Second, time.Second)
	defer r.Close()
	defer tr.Close()

	l, _, _ := tr.Grant(entries[0], 30*time.Second)
	if err := tr.Release(l.ID); err != nil {
		t.Fatal(err)
	}
	if err := tr.Release(l.ID); !errors.Is(err, ErrLeaseUnknown) {
		t.Errorf("double-release: %v", err)
	}
}

func TestLeaseTracker_CloseReleasesLeases(t *testing.T) {
	r, tr, entries := leaseTrackerFixture(t, 2, 30*time.Second, time.Second)
	defer r.Close()

	_, _, _ = tr.Grant(entries[0], 30*time.Second)
	_, _, _ = tr.Grant(entries[1], 30*time.Second)
	if tr.ActiveCount() != 2 {
		t.Fatal("setup")
	}

	tr.Close()
	if tr.ActiveCount() != 0 {
		t.Errorf("Close should have cleared all leases")
	}
	tr.Close() // idempotent

	if _, _, err := tr.Grant(entries[0], time.Second); !errors.Is(err, ErrLeaseTrackerClosed) {
		t.Errorf("Grant after Close: %v want ErrLeaseTrackerClosed", err)
	}
}

func TestLeaseTracker_ConcurrentGrantRenew(t *testing.T) {
	// Hammer Grant + Renew + Release from multiple goroutines on the
	// same entry. Detects races in the byID/byKey/lease state under
	// -race.
	r, tr, entries := leaseTrackerFixture(t, 1, 30*time.Second, time.Second)
	defer r.Close()
	defer tr.Close()

	// Need a registry that lets us Acquire() the same key many times
	// without exhausting; fixture allocates capacity n+4 = 5 here.
	// Each goroutine will Grant once and Release.
	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := r.Acquire(entries[0].Key)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			l, _, err := tr.Grant(e, 30*time.Second)
			if err != nil {
				t.Errorf("Grant: %v", err)
				return
			}
			if _, err := tr.Renew(l.ID, 30*time.Second); err != nil {
				t.Errorf("Renew: %v", err)
				return
			}
			if err := tr.Release(l.ID); err != nil {
				t.Errorf("Release: %v", err)
			}
		}()
	}
	wg.Wait()
	if tr.ActiveCount() != 0 {
		t.Errorf("after concurrent Grant/Release: ActiveCount=%d want 0", tr.ActiveCount())
	}
}
