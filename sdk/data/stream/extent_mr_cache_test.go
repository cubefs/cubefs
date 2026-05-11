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

package stream

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockLookup is a controllable extentMRLookupFunc / extentMRRenewFunc
// pair shared by the cache tests. callDelay simulates a slow remote
// so concurrent Gets actually race the single-flight gate.
type mockLookup struct {
	mu         sync.Mutex
	lookupCnt  atomic.Int64
	renewCnt   atomic.Int64
	failKeys   map[uint64]error
	callDelay  time.Duration
	ttlSeconds uint32

	clockMu sync.Mutex
	clock   time.Time
}

func newMockLookup() *mockLookup {
	return &mockLookup{
		failKeys:   map[uint64]error{},
		ttlSeconds: 60,
		clock:      time.Unix(1_700_000_000, 0),
	}
}

func (m *mockLookup) now() time.Time {
	m.clockMu.Lock()
	defer m.clockMu.Unlock()
	return m.clock
}

func (m *mockLookup) advance(d time.Duration) {
	m.clockMu.Lock()
	m.clock = m.clock.Add(d)
	m.clockMu.Unlock()
}

func (m *mockLookup) lookup(addr string, pid, extentID uint64, _ time.Duration) (*LeaseInfo, error) {
	m.lookupCnt.Add(1)
	if m.callDelay > 0 {
		time.Sleep(m.callDelay)
	}
	if err, ok := m.failKeys[extentID]; ok {
		return nil, err
	}
	info := &LeaseInfo{
		Addr:        addr,
		PartitionID: pid,
		ExtentID:    extentID,
		LeaseID:     extentID + 100,
		Rkey:        uint32(extentID + 1000),
		VA:          uint64(extentID + 2000),
		Size:        1 << 20,
	}
	info.expiresAtNanos.Store(m.now().Add(time.Duration(m.ttlSeconds) * time.Second).UnixNano())
	return info, nil
}

func (m *mockLookup) renew(_ string, _ uint64, _ time.Duration) (uint32, error) {
	m.renewCnt.Add(1)
	return m.ttlSeconds, nil
}

func mockCache(t *testing.T, m *mockLookup, renewInterval time.Duration) *extentMRCache {
	t.Helper()
	cfg := extentMRCacheConfig{
		LookupTTLHint: 60 * time.Second,
		RenewMargin:   30 * time.Second,
		RenewInterval: renewInterval,
		NowFn:         m.now,
	}
	c, err := newExtentMRCache(cfg, m.lookup, m.renew)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestExtentMRCache_Ctor(t *testing.T) {
	if _, err := newExtentMRCache(extentMRCacheConfig{}, nil, func(string, uint64, time.Duration) (uint32, error) { return 0, nil }); err == nil {
		t.Error("nil lookupFn should be rejected")
	}
	if _, err := newExtentMRCache(extentMRCacheConfig{}, func(string, uint64, uint64, time.Duration) (*LeaseInfo, error) { return nil, nil }, nil); err == nil {
		t.Error("nil renewFn should be rejected")
	}
}

func TestExtentMRCache_HitMiss(t *testing.T) {
	m := newMockLookup()
	c := mockCache(t, m, time.Hour)
	defer c.Close()

	info, err := c.Get("dn1", 5, 100)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if info.LeaseID != 200 {
		t.Errorf("LeaseID: got %d want 200", info.LeaseID)
	}
	if got := m.lookupCnt.Load(); got != 1 {
		t.Errorf("lookupCnt: got %d want 1", got)
	}

	// Hit
	info2, err := c.Get("dn1", 5, 100)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if info2 != info {
		t.Error("expected cache hit returning same LeaseInfo")
	}
	if got := m.lookupCnt.Load(); got != 1 {
		t.Errorf("lookupCnt after hit: got %d want 1", got)
	}

	// Different addr is a separate key
	_, err = c.Get("dn2", 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.lookupCnt.Load(); got != 2 {
		t.Errorf("lookupCnt after different addr: got %d want 2", got)
	}
}

func TestExtentMRCache_ExpiredEntryRefetches(t *testing.T) {
	m := newMockLookup()
	c := mockCache(t, m, time.Hour) // renew loop dormant for this test
	defer c.Close()

	_, err := c.Get("dn", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.lookupCnt.Load(); got != 1 {
		t.Fatal("setup")
	}

	// Advance the mock clock past TTL → entry is now expired.
	m.advance(120 * time.Second)
	_, err = c.Get("dn", 1, 1)
	if err != nil {
		t.Fatalf("Get after expiry: %v", err)
	}
	if got := m.lookupCnt.Load(); got != 2 {
		t.Errorf("expected re-lookup after expiry, lookupCnt=%d", got)
	}
}

func TestExtentMRCache_SingleFlight(t *testing.T) {
	m := newMockLookup()
	m.callDelay = 30 * time.Millisecond
	c := mockCache(t, m, time.Hour)
	defer c.Close()

	const N = 16
	infos := make([]*LeaseInfo, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			infos[i], errs[i] = c.Get("dn", 1, 42)
		}()
	}
	wg.Wait()

	if got := m.lookupCnt.Load(); got != 1 {
		t.Errorf("single-flight: lookupCnt = %d, want 1", got)
	}
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Errorf("g%d: %v", i, errs[i])
		}
		if infos[i] != infos[0] {
			t.Errorf("g%d: got different LeaseInfo", i)
		}
	}
}

func TestExtentMRCache_SingleFlightFailure(t *testing.T) {
	m := newMockLookup()
	m.callDelay = 20 * time.Millisecond
	m.failKeys[99] = errors.New("simulated lookup failure")
	c := mockCache(t, m, time.Hour)
	defer c.Close()

	const N = 8
	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = c.Get("dn", 1, 99)
		}()
	}
	wg.Wait()
	for i := 0; i < N; i++ {
		if errs[i] == nil {
			t.Errorf("g%d: expected error", i)
		}
	}
}

func TestExtentMRCache_Invalidate(t *testing.T) {
	m := newMockLookup()
	c := mockCache(t, m, time.Hour)
	defer c.Close()

	_, err := c.Get("dn", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.Len() != 1 {
		t.Fatalf("Len = %d", c.Len())
	}

	c.Invalidate("dn", 1, 1)
	if c.Len() != 0 {
		t.Errorf("Len after Invalidate = %d", c.Len())
	}

	pre := m.lookupCnt.Load()
	_, err = c.Get("dn", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if m.lookupCnt.Load() != pre+1 {
		t.Errorf("expected fresh lookup after Invalidate")
	}
}

func TestExtentMRCache_Close(t *testing.T) {
	m := newMockLookup()
	c := mockCache(t, m, time.Hour)

	_, err := c.Get("dn", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if _, err := c.Get("dn", 1, 2); !errors.Is(err, ErrExtentMRCacheClosed) {
		t.Errorf("Get after Close: %v", err)
	}
	c.Close() // idempotent
}

func TestExtentMRCache_CloseUnblocksPendingLookups(t *testing.T) {
	m := newMockLookup()
	m.callDelay = 200 * time.Millisecond
	c := mockCache(t, m, time.Hour)

	errCh := make(chan error, 1)
	go func() {
		_, err := c.Get("dn", 1, 1)
		errCh <- err
	}()
	time.Sleep(30 * time.Millisecond) // let the lookup start
	c.Close()

	select {
	case err := <-errCh:
		// Either lookup completed in time and got valid result, or it
		// observed close. Both are acceptable; we only require that
		// Close doesn't hang.
		_ = err
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock pending lookup within 1s")
	}
}

func TestExtentMRCache_RenewerExtendsExpiry(t *testing.T) {
	// Short TTL + short renew margin + short interval to exercise
	// the renew path under wall-clock without faking the clock
	// (renewLoop ticks on a real ticker).
	m := newMockLookup()
	m.ttlSeconds = 1 // 1 second TTL
	c, err := newExtentMRCache(extentMRCacheConfig{
		LookupTTLHint: time.Second,
		RenewMargin:   2 * time.Second, // always due
		RenewInterval: 50 * time.Millisecond,
		NowFn:         time.Now,
	}, m.lookup, m.renew)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	info, err := c.Get("dn", 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	origExp := info.expiresAtNanos.Load()

	// Wait several renewer ticks.
	time.Sleep(250 * time.Millisecond)

	if got := m.renewCnt.Load(); got == 0 {
		t.Errorf("renewer never ran (renewCnt=%d)", got)
	}
	if newExp := info.expiresAtNanos.Load(); newExp <= origExp {
		t.Errorf("renewer did not advance expiry: old=%d new=%d", origExp, newExp)
	}
}

func TestExtentMRCache_RenewerInvalidatesOnError(t *testing.T) {
	m := newMockLookup()
	m.ttlSeconds = 1
	renewErr := errors.New("simulated renew failure")
	c, err := newExtentMRCache(extentMRCacheConfig{
		LookupTTLHint: time.Second,
		RenewMargin:   2 * time.Second,
		RenewInterval: 30 * time.Millisecond,
		NowFn:         time.Now,
	}, m.lookup, func(addr string, leaseID uint64, ttlHint time.Duration) (uint32, error) {
		return 0, renewErr
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Get("dn", 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if c.Len() != 1 {
		t.Fatal("setup")
	}
	// Wait long enough for at least one renew attempt.
	time.Sleep(150 * time.Millisecond)
	if c.Len() != 0 {
		t.Errorf("entry not invalidated after renew failure: Len=%d", c.Len())
	}
}

func TestExtentMRCache_ConcurrentStress(t *testing.T) {
	// Hammer Get / Invalidate from many goroutines over a small key
	// space. Combined with -race this catches map/lookup races.
	m := newMockLookup()
	c := mockCache(t, m, time.Hour)
	defer c.Close()

	const workers = 32
	const ops = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				ext := uint64((w*ops + i) % 32)
				_, err := c.Get("dn", 1, ext)
				if err != nil {
					t.Errorf("Get(%d): %v", ext, err)
					return
				}
				if i%50 == 0 {
					c.Invalidate("dn", 1, ext)
				}
			}
		}()
	}
	wg.Wait()
}
