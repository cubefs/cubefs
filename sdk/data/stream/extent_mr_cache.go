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
	"time"
)

// extentMRCache stores per-(addr, partition, extent) leases issued by
// the DataNode's OpExtentMRLookup handler. Cache hits skip the
// round-trip and go straight to RDMA Read; cache misses (and entries
// nearing TTL) trigger a synchronous lookup or a background renew.
//
// Built around three pluggable hooks so the cache logic stays
// transport-agnostic and unit-testable:
//   - lookupFn   does OpExtentMRLookup over RDMA, returns lease info
//   - renewFn    does OpExtentMRRenew, returns the granted TTL
//   - clockFn    returns the current time; tests inject a fake clock
//
// Concurrency invariants:
//   - The cache mutex protects only the map + per-key in-flight
//     state. Lookup/renew callbacks run without it so a slow remote
//     never blocks the rest of the cache.
//   - Single-flight: concurrent Get-misses for the same key share
//     one lookup call.
//   - Background renewer is one goroutine that wakes ~TTL/4 and
//     re-grants any entry whose remaining lifetime is below TTL/2.

// ErrExtentMRCacheClosed is returned by Get after Close.
var ErrExtentMRCacheClosed = errors.New("rdma: extentMRCache closed")

// LeaseInfo describes one valid lease as seen by the SDK. Rkey/VA
// are passed verbatim into a subsequent RDMA Read WR; expiresAt
// drives the renewer and read-time staleness check.
type LeaseInfo struct {
	Addr      string // DataNode addr that issued the lease
	PartitionID uint64
	ExtentID  uint64

	LeaseID uint64
	Rkey    uint32
	VA      uint64
	Size    uint64

	// expiresAtNanos uses atomic helpers (Go 1.17 compat —
	// atomic.Int64 is Go 1.19+).
	expiresAtNanos int64
}

// IsExpired reports whether the lease's deadline has passed.
func (l *LeaseInfo) IsExpired(now time.Time) bool {
	return atomic.LoadInt64(&l.expiresAtNanos) <= now.UnixNano()
}

// extentMRCacheKey is the cache map key. Addr is included because
// the same (pid, extent) may have different lease IDs on different
// DataNodes (each registers its own MR over its own copy of the
// extent file).
type extentMRCacheKey struct {
	addr        string
	partitionID uint64
	extentID    uint64
}

// extentMRLookupFunc executes OpExtentMRLookup over the RDMA
// transport. Production callers wire this to rdma_client.go's
// lookup round-trip; tests inject mocks.
type extentMRLookupFunc func(addr string, pid, extentID uint64, ttlHint time.Duration) (*LeaseInfo, error)

// extentMRRenewFunc executes OpExtentMRRenew and returns the new
// granted TTL in seconds.
type extentMRRenewFunc func(addr string, leaseID uint64, ttlHint time.Duration) (uint32, error)

// extentMRCacheConfig groups tuning parameters so tests can vary
// them without burying constants in code under test.
type extentMRCacheConfig struct {
	LookupTTLHint time.Duration // requested lease duration
	RenewMargin   time.Duration // renew when remaining < this
	RenewInterval time.Duration // background renewer tick
	NowFn         func() time.Time
}

func defaultExtentMRCacheConfig() extentMRCacheConfig {
	return extentMRCacheConfig{
		LookupTTLHint: 60 * time.Second,
		RenewMargin:   30 * time.Second,
		RenewInterval: 5 * time.Second,
		NowFn:         time.Now,
	}
}

// pendingLookup tracks an in-flight lookup so duplicate Get calls
// for the same key collapse into one round-trip.
type pendingLookup struct {
	done  chan struct{}
	info  *LeaseInfo
	err   error
}

type extentMRCache struct {
	cfg      extentMRCacheConfig
	lookupFn extentMRLookupFunc
	renewFn  extentMRRenewFunc

	mu       sync.Mutex
	entries  map[extentMRCacheKey]*LeaseInfo
	pendings map[extentMRCacheKey]*pendingLookup

	closeCh chan struct{}
	wg      sync.WaitGroup
	closed  bool
}

// newExtentMRCache constructs a cache with the supplied callbacks.
// A nil cfg.NowFn defaults to time.Now. Negative / zero intervals
// fall back to the defaults.
func newExtentMRCache(cfg extentMRCacheConfig, lookupFn extentMRLookupFunc, renewFn extentMRRenewFunc) (*extentMRCache, error) {
	if lookupFn == nil {
		return nil, errors.New("rdma: extentMRCache: nil lookupFn")
	}
	if renewFn == nil {
		return nil, errors.New("rdma: extentMRCache: nil renewFn")
	}
	d := defaultExtentMRCacheConfig()
	if cfg.LookupTTLHint <= 0 {
		cfg.LookupTTLHint = d.LookupTTLHint
	}
	if cfg.RenewMargin <= 0 {
		cfg.RenewMargin = d.RenewMargin
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = d.RenewInterval
	}
	if cfg.NowFn == nil {
		cfg.NowFn = d.NowFn
	}
	c := &extentMRCache{
		cfg:      cfg,
		lookupFn: lookupFn,
		renewFn:  renewFn,
		entries:  make(map[extentMRCacheKey]*LeaseInfo),
		pendings: make(map[extentMRCacheKey]*pendingLookup),
		closeCh:  make(chan struct{}),
	}
	c.wg.Add(1)
	go c.renewLoop()
	return c, nil
}

// Get returns a non-expired LeaseInfo for the key, fetching via
// lookupFn on miss / expiry. Concurrent misses for the same key
// share one lookup call (single-flight).
func (c *extentMRCache) Get(addr string, pid, extentID uint64) (*LeaseInfo, error) {
	key := extentMRCacheKey{addr, pid, extentID}
	now := c.cfg.NowFn()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrExtentMRCacheClosed
	}
	if entry, ok := c.entries[key]; ok && !entry.IsExpired(now) {
		c.mu.Unlock()
		return entry, nil
	}
	// Stale entry → drop it and fall through to a fresh lookup.
	delete(c.entries, key)

	if p, ok := c.pendings[key]; ok {
		c.mu.Unlock()
		<-p.done
		return p.info, p.err
	}
	p := &pendingLookup{done: make(chan struct{})}
	c.pendings[key] = p
	c.mu.Unlock()

	info, err := c.lookupFn(addr, pid, extentID, c.cfg.LookupTTLHint)

	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() {
		close(p.done)
		delete(c.pendings, key)
	}()

	if c.closed {
		p.err = ErrExtentMRCacheClosed
		return nil, ErrExtentMRCacheClosed
	}
	if err != nil {
		p.err = err
		return nil, err
	}
	c.entries[key] = info
	p.info = info
	return info, nil
}

// Invalidate drops the cached entry for key. Used by callers that
// observe an RDMA Read failure (e.g. server's MR was deregistered
// after extent delete) so the next Get triggers a fresh lookup.
func (c *extentMRCache) Invalidate(addr string, pid, extentID uint64) {
	key := extentMRCacheKey{addr, pid, extentID}
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// Len reports the current number of cached entries; for tests/metrics.
func (c *extentMRCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Close stops the renewer and clears the cache. Pending Get calls
// observe ErrExtentMRCacheClosed.
func (c *extentMRCache) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.closeCh)
	// Signal pending lookups so their waiters wake up promptly.
	for _, p := range c.pendings {
		p.err = ErrExtentMRCacheClosed
		close(p.done)
	}
	c.pendings = map[extentMRCacheKey]*pendingLookup{}
	c.entries = map[extentMRCacheKey]*LeaseInfo{}
	c.mu.Unlock()
	c.wg.Wait()
}

// renewLoop wakes every cfg.RenewInterval and re-grants any entry
// whose remaining lifetime is below cfg.RenewMargin. Calls renewFn
// without holding the mutex so a slow remote doesn't stall Gets.
func (c *extentMRCache) renewLoop() {
	defer c.wg.Done()
	tick := time.NewTicker(c.cfg.RenewInterval)
	defer tick.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-tick.C:
			c.renewExpiringEntries()
		}
	}
}

func (c *extentMRCache) renewExpiringEntries() {
	now := c.cfg.NowFn()
	marginNanos := int64(c.cfg.RenewMargin)
	// Snapshot under lock so iteration doesn't race deletes.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	type renewTarget struct {
		key  extentMRCacheKey
		info *LeaseInfo
	}
	targets := make([]renewTarget, 0)
	for k, e := range c.entries {
		if atomic.LoadInt64(&e.expiresAtNanos)-now.UnixNano() < marginNanos {
			targets = append(targets, renewTarget{k, e})
		}
	}
	c.mu.Unlock()

	for _, t := range targets {
		granted, err := c.renewFn(t.info.Addr, t.info.LeaseID, c.cfg.LookupTTLHint)
		if err != nil {
			// Lease unknown / expired server-side: drop so next Get
			// triggers a fresh lookup. Network errors also drop the
			// entry; conservative but safe.
			c.Invalidate(t.info.Addr, t.info.PartitionID, t.info.ExtentID)
			continue
		}
		newDeadline := c.cfg.NowFn().Add(time.Duration(granted) * time.Second).UnixNano()
		atomic.StoreInt64(&t.info.expiresAtNanos, newDeadline)
	}
}
