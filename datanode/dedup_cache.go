package datanode

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

// Idempotency cache for normal-extent write packets.
//
// The SDK retries failed writes — most commonly when an RDMA round-trip's
// response is lost, the SDK falls back to TCP and replays the same packet.
// The leader's first attempt already committed the data, so the replay
// hits checkPacketAndPrepare's `dataSize == Offset` check, fails with
// OpTryOtherExtent, and the SDK then enters recovery and reallocates an
// extent for no good reason. Worse, the replay also propagates to the
// followers (via the leader's standard replication path) where the same
// mismatch surfaces as `repl follower rdma: follower ResultCode=244`
// in the leader's warn log.
//
// Caching `(PartitionID, ExtentID, ReqID) → OpOk` at the leader (and
// transparently at followers, since they run the same handler) lets us
// short-circuit the replay with a synthetic OpOk reply, matching what
// the SDK would have seen on the first attempt. The cache is bounded
// in size and time so it cannot grow unbounded under any traffic shape.
//
// Scope:
//   - Normal-extent AppendWrites only (tiny-extent writes have
//     leader-assigned ExtentID/Offset, so cache keys are unstable).
//   - Only OpOk results are remembered. Errors are not cached — a
//     retry might succeed where the first attempt failed.

const (
	writeDedupTTL             = 5 * time.Minute
	writeDedupMaxEntries      = 1000000
	writeDedupCleanupInterval = 60 * time.Second
)

type writeDedupKey struct {
	PartitionID uint64
	ExtentID    uint64
	ReqID       int64
}

type writeDedupCache struct {
	mu      sync.RWMutex
	entries map[writeDedupKey]time.Time

	stop     chan struct{}
	stopOnce sync.Once
}

func newWriteDedupCache() *writeDedupCache {
	c := &writeDedupCache{
		entries: make(map[writeDedupKey]time.Time, writeDedupMaxEntries/8),
		stop:    make(chan struct{}),
	}
	go c.runCleanup()
	return c
}

// Has reports whether the given (pid, extID, reqID) was successfully
// applied within the configured TTL. Nil-safe so callers can avoid a
// branch when the cache is disabled.
func (c *writeDedupCache) Has(pid, extID uint64, reqID int64) bool {
	if c == nil {
		return false
	}
	k := writeDedupKey{pid, extID, reqID}
	c.mu.RLock()
	expiry, ok := c.entries[k]
	c.mu.RUnlock()
	if !ok {
		return false
	}
	return time.Now().Before(expiry)
}

// Remember records this (pid, extID, reqID) as successfully applied.
// Should be called only after the local write actually succeeded.
func (c *writeDedupCache) Remember(pid, extID uint64, reqID int64) {
	if c == nil {
		return
	}
	k := writeDedupKey{pid, extID, reqID}
	expiry := time.Now().Add(writeDedupTTL)
	c.mu.Lock()
	if len(c.entries) >= writeDedupMaxEntries {
		// Random eviction: O(1), avoids LRU heap contention. The TTL
		// sweep will also keep size bounded under normal conditions.
		for ek := range c.entries {
			delete(c.entries, ek)
			break
		}
	}
	c.entries[k] = expiry
	c.mu.Unlock()
}

// Stop terminates the background cleanup goroutine. Safe to call multiple
// times. Intended for tests and graceful shutdown.
func (c *writeDedupCache) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() { close(c.stop) })
}

func (c *writeDedupCache) runCleanup() {
	ticker := time.NewTicker(writeDedupCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.evictExpired()
		case <-c.stop:
			return
		}
	}
}

func (c *writeDedupCache) evictExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for k, exp := range c.entries {
		if now.After(exp) {
			delete(c.entries, k)
			removed++
		}
	}
	if removed > 0 && log.EnableDebug() {
		log.LogDebugf("writeDedupCache: evicted %d expired entries, remaining %d", removed, len(c.entries))
	}
}
