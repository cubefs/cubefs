//go:build linux && rdma

package stream

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/rdma"
)

// One-sided RDMA Read fast path for ExtentReader (Sprint A.6).
//
// Bound to the SDK process via a singleton extentMRCache initialised
// in the same path as the conn pool. ExtentReader.Read tries this
// before the two-sided readChunksParallel — a warm cache hit makes
// each chunk a single RDMA Read with zero server-side CPU on the
// data path.
//
// Observability rule: every code path in this file must either succeed
// (recorded via the metric counter below) or emit a structured log line
// at WARN level. Silent fallthrough is forbidden — the first deploy of
// Phase A burned an afternoon of triage because cache.Get failures were
// at LogDebugf level. Each failure category gets its own atomic counter
// + sampled warn (1 per 256 events) so production logs don't drown but
// operators can still confirm the path is alive.

const (
	// readViaRDMAReadTimeout caps how long one chunk's RDMA Read
	// may take before we abandon and fall back. Sized for healthy
	// RoCE round-trips (≈100 µs) plus generous headroom for
	// transient congestion.
	readViaRDMAReadTimeout = 5 * time.Second

	// phaseAWarnEvery samples failure logs so a permanent fault
	// doesn't spam the log file — but the first occurrence is
	// always logged so the first deploy surfaces the issue.
	phaseAWarnEvery = 256
)

// Phase A observability counters. All are accessed via atomic helpers
// so they're safe to read from the stats logger without holding a
// mutex. Stats output joins them every 60s alongside the existing
// two-sided RDMA stats.
var (
	phaseACounters struct {
		attempt      int64 // tryReadViaRDMARead called at all
		success      int64 // returned (n>0, nil)
		noCacheInit  int64 // cache was never initialised (returned 0,nil)
		lookupErr    int64 // cache.Get returned err
		boundsErr    int64 // read range outside lease.Size
		connErr      int64 // rdmaConnPool.ConnForKey failed
		wrErr        int64 // PostRDMAReadAndWait failed
	}
)

// PhaseAStatsSnapshot returns the atomic counter values for the stats
// logger to compute deltas. Exported via an accessor (rather than a
// package-level variable read) so the rdma package — which lives below
// the SDK — doesn't need an import cycle.
func PhaseAStatsSnapshot() (attempt, success, noCache, lookup, bounds, conn, wr int64) {
	return atomic.LoadInt64(&phaseACounters.attempt),
		atomic.LoadInt64(&phaseACounters.success),
		atomic.LoadInt64(&phaseACounters.noCacheInit),
		atomic.LoadInt64(&phaseACounters.lookupErr),
		atomic.LoadInt64(&phaseACounters.boundsErr),
		atomic.LoadInt64(&phaseACounters.connErr),
		atomic.LoadInt64(&phaseACounters.wrErr)
}

// phaseAShouldWarn returns true on count==1 (first occurrence) and
// every phaseAWarnEvery-th thereafter. Cheap atomic Add — fine on the
// hot path.
func phaseAShouldWarn(counter *int64) bool {
	n := atomic.AddInt64(counter, 1)
	return n == 1 || n%phaseAWarnEvery == 0
}

var (
	// extentMRCacheRef holds the SDK-wide *extentMRCache. We use
	// atomic.Value (rather than atomic.Pointer[T] which is Go 1.19+)
	// to stay on Go 1.17. Stored value is *extentMRCache; nil-safe
	// via the helper below.
	extentMRCacheRef atomic.Value
)

// loadExtentMRCache returns the cached pointer or nil. Wrapping the
// type assertion here keeps the Load sites tidy and ensures we never
// panic on the empty-value (nil) case before any Store.
func loadExtentMRCache() *extentMRCache {
	v := extentMRCacheRef.Load()
	if v == nil {
		return nil
	}
	return v.(*extentMRCache)
}

// initExtentMRCacheOnce sets up the SDK-wide cache the first time a
// reader needs it. Idempotent; subsequent calls observe the cached
// instance. Failures (e.g. allocation problems) leave the pointer
// nil so callers skip the fast path without aborting the read.
var initExtentMRCacheOnce sync.Once

func ensureExtentMRCache() *extentMRCache {
	if c := loadExtentMRCache(); c != nil {
		return c
	}
	initExtentMRCacheOnce.Do(func() {
		c, err := newProductionExtentMRCache()
		if err != nil {
			// CRITICAL: surfaces the case where Phase A is compiled
			// in but the cache never came up. If this line is
			// absent in production logs, the call path below was
			// never reached.
			log.LogWarnf("rdma Phase A: extent MR cache init FAILED: %v — one-sided reads disabled", err)
			return
		}
		extentMRCacheRef.Store(c)
		log.LogInfof("rdma Phase A: extent MR cache initialised — one-sided reads enabled")
	})
	return loadExtentMRCache()
}

// invalidateExtentMRCache is the hook ExtentReader calls after a
// failed one-sided read so the next attempt forces a fresh lookup.
// Safe to call even when the cache hasn't been initialised.
func invalidateExtentMRCache(addr string, pid, extentID uint64) {
	c := loadExtentMRCache()
	if c == nil {
		return
	}
	c.Invalidate(addr, pid, extentID)
}

// tryReadViaRDMARead attempts to satisfy req entirely via one-sided
// RDMA Reads against a cached lease. Returns (bytesRead, nil) on
// success; (0, error) on any failure so the caller can drop to the
// two-sided readViaRDMA path.
//
// Returns (0, nil) when the cache hasn't been initialised — that's
// "no fast path available" rather than a real error, so the caller
// silently moves on without invalidating anything.
func (reader *ExtentReader) tryReadViaRDMARead(rdmaAddr string, reqPacket *Packet, req *ExtentRequest, extentOffset, size int) (int, error) {
	atomic.AddInt64(&phaseACounters.attempt, 1)
	cache := ensureExtentMRCache()
	if cache == nil {
		// Sampled warn so the first cold-start failure is visible
		// without flooding under sustained outage.
		if phaseAShouldWarn(&phaseACounters.noCacheInit) {
			log.LogWarnf("rdma Phase A: cache unavailable (init failed earlier), addr=%s pid=%d ext=%d — falling back to two-sided",
				rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID)
		}
		return 0, nil
	}
	lease, err := cache.Get(rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID)
	if err != nil {
		if phaseAShouldWarn(&phaseACounters.lookupErr) {
			log.LogWarnf("rdma Phase A: cache.Get FAILED addr=%s pid=%d ext=%d: %v",
				rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID, err)
		}
		return 0, fmt.Errorf("extent MR cache lookup: %w", err)
	}
	if lease == nil {
		if phaseAShouldWarn(&phaseACounters.lookupErr) {
			log.LogWarnf("rdma Phase A: cache.Get returned nil lease addr=%s pid=%d ext=%d",
				rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID)
		}
		return 0, errors.New("extent MR cache returned nil lease")
	}
	// Bounds check: the entire read range must fit inside the
	// lease's published extent size. A range past the end signals
	// either (a) stale lease (extent grew, cache out of date) or
	// (b) caller bug. Either way, fall back.
	if uint64(extentOffset+size) > lease.Size {
		if phaseAShouldWarn(&phaseACounters.boundsErr) {
			log.LogWarnf("rdma Phase A: bounds miss addr=%s pid=%d ext=%d off=%d sz=%d leaseSize=%d",
				rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID, extentOffset, size, lease.Size)
		}
		return 0, fmt.Errorf("read range [%d, %d) exceeds lease size %d",
			extentOffset, extentOffset+size, lease.Size)
	}

	// Acquire a conn for the addr without taking a slot AND without
	// triggering a new dial. Phase A is best-effort acceleration —
	// if the two-sided path hasn't built a conn yet, we don't want
	// to race it (and risk the server's per-peer QP cap rejecting a
	// second dial, which empirically happens on this cluster).
	// ConnIfReady returns false silently in that case so the caller
	// drops to the two-sided path without an error log.
	//
	// rdmaAddr here is the caller's view (TCP listen port). The
	// conn pool is keyed by the post-shift RDMA address — same
	// translation rdmaRoundTrip does in rdma_client.go. Without
	// this shift, ConnIfReady misses every time even when conn 0
	// exists, which is exactly what the first deploy of this fix
	// reproduced: attempt=68087 conn=68087 hit=0%.
	poolAddr := rdmaAddr
	if rdmaConnPortShift != 0 {
		poolAddr = util.ShiftAddrPort(rdmaAddr, rdmaConnPortShift)
	}
	conn, ok := rdmaConnPool.ConnIfReady(poolAddr)
	if !ok {
		if phaseAShouldWarn(&phaseACounters.connErr) {
			log.LogWarnf("rdma Phase A: no ready conn for poolAddr=%s (tcpAddr=%s) — falling back (two-sided will dial)", poolAddr, rdmaAddr)
		}
		return 0, nil // fall through silently — no error to invalidate cache over
	}

	chunks := splitReadChunks(extentOffset, size, util.ReadBlockSize)
	if len(chunks) == 0 {
		return 0, nil
	}

	// Single chunk: avoid goroutine + WaitGroup overhead.
	if len(chunks) == 1 {
		chk := chunks[0]
		n, cerr := reader.readChunkViaRDMARead(conn, lease, req, chk)
		if cerr != nil {
			if phaseAShouldWarn(&phaseACounters.wrErr) {
				log.LogWarnf("rdma Phase A: WR FAILED addr=%s pid=%d ext=%d off=%d sz=%d: %v",
					rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID, chk.extentOff, chk.bufSize, cerr)
			}
			return 0, cerr
		}
		atomic.AddInt64(&phaseACounters.success, 1)
		return n, nil
	}

	// Parallel: each chunk runs on its own goroutine, taking its own
	// per-conn read waiter slot. Bounded by readPrefetchDepth (the
	// same cap used by the two-sided readChunksParallel) so the
	// concurrency profile stays familiar.
	sem := make(chan struct{}, readPrefetchDepth)
	errCh := make(chan error, len(chunks))
	var wg sync.WaitGroup
	for i := range chunks {
		chk := chunks[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, cerr := reader.readChunkViaRDMARead(conn, lease, req, chk); cerr != nil {
				errCh <- cerr
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for cerr := range errCh {
		if cerr != nil {
			if phaseAShouldWarn(&phaseACounters.wrErr) {
				log.LogWarnf("rdma Phase A: WR FAILED (parallel) addr=%s pid=%d ext=%d: %v",
					rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID, cerr)
			}
			return 0, cerr
		}
	}

	total := 0
	for _, c := range chunks {
		total += c.bufSize
	}
	atomic.AddInt64(&phaseACounters.success, 1)
	return total, nil
}

// readChunkViaRDMARead is the per-chunk worker for the one-sided
// fast path. Posts an RDMA Read against the lease's (rkey, VA +
// chunkOff) and waits for completion; the conn's per-slot waiter
// machinery handles the bookkeeping.
func (reader *ExtentReader) readChunkViaRDMARead(conn *rdma.RDMAConn, lease *LeaseInfo, req *ExtentRequest, chk readChunkSpec) (int, error) {
	dst := req.Data[chk.bufOff : chk.bufOff+chk.bufSize]
	remoteVA := lease.VA + uint64(chk.extentOff)
	if err := conn.PostRDMAReadAndWait(dst, remoteVA, lease.Rkey, readViaRDMAReadTimeout); err != nil {
		return 0, err
	}
	return chk.bufSize, nil
}
