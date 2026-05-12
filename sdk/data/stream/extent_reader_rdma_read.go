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
	// defaultReadViaRDMAReadTimeout is the fallback for
	// cfg.ReadTimeoutMs when the value is 0 or unset.
	//
	// History — three production data points calibrated this:
	//   - 5 s (original): max read latency 55 s, p99 41 s; failed
	//     WRs blocked the read 5 s before fallback even started.
	//   - 1 s (cut #1): max 15 s, p99 12 s — long tail dropped
	//     3× — but p50 went from 484 ms to 745 ms because some
	//     reads that would have completed in 1–5 s were now killed
	//     and re-tried via the slower fallback. Net regression on
	//     the median.
	//   - 2 s (current): the empirical sweet spot. Keeps the bulk
	//     of in-flight WRs that would complete in <2 s while still
	//     capping the tail at ~2 s + fallback per failed chunk.
	//
	// Healthy RoCE round-trips are ≈100 µs; 2 s is 20 000× over
	// nominal, enough to absorb transient queueing without
	// false-positive timeouts. Operators can override with
	// rdmaReadTimeoutMs= for fabric-specific tuning.
	defaultReadViaRDMAReadTimeout = 2 * time.Second
)

// phaseAWarnEvery is declared in phase_a_counters.go (build-tag-free)
// so the sampler is available to non-RDMA builds too.

// readViaRDMAReadTimeout is the active per-WR Phase A read timeout.
// Set by InitRDMAConnPool from RDMAPoolConfig.ReadTimeoutMs; falls
// back to defaultReadViaRDMAReadTimeout when the cfg is 0. Stored as
// a package var (not const) so operators can flip the value via
// mount option without rebuilding.
var readViaRDMAReadTimeout = defaultReadViaRDMAReadTimeout

// Phase A observability counters and snapshot helpers live in
// phase_a_counters.go (build-tag-free) so the periodic stats logger
// can read them in both RDMA and stub builds. This file is the only
// writer.

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
	// Operator kill switch: when rdmaOneSidedReadEnabled is false
	// (cfg.OneSidedReadDisabled at startup), skip Phase A entirely
	// and let the caller drop to the two-sided path. No attempt
	// counter increment so the stats line correctly shows zero
	// one-sided traffic.
	if !rdmaOneSidedReadEnabled {
		return 0, nil
	}
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

	// Acquire a conn from the Phase A pool WITHOUT taking a slot.
	// The pool will already have dialed the conn (lookup did the dial
	// in cache.Get above), so ConnIfReady is a fast lookup. If the
	// conn has been faulted by an earlier WR error, ConnIfReady
	// returns (nil, false) and we silently fall back to two-sided;
	// the next lookup will trigger a fresh dial via the slot pool.
	//
	// addr translation: pool maps are keyed by the POST-SHIFT RDMA
	// listen addr (lookup's rdmaRoundTripVia translated it before
	// AcquireSlotForKey). rdmaAddr here is the caller's TCP addr
	// from sc.CurrAddr() — without this shift, ConnIfReady looks
	// up a key that was never inserted and returns false even when
	// a live conn exists. Reproduced in production: attempt=7213
	// success=0 fail.conn=7213, every read missed the cache.
	//
	// Same pool as the lookup above — that's the whole point. lookup
	// + read on one conn ⟹ one PD ⟹ rkey returned by lookup is
	// valid for the read QP. Sharing failure-domain with lookup is
	// acceptable because both fail together anyway (no QP means no
	// RDMA at all for this DataNode until redial).
	if rdmaPhaseAConnPool == nil {
		return 0, nil
	}
	poolAddr := rdmaAddr
	if rdmaConnPortShift != 0 {
		poolAddr = util.ShiftAddrPort(rdmaAddr, rdmaConnPortShift)
	}
	// Hash-route the read to the SAME conn that served the lookup.
	// rdmaRoundTripVia (used by lookupExtentMR) builds the key
	// "pid-extId" for non-read operations and AcquireSlotForKey
	// pins the lookup to conn[hash(key) % maxConns]. ConnIfReadyForKey
	// mirrors that hash so the RDMA Read WR posts on the same QP /
	// same PD that owns the lease's rkey. If maxConns is 1 this is
	// a no-op (anyAliveConn would have returned the same conn);
	// for maxConns > 1 it's the correctness lynchpin without which
	// the read would silently 5-second-timeout on every chunk.
	poolKey := fmt.Sprintf("%d-%d", reader.dp.PartitionID, reqPacket.ExtentID)
	conn, ok := rdmaPhaseAConnPool.ConnIfReadyForKey(poolAddr, poolKey)
	if !ok {
		if phaseAShouldWarn(&phaseACounters.connErr) {
			log.LogWarnf("rdma Phase A: no ready conn for poolAddr=%s key=%s (tcpAddr=%s) — falling back (next lookup will dial)", poolAddr, poolKey, rdmaAddr)
		}
		return 0, nil // fall through silently — no error to invalidate cache over
	}
	// Bucket the conn index this hash routed to. Same modulo math as
	// slot_pool.ConnIfReadyForKey so the histogram lines up exactly
	// with which slot got dialed. Cheap atomic add; no impact on the
	// hot path. Skipped when maxConns is 0 (pool not initialised) or
	// the cap is exceeded (see phaseAConnIdxMax doc).
	if idx := rdma.HashKeyToConnIndex(poolKey, rdmaPhaseAConnPool.MaxConns()); idx >= 0 {
		recordPhaseAConnIdx(idx)
	}

	// Phase A chunks against the conn's read scratch slot size — not
	// util.ReadBlockSize. The scratch slot is the upper bound on a
	// single RDMA Read WR's payload; matching the chunk size to it
	// minimises per-WR overhead (one WR per slot) without violating
	// PostRDMAReadAndWait's "n > slotSize" guard. With slot=4 MiB
	// (the default), a 16 MiB object splits into 4 chunks — perfect
	// for readPrefetchDepth=4. Falling back to util.ReadBlockSize
	// when ReadSlotSize is 0 keeps stub-build callers correct.
	chunkSize := conn.ReadSlotSize()
	if chunkSize <= 0 {
		chunkSize = util.ReadBlockSize
	}
	chunks := splitReadChunks(extentOffset, size, chunkSize)
	recordPhaseAChunkBucket(len(chunks))
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
		atomic.AddInt64(&phaseACounters.bytes, int64(n))
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
	atomic.AddInt64(&phaseACounters.bytes, int64(total))
	return total, nil
}

// readChunkViaRDMARead is the per-chunk worker for the one-sided
// fast path. Posts an RDMA Read against the lease's (rkey, VA +
// chunkOff) and waits for completion; the conn's per-slot waiter
// machinery handles the bookkeeping.
//
// Slow-tail diagnostic: times every chunk and bumps phaseACounters.
// slowChunks when the post-and-wait exceeds phaseASlowChunkThreshold.
// Every phaseAWarnEvery'th slow chunk emits a WARN line carrying the
// (poolAddr, conn-index, pid, ext, offset, size, elapsed) tuple so
// an operator can correlate which datanode / QP / extent the long
// tail clusters on.
func (reader *ExtentReader) readChunkViaRDMARead(conn *rdma.RDMAConn, lease *LeaseInfo, req *ExtentRequest, chk readChunkSpec) (int, error) {
	dst := req.Data[chk.bufOff : chk.bufOff+chk.bufSize]
	remoteVA := lease.VA + uint64(chk.extentOff)
	start := time.Now()
	if err := conn.PostRDMAReadAndWait(dst, remoteVA, lease.Rkey, readViaRDMAReadTimeout); err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	if elapsed >= phaseASlowChunkThreshold {
		n := recordPhaseASlowChunk()
		// Slow chunks are by definition rare relative to total chunks
		// (they're the long-tail outliers). The 1/256 sampling we use
		// for failure logs is far too aggressive here — at 27 % slow
		// rate over 938 chunks we saw only ONE WARN, not enough to
		// spot patterns. Sample every 10th instead, with a separate
		// always-log path for the truly bad ones (≥ 2 s = stalled
		// rather than slow). This trades some log volume for the
		// ability to correlate slow tail with specific connIdx /
		// datanode / extent IDs.
		shouldLog := n == 1 || n%phaseASlowChunkWarnEvery == 0 || elapsed >= phaseAStalledChunkThreshold
		if shouldLog {
			pid := reader.dp.PartitionID
			extID := reader.key.ExtentId
			poolKey := fmt.Sprintf("%d-%d", pid, extID)
			idx := rdma.HashKeyToConnIndex(poolKey, rdmaPhaseAConnPool.MaxConns())
			tag := "SLOW"
			if elapsed >= phaseAStalledChunkThreshold {
				tag = "STALLED"
			}
			log.LogWarnf("rdma Phase A %s chunk addr=%s connIdx=%d pid=%d ext=%d off=%d size=%d elapsed=%v threshold=%v (n=%d)",
				tag, reader.dp.LeaderAddr, idx, pid, extID,
				chk.extentOff, chk.bufSize, elapsed, phaseASlowChunkThreshold, n)
		}
	}
	return chk.bufSize, nil
}
