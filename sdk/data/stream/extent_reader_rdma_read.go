//go:build linux && rdma

package stream

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/rdma"
)

// One-sided RDMA Read fast path for ExtentReader (Sprint A.6).
//
// Bound to the SDK process via a singleton extentMRCache initialised
// in the same path as the conn pool. ExtentReader.Read tries this
// before the two-sided readChunksParallel — a warm cache hit makes
// each chunk a single RDMA Read with zero server-side CPU on the
// data path.

const (
	// readViaRDMAReadTimeout caps how long one chunk's RDMA Read
	// may take before we abandon and fall back. Sized for healthy
	// RoCE round-trips (≈100 µs) plus generous headroom for
	// transient congestion.
	readViaRDMAReadTimeout = 5 * time.Second
)

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
		if err == nil {
			extentMRCacheRef.Store(c)
		}
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
	cache := ensureExtentMRCache()
	if cache == nil {
		return 0, nil
	}
	lease, err := cache.Get(rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID)
	if err != nil {
		return 0, fmt.Errorf("extent MR cache lookup: %w", err)
	}
	if lease == nil {
		return 0, errors.New("extent MR cache returned nil lease")
	}
	// Bounds check: the entire read range must fit inside the
	// lease's published extent size. A range past the end signals
	// either (a) stale lease (extent grew, cache out of date) or
	// (b) caller bug. Either way, fall back.
	if uint64(extentOffset+size) > lease.Size {
		return 0, fmt.Errorf("read range [%d, %d) exceeds lease size %d",
			extentOffset, extentOffset+size, lease.Size)
	}

	// Acquire a conn for the addr without taking a slot — one-sided
	// reads use the QP but not the slot pool's 2-sided accounting.
	conn, err := rdmaConnPool.ConnForKey(rdmaAddr, "")
	if err != nil {
		return 0, fmt.Errorf("rdma conn for %s: %w", rdmaAddr, err)
	}

	chunks := splitReadChunks(extentOffset, size, util.ReadBlockSize)
	if len(chunks) == 0 {
		return 0, nil
	}

	// Single chunk: avoid goroutine + WaitGroup overhead.
	if len(chunks) == 1 {
		chk := chunks[0]
		return reader.readChunkViaRDMARead(conn, lease, req, chk)
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
			return 0, cerr
		}
	}

	total := 0
	for _, c := range chunks {
		total += c.bufSize
	}
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
