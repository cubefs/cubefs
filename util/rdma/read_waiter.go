//go:build linux && rdma

package rdma

/*
#include <infiniband/verbs.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// One-sided RDMA Read support (Sprint A.5b).
//
// PostRDMAReadAndWait posts an RDMA_READ WR against a peer-registered
// MR (rkey + remote VA, both obtained via OpExtentMRLookup), waits
// for the signaled completion on the per-slot waiter channel, then
// memcpy's the payload from the conn's internal readScratch into the
// caller's destination buffer.
//
// Why a fixed scratch instead of registering the caller's buffer on
// demand: ibv_reg_mr is in the millisecond range — pinning per-read
// would wipe out the latency win of single-sided RDMA. The scratch
// is pre-registered once during lazy init. The one memcpy from
// scratch → caller's slice (≤ 128 KiB) at ~10 GB/s adds ~13 µs,
// negligible vs the ~10 µs RDMA round-trip we save.
//
// Concurrency: rdmaReadSlots independent waiter channels, acquired
// by atomic CAS on inUse. Exhaustion (all in use) returns
// ErrReadSlotsExhausted so callers can fall back to the two-sided
// path; we deliberately don't block, since pinning the read budget
// behind one stuck waiter would defeat the parallelism goal.

const (
	// defaultReadSlots caps the number of in-flight RDMA Reads per
	// conn when RDMAConnConfig.ReadSlotCount is 0. 64 is the legacy
	// value; we keep it as the conservative default since it matches
	// the typical 256-slot two-sided pool's effective parallelism.
	// Callers expecting heavier Phase A traffic should set
	// cfg.ReadSlotCount explicitly.
	defaultReadSlots = 64
	// defaultReadSlotSize is the fallback per-slot scratch size.
	// 4 MiB is the post-Phase-A-prefault default: at 16 MiB typical
	// object size and readPrefetchDepth=4, a 4 MiB slot lets each
	// object complete in one prefetch batch (4 chunks × 4 MiB =
	// 16 MiB). Smaller slots multiply WR count and per-WR overhead;
	// larger slots waste scratch memory without unlocking more
	// concurrency under the typical 4-way prefetch.
	defaultReadSlotSize = 4 * 1024 * 1024
)

// readSlotConfig returns the (count, size) pair used by a conn's
// read scratch pool, falling back to defaults when the conn config
// fields are 0.
func readSlotConfig(cfg RDMAConnConfig) (int, int) {
	count := cfg.ReadSlotCount
	if count <= 0 {
		count = defaultReadSlots
	}
	size := cfg.ReadSlotSize
	if size <= 0 {
		size = defaultReadSlotSize
	}
	return count, size
}

// ErrReadSlotsExhausted indicates all per-conn read waiter slots
// are in use. Caller should fall back to the two-sided read path
// for this request — the next one will likely fit.
var ErrReadSlotsExhausted = errors.New("rdma: all RDMA Read waiter slots in use")

// ErrReadTimeout indicates the RDMA Read WR did not complete within
// the caller's deadline. Treat as a transport fault — the slot is
// still considered in use until the drainer eventually delivers
// (or QP teardown drains) the completion.
var ErrReadTimeout = errors.New("rdma: RDMA Read timed out")

// readCompletion mirrors CompletionEvent fields the waiter needs.
type readCompletion struct {
	ok     bool
	status int
}

// readWaiter is one entry in RDMAConn.readWaiters.
type readWaiter struct {
	// inUse is 0/1 accessed via atomic.CAS / Store (Go 1.17 compat).
	inUse  uint32
	doneCh chan readCompletion
}

// readScratchState bundles the lazy-init state for one-sided reads.
// Kept off the main RDMAConn struct so the conn header file stays
// readable; access is via the helper methods below.
type readScratchState struct {
	once    sync.Once
	initErr error
	scratch *RDMAMem
	waiters []readWaiter
}

// initReadScratch lazily allocates the scratch buffer + waiter
// channels on the first PostRDMAReadAndWait call. Subsequent calls
// observe the sync.Once and skip the work.
func (c *RDMAConn) initReadScratch() error {
	c.readScratchInitMu.Lock()
	if c.readScratchInited {
		c.readScratchInitMu.Unlock()
		return c.readScratchInitErr
	}
	if c.pd == nil {
		c.readScratchInitErr = errors.New("rdma: PostRDMAReadAndWait on conn with nil PD")
		c.readScratchInited = true
		c.readScratchInitMu.Unlock()
		return c.readScratchInitErr
	}
	mem, err := AllocRDMAMem(c.pd, c.readSlotCount*c.readSlotSize)
	if err != nil {
		c.readScratchInitErr = fmt.Errorf("rdma: alloc read scratch: %w", err)
		c.readScratchInited = true
		c.readScratchInitMu.Unlock()
		return c.readScratchInitErr
	}
	c.readScratch = mem
	c.readWaiters = make([]readWaiter, c.readSlotCount)
	for i := range c.readWaiters {
		// Buffered=1 so a CQE that arrives after the caller times
		// out lands in the channel without blocking the drainer.
		c.readWaiters[i].doneCh = make(chan readCompletion, 1)
	}
	c.readScratchInited = true
	c.readScratchInitMu.Unlock()
	return nil
}

// PostRDMAReadAndWait issues a one-sided RDMA Read against
// (remoteVA, rkey) and waits up to timeout for completion. On
// success the requested bytes are copied into dst. The slot is
// released back to the pool whether the call succeeded, failed, or
// timed out — a slot kept alive by a stuck WR is a permanent leak,
// preferable to letting the pool grow unboundedly under a sticky
// transport fault.
func (c *RDMAConn) PostRDMAReadAndWait(dst []byte, remoteVA uint64, rkey uint32, timeout time.Duration) error {
	if c == nil {
		return errors.New("rdma: PostRDMAReadAndWait on nil conn")
	}
	if c.IsClosed() {
		return errors.New("rdma: PostRDMAReadAndWait on closed conn")
	}
	n := len(dst)
	if n == 0 {
		return nil
	}
	if n > c.readSlotSize {
		return fmt.Errorf("rdma: read size %d exceeds slot size %d", n, c.readSlotSize)
	}
	if err := c.initReadScratch(); err != nil {
		return err
	}

	// Acquire a free waiter slot. Linear scan is fine — rdmaReadSlots
	// is small and the conn-local locality means a hot path usually
	// finds one in the first few iterations.
	slot := -1
	for i := range c.readWaiters {
		if atomic.CompareAndSwapUint32(&c.readWaiters[i].inUse, 0, 1) {
			slot = i
			break
		}
	}
	if slot < 0 {
		return ErrReadSlotsExhausted
	}
	defer atomic.StoreUint32(&c.readWaiters[slot].inUse, 0)

	// Drain any stale completion that landed after a prior caller's
	// timeout. The buffered channel can hold exactly one entry per
	// waiter; clearing it here keeps the post-vs-wait sequence
	// well-defined.
	select {
	case <-c.readWaiters[slot].doneCh:
	default:
	}

	laddr := c.readScratch.VA + uint64(slot*c.readSlotSize)
	qp := getQPFromCMID(c.cmID)
	wrID := encodeWRID(opRDMARead, slot)
	if err := postRDMARead(qp, laddr, c.readScratch.Lkey, uint32(n),
		remoteVA, rkey, wrID, true); err != nil {
		return fmt.Errorf("rdma: postRDMARead: %w", err)
	}

	select {
	case comp := <-c.readWaiters[slot].doneCh:
		if !comp.ok {
			return fmt.Errorf("rdma: RDMA Read WC status %d", comp.status)
		}
	case <-time.After(timeout):
		return ErrReadTimeout
	}

	// Copy the freshly-arrived data from scratch slot into caller's
	// buffer. dst sliced over caller's memory; safe because we now
	// hold the only producer reference to this scratch slot until
	// the next CAS on inUse.
	copy(dst, c.readScratch.Bytes()[slot*c.readSlotSize:slot*c.readSlotSize+n])
	return nil
}

// freeReadScratch releases the scratch MR. Called from conn.Close
// after the drainer has exited; safe to call even when init never
// ran.
func (c *RDMAConn) freeReadScratch() {
	c.readScratchInitMu.Lock()
	mem := c.readScratch
	c.readScratch = nil
	c.readWaiters = nil
	c.readScratchInitMu.Unlock()
	if mem != nil {
		mem.Free()
	}
}

// completeRDMARead is the drainer-side hook called from
// dispatchCompletion when a CQE's WRID decodes to opRDMARead. The
// non-blocking send + channel buffer of 1 ensure the drainer is
// never stalled by a timed-out waiter.
func (c *RDMAConn) completeRDMARead(slot int, ev CompletionEvent) {
	if slot < 0 || slot >= len(c.readWaiters) {
		return
	}
	select {
	case c.readWaiters[slot].doneCh <- readCompletion{ok: ev.Success(), status: ev.Status}:
	default:
		// Waiter already drained or never present; the buffered slot
		// is full from a prior unconsumed completion. Either way,
		// dropping is safe — the caller has already moved on.
	}
}
