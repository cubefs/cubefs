//go:build linux && rdma

package rdma

/*
#include "rdma.h"
*/
import "C"

import (
	"fmt"
	"sync/atomic"
)

// recvDummyEntryBytes is the per-slot recv-SGE buffer size. The buffer is
// only required to satisfy the verbs API: WRITE_WITH_IMM does not deliver
// payload bytes via the recv path, only the imm_data field of the CQE.
// 16 bytes is small enough not to matter and large enough to satisfy any
// recv path that does deliver payload (we never use that path here).
const recvDummyEntryBytes = 16

// recvPool maintains a fixed-size queue of pre-posted recv WRs. Each WR
// references one slot of recvDummyMR; on completion we re-post the same
// slot so the queue stays full. A connection cannot accept incoming
// WRITE_WITH_IMM doorbells if the recv pool is empty — which would put
// the QP in error state — so refilling on every CQE is essential.
type recvPool struct {
	mr   *RDMAMem
	size int

	// dispatched counts WRs handed to the NIC that have not yet been
	// re-posted after their CQE landed. Used purely for diagnostics; the
	// actual refill happens immediately on CQE.
	dispatched int64
}

// newRecvPool allocates an MR holding numSlots × recvDummyEntryBytes and
// pre-posts numSlots recv WRs into qp. Caller transfers ownership of the
// MR to the returned pool; pool.free releases everything.
func newRecvPool(pd *C.struct_ibv_pd, qp *C.struct_ibv_qp, numSlots int) (*recvPool, error) {
	mr, err := AllocRDMAMem(pd, numSlots*recvDummyEntryBytes)
	if err != nil {
		return nil, fmt.Errorf("rdma: recvPool MR: %w", err)
	}
	p := &recvPool{mr: mr, size: numSlots}
	for i := 0; i < numSlots; i++ {
		if err := p.postOne(qp, i); err != nil {
			mr.Free()
			return nil, fmt.Errorf("rdma: recvPool prepost slot %d: %w", i, err)
		}
	}
	return p, nil
}

// postOne enqueues one recv WR pointing at the dummy buffer for slot idx.
// The WR ID is the OpRecv-tagged encoding of idx so completion routing in
// the drainer can dispatch it via decodeWRID.
func (p *recvPool) postOne(qp *C.struct_ibv_qp, idx int) error {
	if idx < 0 || idx >= p.size {
		return fmt.Errorf("rdma: recvPool: slot %d out of range [0,%d)", idx, p.size)
	}
	addr := p.mr.VA + uint64(idx*recvDummyEntryBytes)
	wrID := encodeWRID(opRecv, idx)
	if err := postRecv(qp, addr, p.mr.Lkey, recvDummyEntryBytes, wrID); err != nil {
		return err
	}
	atomic.AddInt64(&p.dispatched, 1)
	return nil
}

// refillOne is called from the completion handler with the WR ID of a
// completed recv WR. It re-posts the same slot so the pool stays full.
//
// Returns an error if the WR ID does not look like a recv WR ID, which
// would indicate completion-handler routing bugs upstream.
func (p *recvPool) refillOne(qp *C.struct_ibv_qp, wrID uint64) error {
	op, idx := decodeWRID(wrID)
	if op != opRecv {
		return fmt.Errorf("rdma: recvPool refill: WR ID 0x%x is not a recv WR", wrID)
	}
	atomic.AddInt64(&p.dispatched, -1)
	return p.postOne(qp, idx)
}

// free releases the underlying MR. The associated recv WRs are cancelled
// implicitly when the QP is destroyed; we do not need to drain them.
func (p *recvPool) free() {
	if p == nil || p.mr == nil {
		return
	}
	p.mr.Free()
	p.mr = nil
}

// inFlight returns the number of recv WRs currently posted but not yet
// completed. Used by tests and diagnostics.
func (p *recvPool) inFlight() int64 {
	return atomic.LoadInt64(&p.dispatched)
}
