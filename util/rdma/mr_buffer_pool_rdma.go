//go:build linux && rdma

package rdma

/*
#include <stdlib.h>
#include <infiniband/verbs.h>
*/
import "C"

import (
	"fmt"
	"time"
)

// NewMRBufferPoolForPD pre-allocates `count` pinned buffers of
// `bufSize` bytes each, registers every one with the given PD via
// regMR, and wraps the result in an MRBufferPool with the requested
// TTL. On any failure during allocation/registration all partially
// allocated buffers are freed before returning the error.
//
// The caller must hold a reference to pd for the lifetime of the
// pool; closing the pool does NOT deallocate pd.
//
// Memory footprint = count * bufSize, all pinned. With the typical
// Sprint-2.1 defaults (32 buffers * 1 MB = 32 MB per conn) this is
// well within the per-connection budget.
func NewMRBufferPoolForPD(pd *C.struct_ibv_pd, count, bufSize int, ttl time.Duration) (*MRBufferPool, error) {
	if count <= 0 || bufSize <= 0 {
		return nil, fmt.Errorf("rdma: NewMRBufferPoolForPD: invalid count=%d bufSize=%d", count, bufSize)
	}
	mems := make([]*RDMAMem, 0, count)
	cleanup := func() {
		for _, m := range mems {
			m.Free()
		}
	}
	buffers := make([]*MRBuffer, count)
	for i := 0; i < count; i++ {
		mem, err := AllocRDMAMem(pd, bufSize)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("rdma: NewMRBufferPoolForPD: alloc[%d]: %w", i, err)
		}
		mems = append(mems, mem)
		buffers[i] = &MRBuffer{
			Rkey: mem.Rkey,
			VA:   mem.VA,
			Size: bufSize,
			Data: mem.Bytes(),
		}
	}
	pool := NewMRBufferPool(buffers, ttl)
	pool.attachMems(mems)
	return pool, nil
}

// attachMems hooks the registered RDMAMems into the pool so Close
// can deregister them. Keeping the mems behind a method (rather than
// a struct field exposed everywhere) lets the build-tag-free pool
// stay agnostic about real-vs-mock memory.
func (p *MRBufferPool) attachMems(mems []*RDMAMem) {
	p.ownedMems = mems
}
