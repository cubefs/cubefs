//go:build linux && rdma

package rdma

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// RDMAMem is a pinned memory region registered with the NIC for DMA.
// Memory is allocated via C.malloc (not Go heap) so the GC cannot move it,
// which is required for RDMA: the DMA address must remain stable.
type RDMAMem struct {
	buf  unsafe.Pointer   // C.malloc allocation
	mr   *C.struct_ibv_mr // ibv_reg_mr result
	Lkey uint32           // local key (used by this process when posting WRs)
	Rkey uint32           // remote key (shared with peer; peer uses when RDMA-Writing here)
	VA   uint64           // virtual address = uintptr(buf); shared with peer
	Size int
}

// AllocRDMAMem allocates size bytes of pinned memory and registers it with pd.
func AllocRDMAMem(pd *C.struct_ibv_pd, size int) (*RDMAMem, error) {
	if size <= 0 {
		return nil, fmt.Errorf("rdma: AllocRDMAMem: invalid size %d", size)
	}
	buf := C.malloc(C.size_t(size))
	if buf == nil {
		return nil, fmt.Errorf("rdma: C.malloc(%d) failed", size)
	}
	C.memset(buf, 0, C.size_t(size))

	mr, err := regMR(pd, buf, size)
	if err != nil {
		C.free(buf)
		return nil, err
	}
	return &RDMAMem{
		buf:  buf,
		mr:   mr,
		Lkey: uint32(mr.lkey),
		Rkey: uint32(mr.rkey),
		VA:   uint64(uintptr(buf)),
		Size: size,
	}, nil
}

// RegisterRDMABuffer wraps an externally-allocated buffer (e.g. an
// mmap'd extent file) in a registered MR without taking ownership of
// the memory itself. The caller MUST keep the underlying buffer alive
// for the lifetime of the returned RDMAMem and call Free() to
// deregister; the buf pointer in the returned struct is left nil so
// (*RDMAMem).Free will NOT C.free it.
//
// onDemand=true requests ODP (IBV_ACCESS_ON_DEMAND) so the kernel
// can demand-page the region — essential for large file-backed
// regions where pinning the entire range would exceed memory budget.
// If ODP isn't supported by the HCA the call returns an error so
// the caller can retry with onDemand=false or fall back to a
// different strategy.
func RegisterRDMABuffer(pd *C.struct_ibv_pd, base uintptr, size int, onDemand bool) (*RDMAMem, error) {
	if size <= 0 {
		return nil, fmt.Errorf("rdma: RegisterRDMABuffer: invalid size %d", size)
	}
	if base == 0 {
		return nil, fmt.Errorf("rdma: RegisterRDMABuffer: nil base pointer")
	}
	mr, err := regMRWithODP(pd, unsafe.Pointer(base), size, onDemand)
	if err != nil {
		return nil, err
	}
	return &RDMAMem{
		buf:  nil, // caller owns the memory; Free will only dereg the MR
		mr:   mr,
		Lkey: uint32(mr.lkey),
		Rkey: uint32(mr.rkey),
		VA:   uint64(base),
		Size: size,
	}, nil
}

// Free deregisters the MR and frees the underlying C memory.
// Must be called when the connection is torn down.
func (m *RDMAMem) Free() {
	if m.mr != nil {
		deregMR(m.mr)
		m.mr = nil
	}
	if m.buf != nil {
		C.free(m.buf)
		m.buf = nil
	}
}

// Bytes returns a Go slice backed by the same C memory.
// The slice is valid as long as Free has not been called.
func (m *RDMAMem) Bytes() []byte {
	return unsafe.Slice((*byte)(m.buf), m.Size)
}

// SlotBytes returns the slice for slot index idx (size=slotSize bytes).
// Panics if idx is out of range.
func (m *RDMAMem) SlotBytes(idx, slotSize int) []byte {
	offset := idx * slotSize
	if offset+slotSize > m.Size {
		panic(fmt.Sprintf("rdma: SlotBytes idx=%d slotSize=%d out of range (total=%d)", idx, slotSize, m.Size))
	}
	return m.Bytes()[offset : offset+slotSize]
}
