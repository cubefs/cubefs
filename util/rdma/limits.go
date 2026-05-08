// Package-level RDMA configuration limits.
// This file is build-tag-free so callers and tests on any platform can reference
// the validation constants and helpers without depending on the CGO RDMA stack.

package rdma

import (
	"fmt"

	"github.com/cubefs/cubefs/util"
)

// Internal constants. These shadow values defined in slot.go (which is gated by
// `linux && rdma`); they are kept in sync by mutual reference and the test in
// slot_test.go cross-checks them.
const (
	// slotHeaderBytes mirrors SlotHeaderSize in slot.go.
	slotHeaderBytes = 16
	// maxPacketHeaderBytes is the largest possible serialized CubeFS packet
	// header (base + VerSeq + ProtoVersion). Mirrors MaxPacketHeaderSize in
	// slot.go.
	maxPacketHeaderBytes = util.PacketHeaderSize + util.PacketVerSeqFiledLen + util.PacketProtoVerFiledLen
)

// MinValidSlotSize is the smallest SlotSize that can carry a CubeFS packet
// with a full BlockSize (128 KB) data payload. SlotSize below this value
// causes silent truncation of large packets and must be rejected at startup.
//
//	layout = SlotHeader (16) + max PacketHeader (69) + BlockSize (128 KB)
const MinValidSlotSize = slotHeaderBytes + maxPacketHeaderBytes + util.BlockSize

// DefaultSlotSize is the recommended default SlotSize. Sized comfortably above
// MinValidSlotSize while staying close to a single block, so memory usage per
// connection scales linearly with NumSlots without large overhead.
const DefaultSlotSize = util.BlockSize + util.PageSize // 132 KB

// ValidateSlotSize reports whether the given size meets the minimum required
// to carry full-sized CubeFS packets. The error message is intentionally
// detailed so operators can adjust their config without reading source.
func ValidateSlotSize(size int) error {
	if size < MinValidSlotSize {
		return fmt.Errorf("rdma: SlotSize=%d too small; need >= %d "+
			"(SlotHeader=%d + MaxPacketHeader=%d + BlockSize=%d). "+
			"Increase rdmaSlotSize in config or accept TCP fallback",
			size, MinValidSlotSize, slotHeaderBytes, maxPacketHeaderBytes, util.BlockSize)
	}
	return nil
}

// CreditAckMode controls how a credit-return RDMA Write is completed by the
// receiver. Affects throughput vs CQ pressure trade-off; see P0 of
// docs/plan/rdma-optimization-spec.md.
type CreditAckMode int

const (
	// CreditAckSync waits for the credit-return Write's CQE before processing
	// the next slot. Bounded CQ depth, simpler error semantics. Default.
	CreditAckSync CreditAckMode = iota
	// CreditAckAsync posts the credit-return Write without waiting for its
	// CQE. Higher throughput; the sender's stale read of the credit counter
	// is bounded by NIC ordering guarantees on the same QP.
	CreditAckAsync
)

// String implements fmt.Stringer for diagnostics.
func (m CreditAckMode) String() string {
	switch m {
	case CreditAckSync:
		return "sync"
	case CreditAckAsync:
		return "async"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}
