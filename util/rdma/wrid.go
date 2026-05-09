// Work-Request ID encoding for the RDMA transport.
//
// All WRs posted on a connection's send / recv queues use a 64-bit ID that
// encodes BOTH the operation type and the slot index it relates to. This is
// the only way the per-conn drainer goroutine can route a CQE back to the
// right per-slot waiter without consulting an extra lookup table.
//
//	bit layout: [op:32][slot:32]
//
// The op codes never collide with the recvWRIDTag used pre-P1; the drainer
// rejects any WR ID that decodes to an unknown op as a routing bug.
//
// This file is build-tag-free so encoder/decoder helpers and tests run on
// any platform (the actual posting lives in conn.go and recv_pool.go).

package rdma

// wrOp identifies what category of work a WR belongs to. Used by the
// drainer to dispatch each CQE to the right consumer.
type wrOp uint32

const (
	// opUnknown is the zero value; bare integers cast to wrOp default to
	// "unknown" so a forgotten encode call surfaces immediately.
	opUnknown wrOp = iota
	// opSlot — the data payload write portion of WritePacket / WriteData.
	// Posted not-signaled, so we should NEVER see one of these in a CQE
	// unless the QP is being torn down (flush completion). Logged at warn.
	opSlot
	// opDoorbell — the WRITE_WITH_IMM doorbell that follows the slot
	// payload. Signaled; one CQE per slot send. The drainer increments
	// sendDoneSeq[slot] when seen.
	opDoorbell
	// opCredit — RDMA Write of the per-slot processed-count back to the
	// peer's credit cell. Signaled. The drainer increments creditDoneSeq.
	opCredit
	// opRecv — pre-posted recv WR consumed by an incoming WITH_IMM
	// doorbell. Signaled implicitly. On completion the drainer refills
	// the pool and signals the recv waiters.
	opRecv
	// opShutdownPing — a self-signaled WR posted by Close() to wake the
	// drainer goroutine if it is blocked on the comp_channel. Carries no
	// real semantics; the drainer just observes it and exits.
	opShutdownPing
)

// String for diagnostics.
func (o wrOp) String() string {
	switch o {
	case opSlot:
		return "slot"
	case opDoorbell:
		return "doorbell"
	case opCredit:
		return "credit"
	case opRecv:
		return "recv"
	case opShutdownPing:
		return "shutdown"
	default:
		return "unknown"
	}
}

// encodeWRID packs (op, slot) into a 64-bit WR ID. slot must fit in 32 bits;
// the rdma transport caps numSlots at 1024 so the slot field is far from
// overflowing.
func encodeWRID(op wrOp, slot int) uint64 {
	return uint64(op)<<32 | uint64(uint32(slot))
}

// decodeWRID returns the (op, slot) pair encoded by encodeWRID. Drainers
// MUST handle decodeWRID returning opUnknown by logging without panicking,
// since flush-on-teardown CQEs may carry stale or bogus IDs depending on
// the verbs implementation.
func decodeWRID(wrID uint64) (wrOp, int) {
	op := wrOp(wrID >> 32)
	slot := int(uint32(wrID))
	return op, slot
}
