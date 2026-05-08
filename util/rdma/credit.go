// Pure-Go credit accounting for RDMA flow control.
//
// This file is build-tag-free so it can be compiled and unit-tested on any
// platform. The CGO/RDMA-tagged files in this package wire creditState up to
// real RDMA Writes; tests can drive it with plain memory.
//
// Protocol (P0 of docs/plan/rdma-optimization-spec.md):
//
//   - At handshake, both peers exchange the rkey/VA of an 8-byte pinned MR
//     used as the credit-return cell. Initial credits = numSlots.
//   - Sender side: increment sentCount before each WritePacket / WriteData.
//     If sentCount reaches received + numSlots, block until received advances.
//   - Receiver side: after processing each slot, increment processedCount and
//     RDMA-Write the new value into the peer's credit-return cell.
//
// `received` is the local view of "how many slots the peer has acknowledged
// as processed". For real connections it points into a pinned MR that the
// peer writes asynchronously; we observe it via atomic.LoadUint64. For tests
// it is a regular Go uint64 that the test mutates directly.

package rdma

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
)

// ErrCreditClosed is returned by acquireCredit when the connection is shut
// down while a goroutine is waiting for credits.
var ErrCreditClosed = errors.New("rdma: credit state closed")

// creditState is the per-connection flow-control bookkeeping. The same
// struct represents both sender and receiver roles because an RDMAConn is
// bidirectional: each end sends requests and acknowledges incoming work.
type creditState struct {
	numSlots uint64

	// sentCount: number of slot-writes we have sent. Monotonic.
	sentCount uint64

	// received: pointer to an 8-byte cell that the peer increments to
	// acknowledge processed slots. In real use it lives in pinned RDMA
	// memory; in tests it lives on the Go heap. Read atomically.
	received *uint64

	// processedCount: number of slots WE have processed locally. The value
	// we RDMA-Write back to the peer after each handler completes.
	processedCount uint64

	// closed flips to 1 when the connection is shutting down. acquireCredit
	// returns ErrCreditClosed instead of spinning forever.
	closed int32
}

// newCreditState constructs a creditState with the given slot count. The
// credit-return cell is allocated by the caller (real conn allocates a pinned
// MR; tests allocate &uint64{}).
func newCreditState(numSlots int, received *uint64) *creditState {
	if numSlots <= 0 {
		// Defensive: a zero-credit state would block all sends forever.
		// Validation upstream prevents this; treat as 1 to fail-safe.
		numSlots = 1
	}
	if received == nil {
		received = new(uint64)
	}
	return &creditState{
		numSlots: uint64(numSlots),
		received: received,
	}
}

// available returns the current number of unused credits without claiming
// any. Used for diagnostics and tests; not on the hot path.
func (s *creditState) available() int64 {
	received := atomic.LoadUint64(s.received)
	sent := atomic.LoadUint64(&s.sentCount)
	// Both counters are monotonic; received <= sent + numSlots after the
	// peer has caught up at least once. Until then, clamp at numSlots.
	avail := int64(received) + int64(s.numSlots) - int64(sent)
	return avail
}

// acquireCredit blocks until a credit is available, then claims it. Returns
// ErrCreditClosed if closeCredits has been called or ctx.Err() if ctx is
// cancelled while waiting. Safe for concurrent callers.
func (s *creditState) acquireCredit(ctx context.Context) error {
	for {
		if atomic.LoadInt32(&s.closed) == 1 {
			return ErrCreditClosed
		}
		received := atomic.LoadUint64(s.received)
		sent := atomic.LoadUint64(&s.sentCount)
		if sent < received+s.numSlots {
			if atomic.CompareAndSwapUint64(&s.sentCount, sent, sent+1) {
				return nil
			}
			// Lost the race; retry without yielding, the contention is brief.
			continue
		}
		// No credit available — wait. Spin-yield is acceptable here because
		// credit exhaustion is rare in steady state (numSlots >= concurrency).
		// P2 will replace this with adaptive sleep.
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		runtime.Gosched()
	}
}

// onProcessSlot records that one slot has been processed locally and returns
// the new total (the value to RDMA-Write back to the peer's received cell).
// Caller is responsible for posting the actual RDMA Write.
func (s *creditState) onProcessSlot() uint64 {
	return atomic.AddUint64(&s.processedCount, 1)
}

// onPeerCreditUpdate is called by the test harness (or the credit-watcher
// goroutine in production) to indicate the peer has acknowledged more slots.
// The real path simply lets the NIC update the cell; this helper exists so
// tests can drive the state machine deterministically.
func (s *creditState) onPeerCreditUpdate(newReceived uint64) {
	for {
		old := atomic.LoadUint64(s.received)
		if newReceived <= old {
			return // monotonic; do not regress
		}
		if atomic.CompareAndSwapUint64(s.received, old, newReceived) {
			return
		}
	}
}

// closeCredits unblocks any goroutines parked in acquireCredit and prevents
// future sends from succeeding. Idempotent.
func (s *creditState) closeCredits() {
	atomic.StoreInt32(&s.closed, 1)
}

// stats returns a snapshot for logging / metrics. Not atomic across fields,
// but safe for individual reads.
func (s *creditState) stats() (sent, received, processed uint64) {
	return atomic.LoadUint64(&s.sentCount),
		atomic.LoadUint64(s.received),
		atomic.LoadUint64(&s.processedCount)
}
