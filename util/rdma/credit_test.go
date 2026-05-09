package rdma

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newCreditStateForTest is a convenience that allocates a fresh received
// cell and wires it into a new creditState. PollConfig is left as zero
// (newCreditState fills in DefaultPollConfig) so tests exercise the same
// adaptive-backoff path production uses.
func newCreditStateForTest(numSlots int) (*creditState, *uint64) {
	r := new(uint64)
	return newCreditState(numSlots, r, PollConfig{}), r
}

func TestCreditState_BasicAcquireAndAvailable(t *testing.T) {
	s, _ := newCreditStateForTest(4)

	if got := s.available(); got != 4 {
		t.Fatalf("initial available: got %d want 4", got)
	}

	for i := 1; i <= 4; i++ {
		if err := s.acquireCredit(context.Background()); err != nil {
			t.Fatalf("acquire #%d: %v", i, err)
		}
		if got := s.available(); got != int64(4-i) {
			t.Errorf("after acquire #%d: available=%d want %d", i, got, 4-i)
		}
	}
}

// TestCreditState_BlocksWhenExhausted verifies the spec acceptance criterion:
// "发送端在 credit=0 时阻塞，不写 slot".
func TestCreditState_BlocksWhenExhausted(t *testing.T) {
	s, received := newCreditStateForTest(2)

	// Drain initial credits.
	for i := 0; i < 2; i++ {
		if err := s.acquireCredit(context.Background()); err != nil {
			t.Fatalf("initial acquire: %v", err)
		}
	}
	if got := s.available(); got != 0 {
		t.Fatalf("available after drain: got %d want 0", got)
	}

	// A further acquire must block, not return error.
	done := make(chan error, 1)
	go func() { done <- s.acquireCredit(context.Background()) }()

	select {
	case err := <-done:
		t.Fatalf("acquire did not block; returned %v", err)
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked.
	}

	// Simulate peer ack of one slot.
	atomic.StoreUint64(received, 1)

	// Sender must unblock and acquire successfully.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire after credit return: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after credit return")
	}
}

// TestCreditState_ContextCancellation ensures a stalled sender can be
// cancelled from outside.
func TestCreditState_ContextCancellation(t *testing.T) {
	s, _ := newCreditStateForTest(1)

	if err := s.acquireCredit(context.Background()); err != nil {
		t.Fatalf("initial acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.acquireCredit(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not respect ctx cancellation")
	}
}

// TestCreditState_CloseUnblocks ensures connection shutdown wakes any
// blocked senders so they can return without deadlock.
func TestCreditState_CloseUnblocks(t *testing.T) {
	s, _ := newCreditStateForTest(1)
	if err := s.acquireCredit(context.Background()); err != nil {
		t.Fatalf("initial: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.acquireCredit(context.Background()) }()

	time.Sleep(20 * time.Millisecond)
	s.closeCredits()

	select {
	case err := <-done:
		if err != ErrCreditClosed {
			t.Fatalf("got %v, want ErrCreditClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock acquire")
	}
}

// TestCreditState_OnProcessSlotMonotonic checks that repeated processing
// produces a strictly increasing processed count (the value RDMA-Written
// back to the sender's credit cell).
func TestCreditState_OnProcessSlotMonotonic(t *testing.T) {
	s, _ := newCreditStateForTest(8)
	prev := uint64(0)
	for i := 0; i < 10; i++ {
		got := s.onProcessSlot()
		if got != prev+1 {
			t.Fatalf("step %d: got %d want %d", i, got, prev+1)
		}
		prev = got
	}
}

// TestCreditState_OnPeerCreditUpdateMonotonic ensures stale credit-return
// values from out-of-order RDMA Writes do not regress the local view.
func TestCreditState_OnPeerCreditUpdateMonotonic(t *testing.T) {
	s, received := newCreditStateForTest(4)
	s.onPeerCreditUpdate(5)
	if v := atomic.LoadUint64(received); v != 5 {
		t.Fatalf("after first update: got %d want 5", v)
	}
	// Stale update is ignored.
	s.onPeerCreditUpdate(3)
	if v := atomic.LoadUint64(received); v != 5 {
		t.Fatalf("after stale update: got %d want 5", v)
	}
	// Newer update wins.
	s.onPeerCreditUpdate(7)
	if v := atomic.LoadUint64(received); v != 7 {
		t.Fatalf("after newer update: got %d want 7", v)
	}
}

// TestCreditState_FullSenderReceiverCycle simulates a sender ↔ receiver pair
// without any RDMA hardware. It exercises the spec's acceptance criteria:
//
//   - 发送端在 credit=0 时阻塞，不写 slot
//   - 接收端处理完后 credit 正确归还
//   - 构造 credit=0 场景，发送端不超时、不崩溃
//
// We do not need a transport mock for P0: the pure state machine plus a
// direct simulation of the peer's RDMA-Write into the sender's `received`
// cell is sufficient to verify the contract.
func TestCreditState_FullSenderReceiverCycle(t *testing.T) {
	const numSlots = 2

	// Sender keeps its own state; senderReceivedCell is the local pinned cell
	// that the receiver would RDMA-Write into.
	senderState, senderReceivedCell := newCreditStateForTest(numSlots)
	// Receiver keeps its own state purely for processedCount accounting.
	receiverState, _ := newCreditStateForTest(numSlots)

	// 1) Sender drains all credits (numSlots writes in flight).
	for i := 0; i < numSlots; i++ {
		if err := senderState.acquireCredit(context.Background()); err != nil {
			t.Fatalf("initial acquire #%d: %v", i, err)
		}
	}
	if got := senderState.available(); got != 0 {
		t.Fatalf("after draining: available=%d want 0", got)
	}

	// 2) A further send must block — it must not return an error and must not
	//    increment sentCount (no slot is written).
	sentBefore := senderState.sentCount
	blocked := make(chan error, 1)
	go func() { blocked <- senderState.acquireCredit(context.Background()) }()

	select {
	case err := <-blocked:
		t.Fatalf("send did not block; returned %v (sentCount went from %d to %d)",
			err, sentBefore, senderState.sentCount)
	case <-time.After(50 * time.Millisecond):
		// Confirmed blocked.
	}

	// 3) Receiver processes one slot and "RDMA-Writes" the new processedCount
	//    into the sender's received cell.
	newCount := receiverState.onProcessSlot()
	if newCount != 1 {
		t.Fatalf("receiver onProcessSlot=%d want 1", newCount)
	}
	atomic.StoreUint64(senderReceivedCell, newCount)

	// 4) Blocked sender must wake up and successfully claim the credit.
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("blocked sender returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked sender did not unblock after credit return")
	}

	// 5) Final invariant: sent <= received + numSlots (would have caused ring overrun if violated).
	sent, received, _ := senderState.stats()
	if sent > received+numSlots {
		t.Fatalf("ring-overrun invariant broken: sent=%d > received(%d)+numSlots(%d)",
			sent, received, numSlots)
	}
}

// TestCreditState_RingOverrunInvariantUnderLoad runs many concurrent senders
// against a receiver that processes at a bounded rate, and asserts the
// invariant (sent <= received + numSlots) holds at every successful claim.
//
// Without the credit gate this would trivially fail: senders would race past
// the ring size. With credit, every CAS in acquireCredit is gated on the
// invariant, so any successful return implies it held at that moment.
func TestCreditState_RingOverrunInvariantUnderLoad(t *testing.T) {
	const (
		numSlots   = 4
		numSenders = 16
		perSender  = 100
	)

	state, receivedCell := newCreditStateForTest(numSlots)

	// Receiver loop: process one credit every short tick, mimicking RDMA Write.
	stop := make(chan struct{})
	var receiverWG sync.WaitGroup
	receiverWG.Add(1)
	go func() {
		defer receiverWG.Done()
		ticker := time.NewTicker(20 * time.Microsecond)
		defer ticker.Stop()
		var processed uint64
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				processed++
				atomic.StoreUint64(receivedCell, processed)
			}
		}
	}()

	// Senders: each acquires perSender credits.
	var senderWG sync.WaitGroup
	senderWG.Add(numSenders)
	violation := make(chan string, 1)
	for i := 0; i < numSenders; i++ {
		go func(id int) {
			defer senderWG.Done()
			for j := 0; j < perSender; j++ {
				if err := state.acquireCredit(context.Background()); err != nil {
					select {
					case violation <- fmt.Sprintf("sender %d acquire err: %v", id, err):
					default:
					}
					return
				}
				// Re-read the invariant immediately after the CAS.
				sent := atomic.LoadUint64(&state.sentCount)
				recv := atomic.LoadUint64(receivedCell)
				if sent > recv+numSlots {
					select {
					case violation <- fmt.Sprintf("invariant broken: sent=%d recv=%d numSlots=%d", sent, recv, numSlots):
					default:
					}
					return
				}
			}
		}(i)
	}
	senderWG.Wait()
	close(stop)
	receiverWG.Wait()

	select {
	case msg := <-violation:
		t.Fatal(msg)
	default:
	}
}

// fmt is referenced by the load test's failure message above.
