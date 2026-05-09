//go:build linux && rdma

package stream

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/rdma"
)

const rdmaRoundTripTimeout = 30 * time.Second

// rdmaConnPool is nil by default; non-nil only when RDMA is enabled at startup.
var rdmaConnPool *rdma.RDMAConnPool

// InitRDMAConnPool initializes the client-side RDMA slot pool.
// Must be called before the first write if RDMA is desired.
func InitRDMAConnPool(cfg rdma.RDMAPoolConfig) error {
	pool, err := rdma.NewRDMAConnPool(cfg)
	if err != nil {
		return fmt.Errorf("rdma client: init pool: %w", err)
	}
	rdmaConnPool = pool
	return nil
}

// sendPacketViaRDMA borrows one slot to addr, writes the request fire-and-
// forget, awaits the response on the same slot, and returns the slot.
//
// Concurrent calls to sendPacketViaRDMA against the same addr now use
// distinct slots (P1) — the slot pool round-robins across pre-dialed conns
// and only blocks when every conn-slot pair is in use AND maxConns is
// reached.
func sendPacketViaRDMA(addr string, req *Packet) error {
	handle, err := rdmaConnPool.AcquireSlot(addr)
	if err != nil {
		return fmt.Errorf("rdma client: acquire slot to %s: %w", addr, err)
	}
	conn := handle.Conn
	slot := handle.SlotIdx
	forceClose := false
	defer func() { rdmaConnPool.ReleaseSlot(handle, forceClose) }()

	// Snapshot the recv signal before posting so a response that arrives
	// concurrently with our send isn't missed by the sleep-phase wait.
	lastDoneSeq := conn.RecvDoneSeq(slot)

	if err = conn.WritePacket(slot, &req.Packet); err != nil {
		forceClose = true
		return fmt.Errorf("rdma client: WritePacket: %w", err)
	}

	resp, err := pollRDMAResponse(conn, slot, lastDoneSeq)
	if err != nil {
		forceClose = true
		return err
	}

	// P0 flow control: the response slot is consumed; let the server
	// reuse it before we evaluate the response.
	if cerr := conn.ReturnCredit(slot); cerr != nil {
		forceClose = true
		return fmt.Errorf("rdma client: ReturnCredit: %w", cerr)
	}

	if resp.ReqID != req.ReqID {
		forceClose = true
		return fmt.Errorf("rdma client: ReqID mismatch: got %d want %d", resp.ReqID, req.ReqID)
	}
	req.ResultCode = resp.ResultCode
	if resp.ResultCode != proto.OpOk {
		return fmt.Errorf("rdma client: server ResultCode=%d", resp.ResultCode)
	}
	return nil
}

// pollRDMAResponse waits for the server's response on the given slot using
// adaptive polling: tight spin → Gosched → cond-block on the connection's
// per-slot recvDoneSeq counter. Bounded by rdmaRoundTripTimeout so a
// stalled server eventually fails the request.
func pollRDMAResponse(conn *rdma.RDMAConn, slot int, lastDoneSeq uint64) (*proto.Packet, error) {
	lastSeq := conn.RecvSeq(slot)
	deadline := time.Now().Add(rdmaRoundTripTimeout)
	poller := rdma.NewAdaptivePoller(conn.PollConfig())
	for {
		seq, ok := conn.PollRecvDoorbell(slot, lastSeq)
		if ok {
			conn.SetRecvSeq(slot, seq)
			return rdma.DeserializePacket(conn.RecvSlotBytes(slot))
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("rdma client: response timeout slot=%d", slot)
		}
		switch poller.NextAction() {
		case rdma.ActionContinue:
			// tight loop
		case rdma.ActionYield:
			runtime.Gosched()
		case rdma.ActionSleep:
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			err := conn.WaitRecvDoneSeq(ctx, slot, lastDoneSeq)
			cancel()
			if err != nil && err != context.DeadlineExceeded {
				return nil, fmt.Errorf("rdma client: WaitRecvDoneSeq: %w", err)
			}
			poller.Reset()
		}
	}
}
