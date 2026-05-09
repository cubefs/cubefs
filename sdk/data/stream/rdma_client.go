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
// Records P3 metrics on every code path:
//   - cubefs_rdma_requests_total + cubefs_rdma_latency_seconds on success
//   - cubefs_rdma_fallback_total with a reason label on every error path
//     (the higher layer's fallback to TCP is covered by these increments)
func sendPacketViaRDMA(addr string, req *Packet) error {
	start := time.Now()
	handle, err := rdmaConnPool.AcquireSlot(addr)
	if err != nil {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "acquire_slot")
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
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "write_packet")
		return fmt.Errorf("rdma client: WritePacket: %w", err)
	}

	resp, err := pollRDMAResponse(conn, slot, lastDoneSeq)
	if err != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "poll_response")
		return err
	}

	// P0 flow control: the response slot is consumed; let the server
	// reuse it before we evaluate the response.
	if cerr := conn.ReturnCredit(slot); cerr != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "return_credit")
		return fmt.Errorf("rdma client: ReturnCredit: %w", cerr)
	}

	if resp.ReqID != req.ReqID {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "reqid_mismatch")
		return fmt.Errorf("rdma client: ReqID mismatch: got %d want %d", resp.ReqID, req.ReqID)
	}
	req.ResultCode = resp.ResultCode
	if resp.ResultCode != proto.OpOk {
		// Server returned an error code — not a transport failure but
		// counted as a non-fallback "request" that completed.
		rdma.MetricsObserveRequest(rdma.RoleClient, addr, time.Since(start))
		return fmt.Errorf("rdma client: server ResultCode=%d", resp.ResultCode)
	}
	rdma.MetricsObserveRequest(rdma.RoleClient, addr, time.Since(start))
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
