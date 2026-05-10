//go:build linux && rdma

package repl

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/rdma"
)

const followerRDMATimeout = 30 * time.Second

var (
	followerRDMAPool      *rdma.RDMAConnPool
	followerRDMAPortShift int
)

// EnableFollowerRDMA initializes the RDMA slot pool for DataNode→DataNode
// replication and activates the RDMA send path.
func EnableFollowerRDMA(cfg rdma.RDMAPoolConfig) error {
	pool, err := rdma.NewRDMAConnPool(cfg)
	if err != nil {
		return fmt.Errorf("repl follower rdma: init pool: %w", err)
	}
	followerRDMAPool = pool
	followerRDMAPortShift = cfg.RDMAPortShift
	followerRDMASend = rdmaSendToFollower
	max := pool.MaxPayloadBytes()
	followerRDMACanCarry = func(fp *FollowerPacket) bool {
		// Reject when data + ArgLen would not fit in a slot — caller
		// then falls back to TCP. max==0 means "size unset" → trust
		// the caller and let the slot serializer fail loudly instead
		// of silently routing everything to TCP.
		if max <= 0 {
			return true
		}
		return int(fp.Size)+int(fp.ArgLen) <= max
	}
	return nil
}

func rdmaSendToFollower(addr string, fp *FollowerPacket) error {
	start := time.Now()
	// Caller passes the follower's data (TCP) address; shift to the
	// follower's RDMA listen port before dialing. With shift=0 this is
	// a no-op and addr is used verbatim.
	rdmaAddr := addr
	if followerRDMAPortShift != 0 {
		rdmaAddr = util.ShiftAddrPort(addr, followerRDMAPortShift)
	}
	// Hash routing: route same (PartitionID, ExtentID) to the same conn
	// on every send. Combined with sequential per-conn dispatch on the
	// server (rdma_server.go pollLoop), this preserves the strict
	// offset ordering that storage/extent.go's append-write path
	// requires. Without this, OpWrite packets for one extent fan out
	// across multiple QPs, arrive out of order, and trip the
	// "extent current size != Offset" check, returning OpTryOtherExtent
	// and forcing repeated retries.
	key := fmt.Sprintf("%d-%d", fp.PartitionID, fp.ExtentID)
	handle, err := followerRDMAPool.AcquireSlotForKey(rdmaAddr, key)
	if err != nil {
		rdma.MetricsObserveFallback(rdma.RoleFollower, addr, "acquire_slot")
		return fmt.Errorf("repl follower rdma: acquire slot to %s: %w", rdmaAddr, err)
	}
	conn := handle.Conn
	slot := handle.SlotIdx
	forceClose := false
	defer func() { followerRDMAPool.ReleaseSlot(handle, forceClose) }()

	lastDoneSeq := conn.RecvDoneSeq(slot)

	if err = conn.WritePacket(slot, &fp.Packet); err != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleFollower, addr, "write_packet")
		return fmt.Errorf("repl follower rdma: WritePacket: %w", err)
	}

	resp, err := pollFollowerRDMAResponse(conn, slot, lastDoneSeq)
	if err != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleFollower, addr, "poll_response")
		return err
	}

	if cerr := conn.ReturnCredit(slot); cerr != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleFollower, addr, "return_credit")
		return fmt.Errorf("repl follower rdma: ReturnCredit: %w", cerr)
	}

	if resp.ReqID != fp.ReqID {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleFollower, addr, "reqid_mismatch")
		return fmt.Errorf("repl follower rdma: ReqID mismatch: got %d want %d", resp.ReqID, fp.ReqID)
	}
	if resp.ResultCode != proto.OpOk {
		rdma.MetricsObserveRequest(rdma.RoleFollower, addr, time.Since(start))
		return fmt.Errorf("repl follower rdma: follower ResultCode=%d", resp.ResultCode)
	}
	rdma.MetricsObserveRequest(rdma.RoleFollower, addr, time.Since(start))
	return nil
}

// pollFollowerRDMAResponse uses adaptive polling with cond-block sleep on
// the connection's per-slot recvDoneSeq counter (P1+P2). Bounded by
// followerRDMATimeout.
func pollFollowerRDMAResponse(conn *rdma.RDMAConn, slot int, lastDoneSeq uint64) (*proto.Packet, error) {
	lastSeq := conn.RecvSeq(slot)
	deadline := time.Now().Add(followerRDMATimeout)
	poller := rdma.NewAdaptivePoller(conn.PollConfig())
	for {
		seq, ok := conn.PollRecvDoorbell(slot, lastSeq)
		if ok {
			conn.SetRecvSeq(slot, seq)
			return rdma.DeserializePacket(conn.RecvSlotBytes(slot))
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("repl follower rdma: response timeout slot=%d", slot)
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
				return nil, fmt.Errorf("repl follower rdma: WaitRecvDoneSeq: %w", err)
			}
			poller.Reset()
		}
	}
}
