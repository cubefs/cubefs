//go:build linux && rdma

package repl

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/rdma"
)

const followerRDMATimeout = 30 * time.Second

var followerRDMAPool *rdma.RDMAConnPool

// EnableFollowerRDMA initializes the RDMA slot pool for DataNode→DataNode
// replication and activates the RDMA send path.
func EnableFollowerRDMA(cfg rdma.RDMAPoolConfig) error {
	pool, err := rdma.NewRDMAConnPool(cfg)
	if err != nil {
		return fmt.Errorf("repl follower rdma: init pool: %w", err)
	}
	followerRDMAPool = pool
	followerRDMASend = rdmaSendToFollower
	return nil
}

func rdmaSendToFollower(addr string, fp *FollowerPacket) error {
	handle, err := followerRDMAPool.AcquireSlot(addr)
	if err != nil {
		return fmt.Errorf("repl follower rdma: acquire slot to %s: %w", addr, err)
	}
	conn := handle.Conn
	slot := handle.SlotIdx
	forceClose := false
	defer func() { followerRDMAPool.ReleaseSlot(handle, forceClose) }()

	lastDoneSeq := conn.RecvDoneSeq(slot)

	if err = conn.WritePacket(slot, &fp.Packet); err != nil {
		forceClose = true
		return fmt.Errorf("repl follower rdma: WritePacket: %w", err)
	}

	resp, err := pollFollowerRDMAResponse(conn, slot, lastDoneSeq)
	if err != nil {
		forceClose = true
		return err
	}

	if cerr := conn.ReturnCredit(slot); cerr != nil {
		forceClose = true
		return fmt.Errorf("repl follower rdma: ReturnCredit: %w", cerr)
	}

	if resp.ReqID != fp.ReqID {
		forceClose = true
		return fmt.Errorf("repl follower rdma: ReqID mismatch: got %d want %d", resp.ReqID, fp.ReqID)
	}
	if resp.ResultCode != proto.OpOk {
		return fmt.Errorf("repl follower rdma: follower ResultCode=%d", resp.ResultCode)
	}
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
