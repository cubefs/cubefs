//go:build linux && rdma

package repl

import (
	"fmt"
	"runtime"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/rdma"
)

const followerRDMATimeout = 30 * time.Second

var followerRDMAPool *rdma.RDMAConnPool

// EnableFollowerRDMA initializes the RDMA connection pool for DataNode→DataNode
// replication and activates the RDMA send path.
func EnableFollowerRDMA(numSlots, slotSize int) error {
	pool, err := rdma.NewRDMAConnPool(rdma.RDMAPoolConfig{
		NumSlots: numSlots,
		SlotSize: slotSize,
	})
	if err != nil {
		return fmt.Errorf("repl follower rdma: init pool: %w", err)
	}
	followerRDMAPool = pool
	followerRDMASend = rdmaSendToFollower
	return nil
}

func rdmaSendToFollower(addr string, fp *FollowerPacket) error {
	conn, err := followerRDMAPool.GetConnect(addr)
	if err != nil {
		return fmt.Errorf("repl follower rdma: get conn to %s: %w", addr, err)
	}
	forceClose := false
	defer func() { followerRDMAPool.PutConnect(conn, forceClose) }()

	if err = conn.WritePacket(0, &fp.Packet); err != nil {
		forceClose = true
		return fmt.Errorf("repl follower rdma: WritePacket: %w", err)
	}

	resp, err := pollFollowerRDMAResponse(conn, 0)
	if err != nil {
		forceClose = true
		return err
	}

	// P0 flow control: ack the response slot so the follower's send credits
	// refill. Without this, repeated leader→follower replication on a
	// pooled conn would drain follower credits within numSlots round-trips.
	if cerr := conn.ReturnCredit(); cerr != nil {
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

func pollFollowerRDMAResponse(conn *rdma.RDMAConn, slot int) (*proto.Packet, error) {
	lastSeq := conn.RecvSeq(slot)
	deadline := time.Now().Add(followerRDMATimeout)
	for {
		seq, ok := conn.PollRecvDoorbell(slot, lastSeq)
		if ok {
			conn.SetRecvSeq(slot, seq)
			return rdma.DeserializePacket(conn.RecvSlotBytes(slot))
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("repl follower rdma: response timeout slot=%d", slot)
		}
		runtime.Gosched()
	}
}
