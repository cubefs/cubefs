//go:build linux && rdma

package stream

import (
	"fmt"
	"runtime"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/rdma"
)

const rdmaRoundTripTimeout = 30 * time.Second

// rdmaConnPool is nil by default; non-nil only when RDMA is enabled at startup.
var rdmaConnPool *rdma.RDMAConnPool

// InitRDMAConnPool initializes the client-side RDMA connection pool.
// Must be called before the first write if RDMA is desired.
func InitRDMAConnPool(numSlots, slotSize int) error {
	pool, err := rdma.NewRDMAConnPool(rdma.RDMAPoolConfig{
		NumSlots: numSlots,
		SlotSize: slotSize,
	})
	if err != nil {
		return fmt.Errorf("rdma client: init pool: %w", err)
	}
	rdmaConnPool = pool
	return nil
}

// sendPacketViaRDMA sends req to addr via RDMA and updates req with the server
// response. slot 0 is used exclusively per connection (pool gives exclusive access).
func sendPacketViaRDMA(addr string, req *Packet) error {
	conn, err := rdmaConnPool.GetConnect(addr)
	if err != nil {
		return fmt.Errorf("rdma client: get conn to %s: %w", addr, err)
	}
	forceClose := false
	defer func() { rdmaConnPool.PutConnect(conn, forceClose) }()

	if err = conn.WritePacket(0, &req.Packet); err != nil {
		forceClose = true
		return fmt.Errorf("rdma client: WritePacket: %w", err)
	}

	resp, err := pollRDMAResponse(conn, 0)
	if err != nil {
		forceClose = true
		return err
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

func pollRDMAResponse(conn *rdma.RDMAConn, slot int) (*proto.Packet, error) {
	lastSeq := conn.RecvSeq(slot)
	deadline := time.Now().Add(rdmaRoundTripTimeout)
	for {
		seq, ok := conn.PollRecvDoorbell(slot, lastSeq)
		if ok {
			conn.SetRecvSeq(slot, seq)
			return rdma.DeserializePacket(conn.RecvSlotBytes(slot))
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("rdma client: response timeout slot=%d", slot)
		}
		runtime.Gosched()
	}
}
