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
//
// The full RDMAPoolConfig is accepted so callers can control NumSlots,
// SlotSize, CreditAckMode, and Poll behaviour without further package-level
// setters. cfg.NumSlots and cfg.SlotSize must satisfy the validation in
// rdma.NewRDMAConnPool; otherwise this returns an error and the SDK falls
// back to TCP transparently.
func InitRDMAConnPool(cfg rdma.RDMAPoolConfig) error {
	pool, err := rdma.NewRDMAConnPool(cfg)
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

	// P0 flow control: the response slot is consumed; let the server reuse
	// it before we evaluate the response. Failing to do this leaks one
	// credit per round-trip on the server's send side.
	if cerr := conn.ReturnCredit(); cerr != nil {
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

// pollRDMAResponse waits for the server's response for the given slot using
// the connection's adaptive poll policy: tight spin → Gosched → comp_channel
// sleep. Bounded by rdmaRoundTripTimeout so a stalled server eventually fails
// the request rather than parking the goroutine forever.
func pollRDMAResponse(conn *rdma.RDMAConn, slot int) (*proto.Packet, error) {
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
			if err := conn.SleepWaitForRecv(); err != nil {
				return nil, fmt.Errorf("rdma client: SleepWaitForRecv: %w", err)
			}
			poller.Reset()
		}
	}
}
