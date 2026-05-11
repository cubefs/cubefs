//go:build linux && rdma

package stream

import (
	"context"
	"fmt"
	"hash/crc32"
	"runtime"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/rdma"
)

const rdmaRoundTripTimeout = 30 * time.Second

// rdmaConnPool is nil by default; non-nil only when RDMA is enabled at startup.
var (
	rdmaConnPool      *rdma.RDMAConnPool
	rdmaConnPortShift int
)

// InitRDMAConnPool initializes the client-side RDMA slot pool.
// Must be called before the first write if RDMA is desired.
func InitRDMAConnPool(cfg rdma.RDMAPoolConfig) error {
	pool, err := rdma.NewRDMAConnPool(cfg)
	if err != nil {
		return fmt.Errorf("rdma client: init pool: %w", err)
	}
	rdmaConnPool = pool
	rdmaConnPortShift = cfg.RDMAPortShift
	return nil
}

// rdmaRoundTrip is the inner one-slot send + wait + return-credit cycle
// shared by both write and read RDMA paths. On success it returns the
// deserialised response packet; the caller is responsible for
// interpreting ResultCode and (for reads) verifying CRC.
func rdmaRoundTrip(addr string, req *Packet) (*proto.Packet, error) {
	// Caller passes the datanode's data (TCP) address; shift to its
	// RDMA listen port. Pool key remains the post-shift address.
	rdmaAddr := addr
	if rdmaConnPortShift != 0 {
		rdmaAddr = util.ShiftAddrPort(addr, rdmaConnPortShift)
	}
	// Hash-route by (PartitionID, ExtentID) so successive writes /
	// reads on the same extent share a QP and arrive at the server in
	// post order. Without this, append-write packets for one extent
	// fan out across multiple conns and trip the offset-mismatch check
	// in datanode/storage/extent.go.
	key := fmt.Sprintf("%d-%d", req.PartitionID, req.ExtentID)
	handle, err := rdmaConnPool.AcquireSlotForKey(rdmaAddr, key)
	if err != nil {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "acquire_slot")
		return nil, fmt.Errorf("rdma client: acquire slot to %s: %w", rdmaAddr, err)
	}
	conn := handle.Conn
	slot := handle.SlotIdx
	forceClose := false
	defer func() { rdmaConnPool.ReleaseSlot(handle, forceClose) }()

	lastDoneSeq := conn.RecvDoneSeq(slot)

	// Mirror packet.go:writeToConn — TCP recomputes CRC right before
	// sending so wire bytes and header CRC are guaranteed consistent.
	// The RDMA path used to skip this; for fresh write packets built
	// by ExtentHandler the CRC field is zero, which made DataNode's
	// checkCrc (wrap_prepare.go) reject every OpWrite / OpSyncWrite
	// with "packet Crc is incorrect". Only writes need a fresh CRC;
	// for reads Size is 0 and Data is nil.
	if req.Data != nil && len(req.Data) >= int(req.Size) && req.Size > 0 {
		req.CRC = crc32.ChecksumIEEE(req.Data[:req.Size])
	}

	if err = conn.WritePacket(slot, &req.Packet); err != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "write_packet")
		return nil, fmt.Errorf("rdma client: WritePacket: %w", err)
	}

	resp, err := pollRDMAResponse(conn, slot, lastDoneSeq)
	if err != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "poll_response")
		return nil, err
	}

	if cerr := conn.ReturnCredit(slot); cerr != nil {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "return_credit")
		return nil, fmt.Errorf("rdma client: ReturnCredit: %w", cerr)
	}

	if resp.ReqID != req.ReqID {
		forceClose = true
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "reqid_mismatch")
		return nil, fmt.Errorf("rdma client: ReqID mismatch: got %d want %d", resp.ReqID, req.ReqID)
	}

	return resp, nil
}

// sendPacketViaRDMA issues a write-side request over RDMA and updates req
// with the server's ResultCode. Reserved for non-read opcodes; the read
// path uses recvPacketViaRDMA which extracts Data + verifies CRC.
func sendPacketViaRDMA(addr string, req *Packet) error {
	if req.IsReadOperation() {
		// Defensive: reads must go through recvPacketViaRDMA. If we
		// reached this path with a read opcode, the SDK call site is
		// wired wrong — fail noisily so the bug is caught in tests.
		return fmt.Errorf("rdma client: sendPacketViaRDMA invoked with read opcode 0x%x", req.Opcode)
	}

	start := time.Now()
	resp, err := rdmaRoundTrip(addr, req)
	if err != nil {
		return err
	}
	req.ResultCode = resp.ResultCode
	rdma.MetricsObserveRequest(rdma.RoleClient, addr, time.Since(start))
	if resp.ResultCode != proto.OpOk {
		return fmt.Errorf("rdma client: server ResultCode=%d", resp.ResultCode)
	}
	return nil
}

// recvPacketViaRDMA issues a read-side request over RDMA and returns the
// full response packet (including the response Data). Verifies CRC of the
// response data; on mismatch returns an error and increments the
// fallback counter so the caller falls back to TCP.
//
// On OpAgain the server is asking the SDK to retry differently
// (typically because the response would not fit in one slot); the caller
// treats this as a transport failure and falls back to TCP for that chunk.
func recvPacketViaRDMA(addr string, req *Packet) (*proto.Packet, error) {
	if !req.IsReadOperation() {
		return nil, fmt.Errorf("rdma client: recvPacketViaRDMA invoked with non-read opcode 0x%x", req.Opcode)
	}
	start := time.Now()
	resp, err := rdmaRoundTrip(addr, req)
	if err != nil {
		return nil, err
	}
	if resp.ResultCode == proto.OpAgain {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "op_again")
		return nil, fmt.Errorf("rdma client: server returned OpAgain (slot capacity exceeded)")
	}
	if resp.ResultCode != proto.OpOk {
		// Server-side error (e.g. extent missing). Not a transport
		// failure; record the request and let the caller decide.
		rdma.MetricsObserveRequest(rdma.RoleClient, addr, time.Since(start))
		return resp, fmt.Errorf("rdma client: server ResultCode=%d", resp.ResultCode)
	}
	if int(resp.Size) > len(resp.Data) {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "size_mismatch")
		return nil, fmt.Errorf("rdma client: resp.Size %d exceeds resp.Data len %d", resp.Size, len(resp.Data))
	}
	expectedCRC := crc32.ChecksumIEEE(resp.Data[:resp.Size])
	if resp.CRC != expectedCRC {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "crc_mismatch")
		return nil, fmt.Errorf("rdma client: CRC mismatch: got %x want %x", resp.CRC, expectedCRC)
	}
	rdma.MetricsObserveRequest(rdma.RoleClient, addr, time.Since(start))
	return resp, nil
}

// rdmaTryForSize is the SDK-side gate for the RDMA write path: returns
// false when the pool is uninitialised, when req.Size is below the
// configured rdma_min_payload_bytes threshold (P6 small-packet skip),
// or when req.Size exceeds the largest payload that fits in one slot
// (oversized fall-back to TCP). On every fall-back it records
// cubefs_rdma_fallback_total with a reason label so operators can see
// which gate fired.
func rdmaTryForSize(addr string, size int) bool {
	if rdmaConnPool == nil {
		return false
	}
	minPayload := rdmaConnPool.MinPayloadBytes()
	if minPayload > 0 && size < minPayload {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "small_payload")
		return false
	}
	maxPayload := rdmaConnPool.MaxPayloadBytes()
	if maxPayload > 0 && size > maxPayload {
		rdma.MetricsObserveFallback(rdma.RoleClient, addr, "large_payload")
		return false
	}
	return true
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
