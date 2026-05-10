//go:build linux && rdma

package repl

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/rdma"
)

const (
	followerRDMATimeout    = 30 * time.Second
	rdmaTransportSendQSize = 256
	rdmaTransportRecvQSize = 256
)

var (
	followerRDMAPool      *rdma.RDMAConnPool
	followerRDMAPortShift int

	// One transport per follower addr, mirroring TCP's FollowerTransport
	// (repl_protocol.go:78). Created lazily on first send to that addr.
	rdmaTransports   sync.Map // map[string]*rdmaFollowerTransport
	rdmaTransportsMu sync.Mutex
)

// rdmaFollowerTransport pipes leader→follower RDMA replication through
// a dedicated send/recv goroutine pair per peer. The split mirrors TCP's
// FollowerTransport.serverWriteToFollower / .serverReadFromFollower:
//
//   - sendLoop drains sendCh, AcquireSlotForKey, posts WritePacket
//     (just enqueues WRs into the QP send queue, returns immediately),
//     then hands off to recvLoop via inflight.
//   - recvLoop drains inflight, polls each handle for its response,
//     ReturnCredit, ReleaseSlot, signals fp.respCh.
//
// This restores cross-follower parallelism (different transports run
// concurrently) AND preserves per-follower send order (single sender
// goroutine per peer = single ibv_post_send caller per QP). The earlier
// sync fan-out in sendRequestToAllFollowers traded throughput for
// ordering; with this transport, both are had.
type rdmaFollowerTransport struct {
	addr     string // follower's data (TCP) address; shifted to RDMA port at dial time
	sendCh   chan *FollowerPacket
	inflight chan *rdmaInflight
	stopCh   chan struct{}
	wg       sync.WaitGroup
	closed   bool
	mu       sync.Mutex
}

// rdmaInflight tracks one round-trip between sendLoop and recvLoop.
type rdmaInflight struct {
	fp          *FollowerPacket
	handle      *rdma.SlotHandle
	start       time.Time
	lastDoneSeq uint64
}

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
		// the caller.
		if max <= 0 {
			return true
		}
		if int(fp.Size)+int(fp.ArgLen) <= max {
			return true
		}
		// Without this metric the canCarry-false path is invisible:
		// the dispatcher silently falls through to TCP and neither
		// rdma_request_total nor rdma_fallback_total moves, looking
		// like "no RDMA traffic".
		rdma.MetricsObserveFallback(rdma.RoleFollower, "", "large_payload")
		return false
	}
	return nil
}

// rdmaSendToFollower is now async. It enqueues fp on the per-(addr)
// transport's sendCh and returns immediately; the actual round-trip
// happens in the transport's send/recv goroutines, and the result is
// pushed to fp.respCh from there. Caller (sendRequestToAllFollowers)
// MUST NOT also write to fp.respCh — see the dispatcher in
// repl_protocol.go for the matching adjustment.
//
// Returning a non-nil error here means the dispatch itself failed
// (queue full, transport closed). In that case the caller is expected
// to push the error to fp.respCh on our behalf.
func rdmaSendToFollower(addr string, fp *FollowerPacket) error {
	t := getOrCreateRDMATransport(addr)
	if t == nil {
		return fmt.Errorf("repl follower rdma: transport for %s closed", addr)
	}
	select {
	case t.sendCh <- fp:
		return nil
	default:
		// sendCh full means too many in-flight requests — back-pressure
		// to the caller via TCP fallback (caller treats this as a
		// transport error and will retry/fallback).
		rdma.MetricsObserveFallback(rdma.RoleFollower, addr, "send_queue_full")
		return fmt.Errorf("repl follower rdma: sendCh full for %s", addr)
	}
}

// getOrCreateRDMATransport returns a long-lived transport for addr,
// dialing on first use. Concurrent callers are serialised so we never
// create two transports for the same addr.
func getOrCreateRDMATransport(addr string) *rdmaFollowerTransport {
	if v, ok := rdmaTransports.Load(addr); ok {
		t := v.(*rdmaFollowerTransport)
		t.mu.Lock()
		closed := t.closed
		t.mu.Unlock()
		if !closed {
			return t
		}
	}
	rdmaTransportsMu.Lock()
	defer rdmaTransportsMu.Unlock()
	// Re-check under lock.
	if v, ok := rdmaTransports.Load(addr); ok {
		t := v.(*rdmaFollowerTransport)
		t.mu.Lock()
		closed := t.closed
		t.mu.Unlock()
		if !closed {
			return t
		}
		// Stale closed entry — replace.
		rdmaTransports.Delete(addr)
	}
	t := newRDMAFollowerTransport(addr)
	rdmaTransports.Store(addr, t)
	return t
}

func newRDMAFollowerTransport(addr string) *rdmaFollowerTransport {
	t := &rdmaFollowerTransport{
		addr:     addr,
		sendCh:   make(chan *FollowerPacket, rdmaTransportSendQSize),
		inflight: make(chan *rdmaInflight, rdmaTransportRecvQSize),
		stopCh:   make(chan struct{}),
	}
	t.wg.Add(2)
	go t.sendLoop()
	go t.recvLoop()
	return t
}

// sendLoop pulls packets in FIFO order from sendCh, acquires a slot
// (hash-routed to a stable conn for ordering), posts the WRs to the QP
// send queue, and hands off to recvLoop. Single goroutine per peer →
// ibv_post_send is called serially per QP → posts arrive at the
// follower in caller order.
func (t *rdmaFollowerTransport) sendLoop() {
	defer t.wg.Done()
	for {
		select {
		case fp := <-t.sendCh:
			t.processSend(fp)
		case <-t.stopCh:
			return
		}
	}
}

func (t *rdmaFollowerTransport) processSend(fp *FollowerPacket) {
	rdmaAddr := t.addr
	if followerRDMAPortShift != 0 {
		rdmaAddr = util.ShiftAddrPort(t.addr, followerRDMAPortShift)
	}
	// Hash routing: same (PartitionID, ExtentID) → same conn → same QP.
	// Combined with the per-conn worker on the server side, this
	// preserves the strict offset ordering extent.go requires.
	key := fmt.Sprintf("%d-%d", fp.PartitionID, fp.ExtentID)
	handle, err := followerRDMAPool.AcquireSlotForKey(rdmaAddr, key)
	if err != nil {
		rdma.MetricsObserveFallback(rdma.RoleFollower, t.addr, "acquire_slot")
		fp.respCh <- fmt.Errorf("repl follower rdma: acquire slot to %s: %w", rdmaAddr, err)
		return
	}
	conn := handle.Conn
	slot := handle.SlotIdx
	lastDoneSeq := conn.RecvDoneSeq(slot)

	if err = conn.WritePacket(slot, &fp.Packet); err != nil {
		followerRDMAPool.ReleaseSlot(handle, true)
		rdma.MetricsObserveFallback(rdma.RoleFollower, t.addr, "write_packet")
		fp.respCh <- fmt.Errorf("repl follower rdma: WritePacket: %w", err)
		return
	}

	in := &rdmaInflight{
		fp:          fp,
		handle:      handle,
		start:       time.Now(),
		lastDoneSeq: lastDoneSeq,
	}
	select {
	case t.inflight <- in:
	case <-t.stopCh:
		// Shutdown raced — release resources and tell the caller.
		followerRDMAPool.ReleaseSlot(handle, true)
		fp.respCh <- fmt.Errorf("repl follower rdma: transport closing")
	}
}

// recvLoop drains the inflight queue and waits for each round-trip's
// response, then signals the caller via fp.respCh.
//
// Single recv goroutine per peer is enough because pipelined sends
// produce responses in order — the QP delivers WRITE_WITH_IMM CQEs
// in posted order on the follower side, and the doorbells fire in
// the same order on our side. Polling per-slot in inflight order
// matches that order.
func (t *rdmaFollowerTransport) recvLoop() {
	defer t.wg.Done()
	for {
		select {
		case in := <-t.inflight:
			t.processRecv(in)
		case <-t.stopCh:
			// Drain remaining inflight so callers don't block forever.
			for {
				select {
				case in := <-t.inflight:
					followerRDMAPool.ReleaseSlot(in.handle, true)
					in.fp.respCh <- fmt.Errorf("repl follower rdma: transport closing")
				default:
					return
				}
			}
		}
	}
}

func (t *rdmaFollowerTransport) processRecv(in *rdmaInflight) {
	conn := in.handle.Conn
	slot := in.handle.SlotIdx
	resp, err := pollFollowerRDMAResponse(conn, slot, in.lastDoneSeq)
	if err != nil {
		followerRDMAPool.ReleaseSlot(in.handle, true)
		rdma.MetricsObserveFallback(rdma.RoleFollower, t.addr, "poll_response")
		in.fp.respCh <- err
		return
	}

	if cerr := conn.ReturnCredit(slot); cerr != nil {
		followerRDMAPool.ReleaseSlot(in.handle, true)
		rdma.MetricsObserveFallback(rdma.RoleFollower, t.addr, "return_credit")
		in.fp.respCh <- fmt.Errorf("repl follower rdma: ReturnCredit: %w", cerr)
		return
	}

	if resp.ReqID != in.fp.ReqID {
		followerRDMAPool.ReleaseSlot(in.handle, true)
		rdma.MetricsObserveFallback(rdma.RoleFollower, t.addr, "reqid_mismatch")
		in.fp.respCh <- fmt.Errorf("repl follower rdma: ReqID mismatch: got %d want %d", resp.ReqID, in.fp.ReqID)
		return
	}

	// Mirror response fields back into the caller's packet so the
	// upstream receiveResponse logic sees the same shape it gets from
	// the TCP path (FollowerPacket.ReadFromConn).
	in.fp.ResultCode = resp.ResultCode

	followerRDMAPool.ReleaseSlot(in.handle, false)

	if resp.ResultCode != proto.OpOk {
		rdma.MetricsObserveRequest(rdma.RoleFollower, t.addr, time.Since(in.start))
		in.fp.respCh <- fmt.Errorf("repl follower rdma: follower ResultCode=%d", resp.ResultCode)
		return
	}
	rdma.MetricsObserveRequest(rdma.RoleFollower, t.addr, time.Since(in.start))
	in.fp.respCh <- nil
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
