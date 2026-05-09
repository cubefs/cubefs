//go:build linux && rdma

package rdma

/*
#include "rdma.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	resolveTimeout = 2000 // ms

	// creditCellBytes is the size of the per-connection credit-return cell.
	// We store a single uint64 (current peer-side processed-slot count).
	creditCellBytes = 8
)

// sendQueueDepthFor returns the SQ depth for a connection with numSlots
// concurrent slots. Each WritePacket posts 2 WRs (slot + doorbell) and each
// ReturnCredit posts 1 — so peak in-flight WRs per slot is ~3, and we add a
// safety margin so the QP never blocks ibv_post_send under sustained load.
func sendQueueDepthFor(numSlots int) int {
	d := numSlots * 4
	if d < 256 {
		d = 256
	}
	return d
}

// cqSizeFor returns the CQ size matching the SQ depth plus the recv pool.
// The CQ must be at least as large as the sum of outstanding signaled WRs
// across both queues; oversizing slightly avoids overflow under bursts.
func cqSizeFor(numSlots int) int {
	return sendQueueDepthFor(numSlots) + numSlots + 64
}

// RDMAConn encapsulates a single RC (Reliable Connected) RDMA connection.
//
// Send-side: WritePacket / WriteData / ReturnCredit are fire-and-forget.
// They post WRs to the SQ and return immediately; completions are reaped
// asynchronously by the per-conn drainer goroutine. Senders that need to
// know when the peer has responded poll recvDoneSeq[slotIdx] (memory) or
// block on recvCond (sleep).
//
// Recv-side: incoming WRITE_WITH_IMM doorbells consume one recv WR each;
// the drainer refills the pool on every CQE so the QP stays serviceable.
//
// Memory layout (per side):
//
//	recvRing      — peer RDMA-Writes incoming data here (we read)
//	recvDB        — peer RDMA-Writes incoming doorbell entries here (we poll)
//	sendScratch   — we RDMA-Write outgoing data from here (to peer's recvRing)
//	sendDB        — we stage outgoing doorbell values here before RDMA-Writing to peer's recvDB
//	localCredit   — peer RDMA-Writes its processed-slot count here (we read for flow control)
//	creditScratch — we stage our processed-slot count here before RDMA-Writing it to peer's localCredit
//	recvPool.mr   — pre-posted recv WR buffers; consumed by incoming WITH_IMM doorbells
//
// A single slot must be used by at most one goroutine at a time; SlotPool
// (slot_pool.go) enforces this for callers that don't manage slots
// directly.
type RDMAConn struct {
	cmID   *C.struct_rdma_cm_id
	pd     *C.struct_ibv_pd
	cq     *C.struct_ibv_cq
	compCh *C.struct_ibv_comp_channel // bound to cq; used by drainer for sleep
	evCh   *C.struct_rdma_event_channel

	// Memory regions
	recvRing      *RDMAMem // peer writes data here
	recvDB        *RDMAMem // peer writes doorbell entries here (we poll)
	sendScratch   *RDMAMem // we stage outgoing data here before RDMA Write
	sendDB        *RDMAMem // we stage outgoing doorbell values here before RDMA Write
	localCredit   *RDMAMem // peer writes its processed-slot count here (8 bytes)
	creditScratch *RDMAMem // we stage outgoing credit values here (8 bytes)
	recvPool      *recvPool

	// Remote peer's memory descriptor (received at connect time)
	peerRecvRkey   uint32
	peerRecvBaseVA uint64
	peerDBRkey     uint32
	peerDBBaseVA   uint64
	peerCreditRkey uint32
	peerCreditVA   uint64

	// Per-slot monotonic sequence for sends (stamped into SlotHeader.Seq).
	nextSendSeq [maxSlots]uint32
	// Per-slot last received seq (for PollRecvDoorbell).
	lastRecvSeq [maxSlots]uint32

	// recvDoneSeq[slotIdx] increments each time the drainer observes a
	// recv-side CQE whose imm_data points at slotIdx. Senders waiting on
	// a response use it to detect arrivals when memory polling has
	// already advanced into the sleep phase.
	recvDoneSeq [maxSlots]uint64
	// recvSignalSeq is a global counter incremented on every recv CQE,
	// for callers that wait on "any recv" rather than a specific slot
	// (server pollLoop in idle mode).
	recvSignalSeq uint64

	// recvCond is broadcast by the drainer after every recv CQE so that
	// callers parked in sleep mode can re-check their per-slot or
	// per-conn signal counter.
	recvMu   sync.Mutex
	recvCond *sync.Cond

	numSlots   int
	slotSize   int
	remoteAddr string
	role       string

	// P0: connection-scoped flow control. credit guards against ring
	// overrun regardless of slot-allocation strategy.
	credit        *creditState
	creditAckMode CreditAckMode

	// P2: adaptive polling parameters.
	pollCfg PollConfig

	// Drainer goroutine lifecycle. drainerStop is closed by Close to
	// signal the goroutine to exit; drainerDone is closed by the
	// goroutine on the way out so Close can wait for it deterministically.
	drainerStop chan struct{}
	drainerDone chan struct{}

	// cqEventsToAck accumulates events delivered via comp_channel so we
	// batch-ack to bound kernel-side bookkeeping. Touched only by the
	// drainer goroutine; Close flushes after the drainer exits.
	cqEventsToAck uint32

	closed int32 // atomic; 1 = closed
}

// RDMAConnConfig is defined in config.go (no build tag) so the same type
// is shared across rdma and stub builds.

// effectivePollConfig returns Poll if any field is non-zero, otherwise
// the spec-defined default.
func effectivePollConfig(cfg RDMAConnConfig) PollConfig {
	if cfg.Poll.BusySpinCount == 0 && cfg.Poll.YieldCount == 0 && cfg.Poll.SleepThresholdUs == 0 {
		return DefaultPollConfig
	}
	return cfg.Poll
}

// validateConnConfig enforces P0 invariants on the configuration. Returns
// a detailed error on violation; callers (Dial / Listen / Accept) must
// propagate this so RDMA init fails cleanly and the caller can fall back
// to TCP.
func validateConnConfig(cfg RDMAConnConfig) error {
	if cfg.NumSlots <= 0 || cfg.NumSlots > maxSlots {
		return fmt.Errorf("rdma: NumSlots %d out of range [1,%d]", cfg.NumSlots, maxSlots)
	}
	if err := ValidateSlotSize(cfg.SlotSize); err != nil {
		return err
	}
	return nil
}

// Dial establishes a client-side RDMA RC connection to addr ("host:port").
// The caller's recvRing and recvDB are communicated to the server as
// ConnectInfo; the drainer goroutine is started before this call returns.
func Dial(addr string, cfg RDMAConnConfig) (*RDMAConn, error) {
	if err := validateConnConfig(cfg); err != nil {
		return nil, err
	}

	ch, err := createEventChannel()
	if err != nil {
		return nil, err
	}
	id, err := createCMID(ch)
	if err != nil {
		destroyEventChannel(ch)
		return nil, err
	}
	if err = resolveAddr(id, addr, resolveTimeout); err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	if err = resolveRoute(id, resolveTimeout); err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}

	ctx := getCTXFromCMID(id)
	pd, err := allocPD(ctx)
	if err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	compCh, err := createCompChannel(ctx)
	if err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	cq, err := createCQ(ctx, cqSizeFor(cfg.NumSlots), compCh)
	if err != nil {
		destroyCompChannel(compCh)
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	if err = createQP(id, pd, cq, sendQueueDepthFor(cfg.NumSlots), cfg.NumSlots); err != nil {
		destroyCompChannel(compCh)
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}

	dbSize := cfg.NumSlots * DoorbellEntrySize
	mems, err := allocConnMems(pd, cfg.NumSlots, cfg.SlotSize, dbSize)
	if err != nil {
		destroyCompChannel(compCh)
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	rp, err := newRecvPool(pd, getQPFromCMID(id), cfg.NumSlots)
	if err != nil {
		mems.free()
		destroyCompChannel(compCh)
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}

	ci := ConnectInfo{
		RespRkey:   mems.recvRing.Rkey,
		RespBaseVA: mems.recvRing.VA,
		RespDbRkey: mems.recvDB.Rkey,
		RespDbVA:   mems.recvDB.VA,
		NumSlots:   uint32(cfg.NumSlots),
		SlotSize:   uint32(cfg.SlotSize),
		CreditRkey: mems.localCredit.Rkey,
		CreditVA:   mems.localCredit.VA,
	}
	serverBytes, err := connectTo(id, MarshalConnectInfo(ci))
	if err != nil {
		rp.free()
		mems.free()
		destroyCompChannel(compCh)
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: connect to %s: %w", addr, err)
	}
	ai, err := UnmarshalAcceptInfo(serverBytes)
	if err != nil {
		rp.free()
		mems.free()
		destroyCompChannel(compCh)
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: unmarshal AcceptInfo from %s: %w", addr, err)
	}

	conn := newRDMAConn(id, pd, cq, compCh, ch, mems, rp, ai.ReqRkey, ai.ReqBaseVA,
		ai.DbRkey, ai.DbVA, ai.CreditRkey, ai.CreditVA, addr, cfg)
	conn.startDrainer()
	return conn, nil
}

// Accept waits for one incoming connection on listenID and returns the
// new conn (with its drainer goroutine already running).
func Accept(listenID *C.struct_rdma_cm_id, cfg RDMAConnConfig) (*RDMAConn, ConnectInfo, error) {
	if err := validateConnConfig(cfg); err != nil {
		return nil, ConnectInfo{}, err
	}

	connID, privData, err := getRequest(listenID)
	if err != nil {
		return nil, ConnectInfo{}, err
	}
	ci, err := UnmarshalConnectInfo(privData)
	if err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, fmt.Errorf("rdma: bad ConnectInfo: %w", err)
	}

	ctx := getCTXFromCMID(connID)
	pd, err := allocPD(ctx)
	if err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	compCh, err := createCompChannel(ctx)
	if err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	cq, err := createCQ(ctx, cqSizeFor(cfg.NumSlots), compCh)
	if err != nil {
		destroyCompChannel(compCh)
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	if err = createQP(connID, pd, cq, sendQueueDepthFor(cfg.NumSlots), cfg.NumSlots); err != nil {
		destroyCompChannel(compCh)
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	dbSize := cfg.NumSlots * DoorbellEntrySize
	mems, err := allocConnMems(pd, cfg.NumSlots, cfg.SlotSize, dbSize)
	if err != nil {
		destroyCompChannel(compCh)
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	rp, err := newRecvPool(pd, getQPFromCMID(connID), cfg.NumSlots)
	if err != nil {
		mems.free()
		destroyCompChannel(compCh)
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	ai := AcceptInfo{
		ReqRkey:    mems.recvRing.Rkey,
		ReqBaseVA:  mems.recvRing.VA,
		DbRkey:     mems.recvDB.Rkey,
		DbVA:       mems.recvDB.VA,
		NumSlots:   uint32(cfg.NumSlots),
		SlotSize:   uint32(cfg.SlotSize),
		CreditRkey: mems.localCredit.Rkey,
		CreditVA:   mems.localCredit.VA,
	}
	if err = acceptConn(connID, MarshalAcceptInfo(ai)); err != nil {
		rp.free()
		mems.free()
		destroyCompChannel(compCh)
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	conn := newRDMAConn(connID, pd, cq, compCh, nil, mems, rp,
		ci.RespRkey, ci.RespBaseVA, ci.RespDbRkey, ci.RespDbVA,
		ci.CreditRkey, ci.CreditVA, "", cfg)
	conn.startDrainer()
	return conn, ci, nil
}

// newRDMAConn constructs the RDMAConn struct and wires up cond / credit /
// poll state. It does NOT start the drainer goroutine — callers must call
// startDrainer after the QP is in RTS state.
func newRDMAConn(
	cmID *C.struct_rdma_cm_id, pd *C.struct_ibv_pd, cq *C.struct_ibv_cq,
	compCh *C.struct_ibv_comp_channel, evCh *C.struct_rdma_event_channel,
	mems *connMems, rp *recvPool,
	peerRecvRkey uint32, peerRecvBaseVA uint64,
	peerDBRkey uint32, peerDBBaseVA uint64,
	peerCreditRkey uint32, peerCreditVA uint64,
	remoteAddr string,
	cfg RDMAConnConfig,
) *RDMAConn {
	c := &RDMAConn{
		cmID:           cmID,
		pd:             pd,
		cq:             cq,
		compCh:         compCh,
		evCh:           evCh,
		recvRing:       mems.recvRing,
		recvDB:         mems.recvDB,
		sendScratch:    mems.sendScratch,
		sendDB:         mems.sendDB,
		localCredit:    mems.localCredit,
		creditScratch:  mems.creditScratch,
		recvPool:       rp,
		peerRecvRkey:   peerRecvRkey,
		peerRecvBaseVA: peerRecvBaseVA,
		peerDBRkey:     peerDBRkey,
		peerDBBaseVA:   peerDBBaseVA,
		peerCreditRkey: peerCreditRkey,
		peerCreditVA:   peerCreditVA,
		numSlots:       cfg.NumSlots,
		slotSize:       cfg.SlotSize,
		remoteAddr:     remoteAddr,
		creditAckMode:  cfg.CreditAckMode,
		pollCfg:        effectivePollConfig(cfg),
		role:           cfg.Role,
		drainerStop:    make(chan struct{}),
		drainerDone:    make(chan struct{}),
	}
	c.recvCond = sync.NewCond(&c.recvMu)
	c.credit = newCreditState(cfg.NumSlots, c.localCreditPtr())
	return c
}

// connMems bundles the six pinned memory regions a connection needs so we
// can allocate them transactionally and roll back on failure.
type connMems struct {
	recvRing      *RDMAMem
	recvDB        *RDMAMem
	sendScratch   *RDMAMem
	sendDB        *RDMAMem
	localCredit   *RDMAMem
	creditScratch *RDMAMem
}

func (m *connMems) free() {
	for _, r := range []*RDMAMem{m.recvRing, m.recvDB, m.sendScratch, m.sendDB, m.localCredit, m.creditScratch} {
		if r != nil {
			r.Free()
		}
	}
}

func allocConnMems(pd *C.struct_ibv_pd, numSlots, slotSize, dbSize int) (*connMems, error) {
	m := &connMems{}
	var err error
	if m.recvRing, err = AllocRDMAMem(pd, numSlots*slotSize); err != nil {
		return nil, err
	}
	if m.recvDB, err = AllocRDMAMem(pd, dbSize); err != nil {
		m.free()
		return nil, err
	}
	if m.sendScratch, err = AllocRDMAMem(pd, numSlots*slotSize); err != nil {
		m.free()
		return nil, err
	}
	if m.sendDB, err = AllocRDMAMem(pd, dbSize); err != nil {
		m.free()
		return nil, err
	}
	if m.localCredit, err = AllocRDMAMem(pd, creditCellBytes); err != nil {
		m.free()
		return nil, err
	}
	if m.creditScratch, err = AllocRDMAMem(pd, creditCellBytes); err != nil {
		m.free()
		return nil, err
	}
	return m, nil
}

// localCreditPtr returns a *uint64 view of the first 8 bytes of localCredit.
// The pointer is stable for the conn's lifetime (pinned C memory).
func (c *RDMAConn) localCreditPtr() *uint64 {
	return (*uint64)(unsafe.Pointer(uintptr(c.localCredit.VA)))
}

// NumSlots returns the number of ring buffer slots.
func (c *RDMAConn) NumSlots() int { return c.numSlots }

// SlotSize returns bytes per slot.
func (c *RDMAConn) SlotSize() int { return c.slotSize }

// PollConfig returns the polling configuration applied to this connection.
func (c *RDMAConn) PollConfig() PollConfig { return c.pollCfg }

// Role returns the metric role label associated with this connection.
// Empty means metrics are disabled for this conn.
func (c *RDMAConn) Role() string { return c.role }

// RecvSlotBytes returns the receive ring slice for slot idx.
func (c *RDMAConn) RecvSlotBytes(idx int) []byte {
	return c.recvRing.SlotBytes(idx, c.slotSize)
}

// SendScratchBytes returns the local send scratch for slot idx.
func (c *RDMAConn) SendScratchBytes(idx int) []byte {
	return c.sendScratch.SlotBytes(idx, c.slotSize)
}

// WritePacket serializes p into the local scratch slot at slotIdx, then
// posts the slot payload (not signaled) followed by the doorbell write
// with imm_data = slotIdx (signaled).
//
// Fire-and-forget: returns as soon as both WRs are enqueued. The drainer
// goroutine consumes the resulting CQEs asynchronously. Senders wait for
// the peer's response by polling recvDoneSeq (memory) or blocking on
// recvCond (sleep) — usually via WaitRecvDoneSeq.
//
// Blocks if no flow-control credits are available. Not goroutine-safe for
// the same slotIdx.
func (c *RDMAConn) WritePacket(slotIdx int, p *proto.Packet) error {
	if err := c.acquireSendCredit(); err != nil {
		return err
	}
	scratch := c.SendScratchBytes(slotIdx)
	seq := c.nextSendSeq[slotIdx] + 1
	c.nextSendSeq[slotIdx] = seq

	totalLen, err := SerializePacket(scratch, p)
	if err != nil {
		return err
	}
	WriteSlotHeader(scratch, seq, uint32(totalLen))

	return c.postSlotAndDoorbell(slotIdx, scratch[:totalLen], seq)
}

// WriteData RDMA-Writes raw data (already serialized) to peer's recvRing
// at slotIdx, then fires the doorbell. Fire-and-forget, same semantics as
// WritePacket. Used by server to write responses back to client.
func (c *RDMAConn) WriteData(slotIdx int, data []byte) error {
	if len(data) > c.slotSize {
		return fmt.Errorf("rdma: WriteData: data %d > slotSize %d", len(data), c.slotSize)
	}
	if err := c.acquireSendCredit(); err != nil {
		return err
	}
	scratch := c.SendScratchBytes(slotIdx)
	copy(scratch[:len(data)], data)

	seq := c.nextSendSeq[slotIdx] + 1
	c.nextSendSeq[slotIdx] = seq

	return c.postSlotAndDoorbell(slotIdx, scratch[:len(data)], seq)
}

// WriteSlotZeroCopy posts the first totalLen bytes of sendScratch[slot]
// to the peer's recvRing[slot] + the doorbell, WITHOUT a memcpy through
// the WriteData path. The caller is responsible for staging the full
// packet (PacketHeader + Arg + Data) at offset SlotHeaderSize within
// sendScratch[slot]; this method stamps the SlotHeader and posts.
//
// Used by P5 zero-copy paths: e.g. handleReadSlot reads disk data
// directly into a slice of sendScratch via store.Read, then calls
// WriteSlotZeroCopy with the precomputed totalLen — eliminating one
// memcpy of the response data per round trip.
//
// Blocks if no flow-control credit is available. Not goroutine-safe for
// the same slotIdx.
func (c *RDMAConn) WriteSlotZeroCopy(slotIdx, totalLen int) error {
	if totalLen < SlotHeaderSize {
		return fmt.Errorf("rdma: WriteSlotZeroCopy: totalLen %d < SlotHeaderSize %d", totalLen, SlotHeaderSize)
	}
	if totalLen > c.slotSize {
		return fmt.Errorf("rdma: WriteSlotZeroCopy: totalLen %d > slotSize %d", totalLen, c.slotSize)
	}
	if err := c.acquireSendCredit(); err != nil {
		return err
	}
	seq := c.nextSendSeq[slotIdx] + 1
	c.nextSendSeq[slotIdx] = seq

	scratch := c.SendScratchBytes(slotIdx)
	WriteSlotHeader(scratch, seq, uint32(totalLen))

	return c.postSlotAndDoorbell(slotIdx, scratch[:totalLen], seq)
}

// acquireSendCredit blocks until a flow-control credit is available.
// Records cubefs_rdma_credit_stall_total when the call had to wait
// non-trivially (>10µs), so a few cycles of pure-spin success don't
// flood the counter.
func (c *RDMAConn) acquireSendCredit() error {
	if c.credit == nil {
		return nil
	}
	if c.IsClosed() {
		return ErrCreditClosed
	}
	start := time.Now()
	if err := c.credit.acquireCredit(context.Background()); err != nil {
		return err
	}
	if time.Since(start) > 10*time.Microsecond {
		metricsIncCreditStall(c.role, c.remoteAddr)
	}
	return nil
}

// postSlotAndDoorbell posts the data + doorbell WR pair without waiting.
func (c *RDMAConn) postSlotAndDoorbell(slotIdx int, payload []byte, seq uint32) error {
	qp := getQPFromCMID(c.cmID)

	lSlotAddr := c.sendScratch.VA + uint64(slotIdx*c.slotSize)
	rSlotAddr := c.peerRecvBaseVA + uint64(slotIdx*c.slotSize)

	// Slot payload: not signaled (no CQE). The doorbell that follows is
	// signaled; ordered RC delivery means data lands in peer memory before
	// the doorbell CQE is delivered to the peer.
	if err := postRDMAWrite(qp,
		lSlotAddr, c.sendScratch.Lkey, uint32(len(payload)),
		rSlotAddr, c.peerRecvRkey,
		encodeWRID(opSlot, slotIdx), false); err != nil {
		return fmt.Errorf("rdma: slot write: %w", err)
	}

	dbOff := slotIdx * DoorbellEntrySize
	dbBuf := c.sendDB.Bytes()[dbOff : dbOff+DoorbellEntrySize]
	WriteDoorbellEntry(dbBuf, 0, seq, uint32(slotIdx))

	lDbAddr := c.sendDB.VA + uint64(dbOff)
	rDbAddr := c.peerDBBaseVA + uint64(dbOff)

	// Doorbell with imm_data = slotIdx. Signaled so the drainer can
	// account it; imm_data also tells the peer's recv side which slot
	// fired without scanning the doorbell array (P2 wakeup hint).
	if err := postRDMAWriteWithImm(qp,
		lDbAddr, c.sendDB.Lkey, DoorbellEntrySize,
		rDbAddr, c.peerDBRkey,
		encodeWRID(opDoorbell, slotIdx), uint32(slotIdx), true); err != nil {
		return fmt.Errorf("rdma: doorbell write: %w", err)
	}
	return nil
}

// PollRecvDoorbell checks if peer has written a new doorbell entry for
// slot idx. Used by busy/yield phases of the response wait; sleep phase
// callers should use WaitRecvDoneSeq or WaitRecvSignal instead.
func (c *RDMAConn) PollRecvDoorbell(idx int, lastSeen uint32) (uint32, bool) {
	off := idx * DoorbellEntrySize
	seq, _ := ReadDoorbellEntry(c.recvDB.Bytes()[off:], 0)
	if seq != lastSeen {
		return seq, true
	}
	return 0, false
}

// RecvDoneSeq returns the per-slot drainer-incremented counter. Senders
// should snapshot this BEFORE posting WritePacket; subsequent reads detect
// the response arrival even when memory polling has already advanced.
func (c *RDMAConn) RecvDoneSeq(slotIdx int) uint64 {
	return atomic.LoadUint64(&c.recvDoneSeq[slotIdx])
}

// RecvSignalSeq returns the global drainer-incremented counter. Used by
// callers that wait on "any recv arrived" rather than a specific slot.
func (c *RDMAConn) RecvSignalSeq() uint64 {
	return atomic.LoadUint64(&c.recvSignalSeq)
}

// WaitRecvDoneSeq blocks until recvDoneSeq[slotIdx] strictly exceeds
// lastSeen, the connection is closed, or ctx is cancelled. Cheap when the
// drainer has already advanced the counter (single atomic load + lock).
func (c *RDMAConn) WaitRecvDoneSeq(ctx context.Context, slotIdx int, lastSeen uint64) error {
	if atomic.LoadUint64(&c.recvDoneSeq[slotIdx]) > lastSeen {
		return nil
	}
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	for atomic.LoadUint64(&c.recvDoneSeq[slotIdx]) <= lastSeen {
		if c.IsClosed() {
			return ErrCreditClosed
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		c.recvCond.Wait()
	}
	return nil
}

// WaitRecvSignal blocks until recvSignalSeq strictly exceeds lastSeen,
// the connection is closed, or ctx is cancelled. Used by server pollLoop's
// sleep phase: any incoming doorbell wakes us, and the caller re-scans
// memory to find the new slot.
func (c *RDMAConn) WaitRecvSignal(ctx context.Context, lastSeen uint64) error {
	if atomic.LoadUint64(&c.recvSignalSeq) > lastSeen {
		return nil
	}
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	for atomic.LoadUint64(&c.recvSignalSeq) <= lastSeen {
		if c.IsClosed() {
			return ErrCreditClosed
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		c.recvCond.Wait()
	}
	return nil
}

// startDrainer spawns the per-conn completion drainer goroutine. Must be
// called exactly once after Dial / Accept; closed by Close.
func (c *RDMAConn) startDrainer() {
	go c.runDrainer()
}

// runDrainer is the per-conn completion-pump loop. It owns the CQ and the
// comp_channel for sleep blocking. Exits cleanly when drainerStop closes.
func (c *RDMAConn) runDrainer() {
	defer close(c.drainerDone)
	qp := getQPFromCMID(c.cmID)
	poller := NewAdaptivePoller(c.pollCfg)
	for {
		select {
		case <-c.drainerStop:
			return
		default:
		}

		evs, err := pollCQEvents(c.cq)
		if err != nil {
			if c.IsClosed() {
				return
			}
			log.LogWarnf("rdma drainer (%s): pollCQEvents: %v", c.remoteAddr, err)
			runtime.Gosched()
			continue
		}

		for _, ev := range evs {
			c.dispatchCompletion(qp, ev)
		}

		if len(evs) > 0 {
			poller.Reset()
			continue
		}

		switch poller.NextAction() {
		case ActionContinue:
		case ActionYield:
			runtime.Gosched()
		case ActionSleep:
			if !c.drainerSleep() {
				return
			}
			poller.Reset()
		}
	}
}

// dispatchCompletion routes one CQE to its waiter. Errors are logged and
// the WR is otherwise skipped; flush-on-teardown errors are common during
// graceful close and must not abort the drainer.
func (c *RDMAConn) dispatchCompletion(qp *C.struct_ibv_qp, ev CompletionEvent) {
	op, slot := decodeWRID(ev.WRID)
	if !ev.Success() {
		// During Close, QP transitions to ERR and WRs flush as
		// IBV_WC_WR_FLUSH_ERR. Suppress logs in that case to avoid noise.
		if !c.IsClosed() {
			log.LogWarnf("rdma drainer (%s): WR error op=%v slot=%d status=%d",
				c.remoteAddr, op, slot, ev.Status)
		}
		// Recv WR errors leave the pool short. Refill best-effort so we
		// can keep accepting traffic after a transient error.
		if op == opRecv {
			_ = c.recvPool.refillOne(qp, ev.WRID)
		}
		return
	}

	switch {
	case op == opRecv:
		// Imm_data carries the slot index of the firing doorbell.
		slotFromImm := slot
		if ev.HasImm {
			slotFromImm = int(ev.ImmData)
		}
		if slotFromImm >= 0 && slotFromImm < c.numSlots {
			atomic.AddUint64(&c.recvDoneSeq[slotFromImm], 1)
		}
		atomic.AddUint64(&c.recvSignalSeq, 1)
		if rerr := c.recvPool.refillOne(qp, ev.WRID); rerr != nil {
			log.LogWarnf("rdma drainer (%s): refill recv: %v", c.remoteAddr, rerr)
		}
		c.recvMu.Lock()
		c.recvCond.Broadcast()
		c.recvMu.Unlock()
	case op == opShutdownPing:
		// Wake-up signal posted by Close; drainerStop will be closed and
		// the next loop iteration exits.
	case op == opDoorbell || op == opCredit || op == opSlot:
		// Send-side completion: nothing to wait on in fire-and-forget mode.
		// Diagnostic counters (CreditStats) live in creditState.
	default:
		log.LogWarnf("rdma drainer (%s): unknown WR op=%v wr_id=0x%x", c.remoteAddr, op, ev.WRID)
	}
}

// drainerSleep arms the CQ and blocks on the comp_channel for the next
// completion event. Returns false if the drainer should exit (close in
// progress and event delivery failed).
func (c *RDMAConn) drainerSleep() bool {
	if c.compCh == nil {
		runtime.Gosched()
		return true
	}
	if err := reqNotifyCQ(c.cq, false); err != nil {
		if c.IsClosed() {
			return false
		}
		log.LogWarnf("rdma drainer (%s): reqNotifyCQ: %v", c.remoteAddr, err)
		return true
	}
	cq, err := waitCQEvent(c.compCh)
	if err != nil {
		if c.IsClosed() {
			return false
		}
		log.LogWarnf("rdma drainer (%s): waitCQEvent: %v", c.remoteAddr, err)
		return true
	}
	c.cqEventsToAck++
	if c.cqEventsToAck >= 16 {
		ackCQEvents(cq, uint(c.cqEventsToAck))
		c.cqEventsToAck = 0
	}
	return true
}

// flushCQEventAcks drains any outstanding event acks. Called from Close
// after the drainer has exited so we do not race with the drainer's
// own ack accounting.
func (c *RDMAConn) flushCQEventAcks() {
	if c.cqEventsToAck > 0 {
		ackCQEvents(c.cq, uint(c.cqEventsToAck))
		c.cqEventsToAck = 0
	}
}

// postShutdownPing posts a self-targeted RDMA Write of length 1 to the
// connection's own credit-scratch buffer. Generates a CQE that wakes the
// drainer if it is blocked on the comp_channel, allowing Close to proceed
// without racing with ibv_get_cq_event.
func (c *RDMAConn) postShutdownPing() {
	qp := getQPFromCMID(c.cmID)
	// We use creditScratch's local key only; remote rkey/raddr point at
	// localCredit (also ours), since the WR is purely a CQE generator.
	if err := postRDMAWrite(qp,
		c.creditScratch.VA, c.creditScratch.Lkey, 1,
		c.localCredit.VA, c.localCredit.Rkey,
		encodeWRID(opShutdownPing, 0), true); err != nil {
		// Best-effort: if even this fails, the drainer will eventually
		// pick up the QP-flush CQEs from rdma_destroy_id below.
		log.LogWarnf("rdma close (%s): postShutdownPing: %v", c.remoteAddr, err)
	}
}

// ReturnCredit signals that one received slot has been processed locally
// and the peer is now free to reuse it. Fire-and-forget: posts the
// credit-write WR and returns immediately. CreditAckMode is honoured by
// requesting (or not) a signaled WR; either way the drainer accounts
// the resulting CQE asynchronously.
//
// slotIdx identifies the slot whose data was just consumed; the peer's
// credit cell stores a global processed-count, but the WR ID encodes the
// slot for completion-routing diagnostics.
func (c *RDMAConn) ReturnCredit(slotIdx int) error {
	if c.credit == nil {
		return nil
	}
	if c.IsClosed() {
		return ErrCreditClosed
	}
	processed := c.credit.onProcessSlot()

	// Encode processed count into the local credit-scratch buffer
	// (8 bytes LE).
	scratch := c.creditScratch.Bytes()
	scratch[0] = byte(processed)
	scratch[1] = byte(processed >> 8)
	scratch[2] = byte(processed >> 16)
	scratch[3] = byte(processed >> 24)
	scratch[4] = byte(processed >> 32)
	scratch[5] = byte(processed >> 40)
	scratch[6] = byte(processed >> 48)
	scratch[7] = byte(processed >> 56)

	signaled := c.creditAckMode == CreditAckSync
	qp := getQPFromCMID(c.cmID)
	if err := postRDMAWrite(qp,
		c.creditScratch.VA, c.creditScratch.Lkey, creditCellBytes,
		c.peerCreditVA, c.peerCreditRkey,
		encodeWRID(opCredit, slotIdx), signaled); err != nil {
		return fmt.Errorf("rdma: credit-return write: %w", err)
	}
	return nil
}

// Close tears down the RDMA connection and frees all resources.
// Idempotent. Safe to call concurrently with WritePacket / pollLoop;
// any in-flight callers will exit via ErrCreditClosed.
func (c *RDMAConn) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	if c.credit != nil {
		c.credit.closeCredits()
	}

	// Wake any callers parked in WaitRecvDoneSeq / WaitRecvSignal.
	c.recvMu.Lock()
	c.recvCond.Broadcast()
	c.recvMu.Unlock()

	// Wake the drainer if it's blocked on comp_channel: post a
	// self-signaled ping that delivers a CQE.
	if c.drainerDone != nil {
		c.postShutdownPing()
		close(c.drainerStop)
		<-c.drainerDone
	}

	c.flushCQEventAcks()

	if c.recvPool != nil {
		c.recvPool.free()
		c.recvPool = nil
	}
	for _, m := range []*RDMAMem{
		c.recvRing, c.recvDB, c.sendScratch, c.sendDB, c.localCredit, c.creditScratch,
	} {
		if m != nil {
			m.Free()
		}
	}
	if c.cmID != nil {
		destroyCMID(c.cmID)
		c.cmID = nil
	}
	if c.compCh != nil {
		destroyCompChannel(c.compCh)
		c.compCh = nil
	}
	if c.evCh != nil {
		destroyEventChannel(c.evCh)
		c.evCh = nil
	}
	return nil
}

// RemoteAddr returns the peer's address string.
func (c *RDMAConn) RemoteAddr() string { return c.remoteAddr }

// IsClosed reports whether the connection has been closed.
func (c *RDMAConn) IsClosed() bool { return atomic.LoadInt32(&c.closed) == 1 }

// RecvSeq returns the last received doorbell seq for slot, used by callers
// to persist state across pooled round-trips on the same connection.
func (c *RDMAConn) RecvSeq(slot int) uint32 { return c.lastRecvSeq[slot] }

// SetRecvSeq updates the persisted receive seq for slot.
func (c *RDMAConn) SetRecvSeq(slot int, seq uint32) { c.lastRecvSeq[slot] = seq }

// CreditStats returns a snapshot of credit accounting state for diagnostics.
// All three counters are monotonic; sent should never exceed
// received + numSlots.
func (c *RDMAConn) CreditStats() (sent, received, processed uint64) {
	if c.credit == nil {
		return 0, 0, 0
	}
	return c.credit.stats()
}

// RDMAListener wraps a RDMA CM listener. Create via Listen().
type RDMAListener struct {
	evCh   *C.struct_rdma_event_channel
	id     *C.struct_rdma_cm_id
	cfg    RDMAConnConfig
	closed int32
}

// Listen creates a RDMA listener bound to port on all interfaces.
func Listen(port int, cfg RDMAConnConfig) (*RDMAListener, error) {
	if err := validateConnConfig(cfg); err != nil {
		return nil, fmt.Errorf("rdma: Listen: %w", err)
	}
	ch, err := createEventChannel()
	if err != nil {
		return nil, err
	}
	id, err := bindAndListen(ch, port)
	if err != nil {
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: Listen port %d: %w", port, err)
	}
	return &RDMAListener{evCh: ch, id: id, cfg: cfg}, nil
}

// Accept blocks until an incoming RDMA connection is established and
// returns it (drainer goroutine already running).
func (l *RDMAListener) Accept() (*RDMAConn, error) {
	if atomic.LoadInt32(&l.closed) != 0 {
		return nil, fmt.Errorf("rdma: listener closed")
	}
	conn, _, err := Accept(l.id, l.cfg)
	return conn, err
}

// Close tears down the listener. Idempotent.
func (l *RDMAListener) Close() error {
	if atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		destroyCMID(l.id)
		destroyEventChannel(l.evCh)
	}
	return nil
}
