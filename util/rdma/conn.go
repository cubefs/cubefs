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
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	resolveTimeout = 2000 // ms

	// creditCellBytes is the size of the per-connection credit-return cell.
	// We store a single uint64 (current peer-side processed-slot count).
	creditCellBytes = 8

	// cqEventAckBatch is how many comp_channel events the drainer
	// accumulates before flushing a single ibv_ack_cq_events. Larger
	// batches reduce ack syscall pressure; the kernel tolerates an
	// unacked-event count up to ~UINT_MAX, so the limit is purely
	// drainer-side bookkeeping. 16 is empirically a good balance for
	// the cubefs hot-path slot rate.
	cqEventAckBatch = 16
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
	// Length == numSlots, indexed by slot. Sized dynamically rather than
	// using a fixed [maxSlots] array so a conn with numSlots=8 doesn't
	// pay 1024 × 8 byte cold-cache footprint for unused slots.
	nextSendSeq []uint32
	// Per-slot last received seq (for PollRecvDoorbell).
	lastRecvSeq []uint32

	// recvDoneSeq[slotIdx] increments each time the drainer observes a
	// recv-side CQE whose imm_data points at slotIdx. Senders waiting on
	// a response use it to detect arrivals when memory polling has
	// already advanced into the sleep phase.
	recvDoneSeq []uint64
	// recvSignalSeq is a global counter incremented on every recv CQE,
	// for callers that wait on "any recv" rather than a specific slot
	// (server pollLoop in idle mode).
	recvSignalSeq uint64

	// Per-slot cond: WaitRecvDoneSeq parks on the cond whose index
	// matches its slot, so an incoming doorbell only wakes the one
	// goroutine actually waiting on that slot rather than fanning out
	// to all numSlots waiters (most of whom would re-sleep). At
	// numSlots=256 this is the difference between 256 spurious wakes
	// per CQE and 1.
	recvSlotMus   []sync.Mutex
	recvSlotConds []*sync.Cond

	// recvCond is a global cond broadcast on every recv CQE for callers
	// that wait on "any recv arrival" (server pollLoop's sleep phase).
	// Slot-specific waiters use recvSlotConds instead.
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

	// faulted is set by markFault() when the drainer observes an
	// unrecoverable transport error. It signals "logical close" — pool
	// stops handing out slots, in-flight credit/recv waiters bail —
	// WITHOUT freeing resources. Resource cleanup remains the
	// responsibility of Close(), which sets `closed`.
	//
	// Two flags rather than one because if markFault() set `closed`
	// directly, the subsequent Close() call from SlotPool would CAS-fail
	// and skip the entire teardown path: drainer goroutine would never
	// observe drainerStop, MRs would leak, and waitCQEvent would block
	// forever on a dead QP.
	faulted int32 // atomic; 1 = drainer detected fault
	closed  int32 // atomic; 1 = Close() called and resources freed

	// One-sided RDMA Read state (Sprint A.5b). Lazily initialised on
	// the first PostRDMAReadAndWait call; allocates an MR-registered
	// scratch buffer + per-slot completion waiters. See read_waiter.go.
	readScratchInitMu sync.Mutex
	readScratchInited bool
	readScratchInitErr error
	readScratch       *RDMAMem
	readWaiters       []readWaiter
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
	pd, err := getOrAllocPDForCtx(ctx)
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
	// Migrate connID off the listener's event channel onto its own
	// dedicated channel BEFORE rdma_accept. Otherwise ESTABLISHED for
	// this conn would land on the same queue as future
	// CONNECT_REQUESTs and the acceptConn waitCMEvent below would
	// race, surfacing as "expected event 9, got 4" — half-accepted
	// conns then fail every send with REM_OP_ERR.
	connCh, err := createEventChannel()
	if err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	if err = migrateCMID(connID, connCh); err != nil {
		destroyEventChannel(connCh)
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	ci, err := UnmarshalConnectInfo(privData)
	if err != nil {
		destroyCMID(connID)
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, fmt.Errorf("rdma: bad ConnectInfo: %w", err)
	}

	ctx := getCTXFromCMID(connID)
	pd, err := getOrAllocPDForCtx(ctx)
	if err != nil {
		destroyCMID(connID)
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, err
	}
	compCh, err := createCompChannel(ctx)
	if err != nil {
		destroyCMID(connID)
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, err
	}
	cq, err := createCQ(ctx, cqSizeFor(cfg.NumSlots), compCh)
	if err != nil {
		destroyCompChannel(compCh)
		destroyCMID(connID)
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, err
	}
	if err = createQP(connID, pd, cq, sendQueueDepthFor(cfg.NumSlots), cfg.NumSlots); err != nil {
		destroyCompChannel(compCh)
		destroyCMID(connID)
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, err
	}

	dbSize := cfg.NumSlots * DoorbellEntrySize
	mems, err := allocConnMems(pd, cfg.NumSlots, cfg.SlotSize, dbSize)
	if err != nil {
		destroyCompChannel(compCh)
		destroyCMID(connID)
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, err
	}
	rp, err := newRecvPool(pd, getQPFromCMID(connID), cfg.NumSlots)
	if err != nil {
		mems.free()
		destroyCompChannel(compCh)
		destroyCMID(connID)
		destroyEventChannel(connCh)
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
		destroyEventChannel(connCh)
		return nil, ConnectInfo{}, err
	}

	conn := newRDMAConn(connID, pd, cq, compCh, connCh, mems, rp,
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
		nextSendSeq:    make([]uint32, cfg.NumSlots),
		lastRecvSeq:    make([]uint32, cfg.NumSlots),
		recvDoneSeq:    make([]uint64, cfg.NumSlots),
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
	c.recvSlotMus = make([]sync.Mutex, cfg.NumSlots)
	c.recvSlotConds = make([]*sync.Cond, cfg.NumSlots)
	for i := range c.recvSlotConds {
		c.recvSlotConds[i] = sync.NewCond(&c.recvSlotMus[i])
	}
	c.credit = newCreditState(cfg.NumSlots, c.localCreditPtr(), c.pollCfg)
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
// totalLen is validated against (SlotHeader + minimum PacketHeader)
// rather than just SlotHeader: a totalLen smaller than that is
// definitely malformed (the receiver's DeserializePacket would fail
// with a confusing "payload too short" error). Catching it here turns
// a silent corruption into a fail-fast.
//
// Blocks if no flow-control credit is available. Not goroutine-safe for
// the same slotIdx.
func (c *RDMAConn) WriteSlotZeroCopy(slotIdx, totalLen int) error {
	const minTotalLen = SlotHeaderSize + util.PacketHeaderSize
	if totalLen < minTotalLen {
		return fmt.Errorf("rdma: WriteSlotZeroCopy: totalLen %d < minimum %d (SlotHeader %d + PacketHeader %d) — caller did not stage a valid packet",
			totalLen, minTotalLen, SlotHeaderSize, util.PacketHeaderSize)
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

	// NOTE: previously held c.sendMu around the (slot WR + doorbell WR)
	// pair to keep the post sequence atomic. With the leader fan-out
	// already serialised by repl_protocol.go (sync followerRDMASend
	// inside OperatorAndForwardPktGoRoutine, single goroutine per
	// replication conn) and the server side draining via a single
	// per-conn worker, only one post call is in flight per QP at a
	// time on either side, so the lock had no callers to exclude and
	// was reverted after WC status=10/12 (REM_OP_ERR/REM_INV_REQ_ERR)
	// errors appeared simultaneously across all peers — symptom of a
	// hot-path interaction we couldn't isolate. The serialisation we
	// need now lives one layer up.

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
//
// Listens on a per-slot cond (m1) so an unrelated slot's CQE doesn't
// wake us. ctx cancellation is honoured promptly via a watcher
// goroutine (m3) that broadcasts the cond on Done — without this, a
// peer that stops responding entirely would never trigger any
// broadcast and the sender would park past its ctx deadline.
func (c *RDMAConn) WaitRecvDoneSeq(ctx context.Context, slotIdx int, lastSeen uint64) error {
	if atomic.LoadUint64(&c.recvDoneSeq[slotIdx]) > lastSeen {
		return nil
	}
	if slotIdx < 0 || slotIdx >= len(c.recvSlotConds) {
		return fmt.Errorf("rdma: WaitRecvDoneSeq: slot %d out of range [0,%d)", slotIdx, len(c.recvSlotConds))
	}

	cond := c.recvSlotConds[slotIdx]
	mu := &c.recvSlotMus[slotIdx]

	// ctx watcher: forwards ctx.Done into a Broadcast on the slot's
	// cond so callers parked in cond.Wait observe cancellation
	// immediately rather than waiting for the next recv CQE. The
	// watcher exits cleanly via watcherDone when the wait completes
	// for any reason.
	var watcherDone chan struct{}
	if ctx != nil && ctx.Done() != nil {
		watcherDone = make(chan struct{})
		defer close(watcherDone)
		go func() {
			select {
			case <-ctx.Done():
				mu.Lock()
				cond.Broadcast()
				mu.Unlock()
			case <-watcherDone:
			}
		}()
	}

	mu.Lock()
	defer mu.Unlock()
	for atomic.LoadUint64(&c.recvDoneSeq[slotIdx]) <= lastSeen {
		if c.IsClosed() {
			return ErrCreditClosed
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		cond.Wait()
	}
	return nil
}

// WaitRecvSignal blocks until recvSignalSeq strictly exceeds lastSeen,
// the connection is closed, or ctx is cancelled. Used by server
// pollLoop's sleep phase: any incoming doorbell wakes us, and the
// caller re-scans memory to find the new slot.
//
// ctx cancellation is honoured promptly via a watcher goroutine
// matching the WaitRecvDoneSeq pattern.
func (c *RDMAConn) WaitRecvSignal(ctx context.Context, lastSeen uint64) error {
	if atomic.LoadUint64(&c.recvSignalSeq) > lastSeen {
		return nil
	}

	var watcherDone chan struct{}
	if ctx != nil && ctx.Done() != nil {
		watcherDone = make(chan struct{})
		defer close(watcherDone)
		go func() {
			select {
			case <-ctx.Done():
				c.recvMu.Lock()
				c.recvCond.Broadcast()
				c.recvMu.Unlock()
			case <-watcherDone:
			}
		}()
	}

	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	for atomic.LoadUint64(&c.recvSignalSeq) <= lastSeen {
		if c.IsClosed() {
			return ErrCreditClosed
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
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

// markFault flips the connection into the faulted state without freeing
// resources or stopping the drainer. Used by the drainer when it
// observes an unrecoverable send-side completion error so callers
// (SlotPool, in-flight WritePacket) stop using the conn promptly.
//
// Crucially this sets the `faulted` flag, NOT `closed`. Close() owns
// the `closed` CAS and the resource-cleanup path; if markFault stole
// the closed CAS, Close would no-op and we would leak the drainer
// goroutine + every pinned MR. IsClosed reports either flag so the
// pool / waiters react identically to faults and explicit closes.
func (c *RDMAConn) markFault() {
	if !atomic.CompareAndSwapInt32(&c.faulted, 0, 1) {
		return
	}
	if c.credit != nil {
		c.credit.closeCredits()
	}
	if c.recvCond != nil {
		c.recvMu.Lock()
		c.recvCond.Broadcast()
		c.recvMu.Unlock()
	}
	// Wake every per-slot waiter so they observe IsClosed and return
	// promptly rather than waiting for an unrelated recv CQE that
	// may never arrive on a faulted conn.
	for i := range c.recvSlotConds {
		if c.recvSlotConds[i] == nil {
			continue
		}
		c.recvSlotMus[i].Lock()
		c.recvSlotConds[i].Broadcast()
		c.recvSlotMus[i].Unlock()
	}
}

// dispatchCompletion routes one CQE to its waiter. Errors are logged and
// the WR is otherwise skipped; flush-on-teardown errors are common during
// graceful close and must not abort the drainer.
func (c *RDMAConn) dispatchCompletion(qp *C.struct_ibv_qp, ev CompletionEvent) {
	op, slot := decodeWRID(ev.WRID)
	if !ev.Success() {
		// IBV_WC_WR_FLUSH_ERR (status 5) is what every in-flight WR
		// reports when the QP transitions to ERR during teardown. We
		// expect those during Close and don't log or treat them as a
		// fault. Any other status on a NOT-yet-closed conn means the
		// HCA is reporting a real transport error (RNR_RETRY_EXC,
		// REM_INV_REQ, LOC_LEN_ERR, etc.) and the QP is unusable.
		const ibvWcWrFlushErr = 5
		isFlush := ev.Status == ibvWcWrFlushErr
		if !c.IsClosed() && !isFlush {
			log.LogWarnf("rdma drainer (%s): WR error op=%v slot=%d status=%d — marking conn faulted",
				c.remoteAddr, op, slot, ev.Status)
			// A failed send-side WR (doorbell / credit / shutdown) means
			// the QP is broken — mark the conn closed so the slot pool
			// stops handing out new slots and existing waiters bail out.
			// Recv-side WC errors are usually a consequence of the same
			// underlying fault; mark them too unless we're already closed.
			c.markFault()
		}
		// Recv WR errors leave the pool short. Refill best-effort so we
		// can keep accepting traffic after a transient error (no-op if
		// QP is now ERR — refill will fail silently and the conn will
		// be torn down via Close).
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
		validSlot := slotFromImm >= 0 && slotFromImm < c.numSlots
		if validSlot {
			atomic.AddUint64(&c.recvDoneSeq[slotFromImm], 1)
		}
		atomic.AddUint64(&c.recvSignalSeq, 1)
		if rerr := c.recvPool.refillOne(qp, ev.WRID); rerr != nil {
			log.LogWarnf("rdma drainer (%s): refill recv: %v", c.remoteAddr, rerr)
		}
		// Targeted wake: only the per-slot waiter (m1 — avoids fanning
		// out to all 256 slot-waiters when only one slot's response
		// arrived). recvCond stays for "any-recv" pollLoop callers.
		if validSlot && c.recvSlotConds != nil {
			c.recvSlotMus[slotFromImm].Lock()
			c.recvSlotConds[slotFromImm].Broadcast()
			c.recvSlotMus[slotFromImm].Unlock()
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
	case op == opRDMARead:
		// One-sided RDMA Read completion (Sprint A.5b). Route to the
		// per-slot read waiter; non-blocking send guarantees the
		// drainer never stalls behind a timed-out caller.
		c.completeRDMARead(slot, ev)
	default:
		log.LogWarnf("rdma drainer (%s): unknown WR op=%v wr_id=0x%x", c.remoteAddr, op, ev.WRID)
	}
}

// drainerSleep arms the CQ and blocks on the comp_channel for the next
// completion event. Returns false if the drainer should exit (close in
// progress and event delivery failed, or fault already observed).
//
// Two pre-flight checks bracket the reqNotifyCQ call to close the
// "QP-already-dead" race window: if markFault has flipped the conn into
// the faulted state OR Close has closed drainerStop, we MUST NOT enter
// waitCQEvent. The QP being in ERR state means postShutdownPing's
// ibv_post_send fails with no CQE, and the kernel never delivers a
// completion event to wake us — leaving the drainer parked forever.
//
// The post-arm check (after reqNotifyCQ) is what guarantees no wake
// signal can be missed: anyone calling Close from this point onward
// must close drainerStop BEFORE attempting to wake us, so a
// drainerStop-closed value is observable here.
func (c *RDMAConn) drainerSleep() bool {
	if c.compCh == nil {
		runtime.Gosched()
		return true
	}

	// Pre-arm: bail if we already know we shouldn't sleep.
	if c.IsClosed() {
		return false
	}
	select {
	case <-c.drainerStop:
		return false
	default:
	}

	if err := reqNotifyCQ(c.cq, false); err != nil {
		if c.IsClosed() {
			return false
		}
		log.LogWarnf("rdma drainer (%s): reqNotifyCQ: %v", c.remoteAddr, err)
		return true
	}

	// Post-arm: this re-check closes the race where Close happened
	// between the pre-arm check and reqNotifyCQ. After this point the
	// CQ is armed; a future signaled WR (or destroy of the channel)
	// would deliver a wake event. But if Close has ALREADY raised
	// drainerStop / closed, no wake will arrive on a faulted QP.
	if c.IsClosed() {
		return false
	}
	select {
	case <-c.drainerStop:
		return false
	default:
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
	if c.cqEventsToAck >= cqEventAckBatch {
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

	// Atomic 8-byte write into the pinned credit-scratch buffer.
	// Multiple handleSlot goroutines can call ReturnCredit concurrently
	// (P1 dispatches one goroutine per arriving slot), all targeting the
	// same 8-byte cell. A naive byte-by-byte stamp races at the Go level
	// (race detector flags it) and exposes the NIC to torn reads
	// composed of bytes from two different processed counts.
	// atomic.StoreUint64 collapses both: the value the NIC DMA-reads is
	// always a complete uint64 snapshot. The peer's onPeerCreditUpdate
	// is monotonic so out-of-order writes between goroutines do not
	// regress its view — only the monotonicity of *each* snapshot
	// matters, and that's what the atomic guarantees.
	//
	// creditScratch.VA is C.malloc'd, which on glibc is at least 16-byte
	// aligned — atomic.StoreUint64 is safe on x86_64 / arm64.
	creditCell := (*uint64)(unsafe.Pointer(uintptr(c.creditScratch.VA)))
	atomic.StoreUint64(creditCell, processed)

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
//
// Close owns the `closed` CAS — the resource-cleanup path. markFault
// sets a separate `faulted` flag so Close still runs end-to-end even
// after the drainer has already noticed a fault.
func (c *RDMAConn) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	// Also mark faulted so any code that races between the load of
	// closed and faulted sees a consistent "not usable" state.
	atomic.StoreInt32(&c.faulted, 1)

	if c.credit != nil {
		c.credit.closeCredits()
	}

	// Wake any callers parked in WaitRecvDoneSeq / WaitRecvSignal.
	// recvCond is normally initialised by newRDMAConn; SlotPool unit
	// tests bypass that path and construct partial-init RDMAConn shells
	// via fakeDial — guard against the nil case so test cleanup paths
	// don't panic.
	if c.recvCond != nil {
		c.recvMu.Lock()
		c.recvCond.Broadcast()
		c.recvMu.Unlock()
	}
	// Per-slot conds (m1) — wake every slot's WaitRecvDoneSeq waiter
	// individually so they each re-check IsClosed and return
	// ErrCreditClosed instead of parking forever after a teardown.
	for i := range c.recvSlotConds {
		if c.recvSlotConds[i] == nil {
			continue
		}
		c.recvSlotMus[i].Lock()
		c.recvSlotConds[i].Broadcast()
		c.recvSlotMus[i].Unlock()
	}

	// Wake the drainer if it's blocked on comp_channel. ORDER MATTERS:
	// close(drainerStop) MUST happen BEFORE postShutdownPing so that a
	// drainer that wakes (or is already idle) sees the closed channel
	// in its post-arm select and exits cleanly even if the ping is
	// rejected by the QP (e.g. QP transitioned to ERR via markFault).
	// If we ping first then close, the drainer could enter waitCQEvent
	// in the gap, the ping would fail to generate a CQE, and the
	// drainer would park forever.
	if c.drainerDone != nil {
		close(c.drainerStop)
		c.postShutdownPing()
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
	// Sprint A.5b: tear down the one-sided read scratch + waiters.
	// Safe even if init never ran — freeReadScratch checks for that.
	c.freeReadScratch()
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

// IsClosed reports whether the connection is no longer usable for new
// requests. Returns true after either markFault (drainer-detected
// fault) or Close (explicit teardown).
func (c *RDMAConn) IsClosed() bool {
	return atomic.LoadInt32(&c.closed)|atomic.LoadInt32(&c.faulted) != 0
}

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
