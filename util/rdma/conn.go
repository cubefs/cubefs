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
	"sync/atomic"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
)

const (
	maxSlots       = 1024
	defaultCQSize  = 512
	resolveTimeout = 2000 // ms
	sendQueueDepth = 256

	// creditCellBytes is the size of the per-connection credit-return cell.
	// We store a single uint64 (current peer-side processed-slot count).
	creditCellBytes = 8
)

// RDMAConn encapsulates a single RC (Reliable Connected) RDMA connection.
// All data is transferred via RDMA Write (one-sided); Send/Recv are never used.
//
// Memory layout (per side):
//
//	recvRing      — peer RDMA-Writes incoming data here (we read)
//	recvDB        — peer RDMA-Writes incoming doorbell entries here (we poll)
//	sendScratch   — we RDMA-Write outgoing data from here (to peer's recvRing)
//	sendDB        — we stage outgoing doorbell values here before RDMA-Writing to peer's recvDB
//	localCredit   — peer RDMA-Writes its processed-slot count here (we read for flow control)
//	creditScratch — we stage our processed-slot count here before RDMA-Writing it to peer's localCredit
//
// Slot index: determined by caller as ReqID % numSlots.
// A single slot must not be used concurrently from multiple goroutines.
type RDMAConn struct {
	cmID *C.struct_rdma_cm_id
	pd   *C.struct_ibv_pd
	cq   *C.struct_ibv_cq
	evCh *C.struct_rdma_event_channel

	// Memory regions
	recvRing      *RDMAMem // peer writes data here
	recvDB        *RDMAMem // peer writes doorbell entries here (we poll)
	sendScratch   *RDMAMem // we stage outgoing data here before RDMA Write
	sendDB        *RDMAMem // we stage outgoing doorbell values here before RDMA Write
	localCredit   *RDMAMem // peer writes its processed-slot count here (8 bytes)
	creditScratch *RDMAMem // we stage outgoing credit values here (8 bytes)

	// Remote peer's memory descriptor (received at connect time)
	peerRecvRkey   uint32
	peerRecvBaseVA uint64
	peerDBRkey     uint32
	peerDBBaseVA   uint64
	peerCreditRkey uint32
	peerCreditVA   uint64

	// Per-slot monotonic sequence for sends
	nextSendSeq [maxSlots]uint32
	// Per-slot last received seq (for PollRecvDoorbell)
	lastRecvSeq [maxSlots]uint32

	numSlots   int
	slotSize   int
	remoteAddr string

	// P0: connection-scoped flow control. credit guards against ring overrun
	// regardless of slot-allocation strategy.
	credit        *creditState
	creditAckMode CreditAckMode

	closed int32 // atomic; 1 = closed
}

// RDMAConnConfig holds per-connection sizing parameters.
type RDMAConnConfig struct {
	NumSlots int
	SlotSize int // bytes per data slot (covers SlotHeader + PacketHeader + Arg + Data)
	// CreditAckMode controls whether the receiver waits for the credit-return
	// RDMA Write's CQE before processing the next slot. CreditAckSync (zero
	// value) is the safe default; CreditAckAsync trades CQ pressure for
	// throughput.
	CreditAckMode CreditAckMode
}

// validateConnConfig enforces P0 invariants on the configuration. Returns a
// detailed error on violation; callers (Dial/Listen/Accept) must propagate
// this so RDMA init fails cleanly and the caller can fall back to TCP.
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
// The caller's recvRing and recvDB are communicated to the server as ConnectInfo.
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
	cq, err := createCQ(ctx, defaultCQSize)
	if err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	if err = createQP(id, pd, cq, sendQueueDepth); err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}

	dbSize := cfg.NumSlots * DoorbellEntrySize
	mems, err := allocConnMems(pd, cfg.NumSlots, cfg.SlotSize, dbSize)
	if err != nil {
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
		mems.free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: connect to %s: %w", addr, err)
	}
	ai, err := UnmarshalAcceptInfo(serverBytes)
	if err != nil {
		mems.free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: unmarshal AcceptInfo from %s: %w", addr, err)
	}

	conn := &RDMAConn{
		cmID:           id,
		pd:             pd,
		cq:             cq,
		evCh:           ch,
		recvRing:       mems.recvRing,
		recvDB:         mems.recvDB,
		sendScratch:    mems.sendScratch,
		sendDB:         mems.sendDB,
		localCredit:    mems.localCredit,
		creditScratch:  mems.creditScratch,
		peerRecvRkey:   ai.ReqRkey,
		peerRecvBaseVA: ai.ReqBaseVA,
		peerDBRkey:     ai.DbRkey,
		peerDBBaseVA:   ai.DbVA,
		peerCreditRkey: ai.CreditRkey,
		peerCreditVA:   ai.CreditVA,
		numSlots:       cfg.NumSlots,
		slotSize:       cfg.SlotSize,
		remoteAddr:     addr,
		creditAckMode:  cfg.CreditAckMode,
	}
	conn.credit = newCreditState(cfg.NumSlots, conn.localCreditPtr())
	return conn, nil
}

// Accept waits for one incoming connection on listenID and returns the new conn.
// ci is the client's ConnectInfo (tells server where to write responses and doorbells).
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
	cq, err := createCQ(ctx, defaultCQSize)
	if err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	if err = createQP(connID, pd, cq, sendQueueDepth); err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	dbSize := cfg.NumSlots * DoorbellEntrySize
	mems, err := allocConnMems(pd, cfg.NumSlots, cfg.SlotSize, dbSize)
	if err != nil {
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
		mems.free()
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	conn := &RDMAConn{
		cmID:           connID,
		pd:             pd,
		cq:             cq,
		recvRing:       mems.recvRing,
		recvDB:         mems.recvDB,
		sendScratch:    mems.sendScratch,
		sendDB:         mems.sendDB,
		localCredit:    mems.localCredit,
		creditScratch:  mems.creditScratch,
		peerRecvRkey:   ci.RespRkey,
		peerRecvBaseVA: ci.RespBaseVA,
		peerDBRkey:     ci.RespDbRkey,
		peerDBBaseVA:   ci.RespDbVA,
		peerCreditRkey: ci.CreditRkey,
		peerCreditVA:   ci.CreditVA,
		numSlots:       cfg.NumSlots,
		slotSize:       cfg.SlotSize,
		creditAckMode:  cfg.CreditAckMode,
	}
	conn.credit = newCreditState(cfg.NumSlots, conn.localCreditPtr())
	return conn, ci, nil
}

// connMems bundles the six pinned memory regions a connection needs so we can
// allocate them transactionally and roll back on failure.
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

// RecvSlotBytes returns the receive ring slice for slot idx.
// On the server, this is where incoming requests land.
// On the client, this is where incoming responses land.
func (c *RDMAConn) RecvSlotBytes(idx int) []byte {
	return c.recvRing.SlotBytes(idx, c.slotSize)
}

// SendScratchBytes returns the local send scratch for slot idx.
// Caller fills this, then calls WritePacket / WriteData.
func (c *RDMAConn) SendScratchBytes(idx int) []byte {
	return c.sendScratch.SlotBytes(idx, c.slotSize)
}

// WritePacket serializes p into the local scratch slot at slotIdx, then:
//  1. RDMA Writes the slot to peer's recvRing (not signaled)
//  2. Writes a doorbell entry and RDMA Writes it to peer's recvDB (signaled)
//
// Blocks if no flow-control credits are available. Not goroutine-safe for the
// same slotIdx.
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
	WriteSlotHeader(scratch, seq, uint32(totalLen)) // stamp seq into SlotHeader

	return c.writeSlotAndDoorbell(slotIdx, scratch[:totalLen], seq)
}

// WriteData RDMA-Writes raw data (already serialized) to peer's recvRing at slotIdx,
// then fires the doorbell. Used by server to write responses back to client.
//
// Blocks if no flow-control credits are available.
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

	return c.writeSlotAndDoorbell(slotIdx, scratch[:len(data)], seq)
}

// acquireSendCredit blocks until a flow-control credit is available. Honors
// connection close so callers do not deadlock during shutdown.
func (c *RDMAConn) acquireSendCredit() error {
	if c.credit == nil {
		return nil
	}
	if c.IsClosed() {
		return ErrCreditClosed
	}
	return c.credit.acquireCredit(context.Background())
}

func (c *RDMAConn) writeSlotAndDoorbell(slotIdx int, payload []byte, seq uint32) error {
	qp := getQPFromCMID(c.cmID)

	lSlotAddr := c.sendScratch.VA + uint64(slotIdx*c.slotSize)
	rSlotAddr := c.peerRecvBaseVA + uint64(slotIdx*c.slotSize)

	// Write 1: slot payload (not signaled — doorbell Write carries the signal)
	if err := postRDMAWrite(qp,
		lSlotAddr, c.sendScratch.Lkey, uint32(len(payload)),
		rSlotAddr, c.peerRecvRkey,
		uint64(slotIdx*2), false); err != nil {
		return fmt.Errorf("rdma: slot write: %w", err)
	}

	// Prepare doorbell entry in local sendDB slot
	dbOff := slotIdx * DoorbellEntrySize
	dbBuf := c.sendDB.Bytes()[dbOff : dbOff+DoorbellEntrySize]
	WriteDoorbellEntry(dbBuf, 0, seq, uint32(slotIdx))

	lDbAddr := c.sendDB.VA + uint64(dbOff)
	rDbAddr := c.peerDBBaseVA + uint64(dbOff)

	// Write 2: doorbell (signaled — CQE confirms both writes have left the local NIC)
	if err := postRDMAWrite(qp,
		lDbAddr, c.sendDB.Lkey, DoorbellEntrySize,
		rDbAddr, c.peerDBRkey,
		uint64(slotIdx*2+1), true); err != nil {
		return fmt.Errorf("rdma: doorbell write: %w", err)
	}

	return c.drainOneCQE()
}

// drainOneCQE spins until at least one successful CQE is collected.
func (c *RDMAConn) drainOneCQE() error {
	for {
		ids, err := pollCQ(c.cq)
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			return nil
		}
		runtime.Gosched()
	}
}

// PollRecvDoorbell checks if peer has written a new doorbell entry for slot idx.
// Returns (newSeq, true) when a new entry is detected.
// lastSeen must be maintained by the caller per-slot.
func (c *RDMAConn) PollRecvDoorbell(idx int, lastSeen uint32) (uint32, bool) {
	off := idx * DoorbellEntrySize
	seq, _ := ReadDoorbellEntry(c.recvDB.Bytes()[off:], 0)
	if seq != lastSeen {
		return seq, true
	}
	return 0, false
}

// ReturnCredit signals that one received slot has been processed locally and
// the peer is now free to reuse it. Returns the new processed-slot count.
//
// In CreditAckSync mode (default) this blocks until the credit-return RDMA
// Write completes; in CreditAckAsync it posts the WR and returns immediately.
//
// Receivers (server-side handleSlot, client-side response handler) must call
// this after consuming each slot. Failing to call it will eventually cause
// the peer's sender to block forever.
func (c *RDMAConn) ReturnCredit() error {
	if c.credit == nil {
		return nil
	}
	if c.IsClosed() {
		return ErrCreditClosed
	}
	processed := c.credit.onProcessSlot()

	// Stage processed count into local credit-scratch buffer (8 bytes LE).
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
		uint64(0xC0EDC0DE), signaled); err != nil {
		return fmt.Errorf("rdma: credit-return write: %w", err)
	}
	if signaled {
		return c.drainOneCQE()
	}
	return nil
}

// Close tears down the RDMA connection and frees all resources. Idempotent.
func (c *RDMAConn) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	// Wake any goroutines parked in acquireSendCredit so they can exit cleanly.
	if c.credit != nil {
		c.credit.closeCredits()
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

// RecvSeq returns the last received doorbell seq for slot, used by callers to
// persist state across pooled round-trips on the same connection.
func (c *RDMAConn) RecvSeq(slot int) uint32 { return c.lastRecvSeq[slot] }

// SetRecvSeq updates the persisted receive seq for slot.
func (c *RDMAConn) SetRecvSeq(slot int, seq uint32) { c.lastRecvSeq[slot] = seq }

// CreditStats returns a snapshot of credit accounting state for diagnostics.
// All three counters are monotonic; sent should never exceed received+numSlots.
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

// Accept blocks until an incoming RDMA connection is established and returns it.
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
