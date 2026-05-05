//go:build linux && rdma

package rdma

/*
#include "rdma.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
)

const (
	maxSlots       = 1024
	defaultCQSize  = 512
	resolveTimeout = 2000 // ms
	sendQueueDepth = 256
)

// RDMAConn encapsulates a single RC (Reliable Connected) RDMA connection.
// All data is transferred via RDMA Write (one-sided); Send/Recv are never used.
//
// Memory layout (per side):
//
//	recvRing  — peer RDMA-Writes incoming data here (we read)
//	recvDB    — peer RDMA-Writes incoming doorbell entries here (we poll)
//	sendScratch — we RDMA-Write outgoing data from here (to peer's recvRing)
//	sendDB    — we stage outgoing doorbell values here before RDMA-Writing to peer's recvDB
//
// Slot index: determined by caller as ReqID % numSlots.
// A single slot must not be used concurrently from multiple goroutines.
type RDMAConn struct {
	cmID    *C.struct_rdma_cm_id
	pd      *C.struct_ibv_pd
	cq      *C.struct_ibv_cq
	evCh    *C.struct_rdma_event_channel

	// Memory regions
	recvRing    *RDMAMem // peer writes data here
	recvDB      *RDMAMem // peer writes doorbell entries here (we poll)
	sendScratch *RDMAMem // we stage outgoing data here before RDMA Write
	sendDB      *RDMAMem // we stage outgoing doorbell values here before RDMA Write

	// Remote peer's memory descriptor (received at connect time)
	peerRecvRkey   uint32
	peerRecvBaseVA uint64
	peerDBRkey     uint32
	peerDBBaseVA   uint64

	// Per-slot monotonic sequence for sends
	nextSendSeq [maxSlots]uint32
	// Per-slot last received seq (for PollRecvDoorbell)
	lastRecvSeq [maxSlots]uint32

	numSlots     int
	slotSize     int
	remoteAddr   string

	closed int32 // atomic; 1 = closed
}

// RDMAConnConfig holds per-connection sizing parameters.
type RDMAConnConfig struct {
	NumSlots int
	SlotSize int // bytes per data slot (covers SlotHeader + PacketHeader + Arg + Data)
}

// Dial establishes a client-side RDMA RC connection to addr ("host:port").
// The caller's recvRing and recvDB are communicated to the server as ConnectInfo.
func Dial(addr string, cfg RDMAConnConfig) (*RDMAConn, error) {
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
	recvRing, err := AllocRDMAMem(pd, cfg.NumSlots*cfg.SlotSize)
	if err != nil {
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	recvDB, err := AllocRDMAMem(pd, dbSize)
	if err != nil {
		recvRing.Free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	sendScratch, err := AllocRDMAMem(pd, cfg.NumSlots*cfg.SlotSize)
	if err != nil {
		recvDB.Free()
		recvRing.Free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}
	sendDB, err := AllocRDMAMem(pd, dbSize)
	if err != nil {
		sendScratch.Free()
		recvDB.Free()
		recvRing.Free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, err
	}

	ci := ConnectInfo{
		RespRkey:   recvRing.Rkey,
		RespBaseVA: recvRing.VA,
		RespDbRkey: recvDB.Rkey,
		RespDbVA:   recvDB.VA,
		NumSlots:   uint32(cfg.NumSlots),
		SlotSize:   uint32(cfg.SlotSize),
	}
	serverBytes, err := connectTo(id, MarshalConnectInfo(ci))
	if err != nil {
		sendDB.Free()
		sendScratch.Free()
		recvDB.Free()
		recvRing.Free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: connect to %s: %w", addr, err)
	}
	ai, err := UnmarshalAcceptInfo(serverBytes)
	if err != nil {
		sendDB.Free()
		sendScratch.Free()
		recvDB.Free()
		recvRing.Free()
		destroyCMID(id)
		destroyEventChannel(ch)
		return nil, fmt.Errorf("rdma: unmarshal AcceptInfo from %s: %w", addr, err)
	}

	return &RDMAConn{
		cmID:           id,
		pd:             pd,
		cq:             cq,
		evCh:           ch,
		recvRing:       recvRing,
		recvDB:         recvDB,
		sendScratch:    sendScratch,
		sendDB:         sendDB,
		peerRecvRkey:   ai.ReqRkey,
		peerRecvBaseVA: ai.ReqBaseVA,
		peerDBRkey:     ai.DbRkey,
		peerDBBaseVA:   ai.DbVA,
		numSlots:       cfg.NumSlots,
		slotSize:       cfg.SlotSize,
		remoteAddr:     addr,
	}, nil
}

// Accept waits for one incoming connection on listenID and returns the new conn.
// ci is the client's ConnectInfo (tells server where to write responses and doorbells).
func Accept(listenID *C.struct_rdma_cm_id, cfg RDMAConnConfig) (*RDMAConn, ConnectInfo, error) {
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
	recvRing, err := AllocRDMAMem(pd, cfg.NumSlots*cfg.SlotSize)
	if err != nil {
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	recvDB, err := AllocRDMAMem(pd, dbSize)
	if err != nil {
		recvRing.Free()
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	sendScratch, err := AllocRDMAMem(pd, cfg.NumSlots*cfg.SlotSize)
	if err != nil {
		recvDB.Free()
		recvRing.Free()
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}
	sendDB, err := AllocRDMAMem(pd, dbSize)
	if err != nil {
		sendScratch.Free()
		recvDB.Free()
		recvRing.Free()
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	ai := AcceptInfo{
		ReqRkey:   recvRing.Rkey,
		ReqBaseVA: recvRing.VA,
		DbRkey:    recvDB.Rkey,
		DbVA:      recvDB.VA,
		NumSlots:  uint32(cfg.NumSlots),
		SlotSize:  uint32(cfg.SlotSize),
	}
	if err = acceptConn(connID, MarshalAcceptInfo(ai)); err != nil {
		sendDB.Free()
		sendScratch.Free()
		recvDB.Free()
		recvRing.Free()
		destroyCMID(connID)
		return nil, ConnectInfo{}, err
	}

	conn := &RDMAConn{
		cmID:           connID,
		pd:             pd,
		cq:             cq,
		recvRing:       recvRing,
		recvDB:         recvDB,
		sendScratch:    sendScratch,
		sendDB:         sendDB,
		peerRecvRkey:   ci.RespRkey,
		peerRecvBaseVA: ci.RespBaseVA,
		peerDBRkey:     ci.RespDbRkey,
		peerDBBaseVA:   ci.RespDbVA,
		numSlots:       cfg.NumSlots,
		slotSize:       cfg.SlotSize,
	}
	return conn, ci, nil
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
// Not goroutine-safe for the same slotIdx.
func (c *RDMAConn) WritePacket(slotIdx int, p *proto.Packet) error {
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
func (c *RDMAConn) WriteData(slotIdx int, data []byte) error {
	if len(data) > c.slotSize {
		return fmt.Errorf("rdma: WriteData: data %d > slotSize %d", len(data), c.slotSize)
	}
	scratch := c.SendScratchBytes(slotIdx)
	copy(scratch[:len(data)], data)

	seq := c.nextSendSeq[slotIdx] + 1
	c.nextSendSeq[slotIdx] = seq

	return c.writeSlotAndDoorbell(slotIdx, scratch[:len(data)], seq)
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

// Close tears down the RDMA connection and frees all resources. Idempotent.
func (c *RDMAConn) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	for _, m := range []*RDMAMem{c.recvRing, c.recvDB, c.sendScratch, c.sendDB} {
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

// RDMAListener wraps a RDMA CM listener. Create via Listen().
type RDMAListener struct {
	evCh   *C.struct_rdma_event_channel
	id     *C.struct_rdma_cm_id
	cfg    RDMAConnConfig
	closed int32
}

// Listen creates a RDMA listener bound to port on all interfaces.
func Listen(port int, cfg RDMAConnConfig) (*RDMAListener, error) {
	if cfg.NumSlots <= 0 || cfg.NumSlots > maxSlots {
		return nil, fmt.Errorf("rdma: Listen: NumSlots %d out of range [1,%d]", cfg.NumSlots, maxSlots)
	}
	if cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma: Listen: SlotSize must be positive")
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
