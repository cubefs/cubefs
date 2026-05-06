//go:build linux && rdma

package datanode

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/rdma"
)

const defaultSpinThreshold = 10000

// RDMAServerConfig configures the DataNode RDMA listener.
type RDMAServerConfig struct {
	Port          int
	NumSlots      int
	SlotSize      int
	SpinThreshold int
}

// connState holds per-connection server-side state.
type connState struct {
	conn    *rdma.RDMAConn
	lastSeq []uint32 // per-slot last-seen doorbell seq
}

// DataNodeRDMACtx manages the DataNode RDMA receive loop.
// Created by NewDataNodeRDMACtx; started by Start; stopped by Stop.
type DataNodeRDMACtx struct {
	cfg          RDMAServerConfig
	listener     *rdma.RDMAListener
	handlePacket func(p *repl.Packet, c net.Conn) error
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewDataNodeRDMACtx creates a context and binds the RDMA listener on cfg.Port.
// handlePacket is called for each received packet (runs in the polling goroutine).
func NewDataNodeRDMACtx(cfg RDMAServerConfig, handlePacket func(*repl.Packet, net.Conn) error) (*DataNodeRDMACtx, error) {
	if cfg.NumSlots <= 0 {
		cfg.NumSlots = 256
	}
	if cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma server: SlotSize must be positive")
	}
	if cfg.SpinThreshold <= 0 {
		cfg.SpinThreshold = defaultSpinThreshold
	}
	connCfg := rdma.RDMAConnConfig{
		NumSlots: cfg.NumSlots,
		SlotSize: cfg.SlotSize,
	}
	listener, err := rdma.Listen(cfg.Port, connCfg)
	if err != nil {
		return nil, fmt.Errorf("rdma server: listen port %d: %w", cfg.Port, err)
	}
	return &DataNodeRDMACtx{
		cfg:          cfg,
		listener:     listener,
		handlePacket: handlePacket,
		stopCh:       make(chan struct{}),
	}, nil
}

// Start launches the accept loop goroutine. Non-blocking.
func (ctx *DataNodeRDMACtx) Start() error {
	ctx.wg.Add(1)
	go ctx.acceptLoop()
	return nil
}

// Stop closes the listener and waits for all per-connection goroutines to exit.
func (ctx *DataNodeRDMACtx) Stop() {
	close(ctx.stopCh)
	ctx.listener.Close()
	ctx.wg.Wait()
}

func (ctx *DataNodeRDMACtx) acceptLoop() {
	defer ctx.wg.Done()
	for {
		conn, err := ctx.listener.Accept()
		if err != nil {
			select {
			case <-ctx.stopCh:
				return
			default:
				log.LogErrorf("rdma acceptLoop: %v", err)
				return
			}
		}
		cs := &connState{
			conn:    conn,
			lastSeq: make([]uint32, conn.NumSlots()),
		}
		ctx.wg.Add(1)
		go func() {
			defer ctx.wg.Done()
			defer conn.Close()
			cs.pollLoop(ctx)
		}()
	}
}

// pollLoop spins over all slots of one connection.
// Hybrid strategy: busy-spin for SpinThreshold iterations, then yield.
func (cs *connState) pollLoop(ctx *DataNodeRDMACtx) {
	spin := 0
	for {
		select {
		case <-ctx.stopCh:
			return
		default:
		}
		if cs.conn.IsClosed() {
			return
		}

		found := false
		for slotIdx := 0; slotIdx < cs.conn.NumSlots(); slotIdx++ {
			seq, ok := cs.conn.PollRecvDoorbell(slotIdx, cs.lastSeq[slotIdx])
			if !ok {
				continue
			}
			cs.lastSeq[slotIdx] = seq
			cs.handleSlot(ctx, slotIdx)
			found = true
		}

		if found {
			spin = 0
		} else {
			spin++
			if spin >= ctx.cfg.SpinThreshold {
				spin = 0
				runtime.Gosched()
			}
		}
	}
}

// handleSlot deserializes one slot, dispatches to handlePacket, and writes the response.
func (cs *connState) handleSlot(ctx *DataNodeRDMACtx, slotIdx int) {
	protoPkt, err := rdma.DeserializePacket(cs.conn.RecvSlotBytes(slotIdx))
	if err != nil {
		log.LogErrorf("rdma handleSlot slot=%d: DeserializePacket: %v", slotIdx, err)
		return
	}

	replPkt := repl.NewPacket()
	replPkt.Packet = *protoPkt
	replPkt.NeedReply = true

	fakeC := &rdmaFakeConn{addr: rdmaNetAddr(cs.conn.RemoteAddr())}
	if err = ctx.handlePacket(replPkt, fakeC); err != nil {
		log.LogErrorf("rdma handleSlot slot=%d: handlePacket: %v", slotIdx, err)
	}

	if err = cs.conn.WritePacket(slotIdx, &replPkt.Packet); err != nil {
		log.LogErrorf("rdma handleSlot slot=%d: WritePacket response: %v", slotIdx, err)
	}
}

// rdmaNetAddr implements net.Addr for RDMA remote addresses.
type rdmaNetAddr string

func (a rdmaNetAddr) Network() string { return "rdma" }
func (a rdmaNetAddr) String() string  { return string(a) }

// rdmaFakeConn is a minimal net.Conn stub passed to OperatePacket for logging.
// Write and Read always fail — write-type handlers don't use the connection.
type rdmaFakeConn struct {
	addr rdmaNetAddr
}

func (c *rdmaFakeConn) Read(_ []byte) (int, error)         { return 0, io.ErrClosedPipe }
func (c *rdmaFakeConn) Write(_ []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *rdmaFakeConn) Close() error                       { return nil }
func (c *rdmaFakeConn) LocalAddr() net.Addr                { return rdmaNetAddr("") }
func (c *rdmaFakeConn) RemoteAddr() net.Addr               { return c.addr }
func (c *rdmaFakeConn) SetDeadline(_ time.Time) error      { return nil }
func (c *rdmaFakeConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *rdmaFakeConn) SetWriteDeadline(_ time.Time) error { return nil }
