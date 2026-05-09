//go:build linux && rdma

package datanode

import (
	"context"
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

// RDMAServerConfig configures the DataNode RDMA listener.
type RDMAServerConfig struct {
	Port     int
	NumSlots int
	SlotSize int
	// Poll governs busy → yield → sleep behaviour of every per-conn poll
	// loop. Zero value means "use rdma.DefaultPollConfig".
	Poll rdma.PollConfig
	// Role labels accepted conns for Prometheus metrics; defaults to
	// rdma.RoleServer when set explicitly by the caller.
	Role string
}

// connState holds per-connection server-side state.
type connState struct {
	conn    *rdma.RDMAConn
	lastSeq []uint32 // per-slot last-seen doorbell seq
}

// DataNodeRDMACtx manages the DataNode RDMA receive loop.
type DataNodeRDMACtx struct {
	cfg          RDMAServerConfig
	listener     *rdma.RDMAListener
	handlePacket func(p *repl.Packet, c net.Conn) error
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewDataNodeRDMACtx creates a context and binds the RDMA listener on cfg.Port.
func NewDataNodeRDMACtx(cfg RDMAServerConfig, handlePacket func(*repl.Packet, net.Conn) error) (*DataNodeRDMACtx, error) {
	if cfg.NumSlots <= 0 {
		cfg.NumSlots = 256
	}
	if cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma server: SlotSize must be positive")
	}
	connCfg := rdma.RDMAConnConfig{
		NumSlots: cfg.NumSlots,
		SlotSize: cfg.SlotSize,
		Poll:     cfg.Poll,
		Role:     cfg.Role,
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

// pollLoop scans every slot's doorbell entry and dispatches handleSlot
// goroutines for new arrivals. Adaptive busy/yield/sleep — sleep blocks on
// the connection's recv-signal cond, woken by the drainer goroutine when
// any incoming WRITE_WITH_IMM doorbell arrives.
//
// handleSlot is dispatched in its own goroutine so a slow handler does not
// block other concurrent slots; this is what enables P1's pipeline
// throughput on the server side.
func (cs *connState) pollLoop(ctx *DataNodeRDMACtx) {
	poller := rdma.NewAdaptivePoller(cs.conn.PollConfig())
	for {
		select {
		case <-ctx.stopCh:
			return
		default:
		}
		if cs.conn.IsClosed() {
			return
		}

		signalBefore := cs.conn.RecvSignalSeq()

		found := false
		for slotIdx := 0; slotIdx < cs.conn.NumSlots(); slotIdx++ {
			seq, ok := cs.conn.PollRecvDoorbell(slotIdx, cs.lastSeq[slotIdx])
			if !ok {
				continue
			}
			cs.lastSeq[slotIdx] = seq
			slot := slotIdx // capture for goroutine
			go cs.handleSlot(ctx, slot)
			found = true
		}

		if found {
			poller.Reset()
			continue
		}

		switch poller.NextAction() {
		case rdma.ActionContinue:
			rdma.MetricsIncPollSpin(rdma.RoleServer, cs.conn.RemoteAddr(), "busy")
		case rdma.ActionYield:
			rdma.MetricsIncPollSpin(rdma.RoleServer, cs.conn.RemoteAddr(), "yield")
			runtime.Gosched()
		case rdma.ActionSleep:
			rdma.MetricsIncPollSpin(rdma.RoleServer, cs.conn.RemoteAddr(), "sleep")
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := cs.conn.WaitRecvSignal(waitCtx, signalBefore)
			cancel()
			if err != nil && err != context.DeadlineExceeded && !cs.conn.IsClosed() {
				log.LogWarnf("rdma pollLoop: WaitRecvSignal: %v", err)
				runtime.Gosched()
			}
			poller.Reset()
		}
	}
}

// handleSlot deserializes one slot, returns its credit, runs the handler,
// and writes the response fire-and-forget. Runs in its own goroutine
// dispatched by pollLoop so multiple slots can be processed concurrently.
//
// Per the P0 flow-control contract, the receive slot is reusable as soon
// as DeserializePacket returns (Arg/Data are copied into the
// proto.Packet); we ReturnCredit immediately so a slow handler does not
// stall the peer's credit pool.
func (cs *connState) handleSlot(ctx *DataNodeRDMACtx, slotIdx int) {
	protoPkt, err := rdma.DeserializePacket(cs.conn.RecvSlotBytes(slotIdx))
	if err != nil {
		log.LogErrorf("rdma handleSlot slot=%d: DeserializePacket: %v", slotIdx, err)
		if rerr := cs.conn.ReturnCredit(slotIdx); rerr != nil {
			log.LogErrorf("rdma handleSlot slot=%d: ReturnCredit after parse error: %v", slotIdx, rerr)
		}
		return
	}
	if rerr := cs.conn.ReturnCredit(slotIdx); rerr != nil {
		log.LogErrorf("rdma handleSlot slot=%d: ReturnCredit: %v", slotIdx, rerr)
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
