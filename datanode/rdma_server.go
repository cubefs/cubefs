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
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
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

// DataNodeRDMACtx manages the DataNode RDMA receive loop. It holds a
// reference to the *DataNode so handlers can call Prepare / OperatePacket
// / partition lookup directly without passing a callback per dispatch.
type DataNodeRDMACtx struct {
	cfg      RDMAServerConfig
	listener *rdma.RDMAListener
	node     *DataNode
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewDataNodeRDMACtx creates a context bound to node and binds the RDMA
// listener on cfg.Port. The DataNode reference replaces the previous
// handlePacket callback so the dispatch logic can split into separate
// write / read paths (P4b).
func NewDataNodeRDMACtx(cfg RDMAServerConfig, node *DataNode) (*DataNodeRDMACtx, error) {
	if cfg.NumSlots <= 0 {
		cfg.NumSlots = 256
	}
	if cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma server: SlotSize must be positive")
	}
	if node == nil {
		return nil, fmt.Errorf("rdma server: nil DataNode")
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
		cfg:      cfg,
		listener: listener,
		node:     node,
		stopCh:   make(chan struct{}),
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

// isReadOp reports whether opcode is a streaming-read opcode that the
// RDMA dispatch path must handle via the single-shot handleReadSlot
// instead of the streaming TCP-style OperatePacket dispatch.
//
// The TCP read path streams multiple response packets per request via
// net.Conn.Write (which now panics on rdmaFakeConn). For RDMA we issue
// one slot per chunk with a single response packet — SDK-side chunking
// at BlockSize keeps each request within the slot capacity.
func isReadOp(op uint8) bool {
	switch op {
	case proto.OpStreamRead,
		proto.OpRead,
		proto.OpStreamFollowerRead,
		proto.OpExtentRepairRead,
		proto.OpBackupRead:
		return true
	}
	return false
}

// handleSlot deserializes one slot, returns its credit, dispatches the
// packet by opcode (read vs other), and writes the response fire-and-
// forget.
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

	if isReadOp(replPkt.Opcode) {
		if cs.handleReadSlot(ctx, replPkt, slotIdx) {
			// handleReadSlot already posted the response via the
			// zero-copy path; skip the trailing WritePacket below.
			return
		}
	} else {
		// Write / control path: existing OperatePacket dispatch with
		// fakeConn (Write panics if a handler tries to stream).
		fakeC := &rdmaFakeConn{addr: rdmaNetAddr(cs.conn.RemoteAddr())}
		if err = ctx.node.Prepare(replPkt); err != nil {
			log.LogErrorf("rdma handleSlot slot=%d: Prepare: %v", slotIdx, err)
		} else if err = ctx.node.OperatePacket(replPkt, fakeC); err != nil {
			log.LogErrorf("rdma handleSlot slot=%d: OperatePacket: %v", slotIdx, err)
		}
	}

	if err = cs.conn.WritePacket(slotIdx, &replPkt.Packet); err != nil {
		log.LogErrorf("rdma handleSlot slot=%d: WritePacket response: %v", slotIdx, err)
	}
}

// maxRDMAReadDataPerSlot is the largest data payload a single-shot RDMA
// read response can carry. Computed conservatively from SlotSize minus
// the framing overhead used by SerializePacket. Reads larger than this
// must be chunked by the SDK; otherwise we reply with OpAgain so the
// SDK can fall back to TCP for that request.
//
// SlotSize - SlotHeader (16) - max packet header (69) = available data.
// In practice SDK always chunks at util.BlockSize (128 KB) which fits
// comfortably under the default 132 KB SlotSize.
func (ctx *DataNodeRDMACtx) maxRDMAReadDataPerSlot() int {
	return ctx.cfg.SlotSize - rdma.SlotHeaderSize - rdma.MaxPacketHeaderSize
}

// handleReadSlot performs a single-shot disk read and ships the result
// over RDMA. On the success path it stages the response packet
// (PacketHeader + data) directly into sendScratch and posts via
// WriteSlotZeroCopy — saving one memcpy of the response data per
// round trip (P5).
//
// Returns true if the response has already been written; the caller
// should NOT then invoke its trailing WritePacket. Returns false on
// error paths so the caller can ship the error reply via the standard
// SerializePacket-based WritePacket flow.
//
// Failure modes that should make the SDK fall back to TCP:
//   - p.Size > maxRDMAReadDataPerSlot: response too big for one slot;
//     reply with OpAgain (SDK's checkStreamReply turns this into a
//     retry, which the SDK upgrades to a TCP path on its side).
//   - extent not found / disk error: reply with the original packet's
//     PackErrorBody and a non-OpOk ResultCode; the SDK distinguishes
//     transport failures from read errors via ResultCode.
func (cs *connState) handleReadSlot(ctx *DataNodeRDMACtx, p *repl.Packet, slotIdx int) (handled bool) {
	if int(p.Size) > ctx.maxRDMAReadDataPerSlot() {
		log.LogWarnf("rdma handleReadSlot: requested size %d exceeds slot capacity %d, asking SDK to fall back",
			p.Size, ctx.maxRDMAReadDataPerSlot())
		p.ResultCode = proto.OpAgain
		return false
	}

	if err := ctx.node.Prepare(p); err != nil {
		log.LogErrorf("rdma handleReadSlot: Prepare: %v", err)
		p.PackErrorBody(repl.ActionPreparePkt, err.Error())
		return false
	}

	partition, ok := p.Object.(*DataPartition)
	if !ok || partition == nil {
		p.PackErrorBody("rdma_read_slot", "partition object missing")
		return false
	}

	// Compute the data offset inside sendScratch and read directly there.
	// SerializePacket lays out: SlotHeader (16) + PacketHeader (57 or 69)
	// + Arg + Data. For read responses ArgLen is 0; layout is the same
	// shape as SerializePacket's output so the SDK's DeserializePacket
	// reads it back correctly.
	scratch := cs.conn.SendScratchBytes(slotIdx)
	hdrSize := p.CalcPacketHeaderSize()
	dataOff := rdma.SlotHeaderSize + hdrSize + int(p.ArgLen)
	totalLen := dataOff + int(p.Size)
	if totalLen > cs.conn.SlotSize() {
		// Defensive: should have been caught by the size check above.
		p.ResultCode = proto.OpAgain
		return false
	}

	dataSlice := scratch[dataOff : dataOff+int(p.Size)]
	store := partition.ExtentStore()
	isBackup := p.Opcode == proto.OpBackupRead
	crc, err := store.Read(p.ExtentID, p.ExtentOffset, int64(p.Size), dataSlice, false /* isRepairRead */, isBackup)
	if err != nil {
		log.LogWarnf("rdma handleReadSlot: store.Read dp=%d ext=%d off=%d size=%d: %v",
			p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, err)
		switch {
		case err == storage.LimitedIoError:
			p.ResultCode = proto.OpLimitedIoErr
		default:
			p.PackErrorBody("rdma_read_slot", err.Error())
		}
		return false
	}

	// Disk data is already in sendScratch[dataOff..]. Stamp the packet
	// header in place at sendScratch[SlotHeaderSize..], then post.
	p.CRC = crc
	p.ResultCode = proto.OpOk
	p.MarshalHeader(scratch[rdma.SlotHeaderSize : rdma.SlotHeaderSize+hdrSize])
	// Arg (if any) would go between PacketHeader and Data; for reads
	// ArgLen is 0 so nothing to stamp here.

	if err := cs.conn.WriteSlotZeroCopy(slotIdx, totalLen); err != nil {
		log.LogErrorf("rdma handleReadSlot: WriteSlotZeroCopy: %v", err)
		// The conn is likely broken; let caller fall through to WritePacket
		// which will probably fail too — at least the error is logged twice
		// from different layers, surfacing the failure mode clearly.
		return false
	}
	return true
}

// rdmaNetAddr implements net.Addr for RDMA remote addresses.
type rdmaNetAddr string

func (a rdmaNetAddr) Network() string { return "rdma" }
func (a rdmaNetAddr) String() string  { return string(a) }

// rdmaFakeConn is a minimal net.Conn stub passed to OperatePacket. The
// RDMA dispatch path expects handlers to mutate the response packet
// in-place (handleSlot's trailing WritePacket sends it back); they must
// NOT invoke any streaming write directly on the conn. We make Write
// panic so any latent handler that tries to streamWrite a TCP-style
// response surfaces immediately as a runtime error rather than silently
// losing bytes (P4a of docs/plan/rdma-optimization-spec.md).
type rdmaFakeConn struct {
	addr rdmaNetAddr
}

func (c *rdmaFakeConn) Read(_ []byte) (int, error) { return 0, io.ErrClosedPipe }
func (c *rdmaFakeConn) Write(b []byte) (int, error) {
	panic(fmt.Sprintf("rdma: handler invoked net.Conn.Write on RDMA fakeConn (%d bytes to %s); "+
		"RDMA dispatch expects handlers to mutate the response packet in-place. "+
		"This indicates a read-style handler running on the RDMA path — route reads through handleReadSlot instead.",
		len(b), c.addr))
}
func (c *rdmaFakeConn) Close() error                       { return nil }
func (c *rdmaFakeConn) LocalAddr() net.Addr                { return rdmaNetAddr("") }
func (c *rdmaFakeConn) RemoteAddr() net.Addr               { return c.addr }
func (c *rdmaFakeConn) SetDeadline(_ time.Time) error      { return nil }
func (c *rdmaFakeConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *rdmaFakeConn) SetWriteDeadline(_ time.Time) error { return nil }
