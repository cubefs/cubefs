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

	// workChs decouples doorbell scanning (cheap, latency-sensitive)
	// from packet handling (potentially slow disk I/O). pollLoop
	// hashes slot indices to a fixed-size worker pool by `slotIdx %
	// numWorkers`; each worker drains its own channel serially. The
	// hash split is what makes the pool safe under the AppendWrite
	// ordering invariant in storage/extent.go:
	//
	//   - The SDK pool (slot_pool.go) routes same-(PartitionID, ExtentID)
	//     traffic to the same conn AND the same slot via AcquireSlotForKey.
	//   - So all writes to one extent always show up on the same slotIdx
	//     on the server.
	//   - Hash by slotIdx → same extent → same worker → serial processing
	//     within that worker, preserving offset == dataSize invariants.
	//   - Different extents may sit on different slots → different
	//     workers → parallel handleSlot/disk-IO/follower-replicate.
	//
	// Previous design ran a single worker per conn, which serialised
	// everything (reads, writes to unrelated extents, follower forwarding)
	// behind disk I/O of the slowest in-flight request. At 64 concurrent
	// clients × 16 MB this manifested as a hard ~700 MB/s write ceiling
	// and cascading 243 (IntraGroupNetErr) errors when the follower's
	// reply queue couldn't catch up.
	//
	// Worker channel capacity is sized so any one worker can absorb its
	// expected fraction of NumSlots without blocking pollLoop. We round
	// up by 1 to handle hash skew.
	workChs []chan int
	workWg  sync.WaitGroup
}

// rdmaServerWorkersPerConn is the per-connection worker pool size. Each
// worker processes its `slotIdx % N` partition of slots. The value
// trades CPU/goroutine count against in-flight parallelism; 4 is a
// reasonable default for the typical 3-replica DataNode where each
// conn carries up to NumSlots in flight (256 by default).
const rdmaServerWorkersPerConn = 4

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

// acceptLoop runs as a single goroutine per RDMA listener and accepts
// incoming RDMA connections. Transient Accept failures (RDMA CM events
// glitching, network blips) are retried with exponential backoff rather
// than terminating the loop — otherwise one transient error would
// permanently disable RDMA acceptance on the node with no recovery
// short of restarting the daemon.
func (ctx *DataNodeRDMACtx) acceptLoop() {
	defer ctx.wg.Done()

	const (
		initialBackoff = 5 * time.Millisecond
		maxBackoff     = 5 * time.Second
	)
	backoff := initialBackoff

	for {
		conn, err := ctx.listener.Accept()
		if err != nil {
			// Stop request always wins.
			select {
			case <-ctx.stopCh:
				return
			default:
			}
			log.LogWarnf("rdma acceptLoop: %v, retrying in %v", err, backoff)
			// Honour stopCh while sleeping so Stop() doesn't have to
			// wait for the full backoff before the goroutine exits.
			select {
			case <-ctx.stopCh:
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}
		// Reset backoff on a successful Accept so the next transient
		// failure starts from the short window again.
		backoff = initialBackoff

		cs := &connState{
			conn:    conn,
			lastSeq: make([]uint32, conn.NumSlots()),
			workChs: make([]chan int, rdmaServerWorkersPerConn),
		}
		perWorker := conn.NumSlots()/rdmaServerWorkersPerConn + 1
		for i := 0; i < rdmaServerWorkersPerConn; i++ {
			cs.workChs[i] = make(chan int, perWorker)
		}
		ctx.wg.Add(1)
		go func() {
			defer ctx.wg.Done()
			defer conn.Close()
			// Spawn the worker pool; pollLoop hashes slots to one of
			// these workers. Closing the channels on pollLoop exit
			// drains each worker before conn.Close releases the QP/MRs.
			for i := 0; i < rdmaServerWorkersPerConn; i++ {
				cs.workWg.Add(1)
				go cs.workerLoop(ctx, i)
			}
			cs.pollLoop(ctx)
			for _, ch := range cs.workChs {
				close(ch)
			}
			cs.workWg.Wait()
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
			// Hand off to the worker pool. Hash by slotIdx so writes
			// to the same extent (same slotIdx via SDK hash-pinning)
			// always land on the same worker → ordered. Channel is
			// sized for the worker's expected slot share so this send
			// is wait-free in steady state; if a worker is genuinely
			// behind, blocking here is the right back pressure (better
			// than racing goroutines that violate the extent.go
			// append-offset invariant).
			cs.workChs[slotIdx%rdmaServerWorkersPerConn] <- slotIdx
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

// workerLoop drains workCh serially; one worker per conn means slots
// are handled in QP arrival order (and therefore leader send order),
// which is what extent.go's append-offset invariant requires. Disk I/O
// workerLoop drains its own slot channel serially; the pool dispatch
// in pollLoop hashes (slotIdx % numWorkers) so same-extent traffic
// stays on the same worker → AppendWrite ordering preserved. Disk I/O
// inside handleSlot stays here, off the pollLoop's hot path.
func (cs *connState) workerLoop(ctx *DataNodeRDMACtx, workerID int) {
	defer cs.workWg.Done()
	for slotIdx := range cs.workChs[workerID] {
		cs.handleSlot(ctx, slotIdx)
	}
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
		// Send a minimal error reply so the client doesn't sit in
		// pollRDMAResponse for the full rdmaRoundTripTimeout (30s)
		// after a malformed slot. ReqID=0 deliberately mismatches any
		// real client request, triggering the SDK's reqid_mismatch
		// fallback path → fast TCP fallback rather than a stalled read.
		errPkt := &proto.Packet{
			Magic:      proto.ProtoMagic,
			ResultCode: proto.OpErr,
		}
		if werr := cs.conn.WritePacket(slotIdx, errPkt); werr != nil {
			log.LogErrorf("rdma handleSlot slot=%d: WritePacket error reply: %v", slotIdx, werr)
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
			// Enrich the error log with packet identity so operators
			// can correlate with the SDK-side ReqID log line and not
			// have to guess which client sent the bad packet. CRC
			// mismatch is the canonical example (see wrap_prepare.go
			// checkCrc) but other Prepare errors benefit from the
			// same context.
			log.LogErrorf("rdma handleSlot slot=%d remote=%s op=0x%x pid=%d ext=%d size=%d claimedCRC=%d reqId=%d: Prepare: %v",
				slotIdx, cs.conn.RemoteAddr(), replPkt.Opcode, replPkt.PartitionID,
				replPkt.ExtentID, replPkt.Size, replPkt.CRC, replPkt.ReqID, err)
		} else {
			// Forward to followers BEFORE applying locally, mirroring
			// the TCP path's OperatorAndForwardPktGoRoutine ordering.
			// Without this, RDMA-received writes apply only on the
			// receiving node — leaving the other replicas stale and
			// surfacing later as OpArgMismatchErr on reads after any
			// leader switch.
			//
			// On dispatch-prerequisite failure (follower RDMA not
			// enabled, Arg parse error) we abort with OpAgain so the
			// SDK falls back to TCP, where the standard replication
			// machinery handles the write. We do NOT apply locally in
			// that case — a leader-only write here is exactly the
			// inconsistency this fix is meant to prevent.
			if forwardErr := repl.PrepareRDMAReplicate(replPkt); forwardErr != nil {
				log.LogWarnf("rdma handleSlot slot=%d reqId=%d op=0x%x pid=%d: PrepareRDMAReplicate: %v, replying OpAgain",
					slotIdx, replPkt.ReqID, replPkt.Opcode, replPkt.PartitionID, forwardErr)
				replPkt.ResultCode = proto.OpAgain
			} else {
				// Local operate runs in parallel with follower processing
				// (followerRDMASend is async — it queues to a per-addr
				// goroutine pair and returns immediately).
				if err = ctx.node.OperatePacket(replPkt, fakeC); err != nil {
					log.LogErrorf("rdma handleSlot slot=%d remote=%s op=0x%x reqId=%d: OperatePacket: %v",
						slotIdx, cs.conn.RemoteAddr(), replPkt.Opcode, replPkt.ReqID, err)
				}
				// Wait for all follower responses. On follower failure,
				// override the response body so the SDK sees the same
				// OpErr it would see via the TCP path.
				if waitErr := repl.WaitForRDMAReplicate(replPkt); waitErr != nil {
					log.LogWarnf("rdma handleSlot slot=%d reqId=%d op=0x%x pid=%d: follower replicate: %v",
						slotIdx, replPkt.ReqID, replPkt.Opcode, replPkt.PartitionID, waitErr)
					replPkt.PackErrorBody(repl.ActionReceiveFromFollower, waitErr.Error())
				}
				// Cache OpOk for idempotent SDK retries. Mirrors the
				// TCP-path Post() hook — both transports must record the
				// same final state so subsequent retries (on either
				// transport) hit dedup before re-applying the write.
				if replPkt.IsNormalWriteOperation() && !proto.IsTinyExtentType(replPkt.ExtentType) && replPkt.ResultCode == proto.OpOk {
					ctx.node.writeDedup.Remember(replPkt.PartitionID, replPkt.ExtentID, replPkt.ReqID)
				}
			}
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
	// isReadOp groups all read-style opcodes together for RDMA dispatch,
	// but the storage layer behaves differently per opcode: repair reads
	// bypass the file's hot cache, backup reads target a snapshot view,
	// and stream reads are normal hot-path reads. Pass the flags
	// faithfully so RDMA-served reads match TCP-served behaviour.
	//
	// TODO: OpExtentRepairRead's TCP path also gates on
	// partition.disk.RequireReadExtentToken to throttle repair traffic;
	// the RDMA path here bypasses that gate. Currently safe because
	// repair traffic is initiated between datanodes and does NOT use
	// RDMA in this revision (datanode/repl/packet.go OpExtentRepairRead
	// senders go via TCP). If repair is migrated to RDMA in future,
	// mirror the disk-token check here.
	isRepairRead := p.Opcode == proto.OpExtentRepairRead
	isBackup := p.Opcode == proto.OpBackupRead
	crc, err := store.Read(p.ExtentID, p.ExtentOffset, int64(p.Size), dataSlice, isRepairRead, isBackup)
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
