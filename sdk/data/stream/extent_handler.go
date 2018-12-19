package stream

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

// state machines
const (
	ExtentStatusOpen int32 = iota
	ExtentStatusClosed
	ExtentStatusRecovery
	ExtentStatusError
)

const (
	MaxPacketErrorCount = 8
)

var (
	GlobalHandlerID = uint64(0)
)

func GetGlobalHandlerID() uint64 {
	return atomic.AddUint64(&GlobalHandlerID, 1)
}

type ExtentHandler struct {
	// Created as initial value, and will not be changed.
	sw         *StreamWriter
	id         uint64
	inode      uint64
	fileOffset int

	// Status is open, closed, recovery, error,
	// and can be changed only from one state to the next adjacent state.
	status int32

	// Created, filled and sent in Write.
	packet *Packet

	// Updated in Write
	size int

	// Increament in Write, and decreament in receiver.
	inflight int32 // pending requests

	// Assigned in Write
	storeMode int

	// Assigned in sender, and will not be changed.
	extID int

	// Initialized and increased in sender.
	extOffset int

	// Allocated in sender, released in receiver and will not be changed.
	conn *net.TCPConn
	dp   *wrapper.DataPartition

	empty chan struct{}

	// Created and updated in receiver, and will be updated to meta node when the handler is finished.
	key   *proto.ExtentKey
	dirty bool

	// Created in receiver
	recoverHandler *ExtentHandler

	// Marked error in receiver if packet reaches maximum recovery times,
	// and all successive packets would be discarded.

	request chan *Packet
	reply   chan *Packet

	// Signaled by stream writer only.
	doneReceiver chan struct{}

	// Signaled by receiver only.
	doneSender chan struct{}
	finished   chan struct{}

	lock sync.RWMutex
}

// Only stream writer can close extent handlers
func NewExtentHandler(sw *StreamWriter, offset int, storeMode int) *ExtentHandler {
	eh := &ExtentHandler{
		sw:           sw,
		id:           GetGlobalHandlerID(),
		inode:        sw.inode,
		fileOffset:   offset,
		storeMode:    storeMode,
		empty:        make(chan struct{}, 1024),
		request:      make(chan *Packet, 10240),
		reply:        make(chan *Packet, 10240),
		doneSender:   make(chan struct{}),
		doneReceiver: make(chan struct{}),
	}

	go eh.receiver()
	go eh.sender()

	return eh
}

func (eh *ExtentHandler) String() string {
	return fmt.Sprintf("ExtentHandler{ID(%v)Inode(%v)FileOffset(%v)}", eh.id, eh.inode, eh.fileOffset)
}

func (eh *ExtentHandler) Write(data []byte, offset, size int) (ek *proto.ExtentKey, err error) {
	var total, write int

	status := eh.getStatus()
	if status >= ExtentStatusClosed {
		err = errors.New(fmt.Sprintf("ExtentHandler Write: Full or Recover, status(%v)", status))
		return
	}

	// If this write request is incontinuous cannot be merged
	// into this extent handler, just close it and return error.
	// And the caller shall try to create a new extent handler.
	if eh.fileOffset+eh.size != offset || eh.size+size > util.ExtentSize {
		err = errors.New("ExtentHandler: full or incontinuous")
		return
	}

	for total < size {
		if eh.packet == nil {
			eh.packet = NewPacket(eh.inode, offset+total)
			//log.LogDebugf("Handler Write: NewPacket, eh(%v) fileOffset(%v)", eh, offset+total)
		}
		packsize := int(eh.packet.Size)
		write = util.Min(size-total, util.BlockSize-packsize)
		if write > 0 {
			copy(eh.packet.Data[packsize:packsize+write], data[total:total+write])
			eh.packet.Size += uint32(write)
			total += write
		}

		if eh.packet.Size >= util.BlockSize {
			eh.flushPacket()
		}
	}

	eh.size += total

	// This is just a local cache to prepare write requests.
	// Partition and extent are not allocated yet.
	ek = &proto.ExtentKey{
		FileOffset: uint64(eh.fileOffset),
		Size:       uint32(eh.size),
	}
	return ek, nil
}

func (eh *ExtentHandler) sender() {
	var err error

	//	t := time.NewTicker(5 * time.Second)
	//	defer t.Stop()

	for {
		select {
		//		case <-t.C:
		//			log.LogDebugf("sender alive: eh(%v) inflight(%v)", eh, atomic.LoadInt32(&eh.inflight))
		case packet := <-eh.request:
			//log.LogDebugf("ExtentHandler sender begin: eh(%v) packet(%v)", eh, packet.GetUniqueLogId())
			// If handler is in recovery or error status,
			// just forward the packet to the reply channel directly.
			if eh.getStatus() >= ExtentStatusRecovery {
				log.LogWarnf("sender in recovery: eh(%v) packet(%v)", eh, packet)
				eh.reply <- packet
				continue
			}

			// Initialize dp, conn, and extID
			if eh.dp == nil {
				if err = eh.allocateExtent(); err != nil {
					log.LogWarnf("sender allocateExtent: failed, eh(%v) err(%v)", eh, err)
					eh.setClosed()
					eh.setRecovery()
					eh.reply <- packet
					continue
				}
			}

			// calculate extent offset
			eh.extOffset = packet.kernelOffset - eh.fileOffset

			// fill packet according to extent
			packet.PartitionID = eh.dp.PartitionID
			packet.ExtentMode = uint8(eh.storeMode)
			packet.ExtentID = uint64(eh.extID)
			packet.ExtentOffset = int64(eh.extOffset)
			packet.Arg = ([]byte)(eh.dp.GetAllAddrs())
			packet.Arglen = uint32(len(packet.Arg))
			packet.RemainReplicates = uint8(len(eh.dp.Hosts) - 1)

			//log.LogDebugf("ExtentHandler sender: extent allocated, eh(%v) dp(%v) extID(%v) packet(%v)", eh, eh.dp, eh.extID, packet.GetUniqueLogId())

			// send to reply channel
			if err = packet.writeTo(eh.conn); err != nil {
				log.LogWarnf("sender writeTo: failed, eh(%v) err(%v) packet(%v)", eh, err, packet)
				eh.setClosed()
				eh.setRecovery()
			}

			eh.reply <- packet
			log.LogDebugf("ExtentHandler sender: sent to the reply channel, eh(%v) packet(%v)", eh, packet)

		case <-eh.doneSender:
			eh.setClosed()
			log.LogDebugf("sender: done, eh(%v) size(%v) ek(%v)", eh, eh.size, eh.key)
			return
		}
	}
}

func (eh *ExtentHandler) receiver() {
	//	t := time.NewTicker(5 * time.Second)
	//	defer t.Stop()

	for {
		select {
		//		case <-t.C:
		//			log.LogDebugf("receiver alive: eh(%v) inflight(%v)", eh, atomic.LoadInt32(&eh.inflight))
		case packet := <-eh.reply:
			//log.LogDebugf("receiver begin: eh(%v) packet(%v)", eh, packet.GetUniqueLogId())
			eh.processReply(packet)
			//log.LogDebugf("receiver end: eh(%v) packet(%v)", eh, packet.GetUniqueLogId())
		case <-eh.doneReceiver:
			log.LogDebugf("receiver done: eh(%v) size(%v) ek(%v)", eh, eh.size, eh.key)
			return
		}
	}
}

func (eh *ExtentHandler) processReply(packet *Packet) {
	defer func() {
		if atomic.AddInt32(&eh.inflight, -1) <= 0 {
			eh.empty <- struct{}{}
		}
	}()

	//log.LogDebugf("processReply enter: eh(%v) packet(%v)", eh, packet.GetUniqueLogId())

	status := eh.getStatus()
	if status >= ExtentStatusError {
		log.LogErrorf("processReply discard packet: handler is in error status, inflight(%v) eh(%v) packet(%v)", atomic.LoadInt32(&eh.inflight), eh, packet)
		return
	} else if status >= ExtentStatusRecovery {
		if err := eh.recoverPacket(packet); err != nil {
			eh.setError()
			log.LogErrorf("processReply discard packet: handler is in recovery status, inflight(%v) eh(%v) packet(%v) err(%v)", atomic.LoadInt32(&eh.inflight), eh, packet, err)
		}
		log.LogDebugf("processReply recover packet: handler is in recovery status, inflight(%v) from eh(%v) to recoverHandler(%v) packet(%v)", atomic.LoadInt32(&eh.inflight), eh, eh.recoverHandler, packet)
		return
	}

	reply := NewReply(packet.ReqID, packet.PartitionID, packet.ExtentID)
	err := reply.ReadFromConn(eh.conn, proto.ReadDeadlineTime)
	if err != nil {
		eh.processReplyError(packet, err.Error())
		return
	}

	log.LogDebugf("processReply: get reply, eh(%v) packet(%v) reply(%v)", eh, packet, reply)

	if reply.ResultCode != proto.OpOk {
		errmsg := fmt.Sprintf("reply NOK: reply(%v)", reply)
		eh.processReplyError(packet, errmsg)
		return
	}

	if !packet.IsEqualWriteReply(reply) {
		errmsg := fmt.Sprintf("request and reply not match: reply(%v)", reply)
		eh.processReplyError(packet, errmsg)
		return
	}

	if reply.CRC != packet.CRC {
		errmsg := fmt.Sprintf("inconsistent CRC: reqCRC(%v) replyCRC(%v) reply(%v) ", packet.CRC, reply.CRC, reply)
		eh.processReplyError(packet, errmsg)
		return
	}

	// Update extent key cache
	if eh.key == nil {
		eh.key = &proto.ExtentKey{
			FileOffset:   uint64(eh.fileOffset),
			PartitionId:  packet.PartitionID,
			ExtentId:     packet.ExtentID,
			ExtentOffset: uint64(packet.kernelOffset) - uint64(eh.fileOffset),
			Size:         packet.Size,
		}
	} else {
		eh.key.Size += packet.Size
	}

	eh.dirty = true
	return
}

func (eh *ExtentHandler) processReplyError(packet *Packet, errmsg string) {
	eh.setClosed()
	eh.setRecovery()
	if packet.errCount >= MaxPacketErrorCount {
		// discard packet
		eh.setError()
		log.LogErrorf("processReplyError discard packet: packet err count reaches max limit, eh(%v) packet(%v) err(%v)", eh, packet, errmsg)
	} else {
		eh.recoverPacket(packet)
		log.LogWarnf("processReplyError recover packet: from eh(%v) to recoverHandler(%v) packet(%v) err(%v)", eh, eh.recoverHandler, packet, errmsg)
	}

}

func (eh *ExtentHandler) flush() (err error) {
	eh.flushPacket()
	eh.waitForFlush()

	err = eh.appendExtentKey()
	if err != nil {
		return
	}

	status := eh.getStatus()
	if status >= ExtentStatusError {
		err = errors.New(fmt.Sprintf("StreamWriter flush: extent handler in error status, eh(%v) size(%v)", eh, eh.size))
	}
	return
}

func (eh *ExtentHandler) cleanup() (err error) {
	eh.doneSender <- struct{}{}
	eh.doneReceiver <- struct{}{}
	if eh.conn != nil {
		conn := eh.conn
		eh.conn = nil
		conn.Close()
	}
	return
}

// can ONLY be called when handler is not open any more
func (eh *ExtentHandler) appendExtentKey() (err error) {
	//log.LogDebugf("appendExtentKey enter: eh(%v)", eh)
	if eh.key != nil && eh.dirty == true {
		eh.sw.stream.extents.Append(eh.key, true)
		//log.LogDebugf("appendExtentKey before talk to meta: eh(%v)", eh)
		err = eh.sw.stream.client.appendExtentKey(eh.inode, *eh.key)
	}
	if err == nil {
		eh.dirty = false
	}
	//log.LogDebugf("appendExtentKey exit: eh(%v)", eh)
	return
}

// This function is meaningful to be called from stream writer flush method,
// because there is no new write request.
func (eh *ExtentHandler) waitForFlush() {
	if atomic.LoadInt32(&eh.inflight) <= 0 {
		return
	}

	for {
		select {
		case <-eh.empty:
			if atomic.LoadInt32(&eh.inflight) <= 0 {
				return
			}
		}
	}
}

func (eh *ExtentHandler) recoverPacket(packet *Packet) error {
	packet.errCount++
	if packet.errCount >= MaxPacketErrorCount {
		return errors.New(fmt.Sprintf("recoverPacket failed: reach max error limit, eh(%v) packet(%v)", eh, packet))
	}

	handler := eh.recoverHandler
	if handler == nil {
		handler = NewExtentHandler(eh.sw, packet.kernelOffset, int(packet.ExtentMode))
		handler.setClosed()
	}
	handler.pushToRequest(packet)
	if eh.recoverHandler == nil {
		eh.recoverHandler = handler
		// Note: put it to dirty list after packet is sent, so this
		// handler is not skipped in flush.
		eh.sw.dirtylist.Put(handler)
	}
	return nil
}

func (eh *ExtentHandler) allocateExtent() (err error) {
	var (
		dp    *wrapper.DataPartition
		conn  *net.TCPConn
		extID int
	)

	//log.LogDebugf("ExtentHandler allocateExtent enter: eh(%v)", eh)

	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if dp, err = gDataWrapper.GetWriteDataPartition(eh.sw.excludePartition); err != nil {
			log.LogWarnf("allocateExtent: failed to get write data partition, eh(%v)", eh)
			continue
		}

		if extID, err = eh.createExtent(dp); err != nil {
			log.LogWarnf("allocateExtent: failed to create extent, eh(%v) err(%v)", eh, err)
			continue
		}

		if conn, err = eh.createConnection(dp); err != nil {
			log.LogWarnf("allocateExtent: failed to create connection, eh(%v) err(%v)", eh, err)
			continue
		}

		// success
		eh.dp = dp
		eh.conn = conn
		eh.extID = extID

		//log.LogDebugf("ExtentHandler allocateExtent exit: eh(%v) dp(%v) extID(%v)", eh, dp, extID)
		return nil
	}

	err = errors.New(fmt.Sprintf("ExtentHandler allocateExtent: failed, reach maximum retry limit, eh(%v)", eh))
	return err
}

func (eh *ExtentHandler) createConnection(dp *wrapper.DataPartition) (*net.TCPConn, error) {
	conn, err := net.DialTimeout("tcp", dp.Hosts[0], time.Second)
	if err != nil {
		return nil, err
	}
	connect := conn.(*net.TCPConn)
	connect.SetKeepAlive(true)
	connect.SetNoDelay(true)
	return connect, nil
}

func (eh *ExtentHandler) createExtent(dp *wrapper.DataPartition) (extID int, err error) {
	conn, err := eh.createConnection(dp)
	if err != nil {
		errors.Annotatef(err, "createExtent: failed to create connection, eh(%v) datapartionHosts(%v)", eh, dp.Hosts[0])
		return
	}
	defer conn.Close()

	p := NewCreateExtentPacket(dp, eh.inode)
	if err = p.WriteToConn(conn); err != nil {
		errors.Annotatef(err, "createExtent: failed to WriteToConn, packet(%v) datapartionHosts(%v)", p, dp.Hosts[0])
		return
	}

	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime*2); err != nil {
		err = errors.Annotatef(err, "createExtent: failed to ReadFromConn, packet(%v) datapartionHosts(%v)", p, dp.Hosts[0])
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("createExtent: ResultCode NOK, packet(%v) datapartionHosts(%v) ResultCode(%v)", p, dp.Hosts[0], p.GetResultMesg()))
		return
	}

	extID = int(p.ExtentID)
	if extID <= 0 {
		err = errors.New(fmt.Sprintf("createExtent: illegal extID(%v) from (%v)", extID, dp.Hosts[0]))
		return
	}

	return extID, nil
}

// Handler lock is held by the caller
func (eh *ExtentHandler) flushPacket() {
	if eh.packet == nil {
		return
	}

	eh.pushToRequest(eh.packet)
	eh.packet = nil
}

func (eh *ExtentHandler) pushToRequest(packet *Packet) {
	// Increase before sending the packet, because inflight is used
	// to determine if handler has finished.
	atomic.AddInt32(&eh.inflight, 1)
	eh.request <- packet
}

func (eh *ExtentHandler) getStatus() int32 {
	return atomic.LoadInt32(&eh.status)
}

func (eh *ExtentHandler) setClosed() bool {
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusOpen, ExtentStatusClosed)
}

func (eh *ExtentHandler) setRecovery() bool {
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusClosed, ExtentStatusRecovery)
}

func (eh *ExtentHandler) setError() bool {
	atomic.StoreInt32(&eh.sw.status, StreamWriterError)
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusRecovery, ExtentStatusError)
}
