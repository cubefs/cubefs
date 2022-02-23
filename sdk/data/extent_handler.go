// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package data

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/tracing"
)

// State machines
const (
	ExtentStatusOpen int32 = iota
	ExtentStatusClosed
	ExtentStatusRecovery
	ExtentStatusError
)

const ConnectUpdateSecond = 30

var (
	gExtentHandlerID = uint64(0)
	gMaxExtentSize   = util.ExtentSize
)

// GetExtentHandlerID returns the extent handler ID.
func GetExtentHandlerID() uint64 {
	return atomic.AddUint64(&gExtentHandlerID, 1)
}

// ExtentHandler defines the struct of the extent handler.
type ExtentHandler struct {
	// Fields created as it is, i.e. will not be changed.
	stream     *Streamer
	id         uint64 // extent handler id
	inode      uint64
	fileOffset int
	storeMode  int

	// Either open/closed/recovery/error.
	// Can transit from one state to the next adjacent state ONLY.
	status int32

	// Created, filled and sent in Write.
	packet *Packet

	// Updated in *write* method ONLY.
	size         int
	extentOffset int

	// Pending packets in sender and receiver.
	// Does not involve the packet in open handler.
	inflight int32

	// For ExtentStore,the extent ID is assigned in the sender.
	// For TinyStore, the extent ID is assigned in the receiver.
	// Will not be changed once assigned.
	extID int

	// Allocated in the sender, and released in the receiver.
	// Will not be changed.
	conn *net.TCPConn
	dp   *DataPartition

	// Issue a signal to this channel when *inflight* hits zero.
	// To wake up *waitForFlush*.
	empty chan struct{}

	// Created and updated in *receiver* ONLY.
	// Not protected by lock, therefore can be used ONLY when there is no
	// pending and new packets.
	key         *proto.ExtentKey
	dirty       bool // indicate if open handler is dirty.
	isPreExtent bool // true if the extent of the key has ever been sent to metanode
	ekMutex     sync.Mutex

	// Created in receiver ONLY in recovery status.
	// Will not be changed once assigned.
	recoverHandler *ExtentHandler

	// The stream writer gets the write requests, and constructs the packets
	// to be sent to the request channel.
	// The *sender* gets the packets from the *request* channel, sends it to the corresponding data
	// node, and then throw it back to the *reply* channel.
	// The *receiver* gets the packets from the *reply* channel, waits for the
	// reply from the data node, and then deals with it.
	request chan *Packet
	reply   chan *Packet

	// Signaled in stream writer ONLY to exit *receiver*.
	doneReceiver chan struct{}

	// Signaled in receiver ONLY to exit *sender*.
	doneSender chan struct{}

	// the last time eh was used
	lastAccessTime int64
}

// NewExtentHandler returns a new extent handler.
func NewExtentHandler(stream *Streamer, offset int, storeMode int) *ExtentHandler {
	eh := &ExtentHandler{
		stream:       stream,
		id:           GetExtentHandlerID(),
		inode:        stream.inode,
		fileOffset:   offset,
		storeMode:    storeMode,
		empty:        make(chan struct{}, 1024),
		request:      make(chan *Packet, 1024),
		reply:        make(chan *Packet, 1024),
		doneSender:   make(chan struct{}),
		doneReceiver: make(chan struct{}),
	}

	go eh.receiver()
	go eh.sender()

	return eh
}

// String returns the string format of the extent handler.
func (eh *ExtentHandler) String() string {
	if eh == nil {
		return ""
	}
	return fmt.Sprintf("ExtentHandler{ID(%v)Inode(%v)FileOffset(%v)StoreMode(%v)}", eh.id, eh.inode, eh.fileOffset, eh.storeMode)
}

func (eh *ExtentHandler) write(ctx context.Context, data []byte, offset, size int, direct bool) (ek *proto.ExtentKey, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentHandler.write")
	defer tracer.Finish()
	ctx = tracer.Context()

	var total, write int

	status := eh.getStatus()
	if status >= ExtentStatusClosed {
		err = errors.New(fmt.Sprintf("ExtentHandler Write: Full or Recover, status(%v)", status))
		return
	}

	var blksize int
	if eh.storeMode == proto.TinyExtentType {
		blksize = eh.stream.tinySizeLimit()
	} else {
		blksize = util.BlockSize
	}

	// If this write request is not continuous, and cannot be merged
	// into the extent handler, just close it and return error.
	// In this case, the caller should try to create a new extent handler.
	if eh.fileOffset+eh.size != offset || eh.extentOffset+eh.size+size > eh.stream.extentSize ||
		(eh.storeMode == proto.TinyExtentType && eh.size+size > blksize) {

		err = errors.New("ExtentHandler: full or incontinuous")
		return
	}

	for total < size {
		if eh.packet == nil {
			eh.packet = NewWritePacket(context.Background(), eh.inode, offset+total, eh.storeMode, blksize)
			if direct {
				eh.packet.Opcode = proto.OpSyncWrite
			}
			//log.LogDebugf("ExtentHandler Write: NewPacket, eh(%v) packet(%v)", eh, eh.packet)
		}
		packsize := int(eh.packet.Size)
		write = util.Min(size-total, blksize-packsize)
		if write > 0 {
			copy(eh.packet.Data[packsize:packsize+write], data[total:total+write])
			eh.packet.Size += uint32(write)
			total += write
		}

		if int(eh.packet.Size) >= blksize {
			eh.flushPacket(ctx)
		}
	}

	eh.size += total

	// This is just a local cache to prepare write requests.
	// Partition and extent are not allocated.
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
			if eh.getStatus() >= ExtentStatusRecovery {
				log.LogWarnf("sender in recovery: eh(%v) packet(%v)", eh, packet)
				eh.reply <- packet
				continue
			}

			// Initialize dp, conn, and extID
			if eh.dp == nil {
				if err = eh.allocateExtent(packet.Ctx()); err != nil {
					eh.setClosed()
					eh.setRecovery()
					eh.setError()
					eh.reply <- packet
					log.LogErrorf("sender: eh(%v) err(%v)", eh, err)
					continue
				}
			}

			// For ExtentStore, calculate the extent offset.
			// For TinyStore, the extent offset is always 0 in the request packet,
			// and the reply packet tells the real extent offset.
			extOffset := int(packet.KernelOffset) - eh.fileOffset + eh.extentOffset

			// fill the packet according to the extent
			packet.PartitionID = eh.dp.PartitionID
			packet.ExtentType = uint8(eh.storeMode)
			packet.ExtentID = uint64(eh.extID)
			packet.ExtentOffset = int64(extOffset)
			packet.SetupReplArg(eh.dp.GetAllHosts(), eh.stream.client.dataWrapper.quorum)
			packet.RemainingFollowers = uint8(len(eh.dp.Hosts) - 1)
			packet.StartT = time.Now().UnixNano()

			//log.LogDebugf("ExtentHandler sender: extent allocated, eh(%v) dp(%v) extID(%v) packet(%v)", eh, eh.dp, eh.extID, packet.GetUniqueLogId())
			if err = eh.updateConn(); err != nil {
				log.LogWarnf("ExtentHandler updateConn err: (%v), eh(%v)", err, eh)
				eh.setClosed()
				eh.setRecovery()
				eh.reply <- packet
				continue
			}
			
			if err = packet.writeToConn(eh.conn, eh.stream.client.dataWrapper.connConfig.WriteTimeoutNs); err != nil {
				log.LogWarnf("sender writeTo: failed, eh(%v) err(%v) packet(%v)", eh, err, packet)
				eh.setClosed()
				eh.setRecovery()
			}
			packet.SendT = time.Now().UnixNano()
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
	var tracer = tracing.TracerFromContext(packet.Ctx()).ChildTracer("ExtentHandler.processReply").
		SetTag("reqID", packet.GetReqID()).
		SetTag("reqOp", packet.GetOpMsg()).
		SetTag("partitionID", packet.PartitionID)
	defer tracer.Finish()
	packet.SetCtx(tracer.Context())

	defer func() {
		if atomic.AddInt32(&eh.inflight, -1) <= 0 {
			eh.empty <- struct{}{}
		}
	}()

	//log.LogDebugf("processReply enter: eh(%v) packet(%v)", eh, packet.GetUniqueLogId())

	status := eh.getStatus()
	if status >= ExtentStatusError {
		eh.discardPacket(packet)
		log.LogErrorf("processReply discard packet: handler is in error status, inflight(%v) eh(%v) packet(%v)", atomic.LoadInt32(&eh.inflight), eh, packet)
		return
	} else if status >= ExtentStatusRecovery {
		errmsg := fmt.Sprintf("recover eh(%v) packet(%v)", eh, packet)
		if err := eh.recoverPacket(packet, errmsg); err != nil {
			eh.discardPacket(packet)
			log.LogErrorf("processReply discard packet: handler is in recovery status, inflight(%v) eh(%v) packet(%v) err(%v)", atomic.LoadInt32(&eh.inflight), eh, packet, err)
		}
		log.LogDebugf("processReply recover packet: handler is in recovery status, inflight(%v) from eh(%v) to recoverHandler(%v) packet(%v)", atomic.LoadInt32(&eh.inflight), eh, eh.recoverHandler, packet)
		return
	}

	cost := time.Since(time.Unix(0, packet.StartT))
	if cost > time.Second*proto.MaxPacketProcessTime {
		errmsg := fmt.Sprintf("processReply: time-out(%v) before recieve, costFromStart(%v), costFromSend(%v) "+
			"packet(%v)", time.Second*proto.MaxPacketProcessTime, cost, time.Since(time.Unix(0, packet.SendT)), packet)
		eh.processReplyError(packet, errmsg)
		return
	}
	packet.WaitT = time.Now().UnixNano()

	reply := NewReply(packet.Ctx(), packet.ReqID, packet.PartitionID, packet.ExtentID)
	readDeadLineTime := ReadTimeoutData - int(cost.Seconds())
	if readDeadLineTime < proto.MinReadDeadlineTime {
		readDeadLineTime = proto.MinReadDeadlineTime
		log.LogWarnf("processReply: send or wait cost too long, costFromStart(%v), costFromSend(%v), packet(%v)",
			cost, time.Since(time.Unix(0, packet.SendT)), packet)
	}
	// todo
	err := reply.ReadFromConnNs(eh.conn, eh.stream.client.dataWrapper.connConfig.ReadTimeoutNs)
	if err != nil {
		errmsg := fmt.Sprintf("ReadFromConn timeout(%vs) err(%v) costBeforeRecv(%v) costFromStart(%v) costFromSend(%v)",
			readDeadLineTime, err.Error(), cost, time.Since(time.Unix(0, packet.StartT)),
			time.Since(time.Unix(0, packet.SendT)))
		eh.processReplyError(packet, errmsg)
		eh.dp.RecordWrite(packet.StartT, true)
		return
	}

	eh.lastAccessTime = time.Now().Unix()

	log.LogDebugf("processReply: get reply, eh(%v) packet(%v) reply(%v)", eh, packet, reply)

	if reply.ResultCode != proto.OpOk {
		errmsg := fmt.Sprintf("reply NOK: reply(%v)", reply)
		eh.processReplyError(packet, errmsg)
		eh.dp.RecordWrite(packet.StartT, true)
		return
	}

	if !packet.isValidWriteReply(reply) {
		errmsg := fmt.Sprintf("request and reply does not match: reply(%v)", reply)
		eh.processReplyError(packet, errmsg)
		eh.dp.RecordWrite(packet.StartT, true)
		return
	}

	if reply.CRC != packet.CRC {
		errmsg := fmt.Sprintf("inconsistent CRC: reqCRC(%v) replyCRC(%v) reply(%v) ", packet.CRC, reply.CRC, reply)
		eh.processReplyError(packet, errmsg)
		eh.dp.RecordWrite(packet.StartT, true)
		return
	}

	packet.RecvT = time.Now().UnixNano()

	eh.dp.RecordWrite(packet.StartT, false)

	var (
		extID, extOffset uint64
	)

	if eh.storeMode == proto.TinyExtentType {
		extID = reply.ExtentID
		extOffset = uint64(reply.ExtentOffset)
	} else {
		extID = packet.ExtentID
		extOffset = packet.KernelOffset - uint64(eh.fileOffset)
	}

	eh.ekMutex.Lock()
	if eh.key == nil {
		eh.key = &proto.ExtentKey{
			FileOffset:   uint64(eh.fileOffset),
			PartitionId:  packet.PartitionID,
			ExtentId:     extID,
			ExtentOffset: extOffset,
			Size:         packet.Size,
		}
	} else {
		eh.key.Size += packet.Size
	}

	proto.Buffers.Put(packet.Data)
	packet.Data = nil
	eh.dirty = true
	eh.ekMutex.Unlock()

	return
}

func (eh *ExtentHandler) processReplyError(packet *Packet, errmsg string) {
	eh.setClosed()
	eh.setRecovery()
	if err := eh.recoverPacket(packet, errmsg); err != nil {
		eh.discardPacket(packet)
		log.LogErrorf("processReplyError discard packet: eh(%v) packet(%v) err(%v) errmsg(%v)", eh, packet, err, errmsg)
	} else {
		log.LogWarnf("processReplyError recover packet: from eh(%v) to recoverHandler(%v) packet(%v) errmsg(%v) errCount(%v)", eh, eh.recoverHandler, packet, errmsg, packet.errCount)
	}
}

func (eh *ExtentHandler) flush(ctx context.Context) (err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentHandler.flush")
	defer tracer.Finish()
	ctx = tracer.Context()

	eh.flushPacket(ctx)
	eh.waitForFlush(ctx)

	err = eh.appendExtentKey(ctx)
	if err != nil {
		return
	}

	if eh.storeMode == proto.TinyExtentType {
		eh.setClosed()
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
		// TODO unhandled error
		if status := eh.getStatus(); status >= ExtentStatusRecovery {
			StreamConnPool.PutConnect(conn, true)
		} else {
			StreamConnPool.PutConnect(conn, false)
		}
	}
	return
}

// can ONLY be called when the handler is not open any more
func (eh *ExtentHandler) appendExtentKey(ctx context.Context) (err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentHandler.insertExtentKey")
	defer tracer.Finish()
	ctx = tracer.Context()

	eh.ekMutex.Lock()
	if eh.key == nil {
		eh.ekMutex.Unlock()
		return
	}
	ek := &proto.ExtentKey{
		FileOffset:   eh.key.FileOffset,
		PartitionId:  eh.key.PartitionId,
		ExtentId:     eh.key.ExtentId,
		ExtentOffset: eh.key.ExtentOffset,
		Size:         eh.key.Size,
		CRC:          eh.key.CRC,
	}
	dirty := eh.dirty
	eh.ekMutex.Unlock()

	log.LogDebugf("appendExtentKey enter: eh(%v) key(%v) dirty(%v)", eh, ek, dirty)

	if dirty {
		eh.stream.extents.Append(ek, true)
		err = eh.stream.client.insertExtentKey(ctx, eh.inode, *ek, eh.isPreExtent)
		if err == nil {
			eh.ekMutex.Lock()
			// If the extent has ever been sent to metanode, all following insertExtentKey requests of the
			// extent will be checked by metanode, in case of the extent been removed by other clients.
			eh.isPreExtent = true
			if ek.GetExtentKey() == eh.key.GetExtentKey() {
				eh.dirty = false
			} else {
				log.LogErrorf("appendExtentKey not consistent: eh(%v) original ek(%v), now ek(%v)", eh, ek, eh.key)
			}
			eh.ekMutex.Unlock()
		}
	} else {
		eh.stream.extents.Lock()
		eh.stream.extents.gen = 0
		eh.stream.extents.Unlock()

		getExtentsErr := eh.stream.GetExtents(ctx)
		if getExtentsErr != nil {
			log.LogWarnf("appendExtentKey getExtents failed: err(%v) eh(%v) eh.key(%v)", getExtentsErr, eh, ek)
		}
	}

	log.LogDebugf("appendExtentKey exit: eh(%v) key(%v) dirty(%v) err(%v)", eh, eh.key, eh.dirty, err)

	return
}

// This function is meaningful to be called from stream writer flush method,
// because there is no new write request.
func (eh *ExtentHandler) waitForFlush(ctx context.Context) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentHandler.waitForFlush")
	defer tracer.Finish()
	ctx = tracer.Context()

	if atomic.LoadInt32(&eh.inflight) <= 0 {
		return
	}

	//	t := time.NewTicker(10 * time.Second)
	//	defer t.Stop()

	for {
		select {
		case <-eh.empty:
			if atomic.LoadInt32(&eh.inflight) <= 0 {
				return
			}
			//		case <-t.C:
			//			if atomic.LoadInt32(&eh.inflight) <= 0 {
			//				return
			//			}
		}
	}
}

func (eh *ExtentHandler) recoverPacket(packet *Packet, errmsg string) error {
	packet.errCount++
	if packet.errCount%50 == 0 {
		log.LogWarnf("recoverPacket: try (%v)th times because of failing to write to extent, eh(%v) packet(%v)", packet.errCount, eh, packet)
		umpMsg := fmt.Sprintf("append write recoverPacket err(%v) eh(%v) packet(%v) try count(%v)", errmsg, eh, packet, packet.errCount)
		handleUmpAlarm(eh.stream.client.dataWrapper.clusterName, eh.stream.client.dataWrapper.volName, "recoverPacket", umpMsg)
		time.Sleep(1 * time.Second)
	}

	handler := eh.recoverHandler
	if handler == nil {
		// Always use normal extent store mode for recovery.
		// Because tiny extent files are limited, tiny store
		// failures might due to lack of tiny extent file.
		handler = NewExtentHandler(eh.stream, int(packet.KernelOffset), proto.NormalExtentType)
		handler.setClosed()
	}
	handler.pushToRequest(packet.Ctx(), packet)
	if eh.recoverHandler == nil {
		eh.recoverHandler = handler
		// Note: put it to dirty list after packet is sent, so this
		// handler is not skipped in flush.
		eh.stream.dirtylist.Put(handler)
	}
	return nil
}

func (eh *ExtentHandler) discardPacket(packet *Packet) {
	proto.Buffers.Put(packet.Data)
	packet.Data = nil
	eh.setError()
}

func (eh *ExtentHandler) allocateExtent(ctx context.Context) (err error) {

	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentHandler.allocateExtent")
	defer tracer.Finish()
	ctx = tracer.Context()

	var (
		dp    *DataPartition
		conn  *net.TCPConn
		extID int
	)

	//log.LogDebugf("ExtentHandler allocateExtent enter: eh(%v)", eh)

	exclude := make(map[string]struct{})
	loopCount := 0

	// loop for creating extent until successfully
	for {
		loopCount++
		if loopCount%50 == 0 {
			log.LogWarnf("allocateExtent: try (%v)th times because of failing to create extent, eh(%v) exclude(%v)", loopCount, eh, exclude)
			umpMsg := fmt.Sprintf("create extent failed, eh(%v), try count(%v)", eh, loopCount)
			handleUmpAlarm(eh.stream.client.dataWrapper.clusterName, eh.stream.client.dataWrapper.volName, "allocateExtent", umpMsg)
		}
		if dp, err = eh.stream.client.dataWrapper.GetDataPartitionForWrite(exclude); err != nil {
			log.LogWarnf("allocateExtent: failed to get write data partition, eh(%v) exclude(%v) err(%v)", eh, exclude,
				err)
			if len(exclude) > 0 {
				// if all dp is excluded, clean exclude map
				exclude = make(map[string]struct{})
				log.LogWarnf("allocateExtent: clean exclude because of no writable partition, eh(%v) exclude(%v)", eh, exclude)
			}
			time.Sleep(5 * time.Second)
			continue
		}

		extID = 0
		if eh.storeMode == proto.NormalExtentType {
			extID, err = eh.ehCreateExtent(ctx, dp)
		}
		if err != nil {
			log.LogWarnf("allocateExtent: create extent failed, dp(%v) eh(%v) err(%v) exclude(%v)", dp, eh, err, exclude)
			dp.CheckAllHostsIsAvail(exclude)
			if isExcluded(dp, exclude, dp.ClientWrapper.quorum) {
				eh.stream.client.dataWrapper.RemoveDataPartitionForWrite(dp.PartitionID)
			}
			continue
		}

		if conn, err = StreamConnPool.GetConnect(dp.Hosts[0]); err != nil {
			log.LogWarnf("allocateExtent: failed to create connection, eh(%v) err(%v) dp(%v) exclude(%v)",
				eh, err, dp, exclude)
			// If storeMode is tinyExtentType and can't create connection, we also check host status.
			dp.CheckAllHostsIsAvail(exclude)
			continue
		}

		// success
		eh.dp = dp
		eh.conn = conn
		eh.extID = extID

		//log.LogDebugf("ExtentHandler allocateExtent exit: eh(%v) dp(%v) extID(%v)", eh, dp, extID)
		return nil
	}
}

//func (eh *ExtentHandler) createConnection(dp *DataPartition) (*net.TCPConn, error) {
//	conn, err := net.DialTimeout("tcp", dp.Hosts[0], time.Second)
//	if err != nil {
//		return nil, err
//	}
//	connect := conn.(*net.TCPConn)
//	// TODO unhandled error
//	connect.SetKeepAlive(true)
//	connect.SetNoDelay(true)
//	return connect, nil
//}

func (eh *ExtentHandler) ehCreateExtent(ctx context.Context, dp *DataPartition) (extID int, err error) {
	return CreateExtent(ctx, eh.inode, dp, eh.stream.client.dataWrapper.quorum)
}

func CreateExtent(ctx context.Context, inode uint64, dp *DataPartition, quorum int) (extID int, err error) {
	conn, err := StreamConnPool.GetConnect(dp.Hosts[0])
	if err != nil {
		errors.Trace(err, "createExtent: failed to create connection, dataPartitionHost(%v)", dp.Hosts[0])
		return
	}

	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()

	p := NewCreateExtentPacket(ctx, dp, quorum, inode)
	if err = p.WriteToConnNs(conn, dp.ClientWrapper.connConfig.WriteTimeoutNs); err != nil {
		errors.Trace(err, "createExtent: failed to WriteToConn, packet(%v) datapartionHosts(%v)", p, dp.Hosts[0])
		return
	}

	if err = p.ReadFromConnNs(conn, dp.ClientWrapper.connConfig.ReadTimeoutNs); err != nil {
		err = errors.Trace(err, "createExtent: failed to ReadFromConn, packet(%v) datapartionHosts(%v)", p, dp.Hosts[0])
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("createExtent: ResultCode NOK, packet(%v) quorum(%v) dataPartitionHost(%v) ResultCode(%v)",
			p, quorum, dp.Hosts[0], p.GetResultMsg()))
		return
	}

	extID = int(p.ExtentID)
	if extID <= 0 {
		err = errors.New(fmt.Sprintf("createExtent: illegal extID(%v) from (%v), quorum(%v)", extID, dp.Hosts[0], quorum))
		return
	}

	return extID, nil
}

// Handler lock is held by the caller.
func (eh *ExtentHandler) flushPacket(ctx context.Context) {
	if eh.packet == nil {
		return
	}
	eh.pushToRequest(ctx, eh.packet)
	eh.packet = nil
}

func (eh *ExtentHandler) pushToRequest(ctx context.Context, packet *Packet) {
	// Increase before sending the packet, because inflight is used
	// to determine if the handler has finished.
	atomic.AddInt32(&eh.inflight, 1)
	packet.SetCtx(ctx)
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
	atomic.StoreInt32(&eh.stream.status, StreamerError)
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusRecovery, ExtentStatusError)
}

func (eh *ExtentHandler) updateConn() error {
	if eh.conn == nil || eh.lastAccessTime == 0 || (time.Now().Unix() - eh.lastAccessTime) < ConnectUpdateSecond {
		return nil
	}
	log.LogInfof("updateConn: close conn(%v), eh(%v)", eh.conn.LocalAddr(), eh)
	eh.conn.Close()
	conn, err := StreamConnPool.GetConnect(eh.dp.Hosts[0])
	if err != nil {
		return err
	}
	eh.conn = conn
	return nil
}

func SetNormalExtentSize(maxSize int) {
	gMaxExtentSize = maxSize
}
