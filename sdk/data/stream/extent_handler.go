// Copyright 2018 The CubeFS Authors.
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

package stream

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// State machines
const (
	ExtentStatusOpen int32 = iota
	ExtentStatusClosed
	ExtentStatusRecovery
	ExtentStatusError
)

const (
	maxRetryInterval = 60000 // 1min
	baseFactor       = 1.5
	maxRetryExpVal   = 26
)

var (
	enableRetryTiny            = false
	extentAllocRetryIntervalMs = 0 // ms
	extentWriteRetryIntervalMs = 0 // ms
	extentHandlerMaxRetryTime  = 0 // min
)

var gExtentHandlerID = uint64(0)

// GetExtentHandlerID returns the extent handler ID.
func GetExtentHandlerID() uint64 {
	return atomic.AddUint64(&gExtentHandlerID, 1)
}

func SetExentRetryArgs(allocInterval, writeInterval, maxRetryMin int, retryTiny bool) {
	extentAllocRetryIntervalMs = allocInterval
	extentWriteRetryIntervalMs = writeInterval
	extentHandlerMaxRetryTime = maxRetryMin
	enableRetryTiny = true
	log.LogWarnf("SetExentRetryArgs: alloc interval %d ms, write interval %d ms, max %d min retry Tiny %v",
		allocInterval, writeInterval, maxRetryMin, retryTiny)
}

func getRetryInterval(intervalMs, retry int) time.Duration {
	return getRetryIntervalTimeOut(intervalMs, retry, true)
}

func getRetryIntervalTimeOut(intervalMs, retry int, checkTimeOut bool) time.Duration {
	d1 := intervalMs

	if retry >= maxRetryExpVal { // to avoid int overflow
		d1 = maxRetryInterval
	} else if retry >= 5 { // quickly retry when retry less than 5
		d1 = intervalMs * int(math.Pow(baseFactor, float64(retry)))
	}

	if d1 > maxRetryInterval {
		d1 = maxRetryInterval
	}

	if checkTimeOut && extentHandlerMaxRetryTime > 0 && d1 >= maxRetryInterval {
		total := 0
		for i := 1; i <= retry; i++ {
			t1 := getRetryIntervalTimeOut(intervalMs, i, false)
			total += int(t1)
		}
		// once over max retry time, retry quickly
		if time.Minute*time.Duration(extentHandlerMaxRetryTime) < time.Duration(total) {
			d1 = intervalMs
		}
	}

	if d1 <= 0 {
		log.LogErrorf("getRetryIntervalTimeOut panic: interval %v, retry %v, timeout %v, d1 %v,", intervalMs, retry, checkTimeOut, d1)
		return maxRetryInterval * time.Millisecond
	}

	d1 = d1 + rand.Intn(d1)/10
	return time.Duration(d1) * time.Millisecond
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
	size int

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
	dp   *wrapper.DataPartition

	// Issue a signal to this channel when *inflight* hits zero.
	// To wake up *waitForFlush*.
	empty chan struct{}

	// Created and updated in *receiver* ONLY.
	// Not protected by lock, therefore can be used ONLY when there is no
	// pending and new packets.
	key   *proto.ExtentKey
	dirty bool // indicate if open handler is dirty.

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

	// ver update need alloc new extent
	verUpdate chan uint64
	appendLK  sync.Mutex
	lastKey   proto.ExtentKey

	meetLimitedIoError bool

	stop chan struct{}
	sync.Once

	storageClass uint32

	isMigration bool
}

// NewExtentHandler returns a new extent handler.
func NewExtentHandler(stream *Streamer, offset int, storeMode int, size int,
	storageClass uint32, isMigration bool,
) *ExtentHandler {
	// TODO: for debug
	// log.LogDebugf("NewExtentHandler, inode(%v) storageClass(%v), stack:\n%v",
	//	 stream.inode, storageClass, string(debug.Stack()))

	eh := &ExtentHandler{
		stream:             stream,
		id:                 GetExtentHandlerID(),
		inode:              stream.inode,
		fileOffset:         offset,
		size:               size,
		storeMode:          storeMode,
		empty:              make(chan struct{}, 1024),
		request:            make(chan *Packet, 1024),
		reply:              make(chan *Packet, 1024),
		doneSender:         make(chan struct{}),
		doneReceiver:       make(chan struct{}),
		stop:               make(chan struct{}),
		meetLimitedIoError: false,
		verUpdate:          make(chan uint64),
		storageClass:       proto.GetMediaTypeByStorageClass(storageClass),
		isMigration:        isMigration,
	}

	go eh.receiver()
	go eh.sender()

	return eh
}

// String returns the string format of the extent handler.
func (eh *ExtentHandler) String() string {
	return fmt.Sprintf("ExtentHandler{ID(%v)Inode(%v)FileOffset(%v)Size(%v)StoreMode(%v)Status(%v)Dp(%v)Ver(%v)key(%v)lastKey(%v)flight(%d)dirty(%v)storageClass(%v)inflight(%v)dirty(%v)}",
		eh.id, eh.inode, eh.fileOffset, eh.size, eh.storeMode, eh.status, eh.dp, eh.stream.verSeq, eh.key, eh.lastKey, eh.inflight, eh.dirty, eh.storageClass, eh.inflight, eh.dirty)
}

func (eh *ExtentHandler) write(data []byte, offset, size int, direct bool) (ek *proto.ExtentKey, err error) {
	var total, write int

	status := eh.getStatus()
	if status >= ExtentStatusClosed {
		err = errors.NewErrorf("ExtentHandler Write: Full or Recover eh(%v) key(%v)", eh, eh.key)
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
	if proto.IsHot(eh.stream.client.volumeType) || proto.IsStorageClassReplica(eh.storageClass) {
		if eh.fileOffset+eh.size != offset || eh.size+size > util.ExtentSize ||
			(eh.storeMode == proto.TinyExtentType && eh.size+size > blksize) {

			err = errors.New("ExtentHandler: full or incontinuous")
			return
		}
	}

	for total < size {
		if eh.packet == nil {
			eh.packet = NewWritePacket(eh.inode, offset+total, eh.storeMode)
			log.LogDebugf("ExtentHandler write packet nil and new packet: eh(%v)", eh)
			if direct {
				eh.packet.Opcode = proto.OpSyncWrite
			}
			// log.LogDebugf("ExtentHandler Write: NewPacket, eh(%v) packet(%v)", eh, eh.packet)
		}
		packsize := int(eh.packet.Size)
		write = util.Min(size-total, blksize-packsize)
		if write > 0 {
			copy(eh.packet.Data[packsize:packsize+write], data[total:total+write])
			eh.packet.Size += uint32(write)
			total += write
		}

		if int(eh.packet.Size) >= blksize {
			eh.flushPacket()
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

	for {
		select {
		case packet := <-eh.request:
			log.LogDebugf("ExtentHandler sender begin: eh(%v) packet(%v)", eh, packet)
			if eh.getStatus() >= ExtentStatusRecovery {
				log.LogWarnf("sender in recovery: eh(%v) packet(%v)", eh, packet)
				eh.reply <- packet
				continue
			}

			// Initialize dp, conn, and extID
			if eh.dp == nil {
				if err = eh.allocateExtent(); err != nil {
					eh.setClosed()
					eh.setRecovery()
					// if dp is not specified and yet we failed, then error out.
					// otherwise, just try to recover.
					if eh.key == nil {
						eh.setError()
						log.LogErrorf("sender: eh(%v) err(%v)", eh, err)
					} else {
						log.LogWarnf("sender: eh(%v) err(%v)", eh, err)
					}
					eh.reply <- packet
					continue
				}
			}

			// For ExtentStore, calculate the extent offset.
			// For TinyStore, the extent offset is always 0 in the request packet,
			// and the reply packet tells the real extent offset.
			extOffset := int(packet.KernelOffset) - eh.fileOffset
			if eh.key != nil {
				extOffset += int(eh.key.ExtentOffset)
			}

			// fill the packet according to the extent
			packet.PartitionID = eh.dp.PartitionID
			packet.ExtentType = uint8(eh.storeMode)
			packet.ExtentType |= proto.PacketProtocolVersionFlag
			packet.ExtentID = uint64(eh.extID)
			packet.ExtentOffset = int64(extOffset)
			packet.Arg = ([]byte)(eh.dp.GetAllAddrs())
			packet.ArgLen = uint32(len(packet.Arg))
			packet.RemainingFollowers = uint8(len(eh.dp.Hosts) - 1)
			if len(eh.dp.Hosts) == 1 {
				packet.RemainingFollowers = 127
			}
			packet.StartT = time.Now().UnixNano()

			if log.EnableDebug() {
				log.LogDebugf("ExtentHandler sender: extent allocated, eh(%v) dp(%v) extID(%v) packet(%v)", eh, eh.dp, eh.extID, packet.GetUniqueLogId())
			}

			if err = packet.writeToConn(eh.conn); err != nil {
				log.LogWarnf("sender writeTo: failed, eh(%v) err(%v) packet(%v)", eh, err, packet)
				eh.setClosed()
				eh.setRecovery()
			}
			eh.reply <- packet
		case <-eh.doneSender:
			eh.setClosed()
			log.LogDebugf("sender: done, eh(%v) size(%v) ek(%v)", eh, eh.size, eh.key)
			return
		}
	}
}

func (eh *ExtentHandler) receiver() {
	for {
		select {
		case packet := <-eh.reply:
			eh.processReply(packet)
		case <-eh.doneReceiver:
			log.LogDebugf("receiver done: eh(%v) size(%v) ek(%v)", eh, eh.size, eh.key)
			return
		}
	}
}

func (eh *ExtentHandler) processReply(packet *Packet) {
	log.LogDebugf("processReply begin: packet(%v), eh(%v)", packet, eh)
	defer func() {
		log.LogDebugf("processReply end: packet(%v), eh(%v)", packet, eh)
		if atomic.AddInt32(&eh.inflight, -1) <= 0 {
			eh.empty <- struct{}{}
		}
	}()

	status := eh.getStatus()
	if status >= ExtentStatusError {
		eh.discardPacket(packet)
		log.LogErrorf("processReply discard packet: handler is in error status, inflight(%v) eh(%v) packet(%v)", atomic.LoadInt32(&eh.inflight), eh, packet)
		return
	} else if status >= ExtentStatusRecovery {
		if err := eh.recoverPacket(packet); err != nil {
			eh.discardPacket(packet)
			log.LogErrorf("processReply discard packet: handler is in recovery status, inflight(%v) eh(%v) packet(%v) err(%v)", atomic.LoadInt32(&eh.inflight), eh, packet, err)
		}
		log.LogDebugf("processReply recover packet: handler is in recovery status, inflight(%v) from eh(%v) to recoverHandler(%v) packet(%v)", atomic.LoadInt32(&eh.inflight), eh, eh.recoverHandler, packet)
		return
	}
	var verUpdate bool
	reply := NewReply(packet.ReqID, packet.PartitionID, packet.ExtentID)
	err := reply.ReadFromConnWithVer(eh.conn, proto.ReadDeadlineTime)
	if err != nil {
		eh.processReplyError(packet, err.Error())
		return
	}

	if reply.VerSeq > atomic.LoadUint64(&eh.stream.verSeq) || (eh.key != nil && reply.VerSeq > eh.key.GetSeq()) {
		log.LogDebugf("processReply.UpdateLatestVer update verseq according to data rsp from version %v to %v", eh.stream.verSeq, reply.VerSeq)
		if err = eh.stream.client.UpdateLatestVer(&proto.VolVersionInfoList{VerList: reply.VerList}); err != nil {
			eh.processReplyError(packet, err.Error())
			return
		}
		if err = eh.appendExtentKey(); err != nil {
			eh.processReplyError(packet, err.Error())
			return
		}
		eh.key = nil
		verUpdate = true
	}

	// NOTE: if meet a io limited error
	if reply.ResultCode == proto.OpLimitedIoErr {
		log.LogWarnf("[processReply] eh(%v) packet(%v) reply(%v) try again", eh, packet, reply)
		eh.meetLimitedIoError = true
		time.Sleep(StreamSendSleepInterval)
	}

	if reply.ResultCode != proto.OpOk {
		if reply.ResultCode != proto.ErrCodeVersionOpError {
			errmsg := fmt.Sprintf("reply NOK: reply(%v)", reply)
			log.LogDebugf("processReply packet (%v) errmsg (%v)", packet, errmsg)
			eh.processReplyError(packet, errmsg)
			return
		}
		// todo(leonchang) need check safety
		log.LogWarnf("processReply: get reply, eh(%v) packet(%v) reply(%v)", eh, packet, reply)
		eh.stream.GetExtentsForceRefresh()
	}

	if !packet.isValidWriteReply(reply) {
		errmsg := fmt.Sprintf("request and reply does not match: reply(%v)", reply)
		eh.processReplyError(packet, errmsg)
		return
	}

	if reply.CRC != packet.CRC {
		errmsg := fmt.Sprintf("inconsistent CRC: reqCRC(%v) replyCRC(%v) reply(%v) ", packet.CRC, reply.CRC, reply)
		eh.processReplyError(packet, errmsg)
		return
	}

	eh.dp.RecordWrite(packet.StartT)

	var extID, extOffset uint64

	if eh.storeMode == proto.TinyExtentType {
		extID = reply.ExtentID
		extOffset = uint64(reply.ExtentOffset)
	} else {
		extID = packet.ExtentID
		extOffset = packet.KernelOffset - uint64(eh.fileOffset)
	}
	fileOffset := uint64(eh.fileOffset)
	if verUpdate {
		fileOffset = reply.KernelOffset
	}
	if eh.key == nil || verUpdate {
		eh.key = &proto.ExtentKey{
			FileOffset:   fileOffset,
			PartitionId:  packet.PartitionID,
			ExtentId:     extID,
			ExtentOffset: extOffset,
			Size:         packet.Size,
			SnapInfo: &proto.ExtSnapInfo{
				VerSeq: reply.VerSeq,
			},
		}
	} else {
		eh.key.Size += packet.Size
	}

	proto.Buffers.Put(packet.Data)
	packet.Data = nil
	eh.dirty = true

	// tiny extent can only write once.
	if eh.storeMode == proto.TinyExtentType {
		eh.setClosed()
		eh.setRecovery()
	}
}

func (eh *ExtentHandler) processReplyError(packet *Packet, errmsg string) {
	log.LogDebugf("processReplyError begin: eh(%v) packet(%v) errmsg(%v)", eh, packet, errmsg)
	eh.setClosed()
	eh.setRecovery()
	if err := eh.recoverPacket(packet); err != nil {
		eh.discardPacket(packet)
		log.LogErrorf("processReplyError discard packet: eh(%v) packet(%v) err(%v) errmsg(%v)", eh, packet, err, errmsg)
	} else {
		log.LogWarnf("processReplyError end: eh(%v) packet(%v) errmsg(%v)", eh, packet, errmsg)
	}
}

func (eh *ExtentHandler) flush() (err error) {
	log.LogDebugf("ExtentHandler flush begin: eh(%s)", eh.String())
	eh.flushPacket()
	err = eh.waitForFlush()
	if err != nil {
		log.LogErrorf("ExtentHandler flush failed, eh(%s), err %s", eh.String(), err.Error())
		return err
	}

	err = eh.appendExtentKey()
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
	log.LogDebugf("cleanup: eh(%v)", eh)
	eh.Once.Do(func() {
		eh.doneSender <- struct{}{}
		eh.doneReceiver <- struct{}{}
		if eh.conn != nil {
			conn := eh.conn
			eh.conn = nil
			// TODO unhandled error
			status := eh.getStatus()
			StreamWriteConnPool.PutConnect(conn, status >= ExtentStatusRecovery)
		}
		close(eh.stop)
	})

	return
}

// can ONLY be called when the handler is not open any more
func (eh *ExtentHandler) appendExtentKey() (err error) {
	eh.appendLK.Lock()
	defer eh.appendLK.Unlock()

	if eh.key != nil {
		if eh.dirty {
			if proto.IsCold(eh.stream.client.volumeType) || proto.IsStorageClassBlobStore(eh.storageClass) &&
				eh.status == ExtentStatusError {
				return
			}
			var status int
			ekey := *eh.key
			doAppend := func() (err error) {
				discard := eh.stream.extents.Append(&ekey, true)
				status, err = eh.stream.client.appendExtentKey(eh.stream.parentInode, eh.inode, ekey, discard, eh.stream.isCache, eh.storageClass, eh.isMigration)
				if atomic.LoadInt32(&eh.stream.needUpdateVer) > 0 {
					if errUpdateExtents := eh.stream.GetExtentsForceRefresh(); errUpdateExtents != nil {
						log.LogErrorf("action[appendExtentKey] inode %v GetExtents err %v errUpdateExtents %v", eh.stream.inode, err, errUpdateExtents)
						return
					}
				}
				if err == nil && len(discard) > 0 {
					eh.stream.extents.RemoveDiscard(discard)
				}
				return
			}
			if err = doAppend(); err == nil {
				eh.dirty = false
				eh.lastKey = *eh.key
				log.LogDebugf("action[appendExtentKey] status %v, needUpdateVer %v, eh{%v}", status, eh.stream.needUpdateVer, eh)
				return nil
			}
			// Due to the asynchronous synchronization of version numbers, the extent cache version of the client is updated first before being written to the meta.
			// However, it is possible for the client version to lag behind the meta version, resulting in partial inconsistencies in judgment.
			// For example, if the version remains unchanged in the client,
			// the append-write principle is to reuse the extent key while changing the length. But if the meta has already changed its version,
			// a new extent key information needs to be constructed for retrying the operation.
			log.LogWarnf("action[appendExtentKey] status %v, handler %v, err %v", status, eh, err)
			if status == meta.StatusConflictExtents &&
				(atomic.LoadInt32(&eh.stream.needUpdateVer) > 0 || eh.stream.verSeq > 0) &&
				eh.lastKey.PartitionId != 0 {
				log.LogDebugf("action[appendExtentKey] do append again err %v, key %v", err, ekey)
				if eh.lastKey.IsSameExtent(&ekey) &&
					eh.lastKey.FileOffset == ekey.FileOffset &&
					eh.lastKey.ExtentOffset == ekey.ExtentOffset &&
					eh.lastKey.Size < ekey.Size {
					ekey.FileOffset += uint64(eh.lastKey.Size)
					ekey.ExtentOffset += uint64(eh.lastKey.Size)
					ekey.Size -= eh.lastKey.Size
					ekey.SetSeq(eh.stream.verSeq)
					eh.lastKey = ekey
					if err = doAppend(); err != nil {
						eh.key = nil
						eh.lastKey.PartitionId = 0
					} else {
						*eh.key = ekey
					}
					log.LogDebugf("action[appendExtentKey] do append again err %v, key %v", err, ekey)
				}
			}
		} else {
			/*
			 * Update extents cache using the ek stored in the eh. This is
			 * indispensable because the ek in the extent cache might be
			 * a temp one with dpid 0, especially when current eh failed and
			 * create a new eh to do recovery.
			 */
			_ = eh.stream.extents.Append(eh.key, false)
		}

		if eh.key.PartitionId > 0 && eh.stream.enableRemoteCacheAutoPrepare() {
			prepareReq := &PrepareRemoteCacheRequest{
				ctx:   context.Background(),
				ek:    eh.key,
				inode: eh.stream.inode,
			}
			eh.stream.sendToPrepareRomoteCacheChan(prepareReq)
			// eh.stream.prepareRemoteCache(ctx, ek)
		}
	}
	if err == nil {
		eh.dirty = false
	} else {
		log.LogErrorf("action[appendExtentKey] %v do append again err %v", eh, err)
		eh.lastKey.PartitionId = 0
	}
	return
}

// This function is meaningful to be called from stream writer flush method,
// because there is no new write request.
func (eh *ExtentHandler) waitForFlush() (err error) {
	log.LogDebugf("ExtentHandler waitForFlush begin: eh(%v)", eh)
	defer func() {
		log.LogDebugf("ExtentHandler waitForFlush end: eh(%v)", eh)
	}()

	if atomic.LoadInt32(&eh.inflight) <= 0 {
		return
	}

	for {
		select {
		case <-eh.empty:
			if atomic.LoadInt32(&eh.inflight) <= 0 {
				return
			}
		case <-eh.stop:
			if atomic.LoadInt32(&eh.inflight) <= 0 {
				return
			}
			return fmt.Errorf("eh maybe cleaned")
		}
	}
}

func (eh *ExtentHandler) recoverPacket(packet *Packet) error {
	log.LogDebugf("ExtentHandler recoverPacket: eh(%v), packet(%v)", eh, packet)
	packet.errCount++
	if packet.errCount >= MaxPacketErrorCount || proto.IsCold(eh.stream.client.volumeType) || proto.IsStorageClassBlobStore(eh.storageClass) {
		return errors.New(fmt.Sprintf("recoverPacket failed: reach max error limit, eh(%v) packet(%v)", eh, packet))
	}

	handler := eh.recoverHandler
	if handler == nil {
		// Always use normal extent store mode for recovery.
		// Because tiny extent files are limited, tiny store
		// failures might due to lack of tiny extent file.
		extentType := proto.NormalExtentType
		// NOTE: but, if we meet a limited io error or enable retry for tiny extent
		// use correct type to recover
		if eh.meetLimitedIoError || enableRetryTiny {
			extentType = eh.storeMode
		}

		if extentWriteRetryIntervalMs > 0 {
			retryInterval := getRetryInterval(extentWriteRetryIntervalMs, packet.errCount)
			log.LogWarnf("ExtentHandler slow recoverPacket: sleep(%dms) before retry. eh(%s), pkt(%v)",
				retryInterval.Milliseconds(), eh.String(), packet)
			time.Sleep(retryInterval)
		}

		handler = NewExtentHandler(eh.stream, int(packet.KernelOffset), extentType, 0, eh.storageClass, eh.isMigration)
		handler.setClosed()
	}
	handler.pushToRequest(packet)
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

func (eh *ExtentHandler) allocateExtent() (err error) {
	var (
		dp    *wrapper.DataPartition
		conn  *net.TCPConn
		extID int
	)

	log.LogDebugf("ExtentHandler allocateExtent enter: eh(%v)", eh)

	exclude := make(map[string]struct{})

	for i := 0; i < MaxSelectDataPartitionForWrite; i++ {
		if eh.key == nil {
			if dp, err = eh.stream.client.dataWrapper.GetDataPartitionForWrite(exclude, eh.storageClass, eh.id); err != nil {
				log.LogWarnf("allocateExtent: failed to get write data partition, eh(%v) exclude(%v), clear exclude and try again!", eh, exclude)
				exclude = make(map[string]struct{})
				continue
			}

			extID = 0
			if eh.storeMode == proto.NormalExtentType {
				extID, err = eh.createExtent(dp)
			}
			if err != nil {
				// reduce cluster messusre by slow retry
				if extentAllocRetryIntervalMs > 0 {
					retryInterval := getRetryInterval(extentAllocRetryIntervalMs, i)
					log.LogWarnf("allocateExtent: slow retry alloc extent, sleep interval %dms, retry(%d), eh(%v)",
						retryInterval.Milliseconds(), i, eh)
					time.Sleep(retryInterval)
				}

				// NOTE: try again
				if strings.Contains(err.Error(), "Again") || strings.Contains(err.Error(), "LimitedIoErr") {
					log.LogWarnf("[allocateExtent] eh(%v) try again, err:%v", eh, err)
					continue
				}
				log.LogWarnf("allocateExtent: exclude dp(%v) mediaType(%v) for write caused by create extent failed, eh(%v) err(%v) exclude(%v)",
					dp.PartitionID, proto.MediaTypeString(dp.MediaType), eh, err, exclude)
				eh.stream.client.dataWrapper.RemoveDataPartitionForWrite(dp.PartitionID)
				dp.CheckAllHostsIsAvail(exclude)
				continue
			}
		} else {
			if dp, err = eh.stream.client.dataWrapper.GetDataPartition(eh.key.PartitionId); err != nil {
				log.LogWarnf("allocateExtent: failed to get write data partition, eh(%v)", eh)
				break
			}
			extID = int(eh.key.ExtentId)
		}

		if conn, err = StreamWriteConnPool.GetConnect(dp.Hosts[0]); err != nil {
			log.LogWarnf("allocateExtent: failed to create connection, eh(%v) err(%v) dp(%v) exclude(%v)",
				eh, err, dp, exclude)
			// If storeMode is tinyExtentType and can't create connection, we also check host status.
			dp.CheckAllHostsIsAvail(exclude)
			if eh.key != nil {
				break
			}
			continue
		}

		// success
		eh.dp = dp
		eh.conn = conn
		eh.extID = extID

		// log.LogDebugf("ExtentHandler allocateExtent exit: eh(%v) dp(%v) extID(%v)", eh, dp, extID)
		return nil
	}

	errmsg := "allocateExtent failed: hit max retry limit"
	if err != nil {
		err = errors.Trace(err, errmsg)
	} else {
		err = errors.New(errmsg)
	}
	return err
}

func (eh *ExtentHandler) createExtent(dp *wrapper.DataPartition) (extID int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("createExtent", err, bgTime, 1)
	}()

	conn, err := StreamWriteConnPool.GetConnect(dp.Hosts[0])
	if err != nil {
		return extID, errors.Trace(err, "createExtent: failed to create connection, eh(%v) datapartionHosts(%v)", eh, dp.Hosts[0])
	}

	defer func() {
		StreamWriteConnPool.PutConnectEx(conn, err)
	}()

	p := NewCreateExtentPacket(dp, eh.inode)
	if err = p.WriteToConn(conn); err != nil {
		return extID, errors.Trace(err, "createExtent: failed to WriteToConn, packet(%v) datapartionHosts(%v)", p, dp.Hosts[0])
	}

	if err = p.ReadFromConnWithVer(conn, proto.ReadDeadlineTime*2); err != nil {
		return extID, errors.Trace(err, "createExtent: failed to ReadFromConn, packet(%v) datapartionHosts(%v)", p, dp.Hosts[0])
	}

	if p.ResultCode != proto.OpOk {
		if p.ResultCode == proto.OpDiskNoSpaceErr {
			log.LogWarnf("[createExtent] dp(%v) disk full or tiny extent runs out", dp.PartitionID)
		}
		return extID, errors.New(fmt.Sprintf("createExtent: ResultCode NOK, packet(%v) datapartionHosts(%v) ResultCode(%v)", p, dp.Hosts[0], p.GetResultMsg()))
	}

	extID = int(p.ExtentID)
	if extID <= 0 {
		return extID, errors.New(fmt.Sprintf("createExtent: illegal extID(%v) from (%v)", extID, dp.Hosts[0]))
	}

	return extID, nil
}

// Handler lock is held by the caller.
func (eh *ExtentHandler) flushPacket() {
	if eh.packet == nil {
		log.LogDebugf("ExtentHandler flushPacket nil, return: eh(%v)", eh)
		return
	}

	eh.pushToRequest(eh.packet)
	eh.packet = nil
	log.LogDebugf("ExtentHandler flushPacket end: eh(%v)", eh)
}

func (eh *ExtentHandler) pushToRequest(packet *Packet) {
	// Increase before sending the packet, because inflight is used
	// to determine if the handler has finished.
	atomic.AddInt32(&eh.inflight, 1)
	eh.request <- packet
}

func (eh *ExtentHandler) getStatus() int32 {
	return atomic.LoadInt32(&eh.status)
}

func (eh *ExtentHandler) setClosed() bool {
	//	log.LogDebugf("action[ExtentHandler.setClosed] stack (%v)", string(debug.Stack()))
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusOpen, ExtentStatusClosed)
}

func (eh *ExtentHandler) setRecovery() bool {
	// log.LogDebugf("action[ExtentHandler.setRecovery] stack (%v)", string(debug.Stack()))
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusClosed, ExtentStatusRecovery)
}

func (eh *ExtentHandler) setError() bool {
	//	log.LogDebugf("action[ExtentHandler.setError] stack (%v)", string(debug.Stack()))
	if proto.IsHot(eh.stream.client.volumeType) || proto.IsStorageClassReplica(eh.storageClass) {
		eh.stream.setError()
	}
	return atomic.CompareAndSwapInt32(&eh.status, ExtentStatusRecovery, ExtentStatusError)
}
