// Copyright 2018 The ChuBao Authors.
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
	"container/list"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/sdk/data"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"time"
)

const (
	ContinueReceive        = true
	NotReceive             = false
	DefaultWriteBufferSize = 2 * util.MB
	ForBidUpdateExtentKey  = -1
	ForBidUpdateMetaNode   = -2
	ExtentFlushIng         = 1
	ExtentHasFlushed       = 2
)

var (
	FlushErr      = errors.New("backend flush error")
	FullExtentErr = errors.New("full extent")
)

type ExtentWriter struct {
	inode         uint64     //Current write Inode
	requestQueue  *list.List //sendPacketList
	dp            *data.DataPartition
	extentId      uint64 //current FileId
	currentPacket *Packet
	byteAck       uint64 //DataNode Has Ack Bytes
	offset        int
	connect       *net.TCPConn
	handleCh      chan bool //a Chan for signal recive goroutine recive packet from connect
	recoverCnt    int       //if failed,then recover contine,this is recover count
	forbidUpdate  int64
	requestLock   sync.Mutex
	isflushIng    int32
	flushSignleCh chan bool
}

func NewExtentWriter(inode uint64, dp *data.DataPartition, extentId uint64) (writer *ExtentWriter, err error) {
	if extentId <= 0 {
		return nil, fmt.Errorf("inode(%v),dp(%v),unavalid extentId(%v)", inode, dp.PartitionID, extentId)
	}
	writer = new(ExtentWriter)
	writer.requestQueue = list.New()
	writer.handleCh = make(chan bool, DefaultWriteBufferSize/(64*util.KB))
	writer.extentId = extentId
	writer.dp = dp
	writer.inode = inode
	writer.flushSignleCh = make(chan bool, 1)
	var connect *net.TCPConn
	conn, err := net.DialTimeout("tcp", dp.Hosts[0], time.Second)
	if err == nil {
		connect, _ = conn.(*net.TCPConn)
		connect.SetKeepAlive(true)
		connect.SetNoDelay(true)
	}
	if err != nil {
		return
	}
	writer.setConnect(connect)
	go writer.receive()

	return
}

//when backEndlush func called,and sdk must wait
func (writer *ExtentWriter) flushWait() {
	writer.flushSignleCh = make(chan bool, 1)
	ticker := time.NewTicker(time.Second)
	atomic.StoreInt32(&writer.isflushIng, ExtentFlushIng)
	defer func() {
		atomic.StoreInt32(&writer.isflushIng, ExtentHasFlushed)
	}()
	if !(writer.getQueueListLen() > 0 || writer.currentPacket != nil) || atomic.LoadInt32(&writer.isflushIng) == ExtentHasFlushed {
		return
	}
	for {
		select {
		case <-writer.flushSignleCh:
			return
		case <-ticker.C:
			return
		}
	}
}

//user call write func
func (writer *ExtentWriter) write(data []byte, kernelOffset, size int) (total int, err error) {
	var canWrite int
	defer func() {
		if err != nil {
			writer.getConnect().Close()
			writer.cleanHandleCh()
			err = errors.Annotatef(err, "writer(%v) write failed", writer.toString())
		}
	}()
	if writer.offset+util.BlockSize*10 >= util.ExtentSize {
		err = FullExtentErr
		return 0, err
	}
	for total < size {
		if writer.currentPacket == nil {
			writer.currentPacket = NewWritePacket(writer.dp, writer.extentId, writer.offset, kernelOffset)
		}
		canWrite = writer.currentPacket.fill(data[total:size], size-total) //fill this packet
		if writer.IsFullCurrentPacket() || canWrite == 0 {
			err = writer.sendCurrPacket()
			if err != nil { //if failed,recover it
				return total, err
			}
		}
		total += canWrite
	}

	return
}

func (writer *ExtentWriter) IsFullCurrentPacket() bool {
	return writer.currentPacket.isFullPacket()
}

func (writer *ExtentWriter) sendCurrPacket() (err error) {
	if writer.currentPacket == nil {
		return
	}
	if writer.currentPacket.getPacketLength() == 0 {
		return
	}
	writer.pushRequestToQueue(writer.currentPacket)
	packet := writer.currentPacket
	writer.currentPacket = nil
	orgOffset := writer.offset
	writer.offset += packet.getPacketLength()
	err = packet.writeTo(writer.connect) //if send packet,then signal recive goroutine for recive from connect
	prefix := fmt.Sprintf("send inode %v_%v", writer.inode, packet.kernelOffset)
	log.LogDebugf(prefix+" to extent(%v) pkg(%v) orgextentOffset(%v)"+
		" packetGetPacketLength(%v) after jia(%v) crc(%v)",
		writer.toString(), packet.GetUniqueLogId(), orgOffset, packet.getPacketLength(),
		writer.offset, packet.Crc)
	if err == nil {
		writer.handleCh <- ContinueReceive
		return
	} else {
		writer.notifyExit()
	}
	err = errors.Annotatef(err, prefix+"sendCurrentPacket Failed")
	log.LogWarn(err.Error())

	return err
}

func (writer *ExtentWriter) notifyExit() {
	writer.cleanHandleCh()
	writer.handleCh <- NotReceive
}

func (writer *ExtentWriter) cleanHandleCh() {
	for {
		select {
		case <-writer.handleCh:
			continue
		default:
			return
		}
	}
}

//every extent is FULL,must is 64MB
func (writer *ExtentWriter) isFullExtent() bool {
	return writer.offset+util.BlockSize*10 >= util.ExtentSize
}

//check allPacket has Ack
func (writer *ExtentWriter) isAllFlushed() bool {
	return !(writer.getQueueListLen() > 0 || writer.currentPacket != nil)
}

func (writer *ExtentWriter) toString() string {
	return fmt.Sprintf("extent{inode=%v dp=%v extentId=%v handleCh(%v) requestQueueLen(%v) }",
		writer.inode, writer.dp.PartitionID, writer.extentId,
		len(writer.handleCh), writer.getQueueListLen())
}

func (writer *ExtentWriter) checkIsStopReciveGoRoutine() {
	if writer.isAllFlushed() && writer.isFullExtent() {
		writer.handleCh <- NotReceive
	}
	return
}

func (writer *ExtentWriter) flush() (err error) {
	start := time.Now().UnixNano()
	err = errors.Annotatef(FlushErr, "cannot backEndlush writer")
	defer func() {
		writer.checkIsStopReciveGoRoutine()
		log.LogDebugf(writer.toString()+" Flush DataNode cost(%v)ns", time.Now().UnixNano()-start)
		if err == nil {
			return
		}
	}()
	if writer.isAllFlushed() {
		err = nil
		return nil
	}
	if writer.getPacket() != nil {
		if err = writer.sendCurrPacket(); err != nil {
			return err
		}
	}
	if writer.isAllFlushed() {
		err = nil
		return nil
	}
	writer.flushWait()
	if !writer.isAllFlushed() {
		err = errors.Annotatef(FlushErr, "cannot backEndlush writer")
		return err
	}

	return nil
}

func (writer *ExtentWriter) close() (err error) {
	if writer.isAllFlushed() {
		select {
		case writer.handleCh <- NotReceive:
		default:
			break
		}
	} else {
		err = writer.flush()
		if err == nil && writer.isAllFlushed() {
			select {
			case writer.handleCh <- NotReceive:
			default:
				break
			}
		}
	}
	return
}

func (writer *ExtentWriter) processReply(e *list.Element, request, reply *Packet) (err error) {
	if reply.ResultCode != proto.OpOk {
		return errors.Annotatef(fmt.Errorf("reply status code(%v) is not ok,request (%v) "+
			"but reply (%v) ", reply.ResultCode, request.GetUniqueLogId(), reply.GetUniqueLogId()),
			fmt.Sprintf("writer(%v)", writer.toString()))
	}
	if !request.IsEqualWriteReply(reply) {
		return errors.Annotatef(fmt.Errorf("request not equare reply , request (%v) "+
			"and reply (%v) ", request.GetUniqueLogId(), reply.GetUniqueLogId()),
			fmt.Sprintf("writer(%v)", writer.toString()))
	}
	if reply.Crc != request.Crc {
		return errors.Annotatef(fmt.Errorf("crc not match on  request (%v) "+
			"and reply (%v) expectCrc(%v) but reciveCrc(%v) ", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.Crc, reply.Crc),
			fmt.Sprintf("writer(%v)", writer.toString()))
	}

	if atomic.LoadInt64(&writer.forbidUpdate) == ForBidUpdateExtentKey {
		return fmt.Errorf("forbid update extent key (%v)", writer.toString())
	}
	if atomic.LoadInt64(&writer.forbidUpdate) == ForBidUpdateMetaNode {
		return fmt.Errorf("forbid update extent key (%v) to metanode", writer.toString())
	}
	writer.removeRquest(e)
	writer.addByteAck(uint64(request.Size))
	if atomic.LoadInt32(&writer.isflushIng) == ExtentFlushIng && !(writer.getQueueListLen() > 0 || writer.currentPacket != nil) {
		atomic.StoreInt32(&writer.isflushIng, ExtentHasFlushed)
		select {
		case writer.flushSignleCh <- true:
			break
		default:
			break
		}
	}
	log.LogDebugf("recive inode(%v) kerneloffset(%v) to extent(%v) pkg(%v) recive(%v)",
		writer.inode, request.kernelOffset, writer.toString(), request.GetUniqueLogId(), reply.GetUniqueLogId())

	return nil
}

func (writer *ExtentWriter) toKey() (k proto.ExtentKey) {
	k = proto.ExtentKey{}
	k.PartitionId = writer.dp.PartitionID
	k.Size = uint32(writer.getByteAck())
	k.ExtentId = writer.extentId
	if atomic.LoadInt64(&writer.forbidUpdate) == ForBidUpdateMetaNode {
		k.Size = 0
	}

	return
}

func (writer *ExtentWriter) receive() {
	for {
		select {
		case code := <-writer.handleCh:
			if code == NotReceive {
				writer.getConnect().Close()
				return
			}
			e := writer.getFrontRequest()
			if e == nil {
				continue
			}
			request := e.Value.(*Packet)
			reply := NewReply(request.ReqID, request.PartitionID, request.FileID)
			reply.Opcode = request.Opcode
			reply.Offset = request.Offset
			reply.Size = request.Size
			err := reply.ReadFromConn(writer.getConnect(), proto.ReadDeadlineTime)
			if err != nil {
				writer.getConnect().Close()
				continue
			}
			if err = writer.processReply(e, request, reply); err != nil {
				writer.getConnect().Close()
				log.LogWarn(err.Error())
				continue
			}
		}
	}
}

func (writer *ExtentWriter) addByteAck(size uint64) {
	atomic.AddUint64(&writer.byteAck, size)
}

func (writer *ExtentWriter) forbirdUpdateToMetanode() {
	atomic.StoreInt64(&writer.forbidUpdate, ForBidUpdateMetaNode)
}

func (writer *ExtentWriter) getByteAck() uint64 {
	return atomic.LoadUint64(&writer.byteAck)
}

func (writer *ExtentWriter) getConnect() *net.TCPConn {
	return writer.connect
}

func (writer *ExtentWriter) setConnect(connect *net.TCPConn) {
	writer.connect = connect
}

func (writer *ExtentWriter) getFrontRequest() (e *list.Element) {
	writer.requestLock.Lock()
	defer writer.requestLock.Unlock()
	return writer.requestQueue.Front()
}

func (writer *ExtentWriter) pushRequestToQueue(request *Packet) {
	writer.requestLock.Lock()
	defer writer.requestLock.Unlock()
	writer.requestQueue.PushBack(request)
}

func (writer *ExtentWriter) removeRquest(e *list.Element) {
	writer.requestLock.Lock()
	defer writer.requestLock.Unlock()
	writer.requestQueue.Remove(e)
}

func (writer *ExtentWriter) getQueueListLen() (length int) {
	writer.requestLock.Lock()
	defer writer.requestLock.Unlock()
	return writer.requestQueue.Len()
}

func (writer *ExtentWriter) getNeedRetrySendPackets() (requests []*Packet) {
	var (
		backPkg *Packet
	)
	atomic.StoreInt64(&writer.forbidUpdate, ForBidUpdateExtentKey)
	writer.requestLock.Lock()
	defer writer.requestLock.Unlock()
	requests = make([]*Packet, 0)
	for e := writer.requestQueue.Front(); e != nil; e = e.Next() {
		requests = append(requests, e.Value.(*Packet))
	}
	if writer.currentPacket == nil {
		return
	}
	if len(requests) == 0 {
		requests = append(requests, writer.currentPacket)
		writer.currentPacket = nil
		return
	}
	backPkg = requests[len(requests)-1]
	if writer.currentPacket.ReqID > backPkg.ReqID && writer.currentPacket.kernelOffset > backPkg.kernelOffset {
		requests = append(requests, writer.currentPacket)
	}
	writer.currentPacket = nil

	return
}

func (writer *ExtentWriter) getPacket() (p *Packet) {
	return writer.currentPacket
}
