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
	"fmt"
	"syscall"
	"time"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/sdk/data"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"net"
	"strings"
)

const (
	MaxSelectDataPartionForWrite = 32
	MaxStreamInitRetry           = 3
	HasClosed                    = -1
)

type WriteRequest struct {
	data         []byte
	size         int
	canWrite     int
	err          error
	kernelOffset int
	cutSize      int
	done         chan struct{}
}

type FlushRequest struct {
	err error
}

type CloseRequest struct {
	err error
}

type StreamWriter struct {
	currentWriter      *ExtentWriter //current ExtentWriter
	errCount           int           //error count
	currentPartitionId uint32        //current PartitionId
	currentExtentId    uint64        //current FileId
	Inode              uint64        //inode
	excludePartition   []uint32
	appendExtentKey    AppendExtentKeyFunc
	writeRequestCh     chan *WriteRequest
	flushRequestCh     chan *FlushRequest
	flushReplyCh       chan *FlushRequest
	closeRequestCh     chan *CloseRequest
	closeReplyCh       chan *CloseRequest
	exitCh             chan bool
	hasUpdateKey       map[string]int
	HasWriteSize       uint64
	HasBufferSize      uint64
	hasClosed          int32
}

func NewStreamWriter(inode, start uint64, appendExtentKey AppendExtentKeyFunc) (stream *StreamWriter) {
	stream = new(StreamWriter)
	stream.appendExtentKey = appendExtentKey
	stream.Inode = inode
	stream.HasWriteSize = start
	stream.writeRequestCh = make(chan *WriteRequest, 1000)
	stream.closeReplyCh = make(chan *CloseRequest, 10)
	stream.closeRequestCh = make(chan *CloseRequest, 10)
	stream.flushReplyCh = make(chan *FlushRequest, 100)
	stream.flushRequestCh = make(chan *FlushRequest, 100)
	stream.exitCh = make(chan bool, 10)
	stream.excludePartition = make([]uint32, 0)
	stream.hasUpdateKey = make(map[string]int)
	go stream.server()

	return
}

func (stream *StreamWriter) toString() (m string) {
	currentWriterMsg := ""
	if stream.currentWriter != nil {
		currentWriterMsg = stream.currentWriter.toString()
	}
	return fmt.Sprintf("inode(%v) currentDataPartion(%v) currentExtentId(%v)"+
		" errCount(%v)", stream.Inode, stream.currentPartitionId, currentWriterMsg,
		stream.errCount)
}

func (stream *StreamWriter) toStringWithWriter(writer *ExtentWriter) (m string) {
	currentWriterMsg := writer.toString()
	return fmt.Sprintf("inode(%v) currentDataPartion(%v) currentExtentId(%v)"+
		" errCount(%v)", stream.Inode, stream.currentPartitionId, currentWriterMsg,
		stream.errCount)
}

//stream init,alloc a extent ,select dp and extent
func (stream *StreamWriter) init() (err error) {
	if stream.currentWriter != nil && stream.currentWriter.isFullExtent() {
		err = stream.flushCurrExtentWriter()
	}
	if err != nil {
		return errors.Annotatef(err, "WriteInit")
	}
	if stream.currentWriter != nil {
		return
	}
	var writer *ExtentWriter
	writer, err = stream.allocateNewExtentWriter()
	if err != nil {
		err = errors.Annotatef(err, "WriteInit AllocNewExtentFailed")
		return err
	}
	stream.currentWriter = writer
	return
}

func (stream *StreamWriter) server() {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	for {
		if stream.HasBufferSize >= gFlushBufferSize {
			stream.flushCurrExtentWriter()
		}
		select {
		case request := <-stream.writeRequestCh:
			if request.kernelOffset < int(stream.HasWriteSize) {
				cutSize := int(stream.HasWriteSize) - request.kernelOffset
				if cutSize < len(request.data) {
					request.kernelOffset += cutSize
					request.data = request.data[cutSize:]
					request.size -= cutSize
					request.cutSize = cutSize
				}
			}
			request.canWrite, request.err = stream.write(request.data, request.kernelOffset, request.size)
			stream.HasWriteSize += uint64(request.canWrite)
			request.done <- struct{}{}
		case request := <-stream.flushRequestCh:
			request.err = stream.flushCurrExtentWriter()
			stream.flushReplyCh <- request
		case request := <-stream.closeRequestCh:
			request.err = stream.flushCurrExtentWriter()
			if request.err == nil {
				request.err = stream.close()
			}
			stream.closeReplyCh <- request
		case <-stream.exitCh:
			stream.flushCurrExtentWriter()
			return
		case <-t.C:
			if stream.currentWriter == nil {
				continue
			}
			stream.flushCurrExtentWriter()
		}
	}
}

func (stream *StreamWriter) write(data []byte, offset, size int) (total int, err error) {
	var (
		write int
	)
	defer func() {
		if err == nil {
			total = size
			stream.HasBufferSize += uint64(total)
			return
		}
		err = errors.Annotatef(err, "UserRequest{inode(%v) write "+
			"KernelOffset(%v) KernelSize(%v) hasWrite(%v)}  stream{ (%v) occous error}",
			stream.Inode, offset, size, total, stream.toString())
		log.LogError(err.Error())
		log.LogError(errors.ErrorStack(err))
	}()

	var initRetry int = 0
	for total < size {
		if err = stream.init(); err != nil {
			if initRetry++; initRetry > MaxStreamInitRetry {
				return total, err
			}
			continue
		}
		write, err = stream.currentWriter.write(data[total:size], offset, size-total)
		if err == nil {
			write = size - total
			total += write
			continue
		}
		if strings.Contains(err.Error(), FullExtentErr.Error()) {
			continue
		}
		if err = stream.recoverExtent(); err != nil {
			return
		} else {
			write = size - total //recover success ,then write is allLength
		}
		total += write
	}

	return total, err
}

func (stream *StreamWriter) close() (err error) {
	if stream.currentWriter != nil {
		err = stream.currentWriter.close()
	}
	return
}

func (stream *StreamWriter) flushCurrExtentWriter() (err error) {
	var status error
	defer func() {
		if err == nil || status == syscall.ENOENT {
			stream.errCount = 0
			stream.HasBufferSize = 0
			err = nil
			return
		}
		stream.errCount++
		if stream.errCount < MaxSelectDataPartionForWrite {
			if err = stream.recoverExtent(); err == nil {
				err = stream.flushCurrExtentWriter()
			}
		}
	}()
	writer := stream.currentWriter
	if writer == nil {
		err = nil
		return nil
	}
	if err = writer.flush(); err != nil {
		err = errors.Annotatef(err, "writer(%v) Flush Failed", writer.toString())
		return err
	}
	if err = stream.updateToMetaNode(); err != nil {
		err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
		return err
	}
	if writer.isFullExtent() {
		writer.close()
		gDataWrapper.PutConnect(writer.getConnect(), NoCloseConnect)
		if err = stream.updateToMetaNode(); err != nil {
			err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
			return err
		}
		stream.currentWriter = nil
	}

	return err
}

func (stream *StreamWriter) updateToMetaNodeSize() (sumSize int) {
	for _, v := range stream.hasUpdateKey {
		sumSize += v
	}
	return sumSize
}

func (stream *StreamWriter) updateToMetaNode() (err error) {
	start := time.Now().UnixNano()
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		ek := stream.currentWriter.toKey() //first get currentExtent Key
		key := ek.GetExtentKey()
		lastUpdateExtentKeySize, ok := stream.hasUpdateKey[key]
		if ok && lastUpdateExtentKeySize == int(ek.Size) {
			return nil
		}
		if ek.Size != 0 {
			err = stream.appendExtentKey(stream.Inode, ek) //put it to metanode
			if err == syscall.ENOENT {
				return
			}
			if err == nil {
				elspetime := time.Now().UnixNano() - start
				stream.hasUpdateKey[key] = int(ek.Size)
				log.LogDebugf("inode(%v) update ek(%v) has update filesize(%v) coseTime (%v)ns ", stream.Inode, ek.String(),
					stream.updateToMetaNodeSize(), elspetime)
				return
			} else {
				err = errors.Annotatef(err, "update extent(%v) to MetaNode Failed", ek.Size)
				log.LogErrorf("stream(%v) err(%v)", stream.toString(), err.Error())
				continue
			}
		}
		break
	}
	return err
}

func (stream *StreamWriter) writeRecoverPackets(writer *ExtentWriter, retryPackets []*Packet) (err error) {
	for _, p := range retryPackets {
		log.LogInfof("recover packet (%v) kernelOffset(%v) to extent(%v)",
			p.GetUniqueLogId(), p.kernelOffset, writer.toString())
		_, err = writer.write(p.Data, p.kernelOffset, int(p.Size))
		if err != nil {
			err = errors.Annotatef(err, "pkg(%v) RecoverExtent write failed", p.GetUniqueLogId())
			log.LogErrorf("stream(%v) err(%v)", stream.toStringWithWriter(writer), err.Error())
			stream.excludePartition = append(stream.excludePartition, writer.dp.PartitionID)
			return err
		}
	}
	return
}

func (stream *StreamWriter) recoverExtent() (err error) {
	stream.excludePartition = append(stream.excludePartition, stream.currentWriter.dp.PartitionID) //exclude current PartionId
	stream.currentWriter.notifyExit()
	retryPackets := stream.currentWriter.getNeedRetrySendPackets() //get need retry recover packets
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if err = stream.updateToMetaNode(); err == nil {
			break
		}
	}
	var writer *ExtentWriter
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		err = nil
		if writer, err = stream.allocateNewExtentWriter(); err != nil { //allocate new extent
			err = errors.Annotatef(err, "RecoverExtent Failed")
			log.LogErrorf("stream(%v) err(%v)", stream.toString(), err.Error())
			continue
		}
		if err = stream.writeRecoverPackets(writer, retryPackets); err == nil {
			stream.excludePartition = make([]uint32, 0)
			stream.currentWriter = writer
			stream.updateToMetaNode()
			return err
		} else {
			writer.forbirdUpdateToMetanode()
			writer.notifyExit()
		}
	}

	return err

}

func (stream *StreamWriter) allocateNewExtentWriter() (writer *ExtentWriter, err error) {
	var (
		dp       *data.DataPartition
		extentId uint64
	)
	err = fmt.Errorf("cannot alloct new extent after maxrery")
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if dp, err = gDataWrapper.GetWriteDataPartition(stream.excludePartition); err != nil {
			log.LogWarn(fmt.Sprintf("stream (%v) ActionAllocNewExtentWriter "+
				"failed on getWriteDataPartion,error(%v) execludeDataPartion(%v)", stream.toString(), err.Error(), stream.excludePartition))
			continue
		}
		if extentId, err = stream.createExtent(dp); err != nil {
			log.LogWarn(fmt.Sprintf("stream (%v)ActionAllocNewExtentWriter "+
				"create Extent,error(%v) execludeDataPartion(%v)", stream.toString(), err.Error(), stream.excludePartition))
			continue
		}
		if writer, err = NewExtentWriter(stream.Inode, dp, extentId); err != nil {
			log.LogWarn(fmt.Sprintf("stream (%v) ActionAllocNewExtentWriter "+
				"NewExtentWriter(%v),error(%v) execludeDataPartion(%v)", stream.toString(), extentId, err.Error(), stream.excludePartition))
			continue
		}
		break
	}
	if extentId <= 0 {
		log.LogErrorf(errors.Annotatef(err, "allocateNewExtentWriter").Error())
		return nil, errors.Annotatef(err, "allocateNewExtentWriter")
	}
	stream.currentPartitionId = dp.PartitionID
	stream.currentExtentId = extentId
	err = nil

	return writer, nil
}

func (stream *StreamWriter) createExtent(dp *data.DataPartition) (extentId uint64, err error) {
	var (
		connect *net.TCPConn
	)
	conn, err := net.DialTimeout("tcp", dp.Hosts[0], time.Second)
	if err != nil {
		err = errors.Annotatef(err, " get connect from datapartionHosts(%v)", dp.Hosts[0])
		return 0, err
	}
	connect, _ = conn.(*net.TCPConn)
	connect.SetKeepAlive(true)
	connect.SetNoDelay(true)
	defer connect.Close()
	p := NewCreateExtentPacket(dp, stream.Inode)
	if err = p.WriteToConn(connect); err != nil {
		err = errors.Annotatef(err, "send CreateExtent(%v) to datapartionHosts(%v)", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime*2); err != nil {
		err = errors.Annotatef(err, "receive CreateExtent(%v) failed datapartionHosts(%v)", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.Annotatef(err, "receive CreateExtent(%v) failed datapartionHosts(%v) ", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	extentId = p.FileID
	if p.FileID <= 0 {
		err = errors.Annotatef(err, "illegal extentId(%v) from (%v) response",
			extentId, dp.Hosts[0])
		return

	}

	return extentId, nil
}
