// Copyright 2018 The Containerfs Authors.
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

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"net"
	"strings"
	"sync/atomic"
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
	err  error
	done chan struct{}
}

type CloseRequest struct {
	err  error
	done chan struct{}
}

type StreamWriter struct {
	currentWriter           *ExtentWriter //current ExtentWriter
	errCount                int           //error count
	currentPartitionId      uint32        //current PartitionId
	currentExtentId         uint64        //current FileId
	Inode                   uint64        //inode
	excludePartition        []uint32
	appendExtentKey         AppendExtentKeyFunc
	requestCh               chan interface{}
	exitCh                  chan bool
	hasUpdateKey            map[string]int
	hasWriteSize            uint64
	hasClosed               int32
	hasUpdateToMetaNodeSize uint64
}

func NewStreamWriter(inode, start uint64, appendExtentKey AppendExtentKeyFunc) (stream *StreamWriter) {
	stream = new(StreamWriter)
	stream.appendExtentKey = appendExtentKey
	stream.Inode = inode
	stream.setHasWriteSize(start)
	stream.requestCh = make(chan interface{}, 1000)
	stream.exitCh = make(chan bool, 10)
	stream.excludePartition = make([]uint32, 0)
	stream.hasUpdateKey = make(map[string]int, 0)
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
		if err = stream.flushCurrExtentWriter(); err != nil {
			return errors.Annotatef(err, "WriteInit")
		}
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

	stream.setCurrentWriter(writer)
	return
}

func (stream *StreamWriter) server() {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		select {
		case request := <-stream.requestCh:
			stream.handleRequest(request)
		case <-stream.exitCh:
			stream.flushCurrExtentWriter()
			return
		case <-t.C:
			atomic.StoreUint64(&stream.hasUpdateToMetaNodeSize, uint64(stream.updateToMetaNodeSize()))
			log.LogDebugf("inode(%v) update to metanode filesize To(%v) user has Write to (%v)",
				stream.Inode, stream.getHasUpdateToMetaNodeSize(), stream.getHasWriteSize())
			if stream.getCurrentWriter() == nil {
				continue
			}
			stream.flushCurrExtentWriter()
		}
	}
}

func (stream *StreamWriter) handleRequest(request interface{}) {
	switch request := request.(type) {
	case *WriteRequest:
		if request.kernelOffset < int(stream.getHasWriteSize()) {
			cutSize := int(stream.getHasWriteSize()) - request.kernelOffset
			if cutSize < len(request.data) {
				request.kernelOffset += cutSize
				request.data = request.data[cutSize:]
				request.size -= cutSize
				request.cutSize = cutSize
			}
		}
		request.canWrite, request.err = stream.write(request.data, request.kernelOffset, request.size)
		stream.addHasWriteSize(request.canWrite)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = stream.flushCurrExtentWriter()
		request.done <- struct{}{}
	case *CloseRequest:
		request.err = stream.flushCurrExtentWriter()
		if request.err == nil {
			request.err = stream.close()
		}
		request.done <- struct{}{}
		stream.exit()
	default:
	}
}

func (stream *StreamWriter) write(data []byte, offset, size int) (total int, err error) {
	var (
		write int
	)
	defer func() {
		if err == nil {
			total = size
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
	writer := stream.getCurrentWriter()
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
		writer.getConnect().Close()
		if err = stream.updateToMetaNode(); err != nil {
			err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
			return err
		}
		stream.setCurrentWriter(nil)
	}

	return err
}

func (stream *StreamWriter) updateToMetaNodeSize() (sumSize int) {
	return int(stream.hasUpdateToMetaNodeSize)
}

func (stream *StreamWriter) setCurrentWriter(writer *ExtentWriter) {
	stream.currentWriter = writer
}

func (stream *StreamWriter) getCurrentWriter() *ExtentWriter {
	return stream.currentWriter
}

func (stream *StreamWriter) updateToMetaNode() (err error) {
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if stream.currentWriter == nil {
			return
		}
		ek := stream.currentWriter.toKey() //first get currentExtent Key
		if ek.Size == 0 {
			return
		}

		updateKey := ek.GetExtentKey()
		lastUpdateExtentKeySize, ok := stream.hasUpdateKey[updateKey]
		if ok && lastUpdateExtentKeySize == int(ek.Size) {
			return nil
		}
		lastUpdateSize := 0
		if ok {
			lastUpdateSize = lastUpdateExtentKeySize
		}
		if lastUpdateSize == int(ek.Size) {
			return nil
		}
		err = stream.appendExtentKey(stream.Inode, ek) //put it to metanode
		if err == syscall.ENOENT {
			stream.exit()
			return
		}
		if err != nil {
			err = errors.Annotatef(err, "update extent(%v) to MetaNode Failed", ek.Size)
			log.LogErrorf("stream(%v) err(%v)", stream.toString(), err.Error())
			continue
		}
		stream.addHasUpdateToMetaNodeSize(int(ek.Size) - lastUpdateSize)
		stream.hasUpdateKey[updateKey] = int(ek.Size)
		return
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
			stream.setCurrentWriter(writer)
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
		dp       *wrapper.DataPartition
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

func (stream *StreamWriter) createExtent(dp *wrapper.DataPartition) (extentId uint64, err error) {
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

func (stream *StreamWriter) exit() {
	select {
	case stream.exitCh <- true:
	default:
	}
}

func (stream *StreamWriter) getHasWriteSize() uint64 {
	return atomic.LoadUint64(&stream.hasWriteSize)
}

func (stream *StreamWriter) addHasWriteSize(writed int) {
	atomic.AddUint64(&stream.hasWriteSize, uint64(writed))
}

func (stream *StreamWriter) setHasWriteSize(writeSize uint64) {
	atomic.StoreUint64(&stream.hasUpdateToMetaNodeSize, writeSize)
	atomic.StoreUint64(&stream.hasWriteSize, writeSize)

}

func (stream *StreamWriter) addHasUpdateToMetaNodeSize(writed int) {
	atomic.AddUint64(&stream.hasUpdateToMetaNodeSize, uint64(writed))
}

func (stream *StreamWriter) getHasUpdateToMetaNodeSize() uint64 {
	return atomic.LoadUint64(&stream.hasUpdateToMetaNodeSize)
}
