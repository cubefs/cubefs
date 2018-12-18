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
	"hash/crc32"
	"net"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	MaxSelectDataPartionForWrite = 32
	MaxNewHandlerRetry           = 3
)

const (
	StreamWriterNormal int32 = iota
	StreamWriterError
)

type WriteRequest struct {
	fileOffset int
	size       int
	data       []byte
	writeBytes int
	err        error
	done       chan struct{}
}

type FlushRequest struct {
	err  error
	done chan struct{}
}

type CloseRequest struct {
	err  error
	done chan struct{}
}

type TruncRequest struct {
	size int
	err  error
	done chan struct{}
}

type StreamWriter struct {
	stream *Streamer
	inode  uint64
	status int32

	excludePartition []uint32

	handler   *ExtentHandler   // current open handler
	dirtylist *ExtentDirtyList // dirty handlers
	dirty     bool             // whether current open handler is in the dirty list

	request chan interface{} // request channel, write/flush/close
	done    chan struct{}    // stream writer is being closed
}

func NewStreamWriter(stream *Streamer, inode uint64) *StreamWriter {
	sw := new(StreamWriter)
	sw.stream = stream
	sw.inode = inode
	sw.request = make(chan interface{}, 1000)
	sw.done = make(chan struct{})
	sw.excludePartition = make([]uint32, 0)
	sw.dirtylist = NewExtentDirtyList()
	go sw.server()
	return sw
}

func (sw *StreamWriter) IssueWriteRequest(offset int, data []byte) (write int, err error) {
	if atomic.LoadInt32(&sw.status) >= StreamWriterError {
		return 0, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", sw.inode))
	}

	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.fileOffset = offset
	request.size = len(data)
	request.done = make(chan struct{}, 1)
	sw.request <- request
	<-request.done
	err = request.err
	write = request.writeBytes
	writeRequestPool.Put(request)
	return
}

func (sw *StreamWriter) IssueFlushRequest() error {
	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	sw.request <- request
	<-request.done
	err := request.err
	flushRequestPool.Put(request)
	return err
}

func (sw *StreamWriter) IssueCloseRequest() error {
	request := closeRequestPool.Get().(*CloseRequest)
	request.done = make(chan struct{}, 1)
	sw.request <- request
	<-request.done
	err := request.err
	closeRequestPool.Put(request)
	sw.done <- struct{}{}
	return err
}

func (sw *StreamWriter) IssueTruncRequest(size int) error {
	request := truncRequestPool.Get().(*TruncRequest)
	request.size = size
	request.done = make(chan struct{}, 1)
	sw.request <- request
	<-request.done
	err := request.err
	truncRequestPool.Put(request)
	return err
}

func (sw *StreamWriter) server() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	for {
		select {
		case request := <-sw.request:
			sw.handleRequest(request)
		case <-sw.done:
			sw.close()
			return
		case <-t.C:
			sw.traverse()
		}
	}
}

func (sw *StreamWriter) handleRequest(request interface{}) (err error) {
	switch request := request.(type) {
	case *WriteRequest:
		request.writeBytes, request.err = sw.write(request.data, request.fileOffset, request.size)
		request.done <- struct{}{}
		err = request.err
	case *TruncRequest:
		request.err = sw.truncate(request.size)
		request.done <- struct{}{}
		err = request.err
	case *FlushRequest:
		request.err = sw.flush()
		request.done <- struct{}{}
		err = request.err
	case *CloseRequest:
		request.err = sw.close()
		request.done <- struct{}{}
		err = request.err
	default:
	}
	return
}

func (sw *StreamWriter) write(data []byte, offset, size int) (total int, err error) {
	log.LogDebugf("StreamWriter write enter: ino(%v) offset(%v) size(%v)", sw.inode, offset, size)

	requests := sw.stream.extents.PrepareWriteRequest(offset, size, data)
	log.LogDebugf("StreamWriter write: ino(%v) prepared requests(%v)", sw.inode, requests)
	for _, req := range requests {
		var writeSize int
		if req.ExtentKey != nil {
			writeSize, err = sw.doOverwrite(req)
		} else {
			writeSize, err = sw.doWrite(req.Data, req.FileOffset, req.Size)
		}
		if err != nil {
			log.LogErrorf("StreamWriter write: ino(%v) err(%v)", sw.inode, err)
			break
		}
		total += writeSize
	}
	if filesize, _ := sw.stream.extents.Size(); offset+total > filesize {
		sw.stream.extents.SetSize(uint64(offset + total))
		log.LogDebugf("StreamWriter write: ino(%v) filesize changed to (%v)", sw.inode, offset+total)
	}
	log.LogDebugf("StreamWriter write exit: ino(%v) offset(%v) size(%v) done total(%v) err(%v)", sw.inode, offset, size, total, err)
	return
}

func (sw *StreamWriter) doOverwrite(req *ExtentRequest) (total int, err error) {
	var dp *wrapper.DataPartition

	err = sw.flush()
	if err != nil {
		return
	}

	offset := req.FileOffset
	size := req.Size
	ekOffset := int(req.ExtentKey.FileOffset)

	// extent key should be updated, since during prepare requests,
	// the extent key obtained might be a local key which is not accurate.
	req.ExtentKey = sw.stream.extents.Get(uint64(offset))
	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ekOffset(%v) ek(%v)", sw.inode, ekOffset, req.ExtentKey))
		return
	}

	if dp, err = gDataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		errors.Annotatef(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", sw.inode, req.ExtentKey)
		return
	}

	sc := NewStreamConn(dp)

	for total < size {
		reqPacket := NewWritePacket(dp, req.ExtentKey.ExtentId, offset-ekOffset+total, sw.inode, offset, true)
		packSize := util.Min(size-total, util.BlockSize)
		copy(reqPacket.Data[:packSize], req.Data[total:total+packSize])
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		replyPacket := new(Packet)
		err = sc.Send(reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime)
			if e != nil {
				return errors.Annotatef(e, "Stream Writer doOverwrite: ino(%v) failed to read from connect", sw.inode), false
			}

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpNotLeaderErr {
				e = NotLeaderError
			}
			return e, false
		})

		log.LogDebugf("doOverwrite: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", sw.inode, req, reqPacket, err, replyPacket)

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			err = errors.New(fmt.Sprintf("doOverwrite: failed or reply NOK: err(%v) ino(%v) req(%v) replyPacket(%v)", err, sw.inode, req, replyPacket))
			break
		}

		if !reqPacket.IsEqualWriteReply(replyPacket) || reqPacket.CRC != replyPacket.CRC {
			err = errors.New(fmt.Sprintf("doOverwrite: is not the corresponding reply, ino(%v) req(%v) replyPacket(%v)", sw.inode, req, replyPacket))
			break
		}

		total += packSize
	}

	return
}

func (sw *StreamWriter) doWrite(data []byte, offset, size int) (total int, err error) {
	var (
		ek *proto.ExtentKey
	)

	log.LogDebugf("doWrite enter: ino(%v) offset(%v) size(%v)", sw.inode, offset, size)

	for i := 0; i < MaxNewHandlerRetry; i++ {
		if sw.handler == nil {
			sw.handler = NewExtentHandler(sw, offset, proto.NormalExtentMode)
			sw.dirty = false
		}

		ek, err = sw.handler.Write(data, offset, size)
		if err == nil && ek != nil {
			if !sw.dirty {
				sw.dirtylist.Put(sw.handler)
				sw.dirty = true
			}
			break
		}

		sw.closeOpenHandler()
	}

	if err != nil || ek == nil {
		log.LogErrorf("doWrite error: ino(%v) offset(%v) size(%v) err(%v) ek(%v)", sw.inode, offset, size, err, ek)
		return
	}

	sw.stream.extents.Append(ek, false)
	total = size

	log.LogDebugf("doWrite exit: ino(%v) offset(%v) size(%v) ek(%v)", sw.inode, offset, size, ek)
	return
}

func (sw *StreamWriter) flush() (err error) {
	for {
		element := sw.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		log.LogDebugf("StreamWriter flush begin: eh(%v)", eh)
		err = eh.flush()
		if err != nil {
			log.LogErrorf("StreamWriter flush failed: eh(%v)", eh)
			return
		}
		eh.sw.dirtylist.Remove(element)
		if eh.getStatus() == ExtentStatusOpen {
			sw.dirty = false
			log.LogDebugf("StreamWriter flush handler open: eh(%v)", eh)
		} else {
			eh.cleanup()
			log.LogDebugf("StreamWriter flush handler cleaned up: eh(%v)", eh)
		}
		log.LogDebugf("StreamWriter flush end: eh(%v)", eh)
	}
	return
}

func (sw *StreamWriter) traverse() (err error) {
	//var closed bool

	len := sw.dirtylist.Len()
	for i := 0; i < len; i++ {
		element := sw.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		log.LogDebugf("StreamWriter traverse begin: eh(%v)", eh)
		if eh.getStatus() >= ExtentStatusClosed && atomic.LoadInt32(&eh.inflight) <= 0 {
			err = eh.appendExtentKey()
			if err != nil {
				return
			}
			sw.dirtylist.Remove(element)
			eh.cleanup()
		}
		//		closed, err = eh.flush()
		//		if err != nil {
		//			return
		//		}
		//		if closed {
		//			sw.dirtylist.Remove(element)
		//		}
		log.LogDebugf("StreamWriter traverse end: eh(%v)", eh)
	}
	return
}

func (sw *StreamWriter) closeOpenHandler() {
	if sw.handler != nil {
		sw.handler.flushPacket()
		sw.handler.setClosed()
		if !sw.dirty {
			// in case current handler is not in the dirty list,
			// and will not get cleaned up.
			sw.handler.cleanup()
		}
		sw.handler = nil
	}
}

func (sw *StreamWriter) close() (err error) {
	sw.closeOpenHandler()
	err = sw.flush()
	if err != nil {
		sw.abort()
	}
	return
}

func (sw *StreamWriter) abort() {
	for {
		element := sw.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)
		sw.dirtylist.Remove(element)
		eh.cleanup()
	}
}

func (sw *StreamWriter) truncate(size int) error {
	sw.closeOpenHandler()
	if err := sw.flush(); err != nil {
		return err
	}
	return sw.stream.client.truncate(sw.inode, uint64(size))
}
