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
	"fmt"
	"hash/crc32"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/tracing"
	"golang.org/x/net/context"
)

const (
	MaxSelectDataPartitionForWrite = 32
	MaxNewHandlerRetry             = 3
	MaxPacketErrorCount            = 32
	MaxDirtyListLen                = 0
)

const (
	StreamerNormal int32 = iota
	StreamerError
)

const (
	streamWriterFlushPeriod       = 5
	streamWriterIdleTimeoutPeriod = 4
)

// OpenRequest defines an open request.
type OpenRequest struct {
	done chan struct{}
	ctx  context.Context
}

// WriteRequest defines a write request.
type WriteRequest struct {
	fileOffset int
	size       int
	data       []byte
	direct     bool
	writeBytes int
	isROW	   bool
	err        error
	done       chan struct{}
	ctx        context.Context
}

// FlushRequest defines a flush request.
type FlushRequest struct {
	err  error
	done chan struct{}
	ctx  context.Context
}

// ReleaseRequest defines a release request.
type ReleaseRequest struct {
	err  error
	done chan struct{}
	ctx  context.Context
}

// TruncRequest defines a truncate request.
type TruncRequest struct {
	size int
	err  error
	done chan struct{}
	ctx  context.Context
}

// EvictRequest defines an evict request.
type EvictRequest struct {
	err  error
	done chan struct{}
	ctx  context.Context
}

// Open request shall grab the lock until request is sent to the request channel
func (s *Streamer) IssueOpenRequest() error {
	request := openRequestPool.Get().(*OpenRequest)
	request.done = make(chan struct{}, 1)
	s.request <- request
	s.streamerMap.Unlock()
	<-request.done
	openRequestPool.Put(request)
	return nil
}

func (s *Streamer) IssueWriteRequest(ctx context.Context, offset int, data []byte, direct bool) (write int, isROW bool, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("StreamWrite.IssueWriteRequest").
		SetTag("arg.inode", s.inode).
		SetTag("arg.offset", offset).
		SetTag("arg.dataLen", len(data)).
		SetTag("arg.direct", direct)

	if atomic.LoadInt32(&s.status) >= StreamerError {
		tracer.SetTag("ret.err", "StreamerError")
		tracer.Finish()
		return 0, false, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}

	s.writeLock.Lock()
	atomic.AddInt32(&s.writeOp, 1)
	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.fileOffset = offset
	request.size = len(data)
	request.direct = direct
	request.done = make(chan struct{}, 1)
	request.isROW = false
	request.ctx = ctx
	tracer.SetTag("request.channel.len", len(s.request))
	s.request <- request
	s.writeLock.Unlock()

	tracer.Finish()

	<-request.done
	atomic.AddInt32(&s.writeOp, -1)
	err = request.err
	write = request.writeBytes
	isROW = request.isROW
	writeRequestPool.Put(request)
	return
}

func (s *Streamer) IssueFlushRequest(ctx context.Context) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.IssueFlushRequest")
	defer tracer.Finish()
	ctx = tracer.Context()

	if atomic.LoadInt32(&s.writeOp) <= 0 && s.dirtylist.Len() <= 0 {
		return nil
	}

	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	request.ctx = ctx
	s.request <- request
	<-request.done
	err := request.err
	flushRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueReleaseRequest(ctx context.Context) error {
	request := releaseRequestPool.Get().(*ReleaseRequest)
	request.done = make(chan struct{}, 1)
	request.ctx = ctx
	s.request <- request
	s.streamerMap.Unlock()
	<-request.done
	err := request.err
	releaseRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueTruncRequest(ctx context.Context, size int) error {
	request := truncRequestPool.Get().(*TruncRequest)
	request.size = size
	request.done = make(chan struct{}, 1)
	request.ctx = ctx
	s.request <- request
	<-request.done
	err := request.err
	truncRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueEvictRequest(ctx context.Context) error {
	request := evictRequestPool.Get().(*EvictRequest)
	request.done = make(chan struct{}, 1)
	request.ctx = ctx
	s.request <- request
	s.streamerMap.Unlock()
	<-request.done
	err := request.err
	evictRequestPool.Put(request)
	return err
}

func (s *Streamer) server() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	ctx := context.Background()

	for {
		select {
		case request := <-s.request:
			var tracer = tracing.NewTracer("Streamer.serverRequest")
			ctx = tracer.Context()

			s.handleRequest(ctx, request)
			s.idle = 0
			s.traversed = 0
			tracer.Finish()
		case <-s.done:
			s.abort()
			log.LogDebugf("done server: evict, ino(%v)", s.inode)
			return
		case <-t.C:
			s.traverse()
			if s.client.autoFlush {
				s.flush(ctx)
			}
			if s.refcnt <= 0 {
				s.streamerMap.Lock()
				if s.idle >= streamWriterIdleTimeoutPeriod && len(s.request) == 0 {
					delete(s.streamerMap.streamers, s.inode)
					if s.client.evictIcache != nil {
						s.client.evictIcache(ctx, s.inode)
					}
					s.streamerMap.Unlock()

					// fail the remaining requests in such case
					s.clearRequests()
					log.LogDebugf("done server: no requests for a long time, ino(%v)", s.inode)
					return
				}
				s.streamerMap.Unlock()
				s.idle++
			}
		}
	}
}

func (s *Streamer) clearRequests() {
	for {
		select {
		case request := <-s.request:
			s.abortRequest(request)
		default:
			return
		}
	}
}

func (s *Streamer) abortRequest(request interface{}) {
	switch request := request.(type) {
	case *OpenRequest:
		request.done <- struct{}{}
	case *WriteRequest:
		request.err = syscall.EAGAIN
		request.done <- struct{}{}
	case *TruncRequest:
		request.err = syscall.EAGAIN
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = syscall.EAGAIN
		request.done <- struct{}{}
	case *ReleaseRequest:
		request.err = syscall.EAGAIN
		request.done <- struct{}{}
	case *EvictRequest:
		request.err = syscall.EAGAIN
		request.done <- struct{}{}
	default:
	}
}

func (s *Streamer) handleRequest(ctx context.Context, request interface{}) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.handleRequest")
	defer tracer.Finish()

	switch request := request.(type) {
	case *OpenRequest:
		tracer.SetTag("open", true)
		s.open()
		request.done <- struct{}{}
	case *WriteRequest:
		tracer.SetTag("write", true)
		tracer.SetTag("offset", request.fileOffset)
		tracer.SetTag("size", request.size)
		request.writeBytes, request.isROW, request.err = s.write(request.ctx, request.data, request.fileOffset, request.size, request.direct)
		request.done <- struct{}{}
	case *TruncRequest:
		tracer.SetTag("trunc", true)
		request.err = s.truncate(request.ctx, request.size)
		request.done <- struct{}{}
	case *FlushRequest:
		tracer.SetTag("flush", true)
		request.err = s.flush(request.ctx)
		request.done <- struct{}{}
	case *ReleaseRequest:
		tracer.SetTag("release", true)
		request.err = s.release(request.ctx)
		request.done <- struct{}{}
	case *EvictRequest:
		tracer.SetTag("evict", true)
		request.err = s.evict(request.ctx)
		request.done <- struct{}{}
	default:
	}
}

func (s *Streamer) write(ctx context.Context, data []byte, offset, size int, direct bool) (total int, isROW bool, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.write").
		SetTag("offset", offset).
		SetTag("size", size).
		SetTag("direct", direct)
	defer tracer.Finish()
	ctx = tracer.Context()

	log.LogDebugf("Streamer write enter: ino(%v) offset(%v) size(%v)", s.inode, offset, size)

	s.client.writeLimiter.Wait(ctx)

	requests := s.extents.PrepareRequests(offset, size, data)
	log.LogDebugf("Streamer write: ino(%v) prepared requests(%v)", s.inode, requests)

	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		err = s.flush(ctx)
		if err != nil {
			return
		}
		requests = s.extents.PrepareRequests(offset, size, data)
		log.LogDebugf("Streamer write: ino(%v) prepared requests after flush(%v)", s.inode, requests)
	}

	for _, req := range requests {
		var (
			writeSize 	int
			rowFlag		bool
		)
		if req.ExtentKey != nil {
			writeSize, rowFlag = s.doOverWriteOrROW(ctx, req, direct)
		} else {
			writeSize, err = s.doWrite(ctx, req.Data, req.FileOffset, req.Size, direct)
		}
		if err != nil {
			log.LogWarnf("Streamer write: ino(%v) err(%v)", s.inode, err)
			break
		}
		if rowFlag {
			isROW = rowFlag
		}
		total += writeSize
	}
	if filesize, _ := s.extents.Size(); offset+total > filesize {
		s.extents.SetSize(uint64(offset+total), false)
		log.LogDebugf("Streamer write: ino(%v) filesize changed to (%v)", s.inode, offset+total)
	}
	log.LogDebugf("Streamer write exit: ino(%v) offset(%v) size(%v) done total(%v) isROW(%v) err(%v)", s.inode, offset, size, total, isROW, err)
	return
}

func (s *Streamer) doOverWriteOrROW(ctx context.Context, req *ExtentRequest, direct bool) (writeSize int, isROW bool) {
	var (
		err    error
		errmsg string
	)
	tryCount := 0
	for {
		tryCount++
		if tryCount%100 == 0 {
			log.LogWarnf("doOverWriteOrROW failed: try (%v)th times, ino(%v) req(%v)", tryCount, s.inode, req)
		}
		if writeSize, err = s.doOverwrite(ctx, req, direct); err == nil {
			break
		}
		log.LogWarnf("doOverWrite failed: ino(%v) err(%v) req(%v)", s.inode, err, req)
		if writeSize, err = s.doROW(ctx, req, direct); err == nil {
			isROW = true
			break
		}
		log.LogWarnf("doOverWriteOrROW failed: ino(%v) err(%v) req(%v)", s.inode, err, req)
		errmsg = fmt.Sprintf("doOverWrite and doROW err(%v) inode(%v) req(%v) try count(%v)", err, s.inode, req, tryCount)
		handleUmpAlarm(s.client.dataWrapper.clusterName, s.client.dataWrapper.volName, "doOverWriteOrROW", errmsg)
		time.Sleep(1 * time.Second)
	}
	return writeSize, isROW
}

func (s *Streamer) writeToExtent(ctx context.Context, oriReq *ExtentRequest, dp *DataPartition, extID int,
	direct bool) (total int, err error) {
	size := oriReq.Size
	var conn *net.TCPConn
	conn, err = StreamConnPool.GetConnect(dp.Hosts[0])
	if err != nil {
		return
	}
	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()

	for total < size {
		currSize := util.Min(size-total, util.OverWritePacketSizeLimit)
		packet := NewROWPacket(ctx, dp, s.inode, extID, oriReq.FileOffset+total, total, currSize)
		if direct {
			packet.Opcode = proto.OpSyncWrite
		}
		packet.Data = oriReq.Data[total : total+currSize]
		packet.CRC = crc32.ChecksumIEEE(packet.Data[:packet.Size])
		err = packet.WriteToConn(conn)
		if err != nil {
			break
		}
		reply := NewReply(packet.Ctx(), packet.ReqID, packet.PartitionID, packet.ExtentID)
		err = reply.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil || reply.ResultCode != proto.OpOk || !packet.isValidWriteReply(reply) || reply.CRC != packet.CRC {
			err = fmt.Errorf("err[%v]-packet[%v]-reply[%v]", err, packet, reply)
			break
		}
		log.LogDebugf("writeToExtent: inode %v packet %v total %v currSize %v", s.inode, packet, total, currSize)
		total += currSize
	}
	log.LogDebugf("writeToExtent: inode %v oriReq %v dp %v extID %v total %v direct %v", s.inode, oriReq, dp, extID, total, direct)
	return
}

func (s *Streamer) writeToNewExtent(ctx context.Context, oriReq *ExtentRequest, direct bool) (dp *DataPartition,
	extID, total int, err error) {
	defer func() {
		if err != nil {
			log.LogWarnf("writeToNewExtent: oriReq %v exceed max retry times(%v), err %v",
				oriReq, MaxSelectDataPartitionForWrite, err)
		}
		log.LogDebugf("writeToNewExtent: inode %v, oriReq %v direct %v", s.inode, oriReq, direct)
	}()

	exclude := make(map[string]struct{})
	for i := 0; i < MaxSelectDataPartitionForWrite; i++ {
		if err != nil {
			log.LogWarnf("writeToNewExtent: oriReq %v, dp %v, extID %v, total %v, err %v, retry(%v/%v)",
				oriReq, dp, extID, total, err, i, MaxSelectDataPartitionForWrite)
			dp, extID, total = nil, 0, 0
		}

		dp, err = s.client.dataWrapper.GetDataPartitionForWrite(exclude)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		extID, err = CreateExtent(ctx, s.inode, dp)
		if err != nil {
			continue
		}
		total, err = s.writeToExtent(ctx, oriReq, dp, extID, direct)
		if err == nil {
			break
		}
	}
	return
}

func (s *Streamer) doROW(ctx context.Context, oriReq *ExtentRequest, direct bool) (total int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.doROW").
		SetTag("direct", direct).
		SetTag("req.Size", oriReq.Size).
		SetTag("req.ExtentKey", oriReq.ExtentKey).
		SetTag("req.FileOffset", oriReq.FileOffset)
	defer func() {
		tracer.Finish()
		if err != nil {
			log.LogWarnf("doROW: total %v, oriReq %v, err %v", total, oriReq, err)
		}
	}()
	ctx = tracer.Context()

	err = s.flush(ctx)
	if err != nil {
		return
	}

	// close handler in case of extent key overwriting in following append write
	s.closeOpenHandler(ctx)

	var dp *DataPartition
	var extID int
	dp, extID, total, err = s.writeToNewExtent(ctx, oriReq, direct)
	if err != nil {
		return
	}

	newEK := &proto.ExtentKey{
		FileOffset:  uint64(oriReq.FileOffset),
		PartitionId: dp.PartitionID,
		ExtentId:    uint64(extID),
		Size:        uint32(oriReq.Size),
	}

	err = s.client.insertExtentKey(ctx, s.inode, *newEK)
	if err != nil {
		return
	}
	s.extents.Lock()
	s.extents.gen = 0
	s.extents.Unlock()

	getExtentsErr := s.GetExtents(ctx)

	log.LogWarnf("doROW: inode %v, total %v, oriReq %v, getExtentsErr %v, newEK %v", s.inode, total, oriReq, getExtentsErr, newEK)

	return
}

func (s *Streamer) doOverwrite(ctx context.Context, req *ExtentRequest, direct bool) (total int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.doOverwrite").
		SetTag("direct", direct).
		SetTag("req.Size", req.Size).
		SetTag("req.ExtentKey", req.ExtentKey).
		SetTag("req.FileOffset", req.FileOffset)
	defer tracer.Finish()
	ctx = tracer.Context()

	var dp *DataPartition

	err = s.flush(ctx)
	if err != nil {
		return
	}

	offset := req.FileOffset
	size := req.Size

	// the extent key needs to be updated because when preparing the requests,
	// the obtained extent key could be a local key which can be inconsistent with the remote key.
	req.ExtentKey = s.extents.Get(uint64(offset))
	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) fileOffset(%v)", s.inode, offset))
		return
	}
	ekFileOffset := int(req.ExtentKey.FileOffset)
	ekExtOffset := int(req.ExtentKey.ExtentOffset)

	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		err = errors.Trace(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	sc := NewStreamConn(dp, false)

	for total < size {
		reqPacket := NewOverwritePacket(ctx, dp, req.ExtentKey.ExtentId, offset-ekFileOffset+total+ekExtOffset, s.inode, offset)
		if direct {
			reqPacket.Opcode = proto.OpSyncRandomWrite
		}
		packSize := util.Min(size-total, util.OverWritePacketSizeLimit)
		reqPacket.Data = req.Data[total : total+packSize]
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		replyPacket := new(Packet)
		err = dp.OverWrite(sc, reqPacket, replyPacket)

		reqPacket.Data = nil
		log.LogDebugf("doOverwrite: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			err = errors.New(fmt.Sprintf("doOverwrite: failed or reply NOK: err(%v) ino(%v) req(%v) replyPacket(%v)", err, s.inode, req, replyPacket))
			break
		}

		if !reqPacket.isValidWriteReply(replyPacket) || reqPacket.CRC != replyPacket.CRC {
			err = errors.New(fmt.Sprintf("doOverwrite: is not the corresponding reply, ino(%v) req(%v) replyPacket(%v)", s.inode, req, replyPacket))
			break
		}

		total += packSize
	}

	return
}

func (s *Streamer) doWrite(ctx context.Context, data []byte, offset, size int, direct bool) (total int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.doWrite").
		SetTag("size", size).
		SetTag("direct", direct).
		SetTag("offset", offset)
	defer tracer.Finish()
	ctx = tracer.Context()

	var (
		ek *proto.ExtentKey
	)

	log.LogDebugf("doWrite enter: ino(%v) offset(%v) size(%v)", s.inode, offset, size)

	for i := 0; i < MaxNewHandlerRetry; i++ {
		if s.handler == nil {
			storeMode := proto.TinyExtentType

			if offset != 0 || offset+size > s.tinySizeLimit() {
				storeMode = proto.NormalExtentType
			}

			log.LogDebugf("doWrite: NewExtentHandler ino(%v) offset(%v) size(%v) storeMode(%v)",
				s.inode, offset, size, storeMode)

			// not use preExtent if once failed
			if i > 0 || !s.usePreExtentHandler(offset, size) {
				s.handler = NewExtentHandler(s, offset, storeMode)
			}
			s.dirty = false
		}

		ek, err = s.handler.write(ctx, data, offset, size, direct)
		if err == nil && ek != nil {
			if !s.dirty {
				s.dirtylist.Put(s.handler)
				s.dirty = true
			}
			break
		}

		s.closeOpenHandler(ctx)
	}

	if err != nil || ek == nil {
		log.LogWarnf("doWrite error: ino(%v) offset(%v) size(%v) err(%v) ek(%v)", s.inode, offset, size, err, ek)
		return
	}

	s.extents.Append(ek, false)
	total = size

	log.LogDebugf("doWrite exit: ino(%v) offset(%v) size(%v) ek(%v)", s.inode, offset, size, ek)
	return
}

func (s *Streamer) flush(ctx context.Context) (err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.flush")
	defer tracer.Finish()
	ctx = tracer.Context()

	for {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		log.LogDebugf("Streamer flush begin: eh(%v)", eh)
		err = eh.flush(ctx)
		if err != nil {
			log.LogWarnf("Streamer flush failed: eh(%v)", eh)
			return
		}
		eh.stream.dirtylist.Remove(element)
		if eh.getStatus() == ExtentStatusOpen {
			s.dirty = false
			log.LogDebugf("Streamer flush handler open: eh(%v)", eh)
		} else {
			// TODO unhandled error
			eh.cleanup()
			log.LogDebugf("Streamer flush handler cleaned up: eh(%v)", eh)
		}
		log.LogDebugf("Streamer flush end: eh(%v)", eh)
	}
	return
}

func (s *Streamer) traverse() (err error) {
	ctx := context.Background()
	if tracing.IsEnabled() {
		var tracer = tracing.NewTracer("Streamer.traverse")
		defer tracer.Finish()
		ctx = tracer.Context()
	}

	s.traversed++
	length := s.dirtylist.Len()
	for i := 0; i < length; i++ {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		log.LogDebugf("Streamer traverse begin: eh(%v)", eh)
		if eh.getStatus() >= ExtentStatusClosed {
			// handler can be in different status such as close, recovery, and error,
			// and therefore there can be packet that has not been flushed yet.
			eh.flushPacket(ctx)
			if atomic.LoadInt32(&eh.inflight) > 0 {
				log.LogDebugf("Streamer traverse skipped: non-zero inflight, eh(%v)", eh)
				continue
			}
			err = eh.appendExtentKey(ctx)
			if err != nil {
				log.LogWarnf("Streamer traverse abort: insertExtentKey failed, eh(%v) err(%v)", eh, err)
				return
			}
			s.dirtylist.Remove(element)
			eh.cleanup()
		} else {
			if s.traversed < streamWriterFlushPeriod {
				log.LogDebugf("Streamer traverse skipped: traversed(%v) eh(%v)", s.traversed, eh)
				continue
			}
			eh.setClosed()
		}
		log.LogDebugf("Streamer traverse end: eh(%v)", eh)
	}

	if s.status >= StreamerError && s.dirtylist.Len() == 0 {
		log.LogWarnf("Streamer traverse clean dirtyList success, set s(%v) status from (%v) to (%v)", s, s.status,
			StreamerNormal)
		atomic.StoreInt32(&s.status, StreamerNormal)
	}

	return
}

func (s *Streamer) closeOpenHandler(ctx context.Context) {
	if s.handler != nil {
		s.handler.setClosed()
		if s.dirtylist.Len() < MaxDirtyListLen {
			s.handler.flushPacket(ctx)
		} else {
			// flush all handler when close current handler, to prevent extent key overwriting
			s.flush(ctx)
		}

		if !s.dirty {
			// in case the current handler is not on the dirty list and will not get cleaned up
			// TODO unhandled error
			s.handler.cleanup()
		}
		s.handler = nil
	}
}

func (s *Streamer) open() {
	s.refcnt++
	log.LogDebugf("open: streamer(%v) refcnt(%v)", s, s.refcnt)
}

func (s *Streamer) release(ctx context.Context) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.release")
	defer tracer.Finish()
	ctx = tracer.Context()

	s.refcnt--
	s.closeOpenHandler(ctx)
	err := s.flush(ctx)
	if err != nil {
		s.abort()
	}
	log.LogDebugf("release: streamer(%v) refcnt(%v)", s, s.refcnt)
	return err
}

func (s *Streamer) evict(ctx context.Context) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.evict")
	defer tracer.Finish()
	ctx = tracer.Context()

	s.streamerMap.Lock()
	if s.refcnt > 0 || len(s.request) != 0 {
		s.streamerMap.Unlock()
		return errors.New(fmt.Sprintf("evict: streamer(%v) refcnt(%v)", s, s.refcnt))
	}
	log.LogDebugf("evict: inode(%v)", s.inode)
	delete(s.streamerMap.streamers, s.inode)
	s.streamerMap.Unlock()
	return nil
}

func (s *Streamer) abort() {
	for {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)
		s.dirtylist.Remove(element)
		// TODO unhandled error
		eh.cleanup()
	}
}

func (s *Streamer) truncate(ctx context.Context, size int) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.truncate").
		SetTag("size", size)
	defer tracer.Finish()
	ctx = tracer.Context()

	s.closeOpenHandler(ctx)
	err := s.flush(ctx)
	if err != nil {
		return err
	}

	oldSize, _ := s.extents.Size()
	log.LogDebugf("streamer truncate: inode(%v) oldSize(%v) size(%v)", s.inode, oldSize, size)

	err = s.client.truncate(ctx, s.inode, uint64(oldSize), uint64(size))
	if err != nil {
		return err
	}

	if oldSize <= size {
		s.extents.SetSize(uint64(size), true)
		return nil
	}

	s.extents.Lock()
	s.extents.gen = 0
	s.extents.Unlock()

	return s.GetExtents(ctx)
}

func (s *Streamer) tinySizeLimit() int {
	return s.tinySize
}

//func (s *Streamer) extentMerge(ctx context.Context, req *ExtentRequest) (err error, newReq *ExtentRequest, writeSize int) {
//	if !s.isNeedMerge(req) {
//		return
//	}
//
//	var tracer = tracing.TracerFromContext(ctx).ChildTracer("Streamer.extentMerge")
//	defer tracer.Finish()
//	ctx = tracer.Context()
//
//	defer func() {
//		if err != nil {
//			log.LogWarnf("extentMerge: extentMerge failed, err(%v), req(%v), newReq(%v), writeSize(%v)",
//				err, req, newReq, writeSize)
//		} else {
//			log.LogDebugf("extentMerge: extentMerge success, req(%v), newReq(%v), writeSize(%v)",
//				req, newReq, writeSize)
//		}
//	}()
//
//	alignSize := s.client.AlignSize()
//
//	mergeStart := req.FileOffset / alignSize * alignSize
//	preSize := req.FileOffset - mergeStart
//	mergeSize := alignSize
//	if preSize+req.Size < alignSize {
//		mergeSize = preSize + req.Size
//	}
//	mergeData := make([]byte, mergeSize)
//
//	_, err = s.read(ctx, mergeData, mergeStart, preSize)
//	if err != nil {
//		return
//	}
//
//	writeSize = mergeSize - preSize
//	copy(mergeData[preSize:], req.Data[:writeSize])
//
//	_, err = s.doWrite(ctx, mergeData, mergeStart, mergeSize, false)
//	if err != nil {
//		return
//	}
//
//	err = s.flush(ctx)
//	if err != nil {
//		return
//	}
//
//	if writeSize == req.Size {
//		return
//	}
//
//	newReqOffset := (req.FileOffset/alignSize + 1) * alignSize
//	newReqSize := req.FileOffset + req.Size - newReqOffset
//	if newReqSize > 0 {
//		newReq = NewExtentRequest(newReqOffset, newReqSize, req.Data[writeSize:], nil)
//	}
//	return
//}
//
//func (s *Streamer) isNeedMerge(req *ExtentRequest) bool {
//	alignSize := s.client.AlignSize()
//	maxExtent := s.client.MaxExtentNumPerAlignArea()
//	force := s.client.ForceAlignMerge()
//
//	if s.handler != nil {
//		return false
//	}
//
//	if req.Size >= alignSize {
//		return false
//	}
//
//	// If this req.FileOffset equal an alignArea start offset, it will nevel need merge.
//	if req.FileOffset == (req.FileOffset)/alignSize*alignSize {
//		return false
//	}
//
//	// In forceAlignMerge mode, when req across alignArea, it will always need merge.
//	if force && (req.FileOffset/alignSize != (req.FileOffset+req.Size)/alignSize) {
//		log.LogDebugf("isNeedMerge true: forceAlignMerge(%v), req(%v) across alignArea(%v).",
//			force, req, alignSize)
//		return true
//	}
//
//	if maxExtent == 0 {
//		return false
//	}
//
//	// Determine whether the current extent number has reached to maxExtent
//	alignStartOffset := req.FileOffset / alignSize * alignSize
//	alignEndOffset := alignStartOffset + alignSize - 1
//	pivot := &proto.ExtentKey{FileOffset: uint64(alignStartOffset)}
//	upper := &proto.ExtentKey{FileOffset: uint64(alignEndOffset)}
//	lower := &proto.ExtentKey{}
//
//	s.extents.RLock()
//	defer s.extents.RUnlock()
//
//	s.extents.root.DescendLessOrEqual(pivot, func(i btree.Item) bool {
//		ek := i.(*proto.ExtentKey)
//		lower.FileOffset = ek.FileOffset
//		return false
//	})
//
//	extentNum := int(0)
//	s.extents.root.AscendRange(lower, upper, func(i btree.Item) bool {
//		extentNum++
//		if extentNum >= maxExtent {
//			return false
//		}
//		return true
//	})
//
//	if extentNum >= maxExtent {
//		log.LogDebugf("isNeedMerge true: current extent numbers(%v) reached to maxExtent(%v).", extentNum, maxExtent)
//		return true
//	}
//
//	return false
//}

func (s *Streamer) usePreExtentHandler(offset, size int) bool {
	preEk := s.extents.Pre(uint64(offset))
	if preEk == nil ||
		s.dirtylist.Len() != 0 ||
		storage.IsTinyExtent(preEk.ExtentId) ||
		preEk.FileOffset+uint64(preEk.Size) != uint64(offset) ||
		int(preEk.Size)+int(preEk.ExtentOffset)+size > s.extentSize {
		return false
	}

	log.LogDebugf("usePreExtentHandler: ino(%v) offset(%v) size(%v) preEk(%v)",
		s.inode, offset, size, preEk)

	var (
		dp   *DataPartition
		conn *net.TCPConn
		err  error
	)

	if dp, err = s.client.dataWrapper.GetDataPartition(preEk.PartitionId); err != nil {
		log.LogWarnf("usePreExtentHandler: GetDataPartition(%v) failed, err(%v)", preEk.PartitionId, err)
		return false
	}

	if conn, err = StreamConnPool.GetConnect(dp.Hosts[0]); err != nil {
		log.LogWarnf("usePreExtentHandler: GetConnect(%v) failed, err(%v)", dp, err)
		return false
	}

	s.handler = NewExtentHandler(s, int(preEk.FileOffset), proto.NormalExtentType)

	s.handler.dp = dp
	s.handler.extID = int(preEk.ExtentId)
	s.handler.key = &proto.ExtentKey{
		FileOffset:   preEk.FileOffset,
		PartitionId:  preEk.PartitionId,
		ExtentId:     preEk.ExtentId,
		ExtentOffset: preEk.ExtentOffset,
		Size:         preEk.Size,
		CRC:          preEk.CRC,
	}
	s.handler.size = int(preEk.Size)
	s.handler.conn = conn
	s.handler.extentOffset = int(preEk.ExtentOffset)

	return true
}
