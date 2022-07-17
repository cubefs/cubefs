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
	fileOffset      uint64
	size            int
	data            []byte
	direct          bool
	overWriteBuffer bool
	writeBytes      int
	isROW           bool
	err             error
	done            chan struct{}
	ctx             context.Context
}

type FlushOverWriteRequest struct {
	write []*OverWriteRequest
	flush *FlushRequest
}

type OverWriteRequest struct {
	direct     bool
	oriReq     *ExtentRequest
	fileOffset int
	size       int
	data       []byte
	writeBytes int
	isROW      bool
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
	mustRelease bool
	err         error
	done        chan struct{}
	ctx         context.Context
}

// TruncRequest defines a truncate request.
type TruncRequest struct {
	size uint64
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

type ExtentMergeRequest struct {
	finish bool
	err    error
	done   chan struct{}
	ctx    context.Context
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

func GetWriteRequestFromPool() (request *WriteRequest) {
	request = writeRequestPool.Get().(*WriteRequest)
	request.data = nil
	request.size = 0
	if request.done == nil {
		request.done = make(chan struct{}, 1)
	}
	return
}

func (s *Streamer) IssueWriteRequest(ctx context.Context, offset uint64, data []byte, direct bool, overWriteBuffer bool) (write int, isROW bool, err error) {
	if atomic.LoadInt32(&s.status) >= StreamerError {
		return 0, false, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}

	s.writeLock.Lock()
	atomic.AddInt32(&s.writeOp, 1)
	request := GetWriteRequestFromPool()
	request.data = data
	request.fileOffset = offset
	request.size = len(data)
	request.direct = direct
	request.overWriteBuffer = overWriteBuffer
	request.done = make(chan struct{}, 1)
	request.isROW = false
	request.ctx = ctx
	//tracer.SetTag("request.channel.len", len(s.request))
	s.request <- request
	s.writeLock.Unlock()

	//tracer.Finish()

	<-request.done
	atomic.AddInt32(&s.writeOp, -1)
	err = request.err
	write = request.writeBytes
	isROW = request.isROW
	writeRequestPool.Put(request)
	return
}

func (s *Streamer) IssueFlushRequest(ctx context.Context) error {
	if atomic.LoadInt32(&s.writeOp) <= 0 && s.dirtylist.Len() <= 0 && len(s.overWriteReq) == 0 {
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

func (s *Streamer) IssueMustReleaseRequest(ctx context.Context) error {
	request := releaseRequestPool.Get().(*ReleaseRequest)
	request.done = make(chan struct{}, 1)
	request.mustRelease = true
	request.ctx = ctx
	s.request <- request
	s.streamerMap.Unlock()
	<-request.done
	err := request.err
	releaseRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueTruncRequest(ctx context.Context, size uint64) error {
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

func (s *Streamer) IssueExtentMergeRequest(ctx context.Context) (finish bool, err error) {
	request := &ExtentMergeRequest{}
	request.done = make(chan struct{}, 1)
	request.ctx = ctx
	s.request <- request
	<-request.done
	finish = request.finish
	err = request.err
	return
}

func (s *Streamer) server() {
	defer s.wg.Done()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	ctx := context.Background()

	for {
		select {
		case request := <-s.request:
			s.handleRequest(ctx, request)
			s.idle = 0
			s.traversed = 0
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
	switch request := request.(type) {
	case *OpenRequest:
		s.open()
		request.done <- struct{}{}
	case *WriteRequest:
		request.writeBytes, request.isROW, request.err = s.write(request.ctx, request.data, request.fileOffset, request.size, request.direct, request.overWriteBuffer)
		request.done <- struct{}{}
	case *TruncRequest:
		request.err = s.truncate(request.ctx, request.size)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = s.flush(request.ctx)
		if len(s.overWriteReq) > 0 {
			s.overWriteReqMutex.Lock()
			overWriteReq := s.overWriteReq
			s.overWriteReq = nil
			s.overWriteReqMutex.Unlock()
			for _, req := range overWriteReq {
				s.doOverWriteOrROW(request.ctx, req.oriReq, req.direct)
			}
		}
		request.done <- struct{}{}
	case *ReleaseRequest:
		request.err = s.release(request.ctx, request.mustRelease)
		request.done <- struct{}{}
	case *EvictRequest:
		request.err = s.evict(request.ctx)
		request.done <- struct{}{}
	case *ExtentMergeRequest:
		request.finish, request.err = s.extentMerge(request.ctx)
		request.done <- struct{}{}
	default:
	}
}

func (s *Streamer) write(ctx context.Context, data []byte, offset uint64, size int, direct bool, overWriteBuffer bool) (total int, isROW bool, err error) {
	if log.IsDebugEnabled() {
		log.LogDebugf("Streamer write enter: ino(%v) offset(%v) size(%v)", s.inode, offset, size)
	}
	ctx = context.Background()
	if s.client.writeRate > 0 {
		s.client.writeLimiter.Wait(ctx)
	}

	requests, _ := s.extents.PrepareRequests(offset, size, data)
	if log.IsDebugEnabled() {
		log.LogDebugf("Streamer write: ino(%v) prepared requests(%v)", s.inode, requests)
	}

	needFlush := false
	for _, req := range requests {
		if req.ExtentKey != nil && req.ExtentKey.PartitionId == 0 {
			needFlush = true
			break
		}
	}

	if needFlush {
		err = s.flush(ctx)
		if err != nil {
			return
		}
		requests, _ = s.extents.PrepareRequests(offset, size, data)
		if log.IsDebugEnabled() {
			log.LogDebugf("Streamer write: ino(%v) prepared requests after flush(%v)", s.inode, requests)
		}
	}

	var (
		writeSize int
		rowFlag   bool
	)
	if !s.enableOverwrite() && len(requests) > 1 {
		req := NewExtentRequest(offset, size, data, nil)
		writeSize, rowFlag, err = s.doOverWriteOrROW(ctx, req, direct)
		total += writeSize
	} else {
		for _, req := range requests {
			if req.ExtentKey != nil {
				// clear read ahead cache
				if s.readAhead && s.extentReader != nil && s.extentReader.key.PartitionId == req.ExtentKey.PartitionId && s.extentReader.key.ExtentId == req.ExtentKey.ExtentId && s.extentReader.req != nil {
					s.extentReader.reqMutex.Lock()
					s.extentReader.req = nil
					s.extentReader.reqMutex.Unlock()
				}
				if overWriteBuffer {
					writeSize = s.appendOverWriteReq(ctx, req, direct)
				} else {
					writeSize, rowFlag, err = s.doOverWriteOrROW(ctx, req, direct)
				}
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
	}

	if filesize, _ := s.extents.Size(); offset+uint64(total) > filesize {
		s.extents.SetSize(offset+uint64(total), false)
		if log.IsDebugEnabled() {
			log.LogDebugf("Streamer write: ino(%v) filesize changed to (%v)", s.inode, offset+uint64(total))
		}
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("Streamer write exit: ino(%v) offset(%v) size(%v) done total(%v) err(%v)", s.inode, offset, size, total, err)
	}
	return
}

func (s *Streamer) doOverWriteOrROW(ctx context.Context, req *ExtentRequest, direct bool) (writeSize int, isROW bool, err error) {
	var errmsg string
	tryCount := 0
	for {
		tryCount++
		if tryCount%100 == 0 {
			log.LogWarnf("doOverWriteOrROW failed: try (%v)th times, ino(%v) req(%v)", tryCount, s.inode, req)
		}
		if s.enableOverwrite() {
			if writeSize, err = s.doOverwrite(ctx, req, direct); err == nil {
				break
			}
			log.LogWarnf("doOverWrite failed: ino(%v) err(%v) req(%v)", s.inode, err, req)
		}
		if writeSize, err = s.doROW(ctx, req, direct); err == nil {
			isROW = true
			break
		}
		log.LogWarnf("doOverWriteOrROW failed: ino(%v) err(%v) req(%v)", s.inode, err, req)
		if err == syscall.ENOENT {
			break
		}
		errmsg = fmt.Sprintf("doOverWrite and doROW err(%v) inode(%v) req(%v) try count(%v)", err, s.inode, req, tryCount)
		handleUmpAlarm(s.client.dataWrapper.clusterName, s.client.dataWrapper.volName, "doOverWriteOrROW", errmsg)
		time.Sleep(1 * time.Second)
	}
	return writeSize, isROW, err
}

func (s *Streamer) enableOverwrite() bool {
	return !s.isForceROW() && !s.client.dataWrapper.CrossRegionHATypeQuorum()
}

func (s *Streamer) writeToExtent(ctx context.Context, oriReq *ExtentRequest, dp *DataPartition, extID int,
	direct bool, conn *net.TCPConn) (total int, err error) {
	size := oriReq.Size

	for total < size {
		currSize := util.Min(size-total, util.OverWritePacketSizeLimit)
		packet := NewROWPacket(ctx, dp, s.client.dataWrapper.quorum, s.inode, extID, oriReq.FileOffset+uint64(total), total, currSize)
		if direct {
			packet.Opcode = proto.OpSyncWrite
		}
		packet.Data = oriReq.Data[total : total+currSize]
		packet.CRC = crc32.ChecksumIEEE(packet.Data[:packet.Size])
		err = packet.WriteToConnNs(conn, s.client.dataWrapper.connConfig.WriteTimeoutNs)
		if err != nil {
			break
		}
		reply := NewReply(packet.Ctx(), packet.ReqID, packet.PartitionID, packet.ExtentID)
		err = reply.ReadFromConnNs(conn, s.client.dataWrapper.connConfig.ReadTimeoutNs)
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
		if log.IsDebugEnabled() {
			log.LogDebugf("writeToNewExtent: inode %v, oriReq %v direct %v", s.inode, oriReq, direct)
		}
	}()

	exclude := make(map[string]struct{})
	var conn *net.TCPConn
	for i := 0; i < MaxSelectDataPartitionForWrite; i++ {
		if err != nil {
			if dp != nil {
				dp.CheckAllHostsIsAvail(exclude)
				if isExcluded(dp, exclude, dp.ClientWrapper.quorum) {
					s.client.dataWrapper.RemoveDataPartitionForWrite(dp.PartitionID)
				}
			}
			log.LogWarnf("writeToNewExtent: stream %v, oriReq %v, dp %v, extID %v, total %v, err %v, retry(%v/%v) exclude(%v)",
				s, oriReq, dp, extID, total, err, i, MaxSelectDataPartitionForWrite, exclude)
			dp, extID, total = nil, 0, 0
		}

		dp, err = s.client.dataWrapper.GetDataPartitionForWrite(exclude)
		if err != nil {
			if len(exclude) > 0 {
				// if all dp is excluded, clean exclude map
				log.LogWarnf("writeToNewExtent: clean exclude because no writable partition, stream(%v) oriReq(%v) exclude(%v)",
					s, oriReq, exclude)
				exclude = make(map[string]struct{})
			}
			time.Sleep(5 * time.Second)
			continue
		}
		conn, err = StreamConnPool.GetConnect(dp.Hosts[0])
		if err != nil {
			log.LogWarnf("writeToNewExtent: failed to create connection, err(%v) dp(%v) exclude(%v)", err, dp, exclude)
			continue
		}
		extID, err = CreateExtent(ctx, conn, s.inode, dp, s.client.dataWrapper.quorum)
		if err != nil {
			StreamConnPool.PutConnectWithErr(conn, err)
			continue
		}
		total, err = s.writeToExtent(ctx, oriReq, dp, extID, direct, conn)
		StreamConnPool.PutConnectWithErr(conn, err)
		if err == nil {
			break
		}
	}
	return
}

func (s *Streamer) doROW(ctx context.Context, oriReq *ExtentRequest, direct bool) (total int, err error) {
	defer func() {
		if err != nil {
			log.LogWarnf("doROW: total %v, oriReq %v, err %v", total, oriReq, err)
		}
	}()

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

	s.extents.Insert(newEK, true)
	err = s.client.insertExtentKey(ctx, s.inode, *newEK, false)
	if err != nil {
		return
	}

	log.LogDebugf("doROW: inode %v, total %v, oriReq %v, newEK %v", s.inode, total, oriReq, newEK)

	return
}

func (s *Streamer) doOverwrite(ctx context.Context, req *ExtentRequest, direct bool) (total int, err error) {
	var dp *DataPartition
	offset := req.FileOffset
	size := req.Size
	ekFileOffset := req.ExtentKey.FileOffset
	ekExtOffset := int(req.ExtentKey.ExtentOffset)

	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		err = errors.Trace(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	sc := NewStreamConn(dp, false)

	for total < size {
		reqPacket := NewOverwritePacket(ctx, dp, req.ExtentKey.ExtentId, int(offset-ekFileOffset)+total+ekExtOffset, s.inode, offset)
		if direct {
			reqPacket.Opcode = proto.OpSyncRandomWrite
		}
		packSize := util.Min(size-total, util.OverWritePacketSizeLimit)
		reqPacket.Data = req.Data[total : total+packSize]
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		replyPacket := GetOverWritePacketFromPool()
		err = dp.OverWrite(sc, reqPacket, replyPacket)

		reqPacket.Data = nil
		if log.IsDebugEnabled() {
			log.LogDebugf("doOverwrite: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)
		}

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			err = errors.New(fmt.Sprintf("doOverwrite: failed or reply NOK: err(%v) ino(%v) req(%v) replyPacket(%v)", err, s.inode, req, replyPacket))
			break
		}

		if !reqPacket.isValidWriteReply(replyPacket) || reqPacket.CRC != replyPacket.CRC {
			err = errors.New(fmt.Sprintf("doOverwrite: is not the corresponding reply, ino(%v) req(%v) replyPacket(%v)", s.inode, req, replyPacket))
			break
		}
		PutOverWritePacketToPool(reqPacket)
		PutOverWritePacketToPool(replyPacket)

		total += packSize
	}

	return
}

func (s *Streamer) doWrite(ctx context.Context, data []byte, offset uint64, size int, direct bool) (total int, err error) {
	var (
		ek *proto.ExtentKey
	)
	if log.IsDebugEnabled() {
		log.LogDebugf("doWrite enter: ino(%v) offset(%v) size(%v)", s.inode, offset, size)
	}

	for i := 0; i < MaxNewHandlerRetry; i++ {
		if s.handler == nil {
			storeMode := proto.TinyExtentType

			if offset != 0 || offset+uint64(size) > uint64(s.tinySizeLimit()) {
				storeMode = proto.NormalExtentType
			}
			if log.IsDebugEnabled() {
				log.LogDebugf("doWrite: NewExtentHandler ino(%v) offset(%v) size(%v) storeMode(%v)",
					s.inode, offset, size, storeMode)
			}

			// not use preExtent if once failed
			if i > 0 || !s.usePreExtentHandler(offset, size) {
				s.handler = NewExtentHandler(s, offset, storeMode, s.appendWriteBuffer)
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

	s.extents.Insert(ek, false)
	total = size
	if log.IsDebugEnabled() {
		log.LogDebugf("doWrite exit: ino(%v) offset(%v) size(%v) ek(%v)", s.inode, offset, size, ek)
	}
	return
}

func (s *Streamer) appendOverWriteReq(ctx context.Context, oriReq *ExtentRequest, direct bool) (writeSize int) {
	var (
		req    *OverWriteRequest = &OverWriteRequest{oriReq: oriReq, direct: direct}
		offset int
	)
	writeSize = oriReq.Size

	s.overWriteReqMutex.Lock()
	defer s.overWriteReqMutex.Unlock()

	for _, curReq := range s.overWriteReq {
		if req.oriReq.ExtentKey.PartitionId != curReq.oriReq.ExtentKey.PartitionId ||
			req.oriReq.ExtentKey.ExtentId != curReq.oriReq.ExtentKey.ExtentId ||
			req.oriReq.FileOffset < curReq.oriReq.FileOffset ||
			req.oriReq.FileOffset > curReq.oriReq.FileOffset+uint64(curReq.oriReq.Size) {
			continue
		}

		offset = int(req.oriReq.FileOffset - curReq.oriReq.FileOffset)
		if req.oriReq.FileOffset+uint64(req.oriReq.Size) <= curReq.oriReq.FileOffset+uint64(curReq.oriReq.Size) {
			copy(curReq.oriReq.Data[offset:offset+req.oriReq.Size], req.oriReq.Data)
		} else if req.oriReq.FileOffset == curReq.oriReq.FileOffset+uint64(curReq.oriReq.Size) {
			curReq.oriReq.Data = append(curReq.oriReq.Data, req.oriReq.Data...)
			curReq.oriReq.Size = len(curReq.oriReq.Data)
		} else {
			copy(curReq.oriReq.Data[offset:], req.oriReq.Data[:curReq.oriReq.Size-offset])
			curReq.oriReq.Data = append(curReq.oriReq.Data, req.oriReq.Data[curReq.oriReq.Size-offset:]...)
			curReq.oriReq.Size = len(curReq.oriReq.Data)
		}
		return
	}

	data := make([]byte, len(req.oriReq.Data))
	copy(data, req.oriReq.Data)
	req.oriReq.Data = data
	s.overWriteReq = append(s.overWriteReq, req)
	//log.LogDebugf("appendOverWriteReq: ino(%v) req(%v)", s.inode, oriReq)
	return
}

func (s *Streamer) flush(ctx context.Context) (err error) {
	for {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)
		if log.IsDebugEnabled() {
			log.LogDebugf("Streamer flush begin: eh(%v)", eh)
		}
		err = eh.flush(ctx)
		if err != nil {
			log.LogWarnf("Streamer flush failed: eh(%v)", eh)
			return
		}
		eh.stream.dirtylist.Remove(element)
		if eh.getStatus() == ExtentStatusOpen {
			s.dirty = false
			if log.IsDebugEnabled() {
				log.LogDebugf("Streamer flush handler open: eh(%v)", eh)
			}
		} else {
			// TODO unhandled error
			eh.cleanup()
			if log.IsDebugEnabled() {
				log.LogDebugf("Streamer flush handler cleaned up: eh(%v)", eh)
			}
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("Streamer flush end: eh(%v)", eh)
		}
	}
	return
}

func (s *Streamer) traverse() (err error) {
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
			eh.flushPacket(nil)
			if atomic.LoadInt32(&eh.inflight) > 0 {
				log.LogDebugf("Streamer traverse skipped: non-zero inflight, eh(%v)", eh)
				continue
			}
			err = eh.appendExtentKey(nil)
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
		s.handlerMutex.Lock()
		defer s.handlerMutex.Unlock()
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

func (s *Streamer) release(ctx context.Context, mustRelease bool) error {
	if mustRelease {
		s.refcnt = 0
	} else {
		s.refcnt--
	}
	s.closeOpenHandler(ctx)
	err := s.flush(ctx)
	if err != nil {
		s.abort()
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("release: streamer(%v) refcnt(%v)", s, s.refcnt)
	}
	return err
}

func (s *Streamer) evict(ctx context.Context) error {
	s.streamerMap.Lock()
	if s.refcnt > 0 || len(s.request) != 0 {
		s.streamerMap.Unlock()
		return errors.New(fmt.Sprintf("evict: streamer(%v) refcnt(%v)", s, s.refcnt))
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("evict: inode(%v)", s.inode)
	}
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

func (s *Streamer) truncate(ctx context.Context, size uint64) error {
	s.closeOpenHandler(ctx)
	err := s.flush(ctx)
	if err != nil {
		return err
	}

	oldSize, _ := s.extents.Size()
	if log.IsDebugEnabled() {
		log.LogDebugf("streamer truncate: inode(%v) oldSize(%v) size(%v)", s.inode, oldSize, size)
	}
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

func (s *Streamer) extentMerge(ctx context.Context) (finish bool, err error) {
	var (
		reader       *ExtentReader
		readBytes    int
		readRequests []*ExtentRequest
		writeRequest *ExtentRequest
	)
	defer func() {
		msg := fmt.Sprintf("extentMerge: ino(%v) readRequests(%v) writeRequest(%v) finish(%v) err(%v)", s.inode, readRequests, writeRequest, finish, err)
		if err != nil {
			log.LogWarnf(msg)
		} else {
			log.LogDebugf(msg)
		}
	}()

	if err = s.flush(ctx); err != nil {
		return
	}

	readRequests, writeRequest, err = s.extents.prepareMergeRequests()
	if err != nil {
		return
	}
	if writeRequest == nil {
		finish = true
		return
	}

	for _, req := range readRequests {
		reader, err = s.GetExtentReader(req.ExtentKey)
		if err != nil {
			return
		}
		readBytes, err = reader.Read(ctx, req)
		if err != nil || readBytes < req.Size {
			return
		}
	}
	_, err = s.doROW(ctx, writeRequest, false)
	return
}

func (s *Streamer) usePreExtentHandler(offset uint64, size int) bool {
	preEk := s.extents.Pre(uint64(offset))
	if preEk == nil ||
		s.dirtylist.Len() != 0 ||
		storage.IsTinyExtent(preEk.ExtentId) ||
		preEk.FileOffset+uint64(preEk.Size) != uint64(offset) ||
		int(preEk.Size)+int(preEk.ExtentOffset)+size > s.extentSize {
		return false
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("usePreExtentHandler: ino(%v) offset(%v) size(%v) preEk(%v)",
			s.inode, offset, size, preEk)
	}
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

	s.handler = NewExtentHandler(s, preEk.FileOffset, proto.NormalExtentType, false)

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
	s.handler.isPreExtent = true
	s.handler.size = int(preEk.Size)
	s.handler.conn = conn
	s.handler.extentOffset = int(preEk.ExtentOffset)

	return true
}

func (s *Streamer) isForceROW() bool {
	return s.client.dataWrapper.forceROW
}
