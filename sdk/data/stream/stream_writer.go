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
	"hash/crc32"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
)

const (
	MaxSelectDataPartitionForWrite = 32
	MaxNewHandlerRetry             = 3
	MaxPacketErrorCount            = 128
	MaxDirtyListLen                = 0
)

const (
	StreamerNormal int32 = iota
	StreamerError
	LastEKVersionNotEqual
)

const (
	streamWriterFlushPeriod       = 3
	streamWriterIdleTimeoutPeriod = 10
)

// VerUpdateRequest defines an verseq update request.
type VerUpdateRequest struct {
	err    error
	verSeq uint64
	done   chan struct{}
}

// OpenRequest defines an open request.
type OpenRequest struct {
	done chan struct{}
}

// WriteRequest defines a write request.
type WriteRequest struct {
	fileOffset int
	size       int
	data       []byte
	flags      int
	writeBytes int
	err        error
	done       chan struct{}
	checkFunc  func() error
}

// FlushRequest defines a flush request.
type FlushRequest struct {
	err  error
	done chan struct{}
}

// ReleaseRequest defines a release request.
type ReleaseRequest struct {
	err  error
	done chan struct{}
}

// TruncRequest defines a truncate request.
type TruncRequest struct {
	size     int
	err      error
	fullPath string
	done     chan struct{}
}

// EvictRequest defines an evict request.
type EvictRequest struct {
	err  error
	done chan struct{}
}

// Open request shall grab the lock until request is sent to the request channel
func (s *Streamer) IssueOpenRequest() error {
	request := openRequestPool.Get().(*OpenRequest)
	request.done = make(chan struct{}, 1)
	s.request <- request
	s.client.streamerLock.Unlock()
	<-request.done
	openRequestPool.Put(request)
	return nil
}

func (s *Streamer) IssueWriteRequest(offset int, data []byte, flags int, checkFunc func() error) (write int, err error) {
	if atomic.LoadInt32(&s.status) >= StreamerError {
		return 0, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}

	s.writeLock.Lock()
	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.fileOffset = offset
	request.size = len(data)
	request.flags = flags
	request.done = make(chan struct{}, 1)
	request.checkFunc = checkFunc

	s.request <- request
	s.writeLock.Unlock()

	<-request.done
	err = request.err
	write = request.writeBytes
	writeRequestPool.Put(request)
	return
}

func (s *Streamer) IssueFlushRequest() error {
	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	s.request <- request
	<-request.done
	err := request.err
	flushRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueReleaseRequest() error {
	request := releaseRequestPool.Get().(*ReleaseRequest)
	request.done = make(chan struct{}, 1)
	s.request <- request
	s.client.streamerLock.Unlock()
	<-request.done
	err := request.err
	releaseRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueTruncRequest(size int, fullPath string) error {
	request := truncRequestPool.Get().(*TruncRequest)
	request.size = size
	request.fullPath = fullPath
	request.done = make(chan struct{}, 1)
	s.request <- request
	<-request.done
	err := request.err
	truncRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueEvictRequest() error {
	request := evictRequestPool.Get().(*EvictRequest)
	request.done = make(chan struct{}, 1)
	s.request <- request
	s.client.streamerLock.Unlock()
	<-request.done
	err := request.err
	evictRequestPool.Put(request)
	return err
}

func (s *Streamer) GetStoreMod(offset int, size int) (storeMode int) {
	// Small files are usually written in a single write, so use tiny extent
	// store only for the first write operation.
	if offset > 0 || offset+size > s.tinySizeLimit() {
		storeMode = proto.NormalExtentType
	} else {
		storeMode = proto.TinyExtentType
	}
	return
}

func (s *Streamer) server(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case request := <-s.request:
			s.handleRequest(ctx, request)
			s.idle = 0
			s.traversed = 0
		case <-s.done:
			s.abort()
			span.Debugf("done server: evict, ino(%v)", s.inode)
			return
		case <-t.C:
			s.traverse(ctx)
			if s.refcnt <= 0 {

				s.client.streamerLock.Lock()
				if s.idle >= streamWriterIdleTimeoutPeriod && len(s.request) == 0 {
					if s.client.disableMetaCache || !s.needBCache {
						delete(s.client.streamers, s.inode)
						if s.client.evictIcache != nil {
							s.client.evictIcache(s.inode)
						}
					}

					s.isOpen = false
					// fail the remaining requests in such case
					s.clearRequests()
					s.client.streamerLock.Unlock()

					span.Debugf("done server: no requests for a long time, ino(%v)", s.inode)
					return
				}
				s.client.streamerLock.Unlock()

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
	if atomic.LoadInt32(&s.needUpdateVer) == 1 {
		s.closeOpenHandler(ctx)
		atomic.StoreInt32(&s.needUpdateVer, 0)
	}

	switch request := request.(type) {
	case *OpenRequest:
		ctx = proto.ContextWithOperation(ctx, "stream-open")
		s.open(ctx)
		request.done <- struct{}{}
	case *WriteRequest:
		ctx = proto.ContextWithOperation(ctx, "stream-write")
		request.writeBytes, request.err = s.write(ctx, request.data, request.fileOffset, request.size, request.flags, request.checkFunc)
		request.done <- struct{}{}
	case *TruncRequest:
		ctx = proto.ContextWithOperation(ctx, "stream-truncate")
		request.err = s.truncate(ctx, request.size, request.fullPath)
		request.done <- struct{}{}
	case *FlushRequest:
		ctx = proto.ContextWithOperation(ctx, "stream-flush")
		request.err = s.flush(ctx)
		request.done <- struct{}{}
	case *ReleaseRequest:
		ctx = proto.ContextWithOperation(ctx, "stream-release")
		request.err = s.release(ctx)
		request.done <- struct{}{}
	case *EvictRequest:
		request.err = s.evict()
		request.done <- struct{}{}
	case *VerUpdateRequest:
		ctx = proto.ContextWithOperation(ctx, "stream-updateVer")
		request.err = s.updateVer(ctx, request.verSeq)
		request.done <- struct{}{}
	default:
	}
}

func (s *Streamer) write(ctx context.Context, data []byte, offset, size, flags int, checkFunc func() error) (total int, err error) {
	var (
		direct     bool
		retryTimes int8
	)

	if flags&proto.FlagsSyncWrite != 0 {
		direct = true
	}
begin:
	if flags&proto.FlagsAppend != 0 {
		filesize, _ := s.extents.Size()
		offset = filesize
	}

	span := proto.SpanFromContext(ctx)
	span.Debugf("Streamer write enter: ino(%v) offset(%v) size(%v) flags(%v)", s.inode, offset, size, flags)
	s.client.writeLimiter.Wait(ctx)

	requests := s.extents.PrepareWriteRequests(ctx, offset, size, data)
	span.Debugf("Streamer write: ino(%v) prepared requests(%v)", s.inode, requests)

	isChecked := false
	// Must flush before doing overwrite
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		ctx = proto.ContextWithOperation(ctx, req.ExtentKey.GetExtentKey())
		err = s.flush(ctx)
		if err != nil {
			return
		}
		// some extent key in requests with partition id 0 means it's append operation and on flight.
		// need to flush and get the right key then used to make modification
		requests = s.extents.PrepareWriteRequests(ctx, offset, size, data)
		span.Debugf("Streamer write: ino(%v) prepared requests after flush(%v)", s.inode, requests)
		break
	}

	for _, req := range requests {
		var writeSize int
		if req.ExtentKey != nil {
			if s.client.bcacheEnable {
				cacheKey := util.GenerateRepVolKey(s.client.volumeName, s.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, uint64(req.FileOffset))
				if _, ok := s.inflightEvictL1cache.Load(cacheKey); !ok {
					go func(cacheKey string) {
						s.inflightEvictL1cache.Store(cacheKey, true)
						s.client.evictBcache(ctx, cacheKey)
						s.inflightEvictL1cache.Delete(cacheKey)
					}(cacheKey)
				}
			}
			span.Debugf("action[streamer.write] inode [%v] latest seq [%v] extentkey seq [%v]  info [%v] before compare seq",
				s.inode, s.verSeq, req.ExtentKey.GetSeq(), req.ExtentKey)
			ctx = proto.ContextWithOperation(ctx, req.ExtentKey.GetExtentKey())
			if req.ExtentKey.GetSeq() == s.verSeq {
				writeSize, err = s.doOverwrite(ctx, req, direct)
				if err == proto.ErrCodeVersionOp {
					span.Debugf("action[streamer.write] write need version update")
					if err = s.GetExtentsForce(ctx); err != nil {
						span.Errorf("action[streamer.write] err %v", err)
						return
					}
					if retryTimes > 3 {
						err = proto.ErrCodeVersionOp
						span.Warnf("action[streamer.write] err %v", err)
						return
					}
					time.Sleep(time.Millisecond * 100)
					retryTimes++
					span.Debugf("action[streamer.write] err %v retryTimes %v", err, retryTimes)
					goto begin
				}
				span.Debugf("action[streamer.write] err %v retryTimes %v", err, retryTimes)
			} else {
				span.Debugf("action[streamer.write] ino %v do OverWriteByAppend extent key (%v) because seq not equal", s.inode, req.ExtentKey)
				writeSize, _, err, _ = s.doOverWriteByAppend(ctx, req, direct)
			}
			if s.client.bcacheEnable {
				cacheKey := util.GenerateKey(s.client.volumeName, s.inode, uint64(req.FileOffset))
				go s.client.evictBcache(ctx, cacheKey)
			}
		} else {
			if !isChecked && checkFunc != nil {
				isChecked = true
				if err = checkFunc(); err != nil {
					return
				}
			}
			writeSize, err = s.doWriteAppend(ctx, req, direct)
		}
		if err != nil {
			span.Errorf("Streamer write: ino(%v) err(%v)", s.inode, err)
			break
		}
		total += writeSize
	}
	if filesize, _ := s.extents.Size(); offset+total > filesize {
		s.extents.SetSize(uint64(offset+total), false)
		span.Debugf("Streamer write: ino(%v) filesize changed to (%v)", s.inode, offset+total)
	}
	span.Debugf("Streamer write exit: ino(%v) offset(%v) size(%v) done total(%v) err(%v)", s.inode, offset, size, total, err)
	return
}

func (s *Streamer) doOverWriteByAppend(ctx context.Context, req *ExtentRequest, direct bool) (total int, extKey *proto.ExtentKey, err error, status int32) {
	// the extent key needs to be updated because when preparing the requests,
	// the obtained extent key could be a local key which can be inconsistent with the remote key.
	// the OpTryWriteAppend is a special case, ignore it
	req.ExtentKey = s.extents.Get(uint64(req.FileOffset))
	return s.doDirectWriteByAppend(ctx, req, direct, proto.OpRandomWriteAppend)
}

func (s *Streamer) tryDirectAppendWrite(ctx context.Context, req *ExtentRequest, direct bool) (total int, extKey *proto.ExtentKey, err error, status int32) {
	req.ExtentKey = s.handler.key
	return s.doDirectWriteByAppend(ctx, req, direct, proto.OpTryWriteAppend)
}

func (s *Streamer) doDirectWriteByAppend(ctx context.Context, req *ExtentRequest, direct bool, op uint8) (total int, extKey *proto.ExtentKey, err error, status int32) {
	span := proto.SpanFromContext(ctx)
	var (
		dp        *wrapper.DataPartition
		reqPacket *Packet
	)

	span.Debugf("action[doDirectWriteByAppend] inode %v enter in req %v", s.inode, req)
	err = s.flush(ctx)
	if err != nil {
		return
	}

	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ekFileOffset(%v) ek(%v)", s.inode, req.FileOffset, req.ExtentKey))
		return
	}

	if dp, err = s.client.dataWrapper.GetDataPartition(ctx, req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		errors.Trace(err, "doDirectWriteByAppend: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	retry := true
	if proto.IsCold(s.client.volumeType) {
		retry = false
	}
	span.Debugf("action[doDirectWriteByAppend] inode %v  data process", s.inode)

	addr := dp.LeaderAddr
	if storage.IsTinyExtent(req.ExtentKey.ExtentId) {
		addr = dp.Hosts[0]
		reqPacket = NewWriteTinyDirectly(s.inode, req.ExtentKey.PartitionId, req.FileOffset, dp)
	} else {
		reqPacket = NewOverwriteByAppendPacket(dp, req.ExtentKey.ExtentId, int(req.ExtentKey.ExtentOffset)+int(req.ExtentKey.Size),
			s.inode, req.FileOffset, direct, op)
	}

	sc := &StreamConn{
		dp:       dp,
		currAddr: addr,
	}

	replyPacket := new(Packet)
	if req.Size > util.BlockSize {
		span.Errorf("action[doDirectWriteByAppend] inode %v size too large %v", s.inode, req.Size)
		panic(nil)
	}
	for total < req.Size { // normally should only run once due to key exist in the system must be less than BlockSize
		// right position in extent:offset-ek4FileOffset+total+ekExtOffset .
		// ekExtOffset will be set by replay packet at addExtentInfo(datanode)

		if direct {
			reqPacket.Opcode = op
		}
		if req.ExtentKey.ExtentId <= storage.TinyExtentCount {
			reqPacket.ExtentType = proto.TinyExtentType
		}

		packSize := util.Min(req.Size-total, util.BlockSize)
		copy(reqPacket.Data[:packSize], req.Data[total:total+packSize])
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		err = sc.Send(ctx, &retry, reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConnWithVer(conn, proto.ReadDeadlineTime)
			if e != nil {
				span.Warnf("doDirectWriteByAppend.Stream Writer doOverwrite: ino(%v) failed to read from connect, req(%v) err(%v)", s.inode, reqPacket, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}
			span.Debugf("action[doDirectWriteByAppend] .UpdateLatestVer ino(%v) get replyPacket %v", s.inode, replyPacket)
			if replyPacket.VerSeq > sc.dp.ClientWrapper.SimpleClient.GetLatestVer() {
				err = sc.dp.ClientWrapper.SimpleClient.UpdateLatestVer(ctx, &proto.VolVersionInfoList{VerList: replyPacket.VerList})
				if err != nil {
					return err, false
				}
			}
			span.Debugf("action[doDirectWriteByAppend] ino(%v) get replyPacket opcode %v resultCode %v", s.inode, replyPacket.Opcode, replyPacket.ResultCode)
			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpTryOtherExtent {
				status = int32(proto.OpTryOtherExtent)
				return nil, false
			}

			if replyPacket.ResultCode == proto.OpTryOtherAddr {
				e = TryOtherAddrError
				span.Debugf("action[doDirectWriteByAppend] data process err %v", e)
			}
			return e, false
		})

		proto.Buffers.Put(reqPacket.Data)
		reqPacket.Data = nil
		span.Debugf("doDirectWriteByAppend: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			status = int32(replyPacket.ResultCode)
			err = errors.New(fmt.Sprintf("doOverwrite: failed or reply NOK: err(%v) ino(%v) req(%v) replyPacket(%v)", err, s.inode, req, replyPacket))
			span.Errorf("action[doDirectWriteByAppend] data process err %v", err)
			s.handler.key = nil // direct write key cann't be used again in flush process
			break
		}

		if !reqPacket.isValidWriteReply(replyPacket) || reqPacket.CRC != replyPacket.CRC {
			err = errors.New(fmt.Sprintf("doOverwrite: is not the corresponding reply, ino(%v) req(%v) replyPacket(%v)", s.inode, req, replyPacket))
			span.Errorf("action[doDirectWriteByAppend] data process err %v", err)
			break
		}

		total += packSize
		break
	}
	if err != nil {
		span.Errorf("action[doDirectWriteByAppend] data process err %v", err)
		return
	}
	if replyPacket.VerSeq > s.verSeq {
		s.client.UpdateLatestVer(ctx, &proto.VolVersionInfoList{VerList: replyPacket.VerList})
	}
	extKey = &proto.ExtentKey{
		FileOffset:   uint64(req.FileOffset),
		PartitionId:  req.ExtentKey.PartitionId,
		ExtentId:     replyPacket.ExtentID,
		ExtentOffset: uint64(replyPacket.ExtentOffset),
		Size:         uint32(total),
		SnapInfo: &proto.ExtSnapInfo{
			VerSeq: s.verSeq,
		},
	}
	if op == proto.OpRandomWriteAppend || op == proto.OpSyncRandomWriteAppend {
		span.Debugf("action[doDirectWriteByAppend] inode %v local cache process start extKey %v", s.inode, extKey)
		if err = s.extents.SplitExtentKey(ctx, s.inode, extKey); err != nil {
			span.Debugf("action[doDirectWriteByAppend] inode %v llocal cache process err %v", s.inode, err)
			return
		}
		span.Debugf("action[doDirectWriteByAppend] inode %v meta extent split with ek (%v)", s.inode, extKey)
		if err = s.client.splitExtentKey(s.parentInode, s.inode, *extKey); err != nil {
			span.Errorf("action[doDirectWriteByAppend] inode %v meta extent split process err %v", s.inode, err)
			return
		}
	} else {
		discards := s.extents.Append(ctx, extKey, true)
		var st int
		if st, err = s.client.appendExtentKey(s.parentInode, s.inode, *extKey, discards); err != nil {
			status = int32(st)
			span.Errorf("action[doDirectWriteByAppend] inode %v meta extent split process err %v", s.inode, err)
			return
		}
		span.Debugf("action[doDirectWriteByAppend] handler fileoffset %v size %v key %v", s.handler.fileOffset, s.handler.size, s.handler.key)
		// adjust the handler key to last direct write one
		s.handler.fileOffset = int(extKey.FileOffset)
		s.handler.size = int(extKey.Size)
		s.handler.key = extKey
	}
	if atomic.LoadInt32(&s.needUpdateVer) > 0 {
		if err = s.GetExtentsForce(ctx); err != nil {
			span.Errorf("action[doDirectWriteByAppend] inode %v GetExtents err %v", s.inode, err)
			return
		}
	}
	span.Debugf("action[doDirectWriteByAppend] inode %v process over!", s.inode)
	return
}

func (s *Streamer) doOverwrite(ctx context.Context, req *ExtentRequest, direct bool) (total int, err error) {
	span := proto.SpanFromContext(ctx)
	var dp *wrapper.DataPartition

	err = s.flush(ctx)
	if err != nil {
		return
	}

	offset := req.FileOffset
	size := req.Size

	// the extent key needs to be updated because when preparing the requests,
	// the obtained extent key could be a local key which can be inconsistent with the remote key.
	req.ExtentKey = s.extents.Get(uint64(offset))
	ekFileOffset := int(req.ExtentKey.FileOffset)
	ekExtOffset := int(req.ExtentKey.ExtentOffset)
	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ekFileOffset(%v) ek(%v)", s.inode, ekFileOffset, req.ExtentKey))
		return
	}

	if dp, err = s.client.dataWrapper.GetDataPartition(ctx, req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		errors.Trace(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	retry := true
	if proto.IsCold(s.client.volumeType) {
		retry = false
	}

	sc := NewStreamConn(ctx, dp, false)

	for total < size {
		reqPacket := NewOverwritePacket(dp, req.ExtentKey.ExtentId, offset-ekFileOffset+total+ekExtOffset, s.inode, offset)
		reqPacket.VerSeq = s.client.multiVerMgr.latestVerSeq
		reqPacket.VerList = make([]*proto.VolVersionInfo, len(s.client.multiVerMgr.verList.VerList))
		copy(reqPacket.VerList, s.client.multiVerMgr.verList.VerList)
		reqPacket.ExtentType |= proto.MultiVersionFlag
		reqPacket.ExtentType |= proto.VersionListFlag

		span.Debugf("action[doOverwrite] inode %v extentid %v,extentOffset %v(%v,%v,%v,%v) offset %v, streamer seq %v", s.inode, req.ExtentKey.ExtentId, reqPacket.ExtentOffset,
			offset, ekFileOffset, total, ekExtOffset, offset, s.verSeq)
		if direct {
			reqPacket.Opcode = proto.OpSyncRandomWrite
		}
		packSize := util.Min(size-total, util.BlockSize)
		copy(reqPacket.Data[:packSize], req.Data[total:total+packSize])
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])
		reqPacket.VerSeq = s.verSeq

		replyPacket := new(Packet)
		err = sc.Send(ctx, &retry, reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConnWithVer(conn, proto.ReadDeadlineTime)
			if e != nil {
				span.Warnf("Stream Writer doOverwrite: ino(%v) failed to read from connect, req(%v) err(%v)", s.inode, reqPacket, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}
			span.Debugf("action[doOverwrite] streamer verseq (%v) datanode rsp seq (%v) code(%v)", s.verSeq, replyPacket.VerSeq, replyPacket.ResultCode)
			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpTryOtherAddr {
				e = TryOtherAddrError
			}

			if replyPacket.ResultCode == proto.ErrCodeVersionOpError {
				e = proto.ErrCodeVersionOp
				span.Debugf("action[doOverwrite] .UpdateLatestVer verseq (%v) be updated by datanode rsp (%v) ", s.verSeq, replyPacket)
				s.verSeq = replyPacket.VerSeq
				s.extents.verSeq = s.verSeq
				s.client.UpdateLatestVer(ctx, &proto.VolVersionInfoList{VerList: replyPacket.VerList})
				return e, false
			}

			return e, false
		})

		proto.Buffers.Put(reqPacket.Data)
		reqPacket.Data = nil
		span.Debugf("doOverwrite: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			if replyPacket.ResultCode == proto.ErrCodeVersionOpError {
				err = proto.ErrCodeVersionOp
				span.Warnf("doOverwrite: need retry.ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)
				return
			}
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

func (s *Streamer) tryInitExtentHandlerByLastEk(ctx context.Context, offset, size int) (isLastEkVerNotEqual bool) {
	span := proto.SpanFromContext(ctx)
	storeMode := s.GetStoreMod(offset, size)
	getEndEkFunc := func() *proto.ExtentKey {
		if ek := s.extents.GetEndForAppendWrite(ctx, uint64(offset), s.verSeq, false); ek != nil && !storage.IsTinyExtent(ek.ExtentId) {
			return ek
		}
		return nil
	}

	checkVerFunc := func(currentEK *proto.ExtentKey) {
		if currentEK.GetSeq() != s.verSeq {
			span.Debugf("tryInitExtentHandlerByLastEk. exist ek seq %v vs request seq %v", currentEK.GetSeq(), s.verSeq)
			if int(currentEK.ExtentOffset)+int(currentEK.Size)+size > util.ExtentSize {
				s.closeOpenHandler(ctx)
				return
			}
			isLastEkVerNotEqual = true
		}
	}

	initExtentHandlerFunc := func(currentEK *proto.ExtentKey) {
		checkVerFunc(currentEK)
		span.Debugf("tryInitExtentHandlerByLastEk: found ek in ExtentCache, extent_id(%v) req_offset(%v) req_size(%v), currentEK [%v] streamer seq %v",
			currentEK.ExtentId, offset, size, currentEK, s.verSeq)
		_, pidErr := s.client.dataWrapper.GetDataPartition(ctx, currentEK.PartitionId)
		if pidErr == nil {
			seq := currentEK.GetSeq()
			if isLastEkVerNotEqual {
				seq = s.verSeq
			}
			span.Debugf("tryInitExtentHandlerByLastEk NewExtentHandler")
			handler := NewExtentHandler(ctx, s, int(currentEK.FileOffset), storeMode, int(currentEK.Size))
			handler.key = &proto.ExtentKey{
				FileOffset:   currentEK.FileOffset,
				PartitionId:  currentEK.PartitionId,
				ExtentId:     currentEK.ExtentId,
				ExtentOffset: currentEK.ExtentOffset,
				Size:         currentEK.Size,
				SnapInfo: &proto.ExtSnapInfo{
					VerSeq: seq,
				},
			}
			handler.lastKey = *currentEK

			if s.handler != nil {
				span.Debugf("tryInitExtentHandlerByLastEk: close old handler, currentEK.PartitionId(%v)",
					currentEK.PartitionId)
				s.closeOpenHandler(ctx)
			}

			s.handler = handler
			s.dirty = false
			span.Debugf("tryInitExtentHandlerByLastEk: currentEK.PartitionId(%v) found", currentEK.PartitionId)
		} else {
			span.Debugf("tryInitExtentHandlerByLastEk: currentEK.PartitionId(%v) not found", currentEK.PartitionId)
		}
	}

	if storeMode == proto.NormalExtentType {
		if s.handler == nil {
			span.Debugf("tryInitExtentHandlerByLastEk: handler nil")
			if ek := getEndEkFunc(); ek != nil {
				initExtentHandlerFunc(ek)
			}
		} else {
			if s.handler.fileOffset+s.handler.size == offset {
				if s.handler.key != nil {
					checkVerFunc(s.handler.key)
				}
				return
			} else {
				if ek := getEndEkFunc(); ek != nil {
					span.Debugf("tryInitExtentHandlerByLastEk: getEndEkFunc get ek %v", ek)
					initExtentHandlerFunc(ek)
				} else {
					span.Debugf("tryInitExtentHandlerByLastEk: not found ek")
				}
			}
		}
	}

	return
}

// First, attempt sequential writes using neighboring extent keys. If the last extent has a different version,
// it indicates that the extent may have been fully utilized by the previous version.
// Next, try writing and directly checking the extent at the datanode. If the extent cannot be reused, create a new extent for writing.
func (s *Streamer) doWriteAppend(ctx context.Context, req *ExtentRequest, direct bool) (writeSize int, err error) {
	span := proto.SpanFromContext(ctx)
	var status int32
	// try append write, get response
	span.Debugf("action[streamer.write] doWriteAppend req: ExtentKey(%v) FileOffset(%v) size(%v)",
		req.ExtentKey, req.FileOffset, req.Size)
	// First, attempt sequential writes using neighboring extent keys. If the last extent has a different version,
	// it indicates that the extent may have been fully utilized by the previous version.
	// Next, try writing and directly checking the extent at the datanode. If the extent cannot be reused, create a new extent for writing.
	if writeSize, err, status = s.doWriteAppendEx(ctx, req.Data, req.FileOffset, req.Size, direct, true); status == LastEKVersionNotEqual {
		span.Debugf("action[streamer.write] tryDirectAppendWrite req %v FileOffset %v size %v", req.ExtentKey, req.FileOffset, req.Size)
		if writeSize, _, err, status = s.tryDirectAppendWrite(ctx, req, direct); status == int32(proto.OpTryOtherExtent) {
			span.Debugf("action[streamer.write] doWriteAppend again req %v FileOffset %v size %v", req.ExtentKey, req.FileOffset, req.Size)
			writeSize, err, _ = s.doWriteAppendEx(ctx, req.Data, req.FileOffset, req.Size, direct, false)
		}
	}
	span.Debugf("action[streamer.write] doWriteAppend status %v err %v", status, err)
	return
}

func (s *Streamer) doWriteAppendEx(ctx context.Context, data []byte, offset, size int, direct bool, reUseEk bool) (total int, err error, status int32) {
	span := proto.SpanFromContext(ctx)
	var (
		ek        *proto.ExtentKey
		storeMode int
	)

	// Small files are usually written in a single write, so use tiny extent
	// store only for the first write operation.
	storeMode = s.GetStoreMod(offset, size)

	span.Debugf("doWriteAppendEx enter: ino(%v) offset(%v) size(%v) storeMode(%v)", s.inode, offset, size, storeMode)
	if proto.IsHot(s.client.volumeType) {
		if reUseEk {
			if isLastEkVerNotEqual := s.tryInitExtentHandlerByLastEk(ctx, offset, size); isLastEkVerNotEqual {
				span.Debugf("doWriteAppendEx enter: ino(%v) tryInitExtentHandlerByLastEk worked but seq not equal", s.inode)
				status = LastEKVersionNotEqual
				return
			}
		} else if s.handler != nil {
			s.closeOpenHandler(ctx)
		}

		for i := 0; i < MaxNewHandlerRetry; i++ {
			if s.handler == nil {
				s.handler = NewExtentHandler(ctx, s, offset, storeMode, 0)
				s.dirty = false
			} else if s.handler.storeMode != storeMode {
				// store mode changed, so close open handler and start a new one
				s.closeOpenHandler(ctx)
				continue
			}
			ek, err = s.handler.write(ctx, data, offset, size, direct)
			if err == nil && ek != nil {
				ek.SetSeq(s.verSeq)
				if !s.dirty {
					s.dirtylist.Put(s.handler)
					s.dirty = true
				}
				break
			}
			s.closeOpenHandler(ctx)
		}
	} else {
		s.handler = NewExtentHandler(ctx, s, offset, storeMode, 0)
		s.dirty = false
		ek, err = s.handler.write(ctx, data, offset, size, direct)
		if err == nil && ek != nil {
			if !s.dirty {
				s.dirtylist.Put(s.handler)
				s.dirty = true
			}
		}

		err = s.closeOpenHandler(ctx)
	}

	if err != nil || ek == nil {
		span.Debugf("doWriteAppendEx error: ino(%v) offset(%v) size(%v) err(%v) ek(%v)", s.inode, offset, size, err, ek)
		return
	}

	// This ek is just a local cache for PrepareWriteRequest, so ignore discard eks here.
	_ = s.extents.Append(ctx, ek, false)
	total = size

	return
}

func (s *Streamer) flush(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	for {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		span.Debugf("Streamer flush begin: eh(%v)", eh)
		err = eh.flush(ctx)
		if err != nil {
			span.Errorf("Streamer flush failed: eh(%v)", eh)
			return
		}
		eh.stream.dirtylist.Remove(element)
		if eh.getStatus() == ExtentStatusOpen {
			s.dirty = false
			span.Debugf("Streamer flush handler open: eh(%v)", eh)
		} else {
			// TODO unhandled error
			eh.cleanup()
			span.Debugf("Streamer flush handler cleaned up: eh(%v)", eh)
		}
		span.Debugf("Streamer flush end: eh(%v)", eh)
	}
	return
}

func (s *Streamer) traverse(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	s.traversed++
	length := s.dirtylist.Len()
	for i := 0; i < length; i++ {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		span.Debugf("Streamer traverse begin: eh(%v)", eh)
		if eh.getStatus() >= ExtentStatusClosed {
			// handler can be in different status such as close, recovery, and error,
			// and therefore there can be packet that has not been flushed yet.
			eh.flushPacket()
			if atomic.LoadInt32(&eh.inflight) > 0 {
				span.Debugf("Streamer traverse skipped: non-zero inflight, eh(%v)", eh)
				continue
			}
			err = eh.appendExtentKey(ctx)
			if err != nil {
				span.Warnf("Streamer traverse abort: appendExtentKey failed, eh(%v) err(%v)", eh, err)
				// set the streamer to error status to avoid further writes
				if err == syscall.EIO {
					atomic.StoreInt32(&eh.stream.status, StreamerError)
				}
				return
			}
			s.dirtylist.Remove(element)
			eh.cleanup()
		} else {
			if s.traversed < streamWriterFlushPeriod {
				span.Debugf("Streamer traverse skipped: traversed(%v) eh(%v)", s.traversed, eh)
				continue
			}
			if err = eh.flush(ctx); err != nil {
				span.Warnf("Streamer traverse flush: eh(%v) err(%v)", eh, err)
			}
		}
		span.Debugf("Streamer traverse end: eh(%v)", eh)
	}
	return
}

func (s *Streamer) closeOpenHandler(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	// just in case to avoid infinite loop
	var cnt int = 2 * MaxPacketErrorCount

	handler := s.handler
	for handler != nil && cnt >= 0 {
		handler.setClosed()
		if s.dirtylist.Len() < MaxDirtyListLen {
			handler.flushPacket()
		} else {
			// TODO unhandled error
			err = s.handler.flush(ctx)
		}
		handler = handler.recoverHandler
		cnt--
	}

	if s.handler != nil {
		if !s.dirty {
			// in case the current handler is not on the dirty list and will not get cleaned up
			// TODO unhandled error
			span.Debugf("action[Streamer.closeOpenHandler]")
			s.handler.cleanup()
		}
		s.handler = nil
	}
	return err
}

func (s *Streamer) open(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	s.refcnt++
	span.Debugf("open: streamer(%v) refcnt(%v)", s, s.refcnt)
}

func (s *Streamer) release(ctx context.Context) error {
	span := proto.SpanFromContext(ctx)
	s.refcnt--
	s.closeOpenHandler(ctx)
	err := s.flush(ctx)
	if err != nil {
		s.abort()
	}
	span.Debugf("release: streamer(%v) refcnt(%v)", s, s.refcnt)
	return err
}

func (s *Streamer) evict() error {
	s.client.streamerLock.Lock()
	if s.refcnt > 0 || len(s.request) != 0 {
		s.client.streamerLock.Unlock()
		return errors.New(fmt.Sprintf("evict: streamer(%v) refcnt(%v)", s, s.refcnt))
	}
	if s.client.disableMetaCache || !s.needBCache {
		delete(s.client.streamers, s.inode)
	}
	s.client.streamerLock.Unlock()
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

func (s *Streamer) truncate(ctx context.Context, size int, fullPath string) error {
	s.closeOpenHandler(ctx)
	err := s.flush(ctx)
	if err != nil {
		return err
	}

	err = s.client.truncate(s.inode, uint64(size), fullPath)
	if err != nil {
		return err
	}

	oldsize, _ := s.extents.Size()
	if oldsize <= size {
		s.extents.SetSize(uint64(size), true)
		return nil
	}

	s.extents.TruncDiscard(ctx, uint64(size))
	return s.GetExtentsForce(ctx)
}

func (s *Streamer) updateVer(ctx context.Context, verSeq uint64) (err error) {
	span := proto.SpanFromContext(ctx)
	span.Infof("action[stream.updateVer] ver %v update to %v", s.verSeq, verSeq)
	if s.verSeq != verSeq {
		span.Infof("action[stream.updateVer] ver %v update to %v", s.verSeq, verSeq)
		s.verSeq = verSeq
		s.extents.verSeq = verSeq
	}
	return
}

func (s *Streamer) tinySizeLimit() int {
	return util.DefaultTinySizeLimit
}
