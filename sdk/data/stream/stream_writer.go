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

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
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
	fileOffset   int
	size         int
	data         []byte
	flags        int
	writeBytes   int
	err          error
	done         chan struct{}
	checkFunc    func() error
	storageClass uint32
	isMigration  bool
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

func (s *Streamer) IssueWriteRequest(offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (write int, err error) {
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
	request.storageClass = storageClass
	request.isMigration = isMigration

	s.request <- request
	s.writeLock.Unlock()

	<-request.done
	err = request.err
	write = request.writeBytes
	writeRequestPool.Put(request)
	return
}

func (s *Streamer) IssueFlushRequest() error {
	if s.rdonly {
		return nil
	}
	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	s.request <- request
	<-request.done
	err := request.err
	if err != nil {
		log.LogErrorf("[IssueFlushRequest] ino(%v) flush failed err(%v)", s.inode, err)
	}
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
	if offset+size > s.tinySizeLimit() {
		storeMode = proto.NormalExtentType
	} else {
		storeMode = proto.TinyExtentType
	}
	return
}

func (s *Streamer) server() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	// only file opened with write request needs to forbidden migration
	renewalTimer := time.NewTicker(proto.ForbiddenMigrationRenewalPeriod / 5)
	defer renewalTimer.Stop()
	log.LogDebugf("start server: streamer(%v)", s)
	for {
		select {
		case request := <-s.request:
			s.handleRequest(request)
			s.idle = 0
			s.traversed = 0
		case <-s.done:
			s.abort()
			log.LogDebugf("done server: evict, streamer(%v)", s)
			return
		case <-t.C:
			if s.rdonly {
				log.LogDebugf("server: rdonly stream no need to start server routine. ino %d", s.inode)
				return
			}
			s.traverse()
			s.client.streamerLock.Lock()
			if s.refcnt <= 0 {
				if s.idle >= streamWriterIdleTimeoutPeriod && len(s.request) == 0 {
					if s.client.disableMetaCache || !s.needBCache {
						log.LogDebugf("done server: delete streamer(%v)", s)
						delete(s.client.streamers, s.inode)
						if s.client.evictIcache != nil {
							s.client.evictIcache(s.inode)
						}
					}
					s.isOpen = false
					// fail the remaining requests in such case
					s.clearRequests()
					s.client.streamerLock.Unlock()
					log.LogDebugf("done server: no requests for a long time, ino(%v), streamer(%v)", s.inode, s)
					return
				}
				s.idle++
			}
			s.client.streamerLock.Unlock()

		case <-renewalTimer.C:
			if !s.openForWrite {
				renewalTimer.Stop()
			} else {
				// renewal forbidden migration
				err := s.client.renewalForbiddenMigration(s.inode)
				if err != nil {
					log.LogWarnf("ino(%v) renewalForbiddenMigration failed err %v", s.inode, err.Error())
					s.setError()
				}
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

func (s *Streamer) handleRequest(request interface{}) {
	if atomic.LoadInt32(&s.needUpdateVer) == 1 {
		s.closeOpenHandler()
		atomic.StoreInt32(&s.needUpdateVer, 0)
	}

	switch request := request.(type) {
	case *OpenRequest:
		s.open()
		request.done <- struct{}{}
	case *WriteRequest:
		request.writeBytes, request.err = s.write(request.data, request.fileOffset, request.size, request.flags,
			request.checkFunc, request.storageClass, request.isMigration)
		request.done <- struct{}{}
	case *TruncRequest:
		request.err = s.truncate(request.size, request.fullPath)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = s.flush()
		request.done <- struct{}{}
	case *ReleaseRequest:
		request.err = s.release()
		request.done <- struct{}{}
	case *EvictRequest:
		request.err = s.evict()
		request.done <- struct{}{}
	case *VerUpdateRequest:
		request.err = s.updateVer(request.verSeq)
		request.done <- struct{}{}
	default:
	}
}

func (s *Streamer) write(data []byte, offset, size, flags int, checkFunc func() error,
	storageClass uint32, isMigration bool,
) (total int, err error) {
	var (
		direct     bool
		retryTimes int8
	)
	if atomic.LoadInt32(&s.status) >= StreamerError {
		return 0, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}
	if flags&proto.FlagsSyncWrite != 0 {
		direct = true
	}
begin:
	if flags&proto.FlagsAppend != 0 {
		filesize, _ := s.extents.Size()
		offset = filesize
	}

	log.LogDebugf("Streamer write enter: ino(%v) offset(%v) size(%v) flags(%v) storageClass(%v) isMigration(%v)",
		s.inode, offset, size, flags, storageClass, isMigration)

	ctx := context.Background()
	s.client.writeLimiter.Wait(ctx)
	s.client.LimitManager.WriteAlloc(ctx, size)

	requests := s.extents.PrepareWriteRequests(offset, size, data)
	log.LogDebugf("Streamer write: ino(%v) prepared requests(%v)", s.inode, requests)

	isChecked := false
	// Must flush before doing overwrite
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		err = s.flush()
		if err != nil {
			return
		}
		// some extent key in requests with partition id 0 means it's append operation and on flight.
		// need to flush and get the right key then used to make modification
		requests = s.extents.PrepareWriteRequests(offset, size, data)
		log.LogDebugf("Streamer write: ino(%v) prepared requests after flush(%v)", s.inode, requests)
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
						s.client.evictBcache(cacheKey)
						s.inflightEvictL1cache.Delete(cacheKey)
					}(cacheKey)
				}
			}
			log.LogDebugf("action[streamer.write] inode [%v] latest seq [%v] extentkey seq [%v]  info [%v] before compare seq",
				s.inode, s.verSeq, req.ExtentKey.GetSeq(), req.ExtentKey)
			if req.ExtentKey.GetSeq() == s.verSeq {
				writeSize, err = s.doOverwrite(req, direct, storageClass)
				if err == proto.ErrCodeVersionOp {
					log.LogDebugf("action[streamer.write] write need version update")
					if err = s.GetExtentsForceRefresh(); err != nil {
						log.LogErrorf("action[streamer.write] err %v", err)
						return
					}
					if retryTimes > 3 {
						err = proto.ErrCodeVersionOp
						log.LogWarnf("action[streamer.write] err %v", err)
						return
					}
					time.Sleep(time.Millisecond * 100)
					retryTimes++
					log.LogDebugf("action[streamer.write] err %v retryTimes %v", err, retryTimes)
					goto begin
				}
				log.LogDebugf("action[streamer.write] err %v retryTimes %v", err, retryTimes)
			} else {
				log.LogDebugf("action[streamer.write] ino %v doOverWriteByAppend extent key (%v)", s.inode, req.ExtentKey)
				writeSize, _, err, _ = s.doOverWriteByAppend(req, direct, storageClass, isMigration)
			}
			if s.aheadReadEnable {
				s.aheadReadWindow.evictCacheBlock(req)
			}
			if s.client.bcacheEnable {
				cacheKey := util.GenerateKey(s.client.volumeName, s.inode, uint64(req.FileOffset))
				go s.client.evictBcache(cacheKey)
			}
		} else {
			if !isChecked && checkFunc != nil {
				isChecked = true
				if err = checkFunc(); err != nil {
					return
				}
			}
			writeSize, err = s.doWriteAppend(req, direct, storageClass, isMigration)
		}
		if err != nil {
			log.LogErrorf("Streamer write: ino(%v) err(%v)", s.inode, err)
			break
		}
		total += writeSize
	}
	filesize, _ := s.extents.Size()
	if offset+total > filesize {
		s.extents.SetSize(uint64(offset+total), false)
		log.LogDebugf("Streamer write: ino(%v) filesize changed to (%v)", s.inode, offset+total)
	}
	log.LogDebugf("Streamer write exit: ino(%v) filesize(%v) offset(%v) size(%v) done total(%v) err(%v)", s.inode, filesize, offset, size, total, err)
	return
}

func (s *Streamer) doOverWriteByAppend(req *ExtentRequest, direct bool, storageClass uint32, isMigration bool) (total int, extKey *proto.ExtentKey, err error, status int32) {
	// the extent key needs to be updated because when preparing the requests,
	// the obtained extent key could be a local key which can be inconsistent with the remote key.
	// the OpTryWriteAppend is a special case, ignore it
	req.ExtentKey = s.extents.Get(uint64(req.FileOffset))
	return s.doDirectWriteByAppend(req, direct, proto.OpRandomWriteAppend, storageClass, isMigration)
}

func (s *Streamer) tryDirectAppendWrite(req *ExtentRequest, direct bool, storageClass uint32, isMigration bool) (total int, extKey *proto.ExtentKey, err error, status int32) {
	req.ExtentKey = s.handler.key
	return s.doDirectWriteByAppend(req, direct, proto.OpTryWriteAppend, storageClass, isMigration)
}

func (s *Streamer) doDirectWriteByAppend(req *ExtentRequest, direct bool, op uint8,
	storageClass uint32, isMigration bool,
) (total int, extKey *proto.ExtentKey, err error, status int32) {
	var (
		dp        *wrapper.DataPartition
		reqPacket *Packet
	)

	log.LogDebugf("action[doDirectWriteByAppend] inode %v enter in req %v", s.inode, req)
	err = s.flush()
	if err != nil {
		return
	}

	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ekFileOffset(%v) ek(%v)", s.inode, req.FileOffset, req.ExtentKey))
		return
	}

	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		errors.Trace(err, "doDirectWriteByAppend: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	retry := true
	if proto.IsCold(s.client.volumeType) || proto.IsStorageClassBlobStore(storageClass) {
		retry = false
	}
	log.LogDebugf("action[doDirectWriteByAppend] inode %v  data process", s.inode)

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

		maxRetryTimeout: s.client.streamRetryTimeout,
	}

	replyPacket := new(Packet)
	if req.Size > util.BlockSize {
		log.LogErrorf("action[doDirectWriteByAppend] inode %v size too large %v", s.inode, req.Size)
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

		err = sc.Send(&retry, reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConnWithVer(conn, proto.ReadDeadlineTime)
			if e != nil {
				log.LogWarnf("doDirectWriteByAppend.Stream Writer doOverwrite: ino(%v) failed to read from connect, req(%v) err(%v)", s.inode, reqPacket, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}
			log.LogDebugf("action[doDirectWriteByAppend] .UpdateLatestVer ino(%v) get replyPacket %v", s.inode, replyPacket)
			if replyPacket.VerSeq > sc.dp.ClientWrapper.SimpleClient.GetLatestVer() {
				err = sc.dp.ClientWrapper.SimpleClient.UpdateLatestVer(&proto.VolVersionInfoList{VerList: replyPacket.VerList})
				if err != nil {
					return err, false
				}
			}
			log.LogDebugf("action[doDirectWriteByAppend] ino(%v) get replyPacket opcode %v resultCode %v", s.inode, replyPacket.Opcode, replyPacket.ResultCode)
			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpTryOtherExtent {
				status = int32(proto.OpTryOtherExtent)
				return nil, false
			}

			if replyPacket.ResultCode == proto.OpTryOtherAddr {
				e = TryOtherAddrError
				log.LogDebugf("action[doDirectWriteByAppend] data process err %v", e)
			}
			return e, false
		})

		proto.Buffers.Put(reqPacket.Data)
		reqPacket.Data = nil
		log.LogDebugf("doDirectWriteByAppend: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			status = int32(replyPacket.ResultCode)
			err = errors.New(fmt.Sprintf("doOverwrite: failed or reply NOK: err(%v) ino(%v) req(%v) replyPacket(%v)", err, s.inode, req, replyPacket))
			log.LogErrorf("action[doDirectWriteByAppend] data process err %v", err)
			s.handler.key = nil // direct write key cann't be used again in flush process
			break
		}

		if !reqPacket.isValidWriteReply(replyPacket) || reqPacket.CRC != replyPacket.CRC {
			err = errors.New(fmt.Sprintf("doOverwrite: is not the corresponding reply, ino(%v) req(%v) replyPacket(%v)", s.inode, req, replyPacket))
			log.LogErrorf("action[doDirectWriteByAppend] data process err %v", err)
			break
		}

		total += packSize
	}
	if err != nil {
		log.LogErrorf("action[doDirectWriteByAppend] data process err %v", err)
		return
	}
	if replyPacket.VerSeq > s.verSeq {
		s.client.UpdateLatestVer(&proto.VolVersionInfoList{VerList: replyPacket.VerList})
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
		log.LogDebugf("action[doDirectWriteByAppend] inode %v local cache process start extKey %v", s.inode, extKey)
		if err = s.extents.SplitExtentKey(s.inode, extKey); err != nil {
			log.LogErrorf("action[doDirectWriteByAppend] inode %v llocal cache process err %v", s.inode, err)
			return
		}
		log.LogDebugf("action[doDirectWriteByAppend] inode %v meta extent split with ek (%v)", s.inode, extKey)
		if err = s.client.splitExtentKey(s.parentInode, s.inode, *extKey, storageClass); err != nil {
			log.LogErrorf("action[doDirectWriteByAppend] inode %v meta extent split process err %v", s.inode, err)
			return
		}
	} else {
		discards := s.extents.Append(extKey, true)
		var st int
		if st, err = s.client.appendExtentKey(s.parentInode, s.inode, *extKey, discards, s.isCache, storageClass, isMigration); err != nil {
			status = int32(st)
			log.LogErrorf("action[doDirectWriteByAppend] inode %v meta extent split process err %v", s.inode, err)
			return
		}
		log.LogDebugf("action[doDirectWriteByAppend] handler fileoffset %v size %v key %v", s.handler.fileOffset, s.handler.size, s.handler.key)
		// adjust the handler key to last direct write one
		s.handler.fileOffset = int(extKey.FileOffset)
		s.handler.size = int(extKey.Size)
		s.handler.key = extKey
	}
	if atomic.LoadInt32(&s.needUpdateVer) > 0 {
		if err = s.GetExtentsForceRefresh(); err != nil {
			log.LogErrorf("action[doDirectWriteByAppend] inode %v GetExtents err %v", s.inode, err)
			return
		}
	}
	log.LogDebugf("action[doDirectWriteByAppend] inode %v process over!", s.inode)
	return
}

func (s *Streamer) doOverwrite(req *ExtentRequest, direct bool, storageClass uint32) (total int, err error) {
	var dp *wrapper.DataPartition

	err = s.flush()
	if err != nil {
		return
	}

	offset := req.FileOffset
	size := req.Size

	// the extent key needs to be updated because when preparing the requests,
	// the obtained extent key could be a local key which can be inconsistent with the remote key.
	req.ExtentKey = s.extents.Get(uint64(offset))
	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ek(%v)", s.inode, req.ExtentKey))
		return
	}

	ekFileOffset := int(req.ExtentKey.FileOffset)
	ekExtOffset := int(req.ExtentKey.ExtentOffset)
	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		errors.Trace(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	retry := true
	if proto.IsCold(s.client.volumeType) || proto.IsStorageClassBlobStore(storageClass) {
		retry = false
	}

	sc := NewStreamConn(dp, false, s.client.streamRetryTimeout)

	for total < size {
		reqPacket := NewOverwritePacket(dp, req.ExtentKey.ExtentId, offset-ekFileOffset+total+ekExtOffset,
			s.inode, offset, s.client.dataWrapper.IsSnapshotEnabled)
		if s.client.dataWrapper.IsSnapshotEnabled {
			reqPacket.VerSeq = s.verSeq
			reqPacket.VerList = make([]*proto.VolVersionInfo, len(s.client.multiVerMgr.verList.VerList))
			copy(reqPacket.VerList, s.client.multiVerMgr.verList.VerList)
			reqPacket.ExtentType |= proto.MultiVersionFlag
			reqPacket.ExtentType |= proto.VersionListFlag
			log.LogDebugf("action[doOverwrite] inode %v extentid %v,extentOffset %v(%v,%v,%v,%v) offset %v, streamer seq %v", s.inode, req.ExtentKey.ExtentId, reqPacket.ExtentOffset,
				offset, ekFileOffset, total, ekExtOffset, offset, s.verSeq)
		}

		if direct {
			reqPacket.Opcode = proto.OpSyncRandomWriteVer
		}
		packSize := util.Min(size-total, util.BlockSize)
		copy(reqPacket.Data[:packSize], req.Data[total:total+packSize])
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		replyPacket := new(Packet)
		err = sc.Send(&retry, reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConnWithVer(conn, proto.ReadDeadlineTime)
			if e != nil {
				log.LogWarnf("Stream Writer doOverwrite: ino(%v) failed to read from connect, req(%v) err(%v)", s.inode, reqPacket, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}
			log.LogDebugf("action[doOverwrite] streamer verseq (%v) datanode rsp seq (%v) code(%v)", s.verSeq, replyPacket.VerSeq, replyPacket.ResultCode)
			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpLimitedIoErr {
				return LimitedIoError, true
			}

			if replyPacket.ResultCode == proto.OpTryOtherAddr {
				e = TryOtherAddrError
			}

			if replyPacket.ResultCode == proto.ErrCodeVersionOpError {
				e = proto.ErrCodeVersionOp
				log.LogDebugf("action[doOverwrite] .UpdateLatestVer verseq (%v) be updated by datanode rsp (%v) ", s.verSeq, replyPacket)
				s.verSeq = replyPacket.VerSeq
				s.extents.verSeq = s.verSeq
				s.client.UpdateLatestVer(&proto.VolVersionInfoList{VerList: replyPacket.VerList})
				return e, false
			}

			return e, false
		})

		proto.Buffers.Put(reqPacket.Data)
		reqPacket.Data = nil
		log.LogDebugf("doOverwrite: ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			if replyPacket.ResultCode == proto.ErrCodeVersionOpError {
				err = proto.ErrCodeVersionOp
				log.LogWarnf("doOverwrite: need retry.ino(%v) req(%v) reqPacket(%v) err(%v) replyPacket(%v)", s.inode, req, reqPacket, err, replyPacket)
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

func (s *Streamer) tryInitExtentHandlerByLastEk(offset, size int, isMigration bool) (isLastEkVerNotEqual bool) {
	storeMode := s.GetStoreMod(offset, size)
	getEndEkFunc := func() *proto.ExtentKey {
		if ek := s.extents.GetEndForAppendWrite(uint64(offset), s.verSeq, false); ek != nil && !storage.IsTinyExtent(ek.ExtentId) {
			return ek
		}
		return nil
	}

	checkVerFunc := func(currentEK *proto.ExtentKey) {
		if currentEK.GetSeq() != s.verSeq {
			log.LogDebugf("tryInitExtentHandlerByLastEk. exist ek seq %v vs request seq %v", currentEK.GetSeq(), s.verSeq)
			if int(currentEK.ExtentOffset)+int(currentEK.Size)+size > util.ExtentSize {
				s.closeOpenHandler()
				return
			}
			isLastEkVerNotEqual = true
		}
	}

	initExtentHandlerFunc := func(currentEK *proto.ExtentKey) {
		checkVerFunc(currentEK)
		log.LogDebugf("tryInitExtentHandlerByLastEk: found ek in ExtentCache, extent_id(%v) req_offset(%v) req_size(%v), currentEK [%v] streamer seq %v",
			currentEK.ExtentId, offset, size, currentEK, s.verSeq)
		dp, pidErr := s.client.dataWrapper.GetDataPartition(currentEK.PartitionId)
		if pidErr == nil {
			seq := currentEK.GetSeq()
			if isLastEkVerNotEqual {
				seq = s.verSeq
			}
			log.LogDebugf("tryInitExtentHandlerByLastEk NewExtentHandler")
			handler := NewExtentHandler(s, int(currentEK.FileOffset), storeMode, int(currentEK.Size), dp.MediaType, isMigration)
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
				log.LogDebugf("tryInitExtentHandlerByLastEk: close old handler, currentEK.PartitionId(%v)",
					currentEK.PartitionId)
				s.closeOpenHandler()
			}

			s.handler = handler
			s.dirty = false
			log.LogDebugf("tryInitExtentHandlerByLastEk: currentEK.PartitionId(%v) found", currentEK.PartitionId)
		} else {
			log.LogDebugf("tryInitExtentHandlerByLastEk: currentEK.PartitionId(%v) not found", currentEK.PartitionId)
		}
	}

	if storeMode == proto.NormalExtentType {
		if s.handler == nil {
			log.LogDebugf("tryInitExtentHandlerByLastEk: handler nil")
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
					log.LogDebugf("tryInitExtentHandlerByLastEk: getEndEkFunc get ek %v", ek)
					initExtentHandlerFunc(ek)
				} else {
					log.LogDebugf("tryInitExtentHandlerByLastEk: not found ek")
				}
			}
		}
	}

	return
}

// First, attempt sequential writes using neighboring extent keys. If the last extent has a different version,
// it indicates that the extent may have been fully utilized by the previous version.
// Next, try writing and directly checking the extent at the datanode. If the extent cannot be reused, create a new extent for writing.
func (s *Streamer) doWriteAppend(req *ExtentRequest, direct bool, storageClass uint32, isMigration bool) (writeSize int, err error) {
	var status int32
	// try append write, get response
	log.LogDebugf("action[streamer.write] doWriteAppend req: ExtentKey(%v) FileOffset(%v) size(%v)",
		req.ExtentKey, req.FileOffset, req.Size)
	// First, attempt sequential writes using neighboring extent keys. If the last extent has a different version,
	// it indicates that the extent may have been fully utilized by the previous version.
	// Next, try writing and directly checking the extent at the datanode. If the extent cannot be reused, create a new extent for writing.
	if writeSize, err, status = s.doWriteAppendEx(req.Data, req.FileOffset, req.Size, direct, true, storageClass, isMigration); status == LastEKVersionNotEqual {
		log.LogDebugf("action[streamer.write] tryDirectAppendWrite req %v FileOffset %v size %v", req.ExtentKey, req.FileOffset, req.Size)
		if writeSize, _, err, status = s.tryDirectAppendWrite(req, direct, storageClass, isMigration); status == int32(proto.OpTryOtherExtent) {
			log.LogDebugf("action[streamer.write] doWriteAppend again req %v FileOffset %v size %v", req.ExtentKey, req.FileOffset, req.Size)
			writeSize, err, _ = s.doWriteAppendEx(req.Data, req.FileOffset, req.Size, direct, false, storageClass, isMigration)
		}
	}
	log.LogDebugf("action[streamer.write] doWriteAppend status %v err %v", status, err)
	return
}

func (s *Streamer) doWriteAppendEx(data []byte, offset, size int, direct bool, reUseEk bool, storageClass uint32, isMigration bool) (total int, err error, status int32) {
	var (
		ek        *proto.ExtentKey
		storeMode int
	)

	// Small files are usually written in a single write, so use tiny extent
	// store only for the first write operation.
	storeMode = s.GetStoreMod(offset, size)

	log.LogDebugf("doWriteAppendEx enter: ino(%v) offset(%v) size(%v) storeMode(%v) storageClass(%v)",
		s.inode, offset, size, storeMode, storageClass)
	if proto.IsHot(s.client.volumeType) || proto.IsStorageClassReplica(storageClass) {
		if reUseEk {
			if isLastEkVerNotEqual := s.tryInitExtentHandlerByLastEk(offset, size, isMigration); isLastEkVerNotEqual {
				log.LogDebugf("doWriteAppendEx enter: ino(%v) tryInitExtentHandlerByLastEk worked but seq not equal", s.inode)
				status = LastEKVersionNotEqual
				return
			}
		} else if s.handler != nil {
			s.closeOpenHandler()
		}

		for i := 0; i < MaxNewHandlerRetry; i++ {
			if s.handler == nil {
				s.handler = NewExtentHandler(s, offset, storeMode, 0, storageClass, isMigration)
				s.dirty = false
			} else if s.handler.storeMode != storeMode {
				// store mode changed, so close open handler and start a new one
				err = s.closeOpenHandler()
				if err != nil {
					break
				}
				continue
			}
			ek, err = s.handler.write(data, offset, size, direct)
			if err == nil && ek != nil {
				ek.SetSeq(s.verSeq)
				if !s.dirty {
					s.dirtylist.Put(s.handler)
					s.dirty = true
				}
				break
			}

			log.LogDebugf("doWrite handler write failed so close open handler: ino(%v) offset(%v) size(%v) storeMode(%v) err(%v)",
				s.inode, offset, size, storeMode, err)

			err = s.closeOpenHandler()
			if err != nil {
				break
			}
		}
	} else {
		s.handler = NewExtentHandler(s, offset, storeMode, 0, storageClass, isMigration)
		s.dirty = false
		ek, err = s.handler.write(data, offset, size, direct)
		if err == nil && ek != nil {
			if !s.dirty {
				s.dirtylist.Put(s.handler)
				s.dirty = true
			}
		}

		err = s.closeOpenHandler()
	}

	if err != nil || ek == nil {
		log.LogErrorf("doWriteAppendEx error: ino(%v) offset(%v) size(%v) err(%v) ek(%v)", s.inode, offset, size, err, ek)
		return
	}

	// This ek is just a local cache for PrepareWriteRequest, so ignore discard eks here.
	_ = s.extents.Append(ek, false)
	total = size

	return
}

func (s *Streamer) flush() (err error) {
	for {
		element := s.dirtylist.Get()
		if element == nil {
			break
		}
		eh := element.Value.(*ExtentHandler)

		log.LogDebugf("Streamer flush begin: eh(%v)", eh)
		err = eh.flush()
		if err != nil {
			log.LogErrorf("Streamer flush failed: eh(%v)", eh)
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
			eh.flushPacket()
			if atomic.LoadInt32(&eh.inflight) > 0 {
				log.LogDebugf("Streamer traverse skipped: non-zero inflight, eh(%v)", eh)
				continue
			}
			err = eh.appendExtentKey()
			if err != nil {
				log.LogWarnf("Streamer traverse abort: appendExtentKey failed, eh(%v) err(%v)", eh, err)
				// set the streamer to error status to avoid further writes
				if err == syscall.EIO {
					eh.stream.setError()
				}
				return
			}
			s.dirtylist.Remove(element)
			eh.cleanup()
		} else {
			if s.traversed < streamWriterFlushPeriod {
				log.LogDebugf("Streamer traverse skipped: traversed(%v) eh(%v)", s.traversed, eh)
				continue
			}
			if err = eh.flush(); err != nil {
				log.LogWarnf("Streamer traverse flush: eh(%v) err(%v)", eh, err)
			}
		}
		log.LogDebugf("Streamer traverse end: eh(%v)", eh)
	}
	return
}

// note: The invocation of the closeOpenHandler function on an inode is serialized
// close open handler, then flush data
func (s *Streamer) closeOpenHandler() (err error) {
	log.LogDebugf("closeOpenHandler: streamer(%v)", s)
	defer func() {
		log.LogDebugf("closeOpenHandler: close success, stream(%v)", s)
	}()

	handler := s.handler
	if handler != nil {
		log.LogDebugf("closeOpenHandler: flush open handler now, eh(%v)", handler)
		err = handler.flush()
		if err != nil {
			log.LogErrorf("closeOpenHandler: eh(%v) flush failed, err %s", handler, err.Error())
			return
		}

		handler.setClosed()
		if !s.dirty {
			// in case the current handler is not on the dirty list and will not get cleaned up
			log.LogDebugf("closeOpenHandler need cleanup: eh(%v)", s.handler)
			s.handler.cleanup()
		}
		s.handler = nil
	}

	err = s.flush()
	if err != nil {
		log.LogErrorf("closeOpenHandler: flush extent failed, err %s", err.Error())
		return err
	}
	return nil
}

func (s *Streamer) open() {
	s.refcnt++
	log.LogDebugf("open: streamer(%v) refcnt(%v)", s, s.refcnt)
}

func (s *Streamer) release() error {
	s.refcnt--
	if s.client.AheadRead != nil {
		s.aheadReadEnable = s.client.AheadRead.enable
	}
	err := s.closeOpenHandler()
	if err != nil {
		s.abort()
	}
	log.LogDebugf("release: streamer(%v) refcnt(%v)", s, s.refcnt)
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
		log.LogDebugf("abort cleanup: eh(%v)", s.handler)
	}
}

func (s *Streamer) truncate(size int, fullPath string) error {
	if atomic.LoadInt32(&s.status) >= StreamerError {
		return errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}
	err := s.closeOpenHandler()
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

	s.extents.TruncDiscard(uint64(size))
	return s.GetExtentsForce()
}

func (s *Streamer) updateVer(verSeq uint64) (err error) {
	log.LogInfof("action[stream.updateVer] ver %v update to %v", s.verSeq, verSeq)
	if s.verSeq != verSeq {
		log.LogInfof("action[stream.updateVer] ver %v update to %v", s.verSeq, verSeq)
		s.verSeq = verSeq
		s.extents.verSeq = verSeq
	}
	return
}

func (s *Streamer) tinySizeLimit() int {
	return util.DefaultTinySizeLimit
}

func (s *Streamer) setError() {
	atomic.StoreInt32(&s.status, StreamerError)
}
