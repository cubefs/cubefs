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

package stream

import (
	"fmt"
	"hash/crc32"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/storage"
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
)

const (
	streamWriterFlushPeriod       = 3
	streamWriterIdleTimeoutPeriod = 10
)

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
	size int
	err  error
	done chan struct{}
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

func (s *Streamer) IssueWriteRequest(offset int, data []byte, flags int) (write int, err error) {
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

func (s *Streamer) IssueTruncRequest(size int) error {
	request := truncRequestPool.Get().(*TruncRequest)
	request.size = size
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

func (s *Streamer) server() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	for {
		select {
		case request := <-s.request:
			s.handleRequest(request)
			s.idle = 0
			s.traversed = 0
		case <-s.done:
			s.abort()
			log.LogDebugf("done server: evict, ino(%v)", s.inode)
			return
		case <-t.C:
			s.traverse()
			if s.refcnt <= 0 {
				s.client.streamerLock.Lock()
				if s.idle >= streamWriterIdleTimeoutPeriod && len(s.request) == 0 {
					delete(s.client.streamers, s.inode)
					if s.client.evictIcache != nil {
						s.client.evictIcache(s.inode)
					}
					s.client.streamerLock.Unlock()

					// fail the remaining requests in such case
					s.clearRequests()
					log.LogDebugf("done server: no requests for a long time, ino(%v)", s.inode)
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

func (s *Streamer) handleRequest(request interface{}) {
	switch request := request.(type) {
	case *OpenRequest:
		s.open()
		request.done <- struct{}{}
	case *WriteRequest:
		request.writeBytes, request.err = s.write(request.data, request.fileOffset, request.size, request.flags)
		request.done <- struct{}{}
	case *TruncRequest:
		request.err = s.truncate(request.size)
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
	default:
	}
}

func (s *Streamer) write(data []byte, offset, size, flags int) (total int, err error) {
	var direct bool

	if flags&proto.FlagsSyncWrite != 0 {
		direct = true
	}

	if flags&proto.FlagsAppend != 0 {
		filesize, _ := s.extents.Size()
		offset = filesize
	}

	log.LogDebugf("Streamer write enter: ino(%v) offset(%v) size(%v)", s.inode, offset, size)

	ctx := context.Background()
	s.client.writeLimiter.Wait(ctx)

	requests := s.extents.PrepareWriteRequests(offset, size, data)
	log.LogDebugf("Streamer write: ino(%v) prepared requests(%v)", s.inode, requests)

	// Must flush before doing overwrite
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		err = s.flush()
		if err != nil {
			return
		}
		requests = s.extents.PrepareWriteRequests(offset, size, data)
		log.LogDebugf("Streamer write: ino(%v) prepared requests after flush(%v)", s.inode, requests)
		break
	}

	for _, req := range requests {
		var writeSize int
		if req.ExtentKey != nil {
			writeSize, err = s.doOverwrite(req, direct)
		} else {
			writeSize, err = s.doWrite(req.Data, req.FileOffset, req.Size, direct)
		}
		if err != nil {
			log.LogErrorf("Streamer write: ino(%v) err(%v)", s.inode, err)
			break
		}
		total += writeSize
	}
	if filesize, _ := s.extents.Size(); offset+total > filesize {
		s.extents.SetSize(uint64(offset+total), false)
		log.LogDebugf("Streamer write: ino(%v) filesize changed to (%v)", s.inode, offset+total)
	}
	log.LogDebugf("Streamer write exit: ino(%v) offset(%v) size(%v) done total(%v) err(%v)", s.inode, offset, size, total, err)
	return
}

func (s *Streamer) doOverwrite(req *ExtentRequest, direct bool) (total int, err error) {
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
	ekFileOffset := int(req.ExtentKey.FileOffset)
	ekExtOffset := int(req.ExtentKey.ExtentOffset)
	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ekFileOffset(%v) ek(%v)", s.inode, ekFileOffset, req.ExtentKey))
		return
	}

	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		errors.Trace(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	sc := NewStreamConn(dp, false)

	for total < size {
		reqPacket := NewOverwritePacket(dp, req.ExtentKey.ExtentId, offset-ekFileOffset+total+ekExtOffset, s.inode, offset)
		if direct {
			reqPacket.Opcode = proto.OpSyncRandomWrite
		}
		packSize := util.Min(size-total, util.BlockSize)
		copy(reqPacket.Data[:packSize], req.Data[total:total+packSize])
		reqPacket.Size = uint32(packSize)
		reqPacket.CRC = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		replyPacket := new(Packet)
		err = sc.Send(reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime)
			if e != nil {
				log.LogWarnf("Stream Writer doOverwrite: ino(%v) failed to read from connect, req(%v) err(%v)", s.inode, reqPacket, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpTryOtherAddr {
				e = TryOtherAddrError
			}
			return e, false
		})

		proto.Buffers.Put(reqPacket.Data)
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

func (s *Streamer) doWrite(data []byte, offset, size int, direct bool) (total int, err error) {
	var (
		ek        *proto.ExtentKey
		storeMode int
	)

	// Small files are usually written in a single write, so use tiny extent
	// store only for the first write operation.
	if offset > 0 {
		storeMode = proto.NormalExtentType
	} else {
		storeMode = proto.TinyExtentType
	}

	log.LogDebugf("doWrite enter: ino(%v) offset(%v) size(%v) storeMode(%v)", s.inode, offset, size, storeMode)

	if s.handler == nil && storeMode == proto.NormalExtentType {
		if currentEK := s.extents.GetEnd(uint64(offset)); currentEK != nil && !storage.IsTinyExtent(currentEK.ExtentId) {
			handler := NewExtentHandler(s, int(currentEK.FileOffset), storeMode, int(currentEK.Size))
			handler.key = &proto.ExtentKey{
				FileOffset:   currentEK.FileOffset,
				PartitionId:  currentEK.PartitionId,
				ExtentId:     currentEK.ExtentId,
				ExtentOffset: currentEK.ExtentOffset,
				Size:         currentEK.Size,
			}
			s.handler = handler
			s.dirty = false
		}
	}

	for i := 0; i < MaxNewHandlerRetry; i++ {
		if s.handler == nil {
			s.handler = NewExtentHandler(s, offset, storeMode, 0)
			s.dirty = false
		} else if s.handler.storeMode != storeMode {
			// store mode changed, so close open handler and start a new one
			s.closeOpenHandler()
			continue
		}

		ek, err = s.handler.write(data, offset, size, direct)
		if err == nil && ek != nil {
			if !s.dirty {
				s.dirtylist.Put(s.handler)
				s.dirty = true
			}
			break
		}

		log.LogDebugf("doWrite handler write failed so close open handler: ino(%v) offset(%v) size(%v) storeMode(%v) err(%v)",
			s.inode, offset, size, storeMode, err)
		s.closeOpenHandler()
	}

	if err != nil || ek == nil {
		log.LogErrorf("doWrite error: ino(%v) offset(%v) size(%v) err(%v) ek(%v)", s.inode, offset, size, err, ek)
		return
	}

	// This ek is just a local cache for PrepareWriteRequest, so ignore discard eks here.
	_ = s.extents.Append(ek, false)
	total = size

	log.LogDebugf("doWrite exit: ino(%v) offset(%v) size(%v) ek(%v)", s.inode, offset, size, ek)
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
					atomic.StoreInt32(&eh.stream.status, StreamerError)
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

func (s *Streamer) closeOpenHandler() {
	// just in case to avoid infinite loop
	var cnt int = 2 * MaxPacketErrorCount

	handler := s.handler
	for handler != nil && cnt >= 0 {
		handler.setClosed()
		if s.dirtylist.Len() < MaxDirtyListLen {
			handler.flushPacket()
		} else {
			// TODO unhandled error
			handler.flush()
		}
		handler = handler.recoverHandler
		cnt--
	}

	if s.handler != nil {
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

func (s *Streamer) release() error {
	s.refcnt--
	if s.client.noFlushOnClose {
		log.LogDebugf("release without flush: streamer(%v) refcnt(%v)", s, s.refcnt)
		return nil
	}
	s.closeOpenHandler()
	err := s.flush()
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
	delete(s.client.streamers, s.inode)
	s.client.streamerLock.Unlock()

	if s.client.noFlushOnClose {
		s.closeOpenHandler()
		err := s.flush()
		if err != nil {
			s.abort()
			return errors.New(fmt.Sprintf("evict: streamer(%v) flush error(%v)", s, err))
		}
	}
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

func (s *Streamer) truncate(size int) error {
	s.closeOpenHandler()
	err := s.flush()
	if err != nil {
		return err
	}

	err = s.client.truncate(s.inode, uint64(size))
	if err != nil {
		return err
	}

	oldsize, _ := s.extents.Size()
	if oldsize <= size {
		s.extents.SetSize(uint64(size), true)
		return nil
	}

	s.extents.TruncDiscard(uint64(size))
	return s.GetExtents()
}

func (s *Streamer) tinySizeLimit() int {
	return util.DefaultTinySizeLimit
}
