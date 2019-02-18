// Copyright 2018 The Container File System Authors.
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

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
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

// OpenRequest defines an open request.
type OpenRequest struct {
	flag uint32
	err  error
	done chan struct{}
}

// WriteRequest defines a write request.
type WriteRequest struct {
	fileOffset int
	size       int
	data       []byte
	direct     bool
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
	flag uint32
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

func (s *Streamer) IssueOpenRequest(flag uint32) error {
	request := openRequestPool.Get().(*OpenRequest)
	request.done = make(chan struct{}, 1)
	request.flag = flag
	s.request <- request
	<-request.done
	err := request.err
	openRequestPool.Put(request)
	return err
}

func (s *Streamer) IssueWriteRequest(offset int, data []byte, direct bool) (write int, err error) {
	if s.authid == 0 {
		return 0, errors.New(fmt.Sprintf("IssueWriteRequest: not authorized, ino(%v)", s.inode))
	}

	if atomic.LoadInt32(&s.status) >= StreamerError {
		return 0, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}

	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.fileOffset = offset
	request.size = len(data)
	request.direct = direct
	request.done = make(chan struct{}, 1)
	s.request <- request
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

func (s *Streamer) IssueReleaseRequest(flag uint32) error {
	request := releaseRequestPool.Get().(*ReleaseRequest)
	request.done = make(chan struct{}, 1)
	request.flag = flag
	s.request <- request
	<-request.done
	err := request.err
	releaseRequestPool.Put(request)
	//s.done <- struct{}{}
	return err
}

func (s *Streamer) IssueTruncRequest(size int) error {
	if s.authid == 0 {
		return errors.New(fmt.Sprintf("IssueTruncRequest: not authorized, ino(%v)", s.inode))
	}

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
		case <-s.done:
			s.abort()
			log.LogDebugf("done server: evict, ino(%v)", s.inode)
			return
		case <-t.C:
			// TODO unhandled error
			s.traverse()
			if s.refcnt <= 0 {
				if s.idle >= 10 && len(s.request) == 0 {
					s.client.streamerLock.Lock()
					delete(s.client.streamers, s.inode)
					s.client.streamerLock.Unlock()

					// fail the remaining requests in such case
					s.clearRequests()
					log.LogDebugf("done server: no requests for a long time, ino(%v)", s.inode)
					return
				}
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
		request.err = syscall.EAGAIN
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
		log.LogDebugf("received an open request: ino(%v) flag(%v)", s.inode, request.flag)
		request.err = s.open(request.flag)
		log.LogDebugf("open returned: ino(%v) flag(%v)", s.inode, request.flag)
		request.done <- struct{}{}
	case *WriteRequest:
		request.writeBytes, request.err = s.write(request.data, request.fileOffset, request.size, request.direct)
		request.done <- struct{}{}
	case *TruncRequest:
		request.err = s.truncate(request.size)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = s.flush()
		request.done <- struct{}{}
	case *ReleaseRequest:
		request.err = s.release(request.flag)
		request.done <- struct{}{}
	case *EvictRequest:
		request.err = s.evict()
		request.done <- struct{}{}
	default:
	}
}

func (s *Streamer) write(data []byte, offset, size int, direct bool) (total int, err error) {
	log.LogDebugf("Streamer write enter: ino(%v) offset(%v) size(%v)", s.inode, offset, size)

	requests := s.extents.PrepareWriteRequests(offset, size, data)
	log.LogDebugf("Streamer write: ino(%v) prepared requests(%v)", s.inode, requests)
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
	ekFileOffset := int(req.ExtentKey.FileOffset)
	ekExtOffset := int(req.ExtentKey.ExtentOffset)

	// the extent key needs to be updated because when preparing the requests,
	// the obtained extent key could be a local key which can be inconsistent with the remote key.
	req.ExtentKey = s.extents.Get(uint64(offset))
	if req.ExtentKey == nil {
		err = errors.New(fmt.Sprintf("doOverwrite: extent key not exist, ino(%v) ekFileOffset(%v) ek(%v)", s.inode, ekFileOffset, req.ExtentKey))
		return
	}

	if dp, err = gDataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		// TODO unhandled error
		errors.Annotatef(err, "doOverwrite: ino(%v) failed to get datapartition, ek(%v)", s.inode, req.ExtentKey)
		return
	}

	sc := NewStreamConn(dp)

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
				return errors.Annotatef(e, "Stream Writer doOverwrite: ino(%v) failed to read from connect", s.inode), false
			}

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpNotLeaderErr {
				e = NotALeaderError
			}
			return e, false
		})

		proto.Buffers.Put(reqPacket.Data)
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

	if offset+size > s.tinySizeLimit() {
		storeMode = proto.NormalExtentType
	} else {
		storeMode = proto.TinyExtentType
	}

	log.LogDebugf("doWrite enter: ino(%v) offset(%v) size(%v) storeMode(%v)", s.inode, offset, size, storeMode)

	for i := 0; i < MaxNewHandlerRetry; i++ {
		if s.handler == nil {
			s.handler = NewExtentHandler(s, offset, storeMode)
			s.dirty = false
		}

		ek, err = s.handler.write(data, offset, size, direct)
		if err == nil && ek != nil {
			if !s.dirty {
				s.dirtylist.Put(s.handler)
				s.dirty = true
			}
			break
		}

		s.closeOpenHandler()
	}

	if err != nil || ek == nil {
		log.LogErrorf("doWrite error: ino(%v) offset(%v) size(%v) err(%v) ek(%v)", s.inode, offset, size, err, ek)
		return
	}

	s.extents.Append(ek, false)
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
	//var closed bool

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
			// and therefore there can be packet that has been be flashed yet.
			eh.flushPacket()
			if atomic.LoadInt32(&eh.inflight) > 0 {
				continue
			}
			err = eh.appendExtentKey()
			if err != nil {
				return
			}
			s.dirtylist.Remove(element)
			// TODO unhandled error
			eh.cleanup()
		}
		log.LogDebugf("Streamer traverse end: eh(%v)", eh)
	}
	return
}

func (s *Streamer) closeOpenHandler() {
	if s.handler != nil {
		s.handler.setClosed()
		if s.dirtylist.Len() < MaxDirtyListLen {
			s.handler.flushPacket()
		} else {
			// TODO unhandled error
			s.handler.flush()
		}

		if !s.dirty {
			// in case the current handler is not on the dirty list and will not get cleaned up
			// TODO unhandled error
			s.handler.cleanup()
		}
		s.handler = nil
	}
}

func (s *Streamer) open(flag uint32) error {
	s.refcnt++
	if !proto.IsWriteFlag(flag) {
		return nil
	}

	if s.authid == 0 {
		authid, err := s.client.open(s.inode, flag)
		if err != nil || authid == 0 {
			s.refcnt--
			return errors.New(fmt.Sprintf("open failed: streamer(%v) flag(%v)", s, flag))
		}
		s.authid = authid
	}

	s.openWriteCnt++
	log.LogDebugf("open: streamer(%v) openWriteCnt(%v) authid(%v)", s, s.openWriteCnt, s.authid)
	return nil
}

func (s *Streamer) release(flag uint32) error {
	s.refcnt--
	if !proto.IsWriteFlag(flag) {
		return nil
	}

	s.closeOpenHandler()
	err := s.flush()
	if err != nil {
		s.abort()
	}

	s.openWriteCnt--
	authid := s.authid
	if s.openWriteCnt <= 0 {
		// TODO unhandled error
		s.client.release(s.inode, authid)
		s.authid = 0
	}
	log.LogDebugf("release: streamer(%v) openWriteCnt(%v) authid(%v)", s, s.openWriteCnt, authid)
	return err
}

func (s *Streamer) evict() error {
	if s.refcnt > 0 {
		return errors.New(fmt.Sprintf("evict: streamer(%v) refcnt(%v) openWriteCnt(%v) authid(%v)", s, s.refcnt, s.openWriteCnt, s.authid))
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

	err = s.client.truncate(s.inode, s.authid, uint64(size))
	if err != nil {
		return err
	}

	oldsize, _ := s.extents.Size()
	if oldsize <= size {
		s.extents.SetSize(uint64(size), true)
		return nil
	}

	return s.GetExtents()
}

func (s *Streamer) tinySizeLimit() int {
	return util.DefaultTinySizeLimit
}
