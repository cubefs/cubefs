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

package data

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/cubefs/cubefs/sdk/flash"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/net/context"
)

// One inode corresponds to one streamer. All the requests to the same inode will be queued.
// TODO rename streamer here is not a good name as it also handles overwrites, not just stream write.
type Streamer struct {
	client      *ExtentClient
	streamerMap *ConcurrentStreamerMapSegment

	inode uint64

	status int32

	refcnt int

	idle      int // how long there is no new request
	traversed int // how many times the streamer is traversed

	extents *ExtentCache
	once    sync.Once

	handler      *ExtentHandler // current open handler
	handlerMutex sync.Mutex
	dirtylist    *DirtyExtentList // dirty handlers
	dirty        bool             // whether current open handler is in the dirty list
	writeOp      int32

	request chan interface{} // request channel, write/flush/close
	done    chan struct{}    // stream writer is being closed
	wg      sync.WaitGroup

	overWriteBuffer   bool
	overWriteReq      []*ExtentRequest
	overWriteReqMutex sync.Mutex

	tinySize   int
	extentSize int

	writeLock         sync.Mutex
	pendingPacketList []*common.Packet

	bloomStatus bool

	initLock   sync.RWMutex
	initServer bool
}

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64, streamMap *ConcurrentStreamerMapSegment, overWriteBuffer bool) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, 64)
	s.done = make(chan struct{})
	s.dirtylist = NewDirtyExtentList()
	s.overWriteBuffer = overWriteBuffer
	s.tinySize = client.tinySize
	s.extentSize = client.extentSize
	s.streamerMap = streamMap
	s.initServer = false
	s.pendingPacketList = make([]*common.Packet, 0)
	s.bloomStatus = client.GetInodeBloomStatus(inode)
	return s
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("Streamer{ino(%v)}", s.inode)
}

func (s *Streamer) InitServer() {
	s.initLock.Lock()
	defer s.initLock.Unlock()
	if s.initServer {
		return
	}
	s.wg.Add(1)
	go s.server()
	s.initServer = true
}

func (s *Streamer) IssueWithoutServer(f func() error) (done bool, err error) {
	s.initLock.RLock()
	defer s.initLock.RUnlock()
	if s.initServer {
		return false, nil
	}
	err = f()
	return true, err
}

// TODO should we call it RefreshExtents instead?
func (s *Streamer) GetExtents(ctx context.Context) error {
	return s.extents.Refresh(ctx, s.inode, s.client.getExtents)
}

func (s *Streamer) InitExtents(ctx context.Context) bool {
	if !s.extents.initialized {
		s.GetExtents(ctx)
	}
	return s.extents.initialized
}

// GetExtentReader returns the extent reader.
// TODO: use memory pool
func (s *Streamer) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	partition, err := s.client.dataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}
	reader := NewExtentReader(s.inode, ek, partition, s.client.dataWrapper.FollowerRead())
	return reader, nil
}

func (s *Streamer) read(ctx context.Context, data []byte, offset uint64, size int) (total int, hasHole bool, err error) {
	var (
		requests          []*ExtentRequest
		revisedRequests   []*ExtentRequest
		cacheReadRequests []*flash.CacheReadRequest
		fileSize          uint64
	)
	ctx = context.Background()
	if s.client.readRate > 0 {
		s.client.readLimiter.Wait(ctx)
	}

	requests, fileSize = s.extents.PrepareRequests(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil || req.ExtentKey.PartitionId > 0 {
			continue
		}
		s.writeLock.Lock()
		if err = s.IssueFlushRequest(ctx); err != nil {
			s.writeLock.Unlock()
			return
		}
		revisedRequests, fileSize = s.extents.PrepareRequests(offset, size, data)
		s.writeLock.Unlock()
		break
	}
	if revisedRequests != nil {
		requests = revisedRequests
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("Stream read: ino(%v) userExpectOffset(%v) userExpectSize(%v) requests(%v) filesize(%v)", s.inode, offset, size, requests, fileSize)
	}
	if offset >= fileSize {
		return 0, false, io.EOF
	}
	holeSize, ioErr := s.readHoles(requests, fileSize)
	if ioErr != nil && ioErr != io.EOF {
		log.LogErrorf("Stream read failed: err:(%v)", ioErr)
		return 0, false, ioErr
	}
	hasHole = holeSize > 0
	total += holeSize

	if s.enableRemoteCache() {
		canReadSize := uint64(size)
		if offset+uint64(size) > fileSize {
			canReadSize = fileSize - offset
		}
		cacheReadRequests, err = s.prepareCacheRequests(offset, canReadSize, data)
		if err == nil {
			var read int
			if read, err = s.readFromRemoteCache(ctx, offset, uint64(size), cacheReadRequests); err == nil {
				return read, hasHole, ioErr
			}
		}
		log.LogWarnf("Stream read: readFromRemoteCache failed: ino(%v) offset(%v) size(%v), err(%v)", s.inode, offset, size, err)
	}

	var read int
	if read, err = s.readFromDataNode(ctx, requests, offset, uint64(size)); err != nil {
		log.LogErrorf("Stream read: readFromDataNode err, ino(%v), userExpectOffset(%v) userExpectSize(%v) requests(%v) err(%v)", s.inode, offset, size, requests, err)
	} else {
		total += read
		err = ioErr
	}
	return
}

func (s *Streamer) readHoles(requests []*ExtentRequest, fileSize uint64) (read int, err error) {
	for _, req := range requests {
		if req.ExtentKey == nil {
			if req.FileOffset >= fileSize {
				err = io.EOF
				return
			}
			for i := 0; i < req.Size; i++ {
				req.Data[i] = 0
			}
			if req.FileOffset+uint64(req.Size) > fileSize {
				req.Size = int(fileSize - req.FileOffset)
				read += req.Size
				err = io.EOF
				return
			}
			read += req.Size
			if log.IsDebugEnabled() {
				log.LogDebugf("Stream readHoles: ino(%v) req(%v) fileSize(%v)", s.inode, req, fileSize)
			}
		} else if req.ExtentKey.PartitionId == 0 {
			err = fmt.Errorf("readHoles: unexpected temporary ek(%v) inode(%v)", req.ExtentKey, s.inode)
			return
		}
	}
	return
}

func (s *Streamer) readFromDataNode(ctx context.Context, requests []*ExtentRequest, offset, size uint64) (read int, err error) {
	var tp = exporter.NewVolumeTPUs("dataRead", s.client.dataWrapper.volName)
	defer func() {
		tp.Set(err)
	}()

	var reader *ExtentReader
	for _, req := range requests {
		if req.ExtentKey == nil || req.ExtentKey.PartitionId == 0 {
			continue
		}
		var readBytes int
		reader, err = s.GetExtentReader(req.ExtentKey)
		if err != nil {
			break
		}
		readBytes, err = reader.Read(ctx, req)
		if log.IsDebugEnabled() {
			log.LogDebugf("Stream readFromDataNode: ino(%v) userExpectOffset(%v) userExpectSize(%v) req(%v) readBytes(%v) err(%v)", s.inode, offset, size, req, readBytes, err)
		}
		read += readBytes
		if err != nil || readBytes < req.Size {
			log.LogWarnf("Stream readFromDataNode: ino(%v) userExpectOffset(%v) userExpectSize(%v) req(%v) readBytes(%v) err(%v)", s.inode, offset, size, req, readBytes, err)
			break
		}
	}
	return
}

func (s *Streamer) UpdateExpiredExtentCache(ctx context.Context, readMaxOffset uint64) {
	if s.client.updateExtentsOnRead && s.client.inodeGet != nil {
		isUpdated := s.updateExtentCacheByReadOffset(ctx, readMaxOffset)
		if isUpdated {
			return
		}
	}

	// ROW in cross-region mode maybe insert a new ek
	if !s.client.dataWrapper.CrossRegionHATypeQuorum() {
		return
	}
	expireSecond := s.client.dataWrapper.extentCacheExpireSec
	if expireSecond <= 0 {
		return
	}
	if s.extents.IsExpired(expireSecond) {
		s.GetExtents(ctx)
	}
}

func (s *Streamer) updateExtentCacheByReadOffset(ctx context.Context, readMaxOffset uint64) (isUpdated bool) {
	extentsSize, extentsGen := s.extents.Size()
	if readMaxOffset <= extentsSize {
		return false
	}
	inodeInfo, err := s.client.inodeGet(ctx, s.inode)
	if err != nil {
		log.LogWarnf("updateExtentCacheByReadOffset: get inode(%v) info err(%v)", s.inode, err)
		return false
	}
	if extentsSize < inodeInfo.Size || extentsGen < inodeInfo.Generation {
		if log.IsDebugEnabled() {
			log.LogDebugf("updateExtentCacheByReadOffset: ino(%v) readMaxOffset(%v) but extents size(%v) gen(%v) is less than inode size(%v) gen(%v)",
				s.inode, readMaxOffset, extentsSize, extentsGen, inodeInfo.Size, inodeInfo.Generation)
		}
		s.GetExtents(ctx)
		return true
	}
	return false
}

func (dp *DataPartition) chooseMaxAppliedDp(ctx context.Context, pid uint64, hosts []string, reqPacket *common.Packet) (targetHosts []string, isErr bool) {
	isErr = false
	appliedIDslice := make(map[string]uint64, len(hosts))
	errSlice := make(map[string]error)
	var (
		wg           sync.WaitGroup
		lock         sync.Mutex
		maxAppliedID uint64
	)
	for _, host := range hosts {
		wg.Add(1)
		go func(curAddr string) {
			appliedID, err := dp.getDpAppliedID(ctx, pid, curAddr, reqPacket)
			ok := false
			lock.Lock()
			if err != nil {
				errSlice[curAddr] = err
			} else {
				appliedIDslice[curAddr] = appliedID
				ok = true
			}
			lock.Unlock()
			log.LogDebugf("chooseMaxAppliedDp: get apply id[%v] ok[%v] from host[%v], pid[%v]", appliedID, ok, curAddr, pid)
			wg.Done()
		}(host)
	}
	wg.Wait()
	if len(errSlice) >= (len(hosts)+1)/2 {
		isErr = true
		log.LogWarnf("chooseMaxAppliedDp err: reqPacket[%v] dp[%v], hosts[%v], appliedID[%v], errMap[%v]", reqPacket, pid, hosts, appliedIDslice, errSlice)
		return
	}
	targetHosts, maxAppliedID = getMaxApplyIDHosts(appliedIDslice)
	log.LogDebugf("chooseMaxAppliedDp: get max apply id[%v] from hosts[%v], pid[%v], reqPacket[%v]", maxAppliedID, targetHosts, pid, reqPacket)
	return
}

func (dp *DataPartition) getDpAppliedID(ctx context.Context, pid uint64, addr string, orgPacket *common.Packet) (appliedID uint64, err error) {
	var conn *net.TCPConn
	if conn, err = StreamConnPool.GetConnect(addr); err != nil {
		log.LogWarnf("getDpAppliedID: failed to create connection, orgPacket(%v) pid(%v) dpHost(%v) err(%v)", orgPacket, pid, addr, err)
		return
	}

	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()

	p := common.NewPacketToGetDpAppliedID(ctx, pid)
	if err = p.WriteToConnNs(conn, dp.ClientWrapper.connConfig.WriteTimeoutNs); err != nil {
		log.LogWarnf("getDpAppliedID: failed to WriteToConn, packet(%v) dpHost(%v) orgPacket(%v) err(%v)", p, addr, orgPacket, err)
		return
	}
	if err = p.ReadFromConnNs(conn, dp.ClientWrapper.connConfig.ReadTimeoutNs); err != nil {
		log.LogWarnf("getDpAppliedID: failed to ReadFromConn, packet(%v) dpHost(%v) orgPacket(%v) err(%v)", p, addr, orgPacket, err)
		return
	}
	if p.ResultCode != proto.OpOk {
		log.LogWarnf("getDpAppliedID: packet(%v) result code isn't ok(%v) from host(%v) orgPacket(%v)", p, p.ResultCode, addr, orgPacket)
		err = errors.NewErrorf("getDpAppliedID error: addr(%v) resultCode(%v) is not ok", addr, p.ResultCode)
		return
	}

	appliedID = binary.BigEndian.Uint64(p.Data)
	return appliedID, nil
}

func getMaxApplyIDHosts(appliedIDslice map[string]uint64) (targetHosts []string, maxID uint64) {
	maxID = uint64(0)
	targetHosts = make([]string, 0)
	for _, id := range appliedIDslice {
		if id >= maxID {
			maxID = id
		}
	}
	for addr, id := range appliedIDslice {
		if id == maxID {
			targetHosts = append(targetHosts, addr)
		}
	}
	return
}
