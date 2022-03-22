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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
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

	overWriteReq      []*OverWriteRequest
	overWriteReqMutex sync.Mutex
	appendWriteBuffer bool

	tinySize   int
	extentSize int
	innerSize  uint64

	readAhead    bool
	extentReader *ExtentReader

	writeLock sync.Mutex
	pendingPacketList []*Packet
}

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64, streamMap *ConcurrentStreamerMapSegment, appendWriteBuffer bool, readAhead bool) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, 64)
	s.done = make(chan struct{})
	s.dirtylist = NewDirtyExtentList()
	s.tinySize = client.tinySize
	s.extentSize = client.extentSize
	s.streamerMap = streamMap
	s.appendWriteBuffer = appendWriteBuffer
	s.readAhead = readAhead
	s.pendingPacketList = make([]*Packet, 0)
	s.innerSize = client.dataWrapper.innerSize
	s.wg.Add(1)
	go s.server()
	return s
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("Streamer{ino(%v)}", s.inode)
}

// TODO should we call it RefreshExtents instead?
func (s *Streamer) GetExtents(ctx context.Context) error {
	return s.extents.Refresh(ctx, s.inode, s.client.getExtents)
}

// GetExtentReader returns the extent reader.
// TODO: use memory pool
func (s *Streamer) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	if s.readAhead && s.extentReader != nil && s.extentReader.key.Equal(ek) {
		return s.extentReader, nil
	}

	partition, err := s.client.dataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}
	reader := NewExtentReader(s.inode, ek, partition, s.client.dataWrapper.FollowerRead(), s.readAhead)
	if s.readAhead {
		s.extentReader = reader
	}
	return reader, nil
}

func (s *Streamer) read(ctx context.Context, data []byte, offset uint64, size int) (total int, hasHole bool, err error) {
	var (
		readBytes       int
		requests        []*ExtentRequest
		revisedRequests []*ExtentRequest
		fileSize        uint64
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
		read, skipFlush := s.readFromCache(req)
		total += read
		if total == size {
			return
		}
		if skipFlush {
			break
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
	for _, req := range requests {
		if req.ExtentKey == nil {
			hasHole = true
			for i := range req.Data {
				req.Data[i] = 0
			}

			if req.FileOffset+uint64(req.Size) > fileSize {
				if req.FileOffset >= fileSize {
					return
				}
				req.Size = int(fileSize - req.FileOffset)
				total += req.Size
				err = io.EOF
				if total == 0 {
					log.LogWarnf("Stream read: ino(%v) userExpectOffset(%v) userExpectSize(%v) req(%v) filesize(%v)", s.inode, offset, size, req, fileSize)
				}
				return
			}

			// Reading a hole, just fill zero
			total += req.Size
			if log.IsDebugEnabled() {
				log.LogDebugf("Stream read hole: ino(%v) userExpectOffset(%v) userExpectSize(%v) req(%v) total(%v)", s.inode, offset, size, req, total)
			}
		} else if req.ExtentKey.PartitionId > 0 {
			switch req.ExtentKey.StoreType {
			case proto.InnerData:
				readBytes, err = s.readInnerData(ctx, req)
			default:
				readBytes, err = s.readNormalData(ctx, req)
			}
			if log.IsDebugEnabled() {
				log.LogDebugf("Stream read: ino(%v) userExpectOffset(%v) userExpectSize(%v) req(%v) readBytes(%v) err(%v)", s.inode, offset, size, req, readBytes, err)
			}
			total += readBytes
			if err != nil || readBytes < req.Size {
				if total == 0 {
					log.LogWarnf("Stream read: ino(%v) userExpectOffset(%v) userExpectSize(%v) req(%v) readBytes(%v) err(%v)", s.inode, offset, size, req, readBytes, err)
				}
				break
			}
		}
	}
	return
}

// concurrent read/write optimization
// If appendWriteBuffer is on, read all data from extent handler cache. Otherwise,
//   1.if data is in handler unflushed packet, read from the packet
//   2.if data has been written to data node, read from datanode without flushing
func (s *Streamer) readFromCache(req *ExtentRequest) (read int, skipFlush bool) {
	s.handlerMutex.Lock()
	defer s.handlerMutex.Unlock()
	if s.handler == nil {
		return
	}

	if !s.appendWriteBuffer {
		if s.handler.key != nil && uint64(req.FileOffset) >= s.handler.key.FileOffset &&
			req.FileOffset+uint64(req.Size) <= s.handler.key.FileOffset+uint64(s.handler.key.Size) {
			req.ExtentKey = s.handler.key
			skipFlush = true
			return
		}

		s.handler.packetMutex.RLock()
		defer s.handler.packetMutex.RUnlock()
		if s.handler.packet != nil && req.FileOffset >= s.handler.packet.KernelOffset &&
			req.FileOffset+uint64(req.Size) <= s.handler.packet.KernelOffset+uint64(s.handler.packet.Size) {
			off := int(req.FileOffset - s.handler.packet.KernelOffset)
			copy(req.Data, s.handler.packet.Data[off:off+req.Size])
			read += req.Size
			skipFlush = true
		}
		return
	}

	// read from extent handler packetList
	remainSize := req.Size
	currentOffset := req.FileOffset
	defer func() {
		if remainSize == 0 {
			skipFlush = true
		} else {
			log.LogErrorf("readFromCache cannot read enough data: expect(%v) read(%v) req(%v) packetList(%v) packet(%v)", req.Size, read, req, s.handler.packetList, s.handler.packet)
			read = 0
			req.Data = req.Data[:0]
		}
	}()
	s.handler.packetMutex.RLock()
	defer s.handler.packetMutex.RUnlock()

	for _, p := range s.handler.packetList {
		if remainSize == 0 {
			break
		}
		if currentOffset >= p.KernelOffset+uint64(p.Size) {
			continue
		}

		offset := int(currentOffset - p.KernelOffset)
		if offset < 0 {
			log.LogErrorf("readFromCache packet offset invalid: req(%v) packet(%v)", req, p)
			return
		}
		dataLen := remainSize
		if offset+dataLen > int(p.Size) {
			dataLen = int(p.Size) - offset
		}
		copy(req.Data[read:read+dataLen], p.Data[offset:offset+dataLen])
		read += dataLen
		currentOffset += uint64(dataLen)
		remainSize -= dataLen
	}
	if remainSize == 0 {
		return
	}

	// read from extent handler packet
	if s.handler.packet == nil {
		return
	}
	offset := int(currentOffset - s.handler.packet.KernelOffset)
	if offset < 0 {
		log.LogErrorf("readFromCache packet offset invalid: req(%v) packet(%v)", req, s.handler.packet)
		return
	}
	dataLen := remainSize
	if offset+dataLen > int(s.handler.packet.Size) {
		dataLen = int(s.handler.packet.Size) - offset
	}
	copy(req.Data[read:read+dataLen], s.handler.packet.Data[offset:offset+dataLen])
	read += dataLen
	remainSize -= dataLen
	return
}

func (s *Streamer) UpdateExpiredExtentCache(ctx context.Context) {
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

func (s *Streamer) readNormalData(ctx context.Context, req *ExtentRequest) (readBytes int, err error) {
	var reader *ExtentReader
	reader, err = s.GetExtentReader(req.ExtentKey)
	if err != nil {
		return
	}
	readBytes, err = reader.Read(ctx, req)
	return
}

func (s *Streamer) readInnerData(ctx context.Context, req *ExtentRequest) (readBytes int, err error) {
	var readData []byte
	readBytes, readData, err = s.client.getInnerData(ctx, s.inode, uint64(req.FileOffset), uint32(req.Size))
	if err != nil {
		return
	}
	copy(req.Data[:readBytes], readData[:readBytes])
	return
}

func (dp *DataPartition) chooseMaxAppliedDp(ctx context.Context, pid uint64, hosts []string, reqPacket *Packet) (targetHosts []string, isErr bool) {
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

func (dp *DataPartition) getDpAppliedID(ctx context.Context, pid uint64, addr string, orgPacket *Packet) (appliedID uint64, err error) {
	var conn *net.TCPConn
	if conn, err = StreamConnPool.GetConnect(addr); err != nil {
		log.LogWarnf("getDpAppliedID: failed to create connection, orgPacket(%v) pid(%v) dpHost(%v) err(%v)", orgPacket, pid, addr, err)
		return
	}

	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()

	p := NewPacketToGetDpAppliedID(ctx, pid)
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
