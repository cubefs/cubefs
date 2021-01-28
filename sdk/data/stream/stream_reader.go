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
	client *ExtentClient
	inode  uint64

	status int32

	refcnt int

	idle      int // how long there is no new request
	traversed int // how many times the streamer is traversed

	extents *ExtentCache
	once    sync.Once

	handler   *ExtentHandler   // current open handler
	dirtylist *DirtyExtentList // dirty handlers
	dirty     bool             // whether current open handler is in the dirty list

	request chan interface{} // request channel, write/flush/close
	done    chan struct{}    // stream writer is being closed

	tinySize int

	writeLock sync.Mutex
}

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, 64)
	s.done = make(chan struct{})
	s.dirtylist = NewDirtyExtentList()
	s.tinySize = client.tinySize
	go s.server()
	return s
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	return fmt.Sprintf("Streamer{ino(%v)}", s.inode)
}

// TODO should we call it RefreshExtents instead?
func (s *Streamer) GetExtents() error {
	return s.extents.Refresh(s.inode, s.client.getExtents)
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

func (s *Streamer) read(data []byte, offset int, size int) (total int, err error) {
	var (
		readBytes       int
		reader          *ExtentReader
		requests        []*ExtentRequest
		revisedRequests []*ExtentRequest
	)

	ctx := context.Background()
	s.client.readLimiter.Wait(ctx)

	requests = s.extents.PrepareReadRequests(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		if req.ExtentKey.PartitionId == 0 || req.ExtentKey.ExtentId == 0 {
			s.writeLock.Lock()
			if err = s.IssueFlushRequest(); err != nil {
				s.writeLock.Unlock()
				return 0, err
			}
			revisedRequests = s.extents.PrepareReadRequests(offset, size, data)
			s.writeLock.Unlock()
			break
		}
	}

	if revisedRequests != nil {
		requests = revisedRequests
	}

	filesize, _ := s.extents.Size()
	log.LogDebugf("read: ino(%v) requests(%v) filesize(%v)", s.inode, requests, filesize)
	for _, req := range requests {
		if req.ExtentKey == nil {
			for i := range req.Data {
				req.Data[i] = 0
			}

			if req.FileOffset+req.Size > filesize {
				if req.FileOffset >= filesize {
					return
				}
				req.Size = filesize - req.FileOffset
				total += req.Size
				err = io.EOF
				if total == 0 {
					log.LogErrorf("read: ino(%v) req(%v) filesize(%v)", s.inode, req, filesize)
				}
				return
			}

			// Reading a hole, just fill zero
			total += req.Size
			log.LogDebugf("Stream read hole: ino(%v) req(%v) total(%v)", s.inode, req, total)
		} else {
			reader, err = s.GetExtentReader(req.ExtentKey)
			if err != nil {
				break
			}
			readBytes, err = reader.Read(req)
			log.LogDebugf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)
			total += readBytes
			if err != nil || readBytes < req.Size {
				if total == 0 {
					log.LogErrorf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)
				}
				break
			}
		}
	}
	return
}

func chooseMaxAppliedDp(pid uint64, hosts []string) (targetHosts []string, isErr bool) {
	log.LogDebugf("chooseMaxAppliedDp because of no leader: pid[%v], hosts[%v]", pid, hosts)
	appliedIDslice := make(map[string]uint64, len(hosts))
	errSlice := make(map[string]bool)
	var (
		wg           sync.WaitGroup
		lock         sync.Mutex
		maxAppliedID uint64
	)
	for _, host := range hosts {
		wg.Add(1)
		go func(curAddr string) {
			appliedID, err := getDpAppliedID(pid, curAddr)
			ok := false
			lock.Lock()
			if err != nil {
				errSlice[curAddr] = true
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
		log.LogErrorf("chooseMaxAppliedDp err: dp[%v], hosts[%v], appliedID[%v]", pid, hosts, appliedIDslice)
		return
	}
	targetHosts, maxAppliedID = getMaxApplyIDHosts(appliedIDslice)
	log.LogDebugf("chooseMaxAppliedDp: get max apply id[%v] from hosts[%v], pid[%v]", maxAppliedID, targetHosts, pid)
	return
}

func getDpAppliedID(pid uint64, addr string) (appliedID uint64, err error) {
	var conn *net.TCPConn
	if conn, err = StreamConnPool.GetConnect(addr); err != nil {
		log.LogErrorf("getDpAppliedID: failed to create connection, pid(%v) dpHost(%v)", pid, addr)
		return
	}

	defer func() {
		if err != nil {
			StreamConnPool.PutConnect(conn, true)
		} else {
			StreamConnPool.PutConnect(conn, false)
		}
	}()

	p := NewPacketToGetDpAppliedID(pid)
	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("getDpAppliedID: failed to WriteToConn, packet(%v) dpHost(%v)", p, addr)
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		log.LogErrorf("getDpAppliedID: failed to ReadFromConn, packet(%v) dpHost(%v)", p, addr)
		return
	}
	if p.ResultCode != proto.OpOk {
		log.LogErrorf("getDpAppliedID: packet(%v) result code isn't ok(%v) from host(%v)", p, p.ResultCode, addr)
		err = errors.New("getDpAppliedID error")
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
