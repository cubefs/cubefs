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
	"io"
	"sync"

	"golang.org/x/net/context"

	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// One inode corresponds to one streamer. All the requests to the same inode will be queued.
// TODO rename streamer here is not a good name as it also handles overwrites, not just stream write.
type Streamer struct {
	client      *ExtentClient
	inode       uint64
	parentInode uint64

	status int32

	refcnt int

	idle      int // how long there is no new request
	traversed int // how many times the streamer is traversed

	extents *ExtentCache
	once    sync.Once

	handler    *ExtentHandler   // current open handler
	dirtylist  *DirtyExtentList // dirty handlers
	dirty      bool             // whether current open handler is in the dirty list
	isOpen     bool
	needBCache bool

	request chan interface{} // request channel, write/flush/close
	done    chan struct{}    // stream writer is being closed

	writeLock       sync.Mutex
	inflightL1cache sync.Map
}

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.parentInode = 0
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, 64)
	s.done = make(chan struct{})
	s.dirtylist = NewDirtyExtentList()
	s.isOpen = true
	go s.server()
	return s
}

func (s *Streamer) SetParentInode(inode uint64) {
	s.parentInode = inode
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	return fmt.Sprintf("Streamer{ino(%v)}", s.inode)
}

// TODO should we call it RefreshExtents instead?
func (s *Streamer) GetExtents() error {
	return s.extents.Refresh(s.inode, s.client.getExtents)
}

func (s *Streamer) GetExtentsForce() error {
	return s.extents.RefreshForce(s.inode, s.client.getExtents)
}

// GetExtentReader returns the extent reader.
// TODO: use memory pool
func (s *Streamer) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	partition, err := s.client.dataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}

	retryRead := true
	if proto.IsCold(s.client.volumeType) {
		retryRead = false
	}

	reader := NewExtentReader(s.inode, ek, partition, s.client.dataWrapper.FollowerRead(), retryRead)
	return reader, nil
}

func (s *Streamer) read(data []byte, offset int, size int) (total int, err error) {
	//log.LogErrorf("==========> Streamer Read Enter, inode(%v).", s.inode)
	//t1 := time.Now()
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
				if req.FileOffset > filesize {
					return
				}
				req.Size = filesize - req.FileOffset
				total += req.Size
				err = io.EOF
				//if total == 0 {
				//	log.LogErrorf("read: ino(%v) req(%v) filesize(%v)", s.inode, req, filesize)
				//}
				//log.LogErrorf("==========> Streamer Read Exit, inode(%v), time[%v us].", s.inode, time.Since(t1).Microseconds())
				return
			}

			// Reading a hole, just fill zero
			total += req.Size
			log.LogDebugf("Stream read hole: ino(%v) req(%v) total(%v)", s.inode, req, total)
		} else {
			//skip hole,ek is not nil,read block cache firstly
			cacheKey := util.GenerateRepVolKey(s.client.volumeName, s.inode, req.ExtentKey.ExtentId, req.ExtentKey.FileOffset)
			if s.client.bcacheEnable && s.needBCache && req.FileOffset <= bcache.MaxBlockSize {
				//todo offset is ok for tinyextent?
				offset := req.FileOffset - int(req.ExtentKey.FileOffset)
				if s.client.loadBcache != nil {
					readBytes, err = s.client.loadBcache(cacheKey, req.Data, uint64(offset), uint32(req.Size))
					if err == nil && readBytes == req.Size {
						total += req.Size
						log.LogDebugf("TRACE Stream read. hit blockCache: ino(%v) cacheKey(%v) readBytes(%v) err(%v)", s.inode, cacheKey, readBytes, err)
						continue
					}
				}
				log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) loadBcache(%v)", cacheKey, s.client.loadBcache)
			}
			//read extent
			reader, err = s.GetExtentReader(req.ExtentKey)
			if err != nil {
				break
			}

			// todo :  optimization
			var needCache = false
			if _, ok := s.inflightL1cache.Load(cacheKey); !ok && s.client.bcacheEnable && s.needBCache {
				s.inflightL1cache.Store(cacheKey, true)
				needCache = true
			}

			var buf []byte
			if needCache && req.FileOffset <= bcache.MaxBlockSize {
				//read full extent
				buf = make([]byte, req.ExtentKey.Size)
				fullReq := NewExtentRequest(int(req.ExtentKey.FileOffset), int(req.ExtentKey.Size), buf, req.ExtentKey)
				readBytes, err = reader.Read(fullReq)
				if err != nil || readBytes != len(buf) {
					s.inflightL1cache.Delete(cacheKey)
					log.LogErrorf("ERROR Stream read. read full extent error. fullReq(%v) readBytes(%v) err(%v)", fullReq, readBytes, err)
					return
				}

				extentOffset := req.FileOffset - int(req.ExtentKey.FileOffset)
				readBytes = copy(req.Data, buf[extentOffset:extentOffset+req.Size])

				go func(cacheKey string, buf []byte) {
					if s.client.cacheBcache != nil {
						log.LogDebugf("TRACE read. write blockCache cacheKey(%v) len_buf(%v),", cacheKey, len(buf))
						s.client.cacheBcache(cacheKey, buf)
					} else {
						log.LogErrorf("TRACE Stream read. write blockCache is nil,loadBcache(%v)", s.client.loadBcache)
					}
					s.inflightL1cache.Delete(cacheKey)
				}(cacheKey, buf)

				log.LogDebugf("TRACE Stream read. read full extent Exit. fullReq(%v) readBytes(%v) err(%v)", fullReq, readBytes, err)
			} else {
				readBytes, err = reader.Read(req)
				if err != nil || readBytes != req.Size {
					log.LogErrorf("ERROR Stream read. read error. req(%v) readBytes(%v) err(%v)", req, readBytes, err)
					return
				}
			}

			log.LogDebugf("TRACE Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)

			total += readBytes

			if err != nil || readBytes < req.Size {
				if total == 0 {
					log.LogErrorf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)
				}
				break
			}
		}
	}
	//log.LogErrorf("==========> Streamer Read Exit, inode(%v), time[%v us].", s.inode, time.Since(t1).Microseconds())
	return
}
