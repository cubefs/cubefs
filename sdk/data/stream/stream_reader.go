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
	"fmt"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/exporter"
	"golang.org/x/net/context"
	"io"
	"sync"
	"sync/atomic"
	"time"

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

	writeLock            sync.Mutex
	inflightEvictL1cache sync.Map
	pendingCache         chan bcacheKey
}

type bcacheKey struct {
	cacheKey  string
	extentKey *proto.ExtentKey
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
	s.pendingCache = make(chan bcacheKey, 1)
	go s.server()
	go s.asyncBlockCache()
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
	if s.client.disableMetaCache || !s.needBCache {
		return s.extents.RefreshForce(s.inode, s.client.getExtents)
	}

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

	if partition.IsDiscard {
		log.LogWarnf("GetExtentReader: datapartition %v is discard", partition.PartitionID)
		return nil, DpDiscardError
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
	s.client.LimitManager.ReadAlloc(ctx, size)
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
			log.LogDebugf("Stream read: ino(%v) req(%v) s.needBCache(%v) s.client.bcacheEnable(%v)", s.inode, req, s.needBCache, s.client.bcacheEnable)
			if s.needBCache {
				bcacheMetric := exporter.NewCounter("fileReadL1Cache")
				bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
			}

			//skip hole,ek is not nil,read block cache firstly
			log.LogDebugf("Stream read: ino(%v) req(%v) s.client.bcacheEnable(%v) s.needBCache(%v)", s.inode, req, s.client.bcacheEnable, s.needBCache)
			cacheKey := util.GenerateRepVolKey(s.client.volumeName, s.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, req.ExtentKey.FileOffset)
			if s.client.bcacheEnable && s.needBCache && filesize <= bcache.MaxFileSize {
				offset := req.FileOffset - int(req.ExtentKey.FileOffset)
				if s.client.loadBcache != nil {
					readBytes, err = s.client.loadBcache(cacheKey, req.Data, uint64(offset), uint32(req.Size))
					if err == nil && readBytes == req.Size {
						total += req.Size
						bcacheMetric := exporter.NewCounter("fileReadL1CacheHit")
						bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
						log.LogDebugf("TRACE Stream read. hit blockCache: ino(%v) cacheKey(%v) readBytes(%v) err(%v)", s.inode, cacheKey, readBytes, err)
						continue
					}
				}
				log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) loadBcache(%v)", cacheKey, s.client.loadBcache)
			}

			if s.needBCache {
				bcacheMetric := exporter.NewCounter("fileReadL1CacheMiss")
				bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
			}

			//read extent
			reader, err = s.GetExtentReader(req.ExtentKey)
			if err != nil {
				break
			}

			if s.client.bcacheEnable && s.needBCache && filesize <= bcache.MaxFileSize {
				//limit big block cache
				if s.exceedBlockSize(req.ExtentKey.Size) && atomic.LoadInt32(&s.client.inflightL1BigBlock) > 10 {
					//do nothing
				} else {
					select {
					case s.pendingCache <- bcacheKey{cacheKey: cacheKey, extentKey: req.ExtentKey}:
						if s.exceedBlockSize(req.ExtentKey.Size) {
							atomic.AddInt32(&s.client.inflightL1BigBlock, 1)
						}
					default:
					}
				}

			}

			readBytes, err = reader.Read(req)
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

func (s *Streamer) asyncBlockCache() {
	if !s.needBCache || !s.isOpen {
		return
	}
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		select {
		case pending := <-s.pendingCache:
			ek := pending.extentKey
			cacheKey := pending.cacheKey
			log.LogDebugf("asyncBlockCache: cacheKey=(%v) ek=(%v)", cacheKey, ek)

			//read full extent
			var data []byte
			if ek.Size == bcache.MaxBlockSize {
				data = buf.BCachePool.Get()
			} else {
				data = make([]byte, ek.Size)
			}
			reader, err := s.GetExtentReader(ek)
			fullReq := NewExtentRequest(int(ek.FileOffset), int(ek.Size), data, ek)
			readBytes, err := reader.Read(fullReq)
			if err != nil || readBytes != len(data) {
				log.LogWarnf("asyncBlockCache: Stream read full extent error. fullReq(%v) readBytes(%v) err(%v)", fullReq, readBytes, err)
				if ek.Size == bcache.MaxBlockSize {
					buf.BCachePool.Put(data)
				}
				if s.exceedBlockSize(ek.Size) {
					atomic.AddInt32(&s.client.inflightL1BigBlock, -1)
				}
				return
			}
			if s.client.cacheBcache != nil {
				log.LogDebugf("TRACE read. write blockCache cacheKey(%v) len_buf(%v),", cacheKey, len(data))
				s.client.cacheBcache(cacheKey, data)
			}
			if ek.Size == bcache.MaxBlockSize {
				buf.BCachePool.Put(data)
			}
			if s.exceedBlockSize(ek.Size) {
				atomic.AddInt32(&s.client.inflightL1BigBlock, -1)
			}
		case <-t.C:
			if s.refcnt <= 0 {
				s.isOpen = false
				return
			}
		}

	}
}

func (s *Streamer) exceedBlockSize(size uint32) bool {
	if size > bcache.BigExtentSize {
		return true
	}
	return false
}
