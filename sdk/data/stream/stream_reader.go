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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/client/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// One inode corresponds to one streamer. All the requests to the same inode will be queued.
// TODO rename streamer here is not a good name as it also handles overwrites, not just stream write.
type Streamer struct {
	client               *ExtentClient
	inode                uint64
	parentInode          uint64
	status               int32
	refcnt               int
	idle                 int // how long there is no new request
	traversed            int // how many times the streamer is traversed
	extents              *ExtentCache
	once                 sync.Once
	handler              *ExtentHandler   // current open handler
	dirtylist            *DirtyExtentList // dirty handlers
	dirty                bool             // whether current open handler is in the dirty list
	isOpen               bool
	needBCache           bool
	request              chan interface{} // request channel, write/flush/close
	done                 chan struct{}    // stream writer is being closed
	writeLock            sync.Mutex
	inflightEvictL1cache sync.Map
	pendingCache         chan bcacheKey
	verSeq               uint64
	needUpdateVer        int32
	isCache              bool
	openForWrite         bool

	rdonly          bool
	aheadReadEnable bool
	aheadReadWindow *AheadReadWindow
}

type bcacheKey struct {
	cacheKey     string
	extentKey    *proto.ExtentKey
	storageClass uint32
}

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64, openForWrite, isCache bool) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.parentInode = 0
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, reqChanSize)
	s.done = make(chan struct{})
	s.dirtylist = NewDirtyExtentList()
	s.isOpen = true
	s.pendingCache = make(chan bcacheKey, 1)
	s.verSeq = client.multiVerMgr.latestVerSeq
	s.extents.verSeq = client.multiVerMgr.latestVerSeq
	s.openForWrite = openForWrite
	s.isCache = isCache
	log.LogDebugf("NewStreamer: streamer(%v)", s)
	if s.openForWrite {
		err := s.client.forbiddenMigration(s.inode)
		if err != nil {
			log.LogWarnf("ino(%v) forbiddenMigration failed err %v", s.inode, err.Error())
			s.setError()
		}
	}
	if client.AheadRead != nil {
		s.aheadReadEnable = client.AheadRead.enable
		s.aheadReadWindow = NewAheadReadWindow(client.AheadRead, s)
	}
	go s.server()
	go s.asyncBlockCache()
	return s
}

func (s *Streamer) SetParentInode(inode uint64) {
	s.parentInode = inode
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	return fmt.Sprintf("Streamer{ino(%v), refcnt(%v), isOpen(%v) openForWrite(%v), inflight(%v), eh(%v) addr(%p)}",
		s.inode, s.refcnt, s.isOpen, s.openForWrite, len(s.request), s.handler, s)
}

// TODO should we call it RefreshExtents instead?
func (s *Streamer) GetExtents(isMigration bool) error {
	if s.client.disableMetaCache || !s.needBCache {
		return s.extents.RefreshForce(s.inode, false, s.client.getExtents, s.isCache, s.openForWrite, isMigration)
	}

	return s.extents.Refresh(s.inode, s.client.getExtents, s.isCache, s.openForWrite, isMigration)
}

func (s *Streamer) GetExtentsForce() error {
	return s.extents.RefreshForce(s.inode, false, s.client.getExtents, s.isCache, s.openForWrite, false)
}

func (s *Streamer) GetExtentsForceRefresh() error {
	return s.extents.RefreshForce(s.inode, true, s.client.getExtents, s.isCache, s.openForWrite, false)
}

// GetExtentReader returns the extent reader.
// TODO: use memory pool
func (s *Streamer) GetExtentReader(ek *proto.ExtentKey, storageClass uint32) (*ExtentReader, error) {
	partition, err := s.client.dataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}

	if partition.IsDiscard {
		log.LogWarnf("GetExtentReader: datapartition %v is discard", partition.PartitionID)
		return nil, DpDiscardError
	}

	retryRead := true
	if proto.IsCold(s.client.volumeType) || proto.IsStorageClassBlobStore(storageClass) {
		retryRead = false
	}

	enableFollowerRead := s.client.dataWrapper.FollowerRead() && !s.client.InnerReq
	reader := NewExtentReader(s.inode, ek, partition, enableFollowerRead, retryRead)
	reader.maxRetryTimeout = s.client.streamRetryTimeout
	return reader, nil
}

func (s *Streamer) read(data []byte, offset int, size int, storageClass uint32) (total int, err error) {
	var (
		readBytes       int
		reader          *ExtentReader
		requests        []*ExtentRequest
		revisedRequests []*ExtentRequest
	)
	log.LogDebugf("action[streamer.read] ino(%v) offset %v size %v", s.inode, offset, size)
	ctx := context.Background()
	if s.client.readLimit() {
		s.client.readLimiter.Wait(ctx)
	}
	s.client.LimitManager.ReadAlloc(ctx, size)
	requests = s.extents.PrepareReadRequests(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		if req.ExtentKey.PartitionId == 0 || req.ExtentKey.ExtentId == 0 {
			s.writeLock.Lock()
			if err = s.IssueFlushRequest(); err != nil {
				log.LogErrorf("[read] failed to issue flush request, ino(%v) offset(%v) size(%v) err(%v) req(%v)", s.inode, offset, size, err, req)
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
		log.LogDebugf("action[streamer.read] req %v", req)
		if req.ExtentKey == nil {
			zeros := make([]byte, len(req.Data))
			copy(req.Data, zeros)

			if req.FileOffset+req.Size > filesize {
				if req.FileOffset > filesize {
					return
				}
				req.Size = filesize - req.FileOffset
				total += req.Size
				err = io.EOF
				return
			}

			// Reading a hole, just fill zero
			total += req.Size
			log.LogDebugf("Stream read hole: ino(%v) req(%v) total(%v)", s.inode, req, total)
		} else {
			log.LogDebugf("Stream read: ino(%v) req(%v) s.needBCache(%v) s.client.bcacheEnable(%v) aheadReadEnable(%v)", s.inode, req, s.needBCache, s.client.bcacheEnable, s.aheadReadEnable)
			if s.aheadReadEnable && req.ExtentKey.Size > util.CacheReadBlockSize {
				readBytes, err = s.aheadRead(req)
				if err == nil && readBytes == req.Size {
					total += readBytes
					continue
				}
				log.LogDebugf("aheadRead inode(%v) FileOffset(%v) readBytes(%v) reqSize(%v) err(%v)", s.inode, req.FileOffset, readBytes, req.Size, err)
			}
			if s.needBCache {
				bcacheMetric := exporter.NewCounter("fileReadL1Cache")
				bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
			}

			// skip hole,ek is not nil,read block cache firstly
			log.LogDebugf("Stream read: ino(%v) req(%v) s.client.bcacheEnable(%v) s.client.bcacheOnlyForNotSSD(%v) s.needBCache(%v)",
				s.inode, req, s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD, s.needBCache)
			if s.client.bcacheEnable && s.needBCache && filesize <= bcache.MaxFileSize {
				cacheKey := util.GenerateRepVolKey(s.client.volumeName, s.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, req.ExtentKey.FileOffset)
				inodeInfo, err := s.client.getInodeInfo(s.inode)
				if err != nil {
					log.LogErrorf("Streamer read: getInodeInfo failed. ino(%v) req(%v) err(%v)", s.inode, req, err)
					return 0, err
				}
				if !s.client.bcacheOnlyForNotSSD || (s.client.bcacheOnlyForNotSSD && inodeInfo.StorageClass != proto.StorageClass_Replica_SSD) {
					log.LogDebugf("Streamer read from bcache, ino(%v) storageClass(%v) s.client.bcacheEnable(%v) bcacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD)
					offset := req.FileOffset - int(req.ExtentKey.FileOffset)
					if s.client.loadBcache != nil {
						readBytes, err = s.client.loadBcache(cacheKey, req.Data, uint64(offset), uint32(req.Size))
						if err == nil && readBytes == req.Size {
							total += req.Size
							bcacheMetric := exporter.NewCounter("fileReadL1CacheHit")
							bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
							log.LogDebugf("TRACE Stream read. hit blockCache: ino(%v) storageClass(%v) cacheKey(%v) readBytes(%v) err(%v)",
								s.inode, inodeInfo.StorageClass, cacheKey, readBytes, err)
							continue
						}
						log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) loadBcache(%v)", cacheKey, s.client.loadBcache)
					}
					log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) loadBcache(%v)", cacheKey, s.client.loadBcache)
				} else {
					log.LogDebugf("Streamer not read from bcache, ino(%v) storageClass(%v) s.client.bcacheEnable(%v) bcacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD)
				}
			}

			if s.needBCache {
				bcacheMetric := exporter.NewCounter("fileReadL1CacheMiss")
				bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
			}

			// read extent
			reader, err = s.GetExtentReader(req.ExtentKey, storageClass)
			if err != nil {
				log.LogErrorf("action[streamer.read] req %v err %v", req, err)
				break
			}

			if s.client.bcacheEnable && s.needBCache && filesize <= bcache.MaxFileSize {
				inodeInfo, err := s.client.getInodeInfo(s.inode)
				if err != nil {
					log.LogErrorf("Streamer read: getInodeInfo failed. ino(%v) req(%v) err(%v)", s.inode, req, err)
					return 0, err
				}
				cacheKey := util.GenerateRepVolKey(s.client.volumeName, s.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, req.ExtentKey.FileOffset)
				// limit big block cache
				if s.exceedBlockSize(req.ExtentKey.Size) && atomic.LoadInt32(&s.client.inflightL1BigBlock) > 10 {
					// do nothing
				} else if !s.client.bcacheOnlyForNotSSD || (s.client.bcacheOnlyForNotSSD && inodeInfo.StorageClass != proto.StorageClass_Replica_SSD) {
					select {
					case s.pendingCache <- bcacheKey{cacheKey: cacheKey, extentKey: req.ExtentKey, storageClass: storageClass}:
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
	log.LogDebugf("action[streamer.read] offset %v size %v exit", offset, size)
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

			// read full extent
			var data []byte
			if ek.Size == bcache.MaxBlockSize {
				data = buf.BCachePool.Get()
			} else {
				data = make([]byte, ek.Size)
			}
			reader, _ := s.GetExtentReader(ek, pending.storageClass)
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
			if !s.isOpen {
				return
			}
		}
	}
}

func (s *Streamer) exceedBlockSize(size uint32) bool {
	return size > bcache.BigExtentSize
}
