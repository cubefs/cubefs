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
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/client/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/remotecache"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/cubefs/cubefs/util/timeutil"
)

// One inode corresponds to one streamer. All the requests to the same inode will be queued.
// TODO rename streamer here is not a good name as it also handles overwrites, not just stream write.
type Streamer struct {
	client               *ExtentClient
	inode                uint64
	parentInode          uint64
	status               int32
	refcnt               int32
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
	rdonly               bool
	aheadReadEnable      bool
	aheadReadWindow      *AheadReadWindow
	fullPath             string

	// Async flush fields
	asyncFlushCh        chan *AsyncFlushRequest // channel for async flush requests
	asyncFlushDone      chan struct{}           // signal to stop async flush goroutine
	asyncFlushSemaphore chan struct{}           // semaphore to limit concurrent processAsyncFlushRequest executions
	asyncFlushWg        sync.WaitGroup          // wait group for async flush operations

	// Local async flush tracking map (per streamer)
	pendingAsyncFlushMap sync.Map // handler.id -> *AsyncFlushRequest (using ExtentHandler ID)
	asyncFlushCompleted  uint64   // monotonic counter for completed/removed pending requests

	// Handler protection for write operations
	writeInProgress     bool           // indicates if a write operation is in progress
	writeHandler        *ExtentHandler // handler being used for current write operation
	writeProtectionLock sync.Mutex     // protects write operation state

	aheadReadBlockSize uint32
	waitForFlush       bool
	// minimum file size to trigger ahead read (bytes)
	minReadAheadSize int
}

type bcacheKey struct {
	cacheKey     string
	extentKey    *proto.ExtentKey
	storageClass uint32
}

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64, openForWrite, isCache bool, fullPath string) *Streamer {
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
	s.fullPath = fullPath

	// Initialize async flush fields
	s.asyncFlushCh = make(chan *AsyncFlushRequest, asyncFlushQueueSize)
	s.asyncFlushDone = make(chan struct{})
	s.asyncFlushSemaphore = make(chan struct{}, asyncFlushSemaphoreSize)

	// Initialize local async flush tracking map
	// sync.Map is zero value ready, no initialization needed

	if log.EnableDebug() {
		log.LogDebugf("NewStreamer: streamer(%v), reqChSize %d", s, reqChanSize)
	}
	if s.openForWrite {
		err := s.client.forbiddenMigration(s.inode)
		if err != nil {
			log.LogWarnf("ino(%v) forbiddenMigration failed err %v", s.inode, err.Error())
			s.setError()
		}
	}
	if client.AheadRead != nil {
		s.aheadReadEnable = client.AheadRead.enable
		s.aheadReadBlockSize = util.CacheReadBlockSize
		// set min read ahead size from config, default 1MB when zero
		if client.extentConfig != nil && client.extentConfig.MinReadAheadSize > 0 {
			s.minReadAheadSize = client.extentConfig.MinReadAheadSize
		} else {
			s.minReadAheadSize = util.MB // 1MB default
		}
	}
	go s.server()
	go s.asyncBlockCache()
	go s.asyncFlushManager() // Start async flush manager
	return s
}

func (s *Streamer) SetParentInode(inode uint64) {
	s.parentInode = inode
}

func (s *Streamer) SetFullPath(fullPath string) {
	s.fullPath = fullPath
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	return fmt.Sprintf("Streamer{ino(%v), fullPath(%v), refcnt(%v), isOpen(%v) openForWrite(%v), request(%v), "+
		"eh(%v) waitForFlush(%v) addr(%p)}",
		s.inode, s.fullPath, atomic.LoadInt32(&s.refcnt), s.isOpen, s.openForWrite, len(s.request), s.handler, s.waitForFlush, s)
}

func (s *Streamer) pendingAsyncFlushCount() int {
	count := 0
	s.pendingAsyncFlushMap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
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
		if strings.Contains(err.Error(), "no writable data partition") {
			partition, err = s.client.dataWrapper.GetDataPartition(ek.PartitionId)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	if partition.IsDiscard {
		log.LogWarnf("GetExtentReader: datapartition %v is discard", partition.PartitionID)
		return nil, DpDiscardError
	}

	retryRead := true
	if proto.IsCold(s.client.volumeType) || proto.IsStorageClassBlobStore(storageClass) {
		retryRead = false
	}

	enableFollowerRead := s.client.dataWrapper.FollowerRead() && !s.client.dataWrapper.InnerReq()
	reader := NewExtentReader(s.inode, ek, partition, enableFollowerRead, retryRead)
	reader.maxRetryTimeout = s.client.streamRetryTimeout
	return reader, nil
}

func (s *Streamer) prepareReadRequestsChecked(data []byte, offset, size, maxRetry int) ([]*ExtentRequest, error) {
	hasUnresolvedReadReq := func(reqs []*ExtentRequest) bool {
		for _, req := range reqs {
			if req.ExtentKey == nil {
				continue
			}
			if req.ExtentKey.PartitionId == 0 || req.ExtentKey.ExtentId == 0 {
				return true
			}
		}
		return false
	}

	current := s.extents.PrepareReadRequests(offset, size, data)
	unresolved := hasUnresolvedReadReq(current)
	for retry := 0; unresolved && retry < maxRetry; retry++ {
		s.writeLock.Lock()
		flushErr := s.IssueFlushRequest()
		if flushErr != nil {
			s.writeLock.Unlock()
			return nil, flushErr
		}
		current = s.extents.PrepareReadRequests(offset, size, data)
		s.writeLock.Unlock()
		unresolved = hasUnresolvedReadReq(current)
		if unresolved {
			// Give async write/append pipeline a short window to publish resolved extent keys.
			time.Sleep(2 * time.Millisecond)
			log.LogWarnf("streamer.read unresolved extentkey retry: ino(%v) offset(%v) size(%v) retry(%v/%v) reqs(%v)",
				s.inode, offset, size, retry+1, maxRetry, current)
		}
	}
	if unresolved {
		return nil, errors.NewErrorf("streamer.read unresolved extentkey remains after retries, ino(%v) offset(%v) size(%v) retries(%v) reqs(%v)",
			s.inode, offset, size, maxRetry, current)
	}
	return current, nil
}

func (s *Streamer) recoverReadHoleByFlush(requests []*ExtentRequest, data []byte, offset, size, filesize int) (updated []*ExtentRequest, updatedFileSize int, err error) {
	var holeBytes int
	var holeInFileRange bool
	const holeRecoverMaxRetry = 5
	calcHoleStats := func(reqs []*ExtentRequest, currentFileSize int) (holeBytes int, holeInFileRange bool) {
		for _, req := range reqs {
			if req.ExtentKey != nil {
				continue
			}
			holeBytes += req.Size
			if req.FileOffset+req.Size <= currentFileSize {
				holeInFileRange = true
			}
		}
		return
	}

	updated = requests
	updatedFileSize = filesize
	holeBytes, holeInFileRange = calcHoleStats(updated, updatedFileSize)
	if !holeInFileRange {
		return
	}

	extentsSnapshot := func() interface{} {
		if s.extents == nil {
			return "<nil>"
		}
		return s.extents.List()
	}
	// TEMP FLUSH_TRACE: investigate write-read visibility window in LTP iogen01.
	for retry := 0; holeInFileRange && retry < holeRecoverMaxRetry; retry++ {
		log.LogWarnf("FLUSH_TRACE read_recover_pre: ino(%v) offset(%v) size(%v) retry(%v/%v) filesize(%v) dirty(%v) waitForFlush(%v) reqs(%v) extents(%v)",
			s.inode, offset, size, retry+1, holeRecoverMaxRetry, updatedFileSize, s.dirty, s.waitForFlush, updated, extentsSnapshot())
		log.LogWarnf("FLUSH_TRACE_PIPE read_recover_pre: ino(%v) offset(%v) size(%v) retry(%v/%v) pipe(%s)",
			s.inode, offset, size, retry+1, holeRecoverMaxRetry, s.flushTracePipeSnapshot())
		log.LogWarnf("streamer.read recover suspicious hole by flush: ino(%v) offset(%v) size(%v) retry(%v/%v) filesize(%v) reqs(%v)",
			s.inode, offset, size, retry+1, holeRecoverMaxRetry, updatedFileSize, updated)

		s.writeLock.Lock()
		if err = s.IssueFlushRequest(); err != nil {
			s.writeLock.Unlock()
			return
		}
		updated = s.extents.PrepareReadRequests(offset, size, data)
		s.writeLock.Unlock()

		updatedFileSize, _ = s.extents.Size()
		holeBytes, holeInFileRange = calcHoleStats(updated, updatedFileSize)
		log.LogWarnf("FLUSH_TRACE read_recover_post: ino(%v) offset(%v) size(%v) retry(%v/%v) filesize(%v) dirty(%v) waitForFlush(%v) holeInFileRange(%v) holeBytes(%v) reqs(%v) extents(%v)",
			s.inode, offset, size, retry+1, holeRecoverMaxRetry, updatedFileSize, s.dirty, s.waitForFlush, holeInFileRange, holeBytes, updated, extentsSnapshot())
		log.LogWarnf("FLUSH_TRACE_PIPE read_recover_post: ino(%v) offset(%v) size(%v) retry(%v/%v) pipe(%s)",
			s.inode, offset, size, retry+1, holeRecoverMaxRetry, s.flushTracePipeSnapshot())

		if holeInFileRange && retry+1 < holeRecoverMaxRetry {
			// Give in-flight append/extent publication a short window before next prepare.
			time.Sleep(2 * time.Millisecond)
		}
	}

	if holeInFileRange {
		log.LogWarnf("streamer.read suspicious hole: ino(%v) offset(%v) size(%v) filesize(%v) holeBytes(%v) dirty(%v) waitForFlush(%v) reqs(%v)",
			s.inode, offset, size, filesize, holeBytes, s.dirty, s.waitForFlush, requests)
	}
	return
}

func (s *Streamer) read(data []byte, offset int, size int, storageClass uint32) (total int, err error) {
	var (
		readBytes int
		reader    *ExtentReader
		requests  []*ExtentRequest
		inodeInfo *proto.InodeInfo
	)
	log.LogDebugf("action[streamer.read] ino(%v) offset %v size %v", s.inode, offset, size)
	defer log.LogDebugf("streamer read ino(%v) offset %v size %v", s.inode, offset, size)
	ctx := context.Background()
	if s.client.readLimit() {
		s.client.readLimiter.Wait(ctx)
	}
	s.client.LimitManager.ReadAlloc(ctx, size)
	requests, err = s.prepareReadRequestsChecked(data, offset, size, 50)
	if err != nil {
		log.LogErrorf("[read] failed to prepare checked requests, ino(%v) offset(%v) size(%v) err(%v)", s.inode, offset, size, err)
		return 0, err
	}

	filesize, _ := s.extents.Size()
	log.LogDebugf("read: ino(%v) requests(%v) filesize(%v)", s.inode, requests, filesize)
	requests, filesize, err = s.recoverReadHoleByFlush(requests, data, offset, size, filesize)
	if err != nil {
		log.LogErrorf("streamer.read recover flush failed: ino(%v) offset(%v) size(%v) err(%v)",
			s.inode, offset, size, err)
		return 0, err
	}

	for _, req := range requests {
		log.LogDebugf("action[streamer.read] req %v", req)
		if req.ExtentKey != nil && (req.ExtentKey.PartitionId == 0 || req.ExtentKey.ExtentId == 0) {
			log.LogWarnf("streamer.read unresolved extentkey after retries: ino(%v) req(%v), treat as hole for safety", s.inode, req)
			req.ExtentKey = nil
		}
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
			log.LogDebugf("Stream read: ino(%v) req(%v) s.needBCache(%v) s.client.bcacheEnable(%v) aheadReadEnable(%v) aheadReadBlockSize(%v) %p",
				s.inode, req, s.needBCache, s.client.bcacheEnable, s.aheadReadEnable, s.aheadReadBlockSize, s)
			if s.aheadReadEnable && filesize > s.minReadAheadSize {
				// Lazily initialize ahead read window when threshold is satisfied
				if s.aheadReadWindow == nil && s.client.AheadRead != nil {
					s.aheadReadWindow = NewAheadReadWindow(s.client.AheadRead, s)
				}
				bgTime := stat.BeginStat()
				readBytes, err = s.aheadRead(req, storageClass)
				if err == nil && readBytes == req.Size {
					stat.EndStat("ReadFromMem", err, bgTime, 1)
					total += readBytes
					continue
				}
				log.LogDebugf("aheadRead inode(%v) FileOffset(%v) readBytes(%v) reqSize(%v) err(%v)", s.inode, req.FileOffset, readBytes, req.Size, err)
			}
			// if s.needBCache {
			//	bcacheMetric := exporter.NewCounter("fileReadL1Cache")
			//	bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
			// }

			// skip hole,ek is not nil,read block cache firstly
			log.LogDebugf("Stream read: ino(%v) req(%v) s.client.bcacheEnable(%v) s.client.bcacheOnlyForNotSSD(%v) s.needBCache(%v)",
				s.inode, req, s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD, s.needBCache)
			if s.client.bcacheEnable && s.needBCache && filesize <= bcache.MaxFileSize {
				cacheKey := util.GenerateRepVolKey(s.client.volumeName, s.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, req.ExtentKey.FileOffset)
				inodeInfo, err = s.client.getInodeInfo(s.inode)
				if err != nil {
					log.LogErrorf("Streamer read: getInodeInfo failed. ino(%v) req(%v) err(%v)", s.inode, req, err)
					return 0, err
				}
				if !s.client.bcacheOnlyForNotSSD || (s.client.bcacheOnlyForNotSSD && inodeInfo.StorageClass != proto.StorageClass_Replica_SSD) {
					log.LogDebugf("Streamer read from bcache, ino(%v) storageClass(%v) s.client.bcacheEnable(%v) bcacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD)
					offset := req.FileOffset - int(req.ExtentKey.FileOffset)
					if s.client.loadBcache != nil {
						bcacheMetric := exporter.NewCounter("fileReadL1Cache")
						bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
						readBytes, err = s.client.loadBcache(s.client.volumeName, cacheKey, req.Data, uint64(offset), uint32(req.Size))
						if err == nil && readBytes == req.Size {
							total += req.Size
							bcacheMetric := exporter.NewCounter("fileReadL1CacheHit")
							bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
							if log.EnableDebug() {
								log.LogDebugf("TRACE Stream read. hit blockCache: cacheKey(%v) inode(%v) "+
									"offset(%v) readBytes(%v) goroutine(%v)", cacheKey, s.inode, offset, readBytes, getGoid())
							}
							continue
						}
						bcacheMissMetric := exporter.NewCounter("fileReadL1CacheMiss")

						bcacheMissMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
					}
					if log.EnableDebug() {
						log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) inode(%v) offset(%v) size(%v)"+
							"goroutine(%v)", cacheKey, s.inode, offset, req.Size, getGoid())
					}
				} else {
					log.LogDebugf("Streamer not read from bcache, ino(%v) storageClass(%v) s.client.bcacheEnable(%v) bcacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD)
				}
				log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) loadBcache(%v)", cacheKey, s.client.loadBcache)
			} else if s.enableRemoteCache() {
				inodeInfo, err = s.client.getInodeInfo(s.inode)
				if err != nil {
					log.LogErrorf("Streamer read: getInodeInfo failed. ino(%v) req(%v) err(%v)", s.inode, req, err)
					return 0, err
				}

				if s.client.forceRemoteCache || !s.client.RemoteCache.remoteCacheOnlyForNotSSD || (s.client.RemoteCache.remoteCacheOnlyForNotSSD && inodeInfo.StorageClass != proto.StorageClass_Replica_SSD) {
					log.LogDebugf("Streamer read from remoteCache, ino(%v) enableRemoteCache(true) storageClass(%v) remoteCacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.RemoteCache.remoteCacheOnlyForNotSSD)
					var cacheReadRequests []*remotecache.CacheReadRequest
					cacheReadRequests, err = s.prepareCacheRequests(uint64(req.FileOffset), uint64(req.Size), req.Data, inodeInfo.Generation)
					if err == nil {
						var read int
						remoteCacheMetric := exporter.NewCounter("readRemoteCache")
						remoteCacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
						if read, err = s.readFromRemoteCache(ctx, uint64(req.FileOffset), uint64(req.Size), cacheReadRequests); err == nil {
							remoteCacheHitMetric := exporter.NewCounter("readRemoteCacheHit")
							remoteCacheHitMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
							total += read
							continue
						}
					}
					if !proto.IsFlashNodeLimitError(err) {
						log.LogWarnf("Stream read: readFromRemoteCache failed: ino(%v) offset(%v) size(%v), err(%v)", s.inode, req.FileOffset, req.Size, err)
					}
				} else {
					log.LogDebugf("Streamer not read from remoteCache, ino(%v) enableRemoteCache(true) storageClass(%v) remoteCacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.RemoteCache.remoteCacheOnlyForNotSSD)
				}
			} else {
				log.LogDebugf("Streamer not read from remoteCache, ino(%v) enableRemoteCache(false)", s.inode)
			}

			// read extent
			reader, err = s.GetExtentReader(req.ExtentKey, storageClass)
			if err != nil {
				log.LogErrorf("action[streamer.read] req %v err %v", req, err)
				break
			}

			if s.client.bcacheEnable && s.needBCache && filesize <= bcache.MaxFileSize {
				inodeInfo, err = s.client.getInodeInfo(s.inode)
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
					case s.pendingCache <- bcacheKey{cacheKey: cacheKey, extentKey: req.ExtentKey}:
						if log.EnableDebug() {
							log.LogDebugf("action[streamer.read] blockCache send cacheKey %v for ino(%v) offset %v size %v goroutine(%v)",
								cacheKey, s.inode, req.FileOffset-int(req.ExtentKey.FileOffset), req.Size, getGoid())
						}
						if s.exceedBlockSize(req.ExtentKey.Size) {
							atomic.AddInt32(&s.client.inflightL1BigBlock, 1)
						}
					default:
						if log.EnableDebug() {
							log.LogDebugf("action[streamer.read] blockCache discard cacheKey %v for ino(%v) offset %v size %v  goroutine(%v)",
								cacheKey, s.inode, req.FileOffset-int(req.ExtentKey.FileOffset), req.Size, getGoid())
						}
					}
				}
			}
			bgTime := stat.BeginStat()
			readBytes, err = reader.Read(req)
			stat.EndStat("ReadFromDataNode", err, bgTime, 1)
			log.LogDebugf("TRACE Stream read: ino(%v) req(%v) readBytes(%v) err(%v) cost(%v)", s.inode, req, readBytes, err, time.Since(*bgTime))

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
			begin := time.Now()
			log.LogDebugf("asyncBlockCache: cacheKey=(%v) ek=(%v)", cacheKey, ek)

			// read full extent
			var data []byte
			if ek.Size == bcache.MaxBlockSize {
				data = buf.BCachePool.Get()
			} else {
				data = make([]byte, ek.Size)
			}
			reader, err := s.GetExtentReader(ek, pending.storageClass)
			if err != nil {
				log.LogErrorf("asyncBlockCache: GetExtentReader err %v", err)
				return
			}
			fullReq := NewExtentRequest(int(ek.FileOffset), int(ek.Size), data, ek)
			metric := exporter.NewTPCnt("bcache-read-cachedata")
			readBytes, err := reader.Read(fullReq)
			if err != nil || readBytes != len(data) {
				metric.SetWithLabels(err, map[string]string{exporter.Vol: s.client.volumeName})
				log.LogWarnf("asyncBlockCache: Stream read full extent error. fullReq(%v) readBytes(%v) err(%v)", fullReq, readBytes, err)
				if ek.Size == bcache.MaxBlockSize {
					buf.BCachePool.Put(data)
				}
				if s.exceedBlockSize(ek.Size) {
					atomic.AddInt32(&s.client.inflightL1BigBlock, -1)
				}
				return
			}
			log.LogDebugf("TRACE read. read blockCache cacheKey(%v) len_buf(%v) cost %v,", cacheKey, len(data), time.Since(begin).String())
			metric.SetWithLabels(err, map[string]string{exporter.Vol: s.client.volumeName})
			if s.client.cacheBcache != nil {
				begin = time.Now()
				s.client.cacheBcache(s.client.volumeName, cacheKey, data)
				log.LogDebugf("TRACE read. read blockCache cacheKey(%v) len_buf(%v) cost %v,", cacheKey, len(data), time.Since(begin).String())
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

func getGoid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(string(buf[:n]))[1]
	gid, _ := strconv.Atoi(idField)
	return gid
}

func (s *Streamer) UpdateStringPath(fullPath string) {
	s.fullPath = fullPath
}

// asyncFlushManager manages asynchronous flush operations using channel-based producer-consumer pattern
func (s *Streamer) asyncFlushManager() {
	log.LogDebugf("asyncFlushManager:  started for streamer(%v)", s)
	const (
		stuckAgeThreshold      = 5 * time.Minute
		noProgressPanicTimeout = 1 * time.Minute
	)
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	var (
		stalledSince       time.Time
		lastCompleted      = atomic.LoadUint64(&s.asyncFlushCompleted)
		lastOldestID       uint64
		lastOldestInflight int32
		lastOldestRequeue  uint64
	)
	for {
		select {
		case <-s.asyncFlushDone:
			log.LogDebugf("asyncFlushManager:  stopped for streamer(%v)", s)
			return
		case req, ok := <-s.asyncFlushCh:
			if !ok {
				// Channel is closed, exit the manager
				log.LogDebugf("asyncFlushManager:  asyncFlushCh closed, stopping asyncFlushManager for streamer(%v)", s)
				return
			}
			if req == nil {
				continue
			}
			// Check if we should stop processing new requests
			select {
			case <-s.asyncFlushDone:
				log.LogDebugf("asyncFlushManager: received stop signal, skipping request for handler(%v)", req.handler)
				// Streamer is being released, fail the request
				s.removePendingAsyncFlush(req.handler.id)
				req.finish(errors.New("streamer is being released"))
				continue
			default:
				// Continue processing
			}
			log.LogDebugf("asyncFlushManager: try processAsyncFlushRequest handler(%v)", req.handler)
			// Process the async flush request with semaphore to limit concurrent executions
			shouldBreak := false
			for {
				select {
				case s.asyncFlushSemaphore <- struct{}{}:
					go func() {
						defer func() { <-s.asyncFlushSemaphore }()
						s.processAsyncFlushRequest(req)
					}()
					shouldBreak = true
					break
				default:
					time.Sleep(time.Millisecond)
					log.LogDebugf("asyncFlushManager: handler(%v) asyncFlushSemaphore is full", req.handler)
				}
				if shouldBreak {
					break
				}
			}
		case <-t.C:
			oldestReq := s.getNextPendingAsyncFlush()
			if oldestReq == nil || oldestReq.handler == nil {
				stalledSince = time.Time{}
				lastCompleted = atomic.LoadUint64(&s.asyncFlushCompleted)
				lastOldestID = 0
				lastOldestInflight = 0
				lastOldestRequeue = 0
			} else {
				oldestID := oldestReq.handler.id
				oldestInflight := atomic.LoadInt32(&oldestReq.handler.inflight)
				oldestRequeue := atomic.LoadUint64(&oldestReq.requeueCount)
				completed := atomic.LoadUint64(&s.asyncFlushCompleted)
				progressed := completed != lastCompleted ||
					oldestID != lastOldestID ||
					oldestInflight != lastOldestInflight ||
					oldestRequeue != lastOldestRequeue

				if progressed {
					stalledSince = time.Time{}
					lastCompleted = completed
					lastOldestID = oldestID
					lastOldestInflight = oldestInflight
					lastOldestRequeue = oldestRequeue
				} else if stalledSince.IsZero() {
					stalledSince = time.Now()
				}

				oldestAge := time.Since(time.Unix(0, oldestReq.firstEnqueueAt))
				if oldestAge >= stuckAgeThreshold && !stalledSince.IsZero() && time.Since(stalledSince) >= noProgressPanicTimeout {
					pendingReqs := s.getPendingRequests()
					log.LogWarnf("asyncFlushManager stuck: inode(%v) oldestAge(%v) noProgressFor(%v) pendingReqs(%v) asyncFlushChLen(%v) dirtyListLen(%v) oldestHandler(%v) oldestInflight(%v) oldestRequeue(%v) completed(%v)",
						s.inode, oldestAge, time.Since(stalledSince), pendingReqs, len(s.asyncFlushCh), s.dirtylist.Len(),
						oldestID, oldestInflight, oldestRequeue, completed)
					// Rate limit repeated stuck warnings for the same no-progress window.
					stalledSince = time.Now()
				}
			}
			if s.rdonly {
				log.LogDebugf("rdonly stream no need to start asyncFlushManager routine. ino %d", s.inode)
				return
			}
			if !s.isOpen && len(s.asyncFlushCh) == 0 {
				log.LogDebugf("asyncFlushManager  is done for streamer(%v)  closed", s.inode)
				return
			}
		}
	}
}

// processAsyncFlushRequest processes a single async flush request
func (s *Streamer) processAsyncFlushRequest(req *AsyncFlushRequest) {
	// Add to wait group to track this operation
	s.asyncFlushWg.Add(1)
	defer s.asyncFlushWg.Done()

	handler := req.handler
	log.LogDebugf("processAsyncFlushRequest:start  handler id %v", handler.id)
	// Note: asyncFlushDone check is now handled in asyncFlushManager
	// to prevent new requests from being processed when streamer is being released

	// Check if inflight count has decreased (packets completed)
	currentInflight := atomic.LoadInt32(&handler.inflight)
	if currentInflight == 0 {
		log.LogDebugf("processAsyncFlushRequest: handler %v currentInflight == 0 ", handler)
		// All packets completed, process extent keys
		go s.completeAsyncFlush(req)
		return
	}

	// Re-queue for next check
	select {
	case s.asyncFlushCh <- req:
		// Successfully re-queued
		cnt := req.markRequeue()
		if cnt <= 3 || cnt%1000 == 0 {
			log.LogWarnf("processAsyncFlushRequest:re-queued handler(%v) inflight(%v) requeueCount(%v)",
				handler, currentInflight, cnt)
		}
	default:
		log.LogDebugf("processAsyncFlushRequest: completeAsyncFlush handler %v", handler)
		// Channel is full or closed, process immediately
		go s.completeAsyncFlush(req)
	}
}

// completeAsyncFlush completes an async flush operation
func (s *Streamer) completeAsyncFlush(req *AsyncFlushRequest) {
	// Add to wait group to track this operation
	s.asyncFlushWg.Add(1)
	defer func() {
		s.asyncFlushWg.Done()
	}()

	handler := req.handler
	log.LogDebugf("completeAsyncFlush: streamer(%v) eh(%v) start", s.inode, handler)
	if !s.isActiveHandlerFlushRequest(handler.id, req) {
		s.finishStaleAsyncFlushRequest(req, "entry-check")
		return
	}

	nextReq := s.getNextPendingAsyncFlush()
	if nextReq == nil {
		// Avoid false negatives caused by concurrent map updates/racing completion.
		s.requeueAsyncFlushRequestOrFinish(req, "empty-pending-snapshot")
		return
	}

	if nextReq.handler.id > handler.id {
		log.LogWarnf("completeAsyncFlush: streamer(%v) handler(%v) is skipped, nextReq(%v)",
			s.inode, handler, nextReq.handler.id)
		s.removePendingAsyncFlush(handler.id)
		req.finish(nil)
		return
	}
	if nextReq.handler.id < handler.id {
		log.LogDebugf("completeAsyncFlush: streamer(%v) eh(%v) id(%v) is not next in sequence (next: %v), waiting...",
			s.inode, handler, handler.id, nextReq.handler.id)
		// Wait for the correct request to be processed
		// This is a simple polling approach - in a production system, you might want to use channels or condition variables
		for {
			select {
			case s.asyncFlushCh <- req:
				// Successfully re-queued
				cnt := req.markRequeue()
				if cnt <= 3 || cnt%1000 == 0 {
					log.LogWarnf("completeAsyncFlush:re-queued handler(%v) next(%v) requeueCount(%v)",
						handler, nextReq.handler.id, cnt)
				}
				return
			default:
				nextReq = s.getNextPendingAsyncFlush()
				if nextReq == nil {
					if !s.isActiveHandlerFlushRequest(handler.id, req) {
						s.finishStaleAsyncFlushRequest(req, "wait-loop-check")
						return
					}
					s.requeueAsyncFlushRequestOrFinish(req, "wait-loop-empty-pending-snapshot")
					return
				}
				if nextReq.handler.id >= handler.id {
					goto end
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
end:
	err := handler.flush()
	if err != nil {
		log.LogWarnf("completeAsyncFlush: completed failed for handler(%v)", handler)
	} else {
		log.LogDebugf("completeAsyncFlush: completed successfully for handler(%v) err(%v)", handler, err)
		if req.clearFunc != nil {
			req.clearFunc()
		}
	}
	s.removePendingAsyncFlush(handler.id)
	req.finish(err)
}

func (s *Streamer) finishStaleAsyncFlushRequest(req *AsyncFlushRequest, phase string) {
	log.LogWarnf("completeAsyncFlush: stale request ignored for streamer(%v) handler(%v) phase(%v)",
		s.inode, req.handler, phase)
	if value, exists := s.pendingAsyncFlushMap.Load(req.handler.id); exists {
		activeReq := value.(*AsyncFlushRequest)
		if activeReq != req {
			// Do not allow stale wait=true callers to return success early.
			// Join active request completion to preserve flush(wait=true) semantics.
			log.LogWarnf("completeAsyncFlush: stale request join active for streamer(%v) handler(%v) phase(%v)",
				s.inode, req.handler, phase)
			<-activeReq.done
			req.finish(activeReq.err)
			return
		}
	}
	req.finish(nil)
}

func (s *Streamer) requeueAsyncFlushRequestOrFinish(req *AsyncFlushRequest, reason string) {
	select {
	case s.asyncFlushCh <- req:
		cnt := req.markRequeue()
		if cnt <= 3 || cnt%1000 == 0 {
			log.LogWarnf("completeAsyncFlush:re-queued handler(%v) reason(%v) requeueCount(%v)",
				req.handler, reason, cnt)
		}
	default:
		log.LogWarnf("completeAsyncFlush: queue full, fallback finish for streamer(%v) handler(%v) reason(%v)",
			s.inode, req.handler, reason)
		s.removePendingAsyncFlush(req.handler.id)
		req.finish(nil)
	}
}

// requestAsyncFlush initiates an asynchronous flush for a handler
func (s *Streamer) requestAsyncFlush(handler *ExtentHandler, clearFunc func()) *AsyncFlushRequest {
	log.LogDebugf("requestAsyncFlush handler %v", handler)

	// Check if this handler already has an active async flush request
	if s.isHandlerFlushActive(handler.id) {
		existingReq := s.getActiveHandlerFlush(handler.id)
		if existingReq != nil {
			log.LogDebugf("Handler %v already has active async flush request, returning existing", handler.id)
			return existingReq
		}
	}

	req := &AsyncFlushRequest{
		handler:        handler,
		done:           make(chan struct{}),
		firstEnqueueAt: time.Now().UnixNano(),
		clearFunc:      clearFunc,
	}

	// Add to pending map using handler.id as key (both for sequencing and duplicate prevention)
	s.addPendingAsyncFlush(handler.id, req)

	// Check if asyncFlushCh is closed before sending
	select {
	case <-s.asyncFlushDone:
		// Streamer is being released, fail the request immediately
		log.LogWarnf("requestAsyncFlush: streamer is being released, failing request for handler(%v)", handler)
		s.removePendingAsyncFlush(handler.id)
		req.finish(errors.New("streamer is being released"))
		return req
	default:
		// Continue with normal processing
	}

	// Send to channel (non-blocking)
	select {
	case s.asyncFlushCh <- req:
		log.LogDebugf("Requested async flush for handler(%v) inflight(%v)",
			handler, atomic.LoadInt32(&handler.inflight))
	default:
		// Channel is full, process immediately
		log.LogWarnf("Async flush channel full, processing immediately for handler(%v)", handler)
		go s.completeAsyncFlush(req)
	}

	return req
}

// isHandlerFlushActive checks if a handler already has an active async flush request
func (s *Streamer) isHandlerFlushActive(handlerID uint64) bool {
	_, exists := s.pendingAsyncFlushMap.Load(handlerID)
	return exists
}

// getActiveHandlerFlush returns the active request for a handler
func (s *Streamer) getActiveHandlerFlush(handlerID uint64) *AsyncFlushRequest {
	if value, exists := s.pendingAsyncFlushMap.Load(handlerID); exists {
		return value.(*AsyncFlushRequest)
	}
	return nil
}

func (s *Streamer) isActiveHandlerFlushRequest(handlerID uint64, req *AsyncFlushRequest) bool {
	if value, exists := s.pendingAsyncFlushMap.Load(handlerID); exists {
		return value.(*AsyncFlushRequest) == req
	}
	return false
}

// addPendingAsyncFlush adds a request to the pending map using handler.id as key
func (s *Streamer) addPendingAsyncFlush(handlerID uint64, req *AsyncFlushRequest) {
	s.pendingAsyncFlushMap.Store(handlerID, req)
	if log.EnableDebug() {
		log.LogWarnf("addPendingAsyncFlush: streamer(%v) handler(%v) firstEnqueueAt(%v) trace(%v)",
			s.inode, handlerID, req.firstEnqueueAt, string(debug.Stack()))
	}
}

// removePendingAsyncFlush removes a request from the pending map
func (s *Streamer) removePendingAsyncFlush(handlerID uint64) {
	value, exists := s.pendingAsyncFlushMap.Load(handlerID)
	s.pendingAsyncFlushMap.Delete(handlerID)
	if exists {
		atomic.AddUint64(&s.asyncFlushCompleted, 1)
	}
	if log.EnableDebug() {
		if exists {
			req := value.(*AsyncFlushRequest)
			log.LogWarnf("removePendingAsyncFlush streamer(%v) handler(%v) existed(true) requeueCount(%v) ageMs(%v) trace(%v)",
				s.inode, handlerID, atomic.LoadUint64(&req.requeueCount),
				time.Since(time.Unix(0, req.firstEnqueueAt)).Milliseconds(), string(debug.Stack()))
		} else {
			log.LogWarnf("removePendingAsyncFlush streamer(%v) handler(%v) existed(false) trace(%v)",
				s.inode, handlerID, string(debug.Stack()))
		}
	}
}

// getPendingRequestsCount returns the number of pending requests
func (s *Streamer) getPendingRequests() []uint64 {
	ids := make([]uint64, 0)
	s.pendingAsyncFlushMap.Range(func(key, value interface{}) bool {
		ids = append(ids, value.(*AsyncFlushRequest).handler.id)
		return true
	})
	return ids
}

// waitForAllAsyncFlushRequests waits for all async flush requests to be processed
// Returns true if all requests were processed successfully, false if timeout
func (s *Streamer) waitForAllAsyncFlushRequests() bool {
	checkInterval := 10 * time.Millisecond // Check every 10ms
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("waitForAllAsyncFlushRequests", nil, bgTime, 1)
	}()
	start := time.Now()
	for {
		// Check if all requests are processed
		pendingReqs := s.getPendingRequests()
		channelLen := len(s.asyncFlushCh)
		dirtyListLen := s.dirtylist.Len()

		if atomic.LoadInt32(&s.status) >= StreamerError {
			log.LogErrorf("streamer(%v) is error status, skip waitForAllAsyncFlushRequests", s.inode)
			return true
		}
		if len(pendingReqs) == 0 && channelLen == 0 {
			return true
		}
		// Wait before next check
		time.Sleep(checkInterval)
		if timeutil.GetCurrentTime().Sub(start) > time.Minute*2 {
			start = time.Now()
			log.LogErrorf("Timeout Wait for streamer(%v), pending: %d, channel: %d channelLen: %d",
				s.inode, pendingReqs, channelLen, dirtyListLen)
		}
	}
}

// Write protection functions
func (s *Streamer) startWriteProtection(handler *ExtentHandler) {
	s.writeProtectionLock.Lock()
	defer s.writeProtectionLock.Unlock()
	s.writeInProgress = true
	s.writeHandler = handler
	log.LogDebugf("Write protection started for handler(%v) in streamer(%v)", handler, s.inode)
}

func (s *Streamer) endWriteProtection() {
	s.writeProtectionLock.Lock()
	defer s.writeProtectionLock.Unlock()
	s.writeInProgress = false
	s.writeHandler = nil
	log.LogDebugf("Write protection ended for streamer(%v)", s.inode)
}

func (s *Streamer) isHandlerProtected(handler *ExtentHandler) bool {
	s.writeProtectionLock.Lock()
	defer s.writeProtectionLock.Unlock()
	return s.writeInProgress && s.writeHandler == handler
}

// getNextPendingAsyncFlush returns the next pending request that should be processed
func (s *Streamer) getNextPendingAsyncFlush() *AsyncFlushRequest {
	var oldestHandlerID uint64 = ^uint64(0) // Max uint64
	var oldestReq *AsyncFlushRequest

	s.pendingAsyncFlushMap.Range(func(key, value interface{}) bool {
		handlerID := key.(uint64)
		req := value.(*AsyncFlushRequest)
		if handlerID < oldestHandlerID {
			oldestHandlerID = handlerID
			oldestReq = req
		}
		return true // continue iteration
	})

	return oldestReq
}
