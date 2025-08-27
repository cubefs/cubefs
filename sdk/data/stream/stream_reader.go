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
	asyncFlushSequencer  uint64   // local sequencer for this streamer

	// Handler protection for write operations
	writeInProgress     bool           // indicates if a write operation is in progress
	writeHandler        *ExtentHandler // handler being used for current write operation
	writeProtectionLock sync.Mutex     // protects write operation state
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
		s.aheadReadWindow = NewAheadReadWindow(client.AheadRead, s)
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
	return fmt.Sprintf("Streamer{ino(%v), fullPath(%v), refcnt(%v), isOpen(%v) openForWrite(%v), request(%v), eh(%v) addr(%p)}",
		s.inode, s.fullPath, atomic.LoadInt32(&s.refcnt), s.isOpen, s.openForWrite, len(s.request), s.handler, s)
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

func (s *Streamer) read(data []byte, offset int, size int, storageClass uint32) (total int, err error) {
	var (
		readBytes       int
		reader          *ExtentReader
		requests        []*ExtentRequest
		revisedRequests []*ExtentRequest
	)
	log.LogDebugf("action[streamer.read] ino(%v) offset %v size %v", s.inode, offset, size)
	defer log.LogDebugf("streamer read ino(%v) offset %v size %v", s.inode, offset, size)
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
			// if s.needBCache {
			//	bcacheMetric := exporter.NewCounter("fileReadL1Cache")
			//	bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
			// }

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
						bcacheMetric := exporter.NewCounter("fileReadL1Cache")
						bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
						readBytes, err = s.client.loadBcache(s.client.volumeName, cacheKey, req.Data, uint64(offset), uint32(req.Size))
						if err == nil && readBytes == req.Size {
							total += req.Size
							bcacheMetric := exporter.NewCounter("fileReadL1CacheHit")
							bcacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
							log.LogDebugf("TRACE Stream read. hit blockCache: cacheKey(%v) inode(%v) "+
								"offset(%v) readBytes(%v) goroutine(%v)", cacheKey, s.inode, offset, readBytes, getGoid())
							continue
						}
						bcacheMissMetric := exporter.NewCounter("fileReadL1CacheMiss")
						bcacheMissMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
					}
					log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) inode(%v) offset(%v) size(%v)"+
						"goroutine(%v)", cacheKey, s.inode, offset, req.Size, getGoid())
				} else {
					log.LogDebugf("Streamer not read from bcache, ino(%v) storageClass(%v) s.client.bcacheEnable(%v) bcacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.bcacheEnable, s.client.bcacheOnlyForNotSSD)
				}
				log.LogDebugf("TRACE Stream read. miss blockCache cacheKey(%v) loadBcache(%v)", cacheKey, s.client.loadBcache)
			} else if s.enableRemoteCache() {
				inodeInfo, err := s.client.getInodeInfo(s.inode)
				if err != nil {
					log.LogErrorf("Streamer read: getInodeInfo failed. ino(%v) req(%v) err(%v)", s.inode, req, err)
					return 0, err
				}

				if s.client.forceRemoteCache || !s.client.RemoteCache.remoteCacheOnlyForNotSSD || (s.client.RemoteCache.remoteCacheOnlyForNotSSD && inodeInfo.StorageClass != proto.StorageClass_Replica_SSD) {
					log.LogDebugf("Streamer read from remoteCache, ino(%v) enableRemoteCache(true) storageClass(%v) remoteCacheOnlyForNotSSD(%v)",
						s.inode, proto.StorageClassString(inodeInfo.StorageClass), s.client.RemoteCache.remoteCacheOnlyForNotSSD)
					var cacheReadRequests []*remotecache.CacheReadRequest
					cacheReadRequests, err = s.prepareCacheRequests(uint64(offset), uint64(size), data, inodeInfo.Generation)
					if err == nil {
						var read int
						remoteCacheMetric := exporter.NewCounter("readRemoteCache")
						remoteCacheMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
						if read, err = s.readFromRemoteCache(ctx, uint64(offset), uint64(size), cacheReadRequests); err == nil {
							remoteCacheHitMetric := exporter.NewCounter("readRemoteCacheHit")
							remoteCacheHitMetric.AddWithLabels(1, map[string]string{exporter.Vol: s.client.volumeName})
							return read, err
						}
					}
					if !proto.IsFlashNodeLimitError(err) {
						log.LogWarnf("Stream read: readFromRemoteCache failed: ino(%v) offset(%v) size(%v), err(%v)", s.inode, offset, size, err)
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
					case s.pendingCache <- bcacheKey{cacheKey: cacheKey, extentKey: req.ExtentKey}:
						log.LogDebugf("action[streamer.read] blockCache send cacheKey %v for ino(%v) offset %v size %v goroutine(%v)",
							cacheKey, s.inode, req.FileOffset-int(req.ExtentKey.FileOffset), req.Size, getGoid())
						if s.exceedBlockSize(req.ExtentKey.Size) {
							atomic.AddInt32(&s.client.inflightL1BigBlock, 1)
						}
					default:
						log.LogDebugf("action[streamer.read] blockCache discard cacheKey %v for ino(%v) offset %v size %v  goroutine(%v)",
							cacheKey, s.inode, req.FileOffset-int(req.ExtentKey.FileOffset), req.Size, getGoid())
					}
				}
			}
			bgTime := stat.BeginStat()
			readBytes, err = reader.Read(req)
			stat.EndStat("ReadFromDataNode", err, bgTime, 1)
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

	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
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
				req.done <- errors.New("streamer is being released")
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
			// Log stats
			stats := s.getAsyncFlushSequencerStats()
			log.LogInfof("Async flush sequencer stats - pending: %d, current_id: %d, oldest: %d, newest: %d, streamer: %v",
				stats["pending_requests"], stats["current_sequencer_id"],
				stats["oldest_request_id"], stats["newest_request_id"], s.inode)
		}
	}
}

// processAsyncFlushRequest processes a single async flush request
func (s *Streamer) processAsyncFlushRequest(req *AsyncFlushRequest) {
	// Add to wait group to track this operation
	s.asyncFlushWg.Add(1)
	defer s.asyncFlushWg.Done()

	handler := req.handler
	now := time.Now()
	log.LogDebugf("processAsyncFlushRequest:start  handler %v", handler)
	// Note: asyncFlushDone check is now handled in asyncFlushManager
	// to prevent new requests from being processed when streamer is being released

	// Log large extent processing for monitoring
	if handler.size > 64*1024*1024 { // 64MB
		log.LogInfof("Processing large extent async flush: handler(%v), size(%v), id(%v), inflight(%v)",
			handler, handler.size, req.id, req.inflightCount)
	}

	// Check if request has timed out
	if now.After(req.timeout) {
		log.LogDebugf("processAsyncFlushRequest:Async flush timeout for handler(%v), id(%v), retryCount(%v), inflight(%v), extentSize(%v), timeout(%v)",
			handler, req.id, req.retryCount, req.inflightCount, handler.size, now.Sub(req.timeout))
		if req.retryCount >= maxAsyncFlushRetries {
			// Max retries reached, fail the request
			s.removePendingAsyncFlush(req.handler.id)
			errMsg := fmt.Sprintf("async flush failed after %d retries, inflight: %d, extentSize: %d, handler %v",
				maxAsyncFlushRetries, req.inflightCount, handler.size, handler)
			log.LogErrorf("%v", errMsg)
			req.done <- errors.New(errMsg)
			return
		}

		// Retry the flush
		req.retryCount++
		timeout := calculateAsyncFlushTimeout(int64(handler.size))
		req.timeout = now.Add(timeout)
		req.inflightCount = atomic.LoadInt32(&handler.inflight)

		// Re-queue for retry
		select {
		case s.asyncFlushCh <- req:
			log.LogDebugf("processAsyncFlushRequest: Re-queued async flush for retry, handler(%v), id(%v), retryCount(%v)", handler, req.id, req.retryCount)
		default:
			// Channel is full or closed, process immediately
			go s.retryAsyncFlush(handler, req)
		}
		return
	}

	// Check if inflight count has decreased (packets completed)
	currentInflight := atomic.LoadInt32(&handler.inflight)
	if currentInflight < req.inflightCount {
		req.inflightCount = currentInflight
		if currentInflight == 0 {
			log.LogDebugf("processAsyncFlushRequest: handler %v currentInflight == 0 ", handler)
			// All packets completed, process extent keys
			go s.completeAsyncFlush(handler, req)
			return
		}
	}

	// Re-queue for next check
	select {
	case s.asyncFlushCh <- req:
		// Successfully re-queued
		log.LogDebugf("processAsyncFlushRequest:re-queued  handler %v", handler)
	default:
		log.LogDebugf("processAsyncFlushRequest: completeAsyncFlush handler %v", handler)
		// Channel is full or closed, process immediately
		go s.completeAsyncFlush(handler, req)
	}
}

// retryAsyncFlush retries a failed async flush
func (s *Streamer) retryAsyncFlush(handler *ExtentHandler, req *AsyncFlushRequest) {
	// Add to wait group to track this operation
	s.asyncFlushWg.Add(1)
	defer s.asyncFlushWg.Done()

	log.LogDebugf("retryAsyncFlush: handler(%v), retryCount(%v)", handler, req.retryCount)

	// Note: asyncFlushDone check is now handled in asyncFlushManager
	// to prevent new requests from being processed when streamer is being released

	// Wait for current inflight packets to complete
	handler.flushPacket()
	err := handler.waitForFlush()
	if err != nil {
		log.LogErrorf("retryAsyncFlush: waitForFlush failed, eh(%v), err %s", handler, err.Error())
		s.removePendingAsyncFlush(req.handler.id)
		req.done <- err
		return
	}

	// Check if this request is the next one to be processed
	nextReq := s.getNextPendingAsyncFlush()
	if nextReq == nil {
		log.LogErrorf("No pending async flush requests found for retry handler(%v), id(%v)", handler, req.id)
		req.done <- errors.New("no pending async flush requests")
		return
	}

	// If this is not the next request to be processed, wait
	if nextReq.id != req.id {
		log.LogDebugf("retryAsyncFlush: retry request id(%v) is not next in sequence (next: %v), waiting...", req.id, nextReq.id)

		// Wait for the correct request to be processed
		for {
			time.Sleep(time.Duration(asyncFlushCheckIntervalMs) * time.Millisecond)
			nextReq = s.getNextPendingAsyncFlush()
			if nextReq == nil {
				log.LogErrorf("No pending async flush requests found while waiting for retry id(%v)", req.id)
				req.done <- errors.New("no pending async flush requests")
				return
			}
			if nextReq.id == req.id {
				break
			}
		}
	}

	// Process extent keys (this is now guaranteed to be in sequence)
	err = handler.appendExtentKey()
	if err != nil {
		log.LogErrorf("retryAsyncFlush: appendExtentKey failed for handler(%v), id(%v): %v", handler, req.id, err)
		s.removePendingAsyncFlush(req.handler.id)
		req.done <- err
		return
	}

	// Remove from pending map after successful completion
	s.removePendingAsyncFlush(req.handler.id)

	// Success
	log.LogDebugf("retryAsyncFlush: completed successfully for handler(%v), id(%v)", handler, req.id)
	req.done <- nil
}

// completeAsyncFlush completes an async flush operation
func (s *Streamer) completeAsyncFlush(handler *ExtentHandler, req *AsyncFlushRequest) {
	// Add to wait group to track this operation
	s.asyncFlushWg.Add(1)
	defer s.asyncFlushWg.Done()

	log.LogDebugf("completeAsyncFlush: for handler(%v), id(%v)", handler, req.id)

	// Check if this request is the next one to be processed
	nextReq := s.getNextPendingAsyncFlush()
	if nextReq == nil {
		log.LogErrorf("completeAsyncFlush: No pending async flush requests found for handler(%v), id(%v)", handler, req.id)
		req.done <- errors.New("no pending async flush requests")
		return
	}

	// If this is not the next request to be processed, wait
	if nextReq.id != req.id {
		log.LogDebugf("Async flush request id(%v) is not next in sequence (next: %v), waiting...", req.id, nextReq.id)

		// Wait for the correct request to be processed
		// This is a simple polling approach - in a production system, you might want to use channels or condition variables
		for {
			time.Sleep(time.Duration(asyncFlushCheckIntervalMs) * time.Millisecond)
			nextReq = s.getNextPendingAsyncFlush()
			if nextReq == nil {
				log.LogErrorf("completeAsyncFlush: No pending async flush requests found while waiting for id(%v)", req.id)
				req.done <- errors.New("no pending async flush requests")
				return
			}
			if nextReq.id == req.id {
				break
			}
		}
	}

	handler.flushPacket()
	err := handler.waitForFlush()
	if err != nil {
		log.LogErrorf("completeAsyncFlush: waitForFlush failed, eh(%s), err %s", handler.String(), err.Error())
		s.removePendingAsyncFlush(req.handler.id)
		req.done <- err
		return
	}

	// Process extent keys (this is now guaranteed to be in sequence)
	err = handler.appendExtentKey()
	if err != nil {
		log.LogErrorf("completeAsyncFlush: appendExtentKey failed for handler(%v), id(%v): %v", handler, req.id, err)
		s.removePendingAsyncFlush(req.handler.id)
		req.done <- err
		return
	}

	// Remove from pending map after successful completion
	s.removePendingAsyncFlush(req.handler.id)

	// Log success metric
	log.LogDebugf("completeAsyncFlush: completed successfully for handler(%v), id(%v)", handler, req.id)
	req.done <- nil
}

// requestAsyncFlush initiates an asynchronous flush for a handler
func (s *Streamer) requestAsyncFlush(handler *ExtentHandler) chan error {
	log.LogDebugf("requestAsyncFlush handler %v", handler)

	// Check if this handler already has an active async flush request
	if s.isHandlerFlushActive(handler.id) {
		existingReq := s.getActiveHandlerFlush(handler.id)
		if existingReq != nil {
			log.LogDebugf("Handler %v already has active async flush request id(%v), returning existing", handler.id, existingReq.id)
			return existingReq.done
		}
	}

	// Calculate dynamic timeout based on extent size
	timeout := calculateAsyncFlushTimeout(int64(handler.size))

	req := &AsyncFlushRequest{
		id:            s.getNextAsyncFlushID(), // Assign sequential ID
		handler:       handler,
		done:          make(chan error, 1),
		timeout:       time.Now().Add(timeout),
		retryCount:    0,
		inflightCount: atomic.LoadInt32(&handler.inflight),
	}

	// Add to pending map using handler.id as key (both for sequencing and duplicate prevention)
	s.addPendingAsyncFlush(handler.id, req)

	// Check if asyncFlushCh is closed before sending
	select {
	case <-s.asyncFlushDone:
		// Streamer is being released, fail the request immediately
		log.LogWarnf("requestAsyncFlush: streamer is being released, failing request for handler(%v)", handler)
		s.removePendingAsyncFlush(handler.id)
		req.done <- errors.New("streamer is being released")
		return req.done
	default:
		// Continue with normal processing
	}

	// Send to channel (non-blocking)
	select {
	case s.asyncFlushCh <- req:
		log.LogDebugf("Requested async flush for handler(%v), id(%v), inflight(%v), timeout(%v)",
			handler, req.id, req.inflightCount, timeout)
	default:
		// Channel is full, process immediately
		log.LogWarnf("Async flush channel full, processing immediately for handler(%v), id(%v)", handler, req.id)
		log.LogDebugf("requestAsyncFlush: handler id %v", handler.id)
		go s.completeAsyncFlush(handler, req)
	}

	return req.done
}

// waitForAsyncFlush waits for an async flush to complete with timeout
func (s *Streamer) waitForAsyncFlush(handler *ExtentHandler, timeout time.Duration) error {
	done := s.requestAsyncFlush(handler)

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		// Timeout occurred, we can't easily remove from channel
		// The request will be processed and timeout will be detected
		return errors.New("async flush timeout")
	}
}

// getAsyncFlushSequencerStats returns detailed statistics about the async flush sequencer
// Note: This function is deprecated as sequencer is now per-streamer
func (s *Streamer) getAsyncFlushSequencerStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Get statistics from current streamer
	stats["current_sequencer_id"] = atomic.LoadUint64(&s.asyncFlushSequencer)
	stats["pending_requests"] = s.getPendingRequests()

	// Find oldest and newest request IDs
	var oldestReqID, newestReqID uint64
	var oldestHandlerID uint64 = ^uint64(0) // Max uint64
	var newestHandlerID uint64 = 0

	s.pendingAsyncFlushMap.Range(func(key, value interface{}) bool {
		handlerID := key.(uint64)
		req := value.(*AsyncFlushRequest)

		if handlerID < oldestHandlerID {
			oldestHandlerID = handlerID
			oldestReqID = req.id
		}
		if handlerID > newestHandlerID {
			newestHandlerID = handlerID
			newestReqID = req.id
		}
		return true
	})

	stats["oldest_request_id"] = oldestReqID
	stats["newest_request_id"] = newestReqID

	return stats
}

// Local async flush tracking methods for Streamer

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

// addPendingAsyncFlush adds a request to the pending map using handler.id as key
func (s *Streamer) addPendingAsyncFlush(handlerID uint64, req *AsyncFlushRequest) {
	s.pendingAsyncFlushMap.Store(handlerID, req)
}

// removePendingAsyncFlush removes a request from the pending map
func (s *Streamer) removePendingAsyncFlush(handlerID uint64) {
	s.pendingAsyncFlushMap.Delete(handlerID)
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
func (s *Streamer) waitForAllAsyncFlushRequests(timeout time.Duration) bool {
	startTime := time.Now()
	checkInterval := 10 * time.Millisecond // Check every 10ms
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("waitForAllAsyncFlushRequests", nil, bgTime, 1)
	}()

	for {
		// Check if all requests are processed
		pendingReqs := s.getPendingRequests()
		channelLen := len(s.asyncFlushCh)

		if len(pendingReqs) == 0 && channelLen == 0 {
			log.LogDebugf("All async flush requests processed for streamer(%v), pending: %d, channel: %d",
				s.inode, pendingReqs, channelLen)
			return true
		}

		// Check timeout
		if time.Since(startTime) > timeout {
			log.LogWarnf("Async flush timeout for streamer(%v), pending: %d, channel: %d, elapsed: %v",
				s.inode, pendingReqs, channelLen, time.Since(startTime))
			return false
		}

		// Wait before next check
		time.Sleep(checkInterval)
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
