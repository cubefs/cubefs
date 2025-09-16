// Copyright 2025 The CubeFS Authors.
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
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/google/uuid"
)

const (
	AheadReadBlockStateInit    uint32 = 0
	AheadReadBlockStateLoading uint32 = 1
)

type AheadReadBlock struct {
	inode       uint64
	partitionId uint64
	extentId    uint64
	offset      uint64
	size        uint64
	data        []byte
	time        int64
	readBytes   uint64
	state       uint32
	key         string
	lock        sync.RWMutex
}

var aheadReadBlockPool = sync.Pool{
	New: func() interface{} {
		return &AheadReadBlock{
			data: make([]byte, 0, util.CacheReadBlockSize),
		}
	},
}

func getAheadReadBlock() *AheadReadBlock {
	return aheadReadBlockPool.Get().(*AheadReadBlock)
}

func putAheadReadBlock(block *AheadReadBlock) {
	block.inode = 0
	block.partitionId = 0
	block.extentId = 0
	block.offset = 0
	block.size = 0
	block.data = block.data[:0]
	block.time = 0
	block.readBytes = 0
	aheadReadBlockPool.Put(block)
}

type AheadReadTask struct {
	p            *Packet
	dp           *wrapper.DataPartition
	time         time.Time
	req          *ExtentRequest
	cacheSize    int
	cacheType    string
	logTime      *time.Time
	reqID        string
	storageClass uint32
}

type AheadReadWindow struct {
	taskC        chan *AheadReadTask
	curTaskMap   map[string]interface{}
	curTaskMutex sync.RWMutex
	cache        *AheadReadCache
	streamer     *Streamer
	canAheadRead bool
}

type AheadReadCache struct {
	enable                bool
	blockTimeOut          int
	winCnt                int
	availableBlockC       chan struct{}
	availableBlockCnt     int64
	totalBlockCnt         int64
	stopC                 chan interface{}
	blockCache            sync.Map
	creatingBlockCacheMap sync.Map
}

func NewAheadReadCache(enable bool, totalMem int64, blockTimeOut, winCnt int) *AheadReadCache {
	if !enable {
		return nil
	}
	arc := &AheadReadCache{
		enable:        enable,
		blockTimeOut:  blockTimeOut,
		winCnt:        winCnt,
		stopC:         make(chan interface{}),
		totalBlockCnt: totalMem / util.CacheReadBlockSize,
	}
	atomic.StoreInt64(&arc.availableBlockCnt, arc.totalBlockCnt)
	arc.availableBlockC = make(chan struct{}, arc.availableBlockCnt)
	for i := int64(0); i < arc.totalBlockCnt; i++ {
		arc.availableBlockC <- struct{}{}
	}
	log.LogInfof("aheadRead enable(%v) totalMem(%v) availableBlockCnt(%v) winCnt(%v)", enable, totalMem, arc.availableBlockCnt, winCnt)
	go arc.checkBlockTimeOut()
	return arc
}

func (arc *AheadReadCache) getAheadReadBlock() (block *AheadReadBlock, err error) {
	select {
	case <-arc.availableBlockC:
		block = getAheadReadBlock()
		atomic.AddInt64(&arc.availableBlockCnt, -1)
	default:
		err = fmt.Errorf("availableBlockC is nil")
	}
	return
}

func (arc *AheadReadCache) putAheadReadBlock(key string, block *AheadReadBlock) {
	if log.EnableDebug() {
		log.LogDebugf("putAheadReadBlock recycle key(%v) addr(%p) %v", key, block, string(debug.Stack()))
	}
	arc.availableBlockC <- struct{}{}
	putAheadReadBlock(block)
	atomic.AddInt64(&arc.availableBlockCnt, 1)
}

func (arc *AheadReadCache) checkBlockTimeOut() {
	blockTimer := time.NewTimer(time.Second)
	printTicker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-arc.stopC:
			blockTimer.Stop()
			printTicker.Stop()
			return
		case <-blockTimer.C:
			blockTimer.Stop()
			arc.doCheckBlockTimeOut()
			blockTimer.Reset(time.Second)
		case <-printTicker.C:
			log.LogInfof("totalAheadReadBlockCnt(%v) curAvailableCnt(%v)", arc.totalBlockCnt, atomic.LoadInt64(&arc.availableBlockCnt))
		}
	}
}

func (arc *AheadReadCache) doCheckBlockTimeOut() {
	curTime := time.Now().Unix()
	arc.blockCache.Range(func(key, value interface{}) bool {
		bv := value.(*AheadReadBlock)
		bv.lock.Lock()
		// cache block is loading data form dataNode
		if atomic.LoadUint32(&bv.state) == AheadReadBlockStateLoading {
			log.LogDebugf("doCheckBlockTimeOut skip unread block: key(%v) addr(%p)", key, bv)
			bv.lock.Unlock()
			return true
		}
		if curTime-bv.time > int64(arc.blockTimeOut) {
			log.LogDebugf("doCheckBlockTimeOut delete readed block: key(%v) addr(%p)", key, bv)
			arc.blockCache.Delete(key.(string))
			// block is ready to recycle
			bv.key = ""
			bv.lock.Unlock()
			arc.putAheadReadBlock(key.(string), bv)
		} else {
			bv.lock.Unlock()
		}
		return true
	})
}

func (arc *AheadReadCache) Stop() {
	close(arc.stopC)
	close(arc.availableBlockC)
	arc.blockCache.Range(func(key, value interface{}) bool {
		arc.blockCache.Delete(key)
		return true
	})
}

func NewAheadReadWindow(arc *AheadReadCache, s *Streamer) *AheadReadWindow {
	arw := &AheadReadWindow{
		taskC:      make(chan *AheadReadTask, arc.winCnt),
		curTaskMap: make(map[string]interface{}),
		cache:      arc,
		streamer:   s,
	}
	go arw.backgroundAheadReadTask()
	return arw
}

func (arw *AheadReadWindow) backgroundAheadReadTask() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case task := <-arw.taskC:
			go arw.doTask(task)
		case <-ticker.C:
			if !arw.streamer.isOpen {
				log.LogInfof("stream is closed, exit background task, s %s", arw.streamer.String())
				return
			}
		}
	}
}

func (arw *AheadReadWindow) doTask(task *AheadReadTask) {
	var (
		err       error
		readBytes int
		realHost  = "invaild"
	)
	key := fmt.Sprintf("%v-%v-%v-%v", task.p.inode, task.p.PartitionID, task.p.ExtentID, task.p.ExtentOffset)
	if arw.putTaskIfNotExist(key) {
		return
	}
	defer func() {
		arw.deleteTask(key)
		if log.EnableDebug() {
			log.LogDebugf("aheadRead done:key(%v) size(%v) err(%v) %v reqID(%v)",
				task.req, task.cacheSize, err, time.Since(task.time), task.reqID)
		}
		stat.EndStat("PrepareAheadData", err, task.logTime, 1)
		stat.EndStat(fmt.Sprintf("PrepareAheadData[%v]", task.cacheType), err, task.logTime, 1)
		stat.EndStat(fmt.Sprintf("PrepareAheadData[%v][%v]", task.cacheType, realHost), err, task.logTime, 1)
	}()
	if _, ok := arw.cache.blockCache.Load(key); ok {
		return
	}
	_, loaded := arw.cache.creatingBlockCacheMap.LoadOrStore(key, make(chan struct{}))
	if loaded {
		return
	} else {
		defer func() {
			arw.cache.creatingBlockCacheMap.Delete(key)
		}()
	}
	cacheBlock, err := arw.cache.getAheadReadBlock()
	if err != nil {
		log.LogWarnf("aheadRead done:key(%v) no more block", key)
		return
	}
	cacheBlock.inode = task.p.inode
	cacheBlock.partitionId = task.p.PartitionID
	cacheBlock.extentId = task.p.ExtentID
	cacheBlock.offset = uint64(task.p.ExtentOffset)
	cacheBlock.size = uint64(task.cacheSize)
	cacheBlock.key = key
	// block cannot be removed by checking timeOut
	atomic.StoreUint32(&cacheBlock.state, AheadReadBlockStateLoading)
	arw.cache.blockCache.Store(key, cacheBlock)
	cacheBlock.time = time.Now().Unix()
	atomic.StoreUint64(&cacheBlock.readBytes, 0)

	enableFollowerRead := arw.streamer.client.dataWrapper.FollowerRead() && !arw.streamer.client.dataWrapper.InnerReq()
	retryRead := true
	if proto.IsCold(arw.streamer.client.volumeType) || proto.IsStorageClassBlobStore(task.storageClass) {
		retryRead = false
	}
	sc := NewStreamConn(task.dp, enableFollowerRead, arw.streamer.client.streamRetryTimeout)
	err = sc.Send(&retryRead, task.p, func(conn *net.TCPConn) (error, bool) {
		// reset readBytes when reading from other datanodes
		readBytes = 0
		atomic.StoreUint64(&cacheBlock.readBytes, 0)
		for readBytes < task.cacheSize {
			rp := NewReply(task.p.ReqID, task.p.PartitionID, task.p.ExtentID)
			bufSize := util.Min(util.CacheReadBlockSize, task.cacheSize-readBytes)
			rp.Data = cacheBlock.data[readBytes : readBytes+bufSize]
			if e := rp.readFromConn(conn, proto.ReadDeadlineTime); e != nil {
				return e, false
			}
			if rp.ResultCode != proto.OpOk {
				return fmt.Errorf("resultCode(%x) not ok", rp.ResultCode), false
			}
			// update timeStamp to prevent from deleted by timeout
			cacheBlock.time = time.Now().Unix()
			readBytes += int(rp.Size)
			atomic.AddUint64(&cacheBlock.readBytes, uint64(rp.Size))
			curSize := atomic.LoadUint64(&cacheBlock.readBytes)
			log.LogDebugf("aheadRead doTask  key(%v) curSize(%v) readBytes(%v) rp.Size(%v) task.cacheSize(%v) addr(%p)",
				key, curSize, readBytes, rp.Size, task.cacheSize, cacheBlock)
			if curSize > util.CacheReadBlockSize {
				log.LogErrorf("aheadRead doTask out of range key(%v) curSize(%v) readBytes(%v) rp.Size(%v) task.cacheSize(%v) addr(%p)",
					key, curSize, readBytes, rp.Size, task.cacheSize, cacheBlock)
			}
		}
		realHost = conn.RemoteAddr().String()
		return nil, false
	})

	if err == nil {
		atomic.StoreUint32(&cacheBlock.state, AheadReadBlockStateInit)
		arw.canAheadRead = true
		log.LogDebugf("aheadRead ready: key(%v) offset(%v) size(%v) err(%v) %v reqID(%v)",
			task.req, task.p.ExtentOffset, task.cacheSize, err, time.Since(task.time), task.reqID)
		return
	}
	atomic.StoreUint32(&cacheBlock.state, AheadReadBlockStateInit)
	log.LogErrorf("aheadRead doTask inode(%v) offset(%v) size(%v) err(%v) - read failed,"+
		" marking as recycled and deleting cache block",
		cacheBlock.inode, task.p.ExtentOffset, task.cacheSize, err)
	arw.cache.blockCache.Delete(key)
	arw.cache.putAheadReadBlock(key, cacheBlock)
}

func (arw *AheadReadWindow) putTaskIfNotExist(key string) (exist bool) {
	arw.curTaskMutex.Lock()
	defer arw.curTaskMutex.Unlock()
	if _, exist = arw.curTaskMap[key]; exist {
		return
	}
	arw.curTaskMap[key] = struct{}{}
	return
}

func (arw *AheadReadWindow) deleteTask(key string) {
	arw.curTaskMutex.Lock()
	defer arw.curTaskMutex.Unlock()
	delete(arw.curTaskMap, key)
}

func (arw *AheadReadWindow) addNextTask(offset int, dp *wrapper.DataPartition, req *ExtentRequest, stTime time.Time,
	reqID string, storageClass uint32) {
	id := offset/util.CacheReadBlockSize + arw.cache.winCnt
	remainSize := int(req.ExtentKey.Size) - id*util.CacheReadBlockSize
	if remainSize <= 0 {
		arw.doMultiAheadRead(0, 0, req, dp, stTime, reqID, storageClass)
		return
	}
	// log.LogDebugf("addNextTask ek(%v) remainSize(%v) %v-%v-%v-%v", req.ExtentKey, arw.streamer.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, id*util.CacheReadBlockSize)
	task := arw.getAheadReadTask(dp, req, id, util.Min(remainSize, util.CacheReadBlockSize), storageClass)
	if task == nil {
		return
	}
	task.time = stTime
	task.cacheType = "add"
	task.logTime = stat.BeginStat()
	task.reqID = reqID
	arw.taskC <- task
}

func (arw *AheadReadWindow) doMultiAheadRead(offset, remainSize int, req *ExtentRequest, dp *wrapper.DataPartition,
	startTime time.Time, reqID string, storageClass uint32) {
	if len(arw.curTaskMap) > 0 && !arw.canAheadRead {
		return
	}
	winCnt := arw.cache.winCnt
	curReq := &ExtentRequest{
		FileOffset: req.FileOffset,
		Size:       req.Size,
		ExtentKey:  req.ExtentKey,
	}
	id := 0
	for i := offset / util.CacheReadBlockSize; i < winCnt; i++ {
		if remainSize <= 0 {
			id = 0
			var err error
			curReq.ExtentKey = arw.streamer.getNextExtent(int(curReq.ExtentKey.FileOffset))
			if curReq.ExtentKey == nil {
				return
			}
			if dp, err = arw.streamer.client.dataWrapper.GetDataPartition(curReq.ExtentKey.PartitionId); err != nil {
				continue
			}
			remainSize = int(curReq.ExtentKey.Size)
		}

		size := util.Min(remainSize, util.CacheReadBlockSize)
		task := arw.getAheadReadTask(dp, curReq, id, size, storageClass)
		if task != nil {
			task.time = startTime
			task.cacheType = "pass"
			task.logTime = stat.BeginStat()
			task.reqID = reqID
			log.LogDebugf("aheadRead send: key(%v) offset(%v) size(%v) reqID(%v)",
				task.req, task.p.ExtentOffset, task.cacheSize, reqID)
			arw.taskC <- task
		}
		remainSize -= size
		id++
	}
}

func (arw *AheadReadWindow) getAheadReadTask(dp *wrapper.DataPartition, req *ExtentRequest, id int, size int, storageClass uint32) *AheadReadTask {
	cacheOffset := int(id) * util.CacheReadBlockSize
	p := NewReadPacket(req.ExtentKey, cacheOffset, size, arw.streamer.inode, req.FileOffset, true)
	task := &AheadReadTask{
		p:            p,
		dp:           dp,
		req:          req,
		cacheSize:    size,
		storageClass: storageClass,
	}
	return task
}

func (arw *AheadReadWindow) evictCacheBlock(req *ExtentRequest) {
	offset := req.FileOffset - int(req.ExtentKey.FileOffset) + int(req.ExtentKey.ExtentOffset)
	cacheOffset := offset / util.CacheReadBlockSize * util.CacheReadBlockSize
	key := fmt.Sprintf("%v-%v-%v-%v", arw.streamer.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, cacheOffset)
	value, ok := arw.cache.blockCache.Load(key)
	if ok {
		bv := value.(*AheadReadBlock)
		bv.lock.Lock()
		bv.key = ""
		if atomic.LoadUint32(&bv.state) == AheadReadBlockStateInit {
			arw.cache.blockCache.Delete(key)
			arw.cache.putAheadReadBlock(key, bv)
		}
		bv.lock.Unlock()
	}
}

func (s *Streamer) getNextExtent(offset int) (ek *proto.ExtentKey) {
	s.extents.RLock()
	defer s.extents.RUnlock()
	s.extents.root.Ascend(func(i btree.Item) bool {
		e := i.(*proto.ExtentKey)
		if e.Size < util.CacheReadBlockSize {
			return true
		}
		if e.FileOffset > uint64(offset) {
			ek = e
			return false
		}
		return true
	})
	return
}

func (s *Streamer) aheadRead(req *ExtentRequest, storageClass uint32) (readSize int, err error) {
	var (
		offset      int
		cacheOffset int
		cacheBlock  *AheadReadBlock
		step        string
		dp          *wrapper.DataPartition
		remainSize  int
		key         string
	)
	startTime := time.Now()
	defer func() {
		log.LogDebugf("aheadRead step: %v, inode(%v) key(%v) FileOffset(%v) reqSize(%v) readSize(%v) err(%v) %v", step, s.inode, key, req.FileOffset, req.Size, readSize, err, time.Since(startTime))
	}()

	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		return
	}

	offset = req.FileOffset - int(req.ExtentKey.FileOffset) + int(req.ExtentKey.ExtentOffset)
	cacheOffset = offset / util.CacheReadBlockSize * util.CacheReadBlockSize
	remainSize = int(req.ExtentKey.Size) - cacheOffset
	if remainSize < 0 {
		remainSize = 0
	}
	needSize := req.Size - readSize
	for needSize > 0 {
		key = fmt.Sprintf("%v-%v-%v-%v", s.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, cacheOffset)
		val, ok := s.aheadReadWindow.cache.blockCache.Load(key)
		if !ok {
			break
		}
		cacheBlock = val.(*AheadReadBlock)
		cacheBlock.lock.RLock()
		if cacheBlock.key != key {
			log.LogDebugf("aheadRead cache block key(%v) expected(%v) is changed, maybe recycled", cacheBlock.key, key)
			cacheBlock.lock.RUnlock()
			break
		}
		curSize := atomic.LoadUint64(&cacheBlock.readBytes)
		if offset-int(cacheBlock.offset) > int(curSize) {
			cacheBlock.lock.RUnlock()
			return
		}
		step = "hit"
		cacheBlock.time = startTime.Unix()

		if log.EnableDebug() {
			log.LogDebugf("aheadRead cache hit inode(%v) FileOffset(%v) offset(%v) size(%v) cacheBlockOffset(%v) cacheBlockSize(%v) %v",
				s.inode, req.FileOffset, offset, needSize, cacheBlock.offset, curSize, time.Since(startTime))
		}
		if cacheBlock.offset <= uint64(offset) {
			if (cacheBlock.offset + curSize) > uint64(offset+needSize) {
				copy(req.Data[readSize:req.Size], cacheBlock.data[offset-int(cacheBlock.offset):offset-int(cacheBlock.offset)+needSize])
				cacheBlock.lock.RUnlock()
				readSize += needSize
				return
			} else {
				reqID := uuid.New().String()
				log.LogDebugf("aheadRead move ahead win inode(%v) FileOffset(%v) offset(%v) "+
					"size(%v) cacheBlockOffset(%v) cacheBlockSize(%v) reqID(%v)", s.inode, req.FileOffset,
					offset, needSize, cacheBlock.offset, curSize, reqID)
				go s.aheadReadWindow.addNextTask(offset, dp, req, startTime, reqID, storageClass)
				copy(req.Data, cacheBlock.data[offset-int(cacheBlock.offset):int(curSize)])
				cacheBlock.lock.RUnlock()
				readSize += int(curSize) + int(cacheBlock.offset) - offset
				offset += readSize
				cacheOffset = offset
				needSize -= readSize
				if needSize <= 0 {
					return
				}
				continue
			}
		}
		cacheBlock.lock.RUnlock()
		break
	}
	step = "pass"
	go s.aheadReadWindow.doMultiAheadRead(offset, remainSize, req, dp, startTime, uuid.New().String(), storageClass)
	return
}
