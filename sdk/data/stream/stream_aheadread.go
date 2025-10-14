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
	"hash/crc32"
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

	AheadReadedInit    uint32 = 0
	AheadReaded        uint32 = 1
	MaxCacheBlockRetry uint32 = 32
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
	readed      uint32
}

var aheadReadBlockPool = sync.Pool{
	New: func() interface{} {
		return &AheadReadBlock{
			data: make([]byte, util.CacheReadBlockSize),
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
	block.data = block.data[:cap(block.data)]
	block.time = 0
	block.readBytes = 0
	block.readed = 0
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
	retry        uint32
}

type AheadReadWindow struct {
	taskC       chan *AheadReadTask
	cache       *AheadReadCache
	streamer    *Streamer
	rightOffset uint64
	cnt         int32
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
	blockTimer := time.NewTicker(time.Second)
	printTicker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-arc.stopC:
			blockTimer.Stop()
			printTicker.Stop()
			return
		case <-blockTimer.C:
			arc.doCheckBlockTimeOut()
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
			if atomic.LoadUint32(&bv.readed) == AheadReadedInit {
				log.LogWarnf("doCheckBlockTimeOut delete unreaded block: key(%v) addr(%p)", key, bv)
			}
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

// maintain a read-ahead window for each file
func NewAheadReadWindow(arc *AheadReadCache, s *Streamer) *AheadReadWindow {
	arw := &AheadReadWindow{
		taskC:       make(chan *AheadReadTask, arc.winCnt),
		cache:       arc,
		streamer:    s,
		rightOffset: 0,
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
	if _, ok := arw.cache.blockCache.Load(key); ok {
		return
	}
	_, loaded := arw.cache.creatingBlockCacheMap.LoadOrStore(key, make(chan struct{}))
	if loaded {
		return
	} else {
		atomic.AddInt32(&arw.cnt, 1)
		defer func() {
			arw.cache.creatingBlockCacheMap.Delete(key)
			cnt := atomic.AddInt32(&arw.cnt, -1)
			if int(cnt) > arw.cache.winCnt {
				log.LogWarnf("doTask cnt %v is more than %v", cnt, arw.cache.winCnt)
			}
		}()
	}
	cacheBlock, err := arw.cache.getAheadReadBlock()
	if err != nil {
		log.LogWarnf("aheadRead done:key(%v) no more block", key)
		return
	}
	defer func() {
		if log.EnableDebug() {
			log.LogDebugf("doTask done:key(%v) size(%v) err(%v) %v reqID(%v)",
				key, task.cacheSize, err, time.Since(task.time), task.reqID)
		}
		stat.EndStat("PrepareAheadData", err, task.logTime, 1)
		stat.EndStat(fmt.Sprintf("PrepareAheadData[%v]", realHost), err, task.logTime, 1)
	}()
	log.LogDebugf("doTask key(%v) reqID(%v) start", key, task.reqID)
	cacheBlock.inode = task.p.inode
	cacheBlock.partitionId = task.p.PartitionID
	cacheBlock.extentId = task.p.ExtentID
	cacheBlock.offset = uint64(task.p.ExtentOffset)
	cacheBlock.size = uint64(task.cacheSize)
	cacheBlock.key = key
	atomic.StoreUint32(&cacheBlock.readed, AheadReadedInit)
	// block cannot be removed by checking timeOut
	atomic.StoreUint32(&cacheBlock.state, AheadReadBlockStateLoading)
	arw.cache.blockCache.Store(key, cacheBlock)
	cacheBlock.time = time.Now().Unix()
	atomic.StoreUint64(&cacheBlock.readBytes, 0)
	// randomly shuffle the order of hosts to evenly distribute access pressure.
	hosts := getRotatedHosts(task.dp.Hosts)
	for _, host := range hosts {
		err = sendToNode(host, task.p, func(conn *net.TCPConn) (error, bool) {
			// reset readBytes when reading from other datanodes
			readBytes = 0
			atomic.StoreUint64(&cacheBlock.readBytes, 0)
			for readBytes < task.cacheSize {
				rp := NewReply(task.p.ReqID, task.p.PartitionID, task.p.ExtentID)
				bufSize := util.Min(int(arw.streamer.aheadReadBlockSize), task.cacheSize-readBytes)
				rp.Data = cacheBlock.data[readBytes : readBytes+bufSize]
				begin := time.Now()
				if e := rp.readFromConn(conn, proto.ReadDeadlineTime); e != nil {
					return e, false
				}
				if rp.ResultCode != proto.OpOk {
					err = fmt.Errorf("result code[%v] dp[%v] host[%v],msg[%v]", rp.ResultCode, task.dp, host, string(rp.Data[:rp.Size]))
					return err, false
				}
				if !task.p.isValidReadReply(rp) {
					err = fmt.Errorf("inconsistent req and reply, req(%v) reply(%v)", task.p, rp)
					return err, false
				}
				expectCrc := crc32.ChecksumIEEE(rp.Data[:rp.Size])
				if expectCrc != rp.CRC {
					err = fmt.Errorf("inconsistent CRC, expectCRC(%v) replyCRC(%v)", expectCrc, rp.CRC)
					return err, false
				}
				// update timeStamp to prevent from deleted by timeout
				cacheBlock.time = time.Now().Unix()
				readBytes += int(rp.Size)
				curSize := atomic.AddUint64(&cacheBlock.readBytes, uint64(rp.Size))
				arw.updateRightIndex(uint64(task.req.FileOffset)+curSize, key, task.reqID)
				log.LogDebugf("doTask  key(%v) curSize(%v) readBytes(%v) rp.Size(%v) task.cacheSize(%v) addr(%p) cost(%v)",
					key, curSize, readBytes, rp.Size, task.cacheSize, cacheBlock, time.Since(begin).String())
				if curSize > uint64(arw.streamer.aheadReadBlockSize) {
					log.LogErrorf("doTask out of range key(%v) curSize(%v) readBytes(%v) rp.Size(%v) task.cacheSize(%v) addr(%p)",
						key, curSize, readBytes, rp.Size, task.cacheSize, cacheBlock)
				}
			}
			realHost = conn.RemoteAddr().String()
			return nil, false
		})
		if err == nil {
			atomic.StoreUint32(&cacheBlock.state, AheadReadBlockStateInit)
			log.LogDebugf("doTask ready: key(%v) offset(%v) size(%v) err(%v) %v reqID(%v)",
				key, task.p.ExtentOffset, task.cacheSize, err, time.Since(task.time).String(), task.reqID)
			return
		} else {
			// try next host
			log.LogWarnf("doTask read from host(%v) hosts(%v) key(%v) packet(%v)  reqID(%v) failed:%v", host, hosts, key, task.p, task.reqID, err)
		}
	}
	atomic.StoreUint32(&cacheBlock.state, AheadReadBlockStateInit)
	log.LogErrorf("doTask inode(%v) key(%v) offset(%v) size(%v) err(%v) reqID(%v)- read failed,"+
		" marking as recycled and deleting cache block",
		cacheBlock.inode, key, task.p.ExtentOffset, task.cacheSize, err, task.reqID)
	arw.cache.blockCache.Delete(key)
	arw.cache.putAheadReadBlock(key, cacheBlock)
	// retry failed task for updateRightIndex cannot be reverted
	if task.retry <= MaxCacheBlockRetry {
		task.retry++
		arw.taskC <- task
	}
}

func (arw *AheadReadWindow) addNextTask(offset int, stTime time.Time, reqID string, storageClass uint32, key string) {
	rightOffset := atomic.LoadUint64(&arw.rightOffset)
	ek := arw.streamer.getCurrentExtent(int(rightOffset))
	if ek == nil {
		log.LogWarnf("addNextTask current ExtentKey for offset(%v) is nil reqID(%v)", rightOffset, reqID)
		return
	}
	// move forward
	curReq := &ExtentRequest{
		FileOffset: int(rightOffset),
		ExtentKey:  ek,
	}
	var (
		dp  *wrapper.DataPartition
		err error
	)
	if dp, err = arw.streamer.client.dataWrapper.GetDataPartition(curReq.ExtentKey.PartitionId); err != nil {
		log.LogWarnf("addNextTask get dp for ExtentKey for offset(%v) reqID(%v), failed %v", rightOffset, reqID, err)
		return
	}
	newOffset := rightOffset - ek.FileOffset + ek.ExtentOffset
	newCacheOffset := rightOffset / uint64(arw.streamer.aheadReadBlockSize) * uint64(arw.streamer.aheadReadBlockSize)
	oldCacheOffset := uint64(offset) / uint64(arw.streamer.aheadReadBlockSize) * uint64(arw.streamer.aheadReadBlockSize)
	diffWinCnt := int(newCacheOffset-oldCacheOffset) / int(arw.streamer.aheadReadBlockSize)
	if diffWinCnt >= arw.cache.winCnt {
		log.LogDebugf("addNextTask inode(%v) key(%v) offset(%v)  right(%v) reqID(%v) ek(%v) winCnt(%v) is engouh",
			arw.streamer.inode, key, offset, rightOffset, reqID, ek, diffWinCnt)
		return
	} else {
		diffWinCnt = arw.cache.winCnt - diffWinCnt
	}
	log.LogDebugf("addNextTask inode(%v) key(%v) offset(%v) move from(%v) reqID(%v) ek(%v) winCnt(%v)",
		arw.streamer.inode, key, offset, rightOffset, reqID, ek, diffWinCnt)
	arw.doMultiAheadRead(int(newOffset), curReq, dp, stTime, reqID, storageClass, diffWinCnt)
}

func (arw *AheadReadWindow) doMultiAheadRead(offset int, req *ExtentRequest, dp *wrapper.DataPartition,
	startTime time.Time, reqID string, storageClass uint32, winCnt int) {
	curReq := &ExtentRequest{
		FileOffset: req.FileOffset,
		Size:       req.Size,
		ExtentKey:  req.ExtentKey,
	}
	// read the entire 1MB data form tiny extent
	if curReq.ExtentKey.ExtentId <= 64 {
		offset = 0
	}
	cacheOffset := offset / int(arw.streamer.aheadReadBlockSize) * int(arw.streamer.aheadReadBlockSize)
	key := fmt.Sprintf("%v-%v-%v-%v", arw.streamer.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, cacheOffset)
	log.LogDebugf("doMultiAheadRead send: key(%v) reqID(%v) index(%v) req（%v）",
		key, reqID, offset/int(arw.streamer.aheadReadBlockSize), req)
	id := offset / int(arw.streamer.aheadReadBlockSize)
	remainSize := 0
	for w := 0; w < winCnt; w++ {
		remainSize = int(curReq.ExtentKey.Size) - id*int(arw.streamer.aheadReadBlockSize)
		if remainSize <= 0 {
			// start from 0 if read new extent key
			id = 0
			var err error
			lastEk := curReq.ExtentKey
			curReq.ExtentKey = arw.streamer.getNextExtent(int(lastEk.FileOffset))
			if curReq.ExtentKey == nil {
				log.LogDebugf("doMultiAheadRead send: key(%v) next ExtentKey is nil", lastEk)
				return
			}
			curReq.FileOffset = int(curReq.ExtentKey.FileOffset)
			if dp, err = arw.streamer.client.dataWrapper.GetDataPartition(curReq.ExtentKey.PartitionId); err != nil {
				log.LogWarnf("doMultiAheadRead send: dp for ek(%v) is nil,err(%v)", curReq.ExtentKey, err)
				continue
			}
			remainSize = int(curReq.ExtentKey.Size)
		}

		size := util.Min(remainSize, int(arw.streamer.aheadReadBlockSize))
		task := arw.getAheadReadTask(dp, curReq, id, size, storageClass)
		if task != nil {
			task.time = startTime
			task.cacheType = "pass"
			task.logTime = stat.BeginStat()
			task.reqID = reqID
			key := fmt.Sprintf("%v-%v-%v-%v", arw.streamer.inode, task.p.PartitionID, task.p.ExtentID, task.p.ExtentOffset)
			log.LogDebugf("doMultiAheadRead send: key(%v) curReq(%v) size(%v) reqID(%v)",
				key, curReq, size, reqID)
			arw.taskC <- task
		}
		id++
	}
}

func (arw *AheadReadWindow) getAheadReadTask(dp *wrapper.DataPartition, req *ExtentRequest, id int, size int,
	storageClass uint32) *AheadReadTask {
	cacheOffset := id * int(arw.streamer.aheadReadBlockSize)
	// tiny need to add ExtentOffset
	if req.ExtentKey.ExtentId <= 64 {
		cacheOffset = int(req.ExtentKey.ExtentOffset)
	}
	p := NewReadPacket(req.ExtentKey, cacheOffset, size, arw.streamer.inode, req.FileOffset, true)
	task := &AheadReadTask{
		p:            p,
		dp:           dp,
		req:          req,
		cacheSize:    size,
		storageClass: storageClass,
		retry:        0,
	}
	return task
}

func (arw *AheadReadWindow) evictCacheBlock(req *ExtentRequest) {
	offset := req.FileOffset - int(req.ExtentKey.FileOffset) + int(req.ExtentKey.ExtentOffset)
	cacheOffset := offset / int(arw.streamer.aheadReadBlockSize) * int(arw.streamer.aheadReadBlockSize)
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
		if e.Size < s.aheadReadBlockSize {
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
		key         string
	)
	startTime := time.Now()
	defer func() {
		log.LogDebugf("aheadRead step: %v, inode(%v) key(%v) FileOffset(%v) reqSize(%v) readSize(%v) err(%v) %v aheadReadBlockSize(%v) streamer(%p)"+
			"", step, s.inode, key, req.FileOffset, req.Size, readSize, err, time.Since(startTime), s.aheadReadBlockSize, s)
	}()

	if dp, err = s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		step = "dp not found"
		return
	}

	offset = req.FileOffset - int(req.ExtentKey.FileOffset) + int(req.ExtentKey.ExtentOffset)
	cacheOffset = offset / int(s.aheadReadBlockSize) * int(s.aheadReadBlockSize)
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
			log.LogDebugf("aheadRead cache block key(%v) need(%v)curSize(%v) is not enough", cacheBlock.key, offset-int(cacheBlock.offset), curSize)
			step = "partly"
			return
		}
		step = "hit"
		cacheBlock.time = startTime.Unix()

		if log.EnableDebug() {
			log.LogDebugf("aheadRead cache hit inode(%v) FileOffset(%v) offset(%v) size(%v) cacheBlockOffset(%v) cacheBlockSize(%v) %v",
				s.inode, req.FileOffset, offset, needSize, cacheBlock.offset, curSize, time.Since(startTime))
		}
		if cacheBlock.offset <= uint64(offset) {
			atomic.CompareAndSwapUint32(&cacheBlock.readed, AheadReadedInit, AheadReaded)
			if cacheBlock.offset == uint64(offset) {
				reqID := uuid.New().String()
				log.LogDebugf("aheadRead move ahead win inode(%v) FileOffset(%v) offset(%v) need(%v) "+
					"cacheBlockOffset(%v) cacheBlockSize(%v) reqID(%v)",
					s.inode, req.FileOffset, offset, needSize, cacheBlock.offset, curSize, reqID)
				go s.aheadReadWindow.addNextTask(req.FileOffset, startTime, reqID, storageClass, key)
			}
			if (cacheBlock.offset + curSize) > uint64(offset+needSize) {
				// all require data is cached, copy completely return directly
				copy(req.Data[readSize:readSize+needSize], cacheBlock.data[offset-int(cacheBlock.offset):offset-int(cacheBlock.offset)+needSize])
				cacheBlock.lock.RUnlock()
				readSize += needSize
				return
			} else {
				end := int(cacheBlock.offset) + int(curSize)
				bytesToEnd := end - offset
				if bytesToEnd == 0 {
					cacheBlock.lock.RUnlock()
					step = "miss"
					err = fmt.Errorf("no cache data avaliable")
					return
				}
				if bytesToEnd > 0 {
					copy(req.Data[readSize:readSize+bytesToEnd],
						cacheBlock.data[offset-int(cacheBlock.offset):offset-int(cacheBlock.offset)+bytesToEnd])
					readSize += bytesToEnd
					offset += bytesToEnd
					cacheOffset = offset
					needSize -= bytesToEnd
				}
				cacheBlock.lock.RUnlock()
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
	reqID := uuid.New().String()
	log.LogDebugf("aheadRead pass ahead win inode(%v) offset(%v) need(%v) "+
		"req(%v) reqID(%v)", s.inode, offset, needSize, req, reqID)
	go s.aheadReadWindow.doMultiAheadRead(offset, req, dp, startTime, reqID, storageClass,
		s.aheadReadWindow.cache.winCnt)
	return
}

func sendToNode(host string, p *Packet, getReply GetReplyFunc) (err error) {
	var conn *net.TCPConn
	start := time.Now()
	if conn, err = StreamConnPool.GetConnect(host); err != nil {
		return
	}
	defer func() {
		StreamConnPool.PutConnect(conn, err != nil)
		log.LogDebugf("sendToNode connect local(%v) remote(%v) cost(%v) err(%v)", conn.LocalAddr().String(),
			conn.RemoteAddr().String(), time.Since(start).String(), err)
	}()
	if err = p.WriteToConn(conn); err != nil {
		return
	}
	err, _ = getReply(conn)
	return
}

var rrIdx uint64

func getRotatedHosts(hosts []string) []string {
	if len(hosts) == 0 {
		return nil
	}
	idx := int(atomic.AddUint64(&rrIdx, 1)-1) % len(hosts)

	rhosts := make([]string, 0, len(hosts))
	rhosts = append(rhosts, hosts[idx:]...)
	if idx > 0 {
		rhosts = append(rhosts, hosts[:idx]...)
	}
	return rhosts
}

func (arw *AheadReadWindow) updateRightIndex(index uint64, key string, reqID string) {
	for {
		current := atomic.LoadUint64(&arw.rightOffset)
		if index <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&arw.rightOffset, current, index) {
			log.LogDebugf("updateRightIndex inode(%v) key(%v) reqID(%v) rightIndex to (%v)",
				arw.streamer.inode, key, reqID, index)
			break
		}
	}
}

func (s *Streamer) getCurrentExtent(offset int) (ek *proto.ExtentKey) {
	s.extents.RLock()
	defer s.extents.RUnlock()
	s.extents.root.Ascend(func(i btree.Item) bool {
		e := i.(*proto.ExtentKey)
		if e.Size < util.CacheReadBlockSize {
			return true
		}
		ekStart := int(e.FileOffset)
		ekEnd := int(e.FileOffset) + int(e.Size)
		if ekStart <= offset {
			if ekEnd >= offset {
				ek = e
				return false
			}
		}
		return true
	})
	return
}
