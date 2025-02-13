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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
)

type AheadReadCache struct {
	enable            bool
	blockTimeOut      int
	winCnt            int
	availableBlockC   chan *AheadReadBlock
	availableBlockCnt int64
	totalBlockCnt     int64
	stopC             chan interface{}
	blockCache        *sync.Map
}

type AheadReadBlock struct {
	inode       uint64
	partitionId uint64
	extentId    uint64
	offset      uint64
	size        uint64
	data        []byte
	time        int64
}

type AheadReadTask struct {
	p         *Packet
	dnHosts   []string
	time      time.Time
	req       *ExtentRequest
	cacheSize int
}

type AheadReadWindow struct {
	taskC        chan *AheadReadTask
	curTaskMap   map[string]interface{}
	curTaskMutex sync.RWMutex
	cache        *AheadReadCache
	streamer     *Streamer
	canAheadRead bool
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
		blockCache:    new(sync.Map),
		totalBlockCnt: totalMem / util.CacheReadBlockSize,
	}
	atomic.StoreInt64(&arc.availableBlockCnt, arc.totalBlockCnt)
	arc.availableBlockC = make(chan *AheadReadBlock, arc.availableBlockCnt)
	for i := int64(0); i < arc.totalBlockCnt; i++ {
		block := &AheadReadBlock{
			data: make([]byte, util.CacheReadBlockSize),
		}
		arc.availableBlockC <- block
	}
	log.LogInfof("aheadRead enable(%v) totalMem(%v) availableBlockCnt(%v) winCnt(%v)", enable, totalMem, arc.availableBlockCnt, winCnt)
	go arc.checkBlockTimeOut()
	return arc
}

func (arc *AheadReadCache) getAheadReadBlock() (block *AheadReadBlock, err error) {
	select {
	case block = <-arc.availableBlockC:
		atomic.AddInt64(&arc.availableBlockCnt, -1)
	default:
		err = fmt.Errorf("availableBlockC is nil")
	}
	return
}

func (arc *AheadReadCache) putAheadReadBlock(block *AheadReadBlock) {
	arc.availableBlockC <- block
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
		if curTime-bv.time > int64(arc.blockTimeOut) {
			arc.blockCache.Delete(key)
			arc.putAheadReadBlock(bv)
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
	for task := range arw.taskC {
		go arw.doTask(task)
	}
}

func (arw *AheadReadWindow) doTask(task *AheadReadTask) {
	var (
		err       error
		hosts     = task.dnHosts
		readBytes int
	)
	key := fmt.Sprintf("%v-%v-%v-%v", task.p.inode, task.p.PartitionID, task.p.ExtentID, task.p.ExtentOffset)
	if arw.putTaskIfNotExist(key) {
		return
	}
	defer func() {
		arw.deleteTask(key)
		log.LogDebugf("aheadRead step: fetch, key(%v) size(%v) err(%v) %v", key, task.cacheSize, err, time.Since(task.time))
	}()
	if _, ok := arw.cache.blockCache.Load(key); ok {
		return
	}
	cacheBlock, err := arw.cache.getAheadReadBlock()
	if err != nil {
		return
	}
	cacheBlock.inode = task.p.inode
	cacheBlock.partitionId = task.p.PartitionID
	cacheBlock.extentId = task.p.ExtentID
	cacheBlock.offset = uint64(task.p.ExtentOffset)
	cacheBlock.size = uint64(task.cacheSize)

	log.LogDebugf("aheadRead doTask key(%v) size(%v)", key, task.p.Size)

	for _, host := range hosts {
		err = sendToNode(host, task.p, func(conn *net.TCPConn) (error, bool) {
			readBytes = 0
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
				readBytes += int(rp.Size)
			}
			return nil, false
		})

		if err != nil && strings.Contains(err.Error(), "timeout") {
			arw.canAheadRead = false
			arw.streamer.aheadReadEnable = false
			break
		}
		if err == nil {
			cacheBlock.time = time.Now().Unix()
			arw.cache.blockCache.Store(key, cacheBlock)
			arw.canAheadRead = true
			return
		}
	}

	arw.cache.putAheadReadBlock(cacheBlock)
	log.LogErrorf("aheadRead doTask inode(%v) offset(%v) size(%v) err(%v)", cacheBlock.inode, task.p.ExtentOffset, task.cacheSize, err)
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

func (arw *AheadReadWindow) addNextTask(offset int, dnHosts []string, req *ExtentRequest, stTime time.Time) {
	id := offset/util.CacheReadBlockSize + arw.cache.winCnt
	remainSize := int(req.ExtentKey.Size) - id*util.CacheReadBlockSize
	if remainSize <= 0 {
		arw.doMultiAheadRead(0, 0, req, dnHosts, stTime)
		return
	}
	// log.LogDebugf("addNextTask ek(%v) remainSize(%v) %v-%v-%v-%v", req.ExtentKey, arw.streamer.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, id*util.CacheReadBlockSize)
	task := arw.getAheadReadTask(dnHosts, req, id, util.Min(remainSize, util.CacheReadBlockSize))
	if task == nil {
		return
	}
	task.time = stTime
	arw.taskC <- task
}

func (arw *AheadReadWindow) doMultiAheadRead(offset, remainSize int, req *ExtentRequest, dnHosts []string, startTime time.Time) {
	if len(arw.curTaskMap) > 0 && !arw.canAheadRead {
		return
	}
	winCnt := 1
	if arw.canAheadRead {
		winCnt = arw.cache.winCnt
	}
	curReq := &ExtentRequest{
		FileOffset: req.FileOffset,
		Size:       req.Size,
		ExtentKey:  req.ExtentKey,
	}
	id := 0
	for i := offset / util.CacheReadBlockSize; i < winCnt; i++ {
		if remainSize <= 0 {
			id = 0
			var (
				dp  *wrapper.DataPartition
				err error
			)
			curReq.ExtentKey = arw.streamer.getNextExtent(int(curReq.ExtentKey.FileOffset))
			if curReq.ExtentKey == nil {
				return
			}
			if dp, err = arw.streamer.client.dataWrapper.GetDataPartition(curReq.ExtentKey.PartitionId); err != nil {
				continue
			}
			dnHosts = dp.Hosts
			remainSize = int(curReq.ExtentKey.Size)
		}

		size := util.Min(remainSize, util.CacheReadBlockSize)
		task := arw.getAheadReadTask(dnHosts, curReq, id, size)
		if task != nil {
			task.time = startTime
			arw.taskC <- task
		}
		remainSize -= size
		id++
	}
}

func (arw *AheadReadWindow) getAheadReadTask(dnHosts []string, req *ExtentRequest, id int, size int) *AheadReadTask {
	cacheOffset := int(id) * util.CacheReadBlockSize
	p := NewReadPacket(req.ExtentKey, cacheOffset, size, arw.streamer.inode, req.FileOffset, true)
	task := &AheadReadTask{
		p:         p,
		dnHosts:   getRandomHostById(dnHosts, id),
		req:       req,
		cacheSize: size,
	}
	return task
}

func (arw *AheadReadWindow) evictCacheBlock(req *ExtentRequest) {
	offset := req.FileOffset - int(req.ExtentKey.FileOffset) + int(req.ExtentKey.ExtentOffset)
	cacheOffset := offset / util.CacheReadBlockSize * util.CacheReadBlockSize
	key := fmt.Sprintf("%v-%v-%v-%v", arw.streamer.inode, req.ExtentKey.PartitionId, req.ExtentKey.ExtentId, cacheOffset)
	value, ok := arw.cache.blockCache.LoadAndDelete(key)
	if ok {
		bv := value.(*AheadReadBlock)
		arw.cache.putAheadReadBlock(bv)
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

func (s *Streamer) aheadRead(req *ExtentRequest) (readSize int, err error) {
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
		step = "hit"
		cacheBlock = val.(*AheadReadBlock)
		cacheBlock.time = startTime.Unix()
		log.LogDebugf("aheadRead cache hit inode(%v) FileOffset(%v) offset(%v) size(%v) cacheBlockOffset(%v) cacheBlockSize(%v) %v", s.inode, req.FileOffset, offset, needSize, cacheBlock.offset, cacheBlock.size, time.Since(startTime))
		if cacheBlock.offset <= uint64(offset) {
			if (cacheBlock.offset + cacheBlock.size) > uint64(offset+needSize) {
				copy(req.Data[readSize:req.Size], cacheBlock.data[offset-int(cacheBlock.offset):offset-int(cacheBlock.offset)+needSize])
				readSize += needSize
				return
			} else {
				go s.aheadReadWindow.addNextTask(offset, dp.Hosts, req, startTime)
				if offset-int(cacheBlock.offset) > int(cacheBlock.size) {
					return
				}
				copy(req.Data, cacheBlock.data[offset-int(cacheBlock.offset):int(cacheBlock.size)])
				readSize += int(cacheBlock.size) + int(cacheBlock.offset) - offset
				offset += readSize
				cacheOffset = offset
				needSize -= readSize
				if needSize <= 0 {
					return
				}
				continue
			}
		}
		break
	}
	step = "pass"
	go s.aheadReadWindow.doMultiAheadRead(offset, remainSize, req, dp.Hosts, startTime)
	return
}

func sendToNode(host string, p *Packet, getReply GetReplyFunc) (err error) {
	var conn *net.TCPConn
	if conn, err = StreamConnPool.GetConnect(host); err != nil {
		return
	}
	defer StreamConnPool.PutConnect(conn, err != nil)
	if err = p.WriteToConn(conn); err != nil {
		return
	}
	err, _ = getReply(conn)
	return
}

func getRandomHostById(hosts []string, id int) []string {
	if len(hosts) == 0 {
		return nil
	}
	hIdx := id % len(hosts)
	rhosts := make([]string, len(hosts))
	for i := 0; i < len(hosts); i++ {
		if hIdx >= len(hosts) {
			hIdx = 0
		}
		rhosts[i] = hosts[hIdx]
		hIdx++
	}
	return rhosts
}
