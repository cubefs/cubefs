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

package cache_engine

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/fastcrc32"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/tmpfs"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type cachePrepareTask struct {
	request *proto.CacheRequest
	reqID   int64
}

type CacheConfig struct {
	MaxAlloc int64 `json:"maxAlloc"`
	Total    int64 `json:"total"`
	Capacity int   `json:"capacity"`
}

type CacheEngine struct {
	dataPath           string
	cachePrepareTaskCh chan *cachePrepareTask
	lruCache           LruCache
	locks              []*sync.RWMutex
	readSourceFunc     ReadExtentData
	monitorFunc        MonitorFunc
	closeC             chan bool
	closed             bool
	config             *CacheConfig
	sync.Mutex
}
type ReadExtentData func(source *proto.DataSource, afterReadFunc func([]byte, int64) error) (readBytes int, err error)
type MonitorFunc func(volume string, action int, size uint64)

func NewCacheEngine(dataDir string, totalSize int64, maxUseRatio float64, capacity int, expireTime time.Duration, readFunc ReadExtentData, monitorFunc MonitorFunc) (s *CacheEngine, err error) {
	s = new(CacheEngine)
	s.dataPath = dataDir
	if err = os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("NewCacheEngine [%v] err[%v]", dataDir, err)
	}
	s.cachePrepareTaskCh = make(chan *cachePrepareTask, 1024)
	s.config = &CacheConfig{
		MaxAlloc: int64(float64(totalSize) * maxUseRatio),
		Total:    totalSize,
		Capacity: capacity,
	}
	s.lruCache = NewCache(s.config.Capacity, s.config.MaxAlloc, expireTime, func(v interface{}) error {
		cb := v.(*CacheBlock)
		return cb.Delete()
	}, func(v interface{}) error {
		cb := v.(*CacheBlock)
		cb.Close()
		return nil
	})
	s.initCacheLock()
	err = s.doMount()
	if err != nil {
		return
	}

	s.readSourceFunc = readFunc
	s.monitorFunc = monitorFunc
	s.closeC = make(chan bool, 1)
	s.closed = false
	return
}

func (c *CacheEngine) Start() {
	c.startCachePrepareWorkers()
	log.LogInfof("CacheEngine started.")
}

func (c *CacheEngine) Stop() (err error) {
	err = c.lruCache.Close()
	if err != nil {
		return err
	}
	close(c.closeC)
	c.closed = true
	time.Sleep(time.Second * 2)
	log.LogInfof("CacheEngine stopped, umount tmpfs: %v.", c.dataPath)
	err = tmpfs.Umount(c.dataPath)
	return err
}

func (c *CacheEngine) createInitFile() (err error) {
	var fd *os.File
	fd, err = os.OpenFile(c.dataPath+"/"+InitFileName, os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	fd.Close()
	return
}

func (c *CacheEngine) initFileExists() bool {
	_, err := os.Stat(c.dataPath + "/" + InitFileName)
	if err == nil {
		return true
	}
	return false
}

func (c *CacheEngine) scheduleCheckMount() {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			mounted, err := tmpfs.IsMountPoint(c.dataPath)
			if err != nil || !mounted {
				exporter.WarningCritical(fmt.Sprintf("Mounted[%v], mount point error:%v", mounted, err))
				continue
			}
			if mounted && !tmpfs.IsTmpfs(c.dataPath) {
				exporter.WarningCritical(fmt.Sprintf("Mounted[%v], mounted by other but not tmpfs!", mounted))
			}
		case <-c.closeC:
			return
		}
	}
}

func (c *CacheEngine) doMount() (err error) {
	var mounted bool
	var fds []os.DirEntry
	_, err = os.Stat(c.dataPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err != nil && os.IsNotExist(err) {
		return c.initTmpfs()
	}

	mounted, err = tmpfs.IsMountPoint(c.dataPath)
	if err != nil {
		return err
	}
	if mounted && !tmpfs.IsTmpfs(c.dataPath) {
		err = fmt.Errorf("already mounted by another device")
		return err
	}
	if mounted && c.initFileExists() {
		err = tmpfs.Umount(c.dataPath)
		if err != nil {
			return err
		}
		return c.initTmpfs()
	}
	fds, err = os.ReadDir(c.dataPath)
	if err != nil {
		return
	}
	if len(fds) > 0 {
		err = fmt.Errorf("not empty dir, mounted(%v) init file(%v)", mounted, c.initFileExists())
		return err
	}
	return c.initTmpfs()
}

func (c *CacheEngine) initTmpfs() (err error) {
	err = tmpfs.MountTmpfs(c.dataPath, c.config.Total)
	if err != nil {
		return err
	}
	err = c.createInitFile()
	return err
}

func (c *CacheEngine) deleteCacheBlock(key string) {
	c.lruCache.Evict(key)
}

func GenCacheBlockKey(volume string, inode, offset uint64, version uint32) string {
	return volume + "/" + strconv.FormatUint(inode, 10) + "#" + strconv.FormatUint(offset, 10) + "#" + strconv.FormatUint(uint64(version), 10)
}

func (c *CacheEngine) GetCacheBlock(volume string, inode, offset uint64, version uint32) (block *CacheBlock, err error) {
	key := GenCacheBlockKey(volume, inode, offset, version)
	lock := c.getCacheLock(key)
	lock.RLock()
	defer lock.RUnlock()
	if value, getErr := c.lruCache.Get(key); getErr == nil {
		block = value.(*CacheBlock)
		c.monitorFunc(volume, proto.ActionCacheHit, uint64(block.allocSize))
		return
	}
	c.monitorFunc(volume, proto.ActionCacheMiss, 0)
	return nil, errors.New("cache block get failed")
}

func (c *CacheEngine) PeekCacheBlock(key string) (block *CacheBlock, err error) {
	defer func() {
		if r := recover(); r != nil {
			warnMsg := fmt.Sprintf("PeekCacheBlock occurred panic:%v", r)
			log.LogErrorf(warnMsg)
			exporter.Warning(warnMsg)
		}
	}()
	lock := c.getCacheLock(key)
	lock.RLock()
	defer lock.RUnlock()
	if value, get := c.lruCache.Peek(key); get {
		block = value.(*CacheBlock)
		return
	}
	return nil, errors.New("cache block peek failed")
}

func (c *CacheEngine) createCacheBlock(volume string, inode, fixedOffset uint64, version uint32, ttl int64, allocSize uint64) (block *CacheBlock, err error) {
	if allocSize == 0 {
		return nil, fmt.Errorf("alloc size is zero")
	}
	var key = GenCacheBlockKey(volume, inode, fixedOffset, version)
	lock := c.getCacheLock(key)
	lock.Lock()
	if value, getErr := c.lruCache.Get(key); getErr == nil {
		lock.Unlock()
		block = value.(*CacheBlock)
		return
	}
	block = NewCacheBlock(c.dataPath, volume, inode, fixedOffset, version, allocSize, c.readSourceFunc)
	var n int
	if ttl <= 0 {
		ttl = proto.DefaultCacheTTLSec
	}
	n, err = c.lruCache.Set(key, block, time.Duration(ttl)*time.Second)
	lock.Unlock()
	if err != nil {
		return
	}
	err = block.initFilePath()
	if n > 0 {
		c.monitorFunc(volume, proto.ActionCacheEvict, uint64(n))
	}
	return
}

func (c *CacheEngine) usedSize() (size int64) {
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(c.dataPath, &stat)
	if err != nil {
		log.LogErrorf("compute used size of cache engine, err:%v", err)
		return
	}
	return int64(stat.Blocks) * int64(stat.Bsize)
}

func (c *CacheEngine) startCachePrepareWorkers() {
	for i := 0; i < 20; i++ {
		go func() {
			for {
				select {
				case <-c.closeC:
					log.LogWarnf("action[startCachePrepareWorkers] close worker on cache engine stopping")
					return
				case task := <-c.cachePrepareTaskCh:
					var block *CacheBlock
					var err error
					if block, err = c.PeekCacheBlock(GenCacheBlockKey(task.request.Volume, task.request.Inode, task.request.FixedFileOffset, task.request.Version)); err != nil {
						log.LogWarnf("action[startCachePrepareWorkers] ReqID(%d) cache block not found, err:%v", task.reqID, err)
						continue
					}
					block.InitOnce(c, task.request.Sources)
				}
			}
		}()
	}
}

func (c *CacheEngine) PrepareCache(reqID int64, req *proto.CacheRequest) (err error) {
	if _, err = c.CreateBlock(req); err != nil {
		return
	}
	c.SendToPrepareTaskCh(reqID, req)
	return
}

func (c *CacheEngine) SendToPrepareTaskCh(reqID int64, req *proto.CacheRequest) {
	t := &cachePrepareTask{
		reqID:   reqID,
		request: req,
	}
	select {
	case c.cachePrepareTaskCh <- t:
	default:
		log.LogWarnf("action[SendToPrepareTaskCh] chan has been full")
	}
}

func (c *CacheEngine) CreateBlock(req *proto.CacheRequest) (block *CacheBlock, err error) {
	if len(req.Sources) == 0 {
		return nil, fmt.Errorf("no source data")
	}
	if block, err = c.createCacheBlock(req.Volume, req.Inode, req.FixedFileOffset, req.Version, req.TTL, computeAllocSize(req)); err != nil {
		c.deleteCacheBlock(GenCacheBlockKey(req.Volume, req.Inode, req.FixedFileOffset, req.Version))
		return nil, err
	}
	return block, nil
}

func (c *CacheEngine) Status() *proto.CacheStatus {
	lruStat := c.lruCache.Status()
	stat := &proto.CacheStatus{
		MaxAlloc: c.config.MaxAlloc,
		HasAlloc: lruStat.Used,
		Total:    c.config.Total,
		Num:      lruStat.Num,
		HitRate:  math.Trunc(c.lruCache.HitRate()*1e4+0.5) * 1e-4,
		Evicts:   c.lruCache.RecentEvict(),
		Capacity: c.config.Capacity,
	}
	for _, k := range lruStat.Keys {
		stat.Keys = append(stat.Keys, k.(string))
	}
	stat.Used = c.usedSize()
	return stat
}

func (c *CacheEngine) EvictCacheByVolume(evictVol string) (failedKeys []interface{}) {
	stat := c.lruCache.Status()
	failedKeys = make([]interface{}, 0)
	for _, k := range stat.Keys {
		vol := strings.Split(k.(string), "#")[0]
		if evictVol == vol {
			c.getCacheLock(k.(string)).Lock()
			if !c.lruCache.Evict(k) {
				failedKeys = append(failedKeys, k)
			}
			c.getCacheLock(k.(string)).Unlock()
		}
	}
	log.LogWarnf("action[EvictCacheByVolume] evict volume(%v) finish", evictVol)
	return
}

func (c *CacheEngine) EvictCacheAll() {
	c.lockAll()
	defer c.unlockAll()
	c.lruCache.EvictAll()
	log.LogWarnf("action[EvictCacheAll] evict all finish")
}

func (c *CacheEngine) lockAll() {
	wg := sync.WaitGroup{}
	for _, l := range c.locks {
		wg.Add(1)
		go func(lock *sync.RWMutex) {
			lock.Lock()
		}(l)
	}
	wg.Wait()
}

func (c *CacheEngine) unlockAll() {
	wg := sync.WaitGroup{}
	for _, l := range c.locks {
		wg.Add(1)
		go func(lock *sync.RWMutex) {
			lock.Unlock()
		}(l)
	}
	wg.Wait()
}

func (c *CacheEngine) getCacheLock(key string) *sync.RWMutex {
	crc := fastcrc32.Checksum([]byte(key))
	return c.locks[crc&3]
}

func (c *CacheEngine) initCacheLock() {
	for i := 0; i < 4; i++ {
		c.locks = append(c.locks, new(sync.RWMutex))
	}
}
