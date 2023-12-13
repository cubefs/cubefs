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

package cachengine

import (
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/tmpfs"
)

//TODO: remove this later.
//go:generate gofumpt -l -w .
//go:generate git diff --exit-code
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose ./...

const (
	prepareWorkers = 20

	DefaultExpireTime        = 60 * 60
	InitFileName             = "flash.init"
	DefaultCacheMaxUsedRatio = 0.9
)

var (
	CacheSizeOverflowsError = errors.New("cache size overflows")
	CacheClosedError        = errors.New("cache is closed")
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
	dataPath string
	config   CacheConfig

	cachePrepareTaskCh chan cachePrepareTask
	lruCache           LruCache
	readSourceFunc     ReadExtentData

	closeOnce sync.Once
	closeCh   chan struct{}
}

type (
	ReadExtentData func(source *proto.DataSource, afterReadFunc func([]byte, int64) error) (n int, err error)
)

func NewCacheEngine(dataDir string, totalSize int64, maxUseRatio float64,
	capacity int, expireTime time.Duration, readFunc ReadExtentData,
) (s *CacheEngine, err error) {
	s = new(CacheEngine)
	s.dataPath = dataDir
	s.config = CacheConfig{
		MaxAlloc: int64(float64(totalSize) * maxUseRatio),
		Total:    totalSize,
		Capacity: capacity,
	}
	s.readSourceFunc = readFunc
	s.closeCh = make(chan struct{})

	if err = os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("NewCacheEngine [%v] err[%v]", dataDir, err)
	}
	if err = s.doMount(); err != nil {
		return
	}

	s.cachePrepareTaskCh = make(chan cachePrepareTask, 1024)
	s.lruCache = NewCache(s.config.Capacity, s.config.MaxAlloc, expireTime,
		func(v interface{}) error {
			cb := v.(*CacheBlock)
			return cb.Delete()
		},
		func(v interface{}) error {
			cb := v.(*CacheBlock)
			return cb.Close()
		})
	return
}

func (c *CacheEngine) Start() {
	c.startCachePrepareWorkers()
	log.LogInfof("CacheEngine started.")
}

func (c *CacheEngine) Stop() (err error) {
	c.closeOnce.Do(func() { close(c.closeCh) })
	if err = c.lruCache.Close(); err != nil {
		return err
	}
	time.Sleep(time.Second)
	log.LogInfof("CacheEngine stopped, umount tmpfs: %v", c.dataPath)
	return tmpfs.Umount(c.dataPath)
}

func (c *CacheEngine) initFileExists() bool {
	_, err := os.Stat(path.Join(c.dataPath, InitFileName))
	return err == nil
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

	var fd *os.File
	fd, err = os.OpenFile(path.Join(c.dataPath, InitFileName), os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	return fd.Close()
}

func (c *CacheEngine) deleteCacheBlock(key string) {
	c.lruCache.Evict(key)
}

func (c *CacheEngine) GetCacheBlockForRead(volume string, inode, offset uint64, version uint32, size uint64) (block *CacheBlock, err error) {
	key := GenCacheBlockKey(volume, inode, offset, version)
	if value, getErr := c.lruCache.Get(key); getErr == nil {
		block = value.(*CacheBlock)
		return
	}
	return nil, errors.New("cache block get failed")
}

func (c *CacheEngine) PeekCacheBlock(key string) (block *CacheBlock, err error) {
	if value, got := c.lruCache.Peek(key); got {
		block = value.(*CacheBlock)
		return
	}
	return nil, errors.New("cache block peek failed")
}

func (c *CacheEngine) createCacheBlock(volume string, inode, fixedOffset uint64, version uint32, ttl int64, allocSize uint64) (block *CacheBlock, err error) {
	if allocSize == 0 {
		return nil, fmt.Errorf("alloc size is zero")
	}
	key := GenCacheBlockKey(volume, inode, fixedOffset, version)
	if value, got := c.lruCache.Peek(key); got {
		block = value.(*CacheBlock)
		return
	}
	block = NewCacheBlock(c.dataPath, volume, inode, fixedOffset, version, allocSize, c.readSourceFunc)
	if ttl <= 0 {
		ttl = proto.DefaultCacheTTLSec
	}
	_, err = c.lruCache.Set(key, block, time.Duration(ttl)*time.Second)
	if err != nil {
		return
	}
	err = block.initFilePath()
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
	for ii := range [prepareWorkers]struct{}{} {
		go func(ii int) {
			for {
				select {
				case <-c.closeCh:
					log.LogInfof("action[startCachePrepareWorkers] worker(%d) closed", ii)
					return
				case task := <-c.cachePrepareTaskCh:
					r := task.request
					block, err := c.PeekCacheBlock(GenCacheBlockKey(r.Volume, r.Inode, r.FixedFileOffset, r.Version))
					if err != nil {
						log.LogWarnf("action[startCachePrepareWorkers] ReqID(%d) cache block not found, err:%v", task.reqID, err)
					} else {
						block.InitOnce(c, r.Sources)
					}
				}
			}
		}(ii + 1)
	}
}

func (c *CacheEngine) PrepareCache(reqID int64, req *proto.CacheRequest) (err error) {
	if _, err = c.CreateBlock(req); err != nil {
		return
	}
	select {
	case c.cachePrepareTaskCh <- cachePrepareTask{reqID: reqID, request: req}:
	default:
		log.LogError("action[PrepareCache] cachePrepareTaskCh has been full")
	}
	return
}

func (c *CacheEngine) CreateBlock(req *proto.CacheRequest) (block *CacheBlock, err error) {
	if len(req.Sources) == 0 {
		return nil, fmt.Errorf("no source data")
	}
	if block, err = c.createCacheBlock(req.Volume, req.Inode, req.FixedFileOffset, req.Version, req.TTL, computeAllocSize(req.Sources)); err != nil {
		c.deleteCacheBlock(GenCacheBlockKey(req.Volume, req.Inode, req.FixedFileOffset, req.Version))
		return nil, err
	}
	return block, nil
}

func (c *CacheEngine) Status() *proto.CacheStatus {
	lruStat := c.lruCache.Status()
	stat := &proto.CacheStatus{
		MaxAlloc: c.config.MaxAlloc,
		HasAlloc: lruStat.Allocated,
		Total:    c.config.Total,
		Used:     c.usedSize(),
		Num:      lruStat.Length,
		HitRate:  math.Trunc(lruStat.HitRate.HitRate*1e4+0.5) * 1e-4,
		Evicts:   int(lruStat.HitRate.Evicts),
		Capacity: c.config.Capacity,
		Keys:     make([]string, 0, len(lruStat.Keys)),
	}
	for _, k := range lruStat.Keys {
		stat.Keys = append(stat.Keys, k.(string))
	}
	return stat
}

func (c *CacheEngine) EvictCacheByVolume(evictVol string) (failedKeys []interface{}) {
	stat := c.lruCache.Status()
	failedKeys = make([]interface{}, 0)
	for _, k := range stat.Keys {
		vol := strings.Split(k.(string), "/")[0]
		if evictVol == vol {
			if !c.lruCache.Evict(k) {
				failedKeys = append(failedKeys, k)
			}
		}
	}
	log.LogWarnf("action[EvictCacheByVolume] evict volume(%v) finish", evictVol)
	return
}

func (c *CacheEngine) EvictCacheAll() {
	c.lruCache.EvictAll()
	log.LogWarnf("action[EvictCacheAll] evict all finish")
}

func GenCacheBlockKey(volume string, inode, offset uint64, version uint32) string {
	u := strconv.FormatUint
	return path.Join(volume, u(inode, 10)+"#"+u(offset, 10)+"#"+u(uint64(version), 10))
}
