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
	"github.com/cubefs/cubefs/util/atomicutil"
	syslog "log"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
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

const (
	prepareWorkers = 20
	EnvDockerTmpfs = "DOCKER_FLASHNODE_TMPFS_OFF"

	DefaultExpireTime        = 60 * 60
	InitFileName             = "flash.init"
	DefaultCacheMaxUsedRatio = 0.9
	DefaultEnableTmpfs       = true

	LRUCacheBlockCacheType = 0
	LRUFileHandleCacheType = 1
)

var (
	RegexpCacheBlockFileName, _ = regexp.Compile(`^\d+#\d+#\d+$`)
	CacheSizeOverflowsError     = errors.New("cache size overflows")
	CacheClosedError            = errors.New("cache is closed")
)

type cachePrepareTask struct {
	request *proto.CacheRequest
	reqID   int64
}

type cacheLoadTask struct {
	volume   string
	fullPath string
}

type CacheConfig struct {
	MaxAlloc   int64 `json:"maxAlloc"`
	Total      int64 `json:"total"`
	Capacity   int   `json:"capacity"`
	FhCapacity int   `json:"fhCapacity"`
	LoadCbTTL  int64 `json:"loadCbTTL"`
}

type CacheEngine struct {
	dataPath string
	config   CacheConfig

	cachePrepareTaskCh chan cachePrepareTask
	cacheLoadTaskCh    chan cacheLoadTask
	lruCache           LruCache
	lruFhCache         LruCache
	readSourceFunc     ReadExtentData

	closeOnce sync.Once
	closeCh   chan struct{}

	enableTmpfs bool // for testing in docker
}

type (
	ReadExtentAfter func([]byte, int64) error
	ReadExtentData  func(source *proto.DataSource, afterReadFunc ReadExtentAfter) (n int, err error)
)

func NewCacheEngine(dataDir string, totalSize int64, maxUseRatio float64,
	capacity int, fhCapacity int, loadCbTTL int64, expireTime time.Duration, readFunc ReadExtentData, enableTmpfs bool,
) (s *CacheEngine, err error) {
	s = new(CacheEngine)
	s.dataPath = dataDir
	s.enableTmpfs = enableTmpfs
	if maxUseRatio < 1e-1 {
		maxUseRatio = DefaultCacheMaxUsedRatio
	}
	s.config = CacheConfig{
		MaxAlloc:   int64(float64(totalSize) * maxUseRatio),
		Total:      totalSize,
		Capacity:   capacity,
		FhCapacity: fhCapacity,
		LoadCbTTL:  loadCbTTL,
	}
	s.readSourceFunc = readFunc
	s.closeCh = make(chan struct{})

	if _, err = os.Stat(dataDir); err != nil {
		if !os.IsNotExist(err.(*os.PathError)) {
			return nil, fmt.Errorf("stat tmpfs directory failed: %s", err.Error())
		}
		if err = os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("NewCacheEngine [%v] err[%v]", dataDir, err)
		}
	}

	if s.enableTmpfs {
		log.LogInfof("CacheEngine enableTmpfs, doMount.")
		if err = s.doMount(); err != nil {
			return
		}
	}

	s.cachePrepareTaskCh = make(chan cachePrepareTask, 1024)
	s.cacheLoadTaskCh = make(chan cacheLoadTask, 1024)
	s.lruCache = NewCache(LRUCacheBlockCacheType, s.config.Capacity, s.config.MaxAlloc, expireTime,
		func(v interface{}) error {
			cb := v.(*CacheBlock)
			return cb.Delete()
		},
		func(v interface{}) error {
			cb := v.(*CacheBlock)
			return cb.Close()
		})
	s.lruFhCache = NewCache(LRUFileHandleCacheType, s.config.FhCapacity, -1, expireTime,
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	return
}

func (c *CacheEngine) isCacheBlockFileName(filename string) (isCacheBlockDir bool) {
	isCacheBlockDir = RegexpCacheBlockFileName.MatchString(filename)
	return
}

func unmarshalCacheBlockName(name string) (inode uint64, offset uint64, version uint32, err error) {
	var value uint64
	arr := strings.Split(name, "#")
	if len(arr) != 3 {
		err = fmt.Errorf("error cacheBlock name(%v)", name)
		return
	}
	if inode, err = strconv.ParseUint(arr[0], 10, 64); err != nil {
		return
	}
	if offset, err = strconv.ParseUint(arr[1], 10, 64); err != nil {
		return
	}
	if value, err = strconv.ParseUint(arr[2], 10, 32); err != nil {
		return
	}
	version = uint32(value)
	return
}

func (c *CacheEngine) LoadCacheBlock() (err error) {
	log.LogDebugf("action[LoadCacheBlock] load cacheBlock from path(%v).", c.dataPath)
	entries, err := os.ReadDir(c.dataPath)
	if err != nil {
		log.LogErrorf("action[LoadCacheBlock] read dir(%v) err(%v).", c.dataPath, err)
		return err
	}

	var (
		wg    sync.WaitGroup
		cbNum atomicutil.Int64
	)
	begin := time.Now()
	defer func() {
		msg := fmt.Sprintf("[LoadCacheBlock] dataPath(%v) load all cacheBlock(%v) using time(%v)", c.dataPath, cbNum.Load(), time.Since(begin))
		syslog.Print(msg)
		log.LogInfo(msg)
	}()

	for ii := range [prepareWorkers]struct{}{} {
		go func(ii int) {
			for {
				select {
				case <-c.closeCh:
					log.LogInfof("action[LoadCacheBlockWorkers] worker(%d) closed", ii)
					return
				case task := <-c.cacheLoadTaskCh:
					volume := task.volume
					fullPath := task.fullPath
					fileInfoList, err := os.ReadDir(fullPath)
					if err != nil {
						log.LogErrorf("action[LoadCacheBlock] read dir(%v) err(%v).", fullPath, err)
						return
					}
					for _, fileInfo := range fileInfoList {
						filename := fileInfo.Name()
						if !c.isCacheBlockFileName(filename) {
							log.LogWarnf("[LoadCacheBlock] find invalid cacheBlock file[%v] on dataPath(%v)", filename, c.dataPath)
							continue
						}
						inode, offset, version, err := unmarshalCacheBlockName(filename)
						if err != nil {
							log.LogErrorf("action[LoadCacheBlock] unmarshal cacheBlockName(%v) from dataPath(%v) volume(%v) err(%v) ",
								filename, c.dataPath, volume, err.Error())
							continue
						}
						log.LogDebugf("acton[LoadCacheBlock] dataPath(%v) cacheBlockName(%v) volume(%v) inode(%v) offset(%v) version(%v).",
							fullPath, fileInfo.Name(), volume, inode, offset, version)

						if _, err = c.createCacheBlock(volume, inode, offset, version, c.config.LoadCbTTL, 0, true); err != nil {
							c.deleteCacheBlock(GenCacheBlockKey(volume, inode, offset, version))
							log.LogErrorf("action[LoadCacheBlock] createCacheBlock(%v) from dataPath(%v) volume(%v) err(%v) ",
								filename, c.dataPath, volume, err.Error())
							continue
						}
						cbNum.Add(1)
					}
					wg.Done()
				}
			}
		}(ii + 1)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		volume := entry.Name()
		fullPath := filepath.Join(c.dataPath, volume)
		select {
		case c.cacheLoadTaskCh <- cacheLoadTask{volume: volume, fullPath: fullPath}:
		}
		wg.Add(1)
	}
	wg.Wait()
	return
}

func (c *CacheEngine) Start() (err error) {
	c.startCachePrepareWorkers()
	if err = c.LoadCacheBlock(); err != nil {
		log.LogErrorf("CacheEngine started failed, err[%v]", err)
		return
	}
	log.LogInfof("CacheEngine started.")
	return
}

func (c *CacheEngine) Stop() (err error) {
	c.closeOnce.Do(func() { close(c.closeCh) })
	if err = c.lruCache.Close(); err != nil {
		return err
	}
	if err = c.lruFhCache.Close(); err != nil {
		return err
	}
	if !c.enableTmpfs {
		log.LogInfof("CacheEngine stopped, tmpfs dir: %s", c.dataPath)
		return
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
	value, getErr := c.lruCache.Get(key)
	if getErr == nil {
		block = value.(*CacheBlock)
		return
	}
	return nil, errors.NewErrorf("cache block get failed:%v", getErr)
}

func (c *CacheEngine) PeekCacheBlock(key string) (block *CacheBlock, err error) {
	if value, got := c.lruCache.Peek(key); got {
		block = value.(*CacheBlock)
		return
	}
	return nil, errors.NewErrorf("cache block peek failed:%v", err)
}

func (c *CacheEngine) createCacheBlock(volume string, inode, fixedOffset uint64, version uint32, ttl int64, allocSize uint64, isLoad bool) (block *CacheBlock, err error) {
	if !isLoad && allocSize == 0 {
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
	block.cacheEngine = c
	if err = block.initFilePath(ttl, isLoad); err != nil {
		return
	}

	if _, err = c.lruCache.Set(key, block, time.Duration(ttl)*time.Second); err != nil {
		return
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
	if block, err = c.createCacheBlock(req.Volume, req.Inode, req.FixedFileOffset, req.Version, req.TTL, computeAllocSize(req.Sources), false); err != nil {
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
	log.LogWarn("action[EvictCacheAll] evict all finish")
}

func GenCacheBlockKey(volume string, inode, offset uint64, version uint32) string {
	u := strconv.FormatUint
	return path.Join(volume, u(inode, 10)+"#"+u(offset, 10)+"#"+u(uint64(version), 10))
}

func enabledTmpfs() bool {
	return os.Getenv(EnvDockerTmpfs) == ""
}

func (c *CacheEngine) GetHitRate() float64 {
	rateStat := c.lruCache.GetRateStat()
	return math.Trunc(rateStat.HitRate*1e4+0.5) * 1e-4
}

func (c *CacheEngine) GetEvictCount() int {
	rateStat := c.lruCache.GetRateStat()
	return int(rateStat.Evicts)
}
