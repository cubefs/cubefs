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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	syslog "log"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/tmpfs"
)

const (
	prepareWorkers = 20
	EnvDockerTmpfs = "DOCKER_FLASHNODE_TMPFS_OFF"

	DefaultExpireTime        = 60 * 60
	InitFileName             = "flash.init"
	DefaultCacheDirName      = "cache"
	DefaultCacheMaxUsedRatio = 0.95
	DefaultEnableTmpfs       = true

	LRUCacheBlockCacheType = 0
	LRUFileHandleCacheType = 1
)

var (
	RegexpCacheBlockFileName, _ = regexp.Compile(`^\d+#\d+#\d+$`)
	CacheSizeOverflowsError     = errors.New("cache size overflows")
	CacheClosedError            = errors.New("cache is closed")
)

type Disk struct {
	Path       string
	TotalSpace int64 // actual disk space configured for caching
	Capacity   int   // lru capacity
}

type cachePrepareTask struct {
	request  *proto.CacheRequest
	reqID    int64
	clientIP string
}

type cacheLoadTask struct {
	volume   string
	dataPath string
}

type cacheLoadFile struct {
	volume   string
	dataPath string
	fullPath string
	fileName string
}

type lruCacheItem struct {
	lruCache      LruCache
	config        CacheConfig
	cacheErrCnt   uint64
	cacheErrCbSet sync.Map
	status        int32
}

type CacheConfig struct {
	Medium                      string `json:"medium"`
	Path                        string `json:"path"`
	MaxAlloc                    int64  `json:"maxAlloc"`
	Total                       int64  `json:"total"`
	Capacity                    int    `json:"capacity"`
	DiskUnavailableCbErrorCount int64  `json:"diskUnavailableCbErrorCount"`
}

type CacheEngine struct {
	memDataPath string

	fhCapacity    int
	keyToDiskMap  sync.Map
	errorCacheNum int
	errorCacheMap sync.Map
	totalCacheNum int
	mc            *master.MasterClient

	creatingCacheBlockMap sync.Map
	cachePrepareTaskCh    chan cachePrepareTask
	cacheLoadWorkerNum    int
	cacheEvictWorkerNum   int
	lruCacheMap           sync.Map
	lruFhCache            LruCache
	readSourceFunc        ReadExtentData

	closeOnce sync.Once
	closeCh   chan struct{}

	enableTmpfs bool // for testing in docker
	localAddr   string

	readDataNodeTimeout int
}

type (
	ReadExtentAfter func([]byte, int64) error
	ReadExtentData  func(source *proto.DataSource, afterReadFunc ReadExtentAfter, timeout int, volume string, ino uint64, clientIP string) (n int, err error)
)

func NewCacheEngine(memDataDir string, totalMemSize int64, maxUseRatio float64, disks []*Disk,
	capacity int, fhCapacity int, diskUnavailableCbErrorCount int64, cacheLoadWorkerNum int, cacheEvictWorkerNum int, mc *master.MasterClient, expireTime time.Duration, readFunc ReadExtentData, enableTmpfs bool, localAddr string,
) (s *CacheEngine, err error) {
	s = new(CacheEngine)
	s.enableTmpfs = enableTmpfs
	if maxUseRatio < 1e-1 {
		maxUseRatio = DefaultCacheMaxUsedRatio
	}

	s.mc = mc
	s.readSourceFunc = readFunc
	s.closeCh = make(chan struct{})
	s.fhCapacity = fhCapacity
	s.cacheLoadWorkerNum = cacheLoadWorkerNum
	s.cacheEvictWorkerNum = cacheEvictWorkerNum
	s.localAddr = localAddr
	if s.enableTmpfs {
		fullPath := path.Join(memDataDir, DefaultCacheDirName)
		memCacheConfig := CacheConfig{
			Medium:                      "memory",
			Path:                        fullPath,
			MaxAlloc:                    int64(float64(totalMemSize) * maxUseRatio),
			Total:                       totalMemSize,
			Capacity:                    capacity,
			DiskUnavailableCbErrorCount: diskUnavailableCbErrorCount,
		}

		s.memDataPath = fullPath
		s.cachePrepareTaskCh = make(chan cachePrepareTask, 1024)
		cache := NewCache(LRUCacheBlockCacheType, memCacheConfig.Capacity, memCacheConfig.MaxAlloc, expireTime,
			func(v interface{}, reason string) error {
				cb := v.(*CacheBlock)
				return cb.Delete(reason)
			},
			func(v interface{}) error {
				cb := v.(*CacheBlock)
				return cb.Close()
			})
		s.lruCacheMap.Store(fullPath, &lruCacheItem{lruCache: cache, config: memCacheConfig, status: proto.ReadWrite})
		s.totalCacheNum = 1
		s.lruFhCache = NewCache(LRUFileHandleCacheType, fhCapacity, -1, expireTime,
			func(v interface{}, reason string) error {
				file := v.(*os.File)
				return file.Close()
			},
			func(v interface{}) error {
				file := v.(*os.File)
				return file.Close()
			})

		if _, err = os.Stat(fullPath); err != nil {
			if !os.IsNotExist(err.(*os.PathError)) {
				return
			}
			if err = os.Mkdir(fullPath, 0o755); err != nil {
				if !os.IsExist(err) {
					return
				}
			}
		}

		log.LogInfof("CacheEngine enableTmpfs, doMount.")
		if err = s.doMount(); err != nil {
			return
		}

		return
	}

	for _, d := range disks {
		fullPath := path.Join(d.Path, DefaultCacheDirName)
		diskCacheConfig := CacheConfig{
			Medium:                      "disk",
			Path:                        fullPath,
			MaxAlloc:                    int64(float64(d.TotalSpace) * maxUseRatio),
			Total:                       int64(d.TotalSpace),
			Capacity:                    d.Capacity,
			DiskUnavailableCbErrorCount: diskUnavailableCbErrorCount,
		}

		log.LogInfof("CacheEngine disableTmpfs.")
		s.cachePrepareTaskCh = make(chan cachePrepareTask, 1024)
		cache := NewCache(LRUCacheBlockCacheType, diskCacheConfig.Capacity, diskCacheConfig.MaxAlloc, expireTime,
			func(v interface{}, reason string) error {
				cb := v.(*CacheBlock)
				return cb.Delete(reason)
			},
			func(v interface{}) error {
				cb := v.(*CacheBlock)
				return cb.Close()
			})
		s.lruCacheMap.Store(fullPath, &lruCacheItem{lruCache: cache, config: diskCacheConfig, status: proto.ReadWrite})
		s.totalCacheNum++

		if _, err = os.Stat(fullPath); err != nil {
			if !os.IsNotExist(err.(*os.PathError)) {
				return
			}
			if err = os.Mkdir(fullPath, 0o755); err != nil {
				if !os.IsExist(err) {
					return
				}
			}
		}
	}
	s.lruFhCache = NewCache(LRUFileHandleCacheType, fhCapacity, -1, expireTime,
		func(v interface{}, reason string) error {
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
	var wg sync.WaitGroup
	loadDiskErrors := make([]error, 0)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		dataPath := key.(string)
		wg.Add(1)
		go func(wg *sync.WaitGroup, path string) {
			defer wg.Done()
			if loadDiskError := c.LoadDisk(path); loadDiskError != nil {
				log.LogErrorf("[loadCacheBlock] load dataPath(%v) failed, err:%v", dataPath, err)
				loadDiskErrors = append(loadDiskErrors, loadDiskError)
			}
		}(&wg, dataPath)
		return true
	})
	wg.Wait()
	if len(loadDiskErrors) != 0 {
		sb := strings.Builder{}
		for index, loadDiskErr := range loadDiskErrors {
			sb.WriteString(fmt.Sprintf("err%v:%v ", index, loadDiskErr.Error()))
		}
		err = fmt.Errorf("loadCacheBlock meet %v errors, %v", len(loadDiskErrors), sb.String())
	}
	return
}

func (c *CacheEngine) LoadDisk(diskPath string) (err error) {
	var (
		dirScanWg  sync.WaitGroup
		fileLoadWg sync.WaitGroup
		cbNum      atomicutil.Int64
		errorCbNum atomicutil.Int64
	)
	begin := time.Now()
	defer func() {
		msg := fmt.Sprintf("[LoadDisk] dataPath(%v) load all cacheBlock(%v) using time(%v), unloaded cacheBlock num is (%v)", diskPath, cbNum.Load(), time.Since(begin), errorCbNum.Load())
		syslog.Print(msg)
		log.LogInfo(msg)
	}()
	filePathChan := make(chan cacheLoadFile, 1024)
	for i := 0; i < c.cacheLoadWorkerNum; i++ {
		fileLoadWg.Add(1)
		go func() {
			defer fileLoadWg.Done()
			for fileInfo := range filePathChan {
				c.handlerFile(&fileInfo, &cbNum, &errorCbNum)
			}
		}()
	}
	cacheLoadTaskCh := make(chan cacheLoadTask, 16)
	for ii := 0; ii < 3; ii++ {
		dirScanWg.Add(1)
		go func() {
			defer dirScanWg.Done()
			for task := range cacheLoadTaskCh {
				dataPath := task.dataPath
				volume := task.volume
				fullPath := filepath.Join(dataPath, volume)
				fileInfoList, err1 := os.ReadDir(fullPath)
				if err1 != nil {
					log.LogErrorf("action[LoadDisk] read dir(%v) err(%v).", fullPath, err)
					continue
				}
				if len(fileInfoList) == 0 {
					_ = os.Remove(fullPath)
					continue
				}
				for _, fileInfo := range fileInfoList {
					filename := fileInfo.Name()
					if !c.isCacheBlockFileName(filename) {
						log.LogWarnf("[LoadDisk] find invalid cacheBlock file[%v] on dataPath(%v)", filename, fullPath)
						continue
					}
					filePathChan <- cacheLoadFile{volume: volume, dataPath: diskPath, fullPath: fullPath, fileName: filename}
				}
			}
		}()
	}

	log.LogDebugf("action[LoadDisk] load cacheBlock from path(%v).", diskPath)
	entries, err := os.ReadDir(diskPath)
	if err != nil {
		log.LogErrorf("action[LoadDisk] read dir(%v) err(%v).", diskPath, err)
		close(cacheLoadTaskCh)
		close(filePathChan)
		return
	}
	for _, volEntry := range entries {
		cacheLoadTaskCh <- cacheLoadTask{volume: volEntry.Name(), dataPath: diskPath}
	}
	close(cacheLoadTaskCh)
	dirScanWg.Wait()
	close(filePathChan)
	fileLoadWg.Wait()
	return
}

func (c *CacheEngine) handlerFile(file *cacheLoadFile, cbNum *atomicutil.Int64, errorCbNum *atomicutil.Int64) {
	inode, offset, version, err := unmarshalCacheBlockName(file.fileName)
	if err != nil {
		log.LogErrorf("action[LoadDisk] unmarshal cacheBlockName(%v) from dataPath(%v) volume(%v) err(%v) ",
			file.fileName, file.fullPath, file.volume, err.Error())
		return
	}
	log.LogDebugf("acton[LoadDisk] dataPath(%v) cacheBlockName(%v) volume(%v) inode(%v) offset(%v) version(%v).",
		file.fullPath, file.fileName, file.volume, inode, offset, version)

	if _, err := c.createCacheBlockFromExist(file.dataPath, file.volume, inode, offset, version, 0, ""); err != nil {
		c.deleteCacheBlock(GenCacheBlockKey(file.volume, inode, offset, version))
		log.LogInfof("action[LoadDisk] createCacheBlock(%v) from dataPath(%v) volume(%v) err(%v) ",
			file.fileName, file.fullPath, file.volume, err.Error())
		errorCbNum.Add(1)
		return
	}
	cbNum.Add(1)
}

func (c *CacheEngine) Start() (err error) {
	c.startCachePrepareWorkers()
	if !c.enableTmpfs {
		if err = c.LoadCacheBlock(); err != nil {
			log.LogErrorf("CacheEngine started failed, err[%v]", err)
			return
		}
	}
	log.LogInfof("CacheEngine started.")
	return
}

func (c *CacheEngine) Stop() (err error) {
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		dataPath := key.(string)
		cacheItem := value.(*lruCacheItem)
		if err = cacheItem.lruCache.Close(); err != nil {
			return true
		}
		log.LogInfof("CacheEngine stopped, data dir: %s", dataPath)
		return true
	})
	if err != nil {
		return err
	}
	if err = c.lruFhCache.Close(); err != nil {
		return err
	}

	if !c.enableTmpfs {
		return
	}

	time.Sleep(time.Second)
	log.LogInfof("CacheEngine stopped, umount tmpfs: %v", c.memDataPath)
	return tmpfs.Umount(c.memDataPath)
}

func (c *CacheEngine) initFileExists() bool {
	_, err := os.Stat(path.Join(c.memDataPath, InitFileName))
	return err == nil
}

func (c *CacheEngine) doMount() (err error) {
	var mounted bool
	var fds []os.DirEntry
	_, err = os.Stat(c.memDataPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err != nil && os.IsNotExist(err) {
		return c.initTmpfs()
	}

	mounted, err = tmpfs.IsMountPoint(c.memDataPath)
	if err != nil {
		return err
	}
	if mounted && !tmpfs.IsTmpfs(c.memDataPath) {
		err = fmt.Errorf("already mounted by another device")
		return err
	}
	if mounted && c.initFileExists() {
		err = tmpfs.Umount(c.memDataPath)
		if err != nil {
			return err
		}
		return c.initTmpfs()
	}
	fds, err = os.ReadDir(c.memDataPath)
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
	value, ok := c.lruCacheMap.Load(c.memDataPath)
	if !ok {
		return fmt.Errorf("lruCacheMap can not find any config related to memDataPath(%v)", c.memDataPath)
	}
	config := value.(*lruCacheItem).config
	err = tmpfs.MountTmpfs(c.memDataPath, config.Total)
	if err != nil {
		return err
	}

	var fd *os.File
	fd, err = os.OpenFile(path.Join(c.memDataPath, InitFileName), os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	return fd.Close()
}

func (c *CacheEngine) deleteCacheBlock(key string) {
	value, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := value.(*lruCacheItem)
		cacheItem.lruCache.Evict(key)
		c.keyToDiskMap.Delete(key)
	}
}

func (c *CacheEngine) GetCacheBlockForRead(volume string, inode, offset uint64, version uint32, size uint64) (block *CacheBlock, err error) {
	key := GenCacheBlockKey(volume, inode, offset, version)
	v, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			blockValue, getErr := cacheItem.lruCache.Get(key)
			if getErr == nil {
				block = blockValue.(*CacheBlock)
				return
			}
			return nil, errors.NewErrorf("cache item(%v) get cache block failed:%v", cacheItem.config.Path, getErr)
		}
		return nil, errors.NewErrorf("cache item(%v) status is unavailable", cacheItem.config.Path)
	}
	return nil, errors.NewErrorf("cache item get failed: no lru cache can find related to key(%v)", key)
}

func (c *CacheEngine) PeekCacheBlock(key string) (block *CacheBlock, err error) {
	v, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			if blockValue, got := v.(*lruCacheItem).lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return
			}
			return nil, errors.NewErrorf("cache item(%v) peek cache block failed: no cache block can find related to key(%v)", cacheItem.config.Path, key)
		}
		return nil, errors.NewErrorf("cache item(%v) status is unavailable", cacheItem.config.Path)
	}
	return nil, errors.NewErrorf("cache item get failed: no lru cache can find related to key(%v)", key)
}

func (c *CacheEngine) selectAvailableLruCache() (cacheItem *lruCacheItem, err error) {
	var maxLeftSpace int64 = math.MinInt64
	var leftSpace int64 = 0
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		item := value.(*lruCacheItem)
		if atomic.LoadInt32(&item.status) == proto.ReadWrite {
			expectedLeftSpace := item.config.MaxAlloc - item.lruCache.GetAllocated()
			fs := syscall.Statfs_t{}
			if err = syscall.Statfs(item.config.Path, &fs); err != nil {
				return true
			}
			realLeftSpace := int64(fs.Bavail * uint64(fs.Bsize))

			if realLeftSpace < expectedLeftSpace {
				leftSpace = realLeftSpace
			} else {
				leftSpace = expectedLeftSpace
			}

			if leftSpace >= maxLeftSpace {
				maxLeftSpace = leftSpace
				cacheItem = item
			}

		}
		return true
	})
	if cacheItem != nil {
		log.LogInfof("select disk(%v) success", cacheItem.config.Path)
		return
	}
	return nil, errors.NewErrorf("no available disk can select")
}

func (c *CacheEngine) createCacheBlockFromExist(dataPath string, volume string, inode, fixedOffset uint64, version uint32, allocSize uint64, clientIP string) (block *CacheBlock, err error) {
	key := GenCacheBlockKey(volume, inode, fixedOffset, version)
	v, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return
			}
		}
	}

	v, ok = c.lruCacheMap.Load(dataPath)
	if !ok {
		return nil, errors.NewErrorf("no lru cache item related to dataPath(%v)", dataPath)
	}
	cacheItem := v.(*lruCacheItem)
	if atomic.LoadInt32(&cacheItem.status) == proto.Unavailable {
		return nil, errors.NewErrorf("lru cache item related to dataPath(%v) is unavailable", dataPath)
	}
	block = NewCacheBlock(cacheItem.config.Path, volume, inode, fixedOffset, version, allocSize, c.readSourceFunc, clientIP)
	block.cacheEngine = c
	defer func() {
		if err != nil {
			block.Delete(fmt.Sprintf("create block from exist failed %v", err))
		}
	}()

	if err = block.initFilePath(true); err != nil {
		return
	}

	if _, err = cacheItem.lruCache.Set(key, block, time.Duration(block.ttl)*time.Second); err != nil {
		return
	}
	c.keyToDiskMap.Store(key, cacheItem)

	return
}

func (c *CacheEngine) createCacheBlock(volume string, inode, fixedOffset uint64, version uint32, ttl int64, allocSize uint64, clientIP string, isPrepare bool) (block *CacheBlock, err error) {
	if allocSize == 0 {
		return nil, fmt.Errorf("alloc size is zero")
	}
	key := GenCacheBlockKey(volume, inode, fixedOffset, version)
	v, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return
			}
		}
	}

	value, loaded := c.creatingCacheBlockMap.LoadOrStore(key, make(chan struct{}))
	ch := value.(chan struct{})
	if loaded {
		<-ch
		v, ok = c.keyToDiskMap.Load(key)
		if ok {
			cacheItem := v.(*lruCacheItem)
			if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
				if blockValue, got := v.(*lruCacheItem).lruCache.Peek(key); got {
					block = blockValue.(*CacheBlock)
					return
				}
			}
		}
		return nil, fmt.Errorf("unable to get created cacheblock")
	} else {
		defer func() {
			close(ch)
			c.creatingCacheBlockMap.Delete(key)
		}()
	}

	v, ok = c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return
			}
		}
	}

	var cacheItem *lruCacheItem
	if cacheItem, err = c.selectAvailableLruCache(); err == nil {
		block = NewCacheBlock(cacheItem.config.Path, volume, inode, fixedOffset, version, allocSize, c.readSourceFunc, clientIP)
		if ttl <= 0 {
			ttl = proto.DefaultCacheTTLSec
		}
		block.cacheEngine = c
		block.ttl = ttl

		defer func() {
			if err != nil {
				block.Delete(fmt.Sprintf("create block failed %v", err))
			}
		}()
		if _, err = cacheItem.lruCache.CheckDiskSpace(block.rootPath, block.blockKey, block.getAllocSize()); err != nil {
			return
		}

		if err = block.initFilePath(false); err != nil {
			return
		}
		if _, err = cacheItem.lruCache.Set(key, block, time.Duration(ttl)*time.Second); err != nil {
			return
		}
		if !isPrepare {
			cacheItem.lruCache.AddMisses()
		}
		c.keyToDiskMap.Store(key, cacheItem)
	}

	return
}

func (c *lruCacheItem) usedSize() (size int64) {
	if atomic.LoadInt32(&c.status) == proto.ReadWrite {
		path := c.config.Path
		stat := syscall.Statfs_t{}
		err := syscall.Statfs(path, &stat)
		if err != nil {
			log.LogErrorf("compute used size of cache engine, err:%v", err)
			return 0
		}
		return int64(stat.Blocks) * int64(stat.Bsize)
	}
	return 0
}

func (c *CacheEngine) usedSize() (size int64) {
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		size += cacheItem.usedSize()
		return true
	})
	return size
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
					if _, err := c.CreateBlock(r, task.clientIP, true); err != nil {
						log.LogWarnf("action[startCachePrepareWorkers] ReqID(%d) create block failed, err:%v", task.reqID, err)
						continue
					}
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

func (c *CacheEngine) PrepareCache(reqID int64, req *proto.CacheRequest, clientIP string) (err error) {
	select {
	case c.cachePrepareTaskCh <- cachePrepareTask{reqID: reqID, request: req, clientIP: clientIP}:
	default:
		log.LogDebugf("action[PrepareCache] cachePrepareTaskCh has been full")
	}
	return
}

func (c *CacheEngine) CreateBlock(req *proto.CacheRequest, clientIP string, isPrepare bool) (block *CacheBlock, err error) {
	if len(req.Sources) == 0 {
		return nil, fmt.Errorf("no source data")
	}
	if block, err = c.createCacheBlock(req.Volume, req.Inode, req.FixedFileOffset, req.Version, req.TTL, computeAllocSize(req.Sources), clientIP, isPrepare); err != nil {
		c.deleteCacheBlock(GenCacheBlockKey(req.Volume, req.Inode, req.FixedFileOffset, req.Version))
		return nil, err
	}
	return block, nil
}

func (c *CacheEngine) Status() []*proto.CacheStatus {
	statSet := make([]*proto.CacheStatus, 0)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		lruStat := cacheItem.lruCache.Status()
		stat := &proto.CacheStatus{
			DataPath: cacheItem.config.Path,
			Medium:   cacheItem.config.Medium,
			MaxAlloc: cacheItem.config.MaxAlloc,
			HasAlloc: lruStat.Allocated,
			Total:    cacheItem.config.Total,
			Used:     cacheItem.usedSize(),
			Num:      lruStat.Length,
			HitRate:  math.Trunc(lruStat.HitRate.HitRate*1e4+0.5) * 1e-4,
			Evicts:   int(lruStat.HitRate.Evicts),
			Capacity: cacheItem.config.Capacity,
			Keys:     make([]string, 0, len(lruStat.Keys)),
			Status:   int(atomic.LoadInt32(&cacheItem.status)),
		}
		for _, k := range lruStat.Keys {
			stat.Keys = append(stat.Keys, k.(string))
		}
		statSet = append(statSet, stat)
		return true
	})
	return statSet
}

func (c *CacheEngine) StatusAll() []*proto.CacheStatus {
	statSet := make([]*proto.CacheStatus, 0)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		lruStat := cacheItem.lruCache.StatusAll()
		stat := &proto.CacheStatus{
			DataPath: cacheItem.config.Path,
			Medium:   cacheItem.config.Medium,
			MaxAlloc: cacheItem.config.MaxAlloc,
			HasAlloc: lruStat.Allocated,
			Total:    cacheItem.config.Total,
			Used:     cacheItem.usedSize(),
			Num:      lruStat.Length,
			HitRate:  math.Trunc(lruStat.HitRate.HitRate*1e4+0.5) * 1e-4,
			Evicts:   int(lruStat.HitRate.Evicts),
			Capacity: cacheItem.config.Capacity,
			Keys:     make([]string, 0, len(lruStat.Keys)),
			Status:   int(atomic.LoadInt32(&cacheItem.status)),
		}
		for _, k := range lruStat.Keys {
			stat.Keys = append(stat.Keys, k.(string))
		}
		statSet = append(statSet, stat)
		return true
	})
	return statSet
}

func (c *CacheEngine) EvictCacheByVolume(evictVol string) (failedKeys []interface{}) {
	failedKeys = make([]interface{}, 0)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		stat := cacheItem.lruCache.Status()
		for _, k := range stat.Keys {
			vol := strings.Split(k.(string), "/")[0]
			if evictVol == vol {
				if !cacheItem.lruCache.Evict(k) {
					failedKeys = append(failedKeys, k)
				} else {
					c.keyToDiskMap.Delete(k)
				}
			}
		}
		return true
	})

	log.LogWarnf("action[EvictCacheByVolume] evict volume(%v) finish", evictVol)
	return
}

func (c *CacheEngine) EvictCacheAll() {
	var wg sync.WaitGroup
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		wg.Add(1)
		go func(item *lruCacheItem) {
			defer wg.Done()
			item.lruCache.EvictAll(c.cacheEvictWorkerNum)
		}(cacheItem)
		return true
	})
	wg.Wait()
	c.keyToDiskMap = sync.Map{}
	log.LogWarn("action[EvictCacheAll] evict all finish")
}

func GenCacheBlockKey(volume string, inode, offset uint64, version uint32) string {
	u := strconv.FormatUint
	return path.Join(volume, u(inode, 10)+"#"+u(offset, 10)+"#"+u(uint64(version), 10))
}

func enabledTmpfs() bool {
	return os.Getenv(EnvDockerTmpfs) == ""
}

func (c *CacheEngine) GetHeartBeatCacheStat() []*proto.CacheStatus {
	statSet := make([]*proto.CacheStatus, 0)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		stat := &proto.CacheStatus{
			DataPath: cacheItem.config.Path,
			Medium:   cacheItem.config.Medium,
			Total:    cacheItem.config.Total,
			MaxAlloc: cacheItem.config.MaxAlloc,
			HasAlloc: cacheItem.lruCache.GetAllocated(),
			HitRate:  math.Trunc(cacheItem.lruCache.GetRateStat().HitRate*1e4+0.5) * 1e-4,
			Evicts:   int(cacheItem.lruCache.GetRateStat().Evicts),
			Num:      cacheItem.lruCache.Len(),
			Status:   int(atomic.LoadInt32(&cacheItem.status)),
		}
		statSet = append(statSet, stat)
		return true
	})
	return statSet
}

func (c *CacheEngine) GetHitRate() map[string]float64 {
	result := make(map[string]float64)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		result[cacheItem.config.Path] = math.Trunc(cacheItem.lruCache.GetRateStat().HitRate*1e4+0.5) * 1e-4
		return true
	})
	return result
}

func (c *CacheEngine) GetEvictCount() map[string]int {
	result := make(map[string]int)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		result[cacheItem.config.Path] = int(cacheItem.lruCache.GetRateStat().Evicts)
		return true
	})
	return result
}

func (c *CacheEngine) DoInactiveDisk(dataPath string) {
	if value, ok := c.lruCacheMap.Load(dataPath); ok {
		cacheItem := value.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			msg := fmt.Sprintf("do inactive disk(%v)", cacheItem.config.Path)
			log.LogWarnf(msg)
			atomic.StoreInt32(&cacheItem.status, proto.Unavailable)
			go func() {
				cacheItem.lruCache.EvictAll(c.cacheEvictWorkerNum)
			}()

			var keysToDelete []interface{}
			c.keyToDiskMap.Range(func(key, value interface{}) bool {
				item := value.(*lruCacheItem)
				if atomic.LoadInt32(&item.status) == proto.Unavailable {
					keysToDelete = append(keysToDelete, key)
				}
				return true
			})
			for _, k := range keysToDelete {
				c.keyToDiskMap.Delete(k)
			}
		}
	} else {
		log.LogErrorf("doInactiveDisk failed: no lru cache item related to dataPath(%v)", dataPath)
	}
}

func (c *CacheEngine) doInactiveFlashNode() (err error) {
	err = c.mc.NodeAPI().SetFlashNode(c.localAddr, false)
	log.LogWarnf("do inactive flashNode(%v), err(%v)", c.localAddr, err)
	auditlog.LogFlashNodeOp("DoInactiveFlashNode", fmt.Sprintf("do inactive flashnode(%v)", c.localAddr), err)
	return
}

func (c *CacheEngine) triggerCacheError(key string, dataPath string) {
	if value, ok := c.lruCacheMap.Load(dataPath); ok {
		cacheItem := value.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.status) == proto.ReadWrite {
			cacheItem.cacheErrCbSet.Store(key, struct{}{})
			cacheErrCnt := atomic.AddUint64(&cacheItem.cacheErrCnt, 1)
			cacheErrCbList := make([]string, 0)
			cacheItem.cacheErrCbSet.Range(func(key, value interface{}) bool {
				cacheErrCbList = append(cacheErrCbList, key.(string))
				return true
			})
			cacheErrCbCnt := uint64(len(cacheErrCbList))
			if cacheErrCbCnt >= uint64(cacheItem.config.DiskUnavailableCbErrorCount) {
				msg := fmt.Sprintf("too many cache error, "+
					"data path(%v), cacheErrCnt(%v), cacheErrCbCnt(%v) threshold(%v)",
					cacheItem.config.Path, cacheErrCnt, cacheErrCbCnt, cacheItem.config.DiskUnavailableCbErrorCount)
				log.LogWarnf(msg)
				atomic.StoreInt32(&cacheItem.status, proto.Unavailable)
				go func() {
					cacheItem.lruCache.EvictAll(c.cacheEvictWorkerNum)
				}()

				var keysToDelete []interface{}
				c.keyToDiskMap.Range(func(key, value interface{}) bool {
					item := value.(*lruCacheItem)
					if atomic.LoadInt32(&item.status) == proto.Unavailable {
						keysToDelete = append(keysToDelete, key)
					}
					return true
				})
				for _, k := range keysToDelete {
					c.keyToDiskMap.Delete(k)
				}

				if _, ok := c.errorCacheMap.Load(dataPath); !ok {
					c.errorCacheMap.Store(dataPath, struct{}{})
					c.errorCacheNum++
				}

				if c.errorCacheNum == c.totalCacheNum {
					log.LogWarnf("all lru cache is unavailable, try to set this flashNode inactive")
					if err := c.doInactiveFlashNode(); err != nil {
						log.LogErrorf("inactive flashNode failed, err:%v", err)
					}
				}
			}

		}
	} else {
		log.LogErrorf("trigger cache error failed: no lru cache item related to dataPath(%v)", dataPath)
	}
}

func (c *CacheEngine) SetReadDataNodeTimeout(timeout int) {
	if c.readDataNodeTimeout != timeout && timeout > 0 {
		log.LogInfof("CacheEngine set readDataNodeTimeout from %d to %d", c.readDataNodeTimeout, timeout)
		c.readDataNodeTimeout = timeout
	}
}

func (c *CacheEngine) GetReadDataNodeTimeout() int {
	return c.readDataNodeTimeout
}
