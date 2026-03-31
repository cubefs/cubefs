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
	"hash/crc32"
	syslog "log"
	"math"
	"math/rand"
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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/cubefs/cubefs/util/tmpfs"
)

const (
	EnvDockerTmpfs = "DOCKER_FLASHNODE_TMPFS_OFF"
	DirMod         = 512

	DefaultExpireTime        = 60 * 60
	InitFileName             = "flash.init"
	DefaultCacheDirName      = "cache"
	DefaultCacheMaxUsedRatio = 0.99
	DefaultEnableTmpfs       = true

	BatchSetCacheLimit           = 10240
	LRUCacheBlockCacheType       = 0
	LRUFileHandleCacheType       = 1
	MaxEvictCountPerRound        = 100
	OneGiB                 int64 = 1 << 30
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
	Status     int32
}

type cachePrepareTask struct {
	request  *proto.CacheRequest
	reqID    int64
	clientIP string
}

type cacheLoadTask struct {
	volume     string
	dataPath   string
	sourceType string
}

type cacheLoadFile struct {
	volume     string
	dataPath   string
	fullPath   string
	fileName   string
	sourceType string
}

type VolCacheStats struct {
	Hits             int32
	Misses           int32
	Evicts           int32
	CacheSize        int64  // total cache size for this volume
	ReadBytes        uint64 // read bytes for this volume
	ReadCount        uint64 // read count for this volume
	WriteBytes       uint64 // write bytes for this volume
	WriteCount       uint64 // write count for this volume
	PreheatReadBytes uint64
}

type VolFlowLimit struct {
	mu    sync.RWMutex
	Flow  int64
	limit *util.IoLimiter
}

type lruCacheItem struct {
	lruCache      LruCache
	config        CacheConfig
	cacheErrCnt   uint64
	cacheErrCbSet sync.Map
	disk          *Disk
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
	keyToDiskMap  map[string]*lruCacheItem
	keyToDiskRWMu sync.RWMutex
	errorCacheNum int
	errorCacheMap sync.Map
	totalCacheNum int
	mc            *master.MasterClient

	creatingCacheBlockMap sync.Map
	cachePrepareTaskCh    chan cachePrepareTask
	prepareWorkersMu      sync.Mutex
	prepareWorkerQuit     chan struct{}
	prepareWorkerWg       sync.WaitGroup
	cacheLoadWorkerNum    int
	cacheEvictWorkerNum   int
	lruCacheMap           sync.Map
	lruFhCache            LruCache
	readSourceFunc        ReadExtentData

	closeOnce sync.Once
	closeCh   chan struct{}

	enableTmpfs bool // for testing in docker
	localAddr   string

	volCache              int32
	readDataNodeTimeout   int
	keyRateLimitThreshold int32
	keyLimiterFlow        int64
	reservedSpace         int64    // reserved disk space
	volStatsMap           sync.Map // volume -> *VolCacheStats
	volReadLimitMap       sync.Map // volume -> *VolFlowLimit
	volWriteLimitMap      sync.Map // volume -> *VolFlowLimit
	statCh                chan StatUpdate
}

type (
	ReadExtentAfter func([]byte, int64) error
	ReadExtentData  func(source *proto.DataSource, afterReadFunc ReadExtentAfter, timeout int, volume string, ino uint64, clientIP string) (n int, err error)
)

func NewCacheEngine(memDataDir string, totalMemSize int64, maxUseRatio float64, disks []*Disk,
	capacity int, fhCapacity int, diskUnavailableCbErrorCount int64, cacheLoadWorkerNum int, cacheEvictWorkerNum int, mc *master.MasterClient, expireTime time.Duration, readFunc ReadExtentData, enableTmpfs bool, localAddr string, keyRateLimitThreshold int32, keyLimiterFlow int64, reservedSpace int64,
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
	s.keyRateLimitThreshold = keyRateLimitThreshold
	s.keyLimiterFlow = keyLimiterFlow
	s.reservedSpace = reservedSpace
	s.keyToDiskMap = make(map[string]*lruCacheItem)
	s.statCh = make(chan StatUpdate, StatChanSize)
	s.startStatWorkers(4)

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
			func(v interface{}, reason string, removeOuter bool) error {
				cb := v.(*CacheBlock)
				de := cb.Delete(reason)
				if removeOuter {
					s.deleteCacheItem(cb.blockKey)
				}
				return de
			},
			func(v interface{}) error {
				cb := v.(*CacheBlock)
				return cb.Close()
			})
		cache.SetStatCh(s.statCh)
		s.lruCacheMap.Store(fullPath, &lruCacheItem{lruCache: cache, config: memCacheConfig, disk: disks[0]})
		s.totalCacheNum = 1
		s.lruFhCache = NewCache(LRUFileHandleCacheType, fhCapacity, -1, expireTime,
			func(v interface{}, reason string, removeOuter bool) error {
				file := v.(*os.File)
				return file.Close()
			},
			func(v interface{}) error {
				file := v.(*os.File)
				return file.Close()
			})
		pPaths := []string{fullPath, fullPath + SourceTypeDefault, fullPath + SourceTypeBlock}
		for _, pPath := range pPaths {
			if _, err = os.Stat(pPath); err != nil {
				if !os.IsNotExist(err.(*os.PathError)) {
					return
				}
				if err = os.Mkdir(pPath, 0o755); err != nil {
					if !os.IsExist(err) {
						return
					}
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
			func(v interface{}, reason string, removeOuter bool) error {
				cb := v.(*CacheBlock)
				de := cb.Delete(reason)
				if removeOuter {
					s.deleteCacheItem(cb.blockKey)
				}
				return de
			},
			func(v interface{}) error {
				cb := v.(*CacheBlock)
				return cb.Close()
			})
		cache.SetStatCh(s.statCh)
		s.lruCacheMap.Store(fullPath, &lruCacheItem{lruCache: cache, config: diskCacheConfig, disk: d})
		s.totalCacheNum++

		pPaths := []string{fullPath, fullPath + SourceTypeDefault, fullPath + SourceTypeBlock}
		for _, pPath := range pPaths {
			if _, err = os.Stat(pPath); err != nil {
				if !os.IsNotExist(err.(*os.PathError)) {
					return
				}
				if err = os.Mkdir(pPath, 0o755); err != nil {
					if !os.IsExist(err) {
						return
					}
				}
			}
		}

	}
	s.lruFhCache = NewCache(LRUFileHandleCacheType, fhCapacity, -1, expireTime,
		func(v interface{}, reason string, removeOuter bool) error {
			file := v.(*os.File)
			if log.EnableInfo() {
				log.LogInfof("delete file %v by %s", file.Name(), reason)
			}
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			if log.EnableInfo() {
				log.LogInfof("close file %v", file.Name())
			}
			return file.Close()
		})
	return
}

func (c *CacheEngine) SetVolCache(v int32) {
	atomic.StoreInt32(&c.volCache, v)
}

func (c *CacheEngine) isCacheBlockFileName(filename string) (isCacheBlockDir bool) {
	isCacheBlockDir = RegexpCacheBlockFileName.MatchString(filename)
	return
}

func (c *CacheEngine) SetRemoteCacheDisableTTL(remoteCacheDisableTTLMap map[string]bool) {
	log.LogDebugf("SetRemoteCacheDisableTTL (%v)", remoteCacheDisableTTLMap)
	// Update volMap in all lruCache instances
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		cacheItem.lruCache.SetRemoteCacheDisableTTL(remoteCacheDisableTTLMap)
		return true
	})
	// Also update lruFhCache if it exists
	if c.lruFhCache != nil {
		c.lruFhCache.SetRemoteCacheDisableTTL(remoteCacheDisableTTLMap)
	}
}

func (c *CacheEngine) GetRemoteCacheDisableTTLMap() map[string]bool {
	remoteCacheDisableTTLMap := make(map[string]bool)
	found := false
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		remoteCacheDisableTTLMap = cacheItem.lruCache.GetRemoteCacheDisableTTLMap()
		found = true
		return false
	})
	if !found && c.lruFhCache != nil {
		remoteCacheDisableTTLMap = c.lruFhCache.GetRemoteCacheDisableTTLMap()
	}
	if remoteCacheDisableTTLMap == nil {
		return make(map[string]bool)
	}
	return remoteCacheDisableTTLMap
}

// IsVolumeDisableTTL checks if remoteCacheDisableTTL is enabled for a specific volume
func (c *CacheEngine) IsVolumeDisableTTL(volume string) bool {
	var disableTTL bool
	found := false
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		disableTTL = cacheItem.lruCache.IsVolumeDisableTTL(volume)
		found = true
		return false // stop iteration after first cache
	})
	if !found && c.lruFhCache != nil {
		disableTTL = c.lruFhCache.IsVolumeDisableTTL(volume)
	}
	return disableTTL
}

func (c *CacheEngine) SetKeyLimiterFlow(keyLimiterFlow int64) {
	atomic.StoreInt64(&c.keyLimiterFlow, keyLimiterFlow)
	log.LogInfof("CacheEngine: set keyLimiterFlow to %d", keyLimiterFlow)
}

func (c *CacheEngine) getCacheItem(key string) (*lruCacheItem, bool) {
	c.keyToDiskRWMu.RLock()
	defer c.keyToDiskRWMu.RUnlock()
	cacheItem, ok := c.keyToDiskMap[key]
	return cacheItem, ok
}

func (c *CacheEngine) setCacheItem(key string, cacheItem *lruCacheItem, volName string, cacheSize int64) {
	c.keyToDiskRWMu.Lock()
	c.keyToDiskMap[key] = cacheItem
	c.keyToDiskRWMu.Unlock()
}

// incrementVolCounter increments the counter for a volume and updates cache size
func (c *CacheEngine) incrementVolCounter(volume string, cacheSize int64) {
	volInfo := c.getOrCreateVolStats(volume)
	if cacheSize > 0 {
		atomic.AddInt64(&volInfo.CacheSize, cacheSize)
	}
}

// decrementVolCounter decrements the counter for a volume and updates cache size
// If counter reaches 0, the volume is removed from volMap
func (c *CacheEngine) decrementVolCounter(volume string, cacheSize int64) {
	value, ok := c.volStatsMap.Load(volume)
	if !ok {
		return
	}
	volInfo := value.(*VolCacheStats)
	if cacheSize > 0 {
		atomic.AddInt64(&volInfo.CacheSize, -cacheSize)
	}
	// Remove from volMap only when counter reaches 0
	if atomic.LoadInt64(&volInfo.CacheSize) == 0 {
		c.volStatsMap.Delete(volume)
	}
}

// UpdateVolReadStats updates read statistics for a volume
func (c *CacheEngine) UpdateVolReadStats(volume string, bytes uint64) {
	if volume == "" {
		return
	}
	stats := c.getOrCreateVolStats(volume)
	atomic.AddUint64(&stats.ReadBytes, bytes)
	atomic.AddUint64(&stats.ReadCount, 1)
}

// UpdateVolWriteStats updates write statistics for a volume
func (c *CacheEngine) UpdateVolWriteStats(volume string, bytes uint64) {
	if volume == "" {
		return
	}
	stats := c.getOrCreateVolStats(volume)
	atomic.AddUint64(&stats.WriteBytes, bytes)
	atomic.AddUint64(&stats.WriteCount, 1)
}

func (c *CacheEngine) getOrCreateVolStats(volume string) *VolCacheStats {
	value, _ := c.volStatsMap.LoadOrStore(volume, &VolCacheStats{Hits: 1})
	return value.(*VolCacheStats)
}

func (c *CacheEngine) SetVolReadFlowMap(volReadFlowMap map[string]int64) {
	if volReadFlowMap == nil {
		volReadFlowMap = make(map[string]int64)
	}

	c.volReadLimitMap.Range(func(key, value interface{}) bool {
		vol := key.(string)
		flow, ok := volReadFlowMap[vol]
		if !ok || flow <= 0 {
			c.volReadLimitMap.Delete(vol)
			log.LogDebugf("SetVolReadFlowMap delete vol: %v", vol)
			return true
		}
		limit := value.(*VolFlowLimit)
		if limit.setFlow(flow) {
			log.LogDebugf("SetVolReadFlowMap set vol: %v readLimit %v", vol, flow)
		}
		return true
	})

	for vol, flow := range volReadFlowMap {
		if flow <= 0 {
			continue
		}
		if _, ok := c.volReadLimitMap.Load(vol); ok {
			continue
		}
		c.SetVolReadFlow(vol, flow)
	}
}

func (c *CacheEngine) SetVolReadFlow(volume string, flow int64) {
	if volume == "" {
		return
	}
	if flow <= 0 {
		if _, loaded := c.volReadLimitMap.LoadAndDelete(volume); loaded {
			log.LogDebugf("SetVolReadFlow delete vol: %v", volume)
		}
		return
	}
	value, _ := c.volReadLimitMap.LoadOrStore(volume, &VolFlowLimit{})
	limit := value.(*VolFlowLimit)
	if limit.setFlow(flow) {
		log.LogDebugf("SetVolReadFlow set vol: %v readLimit %v", volume, flow)
	}
}

func (c *CacheEngine) AcquireVolReadFlow(volume string, size int) error {
	log.LogDebugf("action[AcquireVolReadFlow] vol(%v) size(%v)", volume, size)
	if volume == "" || size <= 0 {
		return nil
	}
	value, ok := c.volReadLimitMap.Load(volume)
	if !ok {
		return nil
	}
	limit := value.(*VolFlowLimit)
	return limit.acquireFlow(size)
}

func (c *CacheEngine) SetVolWriteFlowMap(volWriteFlowMap map[string]int64) {
	if volWriteFlowMap == nil {
		volWriteFlowMap = make(map[string]int64)
	}

	c.volWriteLimitMap.Range(func(key, value interface{}) bool {
		vol := key.(string)
		flow, ok := volWriteFlowMap[vol]
		if !ok || flow <= 0 {
			c.volWriteLimitMap.Delete(vol)
			log.LogDebugf("SetVolWriteFlowMap delete vol: %v", vol)
			return true
		}
		limit := value.(*VolFlowLimit)
		if limit.setFlow(flow) {
			log.LogDebugf("SetVolWriteFlowMap set vol: %v writeLimit %v", vol, flow)
		}
		return true
	})

	for vol, flow := range volWriteFlowMap {
		if flow <= 0 {
			continue
		}
		if _, ok := c.volWriteLimitMap.Load(vol); ok {
			continue
		}
		c.SetVolWriteFlow(vol, flow)
	}
}

func (c *CacheEngine) SetVolWriteFlow(volume string, flow int64) {
	if volume == "" {
		return
	}
	if flow <= 0 {
		if _, loaded := c.volWriteLimitMap.LoadAndDelete(volume); loaded {
			log.LogDebugf("SetVolWriteFlow delete vol: %v", volume)
		}
		return
	}
	value, _ := c.volWriteLimitMap.LoadOrStore(volume, &VolFlowLimit{})
	limit := value.(*VolFlowLimit)
	if limit.setFlow(flow) {
		log.LogDebugf("SetVolWriteFlow set vol: %v writeLimit %v", volume, flow)
	}
}

func (c *CacheEngine) AcquireVolWriteFlow(volume string, size int) error {
	if volume == "" || size <= 0 {
		return nil
	}
	value, ok := c.volWriteLimitMap.Load(volume)
	if !ok {
		return nil
	}
	limit := value.(*VolFlowLimit)
	return limit.acquireFlow(size)
}

func (l *VolFlowLimit) setFlow(flow int64) bool {
	if atomic.LoadInt64(&l.Flow) == flow {
		return false
	}
	atomic.StoreInt64(&l.Flow, flow)

	l.mu.Lock()
	defer l.mu.Unlock()

	if flow <= 0 {
		l.limit = nil
		return true
	}
	if l.limit == nil {
		l.limit = util.NewIOLimiter(int(flow), 0)
		return true
	}
	l.limit.ResetFlow(int(flow))
	return true
}

func (l *VolFlowLimit) acquireFlow(size int) error {
	l.mu.RLock()
	limiter := l.limit
	l.mu.RUnlock()
	if limiter == nil || size <= 0 {
		return nil
	}
	return limiter.RunNoWait(size, false, func() {})
}

func (c *CacheEngine) batchSetCacheItem(items []*lruCacheItem, blocks []*CacheBlock) {
	item := items[0]
	c.keyToDiskRWMu.Lock()
	for i, block := range blocks {
		c.keyToDiskMap[block.blockKey] = items[i]
	}
	c.keyToDiskRWMu.Unlock()
	var (
		ikeys       []interface{}
		values      []interface{}
		expirations []time.Duration
	)
	for _, block := range blocks {
		ikeys = append(ikeys, block.blockKey)
		values = append(values, block)
		expirations = append(expirations, time.Duration(block.ttl)*time.Second)
	}
	_, _ = item.lruCache.BatchSet(ikeys, values, expirations)
}

func (c *CacheEngine) deleteCacheItem(key string) {
	c.keyToDiskRWMu.Lock()
	defer c.keyToDiskRWMu.Unlock()
	delete(c.keyToDiskMap, key)
}

func (c *CacheEngine) clearCacheItems() {
	c.keyToDiskRWMu.Lock()
	defer c.keyToDiskRWMu.Unlock()
	c.keyToDiskMap = make(map[string]*lruCacheItem)
}

func (c *CacheEngine) getUnavailableCacheItems() []string {
	c.keyToDiskRWMu.RLock()
	defer c.keyToDiskRWMu.RUnlock()

	var keysToDelete []string
	for key, item := range c.keyToDiskMap {
		if atomic.LoadInt32(&item.disk.Status) == proto.Unavailable {
			keysToDelete = append(keysToDelete, key)
		}
	}
	return keysToDelete
}

func (c *CacheEngine) deleteCacheItems(keys []string) {
	c.keyToDiskRWMu.Lock()
	defer c.keyToDiskRWMu.Unlock()
	for _, k := range keys {
		delete(c.keyToDiskMap, k)
	}
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
	storages := []string{SourceTypeDefault, SourceTypeBlock}
	begin := time.Now()
	defer func() {
		msg := fmt.Sprintf("[LoadDisk] dataPath(%v) load all cacheBlock(%v) using time(%v), unloaded cacheBlock num is (%v)", diskPath, cbNum.Load(), time.Since(begin), errorCbNum.Load())
		syslog.Print(msg)
		log.LogInfo(msg)
	}()
	filePathChan := make(chan cacheLoadFile, 2048)
	asyncDeleteCh := make(chan func(), 10240)
	var deleteWg sync.WaitGroup

	for i := 0; i < c.cacheLoadWorkerNum*16; i++ {
		deleteWg.Add(1)
		go func() {
			defer deleteWg.Done()
			for f := range asyncDeleteCh {
				f()
			}
		}()
	}

	for i := 0; i < c.cacheLoadWorkerNum; i++ {
		fileLoadWg.Add(1)
		go func() {
			defer fileLoadWg.Done()
			batchItems := make([]*lruCacheItem, 0, BatchSetCacheLimit)
			batchBlocks := make([]*CacheBlock, 0, BatchSetCacheLimit)
			for fileInfo := range filePathChan {
				item, block, err1 := c.handlerFile(&fileInfo, &cbNum, &errorCbNum, asyncDeleteCh)
				if err1 == nil && item != nil && block != nil {
					batchItems = append(batchItems, item)
					batchBlocks = append(batchBlocks, block)
					if len(batchBlocks) >= BatchSetCacheLimit {
						c.batchSetCacheItem(batchItems, batchBlocks)
						batchItems = batchItems[:0]
						batchBlocks = batchBlocks[:0]
					}
				}
			}
			if len(batchBlocks) > 0 {
				c.batchSetCacheItem(batchItems, batchBlocks)
			}
		}()
	}
	cacheLoadTaskCh := make(chan cacheLoadTask, 16)
	for ii := 0; ii < 4; ii++ {
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
					if task.sourceType == SourceTypeDefault && !c.isCacheBlockFileName(filename) {
						log.LogWarnf("[LoadDisk] find invalid cacheBlock file[%v] on dataPath(%v)", filename, fullPath)
						continue
					}
					filePathChan <- cacheLoadFile{volume: volume, dataPath: diskPath, fullPath: fullPath, fileName: filename, sourceType: task.sourceType}
				}
			}
		}()
	}

	log.LogDebugf("action[LoadDisk] load cacheBlock from path(%v).", diskPath)
	for _, s := range storages {
		sPath := diskPath + s
		entries, err1 := os.ReadDir(sPath)
		if err1 != nil {
			log.LogErrorf("action[LoadDisk] read dir(%v) err(%v).", sPath, err1)
			close(cacheLoadTaskCh)
			close(filePathChan)
			return err1
		}
		for _, volEntry := range entries {
			cacheLoadTaskCh <- cacheLoadTask{volume: volEntry.Name(), dataPath: sPath, sourceType: s}
		}
	}
	close(cacheLoadTaskCh)
	dirScanWg.Wait()
	close(filePathChan)
	fileLoadWg.Wait()
	close(asyncDeleteCh)
	deleteWg.Wait()
	return
}

func (c *CacheEngine) handlerFile(file *cacheLoadFile, cbNum *atomicutil.Int64, errorCbNum *atomicutil.Int64, asyncDeleteCh chan func()) (item *lruCacheItem, block *CacheBlock, err error) {
	if SourceTypeDefault == file.sourceType {
		bg := stat.BeginStat()
		inode, offset, version, err1 := unmarshalCacheBlockName(file.fileName)
		stat.EndStat("UnmarshalCacheBlockName", err1, bg, 1)
		if err1 != nil {
			err = err1
			log.LogErrorf("action[LoadDisk] unmarshal cacheBlockName(%v) from dataPath(%v) volume(%v) err(%v) ",
				file.fileName, file.fullPath, file.volume, err.Error())
			return
		}
		log.LogDebugf("acton[LoadDisk] dataPath(%v) cacheBlockName(%v) volume(%v) inode(%v) offset(%v) version(%v).",
			file.fullPath, file.fileName, file.volume, inode, offset, version)
		block, item, err = c.createCacheBlockFromExist(file.dataPath, file.volume, inode, offset, version, 0, "", asyncDeleteCh)
		if err != nil {
			c.DeleteCacheBlock(GenCacheBlockKey(file.volume, inode, offset, version))
			log.LogInfof("action[LoadDisk] createCacheBlock(%v) from dataPath(%v) volume(%v) err(%v) ",
				file.fileName, file.fullPath, file.volume, err.Error())
			errorCbNum.Add(1)
			return
		}
	} else {
		log.LogDebugf("acton[LoadDisk] dataPath(%v) cacheBlockName(%v) volume(%v)",
			file.fullPath, file.fileName, file.volume)
		bg := stat.BeginStat()
		block, item, err = c.createCacheBlockFromExistV2(file.dataPath, file.volume, file.fileName, 0, "", asyncDeleteCh)
		stat.EndStat("CreateCacheBlockFromExistV2", err, bg, 1)
		if err != nil {
			c.DeleteCacheBlock(GenCacheBlockKeyV2(file.volume, file.fileName))
			log.LogInfof("action[LoadDisk] createCacheBlock(%v) from dataPath(%v) volume(%v) err(%v) ",
				file.fileName, file.fullPath, file.volume, err.Error())
			errorCbNum.Add(1)
			return
		}
	}
	cbNum.Add(1)
	return
}

func (c *CacheEngine) Start() (err error) {
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
	var wg sync.WaitGroup
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		wg.Add(1)
		dataPath := key.(string)
		cacheItem := value.(*lruCacheItem)
		go func(d string, ci *lruCacheItem) {
			defer wg.Done()
			if err = cacheItem.lruCache.Close(); err != nil {
				return
			}
			log.LogInfof("CacheEngine stopped, data dir: %s", dataPath)
		}(dataPath, cacheItem)
		return true
	})
	wg.Wait()
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

func (c *CacheEngine) DeleteCacheBlock(key string) {
	if cacheItem, ok := c.getCacheItem(key); ok {
		cacheItem.lruCache.Evict(key)
		c.deleteCacheItem(key)
	}
}

func (c *CacheEngine) GetCacheBlockForRead(volume string, inode, offset uint64, version uint32, size uint64) (block *CacheBlock, err error) {
	key := GenCacheBlockKey(volume, inode, offset, version)
	return c.GetCacheBlockForReadByKey(key)
}

func (c *CacheEngine) GetCacheBlockForReadByKey(key string) (block *CacheBlock, err error) {
	if cacheItem, ok := c.getCacheItem(key); ok {
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
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
	if cacheItem, ok := c.getCacheItem(key); ok {
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
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
	lowSpaceCandidates := make([]*lruCacheItem, 0)
	threshold := c.reservedSpace + OneGiB // reservedSpace + 1GB
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		item := value.(*lruCacheItem)
		if atomic.LoadInt32(&item.disk.Status) == proto.ReadWrite {
			fs := syscall.Statfs_t{}
			if err = syscall.Statfs(item.disk.Path, &fs); err != nil {
				log.LogErrorf("get disk(%s) stat err:%v", item.disk.Path, err)
				return true
			}
			realLeftSpace := int64(fs.Bavail * uint64(fs.Bsize))
			// Collect low-space items and continue scanning
			if realLeftSpace < threshold {
				lowSpaceCandidates = append(lowSpaceCandidates, item)
				return true
			}
			if realLeftSpace >= maxLeftSpace {
				maxLeftSpace = realLeftSpace
				cacheItem = item
			}
		}
		return true
	})
	if cacheItem != nil {
		if log.EnableInfo() {
			log.LogInfof("select disk(%v) success", cacheItem.config.Path)
		}
		return
	}
	if len(lowSpaceCandidates) != 0 {
		if len(lowSpaceCandidates) == 1 {
			cacheItem = lowSpaceCandidates[0]
		} else {
			idx := rand.Intn(len(lowSpaceCandidates))
			cacheItem = lowSpaceCandidates[idx]
		}
		if log.EnableInfo() {
			log.LogInfof("choose disk(%v) success from low-space candidates", cacheItem.config.Path)
		}
		return
	}
	return nil, errors.NewErrorf("no available disk can select")
}

func (c *CacheEngine) createCacheBlockFromExist(dataPath string, volume string, inode, fixedOffset uint64, version uint32, allocSize uint64, clientIP string, asyncDeleteCh chan func()) (block *CacheBlock, cacheItem *lruCacheItem, err error) {
	key := GenCacheBlockKey(volume, inode, fixedOffset, version)
	if cacheItem, ok := c.getCacheItem(key); ok {
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return block, nil, nil
			}
		}
	}

	v, ok := c.lruCacheMap.Load(dataPath)
	if !ok {
		return nil, nil, errors.NewErrorf("no lru cache item related to dataPath(%v)", dataPath)
	}
	cacheItem = v.(*lruCacheItem)
	if atomic.LoadInt32(&cacheItem.disk.Status) == proto.Unavailable {
		return nil, nil, errors.NewErrorf("lru cache item related to dataPath(%v) is unavailable", dataPath)
	}
	if log.EnableDebug() {
		log.LogDebugf("createCacheBlockFromExistNewCacheBlock %v", key)
	}
	block = NewCacheBlock(cacheItem.config.Path, volume, inode, fixedOffset, version, allocSize, c.readSourceFunc,
		clientIP, cacheItem.disk)
	block.cacheEngine = c
	defer func() {
		if err != nil {
			deleteFunc := func() {
				block.Delete(fmt.Sprintf("create block from exist failed %v", err))
			}
			if asyncDeleteCh != nil {
				select {
				case asyncDeleteCh <- deleteFunc:
				default:
					deleteFunc()
				}
			} else {
				deleteFunc()
			}
		}
	}()
	err = block.initFilePath(true)
	if err != nil {
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("createCacheBlockFromExistNewCacheBlock %v volume %v", key, volume)
	}
	return
}

func (c *CacheEngine) createCacheBlock(volume string, inode, fixedOffset uint64, version uint32, ttl int64, allocSize uint64, clientIP string, isPrepare bool) (block *CacheBlock, err error) {
	if allocSize == 0 {
		return nil, fmt.Errorf("alloc size is zero")
	}
	key := GenCacheBlockKey(volume, inode, fixedOffset, version)
	if cacheItem, ok := c.getCacheItem(key); ok {
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
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
		if cacheItem, ok := c.getCacheItem(key); ok {
			if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
				if blockValue, got := cacheItem.lruCache.Peek(key); got {
					block = blockValue.(*CacheBlock)
					return
				}
			}
		}
		return nil, proto.ErrorUnableGetCreatedBlock
	} else {
		defer func() {
			close(ch)
			c.creatingCacheBlockMap.Delete(key)
		}()
	}

	if cacheItem, ok := c.getCacheItem(key); ok {
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return
			}
		}
	}
	var cacheItem *lruCacheItem
	if cacheItem, err = c.selectAvailableLruCache(); err == nil {
		block = NewCacheBlock(cacheItem.config.Path, volume, inode, fixedOffset, version, allocSize,
			c.readSourceFunc, clientIP, cacheItem.disk)
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
		if _, err = cacheItem.lruCache.CheckDiskSpace(cacheItem.disk.Path, block.blockKey, block.getAllocSize(), c.reservedSpace); err != nil {
			return
		}

		if err = block.initFilePath(false); err != nil {
			return
		}
		if _, err = cacheItem.lruCache.Set(key, block, time.Duration(ttl)*time.Second); err != nil {
			return
		}
		if !isPrepare {
			cacheItem.lruCache.AddMisses(key)
		}
		c.setCacheItem(key, cacheItem, volume, block.getUsedSize())
	}

	return
}

func (c *lruCacheItem) usedSize() (size int64) {
	if atomic.LoadInt32(&c.disk.Status) == proto.ReadWrite {
		stat := syscall.Statfs_t{}
		err := syscall.Statfs(c.disk.Path, &stat)
		if err != nil {
			log.LogErrorf("compute used size of cache engine, err:%v", err)
			return 0
		}
		return int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize)
	}
	return 0
}

func (c *lruCacheItem) FreePreAllocatedSize(key string) {
	c.lruCache.FreePreAllocatedSize(key)
}

func (c *CacheEngine) usedSize() (size int64) {
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		size += cacheItem.usedSize()
		return true
	})
	return size
}

func (c *CacheEngine) StartCachePrepareWorkers(flw *util.IoLimiter, prepareWorkers int) {
	if c.cachePrepareTaskCh == nil {
		return
	}
	c.prepareWorkersMu.Lock()
	defer c.prepareWorkersMu.Unlock()

	if c.prepareWorkerQuit != nil {
		close(c.prepareWorkerQuit)
	}
	c.prepareWorkerWg.Wait()
	c.prepareWorkerQuit = nil

	if prepareWorkers <= 0 {
		return
	}

	quitCh := make(chan struct{})
	c.prepareWorkerQuit = quitCh
	for ii := 0; ii < prepareWorkers; ii++ {
		c.prepareWorkerWg.Add(1)
		go func(workerID int, quit <-chan struct{}, limiter *util.IoLimiter) {
			defer c.prepareWorkerWg.Done()
			for {
				select {
				case <-c.closeCh:
					log.LogInfof("action[startCachePrepareWorkers] worker(%d) closed", workerID)
					return
				case <-quit:
					log.LogInfof("action[startCachePrepareWorkers] worker(%d) quit for resize", workerID)
					return
				case task := <-c.cachePrepareTaskCh:
					r := task.request
					reqSize := 0
					for _, source := range r.Sources {
						reqSize += int(source.Size_)
					}
					var err error
					bg := stat.BeginStat()
					_, err3 := c.GetCacheBlockForRead(r.Volume, r.Inode, r.FixedFileOffset, r.Version, 0)
					if err3 == nil {
						continue
					}
					err1 := limiter.Run(reqSize, true, func() {
						bk := GenCacheBlockKey(r.Volume, r.Inode, r.FixedFileOffset, r.Version)
						if log.EnableDebug() {
							log.LogDebugf("action[startCachePrepareWorkers] start cache key(%v)", bk)
						}
						if _, err = c.CreateBlock(r, task.clientIP, true); err != nil {
							log.LogWarnf("action[startCachePrepareWorkers] ReqID(%d) create block failed, err:%v", task.reqID, err)
							return
						}
						var block *CacheBlock
						block, err = c.PeekCacheBlock(bk)
						if err != nil {
							log.LogWarnf("action[startCachePrepareWorkers] ReqID(%d) cache block not found, err:%v", task.reqID, err)
						} else {
							block.InitOnce(c, r.Sources)
						}
					})
					if err1 != nil {
						log.LogWarnf("action[startCachePrepareWorkers] ReqID(%d) apply err:%v", task.reqID, err1)
					}
					if err != nil {
						err1 = err
					}
					stat.EndStat("CachePrepareHandler", err1, bg, 1)
				}
			}
		}(ii+1, quitCh, flw)
	}
}

func (c *CacheEngine) PrepareCache(reqID int64, req *proto.CacheRequest, clientIP string) (err error) {
	c.cachePrepareTaskCh <- cachePrepareTask{reqID: reqID, request: req, clientIP: clientIP}
	return
}

func (c *CacheEngine) CreateBlock(req *proto.CacheRequest, clientIP string, isPrepare bool) (block *CacheBlock, err error) {
	if len(req.Sources) == 0 {
		return nil, fmt.Errorf("no source data")
	}
	if block, err = c.createCacheBlock(req.Volume, req.Inode, req.FixedFileOffset, req.Version, req.TTL, computeAllocSize(req.Sources), clientIP, isPrepare); err != nil {
		log.LogWarnf("action[CreateBlock] createCacheBlock(%v) failed err %v ",
			GenCacheBlockKey(req.Volume, req.Inode, req.FixedFileOffset, req.Version), err)
		c.DeleteCacheBlock(GenCacheBlockKey(req.Volume, req.Inode, req.FixedFileOffset, req.Version))
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
			Status:   int(atomic.LoadInt32(&cacheItem.disk.Status)),
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
			Status:   int(atomic.LoadInt32(&cacheItem.disk.Status)),
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
					c.deleteCacheItem(k.(string))
				}
			}
		}
		return true
	})
	// Delete volume from volMap after all blocks are evicted
	c.volStatsMap.Delete(evictVol)
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
	c.clearCacheItems()
	// Clear all volumes from volMap
	c.volStatsMap.Range(func(key, value interface{}) bool {
		c.volStatsMap.Delete(key.(string))
		return true
	})
	log.LogWarn("action[EvictCacheAll] evict all finish")
}

func GenCacheBlockKey(volume string, inode, offset uint64, version uint32) string {
	u := strconv.FormatUint
	return path.Join(volume, u(inode, 10)+"#"+u(offset, 10)+"#"+u(uint64(version), 10))
}

func GenCacheBlockKeyV2(pDir string, key string) string {
	return path.Join(pDir, key)
}

func MapKeyToDirectory(key string) string {
	dirNum := crc32.ChecksumIEEE([]byte(key)) & 0xFFF % DirMod
	return strconv.Itoa(int(dirNum))
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
			Status:   int(atomic.LoadInt32(&cacheItem.disk.Status)),
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

func (c *CacheEngine) GetCacheErrorCount() map[string]int64 {
	result := make(map[string]int64)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		result[cacheItem.config.Path] = int64(atomic.LoadUint64(&cacheItem.cacheErrCnt))
		return true
	})
	return result
}

func (c *CacheEngine) ResetCacheErrCnt(dataPath string) error {
	if value, ok := c.lruCacheMap.Load(dataPath); ok {
		cacheItem := value.(*lruCacheItem)
		atomic.StoreUint64(&cacheItem.cacheErrCnt, 0)
		cacheItem.cacheErrCbSet.Range(func(key, value interface{}) bool {
			cacheItem.cacheErrCbSet.Delete(key)
			return true
		})
		return nil
	}
	return fmt.Errorf("no lru cache item related to dataPath(%v)", dataPath)
}

func (c *CacheEngine) SetDiskCacheCapacity(dataPath string, capacity int) error {
	if dataPath == "" {
		c.lruCacheMap.Range(func(key, value interface{}) bool {
			cacheItem := value.(*lruCacheItem)
			cacheItem.config.Capacity = capacity
			if cacheItem.disk != nil {
				cacheItem.disk.Capacity = capacity
			}
			cacheItem.lruCache.SetCapacity(capacity)
			return true
		})
		return nil
	} else {
		if value, ok := c.lruCacheMap.Load(dataPath); ok {
			cacheItem := value.(*lruCacheItem)
			cacheItem.config.Capacity = capacity
			if cacheItem.disk != nil {
				cacheItem.disk.Capacity = capacity
			}
			cacheItem.lruCache.SetCapacity(capacity)
			return nil
		}
		return fmt.Errorf("no lru cache item related to dataPath(%v)", dataPath)
	}
}

func (c *CacheEngine) GetCacheBytes() map[string]int64 {
	result := make(map[string]int64)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		result[cacheItem.config.Path] = cacheItem.lruCache.GetAllocated()
		return true
	})
	return result
}

func (c *CacheEngine) GetLruUsageRatio() float64 {
	var totalLen, totalCapacity int
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		totalLen += cacheItem.lruCache.Len()
		totalCapacity += cacheItem.config.Capacity
		return true
	})
	if totalCapacity > 0 {
		return float64(totalLen) / float64(totalCapacity)
	}
	return 0
}

func (c *CacheEngine) GetCacheLengths() (totalLRULen int, fhLRULen int, keyToDiskLen int) {
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		totalLRULen += cacheItem.lruCache.Len()
		return true
	})
	if c.lruFhCache != nil {
		fhLRULen = c.lruFhCache.Len()
	}
	keyToDiskLen = len(c.keyToDiskMap)
	return
}

func (c *CacheEngine) GetDiskUsageRatio() map[string]float64 {
	result := make(map[string]float64)
	c.lruCacheMap.Range(func(key, value interface{}) bool {
		cacheItem := value.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
			fs := syscall.Statfs_t{}
			if err := syscall.Statfs(cacheItem.disk.Path, &fs); err != nil {
				log.LogErrorf("get disk(%s) stat err:%v", cacheItem.disk.Path, err)
				return true
			}
			totalSpace := int64(fs.Blocks) * int64(fs.Bsize)
			usedSpace := int64(fs.Blocks-fs.Bfree) * int64(fs.Bsize)
			if totalSpace > 0 {
				usageRatio := float64(usedSpace) / float64(totalSpace)
				result[cacheItem.config.Path] = usageRatio
			} else {
				result[cacheItem.config.Path] = 0
			}
		}
		return true
	})
	return result
}

func (c *CacheEngine) DoInactiveDisk(dataPath string) {
	if value, ok := c.lruCacheMap.Load(dataPath); ok {
		cacheItem := value.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
			msg := fmt.Sprintf("do inactive disk(%v)", cacheItem.config.Path)
			log.LogWarnf(msg)
			atomic.StoreInt32(&cacheItem.disk.Status, proto.Unavailable)
			go func() {
				cacheItem.lruCache.EvictAll(c.cacheEvictWorkerNum)
			}()

			keysToDelete := c.getUnavailableCacheItems()
			c.deleteCacheItems(keysToDelete)
		}
	} else {
		log.LogErrorf("doInactiveDisk failed: no lru cache item related to dataPath(%v)", dataPath)
	}
}

func (c *CacheEngine) doInactiveFlashNode() (err error) {
	err = c.mc.NodeAPI().SetFlashNode(c.localAddr, false)
	log.LogWarnf("do inactive flashNode(%v), err(%v)", c.localAddr, err)
	auditlog.LogFlashNodeOp("DoInactiveFlashNode", fmt.Sprintf("do inactive remotecache(%v)", c.localAddr), err)
	return
}

func (c *CacheEngine) triggerCacheError(key string, dataPath string) {
	if value, ok := c.lruCacheMap.Load(dataPath); ok {
		cacheItem := value.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
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
				log.LogError(msg)
				atomic.StoreInt32(&cacheItem.disk.Status, proto.Unavailable)
				go func() {
					cacheItem.lruCache.EvictAll(c.cacheEvictWorkerNum)
				}()

				keysToDelete := c.getUnavailableCacheItems()
				c.deleteCacheItems(keysToDelete)

				if _, ok := c.errorCacheMap.Load(dataPath); !ok {
					c.errorCacheMap.Store(dataPath, struct{}{})
					c.errorCacheNum++
				}

				if c.errorCacheNum == c.totalCacheNum {
					log.LogError("all lru cache is unavailable, try to set this flashNode inactive")
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
		log.LogInfof("CacheEngine set readDataNodeTimeout from %d(ms) to %d(ms)", c.readDataNodeTimeout, timeout)
		c.readDataNodeTimeout = timeout
	}
}

func (c *CacheEngine) GetReadDataNodeTimeout() int {
	return c.readDataNodeTimeout
}

func (c *CacheEngine) GetCacheVols() (vols []string) {
	vols = make([]string, 0)
	c.volStatsMap.Range(func(key, value interface{}) bool {
		vols = append(vols, key.(string))
		return true
	})
	return vols
}

// GetVolCacheSizeMap returns a map of volume -> cache size
func (c *CacheEngine) GetVolCacheSizeMap() map[string]int64 {
	result := make(map[string]int64)
	c.volStatsMap.Range(func(key, value interface{}) bool {
		vol := key.(string)
		volInfo := value.(*VolCacheStats)
		result[vol] = atomic.LoadInt64(&volInfo.CacheSize)
		return true
	})
	return result
}

func (c *CacheEngine) startStatWorkers(workerNum int) {
	for i := 0; i < workerNum; i++ {
		go func() {
			for {
				select {
				case <-c.closeCh:
					return
				case statV, ok := <-c.statCh:
					if !ok {
						return
					}
					// ignore object cache
					if atomic.LoadInt32(&c.volCache) == 0 {
						continue
					}
					keyStr, ok := statV.Key.(string)
					if !ok {
						continue
					}
					volume := extractVolumeFromKey(keyStr)
					if volume == "" {
						continue
					}
					val, _ := c.volStatsMap.LoadOrStore(volume, &VolCacheStats{})
					stats := val.(*VolCacheStats)
					switch statV.Type {
					case StatHit:
						atomic.AddInt32(&stats.Hits, int32(statV.Count))
					case StatMiss:
						atomic.AddInt32(&stats.Misses, int32(statV.Count))
					case StatEvict:
						atomic.AddInt32(&stats.Evicts, int32(statV.Count))
					case StatPreheatReadBytes:
						atomic.AddUint64(&stats.PreheatReadBytes, uint64(statV.Count))
					}
				}
			}
		}()
	}
}

func (c *CacheEngine) GetAndResetVolStats() map[string]*VolCacheStats {
	result := make(map[string]*VolCacheStats)
	c.volStatsMap.Range(func(key, value interface{}) bool {
		vol := key.(string)
		stats := value.(*VolCacheStats)
		// Atomic swap to reset and get previous value
		hits := atomic.SwapInt32(&stats.Hits, 1)
		misses := atomic.SwapInt32(&stats.Misses, 0)
		evicts := atomic.SwapInt32(&stats.Evicts, 0)
		size := atomic.LoadInt64(&stats.CacheSize)
		readBytes := atomic.SwapUint64(&stats.ReadBytes, 0)
		readCount := atomic.SwapUint64(&stats.ReadCount, 0)
		writeBytes := atomic.SwapUint64(&stats.WriteBytes, 0)
		writeCount := atomic.SwapUint64(&stats.WriteCount, 0)
		preheatReadBytes := atomic.SwapUint64(&stats.PreheatReadBytes, 0)

		if hits > 0 || misses > 0 || evicts > 0 || size > 0 || readBytes > 0 || readCount > 0 || writeBytes > 0 || writeCount > 0 || preheatReadBytes > 0 {
			result[vol] = &VolCacheStats{
				Hits:             hits,
				Misses:           misses,
				Evicts:           evicts,
				CacheSize:        size,
				ReadBytes:        readBytes,
				ReadCount:        readCount,
				WriteBytes:       writeBytes,
				WriteCount:       writeCount,
				PreheatReadBytes: preheatReadBytes,
			}
		}
		return true
	})
	return result
}

func (c *CacheEngine) UpdateVolPreheatReadBytes(vol string, size uint64) {
	if c.statCh != nil {
		select {
		case c.statCh <- StatUpdate{Key: vol, Type: StatPreheatReadBytes, Count: int64(size)}:
		default:
		}
	}
}
