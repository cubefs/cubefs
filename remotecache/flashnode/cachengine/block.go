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
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	_cacheBlockOpenOpt = os.O_CREATE | os.O_RDWR
	HeaderSize         = 40
	SourceTypeDefault  = ""
	SourceTypeBlock    = "blocks"
	CRCLen             = 4
)

type CacheBlock struct {
	rootPath    string
	filePath    string
	sourceType  string
	blockKey    string
	cacheEngine *CacheEngine
	ttl         int64

	volume      string
	inode       uint64
	fixedOffset uint64
	version     uint32

	usedSize  int64
	allocSize int64
	sizeLock  sync.RWMutex

	initOnce     sync.Once
	sourceReader ReadExtentData

	readyOnce sync.Once
	readyCh   chan struct{}
	closeOnce sync.Once
	closeCh   chan struct{}
	clientIP  string
	disk      *Disk
}

// NewCacheBlock create and returns a new extent instance.
func NewCacheBlock(rootPath string, volume string, inode, fixedOffset uint64, version uint32, allocSize uint64,
	reader ReadExtentData, clientIP string, d *Disk,
) (cb *CacheBlock) {
	cb = new(CacheBlock)
	cb.volume = volume
	cb.inode = inode
	cb.fixedOffset = fixedOffset
	cb.version = version
	cb.sourceType = SourceTypeDefault
	cb.blockKey = GenCacheBlockKey(volume, inode, fixedOffset, version)
	cb.updateAllocSize(int64(allocSize))
	cb.filePath = path.Join(rootPath+SourceTypeDefault, cb.blockKey)
	cb.rootPath = rootPath
	cb.sourceReader = reader
	cb.readyCh = make(chan struct{})
	cb.closeCh = make(chan struct{})
	cb.clientIP = clientIP
	cb.disk = d
	return
}

func (cb *CacheBlock) String() string {
	return fmt.Sprintf("volume(%s) inode(%d) offset(%d) version(%d)", cb.volume, cb.inode, cb.fixedOffset, cb.version)
}

// Close this extent and release FD.
func (cb *CacheBlock) Close() (err error) {
	cb.notifyClose()
	_, getErr := cb.cacheEngine.lruFhCache.Get(cb.blockKey)
	if getErr != nil {
		return
	}
	cb.cacheEngine.lruFhCache.Evict(cb.blockKey)
	return
}

func (cb *CacheBlock) Delete(reason string) (err error) {
	_ = cb.Close()
	if cb.Exist() {
		err = os.Remove(cb.filePath)
		auditlog.LogFlashNodeOp("BlockDelete", fmt.Sprintf("delete block %v, by :%v",
			cb.info(), reason), err)
	}
	return
}

func (cb *CacheBlock) Exist() (exsit bool) {
	_, err := os.Stat(cb.filePath)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func (cb *CacheBlock) GetOrOpenFileHandler() (file *os.File, err error) {
	value, getErr := cb.cacheEngine.lruFhCache.Get(cb.blockKey)
	if getErr == nil {
		file = value.(*os.File)
	} else {
		if file, err = os.OpenFile(cb.filePath, os.O_RDWR, 0o666); err != nil {
			return
		}
		if _, err = cb.cacheEngine.lruFhCache.Set(cb.blockKey, file, time.Hour); err != nil {
			return
		}
	}
	return file, nil
}

func IsDiskErr(errMsg string) bool {
	return strings.Contains(errMsg, syscall.EIO.Error()) ||
		strings.Contains(errMsg, syscall.EROFS.Error()) ||
		strings.Contains(errMsg, syscall.EACCES.Error())
}

// WriteAt writes data to an cacheBlock, only append write supported
func (cb *CacheBlock) WriteAt(data []byte, offset, size int64) (err error) {
	var file *os.File
	bgTime := stat.BeginStat()
	startTime := time.Now()
	defer func() {
		if err != nil {
			if IsDiskErr(err.Error()) {
				log.LogWarnf("[checkIsDiskError] data path(%v) meet io error", cb.filePath)
				cb.cacheEngine.triggerCacheError(cb.blockKey, cb.rootPath)
			}
		}
		stat.EndStat("MissCacheRead:WriteAt", err, bgTime, 1)
		elapsed := time.Since(startTime)
		if elapsed > time.Second {
			log.LogWarnf("[WriteAt] WriteAt function (%v) cost %v", cb.filePath, elapsed.String())
		}
	}()
	if alloced := cb.getAllocSize(); offset >= alloced || size == 0 || offset+size > alloced {
		return fmt.Errorf("parameter offset=%d size=%d allocSize:%d", offset, size, alloced)
	}

	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		log.LogWarnf("[WriteAt] GetOrOpenFileHandler (%v) err %v", cb.filePath, err)
		return
	}

	if _, err = file.WriteAt(data[:size], offset+HeaderSize); err != nil {
		log.LogWarnf("[WriteAt] WriteAt (%v) err %v", cb.filePath, err)
		return
	}
	cb.maybeUpdateUsedSize(offset + size)
	return
}

// Read reads data from an extent.
func (cb *CacheBlock) Read(ctx context.Context, data []byte, offset, size int64, waitForBlock bool, readCrc bool) (crc uint32, err error) {
	var file *os.File
	if err = cb.ready(ctx, waitForBlock); err != nil {
		return
	}
	bgTime := stat.BeginStat()
	defer func() {
		if err != nil {
			if IsDiskErr(err.Error()) {
				log.LogWarnf("[checkIsDiskError] data path(%v) meet io error", cb.filePath)
				cb.cacheEngine.triggerCacheError(cb.blockKey, cb.rootPath)
			}
		}
		stat.EndStat("HitCacheRead:ReadFromDisk", err, bgTime, 1)
	}()

	if offset >= cb.getAllocSize() || offset > cb.getUsedSize() || cb.getUsedSize() == 0 {
		return 0, fmt.Errorf("invalid read, offset:%d, allocSize:%d, usedSize:%d", offset, cb.getAllocSize(), cb.getUsedSize())
	}
	realSize := cb.getUsedSize() - offset
	if realSize >= size {
		realSize = size
	}

	if cb.sourceType == SourceTypeBlock {
		realSize = util.PageSize
	}

	log.LogDebugf("action[Read] read cache block:%v, offset:%d, allocSize:%d, usedSize:%d", cb.blockKey, offset, cb.allocSize, cb.usedSize)

	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		return
	}

	if _, err = file.ReadAt(data[:realSize], offset+HeaderSize); err != nil {
		log.LogErrorf("action[Read] read cacheBlock:%v failed, filename:%v realSize:%d", cb.blockKey, file.Name(), realSize)
		return
	}
	if readCrc {
		sliceIndex := offset / proto.PageSize
		crcOffset := cb.allocSize + HeaderSize + sliceIndex*4
		crcBuf := make([]byte, 4)
		if _, err = file.ReadAt(crcBuf, crcOffset); err != nil {
			log.LogErrorf("action[Read] read crc:%v failed, filename:%v realSize:%d", cb.blockKey, file.Name(), realSize)
			return
		}
		crc = binary.BigEndian.Uint32(crcBuf)
	} else {
		crc = crc32.ChecksumIEEE(data)
	}

	return
}

func (cb *CacheBlock) writeCacheBlockFileHeader(file *os.File) (err error) {
	if _, err = file.Seek(0, 0); err != nil {
		return
	}
	// add two reserverd
	var reserved uint64 = 0
	if err = binary.Write(file, binary.BigEndian, reserved); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, reserved); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, cb.getAllocSize()); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, cb.getUsedSize()); err != nil {
		return
	}
	if cb.getUsedSize() != 0 {
		if value, ok := cb.cacheEngine.lruCacheMap.Load(cb.rootPath); ok {
			cacheItem := value.(*lruCacheItem)
			if expiredTime, ok := cacheItem.lruCache.GetExpiredTime(cb.blockKey); ok {
				if err = binary.Write(file, binary.BigEndian, expiredTime.Unix()); err != nil {
					return
				}
			} else {
				return fmt.Errorf("cacheItem(%v) has no entry related to key(%v)", cacheItem.config.Path, cb.blockKey)
			}
		} else {
			return fmt.Errorf("no lru cache item related to dataPath(%v)", cb.rootPath)
		}
	}
	return
}

func (cb *CacheBlock) checkCacheBlockFileHeader(file *os.File, sourceType string) (allocSize, usedSize int64, expiredTime time.Time, err error) {
	var stat os.FileInfo
	var seconds int64
	var reserved1, reserved2 uint64
	if stat, err = file.Stat(); err != nil {
		return
	}

	if err = binary.Read(file, binary.BigEndian, &reserved1); err != nil {
		return
	}
	if err = binary.Read(file, binary.BigEndian, &reserved2); err != nil {
		return
	}

	if err = binary.Read(file, binary.BigEndian, &allocSize); err != nil {
		return
	}
	if allocSize == 0 {
		err = fmt.Errorf("allocSize is zero")
		return
	}

	if err = binary.Read(file, binary.BigEndian, &usedSize); err != nil {
		return
	}
	if usedSize == 0 {
		err = fmt.Errorf("usedSize is zero")
		return
	}

	if SourceTypeDefault == sourceType && usedSize+HeaderSize != stat.Size() {
		err = fmt.Errorf("usedSize + headerSize[%v] != file real size[%v]", usedSize+HeaderSize, stat.Size())
		return
	} else if SourceTypeBlock == sourceType {
		crcSize := (usedSize + proto.PageSize - 1) / proto.PageSize * CRCLen
		if allocSize+HeaderSize+crcSize != stat.Size() {
			err = fmt.Errorf("allocSize + headerSize + crsSize [%v] != file real size[%v]", allocSize+HeaderSize+crcSize, stat.Size())
			return
		}
	}

	if err = binary.Read(file, binary.BigEndian, &seconds); err != nil {
		err = fmt.Errorf("expired seconds read failed, err:%v", err)
		return
	}

	expiredTime = time.Unix(seconds, 0)
	currentTime := time.Now()
	if expiredTime.Before(currentTime) {
		err = fmt.Errorf("cacheBlock(%v) was expired, expiredTime(%v) currentTime(%v) ",
			cb.blockKey, expiredTime.Format("2006-01-02 15:04:05"), currentTime.Format("2006-01-02 15:04:05"))
		return
	}
	return
}

func (cb *CacheBlock) initFilePath(isLoad bool) (err error) {
	defer func() {
		if err != nil {
			if IsDiskErr(err.Error()) {
				log.LogWarnf("[checkIsDiskError] data path(%v) meet io error", cb.filePath)
				cb.cacheEngine.triggerCacheError(cb.blockKey, cb.rootPath)
			}
		}
	}()

	var file *os.File
	blockParent := path.Join(cb.rootPath+cb.sourceType, cb.volume)

	if _, err = os.Stat(blockParent); err != nil {
		if !os.IsNotExist(err.(*os.PathError)) {
			return fmt.Errorf("initFilePath stat directory[%v] failed: %s", blockParent, err.Error())
		}
		if err = os.Mkdir(blockParent, 0o755); err != nil {
			if !os.IsExist(err) {
				return
			}
		}
	}

	if _, err := os.Stat(cb.filePath); err != nil {
		if !os.IsNotExist(err.(*os.PathError)) {
			return fmt.Errorf("initFilePath stat filePath[%v] failed: %s", cb.filePath, err.Error())
		}
	}

	if file, err = os.OpenFile(cb.filePath, _cacheBlockOpenOpt, 0o666); err != nil {
		return err
	}

	if !isLoad {
		cb.maybeUpdateUsedSize(0)
		if err = cb.writeCacheBlockFileHeader(file); err != nil {
			file.Close()
			return fmt.Errorf("initFilePath write file header failed: %s", err.Error())
		}
		if _, err = cb.cacheEngine.lruFhCache.Set(cb.blockKey, file, time.Hour); err != nil {
			file.Close()
			return
		}
	} else {
		var allocSize, usedSize int64
		var expiredTime time.Time
		if allocSize, usedSize, expiredTime, err = cb.checkCacheBlockFileHeader(file, cb.sourceType); err != nil {
			file.Close()
			return fmt.Errorf("initFilePath check file header failed: %s", err.Error())
		}
		cb.updateAllocSize(allocSize)
		cb.maybeUpdateUsedSize(usedSize)
		cb.ttl = int64(time.Until(expiredTime).Seconds())
		if _, err = cb.cacheEngine.lruFhCache.Set(cb.blockKey, file, time.Hour); err != nil {
			file.Close()
			return
		}
		cb.notifyReady()
	}
	_, err = os.Stat(cb.filePath)
	if !isLoad {
		msg := fmt.Sprintf("init cache block(%s) to local: err %v", cb.info(), err)
		log.LogDebugf("%v", msg)
		auditlog.LogFlashNodeOp("BlockInit", msg, err)
	}
	return
}

func (cb *CacheBlock) Init(sources []*proto.DataSource, readDataNodeTimeout int) {
	var err error
	var file *os.File
	bgTime := stat.BeginStat()
	defer func() {
		if err != nil {
			if IsDiskErr(err.Error()) {
				log.LogWarnf("[checkIsDiskError] data path(%v) meet io error", cb.filePath)
				cb.cacheEngine.triggerCacheError(cb.blockKey, cb.rootPath)
			}
		}
		if value, ok := cb.cacheEngine.lruCacheMap.Load(cb.rootPath); ok {
			cacheItem := value.(*lruCacheItem)
			cacheItem.lruCache.FreePreAllocatedSize(cb.blockKey)
		}
		stat.EndStat("CacheBlock:Init", err, bgTime, 1)
	}()

	// parallel read source data
	sourceTaskCh := make(chan *proto.DataSource)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	for i := 0; i < util.Min(20, len(sources)); i++ {
		wg.Add(1)
		go func() {
			if err := cb.prepareSource(ctx, sourceTaskCh, readDataNodeTimeout); err != nil {
				cancel()
			}
			wg.Done()
		}()
	}

	sb := strings.Builder{}
	for _, s := range sources {
		select {
		case sourceTaskCh <- s:
			if log.EnableInfo() {
				sb.WriteString(s.String())
			}
		case <-ctx.Done():
		}
	}
	close(sourceTaskCh)
	wg.Wait()

	if log.EnableInfo() {
		log.LogInfof("action[Init], block:%s, sources:\n%s", cb.blockKey, sb.String())
	}
	if err = ctx.Err(); err != nil {
		log.LogErrorf("action[Init], block:%s, close %v", cb.blockKey, err)
		cb.notifyClose()
		return
	}

	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		log.LogErrorf("action[Init], block:%s, get file handler err:%v", cb.blockKey, err)
		cb.notifyClose()
		return
	}

	if err = cb.writeCacheBlockFileHeader(file); err != nil {
		log.LogErrorf("action[Init], block:%s, write file header err:%v", cb.blockKey, err)
		cb.notifyClose()
		return
	}

	cb.notifyReady()
}

func (cb *CacheBlock) prepareSource(ctx context.Context, sourceCh <-chan *proto.DataSource, readDataNodeTimeout int) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("CacheBlock:prepareSource", err, bgTime, 1)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case source, got := <-sourceCh:
			if !got {
				return
			}
			offset := int64(source.FileOffset) & (proto.CACHE_BLOCK_SIZE - 1)
			writeCacheAfterRead := func(data []byte, size int64) error {
				if e := cb.WriteAt(data, offset, size); e != nil {
					return e
				}
				offset += size
				return nil
			}
			logPrefix := func() string {
				return fmt.Sprintf("action[prepareSource] block(%s) source:%s offset:%d",
					cb.blockKey, source.String(), offset)
			}

			start := time.Now()
			if log.EnableDebug() {
				log.LogDebugf("%s start", logPrefix())
			}
			if _, err = cb.sourceReader(source, writeCacheAfterRead, readDataNodeTimeout, cb.volume, cb.inode, cb.clientIP); err != nil {
				log.LogErrorf("%s err:%v", logPrefix(), err)
				return
			}
			if log.EnableDebug() {
				log.LogDebugf("%s end cost[%v]", logPrefix(), time.Since(start))
			}
		}
	}
}

func (cb *CacheBlock) ready(ctx context.Context, waitForBlock bool) error {
	for {
		select {
		case <-cb.readyCh:
			return nil
		case <-cb.closeCh:
			return CacheClosedError
		case <-ctx.Done():
			return ctx.Err()
		default:
			if !waitForBlock {
				return fmt.Errorf("require data is caching")
			}
		}
	}
}

func (cb *CacheBlock) notifyClose() {
	cb.closeOnce.Do(func() {
		close(cb.closeCh)
	})
}

func (cb *CacheBlock) notifyReady() {
	cb.readyOnce.Do(func() { close(cb.readyCh) })
}

// align AllocSize with PageSize-4KB
func computeAllocSize(sources []*proto.DataSource) (alloc uint64) {
	if len(sources) == 0 {
		return
	}
	firstSource := sources[0]
	lastSource := sources[len(sources)-1]
	start := firstSource.FileOffset / proto.CACHE_BLOCK_SIZE * proto.CACHE_BLOCK_SIZE
	end := lastSource.FileOffset + lastSource.Size_

	alloc = end - start
	if alloc%proto.PageSize != 0 {
		alloc = (alloc/proto.PageSize + 1) * proto.PageSize
	}
	return
}

func (cb *CacheBlock) InitOnce(engine *CacheEngine, sources []*proto.DataSource) {
	cb.initOnce.Do(func() { cb.Init(sources, engine.readDataNodeTimeout) })
	select {
	case <-cb.closeCh:
		engine.DeleteCacheBlock(cb.blockKey)
		auditlog.LogFlashNodeOp("BlockInit", fmt.Sprintf("%v is closed", cb.info()), nil)
	default:
	}
}

func (cb *CacheBlock) getUsedSize() int64 {
	cb.sizeLock.RLock()
	defer cb.sizeLock.RUnlock()
	return cb.usedSize
}

func (cb *CacheBlock) maybeUpdateUsedSize(size int64) {
	cb.sizeLock.Lock()
	defer cb.sizeLock.Unlock()
	if cb.usedSize < size {
		log.LogDebugf("maybeUpdateUsedSize, cache block:%v, old:%v, new:%v", cb.blockKey, cb.usedSize, size)
		cb.usedSize = size
	}
}

func (cb *CacheBlock) getAllocSize() int64 {
	cb.sizeLock.RLock()
	defer cb.sizeLock.RUnlock()
	return cb.allocSize
}

func (cb *CacheBlock) updateAllocSize(size int64) {
	cb.sizeLock.Lock()
	defer cb.sizeLock.Unlock()
	cb.allocSize = size
}

func (cb *CacheBlock) GetRootPath() string {
	return cb.rootPath
}

func (cb *CacheBlock) InitOnceForCacheRead(engine *CacheEngine, sources []*proto.DataSource, done chan struct{}) {
	cb.initOnce.Do(func() {
		cb.InitForCacheRead(sources, engine.readDataNodeTimeout)
		select {
		case <-cb.closeCh:
			engine.DeleteCacheBlock(cb.blockKey)
			auditlog.LogFlashNodeOp("BlockInit", fmt.Sprintf("%v is closed", cb.info()), nil)
		default:
		}
	})
	close(done)
}

func (cb *CacheBlock) InitForCacheRead(sources []*proto.DataSource, readDataNodeTimeout int) {
	var err error
	var file *os.File
	bgTime := stat.BeginStat()
	defer func() {
		if err != nil {
			if IsDiskErr(err.Error()) {
				log.LogWarnf("[checkIsDiskError] data path(%v) meet io error", cb.filePath)
				cb.cacheEngine.triggerCacheError(cb.blockKey, cb.rootPath)
			}
			cb.notifyClose()
		}
		if value, ok := cb.cacheEngine.lruCacheMap.Load(cb.rootPath); ok {
			cacheItem := value.(*lruCacheItem)
			cacheItem.lruCache.FreePreAllocatedSize(cb.blockKey)
		}
		stat.EndStat("MissCacheRead:InitForCacheRead", err, bgTime, 1)
	}()
	sb := strings.Builder{}
	for _, s := range sources {
		offset := int64(s.FileOffset) & (proto.CACHE_BLOCK_SIZE - 1)
		writeCacheAfterRead := func(data []byte, size int64) error {
			if e := cb.WriteAt(data, offset, size); e != nil {
				return e
			}
			offset += size
			UpdateWriteBytesMetric(uint64(size), cb.GetRootPath())
			UpdateWriteCountMetric(cb.GetRootPath())
			return nil
		}
		logPrefix := func() string {
			return fmt.Sprintf("action[prepareSource] block(%s) source:%s offset:%d",
				cb.blockKey, s.String(), offset)
		}
		start := time.Now()
		if log.EnableDebug() {
			log.LogDebugf("%s start", logPrefix())
		}
		if _, err = cb.sourceReader(s, writeCacheAfterRead, readDataNodeTimeout, cb.volume, cb.inode, cb.clientIP); err != nil {
			log.LogErrorf("%s err:%v", logPrefix(), err)
			break
		}
		if log.EnableDebug() {
			log.LogDebugf("%s end cost[%v]", logPrefix(), time.Since(start))
		}
		if log.EnableInfo() {
			sb.WriteString(s.String())
		}
	}

	if err != nil {
		return
	}
	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		log.LogErrorf("action[Init], block:%s, get file handler err:%v", cb.blockKey, err)
		return
	}

	if err = cb.writeCacheBlockFileHeader(file); err != nil {
		log.LogErrorf("action[Init], block:%s, write file header err:%v", cb.blockKey, err)
		return
	}
	if log.EnableInfo() {
		log.LogInfof("action[InitForCacheRead], block:%s, sources:\n%s", cb.blockKey, sb.String())
	}
	cb.notifyReady()
}

func (cb *CacheBlock) info() string {
	return fmt.Sprintf("path(%v)_from(%v)_size(%v)", cb.filePath, cb.clientIP, cb.allocSize)
}

func NewCacheBlockV2(rootPath string, volume string, uniKey string, allocSize uint64, clientIP string, d *Disk,
) (cb *CacheBlock) {
	cb = new(CacheBlock)
	cb.volume = volume
	cb.blockKey = GenCacheBlockKeyV2(volume, uniKey)
	cb.updateAllocSize(int64(allocSize))
	cb.sourceType = SourceTypeBlock
	cb.filePath = path.Join(rootPath+SourceTypeBlock, cb.blockKey)
	cb.rootPath = rootPath
	cb.readyCh = make(chan struct{})
	cb.closeCh = make(chan struct{})
	cb.clientIP = clientIP
	cb.disk = d
	return
}

// CreateBlockV2 todo merge create block v2 to default
func (c *CacheEngine) CreateBlockV2(pDir string, uniKey string, ttl uint64, size uint32, clientIP string) (block *CacheBlock, err error, created bool) {
	if block, err, created = c.createCacheBlockV2(pDir, uniKey, int64(ttl), uint64(size), clientIP); err != nil {
		log.LogWarnf("action[CreateBlock] createCacheBlock(%v) failed err %v ",
			GenCacheBlockKeyV2(pDir, uniKey), err)
		c.DeleteCacheBlock(GenCacheBlockKeyV2(pDir, uniKey))
		return nil, err, created
	}
	return block, nil, created
}

func (c *CacheEngine) createCacheBlockV2(pDir string, uniKey string, ttl int64, allocSize uint64, clientIP string) (block *CacheBlock, err error, created bool) {
	if allocSize == 0 {
		return nil, fmt.Errorf("alloc size is zero"), false
	}
	key := GenCacheBlockKeyV2(pDir, uniKey)
	v, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
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
		created = true
		<-ch
		v, ok = c.keyToDiskMap.Load(key)
		if ok {
			cacheItem := v.(*lruCacheItem)
			if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
				if blockValue, got := v.(*lruCacheItem).lruCache.Peek(key); got {
					block = blockValue.(*CacheBlock)
					return
				}
			}
		}
		return nil, fmt.Errorf("unable to get created cacheblock"), created
	} else {
		defer func() {
			close(ch)
			c.creatingCacheBlockMap.Delete(key)
		}()
	}

	v, ok = c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
			if blockValue, got := cacheItem.lruCache.Peek(key); got {
				block = blockValue.(*CacheBlock)
				return
			}
		}
	}

	var cacheItem *lruCacheItem
	if cacheItem, err = c.selectAvailableLruCache(); err == nil {
		block = NewCacheBlockV2(cacheItem.config.Path, pDir, uniKey, allocSize, clientIP, cacheItem.disk)
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
		c.keyToDiskMap.Store(key, cacheItem)
	}

	return
}

func (c *CacheEngine) createCacheBlockFromExistV2(dataPath string, volume string, uniKey string, allocSize uint64, clientIP string) (block *CacheBlock, err error) {
	key := GenCacheBlockKeyV2(volume, uniKey)
	v, ok := c.keyToDiskMap.Load(key)
	if ok {
		cacheItem := v.(*lruCacheItem)
		if atomic.LoadInt32(&cacheItem.disk.Status) == proto.ReadWrite {
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
	if atomic.LoadInt32(&cacheItem.disk.Status) == proto.Unavailable {
		return nil, errors.NewErrorf("lru cache item related to dataPath(%v) is unavailable", dataPath)
	}
	block = NewCacheBlockV2(cacheItem.config.Path, volume, uniKey, allocSize, clientIP, cacheItem.disk)
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

func (cb *CacheBlock) WriteAtV2(writeParam *proto.FlashWriteParam) (err error) {
	var file *os.File
	bgTime := stat.BeginStat()
	startTime := time.Now()
	defer func() {
		if err != nil {
			if IsDiskErr(err.Error()) {
				log.LogWarnf("[checkIsDiskError] data path(%v) meet io error", cb.filePath)
				cb.cacheEngine.triggerCacheError(cb.blockKey, cb.rootPath)
			}
		}
		stat.EndStat("CacheBlock:WriteAtV2", err, bgTime, 1)
		elapsed := time.Since(startTime)
		if elapsed > time.Second {
			log.LogWarnf("[WriteAtV2] WriteAt function (%v) cost %v", cb.filePath, elapsed.String())
		}
	}()
	usedSize := cb.getUsedSize()
	if allocSize := cb.getAllocSize(); writeParam.Offset != usedSize || writeParam.Offset >= allocSize || writeParam.DataSize == 0 || writeParam.Offset+writeParam.DataSize > allocSize {
		return fmt.Errorf("parameter offset=%d size=%d allocSize:%d blockUsedSize:%d", writeParam.Offset, writeParam.DataSize, allocSize, usedSize)
	}
	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		log.LogWarnf("[WriteAtV2] GetOrOpenFileHandler (%v) err %v", cb.filePath, err)
		return
	}
	if _, err = file.WriteAt(writeParam.Data, writeParam.Offset+HeaderSize); err != nil {
		log.LogWarnf("[WriteAtV2] WriteAt (%v) data offset %v err %v", cb.filePath, writeParam.Offset+HeaderSize, err)
		return
	}
	n := writeParam.Offset/proto.PageSize*CRCLen + cb.allocSize + HeaderSize
	if log.EnableDebug() {
		log.LogDebugf("[WriteAtV2] file offset %v crc index %v and datasize %v", writeParam.Offset, n, writeParam.DataSize)
	}
	if _, err = file.WriteAt(writeParam.Crc[:CRCLen], n); err != nil {
		log.LogWarnf("[WriteAtV2] WriteAt (%v) crc offset %v err %v", cb.filePath, n, err)
		return
	}
	cb.maybeUpdateUsedSize(writeParam.Offset + writeParam.DataSize)
	return
}

func (cb *CacheBlock) MaybeWriteCompleted(reqLen int64) (err error) {
	if cb.usedSize != reqLen {
		return
	}
	var file *os.File
	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		log.LogErrorf("action[MaybeWriteCompleted], block:%s, get file handler err:%v", cb.blockKey, err)
		return
	}
	if err = cb.writeCacheBlockFileHeader(file); err != nil {
		log.LogErrorf("action[MaybeWriteCompleted], block:%s, write file header err:%v", cb.blockKey, err)
		return
	}
	if err = file.Sync(); err != nil {
		return
	}
	cb.notifyReady()
	return nil
}

func CalcAllocSizeV2(reqLen int) int {
	if reqLen%proto.PageSize != 0 {
		reqLen = (reqLen/proto.PageSize + 1) * proto.PageSize
	}
	return reqLen
}

func (cb *CacheBlock) VerifyObjectReq(offset, size uint64) error {
	end := offset + size - 1
	if offset/proto.CACHE_OBJECT_BLOCK_SIZE != end/proto.CACHE_OBJECT_BLOCK_SIZE {
		log.LogErrorf("invalid range offset(%v) size(%v)", offset, size)
		return fmt.Errorf("invalid range offset(%v) size(%v)", offset, size)
	}

	if uint64(cb.usedSize) <= end {
		log.LogWarnf("block is not read, usedSize(%v) offset(%v) size(%v)", cb.usedSize, offset, size)
		return fmt.Errorf("block is not ready")
	}

	return nil
}
