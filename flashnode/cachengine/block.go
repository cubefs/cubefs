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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	_cacheBlockOpenOpt = os.O_CREATE | os.O_RDWR
	HeaderSize         = 16
)

type CacheBlock struct {
	rootPath    string
	filePath    string
	blockKey    string
	cacheEngine *CacheEngine

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
}

// NewCacheBlock create and returns a new extent instance.
func NewCacheBlock(rootPath string, volume string, inode, fixedOffset uint64, version uint32, allocSize uint64, reader ReadExtentData) (cb *CacheBlock) {
	cb = new(CacheBlock)
	cb.volume = volume
	cb.inode = inode
	cb.fixedOffset = fixedOffset
	cb.version = version
	cb.blockKey = GenCacheBlockKey(volume, inode, fixedOffset, version)
	cb.updateAllocSize(int64(allocSize))
	cb.filePath = path.Join(rootPath, cb.blockKey)
	cb.rootPath = rootPath
	cb.sourceReader = reader
	cb.readyCh = make(chan struct{})
	cb.closeCh = make(chan struct{})
	return
}

func (cb *CacheBlock) String() string {
	return fmt.Sprintf("volume(%s) inode(%d) offset(%d) version(%d)", cb.volume, cb.inode, cb.fixedOffset, cb.version)
}

// Close this extent and release FD.
func (cb *CacheBlock) Close() (err error) {
	cb.notifyClose()
	key := GenCacheBlockKey(cb.volume, cb.inode, cb.fixedOffset, cb.version)
	_, getErr := cb.cacheEngine.lruFhCache.Get(key)
	if getErr != nil {
		return
	}
	cb.cacheEngine.lruFhCache.Evict(key)
	return
}

func (cb *CacheBlock) Delete() (err error) {
	if !cb.Exist() {
		return
	}
	_ = cb.Close()
	return os.Remove(cb.filePath)
}

func (cb *CacheBlock) Exist() (exsit bool) {
	_, err := os.Stat(cb.filePath)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func (cb *CacheBlock) GetOrOpenFileHandler() (file *os.File, err error) {
	key := GenCacheBlockKey(cb.volume, cb.inode, cb.fixedOffset, cb.version)
	value, getErr := cb.cacheEngine.lruFhCache.Get(key)
	if getErr == nil {
		file = value.(*os.File)
	} else {
		if file, err = os.OpenFile(cb.filePath, _cacheBlockOpenOpt, 0o666); err != nil {
			return
		}
		if _, err = cb.cacheEngine.lruFhCache.Set(key, file, time.Hour); err != nil {
			return
		}
	}
	return file, nil
}

// WriteAt writes data to an cacheBlock, only append write supported
func (cb *CacheBlock) WriteAt(data []byte, offset, size int64) (err error) {
	var file *os.File
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("CacheBlock:WriteAt", err, bgTime, 1)
	}()

	if alloced := cb.getAllocSize(); offset >= alloced || size == 0 || offset+size > alloced {
		return fmt.Errorf("parameter offset=%d size=%d allocSize:%d", offset, size, alloced)
	}

	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		return
	}
	if _, err = file.WriteAt(data[:size], offset+HeaderSize); err != nil {
		return
	}
	cb.maybeUpdateUsedSize(offset + size)
	return
}

// Read reads data from an extent.
func (cb *CacheBlock) Read(ctx context.Context, data []byte, offset, size int64) (crc uint32, err error) {
	var file *os.File
	if err = cb.ready(ctx); err != nil {
		return
	}
	if offset >= cb.getAllocSize() || offset > cb.getUsedSize() || cb.getUsedSize() == 0 {
		return 0, fmt.Errorf("invalid read, offset:%d, allocSize:%d, usedSize:%d", offset, cb.getAllocSize(), cb.getUsedSize())
	}
	realSize := cb.getUsedSize() - offset
	if realSize >= size {
		realSize = size
	}
	log.LogDebugf("action[Read] read cache block:%v, offset:%d, allocSize:%d, usedSize:%d", cb.blockKey, offset, cb.allocSize, cb.usedSize)

	if file, err = cb.GetOrOpenFileHandler(); err != nil {
		return
	}
	if _, err = file.ReadAt(data[:realSize], offset+HeaderSize); err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

func (cb *CacheBlock) writeCacheBlockFileHeader(file *os.File) (err error) {
	if _, err = file.Seek(0, 0); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, cb.getAllocSize()); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, cb.getUsedSize()); err != nil {
		return
	}
	return
}

func (cb *CacheBlock) checkCacheBlockFileHeader(file *os.File) (allocSize, usedSize int64, err error) {
	var stat os.FileInfo
	if stat, err = file.Stat(); err != nil {
		return allocSize, usedSize, err
	}
	if err = binary.Read(file, binary.BigEndian, &allocSize); err != nil {
		return
	}
	if allocSize == 0 {
		return allocSize, usedSize, fmt.Errorf("allocSize is zero")
	}

	if err = binary.Read(file, binary.BigEndian, &usedSize); err != nil {
		return
	}
	if usedSize == 0 {
		return allocSize, usedSize, fmt.Errorf("usedSize is zero")
	}
	if usedSize+HeaderSize != stat.Size() {
		return allocSize, usedSize, fmt.Errorf("usedSize + headerSize[%v] != file real size[%v]", usedSize+HeaderSize, stat.Size())
	}

	return allocSize, usedSize, nil
}

func (cb *CacheBlock) initFilePath(ttl int64, isLoad bool) (err error) {
	var file *os.File
	fullPath := path.Join(cb.rootPath, cb.volume)

	if _, err = os.Stat(fullPath); err != nil {
		if !os.IsNotExist(err.(*os.PathError)) {
			return fmt.Errorf("initFilePath stat directory[%v] failed: %s", fullPath, err.Error())
		}
		if err = os.Mkdir(fullPath, 0o755); err != nil {
			if !os.IsExist(err) {
				return
			}
		}
	}

	if file, err = os.OpenFile(cb.filePath, _cacheBlockOpenOpt, 0o666); err != nil {
		return err
	}

	if !isLoad {
		cb.maybeUpdateUsedSize(0)
		if err = cb.writeCacheBlockFileHeader(file); err != nil {
			return fmt.Errorf("initFilePath write file header failed: %s", err.Error())
		}
	} else {
		var allocSize, usedSize int64
		if allocSize, usedSize, err = cb.checkCacheBlockFileHeader(file); err != nil {
			return fmt.Errorf("initFilePath check file header failed: %s", err.Error())
		}
		cb.updateAllocSize(allocSize)
		cb.maybeUpdateUsedSize(usedSize)
		cb.notifyReady()
	}

	key := GenCacheBlockKey(cb.volume, cb.inode, cb.fixedOffset, cb.version)
	if _, err = cb.cacheEngine.lruFhCache.Set(key, file, time.Duration(ttl)*time.Second); err != nil {
		return
	}

	log.LogDebugf("init cache block(%s) to tmpfs", cb.blockKey)
	return
}

func (cb *CacheBlock) Init(sources []*proto.DataSource) {
	var err error
	var file *os.File
	bgTime := stat.BeginStat()
	defer func() {
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
			if err := cb.prepareSource(ctx, sourceTaskCh); err != nil {
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
		log.LogErrorf("action[Init], block:%s, get fileHandler err:%v", cb.blockKey, err)
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

func (cb *CacheBlock) prepareSource(ctx context.Context, sourceCh <-chan *proto.DataSource) (err error) {
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
			if _, err = cb.sourceReader(source, writeCacheAfterRead); err != nil {
				log.LogErrorf("%s err:%v", logPrefix(), err)
				return
			}
			if log.EnableDebug() {
				log.LogDebugf("%s end cost[%v]", logPrefix(), time.Since(start))
			}
		}
	}
}

func (cb *CacheBlock) ready(ctx context.Context) error {
	select {
	case <-cb.readyCh:
		return nil
	case <-cb.closeCh:
		return CacheClosedError
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cb *CacheBlock) notifyClose() {
	cb.closeOnce.Do(func() { close(cb.closeCh) })
}

func (cb *CacheBlock) notifyReady() {
	cb.readyOnce.Do(func() { close(cb.readyCh) })
}

// align AllocSize with PageSize-4KB
func computeAllocSize(sources []*proto.DataSource) (alloc uint64) {
	for _, s := range sources {
		blockOffset := s.FileOffset & (proto.CACHE_BLOCK_SIZE - 1)
		blockEnd := blockOffset + s.Size_ - 1
		pageOffset := blockOffset / proto.PageSize
		pageEnd := blockEnd / proto.PageSize
		if blockEnd < blockOffset {
			return 0
		}
		for i := pageOffset; i <= pageEnd; i++ {
			alloc += proto.PageSize
		}
	}
	return
}

func (cb *CacheBlock) InitOnce(engine *CacheEngine, sources []*proto.DataSource) {
	cb.initOnce.Do(func() { cb.Init(sources) })
	select {
	case <-cb.closeCh:
		engine.deleteCacheBlock(cb.blockKey)
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
