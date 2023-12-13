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
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const _cacheBlockOpenOpt = os.O_CREATE | os.O_RDWR | os.O_EXCL

type CacheBlock struct {
	rootPath string
	filePath string
	blockKey string
	file     *os.File

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
	if cb.file == nil {
		return
	}
	err = cb.file.Close()
	cb.file = nil
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

// WriteAt writes data to an cacheBlock, only append write supported
func (cb *CacheBlock) WriteAt(data []byte, offset, size int64) (err error) {
	if alloced := cb.getAllocSize(); offset >= alloced || size == 0 || offset+size > alloced {
		return fmt.Errorf("parameter offset=%d size=%d allocSize:%d", offset, size, alloced)
	}
	if _, err = cb.file.WriteAt(data[:size], offset); err != nil {
		return
	}
	cb.maybeUpdateUsedSize(offset + size)
	return
}

// Read reads data from an extent.
func (cb *CacheBlock) Read(ctx context.Context, data []byte, offset, size int64) (crc uint32, err error) {
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
	if _, err = cb.file.ReadAt(data[:realSize], offset); err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

func (cb *CacheBlock) initFilePath() (err error) {
	if err = os.Mkdir(path.Join(cb.rootPath, cb.volume), 0o666); err != nil {
		if !os.IsExist(err) {
			return
		}
	}
	if cb.file, err = os.OpenFile(cb.filePath, _cacheBlockOpenOpt, 0o666); err != nil {
		return err
	}
	cb.maybeUpdateUsedSize(0)
	log.LogDebugf("init cache block(%s) to tmpfs", cb.blockKey)
	return
}

func (cb *CacheBlock) Init(sources []*proto.DataSource) {
	var err error
	metric := exporter.NewTPCnt("InitBlock")
	defer func() {
		metric.Set(err)
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
			sb.WriteString(s.String())
		case <-ctx.Done():
		}
	}
	close(sourceTaskCh)
	wg.Wait()

	log.LogInfof("action[Init], block:%s, sources:\n%s", cb.blockKey, sb.String())
	if err = ctx.Err(); err != nil {
		log.LogErrorf("action[Init], block:%s, close %v", cb.blockKey, err)
		cb.notifyClose()
		return
	}
	cb.notifyReady()
}

func (cb *CacheBlock) prepareSource(ctx context.Context, sourceCh <-chan *proto.DataSource) (err error) {
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
			log.LogDebugf("%s start", logPrefix())
			if _, err = cb.sourceReader(source, writeCacheAfterRead); err != nil {
				log.LogErrorf("%s err:%v", logPrefix(), err)
				return
			}
			log.LogDebugf("%s end cost[%v]", logPrefix(), time.Since(start))
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
