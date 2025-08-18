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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/tmpfs"
	"github.com/stretchr/testify/require"
)

const (
	DefaultDiskWriteIOCC = 8
	DefaultDiskWriteFlow = 6 * util.GB
)

func initTestTmpfs() (umount func() error, err error) {
	os.MkdirAll(testTmpFS, 0o777)
	if !enabledTmpfs() {
		return func() error { return os.RemoveAll(testTmpFS) }, nil
	}
	_, err = os.Stat(testTmpFS)
	if err == nil {
		if tmpfs.IsTmpfs(testTmpFS) {
			if err = tmpfs.Umount(testTmpFS); err != nil {
				return
			}
		}
	} else {
		if !os.IsNotExist(err) {
			return
		}
		_ = os.MkdirAll(testTmpFS, 0o777)
	}
	if err = tmpfs.MountTmpfs(testTmpFS, 200*util.MB); err != nil {
		return
	}
	return func() error { return tmpfs.Umount(testTmpFS) }, nil
}

func TestBlockWriteCache(t *testing.T) {
	umount, err := initTestTmpfs()
	require.NoError(t, err)
	defer func() { require.NoError(t, umount()) }()

	testWriteSingleFile(t)
	testWriteSingleFileError(t)
	testWriteCacheBlockFull(t)
	testWriteMultiCacheBlock(t, newCacheBlockWithDiffInode)
	testWriteMultiCacheBlock(t, newCacheBlockWithDiffVolume)
}

func testWriteSingleFile(t *testing.T) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
	bytes := randTestData(1024)
	require.NoError(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	t.Logf("testWriteSingleFile, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteSingleFileError(t *testing.T) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
	bytes := randTestData(1024)
	require.NoError(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	require.Error(t, cacheBlock.WriteAt(bytes, proto.CACHE_BLOCK_SIZE, 1024))
	t.Logf("testWriteSingleFileError, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteCacheBlockFull(t *testing.T) {
	var err error
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
	bytes := randTestData(1024)
	var offset int64
	for {
		if err = cacheBlock.WriteAt(bytes, offset, 1024); err != nil {
			break
		}
		offset += 1024
		if offset/1024%1024 == 0 {
			t.Logf("testWriteCacheBlockFull, offset:%d cacheBlock.datasize:%d", offset, cacheBlock.usedSize)
		}
	}
	require.GreaterOrEqual(t, offset+1024, int64(proto.CACHE_BLOCK_SIZE))
}

func newCacheBlockWithDiffInode(volume string, index int, allocSize uint64) (cacheBlock *CacheBlock, err error) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	cacheBlock = NewCacheBlock(testTmpFS, volume, uint64(index), 1024, 112456871, allocSize,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	err = cacheBlock.initFilePath(false)
	return
}

func newCacheBlockWithDiffVolume(volume string, index int, allocSize uint64) (cacheBlock *CacheBlock, err error) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	newVolume := fmt.Sprintf("%s_%d", volume, index)
	cacheBlock = NewCacheBlock(testTmpFS, newVolume, 1, 1024, 112456871, allocSize,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	err = cacheBlock.initFilePath(false)
	return
}

func testWriteMultiCacheBlock(t *testing.T, newMultiCacheFunc func(volume string, index int, allocSize uint64) (*CacheBlock, error)) {
	count := 100
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var err error
			volume := fmt.Sprintf("%s_%d", t.Name(), index)
			cacheBlock, err := newMultiCacheFunc(volume, 1, proto.CACHE_BLOCK_SIZE)
			require.NoError(t, err)
			defer func() { require.NoError(t, cacheBlock.Delete("test")) }()

			bytes := randTestData(1024)
			var offset int64
			for j := 0; j < count; j++ {
				if err = cacheBlock.WriteAt(bytes, offset, 1024); err != nil {
					break
				}
				offset += 1024
				if j%50 == 0 {
					t.Logf("testWriteMultiCacheBlock, volume:%v, write count:%v, cacheBlock.datasize:%d", volume, j, cacheBlock.usedSize)
				}
			}
			require.GreaterOrEqual(t, offset+1024, int64(1024*count))
		}(i)
	}
	wg.Wait()
}

func TestBlockReadCache(t *testing.T) {
	umount, err := initTestTmpfs()
	require.NoError(t, err)
	defer func() { require.NoError(t, umount()) }()
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 2568748711, proto.CACHE_BLOCK_SIZE,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})

	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()

	bytes := randTestData(1024)
	offset := int64(0)
	require.NoError(t, cacheBlock.WriteAt(bytes, offset, 1024))
	require.NoError(t, err)
	cacheBlock.notifyReady()
	bytesRead := make([]byte, 1024)
	_, err = cacheBlock.Read(context.Background(), bytesRead, offset, 1024, true, false)
	require.NoError(t, err)
	require.Equal(t, bytesRead, bytes)
}

func TestParallelOperation(t *testing.T) {
	for i := 0; i < 1; i++ {
		testParallelOperation(t)
	}
}

func testParallelOperation(t *testing.T) {
	umount, err := initTestTmpfs()
	require.NoError(t, err)
	defer func() { require.NoError(t, umount()) }()
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE,
		nil, "", disk)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))

	stopCh := make(chan struct{})
	// delete func
	go func() {
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, cacheBlock.Delete("test"))
	}()

	require.NoError(t, err)
	cacheBlock.notifyReady()
	// read func
	go func() {
		ticker := time.NewTicker(time.Millisecond * 5)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				cacheBlock.cacheEngine.lruFhCache.Evict(cacheBlock.blockKey)
				return
			case <-ticker.C:
				bytesRead := make([]byte, 1024)
				offset := rand.Intn(int(cacheBlock.allocSize))
				cacheBlock.Read(context.Background(), bytesRead, int64(offset), 1024, true, false)
			}
		}
	}()

	// write func
	go func() {
		ticker := time.NewTicker(time.Millisecond * 3)
		defer ticker.Stop()
		bytes := randTestData(1024)
		offset := int64(0)
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if err := cacheBlock.WriteAt(bytes, offset, 1024); err != nil {
					return
				}
				offset += 1024
			}
		}
	}()
	time.Sleep(time.Second)
	close(stopCh)
	time.Sleep(time.Millisecond * 5)
}

func initTestTmpfs1() (umount func() error, err error) {
	os.MkdirAll(testTmpFS1, 0o777)
	if !enabledTmpfs() {
		return func() error { return os.RemoveAll(testTmpFS1) }, nil
	}
	_, err = os.Stat(testTmpFS1)
	if err == nil {
		if tmpfs.IsTmpfs(testTmpFS1) {
			if err = tmpfs.Umount(testTmpFS1); err != nil {
				return
			}
		}
	} else {
		if !os.IsNotExist(err) {
			return
		}
		_ = os.MkdirAll(testTmpFS1, 0o777)
	}
	if err = tmpfs.MountTmpfs(testTmpFS1, 200*util.MB); err != nil {
		return
	}
	return func() error { return tmpfs.Umount(testTmpFS1) }, nil
}

func TestBlockWriteCacheV2(t *testing.T) {
	umount, err := initTestTmpfs1()
	require.NoError(t, err)
	defer func() { require.NoError(t, umount()) }()

	testWriteSingleFileV2(t)
	testWriteSingleFileErrorV2(t)
	testWriteCacheBlockFullV2(t)
	testWriteMultiCacheBlockV2(t)
}

func testWriteSingleFileV2(t *testing.T) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	uniKey := t.Name()
	pDir := MapKeyToDirectory(uniKey)
	cacheBlock := NewCacheBlockV2(testTmpFS, pDir, uniKey, proto.CACHE_BLOCK_PACKET_SIZE*2, "", disk, 1024*1024, 100)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
	bytes := randTestData(proto.CACHE_BLOCK_PACKET_SIZE)
	crcBuf := make([]byte, 4)
	crcSum1 := crc32.ChecksumIEEE(bytes)
	binary.BigEndian.PutUint32(crcBuf[:4], crcSum1)
	require.NoError(t, cacheBlock.WriteAtV2(&proto.FlashWriteParam{
		Offset:   0,
		Size:     proto.CACHE_BLOCK_PACKET_SIZE * 2,
		Data:     bytes,
		Crc:      crcBuf[:4],
		DataSize: proto.CACHE_BLOCK_PACKET_SIZE,
	}))
	bytes = randTestData(proto.CACHE_BLOCK_PACKET_SIZE)
	crcSum2 := crc32.ChecksumIEEE(bytes)
	binary.BigEndian.PutUint32(crcBuf[:4], crcSum2)
	require.NoError(t, cacheBlock.WriteAtV2(&proto.FlashWriteParam{
		Offset:   proto.CACHE_BLOCK_PACKET_SIZE,
		Size:     proto.CACHE_BLOCK_PACKET_SIZE * 2,
		Data:     bytes,
		Crc:      crcBuf[:4],
		DataSize: 1024,
	}))
	t.Logf("testWriteSingleFileV2, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
	_ = cacheBlock.MaybeWriteCompleted(proto.CACHE_BLOCK_PACKET_SIZE + 1024)
	file, _ := cacheBlock.GetOrOpenFileHandler()
	_, _ = file.ReadAt(crcBuf[:4], proto.CACHE_BLOCK_PACKET_SIZE*2+HeaderSize)
	require.Equal(t, crcSum1, binary.BigEndian.Uint32(crcBuf[:4]))
	fdata := make([]byte, proto.CACHE_BLOCK_PACKET_SIZE)
	_, _ = file.ReadAt(fdata[:proto.CACHE_BLOCK_PACKET_SIZE], HeaderSize)
	require.Equal(t, crcSum1, crc32.ChecksumIEEE(fdata[:proto.CACHE_BLOCK_PACKET_SIZE]))
	_, _ = file.ReadAt(crcBuf[:4], proto.CACHE_BLOCK_PACKET_SIZE*2+HeaderSize+4)
	require.Equal(t, crcSum2, binary.BigEndian.Uint32(crcBuf[:4]))
	_, _ = file.ReadAt(fdata[:proto.CACHE_BLOCK_PACKET_SIZE], HeaderSize+proto.CACHE_BLOCK_PACKET_SIZE)
	require.Equal(t, crcSum2, crc32.ChecksumIEEE(fdata[:proto.CACHE_BLOCK_PACKET_SIZE]))
}

func testWriteSingleFileErrorV2(t *testing.T) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	uniKey := t.Name()
	pDir := MapKeyToDirectory(uniKey)
	cacheBlock := NewCacheBlockV2(testTmpFS, pDir, uniKey, proto.CACHE_BLOCK_PACKET_SIZE, "", disk, 1024*1024, 100)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
	bytes := randTestData(1024)
	require.NoError(t, cacheBlock.WriteAtV2(&proto.FlashWriteParam{
		Offset:   0,
		Size:     1024,
		Data:     bytes,
		Crc:      make([]byte, proto.CACHE_BLOCK_CRC_SIZE),
		DataSize: 1024,
	}))
	require.Error(t, cacheBlock.WriteAtV2(&proto.FlashWriteParam{
		Offset:   proto.CACHE_BLOCK_SIZE,
		Size:     2048,
		Data:     bytes,
		Crc:      make([]byte, proto.CACHE_BLOCK_CRC_SIZE),
		DataSize: 1024,
	}))
	t.Logf("testWriteSingleFileErrorV2, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteCacheBlockFullV2(t *testing.T) {
	var err error
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	uniKey := t.Name()
	pDir := MapKeyToDirectory(uniKey)
	cacheBlock := NewCacheBlockV2(testTmpFS, pDir, uniKey, proto.CACHE_OBJECT_BLOCK_SIZE, "", disk, 1024*1024, 100)
	cacheBlock.cacheEngine = &CacheEngine{}
	cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
		func(v interface{}, reason string) error {
			file := v.(*os.File)
			return file.Close()
		},
		func(v interface{}) error {
			file := v.(*os.File)
			return file.Close()
		})
	require.NoError(t, cacheBlock.initFilePath(false))
	defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
	bytes := randTestData(proto.CACHE_BLOCK_PACKET_SIZE)
	var offset int64
	for {
		if err = cacheBlock.WriteAtV2(&proto.FlashWriteParam{
			Offset:   offset,
			Size:     proto.CACHE_OBJECT_BLOCK_SIZE,
			Data:     bytes,
			Crc:      make([]byte, proto.CACHE_BLOCK_CRC_SIZE),
			DataSize: proto.CACHE_BLOCK_PACKET_SIZE,
		}); err != nil {
			break
		}
		offset += proto.CACHE_BLOCK_PACKET_SIZE
		if offset/proto.CACHE_BLOCK_PACKET_SIZE%128 == 0 {
			t.Logf("testWriteCacheBlockFullV2, offset:%d cacheBlock.datasize:%d", offset, cacheBlock.usedSize)
		}
	}
	require.Equal(t, offset, int64(proto.CACHE_OBJECT_BLOCK_SIZE))
}

func testWriteMultiCacheBlockV2(t *testing.T) {
	disk := new(Disk)
	disk.Path = testTmpFS
	disk.Status = proto.ReadWrite
	count := 25
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var err error
			uniKey := t.Name() + strconv.Itoa(index)
			pDir := MapKeyToDirectory(uniKey)
			cacheBlock := NewCacheBlockV2(testTmpFS, pDir, uniKey, proto.CACHE_BLOCK_PACKET_SIZE*25, "", disk, 1024*1024, 100)
			defer func() { require.NoError(t, cacheBlock.Delete("test")) }()
			cacheBlock.cacheEngine = &CacheEngine{}
			cacheBlock.cacheEngine.lruFhCache = NewCache(LRUFileHandleCacheType, 1, -1, time.Hour,
				func(v interface{}, reason string) error {
					file := v.(*os.File)
					return file.Close()
				},
				func(v interface{}) error {
					file := v.(*os.File)
					return file.Close()
				})
			err = cacheBlock.initFilePath(false)
			require.NoError(t, err)
			bytes := randTestData(proto.CACHE_BLOCK_PACKET_SIZE)
			offset := int64(0)
			for j := 0; j < count; j++ {
				if err = cacheBlock.WriteAtV2(&proto.FlashWriteParam{
					Offset:   offset,
					Size:     proto.CACHE_BLOCK_PACKET_SIZE * 25,
					Data:     bytes,
					Crc:      make([]byte, proto.CACHE_BLOCK_CRC_SIZE),
					DataSize: proto.CACHE_BLOCK_PACKET_SIZE,
				}); err != nil {
					break
				}
				offset += proto.CACHE_BLOCK_PACKET_SIZE
				if j%proto.CACHE_BLOCK_PACKET_SIZE == 0 {
					t.Logf("testWriteMultiCacheBlock, pdir:%v, write count:%v, cacheBlock.datasize:%d", pDir, j, cacheBlock.usedSize)
				}
			}
			require.Equal(t, offset, int64(proto.CACHE_BLOCK_PACKET_SIZE*25))
		}(i)
	}
	wg.Wait()
}
