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
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/tmpfs"
	"github.com/stretchr/testify/require"
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
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	require.NoError(t, cacheBlock.initFilePath())
	defer func() { require.NoError(t, cacheBlock.Delete()) }()
	bytes := randTestData(1024)
	require.NoError(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	t.Logf("testWriteSingleFile, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteSingleFileError(t *testing.T) {
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	require.NoError(t, cacheBlock.initFilePath())
	defer func() { require.NoError(t, cacheBlock.Delete()) }()
	bytes := randTestData(1024)
	require.NoError(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	require.Error(t, cacheBlock.WriteAt(bytes, proto.CACHE_BLOCK_SIZE, 1024))
	t.Logf("testWriteSingleFileError, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteCacheBlockFull(t *testing.T) {
	var err error
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	require.NoError(t, cacheBlock.initFilePath())
	defer func() { require.NoError(t, cacheBlock.Delete()) }()
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
	cacheBlock = NewCacheBlock(testTmpFS, volume, uint64(index), 1024, 112456871, allocSize, nil)
	err = cacheBlock.initFilePath()
	return
}

func newCacheBlockWithDiffVolume(volume string, index int, allocSize uint64) (cacheBlock *CacheBlock, err error) {
	newVolume := fmt.Sprintf("%s_%d", volume, index)
	cacheBlock = NewCacheBlock(testTmpFS, newVolume, 1, 1024, 112456871, allocSize, nil)
	err = cacheBlock.initFilePath()
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
			defer func() { require.NoError(t, cacheBlock.Delete()) }()

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

	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 2568748711, proto.CACHE_BLOCK_SIZE, nil)
	require.NoError(t, cacheBlock.initFilePath())
	defer func() { require.NoError(t, cacheBlock.Delete()) }()

	bytes := randTestData(1024)
	offset := int64(0)
	require.NoError(t, cacheBlock.WriteAt(bytes, offset, 1024))
	cacheBlock.notifyReady()
	bytesRead := make([]byte, 1024)
	_, err = cacheBlock.Read(context.Background(), bytesRead, offset, 1024)
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

	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	require.NoError(t, cacheBlock.initFilePath())

	stopCh := make(chan struct{})
	// delete func
	go func() {
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, cacheBlock.Delete())
	}()

	cacheBlock.notifyReady()
	// read func
	go func() {
		ticker := time.NewTicker(time.Millisecond * 5)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				bytesRead := make([]byte, 1024)
				offset := rand.Intn(int(cacheBlock.allocSize))
				cacheBlock.Read(context.Background(), bytesRead, int64(offset), 1024)
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
}
