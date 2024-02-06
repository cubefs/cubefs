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
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/tmpfs"
	"github.com/stretchr/testify/assert"
	"github.com/tiglabs/raft/util"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	testTmpFS = "/cfs_test/tmpfs"
)

var letterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randTestData(size int) (data []byte) {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return b
}

func initTestTmpfs(size int64) (err error) {
	_, err = os.Stat(testTmpFS)
	if err == nil {
		if tmpfs.IsTmpfs(testTmpFS) {
			if err = tmpfs.Umount(testTmpFS); err != nil {
				return err
			}
		}
	} else {
		if !os.IsNotExist(err) {
			return
		}
		_ = os.MkdirAll(testTmpFS, 0777)
		err = nil
	}
	err = tmpfs.MountTmpfs(testTmpFS, size)
	return
}

func umountTestTmpfs() error {
	return tmpfs.Umount(testTmpFS)
}

func TestWriteCacheBlock(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*util.MB))
	defer func() {
		assert.Nil(t, umountTestTmpfs())
	}()
	testWriteSingleFile(t)
	testWriteSingleFileError(t)
	testWriteCacheBlockFull(t)
	testWriteMultiCacheBlock(t, newCacheBlockWithDiffInode)
	testWriteMultiCacheBlock(t, newCacheBlockWithDiffVolume)
}

func testWriteSingleFile(t *testing.T) {
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initFilePath())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	bytes := randTestData(1024)
	assert.Nil(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	t.Logf("testWriteSingleFile, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteSingleFileError(t *testing.T) {
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initFilePath())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	bytes := randTestData(1024)
	assert.Nil(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	assert.NotNil(t, cacheBlock.WriteAt(bytes, proto.CACHE_BLOCK_SIZE, 1024))
	t.Logf("testWriteSingleFileError, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteCacheBlockFull(t *testing.T) {
	var err error
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initFilePath())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	bytes := randTestData(1024)
	var offset int64
	for {
		err = cacheBlock.WriteAt(bytes, offset, 1024)
		if err != nil {
			break
		}
		offset += 1024
		if offset/1024%1024 == 0 {
			t.Logf("testWriteCacheBlockFull, offset:%d cacheBlock.datasize:%d", offset, cacheBlock.usedSize)
		}
	}
	assert.GreaterOrEqual(t, offset+1024, int64(proto.CACHE_BLOCK_SIZE))
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
			if err != nil {
				t.Errorf("testWriteMultiCacheBlock, err:%v", err)
				return
			}
			defer func() {
				assert.Nil(t, cacheBlock.Delete())
			}()
			bytes := randTestData(1024)
			var offset int64
			for j := 0; j < count; j++ {
				err = cacheBlock.WriteAt(bytes, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
				time.Sleep(time.Millisecond * 100)
				if j%50 == 0 {
					t.Logf("testWriteMultiCacheBlock, volume:%v, write count:%v, cacheBlock.datasize:%d", volume, j, cacheBlock.usedSize)
				}
			}
			assert.GreaterOrEqual(t, offset+1024, int64(1024*count))
		}(i)
	}
	wg.Wait()
}

func TestReadCacheBlock(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*util.MB))
	defer func() {
		assert.Nil(t, umountTestTmpfs())
	}()
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 2568748711, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initFilePath())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()

	bytes := randTestData(1024)
	offset := int64(0)
	assert.Nil(t, cacheBlock.WriteAt(bytes, offset, 1024))
	cacheBlock.markReady()
	bytesRead := make([]byte, 1024)
	_, err := cacheBlock.Read(context.Background(), bytesRead, offset, 1024)
	assert.Nil(t, err)
	for i := 0; i < 1024; i++ {
		assert.Equal(t, bytesRead[i], bytes[i])
	}
	return
}

func TestParallelOperation(t *testing.T) {
	for i := 0; i < 1; i++ {
		testParallelOperation(t)
	}
}

func testParallelOperation(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*util.MB))
	defer func() {
		assert.Nil(t, umountTestTmpfs())
	}()

	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initFilePath())
	stopCh := make(chan struct{}, 1)

	//delete func
	go func() {
		time.Sleep(time.Second * 5)
		assert.Nil(t, cacheBlock.Delete())
	}()
	cacheBlock.markReady()
	//read func
	go func() {
		ticker := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-stopCh:
				break
			case <-ticker.C:
				bytesRead := make([]byte, 1024)
				rand.Seed(time.Now().Unix())
				offset := rand.Intn(int(cacheBlock.allocSize))
				cacheBlock.Read(context.Background(), bytesRead, int64(offset), 1024)
			}
		}
	}()

	//write func
	go func() {
		bytes := randTestData(1024)
		offset := int64(0)
		ticker := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-stopCh:
				break
			case <-ticker.C:
				err := cacheBlock.WriteAt(bytes, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
			}
		}
	}()
	time.Sleep(time.Second * 10)
	close(stopCh)
}

func TestComputeAllocSize(t *testing.T) {
	cases1 := []*proto.DataSource{
		{
			FileOffset: 0,
			Size_:      100,
		},
		{
			FileOffset: 1024,
			Size_:      100,
		},
	}
	alloc, err := computeAllocSize(cases1)
	assert.ErrorContains(t, err, SparseFileError.Error())

	cases2 := []*proto.DataSource{
		{
			FileOffset: proto.CACHE_BLOCK_SIZE + 53,
			Size_:      43,
		},
	}
	alloc, err = computeAllocSize(cases2)
	assert.ErrorContains(t, err, SparseFileError.Error())

	cases3 := []*proto.DataSource{
		{
			FileOffset: proto.CACHE_BLOCK_SIZE,
			Size_:      43,
		},
	}
	alloc, err = computeAllocSize(cases3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(proto.PageSize), alloc)

	cases4 := []*proto.DataSource{
		{
			FileOffset: 0,
			Size_:      1024,
		},
		{
			FileOffset: 1024,
			Size_:      4096,
		},
		{
			FileOffset: 5120,
			Size_:      4096,
		},
	}
	alloc, err = computeAllocSize(cases4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3*proto.PageSize), alloc)

	cases5 := []*proto.DataSource{
		{
			FileOffset: proto.CACHE_BLOCK_SIZE * 128,
			Size_:      1024,
		},
		{
			FileOffset: proto.CACHE_BLOCK_SIZE*128 + 1024,
			Size_:      4096,
		},
		{
			FileOffset: proto.CACHE_BLOCK_SIZE*128 + 5120,
			Size_:      4096,
		},
	}
	alloc, err = computeAllocSize(cases5)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3*proto.PageSize), alloc)

	alloc, err = computeAllocSize(generateSparseSources(1024))
	assert.ErrorContains(t, err, SparseFileError.Error())

	alloc, err = computeAllocSize(generateSources(1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(proto.PageSize), alloc)

	alloc, err = computeAllocSize(generateSources(4 * 1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(4*1024), alloc)

	alloc, err = computeAllocSize(generateSources(128 * 1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(128*1024), alloc)

	alloc, err = computeAllocSize(generateSparseSources(128 * 1024))
	assert.ErrorContains(t, err, SparseFileError.Error())

	alloc, err = computeAllocSize(generateSources(1024 * 1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1024*1024), alloc)

}

func generateSources(fileSize int64) (sources []*proto.DataSource) {
	sources = make([]*proto.DataSource, 0)
	var bufSlice []int
	if fileSize > proto.PageSize {
		bufSlice = []int{1, 4, 16, 64, 128, 512, 1024, proto.PageSize, 16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, proto.CACHE_BLOCK_SIZE}
	} else {
		bufSlice = []int{1, 4, 16, 64, 128}
	}
	var offset int64
	//init test data
	for {
		if offset >= fileSize {
			break
		}
		rand.New(rand.NewSource(time.Now().UnixNano()))
		index := rand.Intn(len(bufSlice))
		if int64(bufSlice[index]) >= fileSize {
			continue
		}
		size := int(math.Min(float64(fileSize-offset), float64(bufSlice[index])))
		sources = append(sources, &proto.DataSource{
			Size_:      uint64(size),
			FileOffset: uint64(offset),
		})
		offset += int64(size)
	}
	return
}

func generateSparseSources(fileSize int64) (sources []*proto.DataSource) {
	for i := 0; i < 1000; i++ {
		originSources := generateSources(fileSize)
		sources = make([]*proto.DataSource, 0)
		for j, s := range originSources {
			if j%2 == 0 {
				sources = append(sources, s)
			}
		}
		if len(sources) >= 2 {
			break
		}
	}
	if len(sources) < 2 {
		return make([]*proto.DataSource, 0)
	}
	return
}
