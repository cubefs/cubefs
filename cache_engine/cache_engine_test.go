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
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"github.com/tiglabs/raft/util"
	"golang.org/x/net/context"
	"sync"
	"testing"
	"time"
)

var bytesCommon = randTestData(1024)

func TestCacheEngine(t *testing.T) {
	ce, err := NewCacheEngine(testTmpFS, 200*util.MB, DefaultCacheMaxUsedRatio, 1024, DefaultExpireTime, nil, func(volume string, action int) *statistics.TpObject { return nil })
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, ce.Stop())
	}()
	var cb *CacheBlock
	inode := uint64(1)
	fixedOffset := uint64(1024)
	version := uint32(112358796)
	cb, err = ce.createCacheBlock(t.Name(), inode, fixedOffset, version, DefaultExpireTime, proto.CACHE_BLOCK_SIZE)
	assert.Nil(t, err)
	assert.Nil(t, cb.WriteAt(bytesCommon, 0, 1024))
}

func TestOverFlow(t *testing.T) {
	var isNoSpace bool
	var isErr bool
	ce, err := NewCacheEngine(testTmpFS, unit.GB, 1.1, 1024, DefaultExpireTime, nil, func(volume string, action int) *statistics.TpObject { return nil })
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, ce.Stop())
	}()
	var index int
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	for {
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func(round int, thread int) {
				defer wg.Done()
				var cb *CacheBlock
				var offset int64
				var err1 error
				inode := uint64(1)
				fixedOffset := uint64(1024)
				version := uint32(112358796)
				cb, err1 = ce.createCacheBlock(fmt.Sprintf("%s_%d_%d", t.Name(), round, thread), inode, fixedOffset, version, DefaultExpireTime, proto.CACHE_BLOCK_SIZE)
				if err1 != nil {
					isErr = true
					t.Error(err1)
					return
				}
				for {
					if offset+1024 > proto.CACHE_BLOCK_SIZE {
						break
					}
					err1 = cb.WriteAt(bytesCommon, offset, 1024)
					if err1 != nil {
						assert.NotContains(t, err1.Error(), proto.ErrTmpfsNoSpace.Error())
					}
					assert.Nil(t, err1)
					offset += 1024
				}
			}(index, j)
		}
		wg.Wait()
		index++
		time.Sleep(time.Millisecond * 500)
		assert.False(t, isNoSpace)
		assert.LessOrEqual(t, ce.usedSize(), ce.config.Total)
		assert.False(t, isErr)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
			break
		default:
		}
		if ce.usedSize() == ce.config.Total {
			break
		}
		t.Logf("index:%d, storeSize:%d, usedSize:%d", index, ce.config.Total, ce.usedSize())
	}
}

func TestTTL(t *testing.T) {
	var ttl int64
	ttl = int64(10)
	lruCap := 10
	inode := uint64(1)
	fixedOffset := uint64(1024)
	version := uint32(112358796)
	ce, err := NewCacheEngine(testTmpFS, unit.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, nil, func(volume string, action int) *statistics.TpObject { return nil })
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ce.Stop()
		if err != nil {
			t.Error(err)
		}
	}()
	wg := sync.WaitGroup{}
	for j := 0; j < lruCap; j++ {
		wg.Add(1)
		go func(index int) {
			var cb *CacheBlock
			var offset int64
			defer wg.Done()
			cb, err = ce.createCacheBlock(fmt.Sprintf("%s_%d", t.Name(), index), inode, fixedOffset, version, ttl, proto.CACHE_BLOCK_SIZE)
			if err != nil {
				t.Error(err)
				return
			}
			for {
				err = cb.WriteAt(bytesCommon, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
			}
			time.Sleep(time.Duration(ttl/2) * time.Second)
			_, err = ce.GetCacheBlockForRead(fmt.Sprintf("%s_%d", t.Name(), index), inode, fixedOffset, version, 0)
			if err != nil {
				t.Errorf("test[%v] expect get cacheBlock[%v] success, but error:%v", t.Name(), cb.blockKey, err)
				return
			}
		}(j)
	}
	wg.Wait()

	//waiting all elements in lruCache expired
	time.Sleep(time.Duration(ttl/2) * time.Second)
	//get foot expired key, it won't be moved to front
	_, err = ce.GetCacheBlockForRead(fmt.Sprintf("%s_%d", t.Name(), lruCap-1), inode, fixedOffset, version, 0)
	if err == nil {
		t.Errorf("test[%v] expect get cacheBlock[%v] fail, but success, lruCacheCap(%v) lruCacheLen(%v)", t.Name(), GenCacheBlockKey(fmt.Sprintf("%s_%d", t.Name(), lruCap-1), inode, fixedOffset, version), lruCap, ce.lruCache.Len())
		return
	}
}

func TestLru(t *testing.T) {
	lruCap := 10
	ce, err := NewCacheEngine(testTmpFS, unit.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, nil, func(volume string, action int) *statistics.TpObject { return nil })
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ce.Stop()
		if err != nil {
			t.Error(err)
		}
	}()
	for j := 0; j < 20; j++ {
		func(index int) {
			var cb *CacheBlock
			var offset int64
			inode := uint64(1)
			fixedOffset := uint64(1024)
			version := uint32(112358796)
			cb, err = ce.createCacheBlock(fmt.Sprintf("%s_%d", t.Name(), index), inode, fixedOffset, version, DefaultExpireTime, proto.CACHE_BLOCK_SIZE)
			if err != nil {
				t.Error(err)
				return
			}
			for {
				err = cb.WriteAt(bytesCommon, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
			}
		}(j)
		if ce.lruCache.Len() > lruCap {
			t.Errorf("lru cache overflow, expect:%v, actrual:%v", lruCap, ce.lruCache.Len())
			return
		}
	}
}

func TestSparseFile(t *testing.T) {
	t.Skipf("Case seems illgal and need to be update") // TODO: 该测试用例有问题，需要调整。
	lruCap := 10
	rawData := make([]byte, 0)
	for i := 0; uint64(i) < 128*unit.KB; i++ {
		rawData = append(rawData, 'b')
	}
	var readSourceFunc = func(source *proto.DataSource, w func(data []byte, off, size int64) error) (readBytes int, err error) {
		err = w(rawData, int64(source.FileOffset)&(proto.CACHE_BLOCK_SIZE-1), int64(source.Size_))
		if err != nil {
			return
		}
		return int(source.Size_), nil
	}
	ce, err := NewCacheEngine(testTmpFS, unit.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, readSourceFunc, func(volume string, action int) *statistics.TpObject { return nil })
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ce.Stop()
		if err != nil {
			t.Error(err)
		}
	}()
	offset1 := uint64(proto.CACHE_BLOCK_SIZE*2 - 256*unit.KB)
	size1 := uint64(128 * unit.KB)
	offset2 := uint64(proto.CACHE_BLOCK_SIZE*2 - 256*unit.KB)
	size2 := uint64(128 * unit.KB)
	sources := []*proto.DataSource{
		{
			FileOffset:   offset1,
			PartitionID:  1,
			ExtentID:     1,
			ExtentOffset: 0,
			Size_:        size1,
		},
		{
			FileOffset:   offset2,
			PartitionID:  2,
			ExtentID:     2,
			ExtentOffset: 0,
			Size_:        size2,
		},
	}
	alloc, err := computeAllocSize(sources)
	assert.NoError(t, err)
	cb, err := ce.createCacheBlock(fmt.Sprintf("%s_%s", t.Name(), "sparse"), 1, 0, 0, DefaultExpireTime, alloc)
	if assert.Error(t, err) {
		return
	}
	for _, source := range sources {
		_, err = cb.readSource(source, cb.WriteAt)
		if assert.Error(t, err) {
			return
		}
	}
	reader := make([]byte, unit.MB)
	n, err := cb.Read(context.Background(), reader, 0, int64(offset2&(proto.CACHE_BLOCK_SIZE-1)+size2))
	if assert.Error(t, err) {
		return
	}
	if assert.NotEqual(t, n, offset2&(proto.CACHE_BLOCK_SIZE-1)+size2) {
		return
	}

	for _, source := range sources {
		reader1 := make([]byte, source.Size_)
		n, err = cb.Read(context.Background(), reader, int64(source.FileOffset&(proto.CACHE_BLOCK_SIZE-1)), int64(source.Size_))
		if assert.Error(t, err) {
			return
		}
		if assert.NotEqual(t, n, source.Size_) {
			return
		}
		if assert.NotEqual(t, reader1, rawData) {
			return
		}
	}
}
