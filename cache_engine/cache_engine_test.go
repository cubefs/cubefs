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
	ce, err := NewCacheEngine(testTmpFS, 200*util.MB, DefaultCacheMaxUsedRatio, 1024, DefaultExpireTime, nil, func(volume string, action int, size uint64) {})
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
	ce, err := NewCacheEngine(testTmpFS, unit.GB, 1.1, 1024, DefaultExpireTime, nil, func(volume string, action int, size uint64) {})
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
	ce, err := NewCacheEngine(testTmpFS, unit.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, nil, func(volume string, action int, size uint64) {})
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
	ce, err := NewCacheEngine(testTmpFS, unit.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, nil, func(volume string, action int, size uint64) {})
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
