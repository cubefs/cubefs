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
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

const (
	testTmpFS = "/cfs_test/tmpfs"
)

var bytesCommon = randTestData(1024)

func randTestData(size int) (data []byte) {
	data = make([]byte, size)
	rand.Read(data)
	return
}

func TestEngineNew(t *testing.T) {
	ce, err := NewCacheEngine(testTmpFS, 200*util.MB, DefaultCacheMaxUsedRatio, 1024, DefaultExpireTime, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ce.Stop()) }()
	var cb *CacheBlock
	inode, fixedOffset, version := uint64(1), uint64(1024), uint32(112358796)
	cb, err = ce.createCacheBlock(t.Name(), inode, fixedOffset, version, DefaultExpireTime, proto.CACHE_BLOCK_SIZE)
	require.NoError(t, err)
	require.NoError(t, cb.WriteAt(bytesCommon, 0, 1024))
}

func TestEngineOverFlow(t *testing.T) {
	ce, err := NewCacheEngine(testTmpFS, util.GB, 1.1, 1024, DefaultExpireTime, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ce.Stop()) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	var index int
	var wg sync.WaitGroup
	var isErr atomic.Value
	isErr.Store(false)
	for {
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func(round int, thread int) {
				defer wg.Done()

				inode, fixedOffset, version := uint64(1), uint64(1024), uint32(112358796)
				cb, err1 := ce.createCacheBlock(fmt.Sprintf("%s_%d_%d", t.Name(), round, thread),
					inode, fixedOffset, version, DefaultExpireTime, proto.CACHE_BLOCK_SIZE)
				if err1 != nil {
					isErr.Store(true)
					return
				}

				var offset int64
				for {
					if offset+1024 > proto.CACHE_BLOCK_SIZE {
						break
					}
					err1 = cb.WriteAt(bytesCommon, offset, 1024)
					if err1 != nil {
						require.NotContains(t, err1.Error(), proto.ErrTmpfsNoSpace.Error())
					}
					require.NoError(t, err1)
					offset += 1024
				}
			}(index, j)
		}
		wg.Wait()
		index++
		time.Sleep(time.Millisecond * 500)
		require.LessOrEqual(t, ce.usedSize(), ce.config.Total)
		require.False(t, isErr.Load().(bool))
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

func TestEngineTTL(t *testing.T) {
	lruCap := 10
	inode, fixedOffset, version := uint64(1), uint64(1024), uint32(112358796)
	ce, err := NewCacheEngine(testTmpFS, util.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, ce.Stop()) }()

	ttl := int64(2)
	var wg sync.WaitGroup
	for j := 0; j < lruCap; j++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			cb, err := ce.createCacheBlock(fmt.Sprintf("%s_%d", t.Name(), index), inode, fixedOffset, version, ttl, proto.CACHE_BLOCK_SIZE)
			require.NoError(t, err)
			var offset int64
			for {
				err = cb.WriteAt(bytesCommon, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
			}
			time.Sleep(time.Duration(ttl/2) * time.Second)
			_, err = ce.GetCacheBlockForRead(fmt.Sprintf("%s_%d", t.Name(), index), inode, fixedOffset, version, 0)
			require.NoError(t, err)
		}(j)
	}
	wg.Wait()

	t.Logf("%+v", ce.Status())
	// waiting all elements in lruCache expired
	time.Sleep(time.Duration(ttl/2) * time.Second)
	t.Logf("%+v", ce.Status())
	_, err = ce.GetCacheBlockForRead(fmt.Sprintf("%s_%d", t.Name(), lruCap-1), inode, fixedOffset, version, 0)
	require.Error(t, err, fmt.Sprintf("test[%s] expect get cacheBlock[%s] fail, but success, lruCacheCap(%d) lruCacheLen(%d)",
		t.Name(), GenCacheBlockKey(fmt.Sprintf("%s_%d", t.Name(), lruCap-1), inode, fixedOffset, version), lruCap, ce.lruCache.Len()))
}

func TestEngineLru(t *testing.T) {
	lruCap := 10
	ce, err := NewCacheEngine(testTmpFS, util.GB, DefaultCacheMaxUsedRatio, lruCap, DefaultExpireTime, nil)
	require.NoError(t, err)
	ce.Start()
	defer func() { require.NoError(t, ce.Stop()) }()

	for j := 0; j < 20; j++ {
		var cb *CacheBlock
		var offset int64
		inode, fixedOffset, version := uint64(1), uint64(1024), uint32(112358796)
		cb, err = ce.createCacheBlock(fmt.Sprintf("%s_%d", t.Name(), j), inode, fixedOffset, version, DefaultExpireTime, proto.CACHE_BLOCK_SIZE)
		require.NoError(t, err)
		for {
			err = cb.WriteAt(bytesCommon, offset, 1024)
			if err != nil {
				break
			}
			offset += 1024
		}
		require.LessOrEqual(t, ce.lruCache.Len(), lruCap)
	}
	t.Logf("%+v", ce.Status())
}
