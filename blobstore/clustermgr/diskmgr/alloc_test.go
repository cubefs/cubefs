// Copyright 2022 The CubeFS Authors.
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

package diskmgr

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/mock"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDiskMgrConfig = DiskMgrConfig{
	RefreshIntervalS:         1000000,
	RackAware:                false,
	HostAware:                true,
	IDC:                      []string{"z0", "z1", "z2"},
	HeartbeatExpireIntervalS: 60,
	FlushIntervalS:           300,
	ChunkSize:                17179869184, // 16G
	CodeModes:                []codemode.CodeMode{codemode.EC15P12, codemode.EC6P6},
}

var (
	defaultRetrySleepIntervalS time.Duration = 2
	testMockScopeMgr           *mock.MockScopeMgrAPI
	testMockBlobNode           *mocks.MockStorageAPI
	testIdcs                   = []string{"z0", "z1", "z2"}
	hostPrefix                 = "test-host-"
)

func initTestDiskMgr(t *testing.T) (d *DiskMgr, closeFunc func()) {
	var err error
	testTmpDBPath := "/tmp/tmpdiskmgrnormaldb" + strconv.Itoa(rand.Intn(10000000000))
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath, false)
	assert.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testMockScopeMgr = mock.NewMockScopeMgrAPI(ctrl)

	testDiskMgr, err := New(testMockScopeMgr, testDB, testDiskMgrConfig)
	if err != nil {
		t.Log(errors.Detail(err))
	}
	testMockBlobNode = mocks.NewMockStorageAPI(ctrl)
	testDiskMgr.blobNodeClient = testMockBlobNode

	assert.NoError(t, err)
	return testDiskMgr, func() {
		testDB.Close()
		os.RemoveAll(testTmpDBPath)
	}
}

func initTestDiskMgrDisks(t *testing.T, testDiskMgr *DiskMgr, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := blobnode.DiskInfo{
		DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
			Used:         0,
			Size:         14.5 * 1024 * 1024 * 1024 * 1024,
			Free:         14.5 * 1024 * 1024 * 1024 * 1024,
			MaxChunkCnt:  14.5 * 1024 / 16,
			FreeChunkCnt: 14.5 * 1024 / 16,
		},
		ClusterID: proto.ClusterID(1),
		Idc:       "z0",
		Status:    proto.DiskStatusNormal,
		Readonly:  false,
	}
	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			diskInfo.DiskID = proto.DiskID(idx*10000 + i)
			hostID := i / 60
			diskInfo.Rack = strconv.Itoa(hostID)
			diskInfo.Host = idc + hostPrefix + strconv.Itoa(hostID)
			diskInfo.Idc = idc
			err := testDiskMgr.addDisk(ctx, diskInfo)
			require.NoError(t, err)
		}
	}
}

func TestAlloc(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.HeartbeatExpireIntervalS = 6000

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// disable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestDiskMgrDisks(t, testDiskMgr, 1, 300, testIdcs...)

		// refresh cluster's disk space allocator
		testDiskMgr.refresh(ctx)

		t.Log(fmt.Sprintf("all disk length: %d", len(testDiskMgr.allDisks)))
		log.SetOutputLevel(log.Ldebug)

		// alloc from not enough space, alloc should return ErrNoEnoughSpace
		for _, idc := range testIdcs {
			allocator := testDiskMgr.allocators[idc].Load().(*idcStorage)
			_, err := allocator.alloc(ctx, 9, nil)
			require.Equal(t, ErrNoEnoughSpace, err)
		}

		// alloc with diff rack
		testDiskMgr.RackAware = true
		testDiskMgr.refresh(ctx)
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		_, err := allocator.alloc(ctx, 9, nil)
		require.Equal(t, ErrNoEnoughSpace, err)
	}

	// enable same host alloc, no error will return
	// refresh cluster's disk space allocator when change HostAware
	{
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-same-host")
		testDiskMgr.HostAware = false
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		ret, err := allocator.alloc(ctx, 9, nil)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))
	}

	// insert more disk and disable same host
	// alloc should be successful
	{
		initTestDiskMgrDisks(t, testDiskMgr, 301, 539, testIdcs[0])
		// refresh cluster's disk space allocator
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-enough-space")
		testDiskMgr.HostAware = true
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)
		// alloc from enough space
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		ret, err := allocator.alloc(ctx, 9, nil)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))

		// alloc with diff rack
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-diff-race")
		testDiskMgr.RackAware = true
		testDiskMgr.refresh(ctx)
		allocator = testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		ret, err = allocator.alloc(ctx, 9, nil)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))

	}

	// test diskMgr AllocChunks
	{

		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-chunk")
		testDiskMgr.HostAware = true
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)

		testMockBlobNode.EXPECT().CreateChunk(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(100000).Return(nil)
		diskIDs, err := testDiskMgr.AllocChunks(ctx, &AllocPolicy{
			Idc:      testIdcs[0],
			Vuids:    []proto.Vuid{proto.EncodeVuid(1, 1)},
			Excludes: []proto.DiskID{1},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskIDs))
		require.NotEqual(t, proto.DiskID(1), diskIDs[0])

		// alloc with exclude all, should return no enough space
		_, err = testDiskMgr.AllocChunks(ctx, &AllocPolicy{
			Idc:      testIdcs[0],
			Vuids:    []proto.Vuid{proto.EncodeVuid(1, 2)},
			Excludes: []proto.DiskID{1, 61, 121, 181, 241, 301, 361, 421, 481},
		})
		require.Equal(t, ErrNoEnoughSpace, err)

		vuids := make([]proto.Vuid, 0)
		for i := 1; i <= 9; i++ {
			vuids = append(vuids, proto.EncodeVuid(1, uint32(i)))
		}
		diskIDs, err = testDiskMgr.AllocChunks(ctx, &AllocPolicy{
			Idc:   testIdcs[0],
			Vuids: vuids,
		})
		require.NoError(t, err)
		require.Equal(t, 9, len(diskIDs))
	}
}

func TestAllocWithSameHost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-same-host-not-enough")

	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs...)
		testDiskMgr.HostAware = false
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)

		t.Log(fmt.Sprintf("all disk length: %d", len(testDiskMgr.allDisks)))
		log.SetOutputLevel(log.Ldebug)

		// alloc from not enough space, alloc should return ErrNoEnoughSpace
		for _, idc := range testIdcs {
			allocator := testDiskMgr.allocators[idc].Load().(*idcStorage)
			_, err := allocator.alloc(ctx, 11, nil)
			require.Equal(t, ErrNoEnoughSpace, err)
		}
	}

	// enable same host, insert enough disk, no error will return
	{
		initTestDiskMgrDisks(t, testDiskMgr, 11, 12, testIdcs...)
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-same-host-not-enough")
		testDiskMgr.refresh(ctx)
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		ret, err := allocator.alloc(ctx, 12, nil)
		require.NoError(t, err)
		require.Equal(t, 12, len(ret))
		t.Log(ret)
	}

	// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
	{
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 12; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 12, nil)
			require.NoError(t, err)
			require.Equal(t, 12, len(diskIDs))
		}

		// alloc exceed available free chunk, error should be return
		_, err := allocator.alloc(ctx, 1, nil)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}

	// reset all data node(6) free chunk into 10, and alloc for 10 times, exclude 1-5, should be successful in this situation
	// allocated disk id should always be 6
	{
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 6; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
				1: testDiskMgr.allDisks[1],
				2: testDiskMgr.allDisks[1],
				3: testDiskMgr.allDisks[1],
				4: testDiskMgr.allDisks[1],
				5: testDiskMgr.allDisks[1],
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(diskIDs))
			require.Equal(t, proto.DiskID(6), diskIDs[0])
		}
		_, err := allocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1: testDiskMgr.allDisks[1],
			2: testDiskMgr.allDisks[1],
			3: testDiskMgr.allDisks[1],
			4: testDiskMgr.allDisks[1],
			5: testDiskMgr.allDisks[1],
		})
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocWithDiffRack(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-diff-rack-enough-host")

	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])

		// 1-8 use test-rack-[1-8]
		// 9-10 use same rack: test-rack-8
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 8; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.Host = "test-host-" + strconv.Itoa(i)
			diskItem.info.Rack = "test-rack-" + strconv.Itoa(i)
			diskItem.lock.Unlock()
		}
		for i := 9; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.Host = "test-host-" + strconv.Itoa(i)
			diskItem.info.Rack = "test-rack-8"
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.HostAware = true
		testDiskMgr.RackAware = true
		testDiskMgr.refresh(ctx)
		log.SetOutputLevel(log.Ldebug)
		// alloc from not enough rack, but enough data node, it should be successful
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		diskIDs, err := allocator.alloc(ctx, 10, nil)
		require.NoError(t, err)
		require.Equal(t, 10, len(diskIDs))

		// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocator = testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 10, nil)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = allocator.alloc(ctx, 1, nil)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocWithDiffHost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-diff-host")
	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])

		// 1-8 use test-rack-[1-8]
		// 9-10 use same rack: test-rack-8
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.Host = "test-host-" + strconv.Itoa(i)
			diskItem.info.Rack = "test-rack-" + strconv.Itoa(i)
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.HostAware = true
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)
		log.SetOutputLevel(log.Ldebug)
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		diskIDs, err := allocator.alloc(ctx, 10, nil)
		require.NoError(t, err)
		require.Equal(t, 10, len(diskIDs))

		// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocator = testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 10, nil)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = allocator.alloc(ctx, 1, nil)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocWithDiffRackAndSameHost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-diff-host")
	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])

		// 1-8 use test-rack-[1-8]
		// 9-10 use same rack: test-rack-8
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 8; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.Host = "test-host-" + strconv.Itoa(i)
			diskItem.info.Rack = "test-rack-" + strconv.Itoa(i)
			diskItem.lock.Unlock()
		}
		for i := 9; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.Host = "test-host-" + strconv.Itoa(i)
			diskItem.info.Rack = "test-rack-8"
			diskItem.lock.Unlock()
		}

		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.HostAware = false
		testDiskMgr.RackAware = true
		testDiskMgr.refresh(ctx)
		log.SetOutputLevel(log.Ldebug)
		allocator := testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		diskIDs, err := allocator.alloc(ctx, 10, nil)
		require.NoError(t, err)
		require.Equal(t, 10, len(diskIDs))

		// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			diskItem.info.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocator = testDiskMgr.allocators[testIdcs[0]].Load().(*idcStorage)
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 10, nil)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = allocator.alloc(ctx, 1, nil)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocCost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	var (
		_, ctx      = trace.StartSpanFromContext(context.Background(), "")
		concurrency = 10
		totalTimes  = 1 * 100
	)
	log.SetOutputLevel(log.Linfo)

	initTestDiskMgrDisks(t, testDiskMgr, 1, 18000, testIdcs[0])
	// refresh cluster's disk space allocator
	testDiskMgr.refresh(ctx)
	allocator := testDiskMgr.allocators["z0"].Load().(*idcStorage)

	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i <= concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < totalTimes/concurrency; j++ {
				allocator.alloc(ctx, 9, nil)
			}
		}()
	}
	wg.Wait()
	t.Log("op cost:", time.Since(start)/time.Duration(totalTimes))
}
