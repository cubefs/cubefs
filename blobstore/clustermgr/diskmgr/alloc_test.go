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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/mock"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"
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
	CopySetConfigs:           make(map[proto.NodeRole]map[proto.DiskType]CopySetConfig),
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
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testMockScopeMgr = mock.NewMockScopeMgrAPI(ctrl)
	testDiskMgrConfig.CopySetConfigs[proto.NodeRoleBlobNode] = make(map[proto.DiskType]CopySetConfig)
	testDiskMgrConfig.CopySetConfigs[proto.NodeRoleBlobNode][proto.DiskTypeHDD] = CopySetConfig{
		NodeSetCap:                108,
		NodeSetIdcCap:             36,
		NodeSetRackCap:            6,
		DiskSetCap:                2160,
		DiskCountPerNodeInDiskSet: 20,
	}

	testDiskMgr, err := New(testMockScopeMgr, testDB, testDiskMgrConfig)
	if err != nil {
		t.Log(errors.Detail(err))
	}
	testMockBlobNode = mocks.NewMockStorageAPI(ctrl)
	testDiskMgr.blobNodeClient = testMockBlobNode

	require.NoError(t, err)
	return testDiskMgr, func() {
		testDB.Close()
		os.RemoveAll(testTmpDBPath)
	}
}

func initTestDiskMgrDisks(t *testing.T, testDiskMgr *DiskMgr, start, end int, specifyNodeID bool, idcs ...string) {
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
			hostID := i/60 + 1
			if specifyNodeID {
				hostID = i
			}
			diskInfo.NodeID = proto.NodeID(idx*10000 + hostID)
			diskInfo.Rack = strconv.Itoa(hostID)
			diskInfo.Host = idc + hostPrefix + strconv.Itoa(hostID)
			diskInfo.Idc = idc

			newDiskInfo := diskInfo
			err := testDiskMgr.addDisk(ctx, &newDiskInfo)
			require.NoError(t, err)
		}
	}
}

func initTestDiskMgrDisksWithReadonly(t *testing.T, testDiskMgr *DiskMgr, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := &blobnode.DiskInfo{
		DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
			Used:         0,
			Size:         1024,
			Free:         1024,
			MaxChunkCnt:  1024 / 16,
			FreeChunkCnt: 1024 / 16,
		},
		ClusterID: proto.ClusterID(1),
		Idc:       "z0",
		Status:    proto.DiskStatusNormal,
		Readonly:  false,
	}
	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			diskInfo.DiskID = proto.DiskID(idx*10000 + i)
			hostID := i/60 + 1
			diskInfo.NodeID = proto.NodeID(idx*10000 + hostID)
			diskInfo.Rack = strconv.Itoa(hostID)
			diskInfo.Host = idc + hostPrefix + strconv.Itoa(hostID)
			diskInfo.Idc = idc
			if i%2 == 0 {
				diskInfo.Readonly = true
			} else {
				diskInfo.Readonly = false
			}
			err := testDiskMgr.addDisk(ctx, diskInfo)
			require.NoError(t, err)
		}
	}
}

func initTestDiskMgrNodes(t *testing.T, testDiskMgr *DiskMgr, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	nodeInfo := blobnode.NodeInfo{
		ClusterID: proto.ClusterID(1),
		DiskType:  proto.DiskTypeHDD,
		Role:      proto.NodeRoleBlobNode,
		Status:    proto.NodeStatusNormal,
	}
	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			nodeInfo.NodeID = proto.NodeID(idx*10000 + i)
			nodeInfo.Rack = strconv.Itoa(i)
			nodeInfo.Host = idc + hostPrefix + strconv.Itoa(i)
			nodeInfo.Idc = idc

			newNodeInfo := nodeInfo
			err := testDiskMgr.addNode(ctx, &newNodeInfo)
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
		initTestDiskMgrNodes(t, testDiskMgr, 1, 6, testIdcs...)
		initTestDiskMgrDisks(t, testDiskMgr, 1, 300, false, testIdcs...)

		// refresh cluster's disk space allocator
		testDiskMgr.refresh(ctx)

		t.Logf("all disk length: %d", len(testDiskMgr.allDisks))

		// alloc from not enough space, alloc should return ErrNoEnoughSpace
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		for _, idc := range testIdcs {
			idcAllocator := idcAllocators[idc]
			_, err := idcAllocator.alloc(ctx, 9, nil)
			require.Equal(t, ErrNoEnoughSpace, err)
		}

		// alloc with diff rack
		testDiskMgr.RackAware = true
		testDiskMgr.refresh(ctx)
		allocators = testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
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
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		ret, err := allocator.alloc(ctx, 9, nil)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))
	}

	// insert more disk and disable same host
	// alloc should be successful
	{
		initTestDiskMgrNodes(t, testDiskMgr, 6, 10, testIdcs[0])
		initTestDiskMgrDisks(t, testDiskMgr, 301, 539, false, testIdcs[0])
		// refresh cluster's disk space allocator
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-enough-space")
		testDiskMgr.HostAware = true
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		// alloc from enough space
		idcAllocator := idcAllocators[testIdcs[0]]
		ret, err := idcAllocator.alloc(ctx, 9, nil)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))

		// alloc with diff rack
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-diff-race")
		testDiskMgr.RackAware = true
		testDiskMgr.refresh(ctx)
		allocators = testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		ret, err = idcAllocator.alloc(ctx, 9, nil)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))

	}

	// test diskMgr AllocChunks
	{

		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-chunk")
		testDiskMgr.HostAware = true
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)

		// testMockBlobNode.EXPECT().CreateChunk(gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(100000).Return(nil)
		testMockBlobNode.EXPECT().CreateChunk(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, host string, args *blobnode.CreateChunkArgs) (err error) {
				if args.Vuid%2 != 0 {
					return ErrBlobNodeCreateChunkFailed
				}
				return nil
			})
		diskIDs, _, err := testDiskMgr.AllocChunks(ctx, AllocPolicy{
			DiskType:  proto.DiskTypeHDD,
			CodeMode:  codemode.EC6P3,
			Idc:       testIdcs[0],
			Vuids:     []proto.Vuid{proto.EncodeVuid(1, 1)},
			Excludes:  []proto.DiskID{1},
			DiskSetID: ecDiskSetID,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskIDs))
		require.NotEqual(t, proto.DiskID(1), diskIDs[0])

		// alloc with exclude all, should return no enough space
		_, _, err = testDiskMgr.AllocChunks(ctx, AllocPolicy{
			DiskType:  proto.DiskTypeHDD,
			CodeMode:  codemode.EC6P3,
			Idc:       testIdcs[0],
			Vuids:     []proto.Vuid{proto.EncodeVuid(1, 2)},
			Excludes:  []proto.DiskID{1, 61, 121, 181, 241, 301, 361, 421, 481},
			DiskSetID: ecDiskSetID,
		})
		require.Equal(t, ErrNoEnoughSpace, err)

		vuids := make([]proto.Vuid, 0)
		for i := 1; i <= 9; i++ {
			_vuid, _ := proto.NewVuid(101, uint8(i), 1)
			vuids = append(vuids, _vuid)
		}
		diskIDs, _vuids, err := testDiskMgr.AllocChunks(ctx, AllocPolicy{
			DiskType:   proto.DiskTypeHDD,
			CodeMode:   codemode.EC6P3,
			Vuids:      vuids,
			RetryTimes: 3,
		})
		require.NoError(t, err)
		require.Equal(t, 9, len(diskIDs))
		require.Equal(t, 9, len(_vuids))

		vuids1 := make([]proto.Vuid, 0)
		for i := 1; i <= 3; i++ {
			_vuid, _ := proto.NewVuid(101, uint8(i), 1)
			vuids1 = append(vuids1, _vuid)
		}
		diskIDs, _, err = testDiskMgr.AllocChunks(ctx, AllocPolicy{
			DiskType:   proto.DiskTypeHDD,
			CodeMode:   codemode.Replica3,
			Vuids:      vuids1,
			RetryTimes: 3,
		})
		require.NoError(t, err)
		require.Equal(t, 3, len(diskIDs))

		vuids2 := make([]proto.Vuid, 0)
		for i := 1; i <= 3; i++ {
			_vuid, _ := proto.NewVuid(101, uint8(i), 1)
			vuids2 = append(vuids2, _vuid)
		}
		diskIDs, newVuids, err := testDiskMgr.AllocChunks(ctx, AllocPolicy{
			DiskType:   proto.DiskTypeHDD,
			CodeMode:   codemode.Replica3,
			Vuids:      vuids2,
			RetryTimes: 3,
		})
		for i, vuid := range vuids2 {
			if vuid%2 != 0 {
				require.NotEqual(t, vuid, newVuids[i])
			}
		}
		require.NoError(t, err)
		require.Equal(t, 3, len(diskIDs))
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
		initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs...)
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs...)
		testDiskMgr.HostAware = false
		testDiskMgr.RackAware = false
		testDiskMgr.refresh(ctx)

		t.Logf("all disk length: %d", len(testDiskMgr.allDisks))

		// alloc from not enough space, alloc should return ErrNoEnoughSpace
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		for _, idc := range testIdcs {
			allocator := idcAllocators[idc]
			_, err := allocator.alloc(ctx, 11, nil)
			require.Equal(t, ErrNoEnoughSpace, err)
		}
	}

	// enable same host, insert enough disk, no error will return
	{
		initTestDiskMgrNodes(t, testDiskMgr, 2, 2, testIdcs...)
		initTestDiskMgrDisks(t, testDiskMgr, 11, 12, false, testIdcs...)
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-same-host-not-enough")
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
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
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
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
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
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
		initTestDiskMgrNodes(t, testDiskMgr, 1, 10, testIdcs[0])
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, true, testIdcs[0])

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
		// alloc from not enough rack, but enough data node, it should be successful
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator := idcAllocators[testIdcs[0]]
		diskIDs, err := idcAllocator.alloc(ctx, 10, nil)
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
		allocators = testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := idcAllocator.alloc(ctx, 10, nil)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = idcAllocator.alloc(ctx, 1, nil)
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
		initTestDiskMgrNodes(t, testDiskMgr, 1, 10, testIdcs[0])
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, true, testIdcs[0])

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
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator := idcAllocators[testIdcs[0]]
		diskIDs, err := idcAllocator.alloc(ctx, 10, nil)
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
		allocators = testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := idcAllocator.alloc(ctx, 10, nil)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = idcAllocator.alloc(ctx, 1, nil)
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
		initTestDiskMgrNodes(t, testDiskMgr, 1, 10, testIdcs[0])
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, true, testIdcs[0])

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
		allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator := idcAllocators[testIdcs[0]]
		diskIDs, err := idcAllocator.alloc(ctx, 10, nil)
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
		allocators = testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := idcAllocator.alloc(ctx, 10, nil)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = idcAllocator.alloc(ctx, 1, nil)
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

	initTestDiskMgrNodes(t, testDiskMgr, 1, 300, testIdcs[0])
	initTestDiskMgrDisks(t, testDiskMgr, 1, 1800, false, testIdcs[0])
	// refresh cluster's disk space allocator
	testDiskMgr.refresh(ctx)
	allocators := testDiskMgr.allocators[proto.NodeRoleBlobNode].Load().(*allocator)
	idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
	allocator := idcAllocators["z0"]

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
