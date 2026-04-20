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

package cluster

import (
	"context"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	mock "github.com/cubefs/cubefs/blobstore/testing/mockclustermgr"
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
	ChunkOversoldRatio:       0.5,
	ReservedSpace:            1 << 28,
	CopySetConfigs:           make(map[proto.DiskType]CopySetConfig),
}

var testShardNodeMgrConfig = DiskMgrConfig{
	RefreshIntervalS:         1000000,
	RackAware:                false,
	HostAware:                true,
	IDC:                      []string{"z0", "z1", "z2"},
	HeartbeatExpireIntervalS: 60,
	FlushIntervalS:           300,
	ShardSize:                17179869184, // 16G
	CodeModes:                []codemode.CodeMode{codemode.Replica3},
	CopySetConfigs:           make(map[proto.DiskType]CopySetConfig),
}

var (
	defaultRetrySleepIntervalS time.Duration = 2
	testMockScopeMgr           *mock.MockScopeMgrAPI
	testMockBlobNode           *mocks.MockStorageAPI
	testMockShardNode          *mock.MockShardNodeAPI
	testIdcs                   = []string{"z0", "z1", "z2"}
	hostPrefix                 = "test-host-"
)

func initTestBlobNodeMgr(t *testing.T) (d *BlobNodeManager, closeFunc func()) {
	var err error
	testTmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(10000000000))
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testMockScopeMgr = mock.NewMockScopeMgrAPI(ctrl)
	testDiskMgrConfig.CopySetConfigs = make(map[proto.DiskType]CopySetConfig)
	testDiskMgrConfig.CopySetConfigs[proto.DiskTypeHDD] = CopySetConfig{
		NodeSetCap:                108,
		NodeSetIdcCap:             36,
		NodeSetRackCap:            6,
		DiskSetCap:                2160,
		DiskCountPerNodeInDiskSet: 20,
	}

	testDiskMgr, err := NewBlobNodeMgr(testMockScopeMgr, testDB, testDiskMgrConfig)
	if err != nil {
		t.Log(errors.Detail(err))
	}
	testMockBlobNode = mocks.NewMockStorageAPI(ctrl)
	testMockRaftServer := mocks.NewMockRaftServer(ctrl)
	testMockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	testDiskMgr.blobNodeClient = testMockBlobNode
	testDiskMgr.SetRaftServer(testMockRaftServer)

	require.NoError(t, err)
	return testDiskMgr, func() {
		testDB.Close()
		os.RemoveAll(testTmpDBPath)
	}
}

func initTestBlobNodeMgrDisks(t *testing.T, testDiskMgr *BlobNodeManager, start, end int, specifyNodeID bool, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			Used:         0,
			Size:         14.5 * 1024 * 1024 * 1024 * 1024,
			Free:         14.5 * 1024 * 1024 * 1024 * 1024,
			MaxChunkCnt:  14.5 * 1024 / 16,
			FreeChunkCnt: 14.5 * 1024 / 16,
		},
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Status:    proto.DiskStatusNormal,
			Readonly:  false,
		},
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
			err := testDiskMgr.applyAddDisk(ctx, &newDiskInfo)
			require.NoError(t, err)
		}
	}
}

func initTestBlobNodeMgrDisksWithChunk(t *testing.T, testDiskMgr *BlobNodeManager, start, end, freeChunkNum int, specifyNodeID bool, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			Used:         0,
			Size:         int64(freeChunkNum) * testDiskMgrConfig.ChunkSize,
			Free:         int64(freeChunkNum) * testDiskMgrConfig.ChunkSize,
			MaxChunkCnt:  int64(freeChunkNum),
			FreeChunkCnt: int64(freeChunkNum),
		},
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Status:    proto.DiskStatusNormal,
			Readonly:  false,
		},
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
			err := testDiskMgr.applyAddDisk(ctx, &newDiskInfo)
			require.NoError(t, err)
		}
	}
}

func initTestDiskMgrDisksWithReadonly(t *testing.T, testDiskMgr *BlobNodeManager, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := &clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			Used:         0,
			Size:         1024,
			Free:         1024,
			MaxChunkCnt:  1024 / 16,
			FreeChunkCnt: 1024 / 16,
		},
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Status:    proto.DiskStatusNormal,
			Readonly:  false,
		},
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
			err := testDiskMgr.applyAddDisk(ctx, diskInfo)
			require.NoError(t, err)
		}
	}
}

func initTestBlobNodeMgrDisksWithOverSold(t *testing.T, testDiskMgr *BlobNodeManager, start, end int, specifyNodeID bool, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			Used:                 0,
			Size:                 16 * 1024 * 1024 * 1024 * 1024,
			Free:                 16 * 1024 * 1024 * 1024 * 1024,
			MaxChunkCnt:          16 * 1024 / 16,
			FreeChunkCnt:         16 * 1024 / 16,
			OversoldFreeChunkCnt: 20 * 1024 / 16,
		},
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Status:    proto.DiskStatusNormal,
			Readonly:  false,
		},
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
			err := testDiskMgr.applyAddDisk(ctx, &newDiskInfo)
			require.NoError(t, err)
		}
	}
}

func initTestBlobNodeMgrNodes(t *testing.T, testDiskMgr *BlobNodeManager, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	nodeInfo := clustermgr.NodeInfo{
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
			newNodeInfo := clustermgr.BlobNodeInfo{
				NodeInfo: nodeInfo,
			}
			err := testDiskMgr.applyAddNode(ctx, &newNodeInfo)
			require.NoError(t, err)
		}
	}
}

func initTestShardNodeMgr(t *testing.T) (d *ShardNodeManager, closeFunc func()) {
	var err error
	testTmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(10000000000))
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testMockScopeMgr = mock.NewMockScopeMgrAPI(ctrl)
	testShardNodeMgrConfig.CopySetConfigs = make(map[proto.DiskType]CopySetConfig)
	testShardNodeMgrConfig.CopySetConfigs[proto.DiskTypeNVMeSSD] = CopySetConfig{
		NodeSetCap:                18,
		NodeSetIdcCap:             6,
		NodeSetRackCap:            6,
		DiskSetCap:                36,
		DiskCountPerNodeInDiskSet: 2,
	}

	shardNodeManager, err := NewShardNodeMgr(testMockScopeMgr, testDB, testShardNodeMgrConfig)
	if err != nil {
		t.Log(errors.Detail(err))
	}
	testMockShardNode = mock.NewMockShardNodeAPI(ctrl)
	testMockRaftServer := mocks.NewMockRaftServer(ctrl)
	testMockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	shardNodeManager.SetRaftServer(testMockRaftServer)
	shardNodeManager.shardNodeClient = testMockShardNode

	require.NoError(t, err)
	return shardNodeManager, func() {
		testDB.Close()
		os.RemoveAll(testTmpDBPath)
	}
}

func initTestShardNodeMgrNodes(t *testing.T, shardNodeManager *ShardNodeManager, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	nodeInfo := clustermgr.NodeInfo{
		ClusterID: proto.ClusterID(1),
		DiskType:  proto.DiskTypeNVMeSSD,
		Role:      proto.NodeRoleShardNode,
		Status:    proto.NodeStatusNormal,
	}
	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			nodeInfo.NodeID = proto.NodeID(idx*10000 + i)
			nodeInfo.Rack = strconv.Itoa(i)
			nodeInfo.Host = idc + hostPrefix + strconv.Itoa(i)
			nodeInfo.Idc = idc
			newNodeInfo := clustermgr.ShardNodeInfo{
				NodeInfo: nodeInfo,
			}
			err := shardNodeManager.applyAddNode(ctx, &newNodeInfo)
			require.NoError(t, err)
		}
	}
}

func initTestShardNodeMgrDisks(t *testing.T, shardNodeManager *ShardNodeManager, start, end int, specifyNodeID bool, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	diskInfo := clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
			Used:         0,
			Size:         14.5 * 1024 * 1024 * 1024 * 1024,
			Free:         14.5 * 1024 * 1024 * 1024 * 1024,
			MaxShardCnt:  14.5 * 1024 / 16,
			FreeShardCnt: 14.5 * 1024 / 16,
		},
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Status:    proto.DiskStatusNormal,
			Readonly:  false,
		},
	}
	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			diskInfo.DiskID = proto.DiskID(idx*10000 + i)
			hostID := 1
			if specifyNodeID {
				hostID = (i-1)/4 + 1
			}
			diskInfo.NodeID = proto.NodeID(idx*10000 + hostID)
			diskInfo.Rack = strconv.Itoa(hostID)
			diskInfo.Host = idc + hostPrefix + strconv.Itoa(hostID)
			diskInfo.Idc = idc

			newDiskInfo := diskInfo
			err := shardNodeManager.applyAddDisk(ctx, &newDiskInfo)
			require.NoError(t, err)
		}
	}
}

func TestAlloc(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// disable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 6, testIdcs...)
		initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 300, false, testIdcs...)

		// refresh cluster's disk space allocator
		testDiskMgr.refresh(ctx)

		t.Logf("all disk length: %d", len(testDiskMgr.allDisks))

		// alloc from not enough space, alloc should return ErrNoEnoughSpace
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		for _, idc := range testIdcs {
			idcAllocator := idcAllocators[idc]
			_, err := idcAllocator.alloc(ctx, 9, nil, false)
			require.Equal(t, ErrNoEnoughSpace, err)
		}

		// alloc with diff rack
		testDiskMgr.cfg.RackAware = true
		testDiskMgr.refresh(ctx)
		allocators = testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		_, err := allocator.alloc(ctx, 9, nil, false)
		require.Equal(t, ErrNoEnoughSpace, err)
	}

	// enable same host alloc, no error will return
	// refresh cluster's disk space allocator when change HostAware
	{
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-same-host")
		testDiskMgr.cfg.HostAware = false
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		ret, err := allocator.alloc(ctx, 9, nil, false)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))
	}

	// insert more disk and disable same host
	// alloc should be successful
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 6, 10, testIdcs[0])
		initTestBlobNodeMgrDisks(t, testDiskMgr, 301, 539, false, testIdcs[0])
		// refresh cluster's disk space allocator
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-enough-space")
		testDiskMgr.cfg.HostAware = true
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		// alloc from enough space
		idcAllocator := idcAllocators[testIdcs[0]]
		ret, err := idcAllocator.alloc(ctx, 9, nil, false)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))

		// alloc with diff rack
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-diff-race")
		testDiskMgr.cfg.RackAware = true
		testDiskMgr.refresh(ctx)
		allocators = testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		ret, err = idcAllocator.alloc(ctx, 9, nil, false)
		require.NoError(t, err)
		require.Equal(t, 9, len(ret))

	}

	// test diskMgr AllocChunks
	{

		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-chunk")
		testDiskMgr.cfg.HostAware = true
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)

		testMockBlobNode.EXPECT().CreateChunk(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, host string, args *blobnode.CreateChunkArgs) (err error) {
				if args.Vuid.Epoch() == 100 {
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
			_vuid, _ := proto.NewVuid(100, uint8(i), 100)
			vuids2 = append(vuids2, _vuid)
		}

		_, _, err = testDiskMgr.AllocChunks(ctx, AllocPolicy{
			DiskType:   proto.DiskTypeHDD,
			CodeMode:   codemode.Replica3,
			Vuids:      vuids2,
			RetryTimes: 3,
		})
		require.Equal(t, ErrBlobNodeCreateChunkFailed, err)
	}
}

func TestAllocWithSameHost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-same-host-not-enough")

	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs...)
		initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs...)
		testDiskMgr.cfg.HostAware = false
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)

		t.Logf("all disk length: %d", len(testDiskMgr.allDisks))

		// alloc from not enough space, alloc should return ErrNoEnoughSpace
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		for _, idc := range testIdcs {
			allocator := idcAllocators[idc]
			_, err := allocator.alloc(ctx, 11, nil, false)
			require.Equal(t, ErrNoEnoughSpace, err)
		}
	}

	// enable same host, insert enough disk, no error will return
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 2, 2, testIdcs...)
		initTestBlobNodeMgrDisks(t, testDiskMgr, 11, 12, false, testIdcs...)
		_, ctx = trace.StartSpanFromContext(context.Background(), "alloc-same-host-not-enough")
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		ret, err := allocator.alloc(ctx, 12, nil, false)
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
			heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			heartbeatInfo.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 12, nil, false)
			require.NoError(t, err)
			require.Equal(t, 12, len(diskIDs))
		}

		// alloc exceed available free chunk, error should be return
		_, err := allocator.alloc(ctx, 1, nil, false)
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
			heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			heartbeatInfo.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := allocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
				1: testDiskMgr.allDisks[1],
				2: testDiskMgr.allDisks[1],
				3: testDiskMgr.allDisks[1],
				4: testDiskMgr.allDisks[1],
				5: testDiskMgr.allDisks[1],
			}, false)
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
		}, false)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocWithDiffRack(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-diff-rack-enough-host")

	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 10, testIdcs[0])
		initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, true, testIdcs[0])

		// 1-8 use test-rack-[1-8]
		// 9-10 use same rack: test-rack-8
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 8; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			nodeItem := testDiskMgr.allNodes[diskItem.info.NodeID]
			nodeItem.lock.Lock()
			nodeItem.info.Host = "test-host-" + strconv.Itoa(i)
			nodeItem.info.Rack = "test-rack-" + strconv.Itoa(i)
			nodeItem.lock.Unlock()
		}
		for i := 9; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			nodeItem := testDiskMgr.allNodes[diskItem.info.NodeID]
			nodeItem.lock.Lock()
			nodeItem.info.Host = "test-host-" + strconv.Itoa(i)
			nodeItem.info.Rack = "test-rack-8"
			nodeItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.cfg.HostAware = true
		testDiskMgr.cfg.RackAware = true
		testDiskMgr.refresh(ctx)
		// alloc from not enough rack, but enough data node, it should be successful
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator := idcAllocators[testIdcs[0]]
		diskIDs, err := idcAllocator.alloc(ctx, 10, nil, false)
		require.NoError(t, err)
		require.Equal(t, 10, len(diskIDs))

		// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			heartbeatInfo.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocators = testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := idcAllocator.alloc(ctx, 10, nil, false)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = idcAllocator.alloc(ctx, 1, nil, false)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocWithDiffHost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-diff-host")
	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 10, testIdcs[0])
		initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, true, testIdcs[0])

		// 1-8 use test-rack-[1-8]
		// 9-10 use same rack: test-rack-8
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			nodeItem := testDiskMgr.allNodes[diskItem.info.NodeID]
			nodeItem.lock.Lock()
			nodeItem.info.Host = "test-host-" + strconv.Itoa(i)
			nodeItem.info.Rack = "test-rack-" + strconv.Itoa(i)
			nodeItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.cfg.HostAware = true
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator := idcAllocators[testIdcs[0]]
		diskIDs, err := idcAllocator.alloc(ctx, 10, nil, false)
		require.NoError(t, err)
		require.Equal(t, 10, len(diskIDs))

		// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			heartbeatInfo.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocators = testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := idcAllocator.alloc(ctx, 10, nil, false)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = idcAllocator.alloc(ctx, 1, nil, false)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocWithDiffRackAndSameHost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	defaultRetrySleepIntervalS = 0

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-diff-host")
	// enable same host, insert not enough disk
	// alloc should return ErrNoEnoughSpace
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 10, testIdcs[0])
		initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, true, testIdcs[0])

		// 1-8 use test-rack-[1-8]
		// 9-10 use same rack: test-rack-8
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 8; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			nodeItem := testDiskMgr.allNodes[diskItem.info.NodeID]
			nodeItem.lock.Lock()
			nodeItem.info.Host = "test-host-" + strconv.Itoa(i)
			nodeItem.info.Rack = "test-rack-" + strconv.Itoa(i)
			nodeItem.lock.Unlock()
		}
		for i := 9; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			nodeItem := testDiskMgr.allNodes[diskItem.info.NodeID]
			nodeItem.lock.Lock()
			nodeItem.info.Host = "test-host-" + strconv.Itoa(i)
			nodeItem.info.Rack = "test-rack-8"
			nodeItem.lock.Unlock()
		}

		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.cfg.HostAware = false
		testDiskMgr.cfg.RackAware = true
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator := idcAllocators[testIdcs[0]]
		diskIDs, err := idcAllocator.alloc(ctx, 10, nil, false)
		require.NoError(t, err)
		require.Equal(t, 10, len(diskIDs))

		// reset all data node free chunk into 10, and alloc for 10 times, should be successful in this situation
		testDiskMgr.metaLock.RLock()
		for i := 1; i <= 10; i++ {
			diskItem := testDiskMgr.allDisks[proto.DiskID(i)]
			diskItem.lock.Lock()
			heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			heartbeatInfo.FreeChunkCnt = 10
			diskItem.lock.Unlock()
		}
		testDiskMgr.metaLock.RUnlock()
		testDiskMgr.refresh(ctx)
		defaultAllocTolerateBuff = 0
		allocators = testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		for i := 1; i <= 10; i++ {
			diskIDs, err := idcAllocator.alloc(ctx, 10, nil, false)
			require.NoError(t, err)
			require.Equal(t, 10, len(diskIDs))
		}
		// alloc exceed available free chunk, error should be return
		_, err = idcAllocator.alloc(ctx, 1, nil, false)
		require.Error(t, err)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func TestAllocCost(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()

	var (
		_, ctx      = trace.StartSpanFromContext(context.Background(), "")
		concurrency = 10
		totalTimes  = 1 * 100
	)

	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 300, testIdcs[0])
	initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 12000, false, testIdcs[0])
	// refresh cluster's disk space allocator
	testDiskMgr.cfg.HostAware = true
	testDiskMgr.cfg.RackAware = true
	testDiskMgr.refresh(ctx)
	allocators := testDiskMgr.manager.allocator.Load().(*allocator)
	idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
	allocator := idcAllocators["z0"]

	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i <= concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < totalTimes/concurrency; j++ {
				_, err := allocator.alloc(ctx, 1, nil, true)
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	t.Log("op cost:", time.Since(start)/time.Duration(totalTimes))
}

func TestShardNodeMgr_AllocShards(t *testing.T) {
	testShardNodeMgr, closeMgr := initTestShardNodeMgr(t)
	defer closeMgr()
	initTestShardNodeMgrNodes(t, testShardNodeMgr, 1, 6, testIdcs...)
	initTestShardNodeMgrDisks(t, testShardNodeMgr, 1, 24, true, testIdcs...)

	_, ctx := trace.StartSpanFromContext(context.Background(), "alloc-shards")
	testShardNodeMgr.cfg.HostAware = true
	testShardNodeMgr.cfg.RackAware = false
	testShardNodeMgr.refresh(ctx)

	testMockShardNode.EXPECT().AddShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, host string, args shardnode.AddShardArgs) (err error) {
			if args.Suid.Epoch() == 2 {
				return ErrShardNodeCreateShardFailed
			}
			return nil
		})

	// create shard normal case
	diskIDs1, excludeDiskSetID1, err := testShardNodeMgr.AllocShards(ctx, AllocShardsPolicy{
		DiskType: proto.DiskTypeNVMeSSD,
		Suids: []proto.Suid{
			proto.EncodeSuid(1, 1, 1),
			proto.EncodeSuid(1, 2, 1),
			proto.EncodeSuid(1, 3, 1),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(diskIDs1))
	require.Equal(t, nullDiskSetID, excludeDiskSetID1)

	// create shard failed case and retry
	_, excludeDiskSetID2, err := testShardNodeMgr.AllocShards(ctx, AllocShardsPolicy{
		DiskType: proto.DiskTypeNVMeSSD,
		Suids: []proto.Suid{
			proto.EncodeSuid(2, 1, 2),
			proto.EncodeSuid(2, 2, 2),
			proto.EncodeSuid(2, 3, 2),
		},
	})
	require.Error(t, err)
	require.NotEqual(t, nullDiskSetID, excludeDiskSetID2)

	diskIDs3, excludeDiskSetID3, err := testShardNodeMgr.AllocShards(ctx, AllocShardsPolicy{
		DiskType: proto.DiskTypeNVMeSSD,
		Suids: []proto.Suid{
			proto.EncodeSuid(2, 1, 3),
			proto.EncodeSuid(2, 2, 3),
			proto.EncodeSuid(2, 3, 3),
		},
		ExcludeDiskSets: []proto.DiskSetID{excludeDiskSetID1},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(diskIDs3))
	require.Equal(t, nullDiskSetID, excludeDiskSetID3)

	// repair shard normal case
	units := make([]clustermgr.ShardUnit, 0, 3)
	for i := 1; i <= 3; i++ {
		unit := clustermgr.ShardUnit{
			Suid:    proto.EncodeSuid(1, uint8(i), 1),
			DiskID:  diskIDs1[i-1],
			Learner: false,
			Host:    "test" + strconv.Itoa(i),
		}
		units = append(units, unit)
	}
	di, _ := testShardNodeMgr.getDisk(diskIDs1[0])
	diskIDs, excludeDiskSetID, err := testShardNodeMgr.AllocShards(ctx, AllocShardsPolicy{
		DiskType:     proto.DiskTypeNVMeSSD,
		Suids:        []proto.Suid{proto.EncodeSuid(1, 1, 1)},
		RepairUnits:  units,
		ExcludeDisks: diskIDs1,
		DiskSetID:    di.info.DiskSetID,
		Idc:          testIdcs[0],
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(diskIDs))
	require.Equal(t, nullDiskSetID, excludeDiskSetID)
}

func TestAllocWithBalance(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// enable same host alloc
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs...)
		initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 6, false, testIdcs...)
		testDiskMgr.cfg.HostAware = false
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		allocator := idcAllocators[testIdcs[0]]
		ret, err := allocator.alloc(ctx, 1, nil, true)
		require.NoError(t, err)
		require.Equal(t, 1, len(ret))

		// exclude disk
		defaultAllocTolerateBuff = 0
		diskIDs, err := allocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1: testDiskMgr.allDisks[1],
			2: testDiskMgr.allDisks[2],
			3: testDiskMgr.allDisks[3],
			4: testDiskMgr.allDisks[4],
			5: testDiskMgr.allDisks[5],
		}, true)
		require.NoError(t, err)
		require.Equal(t, 1, len(diskIDs))
		require.Equal(t, proto.DiskID(6), diskIDs[0])

		_, err = allocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1: testDiskMgr.allDisks[1],
			2: testDiskMgr.allDisks[2],
			3: testDiskMgr.allDisks[3],
			4: testDiskMgr.allDisks[4],
			5: testDiskMgr.allDisks[5],
			6: testDiskMgr.allDisks[6],
		}, true)
		require.Equal(t, ErrNoEnoughSpace, err)
	}

	// alloc with diff host
	{
		initTestBlobNodeMgrNodes(t, testDiskMgr, 2, 2, testIdcs...)
		initTestBlobNodeMgrDisks(t, testDiskMgr, 61, 66, false, testIdcs...)

		// refresh cluster's disk space allocator
		testDiskMgr.cfg.HostAware = true
		testDiskMgr.cfg.RackAware = false
		testDiskMgr.refresh(ctx)
		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		for _, idc := range testIdcs {
			idcAllocator := idcAllocators[idc]
			_, err := idcAllocator.alloc(ctx, 1, nil, true)
			require.NoError(t, err)
		}

		// exclude disk
		idcAllocator := idcAllocators[testIdcs[0]]
		_, err := idcAllocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1: testDiskMgr.allDisks[1],
		}, true)
		require.NoError(t, err)

		_, err = idcAllocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1:  testDiskMgr.allDisks[1],
			61: testDiskMgr.allDisks[61],
		}, true)
		require.Equal(t, ErrNoEnoughSpace, err)

		// alloc with diff rack
		testDiskMgr.cfg.RackAware = true
		testDiskMgr.refresh(ctx)
		allocators = testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators = allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		idcAllocator = idcAllocators[testIdcs[0]]
		_, err = idcAllocator.alloc(ctx, 1, nil, true)
		require.NoError(t, err)

		// exclude disk
		_, err = idcAllocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1: testDiskMgr.allDisks[1],
		}, true)
		require.NoError(t, err)

		_, err = idcAllocator.alloc(ctx, 1, map[proto.DiskID]*diskItem{
			1:  testDiskMgr.allDisks[1],
			61: testDiskMgr.allDisks[61],
		}, true)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

func BenchmarkAllocFromDiskStoragesParallel(b *testing.B) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(&testing.T{})
	defer closeTestDiskMgr()

	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	_, ctx := trace.StartSpanFromContext(context.Background(), "benchmark-parallel")
	initTestBlobNodeMgrNodes(&testing.T{}, testDiskMgr, 1, 20, testIdcs[0])
	initTestBlobNodeMgrDisks(&testing.T{}, testDiskMgr, 1, 1200, true, testIdcs[0])

	testDiskMgr.cfg.HostAware = true
	testDiskMgr.cfg.RackAware = false
	testDiskMgr.refresh(ctx)

	allocators := testDiskMgr.manager.allocator.Load().(*allocator)
	idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
	idcAllocator := idcAllocators[testIdcs[0]]

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := idcAllocator.alloc(ctx, 1, nil, true)
			if err != nil {
				b.Errorf("alloc failed: %v", err)
				return
			}
		}
	})
}

func TestAllocWithReserveChunk(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	// disk never expire
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	{
		defaultDiskReservedFreeChunk = 1
		initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 6, testIdcs...)
		initTestBlobNodeMgrDisksWithChunk(t, testDiskMgr, 1, 6, 2, true, testIdcs...)
		// refresh cluster's disk space allocator
		testDiskMgr.cfg.HostAware = true
		testDiskMgr.refresh(ctx)

		allocators := testDiskMgr.manager.allocator.Load().(*allocator)
		idcAllocators := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators
		for _, idc := range testIdcs {
			idcAllocator := idcAllocators[idc]
			_, err := idcAllocator.alloc(ctx, 6, nil, false)
			require.NoError(t, err)
		}

		allocator := idcAllocators[testIdcs[0]]
		_, err := allocator.alloc(ctx, 6, nil, false)
		require.Equal(t, ErrNoEnoughSpace, err)
	}
}

// buildECAllocator builds an allocator with only an EC nodeSet/diskSet.
// Each IDC has `nodesPerIDC` nodes, each node has `freeChunksPerNode` free chunks
// spread across a single virtual disk (simplifies weight accounting).
func buildECAllocator(idcNames []string, nodesPerIDC int, freeChunksPerNode int64, diffHost bool) *allocator {
	idcAllocs := make(map[string]*idcAllocator, len(idcNames))
	totalWeight := int64(0)
	nodeID := proto.NodeID(1)
	diskID := proto.DiskID(1)

	for _, idc := range idcNames {
		nodeStgs := make([]*nodeAllocator, 0, nodesPerIDC)
		idcWeight := int64(0)
		for n := 0; n < nodesPerIDC; n++ {
			ni := &nodeItem{nodeID: nodeID}
			nodeID++
			di := &diskItem{
				diskID: diskID,
				info: diskItemInfo{
					DiskInfo: clustermgr.DiskInfo{Idc: idc, Status: proto.DiskStatusNormal},
					extraInfo: &clustermgr.DiskHeartBeatInfo{
						MaxChunkCnt:  freeChunksPerNode * 2,
						FreeChunkCnt: freeChunksPerNode,
					},
				},
				weightGetter:   blobNodeDiskWeightGetter,
				weightDecrease: blobNodeDiskWeightDecrease,
			}
			diskID++
			ns := &nodeAllocator{
				node:   ni,
				weight: freeChunksPerNode,
				disks:  []*diskItem{di},
			}
			nodeStgs = append(nodeStgs, ns)
			idcWeight += freeChunksPerNode
		}
		allocatableCount := int64(0)
		for _, ns := range nodeStgs {
			if ns.weight > 0 {
				allocatableCount++
			}
		}
		var allDisks []*diskItem
		for _, ns := range nodeStgs {
			allDisks = append(allDisks, ns.disks...)
		}
		idcAllocs[idc] = &idcAllocator{
			idc:            idc,
			weight:         idcWeight,
			creatableNodes: allocatableCount,
			diffHost:       diffHost,
			nodeStorages:   nodeStgs,
			disks:          allDisks,
		}
		totalWeight += idcWeight
	}

	ds := newDiskSetAllocator(ecDiskSetID, totalWeight, idcAllocs)
	ns := newNodeSetAllocator(ecNodeSetID)
	ns.addDiskSet(ds)
	return newAllocator(allocatorConfig{
		nodeSets: map[proto.DiskType]nodeSetAllocatorMap{proto.DiskTypeHDD: {ecNodeSetID: ns}},
		diskSets: map[proto.DiskType]diskSetAllocatorMap{proto.DiskTypeHDD: {ecDiskSetID: ds}},
		diffHost: diffHost,
	})
}

// buildReplicateAllocator builds an allocator with a single non-EC nodeSet/diskSet,
// suitable for testing replicate mode. Each IDC gets `nodesPerIDC` nodes.
func buildReplicateAllocator(idcNames []string, nodesPerIDC int, freeChunksPerNode int64, diffHost bool) *allocator {
	const replicateNodeSetID = proto.NodeSetID(2)
	const replicateDiskSetID = proto.DiskSetID(2)

	idcAllocs := make(map[string]*idcAllocator, len(idcNames))
	totalWeight := int64(0)
	nodeID := proto.NodeID(1)
	diskID := proto.DiskID(1)

	for _, idc := range idcNames {
		nodeStgs := make([]*nodeAllocator, 0, nodesPerIDC)
		idcWeight := int64(0)
		for n := 0; n < nodesPerIDC; n++ {
			ni := &nodeItem{nodeID: nodeID}
			nodeID++
			di := &diskItem{
				diskID: diskID,
				info: diskItemInfo{
					DiskInfo: clustermgr.DiskInfo{Idc: idc, Status: proto.DiskStatusNormal},
					extraInfo: &clustermgr.DiskHeartBeatInfo{
						MaxChunkCnt:  freeChunksPerNode * 2,
						FreeChunkCnt: freeChunksPerNode,
					},
				},
				weightGetter:   blobNodeDiskWeightGetter,
				weightDecrease: blobNodeDiskWeightDecrease,
			}
			diskID++
			ns := &nodeAllocator{
				node:   ni,
				weight: freeChunksPerNode,
				disks:  []*diskItem{di},
			}
			nodeStgs = append(nodeStgs, ns)
			idcWeight += freeChunksPerNode
		}
		allocatableCount := int64(0)
		for _, ns := range nodeStgs {
			if ns.weight > 0 {
				allocatableCount++
			}
		}
		var allDisks []*diskItem
		for _, ns := range nodeStgs {
			allDisks = append(allDisks, ns.disks...)
		}
		idcAllocs[idc] = &idcAllocator{
			idc:            idc,
			weight:         idcWeight,
			creatableNodes: allocatableCount,
			diffHost:       diffHost,
			nodeStorages:   nodeStgs,
			disks:          allDisks,
		}
		totalWeight += idcWeight
	}

	ds := newDiskSetAllocator(replicateDiskSetID, totalWeight, idcAllocs)
	ns := newNodeSetAllocator(replicateNodeSetID)
	ns.addDiskSet(ds)

	tg := &mockTopoGetter{nodeNum: nodesPerIDC * len(idcNames)}

	return newAllocator(allocatorConfig{
		nodeSets: map[proto.DiskType]nodeSetAllocatorMap{proto.DiskTypeHDD: {replicateNodeSetID: ns}},
		diskSets: map[proto.DiskType]diskSetAllocatorMap{proto.DiskTypeHDD: {replicateDiskSetID: ds}},
		diffHost: diffHost,
		tg:       tg,
	})
}

// mockTopoGetter implements topoInfoGetter for tests.
type mockTopoGetter struct{ nodeNum int }

func (m *mockTopoGetter) getNodeNum(_ proto.DiskType, _ proto.NodeSetID) int { return m.nodeNum }

func TestCanAllocForMode(t *testing.T) {
	idcs := []string{"z0", "z1", "z2"}

	// EC15P12: 27 shards, 9 per IDC (3 IDCs)
	// EC6P6:   12 shards, 4 per IDC (3 IDCs)
	// Replica3: 3 shards, 1 per IDC (3 IDCs, replicate mode)

	t.Run("ec_mode_enough_space_diffhost", func(t *testing.T) {
		// 10 nodes per IDC, each with 5 free chunks; EC6P6 needs 4 nodes per IDC
		alloc := buildECAllocator(idcs, 10, 5, true)
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC15P12))
	})

	t.Run("ec_mode_exact_boundary_diffhost", func(t *testing.T) {
		// exactly 4 nodes per IDC → EC6P6 needs 4 → should pass
		alloc := buildECAllocator(idcs, 4, 5, true)
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_one_fewer_node_diffhost", func(t *testing.T) {
		// 3 nodes per IDC → EC6P6 needs 4 → should fail
		alloc := buildECAllocator(idcs, 3, 5, true)
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_one_idc_exhausted", func(t *testing.T) {
		// Build with 10 nodes per IDC, then zero out z1's creatableNodes
		alloc := buildECAllocator(idcs, 10, 5, true)
		ds := alloc.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID]
		atomic.StoreInt64(&ds.idcAllocators["z1"].creatableNodes, 0)
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_nodeset_weight_too_low", func(t *testing.T) {
		// total nodeSet weight < shardNum → fail at first check
		alloc := buildECAllocator(idcs, 2, 1, true)
		ns := alloc.nodeSets[proto.DiskTypeHDD][ecNodeSetID]
		atomic.StoreInt64(&ns.weight, 1) // EC6P6 needs 12 shards
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_no_nodeset", func(t *testing.T) {
		// diskType has no nodeSets at all
		alloc := buildECAllocator(idcs, 10, 5, true)
		delete(alloc.nodeSets, proto.DiskTypeHDD)
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_diffhost_false_weight_sufficient", func(t *testing.T) {
		// diffHost=false: weight AND disk count both satisfy chunksPerIDC.
		// EC6P6 needs 4 chunks per IDC; build 4 nodes × 1 disk (freeChunk=3) per IDC
		// → weight=12, disks=4 → pass.
		alloc := buildECAllocator(idcs, 4, 3, false)
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_diffhost_false_weight_insufficient", func(t *testing.T) {
		// diffHost=false: 1 node × 1 free chunk = 1 weight, EC6P6 needs 4 → fail
		alloc := buildECAllocator(idcs, 1, 1, false)
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("allocatable_nodes_decrements_on_node_depletion", func(t *testing.T) {
		// 4 nodes per IDC, each with exactly 1 free chunk.
		// EC6P6 needs 4 allocatable nodes per IDC → initially true.
		// defaultAllocTolerateBuff would cause totalWeight(4) - 50 < 0, preventing alloc.
		// Zero it out for this test to isolate the creatableNodes maintenance logic.
		origBuff := defaultAllocTolerateBuff
		defaultAllocTolerateBuff = 0
		origReserved := defaultDiskReservedFreeChunk
		defaultDiskReservedFreeChunk = 0
		defer func() {
			defaultAllocTolerateBuff = origBuff
			defaultDiskReservedFreeChunk = origReserved
		}()

		_, ctx := trace.StartSpanFromContext(context.Background(), "")
		alloc := buildECAllocator(idcs, 4, 1, true)
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))

		ds := alloc.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID]
		idcAlloc := ds.idcAllocators["z0"]
		before := atomic.LoadInt64(&idcAlloc.creatableNodes)

		// alloc 1 chunk: one node's weight drops to 0 → creatableNodes decrements
		_, err := idcAlloc.alloc(ctx, 1, nil, false)
		require.NoError(t, err)

		after := atomic.LoadInt64(&idcAlloc.creatableNodes)
		require.Equal(t, before-1, after)

		// z0 now has 3 allocatable nodes < 4 needed for EC6P6 → false
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("replicate_mode_enough_space", func(t *testing.T) {
		// Replica3: 3 IDCs, 1 shard per IDC; 5 nodes per IDC → pass
		alloc := buildReplicateAllocator(idcs, 5, 10, true)
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.Replica3))
	})

	t.Run("replicate_mode_no_qualifying_nodeset", func(t *testing.T) {
		// Replica3: 3 IDCs, but total weight in nodeSet < shardNum(3)
		alloc := buildReplicateAllocator(idcs, 1, 1, true)
		ns := alloc.nodeSets[proto.DiskTypeHDD]
		for _, n := range ns {
			if n.nodeSetID != ecNodeSetID {
				atomic.StoreInt64(&n.weight, 0)
			}
		}
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.Replica3))
	})

	t.Run("replicate_mode_diffhost_node_count_insufficient", func(t *testing.T) {
		// diffHost=true, nodeNum=1, Replica3 needs 3 shards → topo check fails
		alloc := buildReplicateAllocator(idcs, 5, 10, true)
		// override tg to report only 2 nodes total
		alloc.cfg.tg = &mockTopoGetter{nodeNum: 2}
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.Replica3))
	})

	t.Run("ec_mode_diffhost_false_exact_disk_boundary", func(t *testing.T) {
		// diffHost=false: exactly chunksPerIDC disks per IDC → should pass (>= boundary)
		// EC6P6 needs 4 chunks per IDC; build 4 nodes × 1 disk each per IDC
		alloc := buildECAllocator(idcs, 4, 2, false)
		require.True(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})

	t.Run("ec_mode_diffhost_false_one_fewer_disk", func(t *testing.T) {
		// diffHost=false: 3 disks per IDC but EC6P6 needs 4 chunks per IDC
		// weight per IDC = 3 × 2 = 6 ≥ 4 (passes weight check), but len(disks)=3 < 4 → fail
		alloc := buildECAllocator(idcs, 3, 2, false)
		require.False(t, alloc.canAllocForMode(proto.DiskTypeHDD, codemode.EC6P6))
	})
}

// TestAllocatableNodesConcurrentSafe verifies that concurrent allocations targeting
// the same scarce node do NOT over-decrement creatableNodes below the true count.
// This guards the "cross-zero boundary" check in idcAllocator.alloc:
//
//	if newWeight <= 0 && newWeight+int64(num) > 0 { ... }
//
// Without the boundary check, N concurrent allocs that all pick the same node with
// weight=1 would each decrement creatableNodes (it becomes 1-N, i.e. negative),
// poisoning subsequent canAllocForMode checks.
func TestAllocatableNodesConcurrentSafe(t *testing.T) {
	origBuff := defaultAllocTolerateBuff
	defaultAllocTolerateBuff = 0
	origReserved := defaultDiskReservedFreeChunk
	defaultDiskReservedFreeChunk = 0
	defer func() {
		defaultAllocTolerateBuff = origBuff
		defaultDiskReservedFreeChunk = origReserved
	}()

	// 1 IDC, 1 node, 1 disk with exactly 1 free chunk.
	// Any number of concurrent alloc(1) calls may all pick this single node
	// before anyone decrements its weight.
	alloc := buildECAllocator([]string{"z0"}, 1, 1, true)
	ds := alloc.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID]
	idcAlloc := ds.idcAllocators["z0"]

	require.Equal(t, int64(1), atomic.LoadInt64(&idcAlloc.creatableNodes))

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, _ = idcAlloc.alloc(ctx, 1, nil, false)
		}()
	}
	wg.Wait()

	// Regardless of how many concurrent callers successfully picked the node,
	// creatableNodes should drop from 1 to exactly 0 — never below.
	got := atomic.LoadInt64(&idcAlloc.creatableNodes)
	require.Equal(t, int64(0), got,
		"creatableNodes must not drop below 0 under concurrent alloc; got %d", got)
}

// TestApplyNodeWeightDecrements_CrossZeroGuard is a white-box regression test
// for the cross-zero boundary guard in idcAllocator.applyNodeWeightDecrements:
//
//	if newWeight <= 0 && newWeight+int64(num) > 0 { ... }
//
// Concurrent callers of idcAllocator.alloc may all observe the same positive
// node.weight in allocFromNodeStorages (TOCTOU), each picking the node into
// their own chosenDataStorages. The subsequent atomic AddInt64 from each caller
// can drive node.weight well below zero. Without the cross-zero guard, every
// caller whose AddInt64 returns ≤0 would decrement creatableNodes, poisoning
// it below zero and causing canAllocForMode to spuriously return false.
//
// This test deterministically reproduces that scenario by calling the helper
// directly on progressively-decrementing state.
//
// Expected behavior: creatableNodes is decremented EXACTLY ONCE — when weight
// crosses from positive to ≤ 0. Further decrements while weight is already ≤ 0
// must be ignored.
func TestApplyNodeWeightDecrements_CrossZeroGuard(t *testing.T) {
	idc := &idcAllocator{creatableNodes: 1}
	stg := &nodeAllocator{weight: 1}

	// 1st caller (would be G1 in a race): weight 1 → 0, cross-zero, decrement.
	idc.applyNodeWeightDecrements(map[*nodeAllocator]int{stg: 1})
	require.EqualValues(t, 0, atomic.LoadInt64(&stg.weight))
	require.EqualValues(t, 0, atomic.LoadInt64(&idc.creatableNodes))

	// 2nd caller (G2): weight 0 → -1. Old code would decrement again to -1.
	idc.applyNodeWeightDecrements(map[*nodeAllocator]int{stg: 1})
	require.EqualValues(t, -1, atomic.LoadInt64(&stg.weight))
	require.EqualValues(t, 0, atomic.LoadInt64(&idc.creatableNodes),
		"must not decrement again once weight already crossed zero")

	// 3rd caller with num>1: weight -1 → -4. Still no decrement.
	idc.applyNodeWeightDecrements(map[*nodeAllocator]int{stg: 3})
	require.EqualValues(t, -4, atomic.LoadInt64(&stg.weight))
	require.EqualValues(t, 0, atomic.LoadInt64(&idc.creatableNodes))
}

// TestApplyNodeWeightDecrements_CrossZeroBySingleLargeBatch verifies that a
// single caller whose batch size exceeds current weight still triggers exactly
// one creatableNodes decrement (e.g. weight=2, num=5 → newWeight=-3, crosses zero once).
func TestApplyNodeWeightDecrements_CrossZeroBySingleLargeBatch(t *testing.T) {
	idc := &idcAllocator{creatableNodes: 3}
	stg := &nodeAllocator{weight: 2}

	idc.applyNodeWeightDecrements(map[*nodeAllocator]int{stg: 5})
	require.EqualValues(t, -3, atomic.LoadInt64(&stg.weight))
	require.EqualValues(t, 2, atomic.LoadInt64(&idc.creatableNodes))
}

// TestApplyNodeWeightDecrements_StillPositiveNoDecrement verifies the common path:
// when weight stays positive after subtraction, creatableNodes is not touched.
func TestApplyNodeWeightDecrements_StillPositiveNoDecrement(t *testing.T) {
	idc := &idcAllocator{creatableNodes: 5}
	stg := &nodeAllocator{weight: 10}

	idc.applyNodeWeightDecrements(map[*nodeAllocator]int{stg: 3})
	require.EqualValues(t, 7, atomic.LoadInt64(&stg.weight))
	require.EqualValues(t, 5, atomic.LoadInt64(&idc.creatableNodes))
}

// TestAllocatableNodesResetByRefresh verifies that refresh() re-initializes
// creatableNodes from the latest disk stats, preventing cumulative drift
// across long-running processes.
func TestAllocatableNodesResetByRefresh(t *testing.T) {
	testDiskMgr, closeFunc := initTestBlobNodeMgr(t)
	defer closeFunc()
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	testDiskMgr.cfg.HostAware = true

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// 6 nodes per IDC, 10 chunks each → enough for EC6P6 (4 per IDC)
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 6, testIdcs...)
	initTestBlobNodeMgrDisksWithChunk(t, testDiskMgr, 1, 360, 10, false, testIdcs...)
	testDiskMgr.refresh(ctx)

	allocators := testDiskMgr.manager.allocator.Load().(*allocator)
	idcAlloc := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators[testIdcs[0]]
	before := atomic.LoadInt64(&idcAlloc.creatableNodes)
	require.Greater(t, before, int64(0))

	// Forcibly poison creatableNodes to simulate accumulated drift
	atomic.StoreInt64(&idcAlloc.creatableNodes, -10)

	// refresh rebuilds allocator state from current disk snapshot
	testDiskMgr.refresh(ctx)
	allocators = testDiskMgr.manager.allocator.Load().(*allocator)
	idcAllocAfter := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators[testIdcs[0]]
	after := atomic.LoadInt64(&idcAllocAfter.creatableNodes)
	require.Equal(t, before, after, "refresh must reset creatableNodes to the correct value")
}

// TestDiskReservedFreeChunkFilterKeepsStats verifies that disks with
// free chunks ≤ DiskReservedFreeChunk are excluded from the allocator
// (so canAllocForMode won't over-count them), yet still contribute to
// public DiskStatInfo counters (TotalChunk, TotalFreeChunk, etc.).
func TestDiskReservedFreeChunkFilterKeepsStats(t *testing.T) {
	testDiskMgr, closeFunc := initTestBlobNodeMgr(t)
	defer closeFunc()
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	testDiskMgr.cfg.HostAware = true
	testDiskMgr.cfg.DiskReservedFreeChunk = 1 // any disk with freeChunk <= 1 is reserved

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 6, testIdcs...)
	initTestBlobNodeMgrDisksWithChunk(t, testDiskMgr, 1, 360, 10, false, testIdcs...)

	// Mark the first 60 disks in z0 as nearly full (freeChunk=1, below reserved).
	// MaxChunkCnt stays at 10, so they still count toward total capacity.
	nearlyFullCount := 60
	testDiskMgr.metaLock.RLock()
	for i := 1; i <= nearlyFullCount; i++ {
		di, ok := testDiskMgr.allDisks[proto.DiskID(i)]
		if !ok {
			continue
		}
		di.lock.Lock()
		di.info.extraInfo.(*clustermgr.DiskHeartBeatInfo).FreeChunkCnt = 1
		di.lock.Unlock()
	}
	testDiskMgr.metaLock.RUnlock()
	testDiskMgr.refresh(ctx)

	// Stat accounting: TotalChunk should still include the nearly-full disks
	stat := testDiskMgr.Stat(ctx, proto.DiskTypeHDD)
	var totalChunk int64
	for _, s := range stat.DisksStatInfos {
		totalChunk += s.TotalChunk
	}
	// 3 IDCs × 360 disks × 10 MaxChunkCnt = 10800; all disks (including reserved) must be counted
	expected := int64(len(testIdcs) * 360 * 10)
	require.Equal(t, expected, totalChunk,
		"DiskReservedFreeChunk filter must not corrupt TotalChunk stats")

	// Allocator check: reserved disks now remain in the allocator so that background
	// tasks (repair/migrate) can reach them via isCreateVolume=false / reserveChunk=false.
	// z0 has 360 disks total; all must be present in idcAlloc.disks.
	allocators := testDiskMgr.manager.allocator.Load().(*allocator)
	idcAlloc := allocators.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators[testIdcs[0]]
	require.Equal(t, 360, len(idcAlloc.disks),
		"all disks (including reserved) must remain in the allocator for background tasks")

	// creatableDisks must only count disks with freeChunk > DiskReservedFreeChunk.
	// z0: 360 total − 60 nearly-full = 300 creation-eligible.
	require.Equal(t, int64(300), atomic.LoadInt64(&idcAlloc.creatableDisks),
		"creatableDisks must exclude reserved disks")

	// creatableNodes counts nodes that have ≥1 creation-eligible disk.
	// All 6 nodes in z0 have some non-reserved disks remaining.
	require.Equal(t, int64(6), atomic.LoadInt64(&idcAlloc.creatableNodes),
		"creatableNodes must count nodes with at least one creation-eligible disk")
}

// TestReservedDisksAccessibleByBackgroundTask verifies that disks with free chunks at or
// below DiskReservedFreeChunk remain in the allocator and can be reached by background
// tasks (non-empty excludes → isCreateVolume=false → reserveChunk=false), while volume
// creation (empty excludes → isCreateVolume=true → reserveChunk=true) is still blocked.
func TestReservedDisksAccessibleByBackgroundTask(t *testing.T) {
	testDiskMgr, closeFunc := initTestBlobNodeMgr(t)
	defer closeFunc()
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	testDiskMgr.cfg.HostAware = false // disk-granularity allocation, no node isolation
	testDiskMgr.cfg.DiskReservedFreeChunk = 2

	origReserved := defaultDiskReservedFreeChunk
	defaultDiskReservedFreeChunk = 2
	defer func() { defaultDiskReservedFreeChunk = origReserved }()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// 1 node, 1 disk per IDC with exactly freeChunk=2 (at the reserve threshold).
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisksWithChunk(t, testDiskMgr, 1, 1, 2, false, testIdcs[0])
	testDiskMgr.refresh(ctx)

	alloc := testDiskMgr.manager.allocator.Load().(*allocator)
	idcAlloc := alloc.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators[testIdcs[0]]

	// The disk is in the allocator (background tasks can reach it).
	require.Equal(t, 1, len(idcAlloc.disks), "reserved disk must be in allocator")
	// But creatableDisks=0: the disk is below the reserve threshold for volume creation.
	require.Equal(t, int64(0), atomic.LoadInt64(&idcAlloc.creatableDisks),
		"disk at reserve threshold must not count as creatable")

	// Background task: pass a non-empty excludes map so isCreateVolume=false.
	// The disk should be selected even though its weight == DiskReservedFreeChunk.
	fakeExclude := map[proto.DiskID]*diskItem{proto.DiskID(9999): {}}
	chosen := idcAlloc.disks[0].withRLocked(func() error { return nil })
	_ = chosen // just confirm disk is accessible; actual alloc tested via allocDisk below
	disk := idcAlloc.nodeStorages[0].allocDisk(ctx, fakeExclude, false /* reserveChunk=false */)
	require.NotNil(t, disk, "background task must be able to allocate from reserved disk")

	// Volume creation: empty excludes → isCreateVolume=true → reserveChunk=true → blocked.
	diskForCreate := idcAlloc.nodeStorages[0].allocDisk(ctx, map[proto.DiskID]*diskItem{}, true /* reserveChunk=true */)
	require.Nil(t, diskForCreate, "volume creation must not use reserved disk")
}

// TestAllNodesOnlyReservedChunks verifies that when every disk is at or below the reserve
// threshold, both allocatableNodes and creatableDisks are 0, and HasEnoughSpace returns false.
func TestAllNodesOnlyReservedChunks(t *testing.T) {
	testDiskMgr, closeFunc := initTestBlobNodeMgr(t)
	defer closeFunc()
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	testDiskMgr.cfg.HostAware = true
	testDiskMgr.cfg.DiskReservedFreeChunk = 5

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 4, testIdcs...)
	initTestBlobNodeMgrDisksWithChunk(t, testDiskMgr, 1, 40, 10, false, testIdcs...)

	// Drain all disks to exactly the reserve threshold.
	testDiskMgr.metaLock.RLock()
	for _, di := range testDiskMgr.allDisks {
		di.lock.Lock()
		di.info.extraInfo.(*clustermgr.DiskHeartBeatInfo).FreeChunkCnt = 5
		di.lock.Unlock()
	}
	testDiskMgr.metaLock.RUnlock()
	testDiskMgr.refresh(ctx)

	alloc := testDiskMgr.manager.allocator.Load().(*allocator)
	idcAlloc := alloc.nodeSets[proto.DiskTypeHDD][ecNodeSetID].diskSets[ecDiskSetID].idcAllocators[testIdcs[0]]

	// All disks are in the allocator (background tasks still have access).
	require.NotEmpty(t, idcAlloc.disks, "disks must stay in allocator even at reserve threshold")
	// No disk is eligible for volume creation.
	require.Equal(t, int64(0), atomic.LoadInt64(&idcAlloc.creatableDisks))
	require.Equal(t, int64(0), atomic.LoadInt64(&idcAlloc.creatableNodes))

	// HasEnoughSpace must return false for any code mode.
	require.False(t, testDiskMgr.HasEnoughSpace(ctx, codemode.EC6P6))
}

// TestHasEnoughSpaceIntegration verifies BlobNodeManager.HasEnoughSpace via real refresh.
func TestHasEnoughSpaceIntegration(t *testing.T) {
	testDiskMgr, closeFunc := initTestBlobNodeMgr(t)
	defer closeFunc()
	testDiskMgr.cfg.HeartbeatExpireIntervalS = 6000
	testDiskMgr.cfg.HostAware = true

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// Add enough nodes and disks across all 3 IDCs
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 10, testIdcs...)
	initTestBlobNodeMgrDisksWithChunk(t, testDiskMgr, 1, 600, 20, false, testIdcs...)
	testDiskMgr.refresh(ctx)

	// EC6P6 needs 4 nodes per IDC; we have 10 nodes per IDC → should pass
	require.True(t, testDiskMgr.HasEnoughSpace(ctx, codemode.EC6P6))

	// Drain all free chunks on all disks to simulate full cluster
	testDiskMgr.metaLock.RLock()
	for _, di := range testDiskMgr.allDisks {
		di.lock.Lock()
		di.info.extraInfo.(*clustermgr.DiskHeartBeatInfo).FreeChunkCnt = 0
		di.lock.Unlock()
	}
	testDiskMgr.metaLock.RUnlock()
	testDiskMgr.refresh(ctx)

	// All disks full → HasEnoughSpace should return false
	require.False(t, testDiskMgr.HasEnoughSpace(ctx, codemode.EC6P6))
}

// realisticFreeChunks returns a free chunk count that mimics real-world disk fill distribution
// at ~90% water level. Disks are categorized by index:
//   - 5%  new disks:     25-30 free  (~90% free)
//   - 15% moderate:      5-8  free   (~20% free)
//   - 80% nearly full:   0-2  free   (~5% free)
//
// Overall average ≈ 10% free → 90% water level.
func realisticFreeChunks(diskIdx, maxChunks int, rng *rand.Rand) int {
	switch diskIdx % 20 {
	case 0: // new disk (5%)
		return maxChunks*7/10 + rng.Intn(maxChunks*3/10+1)
	case 1, 2, 3: // moderate (15%)
		return maxChunks/6 + rng.Intn(maxChunks/8+1)
	default: // nearly full (80%)
		return rng.Intn(maxChunks/10 + 1) // 0 ~ 3 for maxChunks=30
	}
}

// buildBenchAllocator constructs an allocator directly without BlobNodeManager overhead.
//
// Topology:
//   - numIDC IDCs, nodesPerIDC nodes each, disksPerNode disks each
//   - maxChunks: MaxChunkCnt per disk
//   - Free chunk counts follow a realistic bimodal distribution (~90% water level on average)
//   - diffHost: whether host-aware allocation is enabled
func buildBenchAllocator(numIDC, nodesPerIDC, disksPerNode, maxChunks int, diffHost bool) *allocator {
	rng := rand.New(rand.NewSource(42)) // deterministic seed for reproducibility

	idcNames := make([]string, numIDC)
	for i := range idcNames {
		idcNames[i] = "z" + strconv.Itoa(i)
	}

	idcAllocs := make(map[string]*idcAllocator, numIDC)
	totalWeight := int64(0)

	diskID := proto.DiskID(1)
	nodeID := proto.NodeID(1)

	for _, idc := range idcNames {
		nodeStgs := make([]*nodeAllocator, 0, nodesPerIDC)
		idcWeight := int64(0)

		for n := 0; n < nodesPerIDC; n++ {
			ni := &nodeItem{nodeID: nodeID}
			nodeID++

			disks := make([]*diskItem, 0, disksPerNode)
			nodeWeight := int64(0)
			for d := 0; d < disksPerNode; d++ {
				free := int64(realisticFreeChunks(int(diskID), maxChunks, rng))
				di := &diskItem{
					diskID: diskID,
					info: diskItemInfo{
						DiskInfo: clustermgr.DiskInfo{
							Idc:    idc,
							Status: proto.DiskStatusNormal,
						},
						extraInfo: &clustermgr.DiskHeartBeatInfo{
							MaxChunkCnt:  int64(maxChunks),
							FreeChunkCnt: free,
						},
					},
					weightGetter:   blobNodeDiskWeightGetter,
					weightDecrease: blobNodeDiskWeightDecrease,
				}
				diskID++
				disks = append(disks, di)
				nodeWeight += free
			}

			ns := &nodeAllocator{
				node:   ni,
				weight: nodeWeight,
				disks:  disks,
			}
			nodeStgs = append(nodeStgs, ns)
			idcWeight += nodeWeight
		}

		allocatableCount := int64(0)
		for _, ns := range nodeStgs {
			if ns.weight > 0 {
				allocatableCount++
			}
		}

		var allDisks []*diskItem
		for _, ns := range nodeStgs {
			allDisks = append(allDisks, ns.disks...)
		}

		idcAllocs[idc] = &idcAllocator{
			idc:            idc,
			weight:         idcWeight,
			creatableNodes: allocatableCount,
			diffHost:       diffHost,
			nodeStorages:   nodeStgs,
			disks:          allDisks,
		}
		totalWeight += idcWeight
	}

	ds := newDiskSetAllocator(ecDiskSetID, totalWeight, idcAllocs)
	ns := newNodeSetAllocator(ecNodeSetID)
	ns.addDiskSet(ds)

	return newAllocator(allocatorConfig{
		nodeSets: map[proto.DiskType]nodeSetAllocatorMap{
			proto.DiskTypeHDD: {ecNodeSetID: ns},
		},
		diskSets: map[proto.DiskType]diskSetAllocatorMap{
			proto.DiskTypeHDD: {ecDiskSetID: ds},
		},
		diffHost: diffHost,
	})
}

// BenchmarkCanAllocForMode_EC15P12 benchmarks canAllocForMode with a production-scale
// cluster: ~13,000 disks (222 nodes × 59 disks), 3 IDCs, realistic ~90% water level.
// Free chunk distribution: 5% new disks (high free), 15% moderate, 80% nearly full.
func BenchmarkCanAllocForMode_EC15P12(b *testing.B) {
	const (
		numIDC       = 3
		nodesPerIDC  = 74 // 222 nodes total
		disksPerNode = 59 // ~13,098 disks total
		maxChunks    = 30
	)

	alloc := buildBenchAllocator(numIDC, nodesPerIDC, disksPerNode, maxChunks, true)
	mode := codemode.EC15P12

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = alloc.canAllocForMode(proto.DiskTypeHDD, mode)
	}
}

// BenchmarkCanAllocForMode_EC6P6 benchmarks canAllocForMode with EC6P6 mode,
// same production-scale topology and realistic free chunk distribution.
func BenchmarkCanAllocForMode_EC6P6(b *testing.B) {
	const (
		numIDC       = 3
		nodesPerIDC  = 74
		disksPerNode = 59
		maxChunks    = 30
	)

	alloc := buildBenchAllocator(numIDC, nodesPerIDC, disksPerNode, maxChunks, true)
	mode := codemode.EC6P6

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = alloc.canAllocForMode(proto.DiskTypeHDD, mode)
	}
}

// TestAlloc_ErrorBranches covers the error paths of Alloc / allocNodeSet /
// allocDiskSet that the broader AllocChunks tests don't exercise.
func TestAlloc_ErrorBranches(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	t.Run("unknown_disk_type", func(t *testing.T) {
		a := &allocator{
			nodeSets: map[proto.DiskType]nodeSetAllocatorMap{},
			cfg:      allocatorConfig{},
		}
		_, err := a.Alloc(ctx, proto.DiskTypeHDD, codemode.EC15P12, nil)
		require.ErrorIs(t, err, ErrNoEnoughSpace)

		_, err = a.allocNodeSet(ctx, proto.DiskTypeHDD, codemode.EC15P12)
		require.ErrorIs(t, err, ErrNoEnoughSpace)
	})

	t.Run("ec_nodeset_weight_not_enough", func(t *testing.T) {
		a := &allocator{
			nodeSets: map[proto.DiskType]nodeSetAllocatorMap{
				proto.DiskTypeHDD: {
					ecNodeSetID: {nodeSetID: ecNodeSetID, weight: 1, diskSets: map[proto.DiskSetID]*diskSetAllocator{}},
				},
			},
			cfg: allocatorConfig{},
		}
		_, err := a.allocNodeSet(ctx, proto.DiskTypeHDD, codemode.EC15P12)
		require.ErrorIs(t, err, ErrNoEnoughSpace)
	})

	t.Run("replicate_no_candidate_nodeset", func(t *testing.T) {
		// Replicate mode but only an ecNodeSetID exists, so the loop body
		// filters it out and leaves totalWeight == 0 => ErrNoEnoughSpace.
		a := &allocator{
			nodeSets: map[proto.DiskType]nodeSetAllocatorMap{
				proto.DiskTypeHDD: {
					ecNodeSetID: {nodeSetID: ecNodeSetID, weight: 100, diskSets: map[proto.DiskSetID]*diskSetAllocator{}},
				},
			},
			cfg: allocatorConfig{diffHost: false, tg: newTopoMgr()},
		}
		_, err := a.allocNodeSet(ctx, proto.DiskTypeHDD, codemode.Replica3)
		require.ErrorIs(t, err, ErrNoEnoughSpace)
	})

	t.Run("allocDiskSet_all_excluded", func(t *testing.T) {
		ds := &diskSetAllocator{diskSetID: proto.DiskSetID(10), weight: 100}
		ns := &nodeSetAllocator{
			nodeSetID: proto.NodeSetID(3),
			weight:    100,
			diskSets:  map[proto.DiskSetID]*diskSetAllocator{ds.diskSetID: ds},
		}
		_, err := ns.allocDiskSet(ctx, 1, []proto.DiskSetID{ds.diskSetID})
		require.ErrorIs(t, err, ErrNoEnoughSpace)
	})

	t.Run("allocDiskSet_count_too_large", func(t *testing.T) {
		ds := &diskSetAllocator{diskSetID: proto.DiskSetID(11), weight: 5}
		ns := &nodeSetAllocator{
			nodeSetID: proto.NodeSetID(4),
			weight:    5,
			diskSets:  map[proto.DiskSetID]*diskSetAllocator{ds.diskSetID: ds},
		}
		_, err := ns.allocDiskSet(ctx, 100, nil)
		require.ErrorIs(t, err, ErrNoEnoughSpace)
	})
}

// TestAlloc_HostAwareFilter exercises the diffHost filter inside allocNodeSet:
// when diffHost=true and the nodeSet does not have enough distinct nodes the
// filter kicks in and totalWeight falls to 0 => ErrNoEnoughSpace.
func TestAlloc_HostAwareFilter(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	mgr.cfg.HeartbeatExpireIntervalS = 6000
	mgr.cfg.HostAware = true

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// only 1 node / 1 disk is never enough for Replica3 which needs 3 distinct hosts
	initTestBlobNodeMgrNodes(t, mgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 1, true, testIdcs[0])
	mgr.refresh(ctx)

	alloc := mgr.manager.allocator.Load().(*allocator)
	_, err := alloc.allocNodeSet(ctx, proto.DiskTypeHDD, codemode.Replica3)
	require.ErrorIs(t, err, ErrNoEnoughSpace)
}
