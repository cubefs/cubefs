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
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestBlobNodeMgr_Normal(t *testing.T) {
	blobNodeManager, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// AllocDiskID
	{
		testMockScopeMgr.EXPECT().Alloc(gomock.Any(), DiskIDScopeName, 1).Return(uint64(1), uint64(1), nil)
		diskID, err := blobNodeManager.AllocDiskID(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.DiskID(1), diskID)

		testMockScopeMgr.EXPECT().GetCurrent(gomock.Any()).Return(uint64(1))
		current := blobNodeManager.scopeMgr.GetCurrent(DiskIDScopeName)
		require.Equal(t, current, uint64(1))
	}

	// addDisk and GetDiskInfo and CheckDiskInfoDuplicated
	{
		initTestBlobNodeMgrNodes(t, blobNodeManager, 1, 1, testIdcs[0])
		initTestBlobNodeMgrDisks(t, blobNodeManager, 1, 10, false, testIdcs[0])

		nodeInfo, err := blobNodeManager.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		require.Equal(t, proto.NodeID(1), nodeInfo.NodeID)
		// test disk exist
		for i := 1; i <= 10; i++ {
			diskInfo, err := blobNodeManager.GetDiskInfo(ctx, proto.DiskID(i))
			require.NoError(t, err)
			require.Equal(t, proto.DiskID(i), diskInfo.DiskID)
			diskExist := blobNodeManager.CheckDiskInfoDuplicated(ctx, diskInfo.DiskID, &diskInfo.DiskInfo, &nodeInfo.NodeInfo)
			require.Equal(t, apierrors.ErrExist, diskExist)
		}

		// test host and path duplicated
		diskInfo, err := blobNodeManager.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		diskInfo.DiskID = proto.DiskID(11)
		nodeInfo, err = blobNodeManager.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		duplicated := blobNodeManager.CheckDiskInfoDuplicated(ctx, diskInfo.DiskID, &diskInfo.DiskInfo, &nodeInfo.NodeInfo)
		require.Equal(t, apierrors.ErrIllegalArguments, duplicated)

		// test normal case
		diskInfo.DiskID = proto.DiskID(11)
		diskInfo.Path += "notDuplicated"
		nodeInfo, err = blobNodeManager.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		duplicated = blobNodeManager.CheckDiskInfoDuplicated(ctx, diskInfo.DiskID, &diskInfo.DiskInfo, &nodeInfo.NodeInfo)
		require.Equal(t, nil, duplicated)
	}

	// IsDiskWritable and SetStatus and SwitchReadonly
	{
		for i := 1; i < 2; i++ {
			writable, err := blobNodeManager.IsDiskWritable(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, true, writable)
		}

		err := blobNodeManager.SetStatus(ctx, 1, proto.DiskStatusBroken, true)
		require.NoError(t, err)

		err = blobNodeManager.applySwitchReadonly(1, true)
		require.NoError(t, err)

		for i := 1; i < 2; i++ {
			writable, err := blobNodeManager.IsDiskWritable(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, false, writable)
		}
	}

	_, suCount := blobNodeManager.getMaxSuCount()
	require.Equal(t, 27, suCount)
}

func TestDiskMgr_Dropping(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// dropping and list dropping
	{
		droppingList, err := testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(droppingList))

		err = testDiskMgr.applySwitchReadonly(1, true)
		require.NoError(t, err)

		_, err = testDiskMgr.applyDroppingDisk(ctx, 1, true)
		require.NoError(t, err)

		// add dropping disk repeatedly
		_, err = testDiskMgr.applyDroppingDisk(ctx, 1, true)
		require.NoError(t, err)

		// set status when disk is dropping, return ErrChangeDiskStatusNotAllow
		err = testDiskMgr.SetStatus(ctx, 1, proto.DiskStatusBroken, false)
		require.ErrorIs(t, apierrors.ErrChangeDiskStatusNotAllow, err)

		droppingList, err = testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))

		ok, err := testDiskMgr.IsDroppingDisk(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, true, ok)

		ok, err = testDiskMgr.IsDroppingDisk(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, false, ok)
	}

	// dropped
	{
		err := testDiskMgr.applySwitchReadonly(2, true)
		require.NoError(t, err)

		_, err = testDiskMgr.applyDroppingDisk(ctx, 2, true)
		require.NoError(t, err)
		droppingList, err := testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(droppingList))

		err = testDiskMgr.applyDroppedDisk(ctx, 1)
		require.NoError(t, err)

		// add dropping disk 1 repeatedly
		_, err = testDiskMgr.applyDroppingDisk(ctx, 1, true)
		require.NoError(t, err)

		droppingList, err = testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))
		t.Log(droppingList[0].DiskID)

		ok, err := testDiskMgr.IsDroppingDisk(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, false, ok)

		writable, err := testDiskMgr.IsDiskWritable(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, false, writable)
	}
}

func TestDiskMgr_Heartbeat(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	heartbeatInfos := make([]*clustermgr.DiskHeartBeatInfo, 0)
	for i := 1; i <= 10; i++ {
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		// diskInfo.DiskHeartBeatInfo.Free = 0
		diskInfo.DiskHeartBeatInfo.FreeChunkCnt = 0
		heartbeatInfos = append(heartbeatInfos, &diskInfo.DiskHeartBeatInfo)
	}
	err := testDiskMgr.applyHeartBeatDiskInfo(ctx, heartbeatInfos)
	require.NoError(t, err)

	// heartbeat check
	for i := 1; i <= 10; i++ {
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		require.Equal(t, diskInfo.Free/testDiskMgr.cfg.ChunkSize, diskInfo.FreeChunkCnt)
		require.Greater(t, diskInfo.OversoldFreeChunkCnt, diskInfo.FreeChunkCnt)
	}

	// reset oversold_chunk_ratio into 0
	testDiskMgr.cfg.ChunkOversoldRatio = 0
	err = testDiskMgr.applyHeartBeatDiskInfo(ctx, heartbeatInfos)
	require.NoError(t, err)
	// validate OversoldFreeChunkCnt and FreeChunkCnt
	for i := 1; i <= 10; i++ {
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		require.Equal(t, diskInfo.Free/testDiskMgr.cfg.ChunkSize, diskInfo.FreeChunkCnt)
		require.Equal(t, int64(0), diskInfo.OversoldFreeChunkCnt)
	}

	// get heartbeat change disk
	disks := testDiskMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 0, len(disks))

	disk, _ := testDiskMgr.getDisk(proto.DiskID(1))
	disk.lock.Lock()
	disk.expireTime = time.Now().Add(-time.Second)
	disk.lock.Unlock()
	disks = testDiskMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 1, len(disks))
	require.Equal(t, HeartbeatEvent{DiskID: proto.DiskID(1), IsAlive: false}, disks[0])

	disk, _ = testDiskMgr.getDisk(proto.DiskID(2))
	disk.lock.Lock()
	disk.lastExpireTime = time.Now().Add(time.Duration(testDiskMgr.cfg.HeartbeatExpireIntervalS) * time.Second * -3)
	disk.lock.Unlock()
	disks = testDiskMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 2, len(disks))
}

func TestDiskMgr_ListDisks(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(1))
	require.NoError(t, err)

	{
		ret, marker, err := testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Host: diskInfo.Host, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))
		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Host: diskInfo.Host, Count: 1000, Marker: marker})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret))

		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Status: proto.DiskStatusNormal, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))

		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))

		ret, marker, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Count: 2})
		require.NoError(t, err)
		require.Equal(t, 2, len(ret))
		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Count: 1000, Marker: marker})
		require.NoError(t, err)
		require.Equal(t, 8, len(ret))

		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Status: proto.DiskStatusNormal, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))

		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Status: proto.DiskStatusDropped, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret))

		ret, _, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Host: diskInfo.Host, Status: proto.DiskStatusDropped, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret))
	}

	{
		err := testDiskMgr.SetStatus(ctx, proto.DiskID(1), proto.DiskStatusBroken, true)
		require.NoError(t, err)

		ret, _, err := testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 1, len(ret))
	}
}

func TestDiskMgr_AdminUpdateDisk(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestBlobNodeMgr(t)
	defer closeTestDiskMgr()
	initTestBlobNodeMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	diskInfo := &clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			DiskID:               1,
			MaxChunkCnt:          99,
			FreeChunkCnt:         9,
			OversoldFreeChunkCnt: 19,
		},
		DiskInfo: clustermgr.DiskInfo{
			Status: 1,
		},
	}
	err := testDiskMgr.applyAdminUpdateDisk(ctx, diskInfo)
	require.NoError(t, err)

	diskItem := testDiskMgr.allDisks[diskInfo.DiskID]
	heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
	require.Equal(t, heartbeatInfo.MaxChunkCnt, diskInfo.MaxChunkCnt)
	require.Equal(t, heartbeatInfo.FreeChunkCnt, diskInfo.FreeChunkCnt)
	require.Equal(t, diskItem.info.Status, diskInfo.Status)

	diskRecord, err := testDiskMgr.diskTbl.GetDisk(diskInfo.DiskID)
	require.NoError(t, err)
	require.Equal(t, diskRecord.Status, diskInfo.Status)
	require.Equal(t, diskRecord.MaxChunkCnt, diskInfo.MaxChunkCnt)
	require.Equal(t, diskRecord.FreeChunkCnt, diskInfo.FreeChunkCnt)
	require.Equal(t, diskRecord.OversoldFreeChunkCnt, diskInfo.OversoldFreeChunkCnt)

	// failed case, diskid not exisr
	diskInfo1 := &clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			DiskID:       199,
			MaxChunkCnt:  99,
			FreeChunkCnt: 9,
		},
		DiskInfo: clustermgr.DiskInfo{
			Status: 1,
		},
	}
	err = testDiskMgr.applyAdminUpdateDisk(ctx, diskInfo1)
	require.Error(t, err)
}

func TestLoadData(t *testing.T) {
	testTmpDBPath := path.Join(os.TempDir(), fmt.Sprintf("diskmgr-%d-%010d", time.Now().Unix(), rand.Intn(100000000)))
	defer os.RemoveAll(testTmpDBPath)
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath)
	require.NoError(t, err)
	defer testDB.Close()

	nr := normaldb.BlobNodeInfoRecord{
		NodeInfoRecord: normaldb.NodeInfoRecord{
			Version:   normaldb.NodeInfoVersionNormal,
			NodeID:    proto.NodeID(1),
			ClusterID: proto.ClusterID(1),
			NodeSetID: proto.NodeSetID(2),
			Status:    proto.NodeStatusDropped,
			Role:      proto.NodeRoleBlobNode,
			DiskType:  proto.DiskTypeHDD,
		},
	}
	nodeTbl, err := normaldb.OpenBlobNodeTable(testDB)
	require.NoError(t, err)
	err = nodeTbl.UpdateNode(&nr)
	require.NoError(t, err)
	blobNodeInfoRecord := normaldb.BlobNodeDiskInfoRecord{
		DiskInfoRecord: normaldb.DiskInfoRecord{
			Version:   normaldb.DiskInfoVersionNormal,
			DiskID:    proto.DiskID(1),
			NodeID:    proto.NodeID(1),
			ClusterID: proto.ClusterID(1),
			DiskSetID: proto.DiskSetID(2),
			Status:    proto.DiskStatusRepaired,
		},
	}
	diskTbl, err := normaldb.OpenBlobNodeDiskTable(testDB, true)
	require.NoError(t, err)
	err = diskTbl.AddDisk(&blobNodeInfoRecord)
	require.NoError(t, err)
	blobNodeMgr, err := NewBlobNodeMgr(testMockScopeMgr, testDB, testDiskMgrConfig)
	require.NoError(t, err)

	// mock snapshot load data
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	bm := &BlobNodeManager{
		manager: &manager{
			topoMgr:  newTopoMgr(),
			scopeMgr: testMockScopeMgr,
			taskPool: base.NewTaskDistribution(int(testDiskMgrConfig.ApplyConcurrency), 1),
			cfg:      testDiskMgrConfig,
		},
		diskTbl:        diskTbl,
		nodeTbl:        nodeTbl,
		blobNodeClient: blobnode.New(&testDiskMgrConfig.BlobNodeConfig),
	}
	err = bm.LoadData(ctx)
	require.NoError(t, err)
	require.NotNil(t, bm.allocator)

	topoInfo := blobNodeMgr.GetTopoInfo(ctx)
	blobNodeHDDNodeSets := topoInfo.AllNodeSets[proto.DiskTypeHDD.String()]
	nodeSet, nodeSetExist := blobNodeHDDNodeSets[proto.NodeSetID(2)]
	_, diskSetExist := nodeSet.DiskSets[proto.DiskSetID(2)]
	require.Equal(t, nodeSetExist, true)
	require.Equal(t, diskSetExist, true)
	blobNodeMgr.Start()
}

func TestBlobNodeManager_Disk(t *testing.T) {
	blobNodeManager, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()

	initTestBlobNodeMgrNodes(t, blobNodeManager, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, blobNodeManager, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// AddDisk
	{
		diskInfo, err := blobNodeManager.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		diskInfo.NodeID = proto.NodeID(1000)
		err = blobNodeManager.AddDisk(ctx, diskInfo)
		require.ErrorIs(t, err, apierrors.ErrCMNodeNotFound)

		diskInfo, _ = blobNodeManager.GetDiskInfo(ctx, proto.DiskID(1))
		diskInfo.Path = "new disk path"
		diskInfo.DiskID = proto.DiskID(1000)
		err = blobNodeManager.AddDisk(ctx, diskInfo)
		require.NoError(t, err)
	}
	// DropDisk
	{
		err := blobNodeManager.DropDisk(ctx, &clustermgr.DiskInfoArgs{DiskID: proto.DiskID(10)})
		require.ErrorIs(t, err, apierrors.ErrDiskAbnormalOrNotReadOnly)

		err = blobNodeManager.applySwitchReadonly(proto.DiskID(10), true)
		require.NoError(t, err)

		err = blobNodeManager.DropDisk(ctx, &clustermgr.DiskInfoArgs{DiskID: proto.DiskID(10)})
		require.NoError(t, err)
	}
	// DropNode
	{
		for i := 1; i <= 10; i++ {
			err := blobNodeManager.applySwitchReadonly(proto.DiskID(i), true)
			require.NoError(t, err)
		}
		err := blobNodeManager.DropNode(ctx, &clustermgr.NodeInfoArgs{NodeID: proto.NodeID(1)})
		require.NoError(t, err)
	}
}
