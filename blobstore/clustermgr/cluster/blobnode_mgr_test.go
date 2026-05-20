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
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/errors"
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

		err := blobNodeManager.applySetStatus(ctx, 1, proto.DiskStatusBroken, true)
		require.NoError(t, err)

		// apply change status back
		pendingKey := fmtApplyContextKey("disk-setstatus", "1")
		blobNodeManager.pendingEntries.Store(pendingKey, nil)
		defer blobNodeManager.pendingEntries.Delete(pendingKey)
		err = blobNodeManager.applySetStatus(ctx, 1, proto.DiskStatusNormal, true)
		require.NoError(t, err)
		v, _ := blobNodeManager.pendingEntries.Load(pendingKey)
		require.Equal(t, apierrors.ErrChangeDiskStatusNotAllow, v)

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

		pendingKey := fmtApplyContextKey("disk-dropping", "1")
		testDiskMgr.pendingEntries.Store(pendingKey, nil)
		defer testDiskMgr.pendingEntries.Delete(pendingKey)
		_, err = testDiskMgr.applyDroppingDisk(ctx, 1, true)
		require.NoError(t, err)
		v, _ := testDiskMgr.pendingEntries.Load(pendingKey)
		require.Equal(t, apierrors.ErrDiskAbnormalOrNotReadOnly, v)

		err = testDiskMgr.applySwitchReadonly(1, true)
		require.NoError(t, err)

		_, err = testDiskMgr.applyDroppingDisk(ctx, 1, true)
		require.NoError(t, err)

		// set disk broken not commit case
		err = testDiskMgr.applySetStatus(ctx, 9, proto.DiskStatusBroken, false)
		require.NoError(t, err)
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, 9)
		require.NoError(t, err)
		require.NotEqual(t, proto.DiskStatusBroken, diskInfo.Status)

		// apply set dropping disk broken
		pendingKey = fmtApplyContextKey("disk-setstatus", "1")
		testDiskMgr.pendingEntries.Store(pendingKey, nil)
		defer testDiskMgr.pendingEntries.Delete(pendingKey)
		err = testDiskMgr.applySetStatus(ctx, 1, proto.DiskStatusBroken, true)
		require.NoError(t, err)
		v, _ = testDiskMgr.pendingEntries.Load(pendingKey)
		require.Equal(t, apierrors.ErrChangeDiskStatusNotAllow, v)

		// add dropping disk repeatedly
		_, err = testDiskMgr.applyDroppingDisk(ctx, 1, true)
		require.NoError(t, err)

		// set status when disk is dropping, return ErrChangeDiskStatusNotAllow
		err = testDiskMgr.applySetStatus(ctx, 1, proto.DiskStatusBroken, false)
		require.ErrorIs(t, err, apierrors.ErrChangeDiskStatusNotAllow)

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
		err := testDiskMgr.applySetStatus(ctx, proto.DiskID(1), proto.DiskStatusBroken, true)
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

	diskRecord, err := testDiskMgr.nodeDiskTable.GetDisk(diskInfo.DiskID)
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
	nodeDiskTbl, err := normaldb.OpenBlobNodeDiskTable(testDB, true)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&nr)
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
	err = nodeDiskTbl.AddDisk(&blobNodeInfoRecord)
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
		nodeDiskTable:  nodeDiskTbl,
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

// TestApplyUpdateNode_HostPathFilter verifies that applyUpdateNode correctly updates
// hostPathFilter after a host change: normal disks are stored under the new host key,
// while Repaired/Dropped disks are excluded from the filter.
func TestApplyUpdateNode_HostPathFilter(t *testing.T) {
	blobNodeManager, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	oldHost := testIdcs[0] + hostPrefix + "1"
	newHost := testIdcs[0] + hostPrefix + "new"

	// Add node 1.
	initTestBlobNodeMgrNodes(t, blobNodeManager, 1, 1, testIdcs[0])

	// Add two disks with distinct paths so their filter keys are different.
	normalPath := "/data/disk-normal"
	repairedPath := "/data/disk-repaired"
	baseInfo := clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{
			Size:         14.5 * 1024 * 1024 * 1024 * 1024,
			Free:         14.5 * 1024 * 1024 * 1024 * 1024,
			MaxChunkCnt:  14.5 * 1024 / 16,
			FreeChunkCnt: 14.5 * 1024 / 16,
		},
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: proto.ClusterID(1),
			Idc:       testIdcs[0],
			Rack:      "1",
			Host:      oldHost,
			NodeID:    proto.NodeID(1),
			Status:    proto.DiskStatusNormal,
		},
	}

	normalDiskInfo := baseInfo
	normalDiskInfo.DiskID = proto.DiskID(101)
	normalDiskInfo.Path = normalPath
	require.NoError(t, blobNodeManager.applyAddDisk(ctx, &normalDiskInfo))

	repairedDiskInfo := baseInfo
	repairedDiskInfo.DiskID = proto.DiskID(102)
	repairedDiskInfo.Path = repairedPath
	require.NoError(t, blobNodeManager.applyAddDisk(ctx, &repairedDiskInfo))

	// Transition disk 102 to Repaired: Normal→Broken→Repairing→Repaired.
	require.NoError(t, blobNodeManager.applySetStatus(ctx, proto.DiskID(102), proto.DiskStatusBroken, true))
	require.NoError(t, blobNodeManager.applySetStatus(ctx, proto.DiskID(102), proto.DiskStatusRepairing, true))
	require.NoError(t, blobNodeManager.applySetStatus(ctx, proto.DiskID(102), proto.DiskStatusRepaired, true))

	// Call applyUpdateNode to change the node's host.
	err := blobNodeManager.applyUpdateNode(ctx, &clustermgr.BlobNodeInfo{
		NodeInfo: clustermgr.NodeInfo{
			NodeID:   proto.NodeID(1),
			Host:     newHost,
			Rack:     "1",
			Idc:      testIdcs[0],
			DiskType: proto.DiskTypeHDD,
		},
	})
	require.NoError(t, err)

	// The repaired disk's filter key under the new host must NOT exist.
	_, repairedInFilter := blobNodeManager.hostPathFilter.Load(newHost + repairedPath)
	require.False(t, repairedInFilter, "repaired disk should not be stored in hostPathFilter after host update")

	// The normal disk's filter key under the new host must exist.
	_, normalInFilter := blobNodeManager.hostPathFilter.Load(newHost + normalPath)
	require.True(t, normalInFilter, "normal disk should be stored in hostPathFilter after host update")

	// The old host keys must have been removed for both disks.
	_, oldNormalInFilter := blobNodeManager.hostPathFilter.Load(oldHost + normalPath)
	require.False(t, oldNormalInFilter, "old filter key for normal disk should be removed after host update")
}

func TestMigrateRepairedDiskNodeID(t *testing.T) {
	blobNodeManager, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()

	// node: NodeID=1, host="z0test-host-1"
	initTestBlobNodeMgrNodes(t, blobNodeManager, 1, 1, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	nodeInfo, err := blobNodeManager.GetNodeInfo(ctx, proto.NodeID(1))
	require.NoError(t, err)
	nodeHost := nodeInfo.Host

	// Inject legacy diskItems directly into allDisks with NodeID=0.
	// diskIDs 1,2,3 → DiskStatusRepaired (should be proposed for migration).
	// diskID 4       → DiskStatusNormal  (should be skipped).
	// diskID 5       → DiskStatusRepaired but no matching node host (should warn, skip).
	legacyDisks := []struct {
		diskID proto.DiskID
		status proto.DiskStatus
		host   string
	}{
		{1, proto.DiskStatusRepaired, nodeHost},
		{2, proto.DiskStatusRepaired, nodeHost},
		{3, proto.DiskStatusRepaired, nodeHost},
		{4, proto.DiskStatusNormal, nodeHost},
		{5, proto.DiskStatusRepaired, "unknown-host-99"},
	}
	for _, d := range legacyDisks {
		di := &diskItem{
			diskID: d.diskID,
			info: diskItemInfo{
				DiskInfo: clustermgr.DiskInfo{
					Host:   d.host,
					Status: d.status,
					NodeID: proto.InvalidNodeID,
				},
				extraInfo: &clustermgr.DiskHeartBeatInfo{DiskID: d.diskID},
			},
			weightGetter:   blobNodeDiskWeightGetter,
			weightDecrease: blobNodeDiskWeightDecrease,
		}
		blobNodeManager.metaLock.Lock()
		blobNodeManager.allDisks[d.diskID] = di
		blobNodeManager.metaLock.Unlock()
	}

	// Expect Propose to be called exactly 3 times: diskIDs 1, 2, 3.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaft := mocks.NewMockRaftServer(ctrl)
	mockRaft.EXPECT().Propose(gomock.Any(), gomock.Any()).Times(3).Return(nil)
	blobNodeManager.SetRaftServer(mockRaft)

	ok := blobNodeManager.migrateRepairedDiskNodeID(ctx)
	require.True(t, ok)

	// Simulate raft apply for the three proposed disks, verify applyAdminUpdateDisk
	// correctly updates NodeID in memory and adds disks to ni.disks.
	for _, diskID := range []proto.DiskID{1, 2, 3} {
		err := blobNodeManager.applyAdminUpdateDisk(ctx, &clustermgr.BlobNodeDiskInfo{
			DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: diskID},
			DiskInfo:          clustermgr.DiskInfo{NodeID: proto.NodeID(1)},
		})
		require.NoError(t, err)

		di, exists := blobNodeManager.getDisk(diskID)
		require.True(t, exists)
		di.withRLocked(func() error {
			require.Equal(t, proto.NodeID(1), di.info.NodeID)
			return nil
		})
	}

	// Disk 4 (Normal status) must remain untouched.
	di4, exists := blobNodeManager.getDisk(proto.DiskID(4))
	require.True(t, exists)
	di4.withRLocked(func() error {
		require.Equal(t, proto.InvalidNodeID, di4.info.NodeID)
		return nil
	})

	// ni.disks should now include the three migrated disks.
	ni, exists := blobNodeManager.getNode(proto.NodeID(1))
	require.True(t, exists)
	ni.withRLocked(func() error {
		require.Contains(t, ni.disks, proto.DiskID(1))
		require.Contains(t, ni.disks, proto.DiskID(2))
		require.Contains(t, ni.disks, proto.DiskID(3))
		require.NotContains(t, ni.disks, proto.DiskID(4))
		return nil
	})
}

func TestMigrateRepairedDiskNodeID_ProposeError(t *testing.T) {
	blobNodeManager, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()

	initTestBlobNodeMgrNodes(t, blobNodeManager, 1, 1, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	nodeInfo, err := blobNodeManager.GetNodeInfo(ctx, proto.NodeID(1))
	require.NoError(t, err)

	di := &diskItem{
		diskID: proto.DiskID(1),
		info: diskItemInfo{
			DiskInfo: clustermgr.DiskInfo{
				Host:   nodeInfo.Host,
				Status: proto.DiskStatusRepaired,
				NodeID: proto.InvalidNodeID,
			},
			extraInfo: &clustermgr.DiskHeartBeatInfo{DiskID: 1},
		},
		weightGetter:   blobNodeDiskWeightGetter,
		weightDecrease: blobNodeDiskWeightDecrease,
	}
	blobNodeManager.metaLock.Lock()
	blobNodeManager.allDisks[proto.DiskID(1)] = di
	blobNodeManager.metaLock.Unlock()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaft := mocks.NewMockRaftServer(ctrl)
	mockRaft.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(errors.New("raft unavailable"))
	blobNodeManager.SetRaftServer(mockRaft)

	ok := blobNodeManager.migrateRepairedDiskNodeID(ctx)
	require.False(t, ok)
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

// TestDiskMgr_AllocNodeID covers AllocNodeID success and scopeMgr error paths.
func TestDiskMgr_AllocNodeID(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	testMockScopeMgr.EXPECT().Alloc(gomock.Any(), NodeIDScopeName, 1).Return(uint64(7), uint64(7), nil)
	nid, err := mgr.AllocNodeID(ctx)
	require.NoError(t, err)
	require.Equal(t, proto.NodeID(7), nid)

	// error from scopeMgr
	testMockScopeMgr.EXPECT().Alloc(gomock.Any(), NodeIDScopeName, 1).Return(uint64(0), uint64(0), apierrors.ErrUnexpected)
	_, err = mgr.AllocNodeID(ctx)
	require.Error(t, err)
}

// TestDiskMgr_IsFrequentHeartBeat covers disk-not-found and both true/false
// branches based on the notify interval.
func TestDiskMgr_IsFrequentHeartBeat(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	mgr.cfg.HeartbeatExpireIntervalS = 60
	initTestBlobNodeMgrNodes(t, mgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 1, false, testIdcs[0])

	// disk not found
	_, err := mgr.IsFrequentHeartBeat(proto.DiskID(999), 10)
	require.ErrorIs(t, err, apierrors.ErrCMDiskNotFound)

	// force expireTime to "now" so that the gap from now is 0 seconds => true when
	// notifyInterval >= heartbeatExpireInterval
	disk, _ := mgr.getDisk(proto.DiskID(1))
	disk.lock.Lock()
	disk.expireTime = time.Now()
	disk.lock.Unlock()
	frequent, err := mgr.IsFrequentHeartBeat(proto.DiskID(1), mgr.cfg.HeartbeatExpireIntervalS+1)
	require.NoError(t, err)
	require.True(t, frequent)

	frequent, err = mgr.IsFrequentHeartBeat(proto.DiskID(1), 0)
	require.NoError(t, err)
	require.False(t, frequent)
}

// TestDiskMgr_CheckNodeInfoDuplicated covers both found and not-found paths.
func TestDiskMgr_CheckNodeInfoDuplicated(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestBlobNodeMgrNodes(t, mgr, 1, 1, testIdcs[0])

	nodeInfo, err := mgr.GetNodeInfo(ctx, proto.NodeID(1))
	require.NoError(t, err)

	// existing node => duplicated
	nid, dup := mgr.CheckNodeInfoDuplicated(ctx, &nodeInfo.NodeInfo)
	require.True(t, dup)
	require.Equal(t, nodeInfo.NodeID, nid)

	// non-existing node => not duplicated
	ni := nodeInfo.NodeInfo
	ni.Host = "not-exist-host"
	nid, dup = mgr.CheckNodeInfoDuplicated(ctx, &ni)
	require.False(t, dup)
	require.Equal(t, proto.InvalidNodeID, nid)
}

// TestDiskMgr_AllowNodeIPChange covers every early-return branch.
func TestDiskMgr_AllowNodeIPChange(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestBlobNodeMgrNodes(t, mgr, 1, 2, testIdcs[0])

	nodeInfo, err := mgr.GetNodeInfo(ctx, proto.NodeID(1))
	require.NoError(t, err)

	// feature disabled
	mgr.cfg.EnableNodeIPChange = false
	require.False(t, mgr.AllowNodeIPChange(ctx, &nodeInfo.NodeInfo))

	// feature enabled, node not exist
	mgr.cfg.EnableNodeIPChange = true
	missing := nodeInfo.NodeInfo
	missing.NodeID = proto.NodeID(9999)
	require.False(t, mgr.AllowNodeIPChange(ctx, &missing))

	// only Host/Rack differ => allowed. AllowNodeIPChange internally zeroes
	// NodeSetID and Status before comparing, so callers must pass zero values
	// for those fields when requesting a host/rack update.
	want := nodeInfo.NodeInfo
	want.Host = "changed-host"
	want.Rack = "changed-rack"
	want.NodeSetID = 0
	want.Status = 0
	require.True(t, mgr.AllowNodeIPChange(ctx, &want))

	// other field differs => not allowed
	bad := want
	bad.DiskType = proto.DiskTypeSSD
	require.False(t, mgr.AllowNodeIPChange(ctx, &bad))

	// node already dropped => not allowed
	dropped, _ := mgr.getNode(proto.NodeID(2))
	dropped.withLocked(func() error {
		dropped.info.Status = proto.NodeStatusDropped
		return nil
	})
	nodeInfo2, err := mgr.GetNodeInfo(ctx, proto.NodeID(2))
	require.NoError(t, err)
	require.False(t, mgr.AllowNodeIPChange(ctx, &nodeInfo2.NodeInfo))
}

// TestDiskMgr_ValidateNodeInfo covers every error/ok branch.
func TestDiskMgr_ValidateNodeInfo(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// invalid role
	bad := &clustermgr.NodeInfo{Role: proto.NodeRole(0), DiskType: proto.DiskTypeHDD}
	require.ErrorIs(t, mgr.ValidateNodeInfo(ctx, bad), apierrors.ErrIllegalArguments)

	// invalid disk type
	bad = &clustermgr.NodeInfo{Role: proto.NodeRoleBlobNode, DiskType: proto.DiskType(0)}
	require.ErrorIs(t, mgr.ValidateNodeInfo(ctx, bad), apierrors.ErrIllegalArguments)

	// no NodeSetID => happy path (null nodeSetID skips topo validation)
	ok := &clustermgr.NodeInfo{Role: proto.NodeRoleBlobNode, DiskType: proto.DiskTypeHDD}
	require.NoError(t, mgr.ValidateNodeInfo(ctx, ok))

	// non-null NodeSetID that doesn't exist => ErrCMNodeSetNotFound
	ok.NodeSetID = proto.NodeSetID(12345)
	require.Error(t, mgr.ValidateNodeInfo(ctx, ok))
}

// TestDiskMgr_SetStatus exercises the raft-protected SetStatus entry point and
// its error branches (invalid status / disk not found / changing back).
func TestDiskMgr_SetStatus(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestBlobNodeMgrNodes(t, mgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 3, false, testIdcs[0])

	// disk not found
	err := mgr.SetStatus(ctx, &clustermgr.DiskSetArgs{DiskID: 99999, Status: proto.DiskStatusNormal}, "blobnode")
	require.ErrorIs(t, err, apierrors.ErrCMDiskNotFound)

	// invalid status
	err = mgr.SetStatus(ctx, &clustermgr.DiskSetArgs{DiskID: 1, Status: proto.DiskStatus(99)}, "blobnode")
	require.ErrorIs(t, err, apierrors.ErrInvalidStatus)

	// happy path (Propose is mocked to return nil in the test harness)
	err = mgr.SetStatus(ctx, &clustermgr.DiskSetArgs{DiskID: 1, Status: proto.DiskStatusBroken}, "blobnode")
	require.NoError(t, err)
}

// TestDiskMgr_Close covers the Close() lifecycle.
func TestDiskMgr_Close(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()

	// Close should not panic and should be safe to call once.
	mgr.Close()
}

// TestDiskMgr_RefreshExpireTime covers RefreshExpireTime bumping every disk.
func TestDiskMgr_RefreshExpireTime(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	mgr.cfg.HeartbeatExpireIntervalS = 60
	initTestBlobNodeMgrNodes(t, mgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 3, false, testIdcs[0])

	// manually set a stale expireTime
	now := time.Now()
	for i := 1; i <= 3; i++ {
		di, _ := mgr.getDisk(proto.DiskID(i))
		di.lock.Lock()
		di.expireTime = now.Add(-time.Hour)
		di.lastExpireTime = now.Add(-time.Hour)
		di.lock.Unlock()
	}

	mgr.RefreshExpireTime()
	// every disk's expireTime should be pushed beyond "now"
	for i := 1; i <= 3; i++ {
		di, _ := mgr.getDisk(proto.DiskID(i))
		di.lock.RLock()
		require.True(t, di.expireTime.After(now))
		require.True(t, di.lastExpireTime.After(now))
		di.lock.RUnlock()
	}
}

// TestDiskMgr_ValidateAllocRet covers the non-HostAware branch and error paths
// that the broader AllocChunks tests don't hit.
func TestDiskMgr_ValidateAllocRet(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	initTestBlobNodeMgrNodes(t, mgr, 1, 1, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 3, false, testIdcs[0])

	// HostAware=true: duplicated host (all disks share NodeID=1) => error
	mgr.cfg.HostAware = true
	require.Error(t, mgr.validateAllocRet([]proto.DiskID{1, 2}))
	// HostAware=true with unknown disk => ErrDiskNotExist path
	require.Error(t, mgr.validateAllocRet([]proto.DiskID{9999}))

	// HostAware=false: duplicated disk id => error
	mgr.cfg.HostAware = false
	require.Error(t, mgr.validateAllocRet([]proto.DiskID{1, 1}))
	// HostAware=false, distinct disks => ok
	require.NoError(t, mgr.validateAllocRet([]proto.DiskID{1, 2, 3}))
}

// TestBlobNodeManager_ListNodes verifies ListNodes with status filters:
// 0 (all), NodeStatusNormal, and NodeStatusDropped.
func TestBlobNodeManager_ListNodes(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// Add 4 nodes with one disk each (specifyNodeID=true → disk i belongs to node i).
	initTestBlobNodeMgrNodes(t, mgr, 1, 4, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 4, true, testIdcs[0])

	// ── initial state: all 4 nodes are Normal ──────────────────────────────
	all := mgr.ListNodes(ctx, 0)
	require.Equal(t, 4, len(all))

	normals := mgr.ListNodes(ctx, proto.NodeStatusNormal)
	require.Equal(t, 4, len(normals))

	dropped := mgr.ListNodes(ctx, proto.NodeStatusDropped)
	require.Equal(t, 0, len(dropped))

	// ── put node 2 into Dropping state (disk 2 readonly → applyDroppingNode) ──
	// Dropping nodes keep NodeStatusNormal as their persisted status, so they
	// still appear in the Normal filter result.
	require.NoError(t, mgr.applySwitchReadonly(proto.DiskID(2), true))
	_, err := mgr.applyDroppingNode(ctx, proto.NodeID(2), true)
	require.NoError(t, err)

	// Dropping nodes keep NodeStatusNormal as persisted status, so they still appear.
	normals = mgr.ListNodes(ctx, proto.NodeStatusNormal)
	require.Equal(t, 4, len(normals))

	// ── fully drop node 1 ─────────────────────────────────────────────────
	require.NoError(t, mgr.applySwitchReadonly(proto.DiskID(1), true))
	_, err = mgr.applyDroppingNode(ctx, proto.NodeID(1), true)
	require.NoError(t, err)
	require.NoError(t, mgr.applyDroppedDisk(ctx, proto.DiskID(1)))
	require.NoError(t, mgr.applyDroppedNode(ctx, proto.NodeID(1)))

	all = mgr.ListNodes(ctx, 0)
	require.Equal(t, 4, len(all)) // node 1 is still in allNodes, just Dropped

	dropped = mgr.ListNodes(ctx, proto.NodeStatusDropped)
	require.Equal(t, 1, len(dropped))
	require.Equal(t, proto.NodeID(1), dropped[0].NodeID)
	require.Equal(t, proto.NodeStatusDropped, dropped[0].Status)

	// node 2 (dropping) + node 3 + node 4 all have NodeStatusNormal
	normals = mgr.ListNodes(ctx, proto.NodeStatusNormal)
	require.Equal(t, 3, len(normals))

	normalIDs := make(map[proto.NodeID]struct{}, 3)
	for _, n := range normals {
		normalIDs[n.NodeID] = struct{}{}
	}
	require.Contains(t, normalIDs, proto.NodeID(2))
	require.Contains(t, normalIDs, proto.NodeID(3))
	require.Contains(t, normalIDs, proto.NodeID(4))
	require.NotContains(t, normalIDs, proto.NodeID(1)) // node 1 is Dropped
}

// TestDiskMgr_DroppingNode covers applyDroppingNode/applyDroppedNode corner cases:
// node not found, dropping propagation, and dropped without disks.
func TestDiskMgr_DroppingNode(t *testing.T) {
	mgr, closeMgr := initTestBlobNodeMgr(t)
	defer closeMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestBlobNodeMgrNodes(t, mgr, 1, 2, testIdcs[0])
	initTestBlobNodeMgrDisks(t, mgr, 1, 2, true, testIdcs[0]) // disk 1 -> node 1, disk 2 -> node 2

	// node not found
	_, err := mgr.applyDroppingNode(ctx, proto.NodeID(9999), true)
	require.ErrorIs(t, err, apierrors.ErrCMNodeNotFound)

	// node 1 has a normal (non-readonly) disk => propagated error via pendingEntries
	// set the disk to readonly first so dropping succeeds
	require.NoError(t, mgr.applySwitchReadonly(proto.DiskID(1), true))
	_, err = mgr.applyDroppingNode(ctx, proto.NodeID(1), true)
	require.NoError(t, err)

	// double dropping => isUsingStatus() returns false OR dropping=true => returns (true, nil)
	already, err := mgr.applyDroppingNode(ctx, proto.NodeID(1), true)
	require.NoError(t, err)
	require.True(t, already)

	// applyDroppedNode: not a dropping node (node 2 has never been dropped) => no-op, nil
	require.NoError(t, mgr.applyDroppedNode(ctx, proto.NodeID(2)))

	// applyDroppedNode: non-existing node id through a mocked dropping record.
	// Since persistentHandler.isDroppingNode would query the DB directly, we rely
	// on the happy path that the DB has no such record and returns nil.
	require.NoError(t, mgr.applyDroppedNode(ctx, proto.NodeID(9999)))
}
