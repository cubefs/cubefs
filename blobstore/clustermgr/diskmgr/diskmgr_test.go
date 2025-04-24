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
	"path"
	"sync/atomic"
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

func TestDiskMgr_Normal(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// AllocDiskID
	{
		testMockScopeMgr.EXPECT().Alloc(gomock.Any(), DiskIDScopeName, 1).Return(uint64(1), uint64(1), nil)
		diskID, err := testDiskMgr.AllocDiskID(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.DiskID(1), diskID)

		testMockScopeMgr.EXPECT().GetCurrent(gomock.Any()).Return(uint64(1))
		current := testDiskMgr.scopeMgr.GetCurrent(DiskIDScopeName)
		require.Equal(t, current, uint64(1))
	}

	// addDisk and GetDiskInfo and CheckDiskInfoDuplicated
	{
		initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])

		nodeInfo, err := testDiskMgr.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		require.Equal(t, proto.NodeID(1), nodeInfo.NodeID)
		// test disk exist
		for i := 1; i <= 10; i++ {
			diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
			require.NoError(t, err)
			require.Equal(t, proto.DiskID(i), diskInfo.DiskID)
			diskExist := testDiskMgr.CheckDiskInfoDuplicated(ctx, diskInfo, nodeInfo)
			require.Equal(t, apierrors.ErrExist, diskExist)
		}

		// test host and path duplicated
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		diskInfo.DiskID = proto.DiskID(11)
		nodeInfo, err = testDiskMgr.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		duplicated := testDiskMgr.CheckDiskInfoDuplicated(ctx, diskInfo, nodeInfo)
		require.Equal(t, apierrors.ErrIllegalArguments, duplicated)

		// test normal case
		diskInfo.DiskID = proto.DiskID(11)
		diskInfo.Path += "notDuplicated"
		nodeInfo, err = testDiskMgr.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		duplicated = testDiskMgr.CheckDiskInfoDuplicated(ctx, diskInfo, nodeInfo)
		require.Equal(t, nil, duplicated)
	}

	// IsDiskWritable and SetStatus and SwitchReadonly
	{
		for i := 1; i < 2; i++ {
			writable, err := testDiskMgr.IsDiskWritable(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, true, writable)
		}

		err := testDiskMgr.SetStatus(ctx, 1, proto.DiskStatusBroken, true)
		require.NoError(t, err)

		err = testDiskMgr.SwitchReadonly(1, true)
		require.NoError(t, err)

		for i := 1; i < 2; i++ {
			writable, err := testDiskMgr.IsDiskWritable(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, false, writable)
		}
	}

	_, suCount := testDiskMgr.getMaxSuCount()
	require.Equal(t, 27, suCount)
}

func TestDiskMgr_Dropping(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// dropping and list dropping
	{
		droppingList, err := testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(droppingList))

		err = testDiskMgr.droppingDisk(ctx, 1)
		require.NoError(t, err)

		// add dropping disk repeatedly
		err = testDiskMgr.droppingDisk(ctx, 1)
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
		err := testDiskMgr.droppingDisk(ctx, 2)
		require.NoError(t, err)
		droppingList, err := testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(droppingList))

		err = testDiskMgr.droppedDisk(ctx, 1)
		require.NoError(t, err)

		// add dropping disk 1 repeatedly
		err = testDiskMgr.droppingDisk(ctx, 1)
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
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	heartbeatInfos := make([]*blobnode.DiskHeartBeatInfo, 0)
	for i := 1; i <= 10; i++ {
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		diskInfo.DiskHeartBeatInfo.Free = 0
		diskInfo.DiskHeartBeatInfo.FreeChunkCnt = 0
		heartbeatInfos = append(heartbeatInfos, &diskInfo.DiskHeartBeatInfo)
	}
	err := testDiskMgr.heartBeatDiskInfo(ctx, heartbeatInfos)
	require.NoError(t, err)

	// heartbeat check
	for i := 1; i <= 10; i++ {
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		require.Equal(t, diskInfo.Free/testDiskMgr.ChunkSize, diskInfo.FreeChunkCnt)
		require.Equal(t, int64(0), diskInfo.Free)
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
	disk.lastExpireTime = time.Now().Add(time.Duration(testDiskMgr.HeartbeatExpireIntervalS) * time.Second * -3)
	disk.lock.Unlock()
	disks = testDiskMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 2, len(disks))
}

func TestDiskMgr_ListDisks(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(1))
	require.NoError(t, err)

	{
		ret, err := testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Host: diskInfo.Host, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Disks))
		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Host: diskInfo.Host, Count: 1000, Marker: ret.Marker})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret.Disks))

		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Status: proto.DiskStatusNormal, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Disks))

		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Disks))

		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Count: 2})
		require.NoError(t, err)
		require.Equal(t, 2, len(ret.Disks))
		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Count: 1000, Marker: ret.Marker})
		require.NoError(t, err)
		require.Equal(t, 8, len(ret.Disks))

		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Status: proto.DiskStatusNormal, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Disks))

		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Status: proto.DiskStatusDropped, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret.Disks))

		ret, err = testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Host: diskInfo.Host, Status: proto.DiskStatusDropped, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret.Disks))
	}

	{
		err := testDiskMgr.SetStatus(ctx, proto.DiskID(1), proto.DiskStatusBroken, true)
		require.NoError(t, err)

		ret, err := testDiskMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 1, len(ret.Disks))
	}
}

func TestDiskMgr_AdminUpdateDisk(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs[0])
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	diskInfo := &blobnode.DiskInfo{
		DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
			DiskID:       1,
			MaxChunkCnt:  99,
			FreeChunkCnt: 9,
		},
		Status: 1,
	}
	err := testDiskMgr.adminUpdateDisk(ctx, diskInfo)
	require.NoError(t, err)

	diskItem := testDiskMgr.allDisks[diskInfo.DiskID]
	require.Equal(t, diskItem.info.MaxChunkCnt, diskInfo.MaxChunkCnt)
	require.Equal(t, diskItem.info.FreeChunkCnt, diskInfo.FreeChunkCnt)
	require.Equal(t, diskItem.info.Status, diskInfo.Status)

	diskRecord, err := testDiskMgr.diskTbl.GetDisk(diskInfo.DiskID)
	require.NoError(t, err)
	require.Equal(t, diskRecord.Status, diskInfo.Status)
	require.Equal(t, diskRecord.MaxChunkCnt, diskInfo.MaxChunkCnt)
	require.Equal(t, diskRecord.FreeChunkCnt, diskInfo.FreeChunkCnt)

	// failed case, diskid not exisr
	diskInfo1 := &blobnode.DiskInfo{
		DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
			DiskID:       199,
			MaxChunkCnt:  99,
			FreeChunkCnt: 9,
		},
		Status: 1,
	}
	err = testDiskMgr.adminUpdateDisk(ctx, diskInfo1)
	require.Error(t, err)
}

func TestLoadData(t *testing.T) {
	testTmpDBPath := path.Join(os.TempDir(), fmt.Sprintf("diskmgr-%d-%010d", time.Now().Unix(), rand.Intn(100000000)))
	defer os.RemoveAll(testTmpDBPath)
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath)
	require.NoError(t, err)
	defer testDB.Close()

	nr := normaldb.NodeInfoRecord{
		Version:   normaldb.NodeInfoVersionNormal,
		NodeID:    proto.NodeID(1),
		ClusterID: proto.ClusterID(1),
		NodeSetID: proto.NodeSetID(2),
		Status:    proto.NodeStatusDropped,
		Role:      proto.NodeRoleBlobNode,
		DiskType:  proto.DiskTypeHDD,
	}
	nodeTbl, err := normaldb.OpenNodeTable(testDB)
	require.NoError(t, err)
	err = nodeTbl.UpdateNode(&nr)
	require.NoError(t, err)
	dr := normaldb.DiskInfoRecord{
		Version:   normaldb.DiskInfoVersionNormal,
		DiskID:    proto.DiskID(1),
		NodeID:    proto.NodeID(1),
		ClusterID: proto.ClusterID(1),
		DiskSetID: proto.DiskSetID(2),
		Status:    proto.DiskStatusRepaired,
	}
	diskTbl, err := normaldb.OpenDiskTable(testDB, true)
	require.NoError(t, err)
	err = diskTbl.AddDisk(&dr)
	require.NoError(t, err)
	droppedDiskTbl, err := normaldb.OpenDroppedDiskTable(testDB)
	require.NoError(t, err)
	dm := &DiskMgr{
		allocators:     map[proto.NodeRole]*atomic.Value{},
		topoMgrs:       map[proto.NodeRole]*topoMgr{proto.NodeRoleBlobNode: newTopoMgr()},
		taskPool:       base.NewTaskDistribution(int(testDiskMgrConfig.ApplyConcurrency), 1),
		scopeMgr:       testMockScopeMgr,
		diskTbl:        diskTbl,
		nodeTbl:        nodeTbl,
		droppedDiskTbl: droppedDiskTbl,
		blobNodeClient: blobnode.New(&testDiskMgrConfig.BlobNodeConfig),
		closeCh:        make(chan interface{}),
		DiskMgrConfig:  testDiskMgrConfig,
	}
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// mock snapshot load data
	err = dm.LoadData(ctx)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(dm.allocators))

	diskMgr, err := New(testMockScopeMgr, testDB, testDiskMgrConfig)
	require.NoError(t, err)
	topoInfo := diskMgr.GetTopoInfo(ctx)
	blobNodeHDDNodeSets := topoInfo.AllNodeSets[proto.NodeRoleBlobNode.String()][proto.DiskTypeHDD.String()]
	nodeSet, nodeSetExist := blobNodeHDDNodeSets[proto.NodeSetID(2)]
	_, diskSetExist := nodeSet.DiskSets[proto.DiskSetID(2)]
	require.Equal(t, nodeSetExist, true)
	require.Equal(t, diskSetExist, true)
}
