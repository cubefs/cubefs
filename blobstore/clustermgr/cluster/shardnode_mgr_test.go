// Copyright 2024 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestShardNodeMgr_Normal(t *testing.T) {
	testShardNodeMgr, closeMgr := initTestShardNodeMgr(t)
	defer closeMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// AllocDiskID
	{
		testMockScopeMgr.EXPECT().Alloc(gomock.Any(), ShardNodeDiskIDScopeName, 1).Return(uint64(1), uint64(1), nil)
		diskID, err := testShardNodeMgr.AllocDiskID(ctx)
		require.NoError(t, err)
		require.Equal(t, proto.DiskID(1), diskID)

		testMockScopeMgr.EXPECT().GetCurrent(gomock.Any()).Return(uint64(1))
		current := testShardNodeMgr.scopeMgr.GetCurrent(ShardNodeDiskIDScopeName)
		require.Equal(t, current, uint64(1))
	}

	// addDisk and GetDiskInfo and CheckDiskInfoDuplicated
	{
		initTestShardNodeMgrNodes(t, testShardNodeMgr, 1, 1, testIdcs[0])
		initTestShardNodeMgrDisks(t, testShardNodeMgr, 1, 10, false, testIdcs[0])

		nodeInfo, err := testShardNodeMgr.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		require.Equal(t, proto.NodeID(1), nodeInfo.NodeID)
		// test disk exist
		for i := 1; i <= 10; i++ {
			diskInfo, err := testShardNodeMgr.GetDiskInfo(ctx, proto.DiskID(i))
			require.NoError(t, err)
			require.Equal(t, proto.DiskID(i), diskInfo.DiskID)
			diskExist := testShardNodeMgr.CheckDiskInfoDuplicated(ctx, diskInfo.DiskID, &diskInfo.DiskInfo, &nodeInfo.NodeInfo)
			require.Equal(t, apierrors.ErrExist, diskExist)
		}

		// test host and path duplicated
		diskInfo, err := testShardNodeMgr.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		diskInfo.DiskID = proto.DiskID(11)
		nodeInfo, err = testShardNodeMgr.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		duplicated := testShardNodeMgr.CheckDiskInfoDuplicated(ctx, diskInfo.DiskID, &diskInfo.DiskInfo, &nodeInfo.NodeInfo)
		require.Equal(t, apierrors.ErrIllegalArguments, duplicated)

		// test normal case
		diskInfo.DiskID = proto.DiskID(11)
		diskInfo.Path += "notDuplicated"
		nodeInfo, err = testShardNodeMgr.GetNodeInfo(ctx, proto.NodeID(1))
		require.NoError(t, err)
		duplicated = testShardNodeMgr.CheckDiskInfoDuplicated(ctx, diskInfo.DiskID, &diskInfo.DiskInfo, &nodeInfo.NodeInfo)
		require.Equal(t, nil, duplicated)
	}

	// IsDiskWritable and SetStatus and SwitchReadonly
	{
		for i := 1; i < 2; i++ {
			writable, err := testShardNodeMgr.IsDiskWritable(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, true, writable)
		}

		err := testShardNodeMgr.SetStatus(ctx, 1, proto.DiskStatusBroken, true)
		require.NoError(t, err)

		err = testShardNodeMgr.applySwitchReadonly(1, true)
		require.NoError(t, err)

		for i := 1; i < 2; i++ {
			writable, err := testShardNodeMgr.IsDiskWritable(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, false, writable)
		}
	}

	_, suCount := testShardNodeMgr.getMaxSuCount()
	require.Equal(t, 3, suCount)
}

func TestShardNodeMgr_Heartbeat(t *testing.T) {
	shardNodeMgr, closeMgr := initTestShardNodeMgr(t)
	defer closeMgr()
	initTestShardNodeMgrNodes(t, shardNodeMgr, 1, 1, testIdcs[0])
	initTestShardNodeMgrDisks(t, shardNodeMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	heartbeatInfos := make([]clustermgr.ShardNodeDiskHeartbeatInfo, 0)
	for i := 1; i <= 10; i++ {
		diskInfo, err := shardNodeMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		diskInfo.ShardNodeDiskHeartbeatInfo.Free = 0
		diskInfo.ShardNodeDiskHeartbeatInfo.FreeShardCnt = 0
		heartbeatInfos = append(heartbeatInfos, diskInfo.ShardNodeDiskHeartbeatInfo)
	}
	err := shardNodeMgr.applyHeartBeatDiskInfo(ctx, heartbeatInfos)
	require.NoError(t, err)

	// heartbeat check
	for i := 1; i <= 10; i++ {
		diskInfo, err := shardNodeMgr.GetDiskInfo(ctx, proto.DiskID(i))
		require.NoError(t, err)
		require.Equal(t, int32(diskInfo.Free/shardNodeMgr.cfg.ShardSize), diskInfo.FreeShardCnt)
		require.Equal(t, int64(0), diskInfo.Free)
	}

	// get heartbeat change disk
	disks := shardNodeMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 0, len(disks))

	disk, _ := shardNodeMgr.getDisk(proto.DiskID(1))
	disk.lock.Lock()
	disk.expireTime = time.Now().Add(-time.Second)
	disk.lock.Unlock()
	disks = shardNodeMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 1, len(disks))
	require.Equal(t, HeartbeatEvent{DiskID: proto.DiskID(1), IsAlive: false}, disks[0])

	disk, _ = shardNodeMgr.getDisk(proto.DiskID(2))
	disk.lock.Lock()
	disk.lastExpireTime = time.Now().Add(time.Duration(shardNodeMgr.cfg.HeartbeatExpireIntervalS) * time.Second * -3)
	disk.lock.Unlock()
	disks = shardNodeMgr.GetHeartbeatChangeDisks()
	require.Equal(t, 2, len(disks))
}

func TestShardNode_ListDisks(t *testing.T) {
	shardNodeMgr, closeMgr := initTestShardNodeMgr(t)
	defer closeMgr()
	initTestShardNodeMgrNodes(t, shardNodeMgr, 1, 1, testIdcs[0])
	initTestShardNodeMgrDisks(t, shardNodeMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	diskInfo, err := shardNodeMgr.GetDiskInfo(ctx, proto.DiskID(1))
	require.NoError(t, err)

	{
		ret, marker, err := shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Host: diskInfo.Host, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))
		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Host: diskInfo.Host, Count: 1000, Marker: marker})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret))

		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Status: proto.DiskStatusNormal, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))

		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))

		ret, marker, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Count: 2})
		require.NoError(t, err)
		require.Equal(t, 2, len(ret))
		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Count: 1000, Marker: marker})
		require.NoError(t, err)
		require.Equal(t, 8, len(ret))

		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Status: proto.DiskStatusNormal, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret))

		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Status: proto.DiskStatusDropped, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret))

		ret, _, err = shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Idc: diskInfo.Idc, Rack: diskInfo.Rack, Host: diskInfo.Host, Status: proto.DiskStatusDropped, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret))
	}

	{
		err := shardNodeMgr.SetStatus(ctx, proto.DiskID(1), proto.DiskStatusBroken, true)
		require.NoError(t, err)

		ret, _, err := shardNodeMgr.ListDiskInfo(ctx, &clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Count: 1000})
		require.NoError(t, err)
		require.Equal(t, 1, len(ret))
	}
}

func TestShardNode_AdminUpdateDisk(t *testing.T) {
	shardNodeMgr, closeMgr := initTestShardNodeMgr(t)
	defer closeMgr()
	initTestShardNodeMgrNodes(t, shardNodeMgr, 1, 1, testIdcs[0])
	initTestShardNodeMgrDisks(t, shardNodeMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	diskInfo := &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
			DiskID:       1,
			MaxShardCnt:  99,
			FreeShardCnt: 9,
		},
		DiskInfo: clustermgr.DiskInfo{
			Status: 1,
		},
	}
	err := shardNodeMgr.applyAdminUpdateDisk(ctx, diskInfo)
	require.NoError(t, err)

	diskItem := shardNodeMgr.allDisks[diskInfo.DiskID]
	heartbeatInfo := diskItem.info.extraInfo.(*clustermgr.ShardNodeDiskHeartbeatInfo)
	require.Equal(t, heartbeatInfo.MaxShardCnt, diskInfo.MaxShardCnt)
	require.Equal(t, heartbeatInfo.FreeShardCnt, diskInfo.FreeShardCnt)
	require.Equal(t, diskItem.info.Status, diskInfo.Status)

	diskRecord, err := shardNodeMgr.diskTbl.GetDisk(diskInfo.DiskID)
	require.NoError(t, err)
	require.Equal(t, diskRecord.Status, diskInfo.Status)
	require.Equal(t, diskRecord.MaxShardCnt, diskInfo.MaxShardCnt)
	require.Equal(t, diskRecord.FreeShardCnt, diskInfo.FreeShardCnt)

	// failed case, diskid not exisr
	diskInfo1 := &clustermgr.ShardNodeDiskInfo{
		ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
			DiskID:       199,
			MaxShardCnt:  99,
			FreeShardCnt: 9,
		},
		DiskInfo: clustermgr.DiskInfo{
			Status: 1,
		},
	}
	err = shardNodeMgr.applyAdminUpdateDisk(ctx, diskInfo1)
	require.Error(t, err)
}

func TestShardNode_LoadData(t *testing.T) {
	testTmpDBPath := path.Join(os.TempDir(), fmt.Sprintf("diskmgr-%d-%010d", time.Now().Unix(), rand.Intn(100000000)))
	defer os.RemoveAll(testTmpDBPath)
	testDB, err := normaldb.OpenNormalDB(testTmpDBPath)
	require.NoError(t, err)
	defer testDB.Close()

	nr := normaldb.ShardNodeInfoRecord{
		NodeInfoRecord: normaldb.NodeInfoRecord{
			Version:   normaldb.NodeInfoVersionNormal,
			NodeID:    proto.NodeID(1),
			ClusterID: proto.ClusterID(1),
			NodeSetID: proto.NodeSetID(2),
			Status:    proto.NodeStatusDropped,
			Role:      proto.NodeRoleShardNode,
			DiskType:  proto.DiskTypeNVMeSSD,
		},
	}
	nodeTbl, err := normaldb.OpenShardNodeTable(testDB)
	require.NoError(t, err)
	err = nodeTbl.UpdateNode(&nr)
	require.NoError(t, err)
	nodeInfoRecord := normaldb.ShardNodeDiskInfoRecord{
		DiskInfoRecord: normaldb.DiskInfoRecord{
			Version:   normaldb.DiskInfoVersionNormal,
			DiskID:    proto.DiskID(1),
			NodeID:    proto.NodeID(1),
			ClusterID: proto.ClusterID(1),
			DiskSetID: proto.DiskSetID(2),
			Status:    proto.DiskStatusRepaired,
		},
	}
	diskTbl, err := normaldb.OpenShardNodeDiskTable(testDB, true)
	require.NoError(t, err)
	err = diskTbl.AddDisk(&nodeInfoRecord)
	require.NoError(t, err)
	shardNodeMgr, err := NewShardNodeMgr(testMockScopeMgr, testDB, testShardNodeMgrConfig)
	require.NoError(t, err)

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	topoInfo := shardNodeMgr.GetTopoInfo(ctx)
	nodeSets := topoInfo.AllNodeSets[proto.DiskTypeNVMeSSD.String()]
	nodeSet, nodeSetExist := nodeSets[proto.NodeSetID(2)]
	_, diskSetExist := nodeSet.DiskSets[proto.DiskSetID(2)]
	require.Equal(t, nodeSetExist, true)
	require.Equal(t, diskSetExist, true)
}

func TestShardNodeManager_Disk(t *testing.T) {
	shardNodeMgr, closeMgr := initTestShardNodeMgr(t)
	defer closeMgr()

	initTestShardNodeMgrNodes(t, shardNodeMgr, 1, 1, testIdcs[0])
	initTestShardNodeMgrDisks(t, shardNodeMgr, 1, 10, false, testIdcs[0])
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	// AddDisk
	{
		diskInfo, err := shardNodeMgr.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		diskInfo.NodeID = proto.NodeID(1000)
		err = shardNodeMgr.AddDisk(ctx, diskInfo)
		require.ErrorIs(t, err, apierrors.ErrCMNodeNotFound)

		diskInfo, _ = shardNodeMgr.GetDiskInfo(ctx, proto.DiskID(1))
		diskInfo.Path = "new disk path"
		diskInfo.DiskID = proto.DiskID(1000)
		err = shardNodeMgr.AddDisk(ctx, diskInfo)
		require.NoError(t, err)
	}
}
