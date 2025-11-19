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

package normaldb

import (
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	snnr1 = ShardNodeInfoRecord{
		NodeInfoRecord: NodeInfoRecord{
			Version:   NodeInfoVersionNormal,
			NodeID:    proto.NodeID(1),
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Rack:      "rack1",
			Host:      "127.0.0.1",
			Status:    proto.NodeStatusNormal,
			Role:      proto.NodeRoleShardNode,
			DiskType:  proto.DiskTypeNVMeSSD,
		},
		RaftHost: "127.0.0.1:10011",
	}
	snnr2 = ShardNodeInfoRecord{
		NodeInfoRecord: NodeInfoRecord{
			Version:   NodeInfoVersionNormal,
			NodeID:    proto.NodeID(2),
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Rack:      "rack2",
			Host:      "127.0.0.2",
			Status:    proto.NodeStatusNormal,
			Role:      proto.NodeRoleShardNode,
			DiskType:  proto.DiskTypeNVMeSSD,
		},
		RaftHost: "127.0.0.1:10012",
	}

	sndr1 = ShardNodeDiskInfoRecord{
		DiskInfoRecord: DiskInfoRecord{
			Version:      DiskInfoVersionNormal,
			DiskID:       proto.DiskID(1),
			ClusterID:    proto.ClusterID(1),
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1",
			Path:         "",
			Status:       proto.DiskStatusNormal,
			Readonly:     false,
			CreateAt:     time.Now(),
			LastUpdateAt: time.Now(),
			NodeID:       snnr1.NodeID,
		},
		UsedShardCnt: 0,
		Used:         0,
		Size:         100000,
		Free:         100000,
		MaxShardCnt:  10,
		FreeShardCnt: 10,
	}
	sndr2 = ShardNodeDiskInfoRecord{
		DiskInfoRecord: DiskInfoRecord{
			Version:      DiskInfoVersionNormal,
			DiskID:       proto.DiskID(2),
			ClusterID:    proto.ClusterID(1),
			Idc:          "z0",
			Rack:         "rack2",
			Host:         "127.0.0.2",
			Path:         "",
			Status:       proto.DiskStatusBroken,
			Readonly:     false,
			CreateAt:     time.Now(),
			LastUpdateAt: time.Now(),
			NodeID:       snnr2.NodeID,
		},
		UsedShardCnt: 0,
		Used:         0,
		Size:         100000,
		Free:         100000,
		MaxShardCnt:  10,
		FreeShardCnt: 10,
	}
)

func TestShardNodeDiskTbl(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeDiskTbl, err := OpenShardNodeDiskTable(db, true)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&snnr1)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&snnr2)
	require.NoError(t, err)

	// get all disk/ add disk / delete disk
	{
		diskList, err := nodeDiskTbl.GetAllDisks()
		require.NoError(t, err)
		require.Equal(t, 0, len(diskList))

		err = nodeDiskTbl.AddDisk(&sndr1)
		require.NoError(t, err)

		err = nodeDiskTbl.AddDisk(&sndr2)
		require.NoError(t, err)

		diskList, err = nodeDiskTbl.GetAllDisks()
		require.NoError(t, err)
		require.Equal(t, 2, len(diskList))
	}

	// get disk and update disk
	{
		diskInfo, err := nodeDiskTbl.GetDisk(sndr1.DiskID)
		require.NoError(t, err)
		t.Log(diskInfo.CreateAt.String())
		t.Log(sndr1.CreateAt.String())
		require.EqualValues(t, diskInfo.CreateAt.Unix(), sndr1.CreateAt.Unix())

		diskInfo.Readonly = true
		err = nodeDiskTbl.UpdateDisk(sndr1.DiskID, diskInfo)
		require.NoError(t, err)
		diskInfo, err = nodeDiskTbl.GetDisk(sndr1.DiskID)
		require.NoError(t, err)
		require.Equal(t, true, diskInfo.Readonly)

		err = nodeDiskTbl.UpdateDiskStatus(sndr1.DiskID, proto.DiskStatusRepairing)
		require.NoError(t, err)
		diskInfo, err = nodeDiskTbl.GetDisk(sndr1.DiskID)
		require.NoError(t, err)
		require.Equal(t, proto.DiskStatusRepairing, diskInfo.Status)
	}

	// list disk
	{
		diskList, err := nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Host: sndr1.Host, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))
		require.Equal(t, sndr1.DiskID, diskList[0].DiskID)

		err = nodeDiskTbl.AddDisk(&sndr2)
		require.NoError(t, err)

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Host: sndr2.Host, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))
		require.Equal(t, sndr2.DiskID, diskList[0].DiskID)

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))
		require.Equal(t, sndr2.DiskID, diskList[0].DiskID)

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Marker: dr2.DiskID, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 0, len(diskList))

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Marker: dr2.DiskID, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 0, len(diskList))

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Idc: sndr1.Idc, Rack: sndr1.Rack, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Count: 10})
		require.NoError(t, err)
		require.Equal(t, 2, len(diskList))
	}
}

func TestShardNodeDiskDropTbl(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeDiskTbl, err := OpenShardNodeDiskTable(db, true)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&snnr1)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&snnr2)
	require.NoError(t, err)

	require.NoError(t, err)
	err = nodeDiskTbl.AddDisk(&sndr1)
	require.NoError(t, err)
	err = nodeDiskTbl.AddDisk(&sndr2)
	require.NoError(t, err)

	dropList, err := nodeDiskTbl.GetAllDroppingDisk()
	require.NoError(t, err)
	require.Equal(t, 0, len(dropList))

	diskID1 := proto.DiskID(1)
	diskID2 := proto.DiskID(2)

	// add dropping disk and check list result
	{
		err = nodeDiskTbl.AddDroppingDisk(diskID1)
		require.NoError(t, err)

		droppingList, err := nodeDiskTbl.GetAllDroppingDisk()
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))
		require.Equal(t, []proto.DiskID{diskID1}, droppingList)

		err = nodeDiskTbl.AddDroppingDisk(diskID2)
		require.NoError(t, err)

		droppingList, err = nodeDiskTbl.GetAllDroppingDisk()
		require.NoError(t, err)
		require.Equal(t, []proto.DiskID{diskID1, diskID2}, droppingList)
	}

	// dropping disk
	{
		droppingList, _ := nodeDiskTbl.GetAllDroppingDisk()
		t.Log("dropping list: ", droppingList)
		exist, err := nodeDiskTbl.IsDroppingDisk(diskID1)
		require.NoError(t, err)
		require.Equal(t, true, exist)

		exist, err = nodeDiskTbl.IsDroppingDisk(diskID2)
		require.NoError(t, err)
		require.Equal(t, true, exist)

		exist, err = nodeDiskTbl.IsDroppingDisk(proto.InvalidDiskID)
		require.NoError(t, err)
		require.Equal(t, false, exist)

		err = nodeDiskTbl.DroppedDisk(diskID1)
		require.NoError(t, err)

		exist, err = nodeDiskTbl.IsDroppingDisk(diskID1)
		require.NoError(t, err)
		require.Equal(t, false, exist)

		droppingList, err = nodeDiskTbl.GetAllDroppingDisk()
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))
		require.Equal(t, []proto.DiskID{diskID2}, droppingList)
	}
}

func TestShardNodeTbl(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeTbl, err := OpenShardNodeDiskTable(db, true)
	require.NoError(t, err)

	// get all node/ add node
	nodeList, err := nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 0, len(nodeList))

	err = nodeTbl.UpdateNode(&snnr1)
	require.NoError(t, err)

	err = nodeTbl.UpdateNode(&snnr2)
	require.NoError(t, err)

	nodeList, err = nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 2, len(nodeList))
}
