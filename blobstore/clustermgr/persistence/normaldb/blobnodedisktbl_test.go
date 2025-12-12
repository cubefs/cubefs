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
	nr1 = BlobNodeInfoRecord{
		NodeInfoRecord: NodeInfoRecord{
			Version:   NodeInfoVersionNormal,
			NodeID:    proto.NodeID(1),
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Rack:      "rack1",
			Host:      "127.0.0.1",
			Status:    proto.NodeStatusNormal,
			Role:      proto.NodeRoleBlobNode,
			DiskType:  proto.DiskTypeHDD,
		},
	}
	nr2 = BlobNodeInfoRecord{
		NodeInfoRecord: NodeInfoRecord{
			Version:   NodeInfoVersionNormal,
			NodeID:    proto.NodeID(2),
			ClusterID: proto.ClusterID(1),
			Idc:       "z0",
			Rack:      "rack2",
			Host:      "127.0.0.2",
			Status:    proto.NodeStatusNormal,
			Role:      proto.NodeRoleBlobNode,
			DiskType:  proto.DiskTypeHDD,
		},
	}

	dr1 = BlobNodeDiskInfoRecord{
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
			NodeID:       nr1.NodeID,
		},
		UsedChunkCnt:         0,
		Used:                 0,
		Size:                 100000,
		Free:                 100000,
		MaxChunkCnt:          10,
		FreeChunkCnt:         10,
		OversoldFreeChunkCnt: 20,
	}
	dr2 = BlobNodeDiskInfoRecord{
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
			NodeID:       nr2.NodeID,
		},
		UsedChunkCnt:         0,
		Used:                 0,
		Size:                 100000,
		Free:                 100000,
		MaxChunkCnt:          10,
		FreeChunkCnt:         10,
		OversoldFreeChunkCnt: 20,
	}
)

func TestBlobNodeDiskTbl(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeDiskTbl, err := OpenBlobNodeDiskTable(db, true)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&nr1)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&nr2)
	require.NoError(t, err)

	// get all disk/ add disk / delete disk
	{
		diskList, err := nodeDiskTbl.GetAllDisks()
		require.NoError(t, err)
		require.Equal(t, 0, len(diskList))

		err = nodeDiskTbl.AddDisk(&dr1)
		require.NoError(t, err)
		err = nodeDiskTbl.AddDisk(&dr2)
		require.NoError(t, err)

		diskList, err = nodeDiskTbl.GetAllDisks()
		require.NoError(t, err)
		require.Equal(t, 2, len(diskList))
	}

	// get disk and update disk
	{
		diskInfo, err := nodeDiskTbl.GetDisk(dr1.DiskID)
		require.NoError(t, err)
		t.Log(diskInfo.CreateAt.String())
		t.Log(dr1.CreateAt.String())
		require.EqualValues(t, diskInfo.CreateAt.Unix(), dr1.CreateAt.Unix())

		diskInfo.Readonly = true
		err = nodeDiskTbl.UpdateDisk(dr1.DiskID, diskInfo)
		require.NoError(t, err)
		diskInfo, err = nodeDiskTbl.GetDisk(dr1.DiskID)
		require.NoError(t, err)
		require.Equal(t, true, diskInfo.Readonly)

		err = nodeDiskTbl.UpdateDiskStatus(dr1.DiskID, proto.DiskStatusRepairing)
		require.NoError(t, err)
		diskInfo, err = nodeDiskTbl.GetDisk(dr1.DiskID)
		require.NoError(t, err)
		require.Equal(t, proto.DiskStatusRepairing, diskInfo.Status)
		require.Less(t, dr1.LastUpdateAt, diskInfo.LastUpdateAt)
	}

	// list disk
	{
		diskList, err := nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Host: dr1.Host, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))
		require.Equal(t, dr1.DiskID, diskList[0].DiskID)

		err = nodeDiskTbl.AddDisk(&dr2)
		require.NoError(t, err)

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Host: dr2.Host, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))
		require.Equal(t, dr2.DiskID, diskList[0].DiskID)

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))
		require.Equal(t, dr2.DiskID, diskList[0].DiskID)

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Marker: dr2.DiskID, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 0, len(diskList))

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Marker: dr2.DiskID, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 0, len(diskList))

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Idc: dr1.Idc, Rack: dr1.Rack, Count: 10})
		require.NoError(t, err)
		require.Equal(t, 1, len(diskList))

		diskList, err = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{Count: 10})
		require.NoError(t, err)
		require.Equal(t, 2, len(diskList))
	}
}

func TestBlobNodeDiskDropTbl(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeDiskTbl, err := OpenBlobNodeDiskTable(db, true)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&nr1)
	require.NoError(t, err)
	err = nodeDiskTbl.UpdateNode(&nr2)
	require.NoError(t, err)

	err = nodeDiskTbl.AddDisk(&dr1)
	require.NoError(t, err)
	err = nodeDiskTbl.AddDisk(&dr2)
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

		diskInfo, err := nodeDiskTbl.GetDisk(dr1.DiskID)
		require.NoError(t, err)
		require.Less(t, dr1.LastUpdateAt, diskInfo.LastUpdateAt)

		exist, err = nodeDiskTbl.IsDroppingDisk(diskID1)
		require.NoError(t, err)
		require.Equal(t, false, exist)

		droppingList, err = nodeDiskTbl.GetAllDroppingDisk()
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))
		require.Equal(t, []proto.DiskID{diskID2}, droppingList)
	}
}

func TestNodeTbl(t *testing.T) {
	tmpDBPath := os.TempDir() + "/" + uuid.NewString() + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeTbl, err := OpenBlobNodeDiskTable(db, true)
	require.NoError(t, err)

	// get all node/ add node
	nodeList, err := nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 0, len(nodeList))

	err = nodeTbl.UpdateNode(&nr1)
	require.NoError(t, err)

	err = nodeTbl.UpdateNode(&nr2)
	require.NoError(t, err)

	nodeList, err = nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 2, len(nodeList))
}

func TestNodeDropTbl(t *testing.T) {
	tmpDBPath := os.TempDir() + "/" + uuid.NewString() + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeDropTbl, err := OpenBlobNodeDiskTable(db, true)
	require.NoError(t, err)
	err = nodeDropTbl.UpdateNode(&nr1)
	require.NoError(t, err)
	err = nodeDropTbl.UpdateNode(&nr2)
	require.NoError(t, err)

	dropList, err := nodeDropTbl.GetAllDroppingNode()
	require.NoError(t, err)
	require.Equal(t, 0, len(dropList))

	nodeID1 := proto.NodeID(1)
	nodeID2 := proto.NodeID(2)

	// add dropping node and check list result
	{
		err = nodeDropTbl.AddDroppingNode(nodeID1)
		require.NoError(t, err)

		droppingList, err := nodeDropTbl.GetAllDroppingNode()
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))
		require.Equal(t, []proto.NodeID{nodeID1}, droppingList)

		err = nodeDropTbl.AddDroppingNode(nodeID2)
		require.NoError(t, err)

		droppingList, err = nodeDropTbl.GetAllDroppingNode()
		require.NoError(t, err)
		require.Equal(t, []proto.NodeID{nodeID1, nodeID2}, droppingList)
	}

	// dropping node
	{
		droppingList, _ := nodeDropTbl.GetAllDroppingNode()
		t.Log("dropping list: ", droppingList)
		exist, err := nodeDropTbl.IsDroppingNode(nodeID1)
		require.NoError(t, err)
		require.Equal(t, true, exist)

		exist, err = nodeDropTbl.IsDroppingNode(nodeID2)
		require.NoError(t, err)
		require.Equal(t, true, exist)

		exist, err = nodeDropTbl.IsDroppingNode(proto.InvalidNodeID)
		require.NoError(t, err)
		require.Equal(t, false, exist)

		err = nodeDropTbl.DroppedNode(nodeID1)
		require.NoError(t, err)

		exist, err = nodeDropTbl.IsDroppingNode(nodeID1)
		require.NoError(t, err)
		require.Equal(t, false, exist)

		droppingList, err = nodeDropTbl.GetAllDroppingNode()
		require.NoError(t, err)
		require.Equal(t, 1, len(droppingList))
		require.Equal(t, []proto.NodeID{nodeID2}, droppingList)
	}
}

func TestUpdateNodeHostAndRack(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()
	nodeDiskTbl, err := OpenBlobNodeDiskTable(db, true)
	require.NoError(t, err)

	{
		err = nodeDiskTbl.UpdateNode(&nr1)
		require.NoError(t, err)
		err = nodeDiskTbl.UpdateNode(&nr2)
		require.NoError(t, err)
		err = nodeDiskTbl.AddDisk(&dr1)
		require.NoError(t, err)

		dr1_2 := dr1
		dr1_2.DiskID = proto.DiskID(11)
		dr1_2.Path = "/data2"
		err = nodeDiskTbl.AddDisk(&dr1_2)
		require.NoError(t, err)
		err = nodeDiskTbl.AddDisk(&dr2)
		require.NoError(t, err)
	}

	{
		newNodeInfo := clustermgr.NodeInfo{
			NodeID: nr1.NodeID,
			Host:   "127.0.0.100",
			Rack:   "rack100",
		}
		diskIDs := []proto.DiskID{dr1.DiskID, proto.DiskID(11)}
		err = nodeDiskTbl.UpdateNodeHostAndRack(newNodeInfo, diskIDs)
		require.NoError(t, err)

		nodeList, err1 := nodeDiskTbl.GetAllNodes()
		require.NoError(t, err1)
		found := false
		for _, node := range nodeList {
			if node.NodeID == nr1.NodeID {
				require.Equal(t, "127.0.0.100", node.Host)
				require.Equal(t, "rack100", node.Rack)
				found = true
				break
			}
		}
		require.True(t, found)

		diskInfo, err1 := nodeDiskTbl.GetDisk(dr1.DiskID)
		require.NoError(t, err1)
		require.Equal(t, "127.0.0.100", diskInfo.Host)
		require.Equal(t, "rack100", diskInfo.Rack)
		diskInfo2, err1 := nodeDiskTbl.GetDisk(proto.DiskID(11))
		require.NoError(t, err1)
		require.Equal(t, "127.0.0.100", diskInfo2.Host)
		require.Equal(t, "rack100", diskInfo2.Rack)

		diskList, err1 := nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{
			Host:  "127.0.0.100",
			Count: 10,
		})
		require.NoError(t, err1)
		require.Equal(t, 2, len(diskList))
		diskList, err1 = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{
			Host:  "127.0.0.1",
			Count: 10,
		})
		require.NoError(t, err1)
		require.Equal(t, 0, len(diskList))

		diskList, err1 = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{
			Idc:   nr1.Idc,
			Rack:  "rack100",
			Count: 10,
		})
		require.NoError(t, err1)
		require.Equal(t, 2, len(diskList))
		diskList, err1 = nodeDiskTbl.ListDisk(&clustermgr.ListOptionArgs{
			Idc:   nr1.Idc,
			Rack:  "rack1",
			Count: 10,
		})
		require.NoError(t, err1)
		require.Equal(t, 0, len(diskList))
	}

	{
		invalidNodeInfo := clustermgr.NodeInfo{
			NodeID: proto.NodeID(9999),
			Host:   "127.0.0.99",
			Rack:   "rack99",
		}
		err = nodeDiskTbl.UpdateNodeHostAndRack(invalidNodeInfo, []proto.DiskID{})
		require.Error(t, err)

		newNodeInfo := clustermgr.NodeInfo{
			NodeID: nr1.NodeID,
			Host:   "127.0.0.101",
			Rack:   "rack101",
		}
		invalidDiskIDs := []proto.DiskID{proto.DiskID(9999)}
		err = nodeDiskTbl.UpdateNodeHostAndRack(newNodeInfo, invalidDiskIDs)
		require.Error(t, err)
	}
}
