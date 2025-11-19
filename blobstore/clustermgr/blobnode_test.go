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

package clustermgr

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestNodeAddandDrop(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()
	{
		// add node
		nodeInfo := testNodeInfo
		nodeInfo.Rack = "testrack-" + strconv.Itoa(0)
		nodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
		nodeInfo.Idc = testService.IDC[0]
		nodeID, err := testClusterClient.AddNode(ctx, &nodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		// duplicated case
		nodeID, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		nodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(1)
		nodeInfo.Idc = "z4"
		_, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.Error(t, err)

		nodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(2)
		nodeInfo.Idc = testService.IDC[0]
		nodeInfo.Role = proto.NodeRole(0)
		_, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.Error(t, err)

		nodeInfo.Role = proto.NodeRoleBlobNode
		nodeInfo.ClusterID = proto.ClusterID(2)
		_, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.Error(t, err)

		// drop node
		nodeInfo.ClusterID = proto.ClusterID(1)
		_, err = testClusterClient.AllocDiskID(ctx)
		require.NoError(t, err)
		testDiskInfo.DiskID = proto.DiskID(1)
		testDiskInfo.NodeID = proto.NodeID(1)
		testDiskInfo.Path = "testpath-" + testDiskInfo.DiskID.ToString()
		err = testClusterClient.AddDisk(ctx, &testDiskInfo)
		require.NoError(t, err)
		err = testClusterClient.SetReadonlyDisk(ctx, proto.DiskID(1), true)
		require.NoError(t, err)
		err = testClusterClient.DropNode(ctx, proto.NodeID(1))
		require.NoError(t, err)

		// drop the dropping node
		err = testClusterClient.DropNode(ctx, proto.NodeID(1))
		require.NoError(t, err)

		// add disk to the dropping node
		err = testClusterClient.AddDisk(ctx, &testDiskInfo)
		require.Error(t, err)

		// drop the node which has no normal disk
		nodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(3)
		nodeInfo.Idc = testService.IDC[0]
		nodeInfo.Role = proto.NodeRoleBlobNode
		_, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.NoError(t, err)

		err = testClusterClient.DropNode(ctx, proto.NodeID(2))
		require.NoError(t, err)

		// add node without changing ip and port
		nodeInfo.Rack = "testrack-" + strconv.Itoa(0)
		nodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
		nodeInfo.Idc = testService.IDC[0]
		nodeID, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		// invalid nodeRole
		nodeInfo.Role = proto.NodeRoleMax
		_, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.Error(t, err)

		// invalid diskType
		nodeInfo.Role = proto.NodeRoleBlobNode
		nodeInfo.DiskType = proto.DiskTypeMax
		_, err = testClusterClient.AddNode(ctx, &nodeInfo)
		require.Error(t, err)
	}
}

func TestNodeUpdate(t *testing.T) {
	testServiceCfg.BlobNodeDiskMgrConfig.EnableNodeIPChange = true
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	// add node
	nodeInfo := testNodeInfo
	nodeInfo.Rack = "testrack-" + strconv.Itoa(0)
	nodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
	nodeInfo.Idc = testService.IDC[0]
	nodeID, err := testClusterClient.AddNode(ctx, &nodeInfo)
	require.NoError(t, err)
	require.Equal(t, nodeID, proto.NodeID(1))

	info, err := testClusterClient.NodeInfo(ctx, 1)
	require.NoError(t, err)

	// update node
	info.Host = "127.0.0.1:9110"
	info.Rack = "newRack"
	nodeID, err = testClusterClient.AddNode(ctx, info)
	require.NoError(t, err)
	require.Equal(t, nodeID, proto.NodeID(1))

	info1, err := testClusterClient.NodeInfo(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, info, info1)

	// update not exist node
	info.NodeID = proto.NodeID(100)
	info.Host = "127.0.0.1:1111"
	_, err = testClusterClient.AddNode(ctx, info)
	require.Error(t, err)

	// update node idc not allowed
	info.NodeID = proto.NodeID(1)
	info.Idc = testService.IDC[1]
	_, err = testClusterClient.AddNode(ctx, info)
	require.Error(t, err)
}

func TestTopoInfo(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	insertNodeInfos(t, testClusterClient, 0, 9, testService.IDC...)
	insertDiskInfos(t, testClusterClient, 1, 10, testService.IDC[0])
	ret, err := testClusterClient.TopoInfo(ctx)
	require.NoError(t, err)

	var diskSetMaxLen, nodeSetMaxLen int
	blobNodeHDDNodeSets := ret.AllNodeSets[proto.DiskTypeHDD.String()]
	copySetConf := testService.Config.BlobNodeDiskMgrConfig.CopySetConfigs[proto.DiskTypeHDD]
	diskSetCap, nodeSetCap, diskSetIdcCap := copySetConf.DiskSetCap, copySetConf.NodeSetCap, copySetConf.NodeSetIdcCap
	for _, nodeSet := range blobNodeHDDNodeSets {
		if nodeSet.Number > nodeSetMaxLen {
			nodeSetMaxLen = nodeSet.Number
		}
		for _, disks := range nodeSet.DiskSets {
			if len(disks) > diskSetMaxLen {
				diskSetMaxLen = len(disks)
			}
		}
	}
	require.Equal(t, diskSetCap, diskSetMaxLen)
	require.Equal(t, nodeSetCap, nodeSetMaxLen)
	require.Equal(t, diskSetIdcCap, (nodeSetCap+len(testService.IDC)-1)/len(testService.IDC))
}
