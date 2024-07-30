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

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/require"
)

func TestShardNodeAdd(t *testing.T) {
	testService, clean := initTestServiceWithShardNode(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()
	{
		// add node
		testShardNodeInfo.Rack = "testrack-" + strconv.Itoa(0)
		testShardNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
		testShardNodeInfo.Idc = testService.IDC[0]
		nodeID, err := testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		ret, err := testClusterClient.ShardNodeInfo(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, ret.NodeID, proto.NodeID(1))

		// duplicated case
		nodeID, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		testShardNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(1)
		testShardNodeInfo.Idc = "z4"
		_, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.Error(t, err)

		testShardNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(2)
		testShardNodeInfo.Idc = testService.IDC[0]
		testShardNodeInfo.Role = proto.NodeRole(0)
		_, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.Error(t, err)

		testShardNodeInfo.Role = proto.NodeRoleShardNode
		testShardNodeInfo.ClusterID = proto.ClusterID(2)
		_, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.Error(t, err)

		// add node without changing ip and port
		testShardNodeInfo.ClusterID = proto.ClusterID(1)
		testShardNodeInfo.Rack = "testrack-" + strconv.Itoa(0)
		testShardNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
		testShardNodeInfo.Idc = testService.IDC[0]
		nodeID, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		// invalid nodeRole
		testShardNodeInfo.Role = proto.NodeRoleMax
		_, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.Error(t, err)

		// invalid diskType
		testShardNodeInfo.Role = proto.NodeRoleShardNode
		testShardNodeInfo.DiskType = proto.DiskTypeMax
		_, err = testClusterClient.AddShardNode(ctx, &testShardNodeInfo)
		require.Error(t, err)
	}
}

func TestShardNodeTopoInfo(t *testing.T) {
	testService, clean := initTestServiceWithShardNode(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	insertShardNodeInfos(t, testClusterClient, 0, 9, testService.IDC...)
	insertShardNodeDiskInfos(t, testClusterClient, 1, 10, testService.IDC[0])
	ret, err := testClusterClient.ShardNodeTopoInfo(ctx)
	require.NoError(t, err)

	var diskSetMaxLen, nodeSetMaxLen int
	shardNodeNVMeNodeSets := ret.AllNodeSets[proto.DiskTypeNVMeSSD.String()]
	copySetConf := testService.Config.ShardNodeDiskMgrConfig.CopySetConfigs[proto.DiskTypeNVMeSSD]
	diskSetCap, nodeSetCap, diskSetIdcCap := copySetConf.DiskSetCap, copySetConf.NodeSetCap, copySetConf.NodeSetIdcCap
	for _, nodeSet := range shardNodeNVMeNodeSets {
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
