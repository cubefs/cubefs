package clustermgr

import (
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/require"
)

func TestNodeAddandDrop(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()
	{
		// add node
		testNodeInfo.Rack = "testrack-" + strconv.Itoa(0)
		testNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
		testNodeInfo.Idc = testService.IDC[0]
		nodeID, err := testClusterClient.AddNode(ctx, &testNodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		// duplicated case
		nodeID, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		testNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(1)
		testNodeInfo.Idc = "z4"
		_, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.Error(t, err)

		testNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(2)
		testNodeInfo.Idc = testService.IDC[0]
		testNodeInfo.Role = proto.NodeRole(0)
		_, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.Error(t, err)

		testNodeInfo.Role = proto.NodeRoleBlobNode
		testNodeInfo.ClusterID = proto.ClusterID(2)
		_, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.Error(t, err)

		// drop node
		testNodeInfo.ClusterID = proto.ClusterID(1)
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
		testNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(3)
		testNodeInfo.Idc = testService.IDC[0]
		testNodeInfo.Role = proto.NodeRoleBlobNode
		_, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.NoError(t, err)

		err = testClusterClient.DropNode(ctx, proto.NodeID(2))
		require.NoError(t, err)

		// add node without changing ip and port
		testNodeInfo.Rack = "testrack-" + strconv.Itoa(0)
		testNodeInfo.Host = testService.IDC[0] + "testhost-" + strconv.Itoa(0)
		testNodeInfo.Idc = testService.IDC[0]
		nodeID, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.NoError(t, err)
		require.Equal(t, nodeID, proto.NodeID(1))

		// invalid nodeRole
		testNodeInfo.Role = proto.NodeRoleMax
		_, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.Error(t, err)

		// invalid diskType
		testNodeInfo.Role = proto.NodeRoleBlobNode
		testNodeInfo.DiskType = proto.DiskTypeMax
		_, err = testClusterClient.AddNode(ctx, &testNodeInfo)
		require.Error(t, err)
	}
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
