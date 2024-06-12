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

		err = testClusterClient.DropNode(ctx, proto.NodeID(1))
		require.Error(t, err)

		err = testClusterClient.SetReadonlyDisk(ctx, proto.DiskID(1), true)
		require.NoError(t, err)
		err = testClusterClient.DropDisk(ctx, proto.DiskID(1))
		require.NoError(t, err)
		err = testClusterClient.DroppedDisk(ctx, proto.DiskID(1))
		require.NoError(t, err)
		err = testClusterClient.DropNode(ctx, proto.NodeID(1))
		require.NoError(t, err)
	}
}

func TestTopoInfo(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	insertNodeInfos(t, testClusterClient, 0, 0, testService.IDC[0])
	insertDiskInfos(t, testClusterClient, 1, 10, testService.IDC[0])
	_, err := testClusterClient.TopoInfo(ctx)
	require.NoError(t, err)
}
