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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/require"
)

var testShardNodeDiskInfo = clustermgr.ShardNodeDiskInfo{
	ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
		Used:         0,
		Size:         14.5 * 1024 * 1024 * 1024 * 1024,
		Free:         14.5 * 1024 * 1024 * 1024 * 1024,
		MaxShardCnt:  14.5 * 1024 / 16,
		FreeShardCnt: 14.5 * 1024 / 16,
	},
	DiskInfo: clustermgr.DiskInfo{
		ClusterID: proto.ClusterID(1),
		Idc:       "z0",
		Status:    proto.DiskStatusNormal,
		Readonly:  false,
	},
}

var testShardNodeInfo = clustermgr.ShardNodeInfo{
	NodeInfo: clustermgr.NodeInfo{
		ClusterID: proto.ClusterID(1),
		Idc:       "z0",
		Status:    proto.NodeStatusNormal,
		DiskType:  proto.DiskTypeNVMeSSD,
		Role:      proto.NodeRoleShardNode,
	},
	ShardNodeExtraInfo: clustermgr.ShardNodeExtraInfo{
		RaftHost: "http://127.0.0.1:10001",
	},
}

func insertShardNodeDiskInfos(t *testing.T, client *clustermgr.Client, start, end int, idcs ...string) {
	ctx := newCtx()
	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			_, err := client.AllocShardNodeDiskID(ctx)
			require.NoError(t, err)
			testShardNodeDiskInfo.DiskID = proto.DiskID(idx*10000 + i)
			hostID := i/60 + 1
			testShardNodeDiskInfo.NodeID = proto.NodeID(idx*10000 + hostID)
			testShardNodeDiskInfo.Rack = "testrack-" + strconv.Itoa(hostID)
			testShardNodeDiskInfo.Host = idc + "testhost-" + strconv.Itoa(hostID)
			testShardNodeDiskInfo.Idc = idc
			testShardNodeDiskInfo.Path = "testpath-" + testShardNodeDiskInfo.DiskID.ToString()
			err = client.AddShardNodeDisk(ctx, &testShardNodeDiskInfo)
			require.NoError(t, err)
		}
	}
}

func insertShardNodeInfos(t *testing.T, client *clustermgr.Client, start, end int, idcs ...string) {
	ctx := newCtx()
	for _, idc := range idcs {
		for i := start; i <= end; i++ {
			testShardNodeInfo.Rack = "testrack-" + strconv.Itoa(i+1)
			testShardNodeInfo.Host = idc + "testhost-" + strconv.Itoa(i+1)
			testShardNodeInfo.Idc = idc
			testShardNodeInfo.DiskType = proto.DiskTypeNVMeSSD
			_, err := client.AddShardNode(ctx, &testShardNodeInfo)
			require.NoError(t, err)
		}
	}
}

func TestShardNodeDisk(t *testing.T) {
	testService, clean := initTestServiceWithShardNode(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	// test disk id alloc
	{
		for i := 1; i <= 10; i++ {
			ret, err := testClusterClient.AllocShardNodeDiskID(ctx)
			require.NoError(t, err)
			require.Equal(t, proto.DiskID(i), ret)
		}
	}

	// test disk add/set disk
	{
		insertShardNodeInfos(t, testClusterClient, 0, 0, testService.IDC[0])
		insertShardNodeDiskInfos(t, testClusterClient, 1, 10, testService.IDC[0])
		disk1, err := testClusterClient.ShardNodeDiskInfo(ctx, 1)
		require.NoError(t, err)

		// failed case,diskid already exist
		err = testClusterClient.AddShardNodeDisk(ctx, disk1)
		require.Error(t, err)

		// failed case, host duplicated
		disk1.DiskID = 99
		err = testClusterClient.AddShardNodeDisk(ctx, disk1)
		require.Error(t, err)

		// failed case,diskId not invalid,over current diskId in CM
		disk1.Host = "127.0.0.99:"
		disk1.Path = "new-test-path"
		err = testClusterClient.AddShardNodeDisk(ctx, disk1)
		require.Error(t, err)

		// failed case,clusterId not match
		disk1.ClusterID = 999
		err = testClusterClient.AddShardNodeDisk(ctx, disk1)
		require.Error(t, err)

		// failed case,idc not match
		disk1.ClusterID = 1
		disk1.Idc = "xx"
		err = testClusterClient.AddShardNodeDisk(ctx, disk1)
		require.Error(t, err)

		err = testClusterClient.SetShardNodeDisk(ctx, 1, proto.DiskStatusBroken)
		require.NoError(t, err)

		// setDisk failed case
		err = testClusterClient.SetShardNodeDisk(ctx, 1, 0)
		require.Error(t, err)

		// setDisk failed case
		err = testClusterClient.SetShardNodeDisk(ctx, 1, proto.DiskStatusDropped)
		require.Error(t, err)
	}

	// test get disk/list disk/list host disk
	{
		disk, err := testClusterClient.ShardNodeDiskInfo(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, testDiskInfo.Idc, disk.Idc)

		_, err = testClusterClient.ShardNodeDiskInfo(ctx, 100)
		require.Error(t, err)

		listArgs := &clustermgr.ListOptionArgs{Host: disk.Host, Count: 200}
		list, err := testClusterClient.ListShardNodeDisk(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 10, len(list.Disks))

		ret, err := testClusterClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Host: disk.Host, Count: 100})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Disks))
		ret, err = testClusterClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Host: disk.Host, Count: 100, Marker: ret.Marker})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret.Disks))
		require.Equal(t, proto.InvalidDiskID, ret.Marker)

		ret, err = testClusterClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Status: disk.Status, Count: 100})
		require.NoError(t, err)
		require.Equal(t, 1, len(ret.Disks))

		ret, err = testClusterClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Count: 0})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Disks))

		// rack not nil,while idc is nil is not permit
		_, err = testClusterClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Rack: "test", Idc: ""})
		require.Error(t, err)
	}

	// test heartbeat
	{
		heartbeatInfos := make([]clustermgr.ShardNodeDiskHeartbeatInfo, 0)
		for i := 1; i <= 10; i++ {
			diskInfo, err := testClusterClient.ShardNodeDiskInfo(ctx, proto.DiskID(i))
			require.NoError(t, err)
			diskInfo.ShardNodeDiskHeartbeatInfo.Free = 0
			diskInfo.ShardNodeDiskHeartbeatInfo.FreeShardCnt = 0
			heartbeatInfos = append(heartbeatInfos, diskInfo.ShardNodeDiskHeartbeatInfo)
		}
		err := testClusterClient.HeartbeatShardNodeDisk(ctx, heartbeatInfos)
		require.NoError(t, err)

		// failed case ,diskId not exist
		heartbeatInfos[0].DiskID = 99
		err = testClusterClient.HeartbeatShardNodeDisk(ctx, heartbeatInfos)
		require.Error(t, err)
	}

	{
		args := &clustermgr.ShardNodeDiskInfo{
			ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
				DiskID:       1,
				MaxShardCnt:  99,
				FreeShardCnt: 9,
			},
			DiskInfo: clustermgr.DiskInfo{
				Status: proto.DiskStatusNormal,
			},
		}
		err := testClusterClient.PostWith(ctx, "/admin/shardnode/disk/update", nil, args)
		require.NoError(t, err)
		diskInfo, err := testClusterClient.ShardNodeDiskInfo(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, diskInfo.FreeShardCnt, args.FreeShardCnt)
		require.Equal(t, diskInfo.MaxShardCnt, args.MaxShardCnt)

		// failed case ,diskid not exist
		args.DiskID = 99
		err = testClusterClient.PostWith(ctx, "/admin/shardnode/disk/update", nil, args)
		require.Error(t, err)
	}
}
