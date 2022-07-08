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

package clustermgr

import (
	"context"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/stretchr/testify/assert"
)

var testDiskInfo = blobnode.DiskInfo{
	DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
		Used:         0,
		Size:         14.5 * 1024 * 1024 * 1024 * 1024,
		Free:         14.5 * 1024 * 1024 * 1024 * 1024,
		MaxChunkCnt:  14.5 * 1024 / 16,
		FreeChunkCnt: 14.5 * 1024 / 16,
	},
	ClusterID: proto.ClusterID(1),
	Idc:       "z0",
	Status:    proto.DiskStatusNormal,
	Readonly:  false,
}

func insertDiskInfos(t *testing.T, client *clustermgr.Client, start, end int, idcs ...string) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	for idx, idc := range idcs {
		for i := start; i <= end; i++ {
			_, err := client.AllocDiskID(ctx)
			assert.NoError(t, err)
			testDiskInfo.DiskID = proto.DiskID(idx*10000 + i)
			hostID := i / 60
			testDiskInfo.Rack = "testrack-" + strconv.Itoa(hostID)
			testDiskInfo.Host = idc + "testhost-" + strconv.Itoa(hostID)
			testDiskInfo.Idc = idc
			testDiskInfo.Path = "testpath-" + testDiskInfo.DiskID.ToString()
			err = client.AddDisk(ctx, &testDiskInfo)
			assert.NoError(t, err)
		}
	}
}

func TestDisk(t *testing.T) {
	testService := initTestService(t)
	defer clear(testService)
	defer testService.Close()
	testClusterClient := initTestClusterClient(testService)

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// test disk id alloc
	{
		for i := 1; i <= 10; i++ {
			ret, err := testClusterClient.AllocDiskID(ctx)
			assert.NoError(t, err)
			assert.Equal(t, proto.DiskID(i), ret)
		}
	}

	// test disk add/set disk
	{
		insertDiskInfos(t, testClusterClient, 1, 10, testService.IDC[0])
		disk1, err := testClusterClient.DiskInfo(ctx, 1)
		assert.NoError(t, err)

		// failed case,diskid already exist
		err = testClusterClient.AddDisk(ctx, disk1)
		assert.Error(t, err)

		// failed case, host duplicated
		disk1.DiskID = 99
		err = testClusterClient.AddDisk(ctx, disk1)
		assert.Error(t, err)

		// failed case,diskId not invalid,over current diskId in CM
		disk1.Host = "127.0.0.99:"
		disk1.Path = "new-test-path"
		err = testClusterClient.AddDisk(ctx, disk1)
		assert.Error(t, err)

		// failed case,clusterId not match
		disk1.ClusterID = 999
		err = testClusterClient.AddDisk(ctx, disk1)
		assert.Error(t, err)

		// failed case,idc not match
		disk1.ClusterID = 1
		disk1.Idc = "xx"
		err = testClusterClient.AddDisk(ctx, disk1)
		assert.Error(t, err)

		err = testClusterClient.SetDisk(ctx, 1, proto.DiskStatusBroken)
		assert.NoError(t, err)

		// setDisk failed case
		err = testClusterClient.SetDisk(ctx, 1, 0)
		assert.Error(t, err)

		// setDisk failed case
		err = testClusterClient.SetDisk(ctx, 1, proto.DiskStatusDropped)
		assert.Error(t, err)
	}

	// test get disk/list disk/list host disk
	{
		disk, err := testClusterClient.DiskInfo(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, testDiskInfo.Idc, disk.Idc)

		_, err = testClusterClient.DiskInfo(ctx, 100)
		assert.Error(t, err)

		list, err := testClusterClient.ListHostDisk(ctx, disk.Host)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(list))

		ret, err := testClusterClient.ListDisk(ctx, &clustermgr.ListOptionArgs{Host: disk.Host, Count: 100})
		assert.NoError(t, err)
		assert.Equal(t, 10, len(ret.Disks))
		ret, err = testClusterClient.ListDisk(ctx, &clustermgr.ListOptionArgs{Host: disk.Host, Count: 100, Marker: ret.Marker})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(ret.Disks))
		assert.Equal(t, proto.InvalidDiskID, ret.Marker)

		ret, err = testClusterClient.ListDisk(ctx, &clustermgr.ListOptionArgs{Status: disk.Status, Count: 100})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ret.Disks))

		ret, err = testClusterClient.ListDisk(ctx, &clustermgr.ListOptionArgs{Count: 0})
		assert.NoError(t, err)
		assert.Equal(t, 10, len(ret.Disks))

		// rack not nil,while idc is nil is not permit
		_, err = testClusterClient.ListDisk(ctx, &clustermgr.ListOptionArgs{Rack: "test", Idc: ""})
		assert.Error(t, err)
	}

	// test drop list
	{
		err := testClusterClient.DropDisk(ctx, 2)
		assert.Error(t, err)

		err = testClusterClient.SetReadonlyDisk(ctx, 2, true)
		assert.NoError(t, err)
		err = testClusterClient.DropDisk(ctx, 2)
		assert.NoError(t, err)
		ret, err := testClusterClient.ListDroppingDisk(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ret))

		err = testClusterClient.SetReadonlyDisk(ctx, 2, false)
		assert.Error(t, err)
		err = testClusterClient.SetDisk(ctx, 2, proto.DiskStatusBroken)
		assert.Error(t, err)

		// drop already dropped disk
		err = testClusterClient.DropDisk(ctx, 2)
		assert.NoError(t, err)

		err = testClusterClient.DroppedDisk(ctx, 2)
		assert.NoError(t, err)
		ret, err = testClusterClient.ListDroppingDisk(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(ret))

		// drop disk repeatably will return error
		err = testClusterClient.DropDisk(ctx, 2)
		assert.Error(t, err)
		t.Log(err.Error())

		disk, err := testClusterClient.DiskInfo(ctx, 2)
		assert.NoError(t, err)
		assert.Equal(t, proto.DiskStatusDropped, disk.Status)

		// dropped not dropping disk will return error
		err = testClusterClient.DroppedDisk(ctx, 10)
		assert.Error(t, err)
		t.Log(err.Error())

		// failed case,drop diskid not exist
		err = testClusterClient.DroppedDisk(ctx, 99)
		assert.Error(t, err)
	}

	// test heartbeat
	{
		heartbeatInfos := make([]*blobnode.DiskHeartBeatInfo, 0)
		for i := 1; i <= 10; i++ {
			diskInfo, err := testClusterClient.DiskInfo(ctx, proto.DiskID(i))
			assert.NoError(t, err)
			diskInfo.DiskHeartBeatInfo.Free = 0
			diskInfo.DiskHeartBeatInfo.FreeChunkCnt = 0
			heartbeatInfos = append(heartbeatInfos, &diskInfo.DiskHeartBeatInfo)
		}
		ret, err := testClusterClient.HeartbeatDisk(ctx, heartbeatInfos)
		assert.NoError(t, err)
		for i := range ret {
			if ret[i].DiskID == 2 {
				assert.Equal(t, proto.DiskStatusDropped, ret[i].Status)
			}
			if ret[i].DiskID == 1 {
				assert.Equal(t, proto.DiskStatusBroken, ret[i].Status)
			}
		}

		// failed case ,diskId not exist
		heartbeatInfos[0].DiskID = 99
		_, err = testClusterClient.HeartbeatDisk(ctx, heartbeatInfos)
		assert.Error(t, err)
	}

	// test disk access
	{
		err := testClusterClient.SetReadonlyDisk(ctx, 4, true)
		assert.NoError(t, err)
		diskInfo, err := testClusterClient.DiskInfo(ctx, 4)
		assert.NoError(t, err)
		assert.Equal(t, true, diskInfo.Readonly)

		err = testClusterClient.SetReadonlyDisk(ctx, 4, true)
		assert.NoError(t, err)

		err = testClusterClient.SetReadonlyDisk(ctx, 4, false)
		assert.NoError(t, err)
		diskInfo, err = testClusterClient.DiskInfo(ctx, 4)
		assert.NoError(t, err)
		assert.Equal(t, false, diskInfo.Readonly)

		// failed case
		err = testClusterClient.SetReadonlyDisk(ctx, 44, true)
		assert.Error(t, err)
	}

	{
		args := &blobnode.DiskInfo{
			DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
				DiskID:       1,
				MaxChunkCnt:  99,
				FreeChunkCnt: 9,
			},
			Status: 1,
		}
		err := testClusterClient.PostWith(ctx, "/admin/disk/update", nil, args)
		assert.NoError(t, err)
		diskInfo, err := testClusterClient.DiskInfo(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, diskInfo.FreeChunkCnt, args.FreeChunkCnt)
		assert.Equal(t, diskInfo.MaxChunkCnt, args.MaxChunkCnt)

		// failed case ,diskid not exist
		args.DiskID = 99
		err = testClusterClient.PostWith(ctx, "/admin/disk/update", nil, args)
		assert.Error(t, err)

	}
}
