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
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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
		initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])

		for i := 1; i <= 10; i++ {
			diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(i))
			require.NoError(t, err)
			require.Equal(t, proto.DiskID(i), diskInfo.DiskID)

			duplicated := testDiskMgr.CheckDiskInfoDuplicated(ctx, diskInfo)
			require.Equal(t, true, duplicated)
		}

		// test CheckDiskInfoDuplicated return false case
		diskInfo, err := testDiskMgr.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		diskInfo.Path += "notDuplicated"
		duplicated := testDiskMgr.CheckDiskInfoDuplicated(ctx, diskInfo)
		require.Equal(t, false, duplicated)
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
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// dropping and list dropping
	{
		droppingList, err := testDiskMgr.ListDroppingDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(droppingList))

		err = testDiskMgr.droppingDisk(ctx, 1)
		require.NoError(t, err)

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
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])
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
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])
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
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs[0])
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
