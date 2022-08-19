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
	"encoding/json"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/stretchr/testify/require"
)

func TestApplier_Others(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs...)

	// test module and others
	{
		testModuleName := "DiskMgr"
		testDiskMgr.SetModuleName(testModuleName)
		module := testDiskMgr.GetModuleName()
		require.Equal(t, testModuleName, module)

		testDiskMgr.NotifyLeaderChange(ctx, 0, "")
	}

	// test flush
	{
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

		err = testDiskMgr.Flush(ctx)
		require.NoError(t, err)
	}
}

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
	Rack:      "testrack",
	Status:    proto.DiskStatusNormal,
	Readonly:  false,
}

func TestApplier_Apply(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	operTypes := make([]int32, 0)
	datas := make([][]byte, 0)

	// OperTypeAddDisk
	{
		for i := 1; i <= 3; i++ {
			info := testDiskInfo
			info.DiskID = proto.DiskID(i)
			info.Host = hostPrefix + strconv.Itoa(i)

			operTypes = append(operTypes, OperTypeAddDisk)
			data, err := json.Marshal(&info)
			require.NoError(t, err)
			datas = append(datas, data)
		}
	}

	// OperTypeSetDiskStatus
	{
		data, err := json.Marshal(&clustermgr.DiskSetArgs{
			DiskID: proto.DiskID(1),
			Status: proto.DiskStatusBroken,
		})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeSetDiskStatus)
		datas = append(datas, data)
	}

	// OperTypeDroppingDisk
	{
		data, err := json.Marshal(&clustermgr.DiskInfoArgs{DiskID: proto.DiskID(2)})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeDroppingDisk)
		datas = append(datas, data)
	}

	// OperTypeHeartbeatDiskInfo
	{
		heartbeatInfos := make([]*blobnode.DiskHeartBeatInfo, 0)
		for i := 1; i <= 3; i++ {
			heartbeatInfo := testDiskInfo.DiskHeartBeatInfo
			heartbeatInfo.Free = 0
			heartbeatInfo.FreeChunkCnt = 0
			heartbeatInfos = append(heartbeatInfos, &heartbeatInfo)
		}
		data, err := json.Marshal(&clustermgr.DisksHeartbeatArgs{Disks: heartbeatInfos})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeHeartbeatDiskInfo)
		datas = append(datas, data)
	}

	// OperTypeDroppedDisk
	{
		data, err := json.Marshal(&clustermgr.DiskInfoArgs{DiskID: proto.DiskID(2)})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeDroppedDisk)
		datas = append(datas, data)
	}

	// OperTypeSwitchReadonly
	{
		data, err := json.Marshal(&clustermgr.DiskAccessArgs{DiskID: proto.DiskID(3), Readonly: true})
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeSwitchReadonly)
		datas = append(datas, data)
	}

	ctxs := make([]base.ProposeContext, 0)
	for i := 0; i < len(operTypes); i++ {
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
	}

	err := testDiskMgr.Apply(ctx, operTypes, datas, ctxs)
	require.NoError(t, err)
}
