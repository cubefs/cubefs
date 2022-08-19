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

package volumemgr

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/stretchr/testify/require"
)

func TestVolumeMgr_Apply(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	operTypes := make([]int32, 0)
	datas := make([][]byte, 0)

	// OperTypeInitCreateVolume
	{
		vol := mockVolumeMgr.all.getVol(1)
		var vuInfos []*clustermgr.VolumeUnitInfo
		for _, vUnit := range vol.vUnits {
			vuInfos = append(vuInfos, vUnit.vuInfo)
		}
		args := CreateVolumeCtx{
			VuInfos: vuInfos,
			Vid:     proto.Vid(99),
			VolInfo: vol.volInfoBase,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeInitCreateVolume)
		datas = append(datas, data)
	}

	// OperTypeIncreaseVolumeUnitsEpoch
	{
		vol := mockVolumeMgr.all.getVol(1)
		var unitRecs []*volumedb.VolumeUnitRecord
		for _, vUnit := range vol.vUnits {
			unitRec := vUnit.ToVolumeUnitRecord()
			unitRec.Epoch += IncreaseEpochInterval
			unitRecs = append(unitRecs, unitRec)
		}

		data, err := json.Marshal(unitRecs)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeIncreaseVolumeUnitsEpoch)
		datas = append(datas, data)
	}

	// OperTypeCreateVolume
	{
		vol := mockVolumeMgr.all.getVol(1)
		var vuInfos []*clustermgr.VolumeUnitInfo
		for _, vUnit := range vol.vUnits {
			vuInfos = append(vuInfos, vUnit.vuInfo)
		}
		args := CreateVolumeCtx{
			VuInfos: vuInfos,
			Vid:     proto.Vid(99),
			VolInfo: vol.volInfoBase,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeCreateVolume)
		datas = append(datas, data)
	}

	// OperTypeAllocVolume
	{
		allocVol := &AllocVolumeCtx{
			Vids:               []proto.Vid{1, 2, 3},
			Host:               "127.0.0.1",
			ExpireTime:         time.Now().Add(time.Minute).UnixNano(),
			PendingAllocVolKey: "1",
		}
		data, err := json.Marshal(allocVol)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeAllocVolume)
		datas = append(datas, data)

	}

	// OperTypeRetainVolume
	{
		args := &clustermgr.RetainVolumes{
			RetainVolTokens: []clustermgr.RetainVolume{
				{
					Token:      "127.0.0.1:8080;1",
					ExpireTime: time.Now().Add(time.Minute).UnixNano(),
				},
			},
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeRetainVolume)
		datas = append(datas, data)
	}

	// OperTypeChangeVolumeStatus
	{
		args := &ChangeVolStatusCtx{
			Vid:      1,
			TaskType: base.VolumeTaskTypeLock,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeChangeVolumeStatus)
		datas = append(datas, data)
	}

	// OperTypeUpdateVolumeUnit
	{
		args := &clustermgr.UpdateVolumeArgs{
			NewDiskID: 1,
			NewVuid:   proto.EncodeVuid(4294967296, 1),
			OldVuid:   proto.EncodeVuid(4294967296, 1),
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeUpdateVolumeUnit)
		datas = append(datas, data)
	}

	// OperTypeChunkReport
	{
		args := &clustermgr.ReportChunkArgs{
			ChunkInfos: []blobnode.ChunkInfo{
				{
					Vuid:   proto.EncodeVuid(4294967296, 1),
					DiskID: 1,
					Free:   122345,
					Total:  456789,
					Used:   0,
				},
			},
		}
		data, err := args.Encode()
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeChunkReport)
		datas = append(datas, data)
	}

	// OperTypeChunkSetCompact
	{
		args := &clustermgr.SetCompactChunkArgs{
			Vuid:       proto.EncodeVuid(4294967296, 1),
			Compacting: true,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeChunkSetCompact)
		datas = append(datas, data)
	}

	// OperTypeVolumeUnitSetWritable
	{
		args := make([]proto.VuidPrefix, 0)
		args = append(args, 4294967296)
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeVolumeUnitSetWritable)
		datas = append(datas, data)
	}

	// OperTypeAllocVolumeUnit
	{
		args := &allocVolumeUnitCtx{
			Vuid: proto.EncodeVuid(4294967296, 1),
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeAllocVolumeUnit)
		datas = append(datas, data)
	}

	// OperTypeDeleteTask
	{
		args := &DeleteTaskCtx{
			Vid:      1,
			TaskType: base.VolumeTaskTypeLock,
			TaskId:   "1",
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeDeleteTask)
		datas = append(datas, data)
	}

	// OperTypeExpireVolume
	{
		args := []proto.Vid{1, 2}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeExpireVolume)
		datas = append(datas, data)
	}

	// OperAdminUpdateVolume
	{
		args := clustermgr.VolumeInfo{
			VolumeInfoBase: clustermgr.VolumeInfoBase{
				Vid:  1,
				Used: 999,
			},
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeAdminUpdateVolume)
		datas = append(datas, data)
	}

	ctxs := make([]base.ProposeContext, 0)
	for i := 0; i < len(operTypes); i++ {
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
	}

	err := mockVolumeMgr.Apply(ctx, operTypes, datas, ctxs)
	require.NoError(t, err)

	// test error datas
	for i := range datas {
		datas[i] = append(datas[i], []byte{1}...)
		err = mockVolumeMgr.Apply(ctx, operTypes, datas, ctxs)
		require.Error(t, err)
	}

	// test error opertype
	operTypes = []int32{0}
	datas = [][]byte{{0}}
	err = mockVolumeMgr.Apply(ctx, operTypes, datas, ctxs[0:1])
	require.Error(t, err)
}

func TestVolumeMgr_Others(t *testing.T) {
	initMockVolumeMgr(t)
	defer closeTestVolumeMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	mockVolumeMgr.NotifyLeaderChange(ctx, 1, "")
	mockVolumeMgr.SetModuleName("volumemgr")
	name := mockVolumeMgr.GetModuleName()
	require.Equal(t, name, "volumemgr")
	mockVolumeMgr.Flush(ctx)
}
