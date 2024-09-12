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

package catalog

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/stretchr/testify/require"
)

func TestCatalogMgr_Apply(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	operTypes := make([]int32, 0)
	datas := make([][]byte, 0)

	// OperTypeCreateSpace
	{
		fildMeta := clustermgr.FieldMeta{
			ID:          1,
			Name:        "fildName1",
			FieldType:   proto.FieldTypeBool,
			IndexOption: proto.IndexOptionIndexed,
		}
		spaceInfo := &clustermgr.Space{
			SpaceID:    11,
			Name:       "test",
			Status:     proto.SpaceStatusNormal,
			FieldMetas: []clustermgr.FieldMeta{fildMeta},
			AccessKey:  "ak",
			SecretKey:  "sk",
		}
		data, err := json.Marshal(spaceInfo)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeCreateSpace)
		datas = append(datas, data)
	}

	// OperTypeInitCreateShard
	{
		shard := mockCatalogMgr.allShards.getShard(1)
		var suInfos []clustermgr.ShardUnitInfo
		for _, unit := range shard.units {
			suInfos = append(suInfos, *unit.info)
		}
		args := createShardCtx{
			ShardID:        proto.ShardID(99),
			ShardInfo:      shard.info,
			ShardUnitInfos: suInfos,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeInitCreateShard)
		datas = append(datas, data)
	}

	// OperTypeIncreaseShardUnitsEpoch
	{
		shard := mockCatalogMgr.allShards.getShard(1)
		var unitRecs []*catalogdb.ShardUnitInfoRecord
		for _, unit := range shard.units {
			unitRec := unit.toShardUnitRecord()
			unitRec.Epoch += IncreaseEpochInterval
			unitRecs = append(unitRecs, unitRec)
		}

		data, err := json.Marshal(unitRecs)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeIncreaseShardUnitsEpoch)
		datas = append(datas, data)
	}

	// OperTypeCreateShard
	{
		shard := mockCatalogMgr.allShards.getShard(1)
		var suInfos []clustermgr.ShardUnitInfo
		for _, unit := range shard.units {
			suInfos = append(suInfos, *unit.info)
		}
		args := createShardCtx{
			ShardID:        proto.ShardID(99),
			ShardInfo:      shard.info,
			ShardUnitInfos: suInfos,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeCreateShard)
		datas = append(datas, data)
	}

	// OperTypeUpdateShardUnit
	{
		args := &clustermgr.UpdateShardArgs{
			NewDiskID:   1,
			NewSuid:     proto.EncodeSuid(1, 1, 1),
			OldSuid:     proto.EncodeSuid(2, 1, 1),
			NewIsLeaner: false,
			OldIsLeaner: false,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeUpdateShardUnit)
		datas = append(datas, data)
	}

	// OperTypeUpdateShardUnitStatus
	{
		args := []proto.SuidPrefix{proto.EncodeSuidPrefix(1, 1)}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeUpdateShardUnitStatus)
		datas = append(datas, data)
	}

	// OperTypeShardReport
	{
		args := &clustermgr.ShardReportArgs{
			Shards: []clustermgr.ShardUnitInfo{
				{
					Suid:         proto.EncodeSuid(1, 1, 1),
					DiskID:       1,
					RouteVersion: proto.RouteVersion(1),
				},
			},
		}
		data, err := args.Marshal()
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeShardReport)
		datas = append(datas, data)
	}

	// OperTypeAllocShardUnit
	{
		args := &allocShardUnitCtx{
			Suid: proto.EncodeSuid(1, 1, 1),
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeAllocShardUnit)
		datas = append(datas, data)
	}

	// OperTypeAdminUpdateShard
	{
		args := clustermgr.Shard{
			ShardID:      1,
			RouteVersion: 100,
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeAdminUpdateShard)
		datas = append(datas, data)
	}

	// OperTypeAdminUpdateShardUnit
	{
		args := clustermgr.AdminUpdateShardUnitArgs{
			Epoch:     10,
			NextEpoch: 10,
			ShardUnitInfo: clustermgr.ShardUnitInfo{
				Suid: proto.EncodeSuid(1, 1, 1),
			},
		}
		data, err := json.Marshal(args)
		require.NoError(t, err)
		operTypes = append(operTypes, OperTypeAdminUpdateShardUnit)
		datas = append(datas, data)
	}

	ctxs := make([]base.ProposeContext, 0)
	for i := 0; i < len(operTypes); i++ {
		ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
	}

	err := mockCatalogMgr.Apply(ctx, operTypes, datas, ctxs)
	require.NoError(t, err)

	// test error datas
	for i := range datas {
		datas[i] = append(datas[i], []byte{1}...)
		err = mockCatalogMgr.Apply(ctx, operTypes, datas, ctxs)
		require.Error(t, err)
	}

	// test error opertype
	operTypes = []int32{0}
	datas = [][]byte{{0}}
	err = mockCatalogMgr.Apply(ctx, operTypes, datas, ctxs[0:1])
	require.Error(t, err)
}

func TestCatalogMgr_Others(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	mockCatalogMgr.NotifyLeaderChange(ctx, 1, "")
	mockCatalogMgr.SetModuleName("catalogmgr")
	name := mockCatalogMgr.GetModuleName()
	require.Equal(t, name, "catalogmgr")

	shard := mockCatalogMgr.allShards.getShard(1)
	dirty := mockCatalogMgr.dirty.Load().(*concurrentShards)
	dirty.putShard(shard)

	mockCatalogMgr.Flush(ctx)
}
