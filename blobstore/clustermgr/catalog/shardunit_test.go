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
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/stretchr/testify/require"
)

func TestCatalogMgr_ShardUnit(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "shardUnit")

	// list shard unit
	infos, err := mockCatalogMgr.ListShardUnitInfo(ctx, &clustermgr.ListShardUnitArgs{
		DiskID: 1,
	})
	require.NoError(t, err)
	require.Equal(t, len(infos), 10)

	// alloc shard unit
	_, err = mockCatalogMgr.AllocShardUnit(ctx, proto.EncodeSuid(1, 1, 1))
	require.NoError(t, err)

	_, err = mockCatalogMgr.AllocShardUnit(ctx, proto.EncodeSuid(100, 1, 1))
	require.Error(t, err)

	// pre update shard unit
	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid: proto.EncodeSuid(100, 1, 1),
	})
	require.ErrorIs(t, err, ErrShardNotExist)

	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid: proto.EncodeSuid(1, 4, 1),
	})
	require.ErrorIs(t, err, ErrNewSuidNotMatch)

	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid: proto.EncodeSuid(1, 1, 100),
	})
	require.ErrorIs(t, err, ErrOldSuidNotMatch)

	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid: proto.EncodeSuid(1, 1, 1),
		NewSuid: proto.EncodeSuid(1, 1, 1),
	})
	require.ErrorIs(t, err, ErrRepeatUpdateShardUnit)

	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid:     proto.EncodeSuid(1, 1, 1),
		NewSuid:     proto.EncodeSuid(1, 1, 2),
		OldIsLeaner: true,
	})
	require.ErrorIs(t, err, ErrOldIsLeanerNotMatch)

	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid: proto.EncodeSuid(1, 1, 1),
		NewSuid: proto.EncodeSuid(1, 1, 100),
	})
	require.ErrorIs(t, err, ErrNewSuidNotMatch)

	err = mockCatalogMgr.PreUpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid:   proto.EncodeSuid(1, 1, 1),
		NewSuid:   proto.EncodeSuid(1, 1, 3),
		NewDiskID: proto.DiskID(31),
	})
	require.NoError(t, err)

	// update shard unit
	err = mockCatalogMgr.UpdateShardUnit(ctx, &clustermgr.UpdateShardArgs{
		OldSuid:   proto.EncodeSuid(1, 1, 1),
		NewSuid:   proto.EncodeSuid(1, 1, 3),
		NewDiskID: proto.DiskID(31),
	})
	require.NoError(t, err)

	// update shard unit status
	err = mockCatalogMgr.UpdateShardUnitStatus(ctx, 100)
	require.NoError(t, err)

	err = mockCatalogMgr.UpdateShardUnitStatus(ctx, 1)
	require.NoError(t, err)

	// report shard
	shardUnit1 := clustermgr.ShardUnitInfo{
		Suid: proto.EncodeSuid(100, 1, 1),
	}
	shardUnit2 := clustermgr.ShardUnitInfo{
		Suid:   proto.EncodeSuid(1, 1, 0),
		DiskID: 1,
	}
	shardUnit3 := clustermgr.ShardUnitInfo{
		Suid:   proto.EncodeSuid(2, 1, 0),
		DiskID: 400,
	}
	shardUnit4 := clustermgr.ShardUnitInfo{
		Suid:         proto.EncodeSuid(8, 2, 1),
		DiskID:       2,
		RouteVersion: 2,
	}
	shardUnits := []clustermgr.ShardUnitInfo{shardUnit1, shardUnit2, shardUnit3, shardUnit4}
	reportShardArgs := &clustermgr.ShardReportArgs{Shards: shardUnits}
	_, err = mockCatalogMgr.ReportShard(ctx, reportShardArgs, nil)
	require.NoError(t, err)
}
