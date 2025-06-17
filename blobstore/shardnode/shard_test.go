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

package shardnode

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
)

func TestService_Task(t *testing.T) {
	diskID := genDiskID()
	d, _, err := storage.NewMockDisk(t, diskID)
	require.Nil(t, err)
	disks := make(map[proto.DiskID]*storage.MockDisk)
	disks[diskID] = d

	s, clear, err := newMockService(t, mockServiceCfg{
		tp:    newBaseTp(t),
		disks: disks,
	})
	require.Nil(t, err)
	defer clear()

	err = d.GetDisk().AddShard(ctx, suid, 0, *rg, []clustermgr.ShardUnit{
		{DiskID: diskID, Suid: suid},
	})
	require.Nil(t, err)

	tasks := make([]clustermgr.ShardTask, 0)
	s.generateTasksAndExecute(ctx, tasks, proto.ShardTaskTypeCheckpoint, "")

	task := clustermgr.ShardTask{
		TaskType:        proto.ShardTaskTypeSyncRouteVersion,
		DiskID:          diskID,
		Suid:            suid,
		OldRouteVersion: 0,
		RouteVersion:    0,
	}
	err = s.executeShardTask(ctx, task, true)
	require.NotNil(t, err)

	task.RouteVersion = 1
	err = s.executeShardTask(ctx, task, true)
	require.Nil(t, err)

	task.TaskType = proto.ShardTaskTypeCheckpoint
	err = s.executeShardTask(ctx, task, true)
	require.Nil(t, err)

	task.TaskType = proto.ShardTaskTypeCheckAndClear
	err = s.executeShardTask(ctx, task, false)
	require.Nil(t, err)

	task.TaskType = proto.ShardTaskTypeClearShard
	err = s.executeShardTask(ctx, task, true)
	require.Nil(t, err)
}

func TestService_ShardReport(t *testing.T) {
	diskID := genDiskID()
	disk, _, err := storage.NewMockDisk(t, diskID)
	require.Nil(t, err)

	disks := make(map[proto.DiskID]*storage.MockDisk)
	disks[diskID] = disk

	tp := newBaseTp(t)

	s, clear, err := newMockService(t, mockServiceCfg{
		tp:    tp,
		disks: disks,
	})
	require.Nil(t, err)
	defer clear()

	shardCnt := 2
	rgs := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 1, shardCnt)
	suids := make([]proto.Suid, 0)
	for i := 0; i < shardCnt; i++ {
		suid := proto.EncodeSuid(proto.ShardID(i), 0, 0)
		err := s.addShard(ctx, &shardnode.AddShardArgs{
			DiskID: diskID,
			Suid:   suid,
			Range:  *rgs[i],
			Units: []clustermgr.ShardUnit{
				{DiskID: diskID, Suid: suid},
			},
			RouteVersion: 0,
		})
		require.Nil(t, err)
		suids = append(suids, suid)
	}

	tasks := make([]clustermgr.ShardTask, 0)
	for _, suid := range suids {
		tasks = append(tasks, clustermgr.ShardTask{
			TaskType:        proto.ShardTaskTypeSyncRouteVersion,
			DiskID:          diskID,
			Suid:            suid,
			OldRouteVersion: proto.InvalidRouteVersion,
			RouteVersion:    proto.RouteVersion(1),
		})
	}
	tasks = append(tasks, clustermgr.ShardTask{TaskType: proto.ShardTaskTypeClearShard})

	tp.EXPECT().ShardReport(A, A).Return(tasks, nil)

	shards := make([]storage.ShardHandler, 0)
	reports := make([]clustermgr.ShardUnitInfo, 0)
	err = s.shardReports(ctx, shards, reports, false, proto.ShardTaskTypeSyncRouteVersion)
	require.Nil(t, err)
}
