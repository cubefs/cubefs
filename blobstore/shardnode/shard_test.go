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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

var (
	diskID  = proto.DiskID(1)
	shardID = proto.ShardID(1)
	suid    = proto.EncodeSuid(shardID, 0, 0)
	_, ctx  = trace.StartSpanFromContext(context.Background(), "Testing")
)

func newMockService(t *testing.T) (*service, func()) {
	s := &service{}

	mockDisk, clearFunc := storage.NewMockDisk(t)
	disk := mockDisk.GetDisk()

	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	disk.AddShard(ctx, suid, 0, *rg, []clustermgr.ShardUnit{{DiskID: diskID}})
	s.disks = make(map[proto.DiskID]*storage.Disk, 0)
	s.disks[diskID] = disk

	s.taskPool = taskpool.New(1, 1)
	return s, clearFunc
}

func TestService_Task(t *testing.T) {
	s, clear := newMockService(t)
	defer clear()

	task := clustermgr.ShardTask{
		TaskType:        proto.ShardTaskTypeSyncRouteVersion,
		DiskID:          diskID,
		Suid:            suid,
		OldRouteVersion: 0,
		RouteVersion:    0,
	}
	err := s.executeShardTask(ctx, task)
	require.Nil(t, err)
	task.RouteVersion = 1
	err = s.executeShardTask(ctx, task)
	require.Nil(t, err)
	task.TaskType = proto.ShardTaskTypeClearShard
	err = s.executeShardTask(ctx, task)
	require.Nil(t, err)
	task.OldRouteVersion = 1
	task.RouteVersion = 0
	err = s.executeShardTask(ctx, task)
	require.Nil(t, err)
	task.OldRouteVersion = 1
	task.RouteVersion = 2
	err = s.executeShardTask(ctx, task)
	require.Nil(t, err)
}
