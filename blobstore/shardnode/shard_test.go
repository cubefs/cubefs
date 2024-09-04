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
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestService_Task(t *testing.T) {
	s, clear := newMockService(t)
	defer clear()

	err := s.disks[diskID].AddShard(ctx, suid, 0, *rg, []clustermgr.ShardUnit{{DiskID: diskID}})
	require.Nil(t, err)

	task := clustermgr.ShardTask{
		TaskType:        proto.ShardTaskTypeSyncRouteVersion,
		DiskID:          diskID,
		Suid:            suid,
		OldRouteVersion: 0,
		RouteVersion:    0,
	}
	err = s.executeShardTask(ctx, task)
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
