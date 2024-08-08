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

package blobnode

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func mockGenShardMigrateTask(shardID proto.ShardID, taskType proto.TaskType, idc string, diskID proto.DiskID,
	state proto.ShardTaskState) (task *proto.ShardMigrateTask,
) {
	sourceSuid := proto.EncodeSuid(shardID, 2, 0)
	desSuid := proto.EncodeSuid(shardID, 2, 1)
	leaderSuid := proto.EncodeSuid(shardID, 0, 1)

	task = &proto.ShardMigrateTask{
		TaskID:      "mock_task_id",
		TaskType:    taskType,
		Ctime:       time.Now().String(),
		SourceIDC:   idc,
		State:       state,
		Source:      proto.ShardUnitInfoSimple{Suid: sourceSuid, DiskID: diskID, Host: "127.0.0.1:xx"},
		Leader:      proto.ShardUnitInfoSimple{Suid: leaderSuid, DiskID: 2, Host: "127.0.0.1:xx"},
		Destination: proto.ShardUnitInfoSimple{Suid: desSuid, DiskID: 3, Host: "127.0.0.1:xx"},
		Threshold:   0,
	}
	return task
}

func TestShardWorker_AddShardMember(t *testing.T) {
	ctr := gomock.NewController(t)
	shardNode := NewMockIShardNode(ctr)
	ctx := context.Background()

	{
		task := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z0", 1, proto.ShardTaskStatePrepared)
		shardNode.EXPECT().UpdateShard(any, any).Return(nil)
		shardNode.EXPECT().GetShardStatus(any, any, any).Return(&client.ShardStatusRet{
			LeaderIndex:  1,
			AppliedIndex: 1,
		}, nil)
		shardWorker := NewShardWorker(task, shardNode, 1)
		err := shardWorker.AddShardMember(ctx)
		require.NoError(t, err)
	}
	{
		task := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z0", 1, proto.ShardTaskStatePrepared)
		shardNode.EXPECT().UpdateShard(any, any).Return(nil)
		shardNode.EXPECT().GetShardStatus(any, any, any).Times(4).Return(&client.ShardStatusRet{
			LeaderIndex:  2,
			AppliedIndex: 1,
		}, nil)
		shardWorker := NewShardWorker(task, shardNode, 10)
		err := shardWorker.AddShardMember(ctx)
		require.Error(t, err)
	}
	{
		task := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z0", 1, proto.ShardTaskStatePrepared)
		shardNode.EXPECT().UpdateShard(any, any).Times(3).Return(errMock)
		shardWorker := NewShardWorker(task, shardNode, 10)
		err := shardWorker.AddShardMember(ctx)
		require.Error(t, err)
	}
}

func TestShardWorker_RemoveShardMember(t *testing.T) {
	ctr := gomock.NewController(t)
	shardNode := NewMockIShardNode(ctr)
	ctx := context.Background()

	{
		task := mockGenShardMigrateTask(101, proto.TaskTypeShardDiskRepair, "z0", 1, proto.ShardTaskStatePrepared)
		shardNode.EXPECT().UpdateShard(any, any).Times(3).Return(errMock)
		shardWorker := NewShardWorker(task, shardNode, 10)
		err := shardWorker.RemoveShardMember(ctx)
		require.Error(t, err)
	}
}
