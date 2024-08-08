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

package client

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

//go:generate mockgen -destination=./shard_client_mock_test.go -package=client -mock_names IShardNode=MockIShardNode github.com/cubefs/cubefs/blobstore/blobnode/client IShardNode

type ShardStatusRet struct {
	Suid         proto.Suid   `json:"suid"`
	DiskID       proto.DiskID `json:"disk_id"`
	AppliedIndex uint64       `json:"applied_index"`
	LeaderIndex  uint64       `json:"leader_index"`
}

type ShardUnitInfo struct {
	Suid    proto.Suid   `json:"suid"`
	DiskID  proto.DiskID `json:"disk_id"`
	Learner bool         `json:"learner"`
}

type UpdateShardArgs struct {
	Unit proto.ShardUnitInfoSimple `json:"unit"`
	Type proto.ShardUpdateType     `json:"type"`
}

// IShardNode ShardNode client
type IShardNode interface {
	UpdateShard(ctx context.Context, args *UpdateShardArgs) error
	GetShardStatus(ctx context.Context, suid proto.Suid, diskID proto.DiskID) (*ShardStatusRet, error)
}

type ShardNodeClient struct{}

func (c *ShardNodeClient) UpdateShard(ctx context.Context, args *UpdateShardArgs) error {
	return nil
}

func (c *ShardNodeClient) GetShardStatus(ctx context.Context, suid proto.Suid, diskID proto.DiskID) (*ShardStatusRet, error) {
	return nil, nil
}
