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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func (c *Client) AddShard(ctx context.Context, host string, args AddShardArgs) error {
	return nil
}

func (c *Client) UpdateShard(ctx context.Context, host string, args UpdateShardArgs) error {
	return nil
}

func (c *Client) GetShardUintInfo(ctx context.Context, host string, args GetShardArgs) (ret clustermgr.ShardUnitInfo, err error) {
	return clustermgr.ShardUnitInfo{}, nil
}

func (c *Client) GetShardStats(ctx context.Context, host string, diskID proto.DiskID, suid proto.Suid) (ret ShardStats, err error) {
	return ShardStats{}, nil
}
