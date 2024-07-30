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

package clustermgr

import (
	"context"
	"fmt"
)

func (c *Client) AllocShardUnit(ctx context.Context, args *AllocShardUnitArgs) (ret *AllocShardUnitRet, err error) {
	err = c.PostWith(ctx, "/shard/unit/alloc", &ret, args)
	return
}

func (c *Client) UpdateShard(ctx context.Context, args *UpdateShardArgs) (err error) {
	err = c.PostWith(ctx, "/shard/update", nil, args)
	return
}

func (c *Client) ReportShard(ctx context.Context, args *ShardReportArgs) (ret []ShardTask, err error) {
	result := &ShardReportRet{}
	err = c.PostWith(ctx, "/shard/report", result, args)
	return result.ShardTasks, err
}

func (c *Client) GetShardInfo(ctx context.Context, args *GetShardArgs) (ret *Shard, err error) {
	ret = &Shard{}
	err = c.GetWith(ctx, "/shard/get?shard_id="+args.ShardID.ToString(), ret)
	return
}

func (c *Client) ListShardUnit(ctx context.Context, args *ListShardUnitArgs) ([]ShardUnitInfo, error) {
	ret := &ListShardUnitRet{}
	err := c.GetWith(ctx, "/shard/unit/list?disk_id="+args.DiskID.ToString(), ret)
	return ret.ShardUnitInfos, err
}

func (c *Client) ListShard(ctx context.Context, args *ListShardArgs) (ret ListShardRet, err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/shard/list?marker=%d&count=%d", args.Marker, args.Count), &ret)
	return
}
