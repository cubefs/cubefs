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
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type ShardRepairArgs struct {
	Task proto.ShardRepairTask `json:"task"`
}

type WorkerStats struct {
	CancelCount  string `json:"cancel_count"`
	ReclaimCount string `json:"reclaim_count"`
}

func (c *client) RepairShard(ctx context.Context, host string, args *ShardRepairArgs) (err error) {
	urlStr := fmt.Sprintf("%v/shard/repair", host)
	err = c.PostWith(ctx, urlStr, nil, args)
	return
}

func (c *client) WorkerStats(ctx context.Context, host string) (ret WorkerStats, err error) {
	err = c.GetWith(ctx, host+"/worker/stats", &ret)
	return
}
