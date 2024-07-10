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

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type AllocShardUnitArgs struct {
	Suid proto.Suid `json:"suid"`
}

type AllocShardUnitRet struct {
	Suid   proto.Suid   `json:"suid"`
	DiskID proto.DiskID `json:"disk_id"`
	Host   string       `json:"host"`
}

func (c *Client) AllocShardUnit(ctx context.Context, args *AllocShardUnitArgs) (ret *AllocShardUnitRet, err error) {
	err = c.PostWith(ctx, "/shard/unit/alloc", &ret, args)
	return
}

type UpdateShardArgs struct {
	NewSuid     proto.Suid   `json:"new_suid"`
	NewIsLeaner bool         `json:"new_is_leaner"`
	NewDiskID   proto.DiskID `json:"new_disk_id"`
	OldSuid     proto.Suid   `json:"old_suid"`
	OldIsLeaner bool         `json:"old_is_leaner"`
}

func (c *Client) UpdateShard(ctx context.Context, args *UpdateShardArgs) (err error) {
	err = c.PostWith(ctx, "/shard/update", nil, args)
	return
}

type ReportShardArgs struct {
	ShardReports []ShardReport `json:"shard_reports"`
}

type ReportShardRet struct {
	ShardTasks []ShardTask `json:"shard_tasks"`
}

func (c *Client) ReportShard(ctx context.Context, args *ReportShardArgs) (ret []ShardTask, err error) {
	result := &ReportShardRet{}
	err = c.PostWith(ctx, "/shard/report", result, args)
	return result.ShardTasks, err
}

type GetShardArgs struct {
	ShardID proto.ShardID `json:"shard_id"`
}

func (c *Client) GetShardInfo(ctx context.Context, args *GetShardArgs) (ret *Shard, err error) {
	ret = &Shard{}
	err = c.GetWith(ctx, "/shard/get?shard_id="+args.ShardID.ToString(), ret)
	return
}

type ListShardUnitArgs struct {
	DiskID proto.DiskID `json:"disk_id"`
}

type ListShardUnitInfos struct {
	ShardUnitInfos []*ShardUnitInfo `json:"shard_unit_infos"`
}

func (c *Client) ListShardUnit(ctx context.Context, args *ListShardUnitArgs) ([]*ShardUnitInfo, error) {
	ret := &ListShardUnitInfos{}
	err := c.GetWith(ctx, "/shard/unit/list?disk_id="+args.DiskID.ToString(), &ret)
	return ret.ShardUnitInfos, err
}

type ListShardArgs struct {
	// list marker
	Marker proto.ShardID `json:"marker,omitempty"`
	// one-page count
	Count int `json:"count"`
}

type ListShards struct {
	Shards []*Shard      `json:"shards"`
	Marker proto.ShardID `json:"marker"`
}

func (c *Client) ListShard(ctx context.Context, args *ListShardArgs) (ret ListShards, err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/shard/list?marker=%d&count=%d", args.Marker, args.Count), &ret)
	return
}
