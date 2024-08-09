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
	"errors"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

//go:generate mockgen -destination=../mock_shard_client_test.go -package=blobnode -mock_names IShardNode=MockIShardNode github.com/cubefs/cubefs/blobstore/blobnode/client IShardNode

var (
	LeaderOutdatedErr       = errors.New("leader outdated")
	SuidNotMemberOfShardErr = errors.New("suid not member of shard")
)

type ShardStatusRet struct {
	Suid               proto.Suid                `json:"suid"`
	DiskID             proto.DiskID              `json:"disk_id"`
	Leader             proto.ShardUnitInfoSimple `json:"leader"`        // leader
	AppliedIndex       uint64                    `json:"applied_index"` // applied index of new add node
	LeaderAppliedIndex uint64                    `json:"leader_index"`  // applied index of leader
}

type UpdateShardArgs struct {
	Unit   proto.ShardUnitInfoSimple `json:"unit"`
	Type   proto.ShardUpdateType     `json:"type"`
	Leader proto.ShardUnitInfoSimple `json:"leader"`
}

type LeaderTransferArgs struct {
	DiskID proto.DiskID              `json:"disk_id"`
	Leader proto.ShardUnitInfoSimple `json:"leader"`
}

// IShardNode ShardNode client
type IShardNode interface {
	UpdateShard(ctx context.Context, args *UpdateShardArgs) error
	GetShardStatus(ctx context.Context, suid proto.Suid, leader proto.ShardUnitInfoSimple) (*ShardStatusRet, error)
	LeaderTransfer(ctx context.Context, args *LeaderTransferArgs) error
	UpdateTaskLeader(ctx context.Context, leader proto.ShardUnitInfoSimple) (*proto.ShardUnitInfoSimple, error)
}

type ShardNodeClient struct {
	cli *shardnode.Client
}

func NewShardNodeClient(cfg shardnode.Config) IShardNode {
	return &ShardNodeClient{
		cli: shardnode.New(cfg),
	}
}

func (c *ShardNodeClient) UpdateShard(ctx context.Context, args *UpdateShardArgs) error {
	return c.cli.UpdateShard(ctx, args.Leader.Host, shardnode.UpdateShardArgs{
		Suid:            args.Leader.Suid,
		DiskID:          args.Leader.DiskID,
		ShardUpdateType: args.Type,
		Unit: clustermgr.ShardUnit{
			Suid:    args.Unit.Suid,
			DiskID:  args.Unit.DiskID,
			Learner: args.Unit.Learner,
		},
	})
}

func (c *ShardNodeClient) GetShardStatus(ctx context.Context, suid proto.Suid, leader proto.ShardUnitInfoSimple) (*ShardStatusRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	stat, err := c.cli.GetShardStats(ctx, leader.Host, shardnode.GetShardArgs{Suid: leader.Suid, DiskID: leader.DiskID})
	if err != nil {
		return nil, err
	}
	ret := new(ShardStatusRet)
	if stat.LeaderSuid != leader.Suid {
		ret.Leader = proto.ShardUnitInfoSimple{
			Suid:   stat.LeaderSuid,
			DiskID: stat.LeaderDiskID,
			Host:   stat.LeaderHost,
		}
		return ret, LeaderOutdatedErr
	}
	newIdx := len(stat.Units)
	leaderIdx := len(stat.Units)
	for idx, unit := range stat.Units {
		if unit.Suid == suid {
			newIdx = idx
		}
		if unit.DiskID == stat.LeaderDiskID {
			leaderIdx = idx
		}
	}
	if newIdx >= len(stat.Units) || leaderIdx >= len(stat.Units) {
		span.Errorf("suid[%d] not member of shard leader suid[%d], disk[%d]", suid, leader.Suid, leader.DiskID)
		return ret, SuidNotMemberOfShardErr
	}

	ret.Leader = leader
	ret.Suid = stat.Units[newIdx].Suid
	ret.DiskID = stat.Units[newIdx].DiskID
	ret.AppliedIndex = stat.RaftStat.Peers[newIdx].Match
	ret.LeaderAppliedIndex = stat.RaftStat.Peers[leaderIdx].Match

	return ret, nil
}

func (c *ShardNodeClient) UpdateTaskLeader(ctx context.Context,
	leader proto.ShardUnitInfoSimple,
) (*proto.ShardUnitInfoSimple, error) {
	stat, err := c.cli.GetShardStats(ctx, leader.Host, shardnode.GetShardArgs{Suid: leader.Suid, DiskID: leader.DiskID})
	if err != nil {
		return nil, err
	}
	if stat.LeaderSuid != leader.Suid {
		leader = proto.ShardUnitInfoSimple{
			Suid:   stat.LeaderSuid,
			DiskID: stat.LeaderDiskID,
			Host:   stat.LeaderHost,
		}
	}
	return &leader, nil
}

func (c *ShardNodeClient) LeaderTransfer(ctx context.Context, args *LeaderTransferArgs) error {
	return c.cli.TransferShardLeader(ctx, args.Leader.Host, shardnode.TransferShardLeaderArgs{
		DestDiskID: args.DiskID,
		DiskID:     args.Leader.DiskID,
		Suid:       args.Leader.Suid,
	})
}
