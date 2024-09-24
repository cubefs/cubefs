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
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

func addCmdShard(cmd *grumble.Command) {
	shardCommand := &grumble.Command{
		Name:     "shard",
		Help:     "shard tools",
		LongHelp: "shard tools for shardNode",
	}
	cmd.AddCommand(shardCommand)

	// get shard stats
	shardCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get shard form shardNode",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			args.SuidRegister(a)
		},
		Run: cmdGetShard,
	})

	//  list shard
	shardCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list shards form shardNode",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			a.Uint64("count", "list shard count")
		},
		Run: cmdListShard,
	})

	// transfer shard leader
	shardCommand.AddCommand(&grumble.Command{
		Name: "transferLeader",
		Help: "transfer shard leader",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			args.SuidRegister(a)
			a.Uint("targetDiskID", "target leader diskID")
		},
		Run: cmdTransferShardLeader,
	})
}

func cmdGetShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	suid := args.Suid(c.Args)

	cli := shardnode.New(rpc2.Client{})
	ret, err := cli.GetShardStats(ctx, host, shardnode.GetShardArgs{
		DiskID: diskID,
		Suid:   suid,
	})
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(ret))
	return nil
}

func cmdListShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	count := c.Args.Uint64("count")

	cli := shardnode.New(rpc2.Client{})
	ret, err := cli.ListShards(ctx, host, shardnode.ListShardArgs{
		DiskID: diskID,
		Count:  count,
	})
	if err != nil {
		return err
	}
	for _, shard := range ret.Shards {
		fmt.Println(common.Readable(shard))
	}
	return nil
}

func cmdTransferShardLeader(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	suid := args.Suid(c.Args)
	targetDiskID := c.Args.Uint("targetDiskID")

	cli := shardnode.New(rpc2.Client{})
	err := cli.TransferShardLeader(ctx, host, shardnode.TransferShardLeaderArgs{
		DiskID:     diskID,
		Suid:       suid,
		DestDiskID: proto.DiskID(targetDiskID),
	})
	if err != nil {
		return err
	}
	return nil
}
