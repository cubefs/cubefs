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
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdCatalog(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "catalog",
		Help:     "catalog tools",
		LongHelp: "catalog tools for clustermgr",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "getShardInfo",
		Help: "get shard info",
		Run:  cmdGetShardInfo,
		Args: func(a *grumble.Args) {
			args.ShardIDRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "listShardUnit",
		Help: "list shard unit",
		Run:  cmdListShardUnit,
		Args: func(a *grumble.Args) {
			args.DiskIDRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "listShard",
		Help: "list shard",
		Run:  cmdListShard,
		Args: func(a *grumble.Args) {
			a.Uint("count", "number of shards to list")
			a.Uint64("marker", "list shard start from special ShardID", grumble.Default(uint64(0)))
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "getSpaceInfo",
		Help: "get space info",
		Run:  cmdGetSpaceInfo,
		Args: func(a *grumble.Args) {
			args.SpaceIDRegister(a, grumble.Default(uint64(0)))
			args.SpaceNameRegister(a, grumble.Default(""))
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "listSpace",
		Help: "list space",
		Run:  cmdListSpace,
		Args: func(a *grumble.Args) {
			a.Uint("count", "number of spaces to list")
			a.Uint64("marker", "list space start from special SpaceID", grumble.Default(uint64(0)))
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})
}

func cmdListShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	count := c.Args.Uint("count")
	marker := c.Args.Uint64("marker")
	listShardArgs := &clustermgr.ListShardArgs{Count: uint32(count), Marker: proto.ShardID(marker)}

	ret, err := cmClient.ListShard(ctx, listShardArgs)
	if err != nil {
		return err
	}
	for _, shard := range ret.Shards {
		fmt.Println(common.Readable(shard))
	}
	return nil
}

func cmdGetShardInfo(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	shardInfo, err := cmClient.GetShardInfo(ctx,
		&clustermgr.GetShardArgs{ShardID: args.ShardID(c.Args)})
	if err != nil {
		return err
	}

	fmt.Println("shard info:")
	fmt.Println(cfmt.ShardInfoJoin(shardInfo, "\t"))
	return nil
}

func cmdListShardUnit(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	ret, err := cmClient.ListShardUnit(ctx,
		&clustermgr.ListShardUnitArgs{DiskID: args.DiskID(c.Args)})
	if err != nil {
		return err
	}

	fmt.Println("shard units:")
	for _, unit := range ret {
		fmt.Println(common.Readable(unit))
	}
	return nil
}

func cmdGetSpaceInfo(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	var err error
	var space *clustermgr.Space
	spaceID := args.SpaceID(c.Args)
	spaceName := args.SpaceName(c.Args)

	if len(spaceName) != 0 {
		space, err = cmClient.GetSpaceByName(ctx, &clustermgr.GetSpaceByNameArgs{Name: spaceName})
	} else {
		space, err = cmClient.GetSpaceByID(ctx, &clustermgr.GetSpaceByIDArgs{SpaceID: spaceID})
	}
	if err != nil {
		return err
	}

	fmt.Println("space info:")
	fmt.Println(common.Readable(space))
	return nil
}

func cmdListSpace(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	count := c.Args.Uint("count")
	marker := c.Args.Uint64("marker")
	listSpaceArgs := &clustermgr.ListSpaceArgs{Count: uint32(count), Marker: proto.SpaceID(marker)}

	ret, err := cmClient.ListSpace(ctx, listSpaceArgs)
	if err != nil {
		return err
	}
	for _, space := range ret.Spaces {
		fmt.Println(common.Readable(space))
	}
	return nil
}
