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
	"strconv"
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
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

	command.AddCommand(&grumble.Command{
		Name: "createSpace",
		Help: "create space",
		Run:  cmdCreateSpace,
		Args: func(a *grumble.Args) {
			args.SpaceNameRegister(a)
			a.StringList("fieldMetas", "field metas list, like [name|type|index, ...]", grumble.Default([]string{}), grumble.Max(100))
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "adminUpdateShardUnit",
		Help: "admin update shard unit",
		Run:  cmdAdminUpdateShardUnit,
		Args: func(a *grumble.Args) {
			a.Uint64("suid", "shard unit id")
			a.Uint("epoch", "suid epoch to update")
			a.Uint("nextEpoch", "suid nextEpoch to update")
			a.Uint("diskID", "suid diskID to update")
			a.Uint("status", "suid status to update")
			a.Bool("learner", "suid learner to update")
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "adminUpdateShard",
		Help: "admin update shard",
		Run:  cmdAdminUpdateShard,
		Args: func(a *grumble.Args) {
			args.ShardIDRegister(a)
			a.Uint64("routeVersion", "route version to update")
			a.Int("rangeType", "range type to update")
			a.StringList("subRanges", "sub range list, like [min|max, ...]", grumble.Min(2), grumble.Max(2))
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

func cmdCreateSpace(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	spaceName := args.SpaceName(c.Args)
	fieldMetaList := c.Args.StringList("fieldMetas")
	fieldMetas := make([]clustermgr.FieldMeta, 0, len(fieldMetaList))
	for _, fieldMeta := range fieldMetaList {
		strs := strings.SplitN(fieldMeta, "|", 3)
		fieldName := strs[0]
		fieldType, err := strconv.ParseUint(strs[1], 10, 8)
		if err != nil {
			return err
		}
		fieldIndex, err := strconv.ParseUint(strs[2], 10, 8)
		if err != nil {
			return err
		}
		fieldMetas = append(fieldMetas, clustermgr.FieldMeta{
			Name:        fieldName,
			FieldType:   proto.FieldType(fieldType),
			IndexOption: proto.IndexOption(fieldIndex),
		})
	}

	createSpaceArgs := &clustermgr.CreateSpaceArgs{
		Name:       spaceName,
		FieldMetas: fieldMetas,
	}
	err := cmClient.CreateSpace(ctx, createSpaceArgs)
	if err != nil {
		return err
	}
	fmt.Println("create space success")

	space, err := cmClient.GetSpaceByName(ctx, &clustermgr.GetSpaceByNameArgs{Name: spaceName})
	if err != nil {
		return err
	}
	fmt.Println("space info:")
	fmt.Println(common.Readable(space))
	return nil
}

func cmdAdminUpdateShardUnit(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	suid := c.Args.Uint64("suid")
	epoch := c.Args.Uint("epoch")
	nextEpoch := c.Args.Uint("nextEpoch")
	diskID := c.Args.Uint("diskID")
	status := c.Args.Uint("status")
	learner := c.Args.Bool("learner")

	updateShardUnitArgs := &clustermgr.AdminUpdateShardUnitArgs{
		Epoch:     uint32(epoch),
		NextEpoch: uint32(nextEpoch),
		ShardUnit: clustermgr.ShardUnit{
			Suid:    proto.Suid(suid),
			DiskID:  proto.DiskID(diskID),
			Status:  proto.ShardUnitStatus(status),
			Learner: learner,
		},
	}
	err := cmClient.AdminUpdateShardUnit(ctx, updateShardUnitArgs)
	if err != nil {
		return err
	}
	fmt.Println("update shard unit success")

	info, err := cmClient.GetShardInfo(ctx, &clustermgr.GetShardArgs{ShardID: updateShardUnitArgs.Suid.ShardID()})
	if err != nil {
		return err
	}
	fmt.Println("shard info:")
	fmt.Println(common.Readable(info))
	return nil
}

func cmdAdminUpdateShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	shardID := args.ShardID(c.Args)
	routeVersion := c.Args.Uint64("routeVersion")
	rangeType := c.Args.Int("rangeType")
	subRangeList := c.Args.StringList("subRanges")
	subRanges := make([]sharding.SubRange, 0, len(subRangeList))
	for _, subRangeStr := range subRangeList {
		strs := strings.SplitN(subRangeStr, "|", 2)
		min, err := strconv.ParseUint(strs[0], 10, 64)
		if err != nil {
			return err
		}
		max, err := strconv.ParseUint(strs[1], 10, 64)
		if err != nil {
			return err
		}
		subRanges = append(subRanges, sharding.SubRange{
			Min: min,
			Max: max,
		})
	}

	updateShardArgs := &clustermgr.Shard{
		ShardID:      shardID,
		RouteVersion: proto.RouteVersion(routeVersion),
		Range: sharding.Range{
			Type: sharding.RangeType(rangeType),
			Subs: subRanges,
		},
	}
	err := cmClient.AdminUpdateShard(ctx, updateShardArgs)
	if err != nil {
		return err
	}
	fmt.Println("update shard success")

	info, err := cmClient.GetShardInfo(ctx, &clustermgr.GetShardArgs{ShardID: shardID})
	if err != nil {
		return err
	}
	fmt.Println("shard info:")
	fmt.Println(common.Readable(info))
	return nil
}
