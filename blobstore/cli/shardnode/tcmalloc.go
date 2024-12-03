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
	"errors"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/desertbit/grumble"
)

func addCmdTCMalloc(cmd *grumble.Command) {
	tcMallocCommand := &grumble.Command{
		Name:     "tcmalloc",
		Help:     "tcmalloc tools",
		LongHelp: "tcmalloc tools for shardNode",
	}
	cmd.AddCommand(tcMallocCommand)

	tcMallocCommand.AddCommand(&grumble.Command{
		Name: "stats",
		Help: "tcmalloc stats",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			a.String("op", "stats, free or rate")
		},
		Run: cmdTCMallocStats,
	})

	tcMallocCommand.AddCommand(&grumble.Command{
		Name: "db",
		Help: "db stats",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			a.String("db", "stats, free or rate")
		},
		Run: cmdDBStats,
	})
}

func cmdTCMallocStats(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	op := c.Args.String("op")

	var (
		ret shardnode.TCMallocRet
		err error
	)
	cli := shardnode.New(rpc2.Client{})
	args := shardnode.TCMallocArgs{}
	switch op {
	case "stats":
		ret, err = cli.TCMallocStats(ctx, host, args)
	case "free":
		ret, err = cli.TCMallocFree(ctx, host, args)
	case "rate":
		ret, err = cli.TCMallocRate(ctx, host, args)
	default:
		err = errors.New("unknown op of tcmalloc")
	}
	if err != nil {
		return err
	}
	fmt.Println(ret.Stats)
	return nil
}

func cmdDBStats(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	db := c.Args.String("db")

	cli := shardnode.New(rpc2.Client{})
	args := shardnode.DBStatsArgs{
		DiskID: diskID,
		DBName: db,
	}
	ret, err := cli.DBStats(ctx, host, args)
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(ret))
	return nil
}
