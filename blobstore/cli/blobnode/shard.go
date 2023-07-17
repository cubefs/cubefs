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
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/desertbit/grumble"
)

func addCmdShard(cmd *grumble.Command) {
	chunkCommand := &grumble.Command{
		Name:     "shard",
		Help:     "shard tools",
		LongHelp: "shard tools for blobnode",
	}
	cmd.AddCommand(chunkCommand)

	chunkCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "shard stat",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
			f.UintL("vuid", 1, "vuid")
			f.UintL("bid", 1, "bid")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.StatShardArgs{
				DiskID: proto.DiskID(c.Flags.Uint("diskid")),
				Vuid:   proto.Vuid(c.Flags.Uint("vuid")),
				Bid:    proto.BlobID(c.Flags.Uint("bid")),
			}
			stat, err := cli.StatShard(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})

	chunkCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get shard",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
			f.UintL("vuid", 1, "vuid")
			f.UintL("bid", 1, "bid")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.GetShardArgs{
				DiskID: proto.DiskID(c.Flags.Uint("diskid")),
				Vuid:   proto.Vuid(c.Flags.Uint("vuid")),
				Bid:    proto.BlobID(c.Flags.Uint("bid")),
				Type:   blobnode.IOType(0),
			}
			body, _, err := cli.GetShard(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println(body)
			return nil
		},
	})

	chunkCommand.AddCommand(&grumble.Command{
		Name: "mark",
		Help: "mark delete is dangerous operation, execute with caution",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
		},
		Args: func(c *grumble.Args) {
			c.Uint64("diskid", "disk id")
			c.Uint64("vuid", "vuid")
			c.Uint64("bid", "bid")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.DeleteShardArgs{
				DiskID: proto.DiskID(c.Args.Uint64("diskid")),
				Vuid:   proto.Vuid(c.Args.Uint64("vuid")),
				Bid:    proto.BlobID(c.Args.Uint64("bid")),
			}
			if !common.Confirm("to mark delete?") {
				return nil
			}
			if err := cli.MarkDeleteShard(common.CmdContext(), host, &args); err != nil {
				return err
			}
			fmt.Println("mark delete success")
			return nil
		},
	})
}
