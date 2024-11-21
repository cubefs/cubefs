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
	"bytes"
	"io"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util"

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
			f.UintL("diskid", 1, "disk id to stat")
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
			f.UintL("diskid", 1, "disk id to get")
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
			c.Uint64("diskid", "disk id to mark")
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

	chunkCommand.AddCommand(&grumble.Command{
		Name: "put2",
		Help: "put shard rpc2",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 0, "diskid")
			f.UintL("vuid", 0, "vuid")
			f.UintL("bid", 0, "bid")
			f.Int64L("length", 0, "length")
			f.Int64L("size", 0, "size")
			f.StringL("raw", "", "raw data")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New2(rpc2.Client{})
			raw := c.Flags.String("raw")
			size := c.Flags.Int64("size")
			var body io.Reader = util.DiscardReader(int(size))
			if raw != "" {
				size = int64(len(raw))
				body = bytes.NewReader([]byte(raw))
			}
			args := blobnode.PutShardArgs{
				DiskID: proto.DiskID(c.Flags.Uint("diskid")),
				Vuid:   proto.Vuid(c.Flags.Uint("vuid")),
				Bid:    proto.BlobID(c.Flags.Uint("bid")),
				Length: c.Flags.Int64("length"),
				Size:   size,
				Body:   body,
			}
			_, err := cli.PutShard(common.CmdContext(), c.Flags.String("host"), &args)
			return err
		},
	})
	chunkCommand.AddCommand(&grumble.Command{
		Name: "get2",
		Help: "get shard rpc2",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id to get")
			f.UintL("vuid", 1, "vuid")
			f.UintL("bid", 1, "bid")
			f.Int64L("offset", 0, "offset")
			f.Int64L("size", -1, "size")
			f.BoolL("withcrc", false, "withcrc")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New2(rpc2.Client{})
			host := c.Flags.String("host")
			offset, size := c.Flags.Int64("offset"), c.Flags.Int64("size")
			if offset < 0 || size < 0 {
				return fmt.Errorf("pls set --offset and --size")
			}
			body, _, err := cli.RangeGetShard(common.CmdContext(), host, &blobnode.RangeGetShardArgs{
				GetShardArgs: blobnode.GetShardArgs{
					DiskID:  proto.DiskID(c.Flags.Uint("diskid")),
					Vuid:    proto.Vuid(c.Flags.Uint("vuid")),
					Bid:     proto.BlobID(c.Flags.Uint("bid")),
					WithCrc: c.Flags.Bool("withcrc"),
				},
				Offset: offset,
				Size:   size,
			})
			if err != nil {
				return err
			}
			defer body.Close()
			if size > (1 << 10) {
				fmt.Println("discard body data ...")
				_, err = io.CopyN(io.Discard, body, size)
				return err
			}
			buff := make([]byte, size)
			if _, err = io.ReadFull(body, buff); err == nil {
				fmt.Printf("got length: %d\n", len(buff))
				fmt.Printf("got   data: >>> `%s` <<<\n", string(buff))
			}
			return err
		},
	})
}
