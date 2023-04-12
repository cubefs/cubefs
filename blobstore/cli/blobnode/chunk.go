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
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/desertbit/grumble"
)

func addCmdChunk(cmd *grumble.Command) {
	chunkCommand := &grumble.Command{
		Name:     "chunk",
		Help:     "chunk tools",
		LongHelp: "chunk tools for blobnode",
	}
	cmd.AddCommand(chunkCommand)

	chunkCommand.AddCommand(&grumble.Command{
		Name: "id",
		Help: "decode chunk id",
		Args: func(a *grumble.Args) {
			a.String("id", "chunk id")
		},
		Run: func(c *grumble.Context) error {
			chunkID, err := blobnode.DecodeChunk(c.Args.String("id"))
			if err != nil {
				return err
			}
			fmt.Println(cfmt.ChunkidF(chunkID))
			return nil
		},
	})

	chunkCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "show stat of chunk",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
			f.UintL("vuid", 1, "vuid")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.StatChunkArgs{
				DiskID: proto.DiskID(c.Flags.Uint("diskid")),
				Vuid:   proto.Vuid(c.Flags.Uint("vuid")),
			}
			stat, err := cli.StatChunk(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})

	chunkCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list of chunks",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.ListChunkArgs{DiskID: proto.DiskID(c.Flags.Uint("diskid"))}
			stat, err := cli.ListChunks(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})

	chunkCommand.AddCommand(&grumble.Command{
		Name: "create",
		Help: "create chunk",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
			f.UintL("vuid", 101, "vuid")
			f.Int64L("chunksize", 1024, "chunk size")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.CreateChunkArgs{
				DiskID:    proto.DiskID(c.Flags.Uint("diskid")),
				Vuid:      proto.Vuid(c.Flags.Uint("vuid")),
				ChunkSize: c.Flags.Int64("chunksize"),
			}
			err := cli.CreateChunk(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println("chunk create success.")
			return nil
		},
	})

	addCmdChunkDump(chunkCommand)
}
