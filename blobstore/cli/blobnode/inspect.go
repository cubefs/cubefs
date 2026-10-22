// Copyright 2026 The CubeFS Authors.
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

func addCmdInspect(cmd *grumble.Command) {
	inspectCommand := &grumble.Command{
		Name:     "inspect",
		Help:     "inspect tools",
		LongHelp: "inspect tools for blobnode",
	}
	cmd.AddCommand(inspectCommand)

	inspectCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "show inspect manager stat",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			ret, err := cli.GetInspectStat(common.CmdContext(), host)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(ret))
			return nil
		},
	})

	inspectCommand.AddCommand(&grumble.Command{
		Name: "disk_stat",
		Help: "show inspect disk state, diskid=0 means all disks",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 0, "disk id, 0 means all")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			ret, err := cli.GetInspectDiskState(common.CmdContext(), host, &blobnode.DiskStatArgs{
				DiskID: proto.DiskID(c.Flags.Uint("diskid")),
			})
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(ret))
			return nil
		},
	})

	inspectCommand.AddCommand(&grumble.Command{
		Name: "chunk_stat",
		Help: "show inspect chunk state, vuid=0 means all chunks under the disk",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
			f.UintL("vuid", 0, "vuid, 0 means all chunks")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			ret, err := cli.GetInspectChunkState(common.CmdContext(), host, &blobnode.ChunkInspectArgs{
				DiskID: proto.DiskID(c.Flags.Uint("diskid")),
				Vuid:   proto.Vuid(c.Flags.Uint("vuid")),
			})
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(ret))
			return nil
		},
	})
}
