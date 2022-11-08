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

func addCmdDisk(cmd *grumble.Command) {
	diskCommand := &grumble.Command{
		Name:     "disk",
		Help:     "disk tools",
		LongHelp: "disk tools for blobnode",
	}
	cmd.AddCommand(diskCommand)

	diskCommand.AddCommand(&grumble.Command{
		Name: "all",
		Help: "show all register disks",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			stat, err := cli.Stat(common.CmdContext(), host)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})

	diskCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "show disk info",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.DiskStatArgs{DiskID: proto.DiskID(c.Flags.Uint("diskid"))}
			stat, err := cli.DiskInfo(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})
}
