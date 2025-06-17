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
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

func addCmdVol(cmd *grumble.Command) {
	volumeCommand := &grumble.Command{
		Name:     "volume",
		Help:     "volume tools",
		LongHelp: "volume tools for shardNode",
	}
	cmd.AddCommand(volumeCommand)

	volumeCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list all volume shardNode allocated",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.CodeModeRegister(a)
		},
		Run: cmdListVolume,
	})
}

func cmdListVolume(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	mode := args.CodeMode(c.Args)

	cli := shardnode.New(rpc2.Client{})
	value, err := cli.ListVolume(ctx, host, shardnode.ListVolumeArgs{
		CodeMode: mode,
	})
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(value))
	return nil
}
