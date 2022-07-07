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

package clustermgr

import (
	"fmt"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func cmdGetService(c *grumble.Context) error {
	cli, ctx := newCMClient(c.Flags.String("secret"), specificHosts(c.Flags)...), common.CmdContext()

	names := []string{
		proto.ServiceNameProxy,
		proto.ServiceNameBlobNode,
	}
	name := c.Args.String("name")
	if name != "" {
		names = []string{name}
	}

	for _, name := range names {
		fmt.Println("Service of", name, ":")
		info, err := cli.GetService(ctx, clustermgr.GetServiceArgs{
			Name: name,
		})
		if err != nil {
			return err
		}
		fmt.Println(common.Readable(info))
		fmt.Println()
	}

	return nil
}

func addCmdService(cmd *grumble.Command) {
	serviceCommand := &grumble.Command{
		Name:     "service",
		Help:     "service tools",
		LongHelp: "service tools for clustermgr [name]",
		Run:      cmdGetService,
		Args: func(a *grumble.Args) {
			a.String("name", "service name", grumble.Default(""))
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
	}
	cmd.AddCommand(serviceCommand)
}
