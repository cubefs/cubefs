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
	"time"

	"github.com/desertbit/grumble"
	"github.com/dustin/go-humanize"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/util/tablefmt"
)

func cmdGetService(c *grumble.Context) error {
	ctx := common.CmdContext()
	cli := newCMClient(c.Flags)

	var services clustermgr.ServiceInfo
	var err error
	if name := c.Args.String("name"); name != "" {
		services, err = cli.GetService(ctx, clustermgr.GetServiceArgs{Name: name})
	} else {
		services, err = cli.ListService(ctx)
	}
	if err != nil {
		return err
	}

	const timeformat = "2006-01-02 15:04:05"

	rows := tablefmt.Table{tablefmt.NewRow("ClusterID", "Name", "IDC", "Host", "Expired", "ExpireAt")}
	for _, node := range services.Nodes {
		expireTime := ""
		if node.ExpireAt > 0 {
			t := time.Unix(node.ExpireAt, 0)
			expireTime = fmt.Sprintf("%s (%s)", t.Format(timeformat), humanize.Time(t))
		}
		rows = rows.Append(tablefmt.NewRow(node.ClusterID, node.Name, node.Idc, node.Host,
			node.ExpireAt > 0, expireTime))
	}
	fmt.Println(tablefmt.AlignWith([]tablefmt.Alignment{tablefmt.AlignCenter}, rows...))
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
		Flags: clusterFlags,
	}
	cmd.AddCommand(serviceCommand)
}
