// Copyright 2023 The CubeFS Authors.
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

package cmd

import (
	"sort"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDiskUse   = "disk [COMMAND]"
	cmdDiskShort = "Manage cluster disks"
)

func newDiskCmd(client master.IMasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdDiskUse,
		Short:   cmdDiskShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"disk"},
	}
	cmd.AddCommand(
		newListBadDiskCmd(client),
	)
	return cmd
}

const (
	cmdCheckBadDisksShort = "Check and list unhealthy disks"
)

func newListBadDiskCmd(client master.IMasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckBadDisksShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.BadDiskInfos
				err   error
			)
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			if infos, err = client.AdminAPI().QueryBadDisks(); err != nil {
				return
			}
			stdout("[Unavaliable disks]:\n")
			stdout("%v\n", formatBadDiskTableHeader())

			sort.SliceStable(infos.BadDisks, func(i, j int) bool {
				return infos.BadDisks[i].Address < infos.BadDisks[i].Address
			})
			for _, disk := range infos.BadDisks {
				stdout("%v\n", formatBadDiskInfoRow(disk))
			}
		},
	}
	return cmd
}
