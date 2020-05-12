// Copyright 2018 The Chubao Authors.
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
	"os"
	"sort"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDataNodeShort = "Manage data nodes"
)

func newDataNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliResourceDataNode,
		Short: cmdDataNodeShort,
	}
	cmd.AddCommand(
		newDataNodeListCmd(client),
	)
	return cmd
}

const (
	cmdDataNodeListShort = "List information of meta nodes"
)

func newDataNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdDataNodeListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("List cluster meta nodes failed: %v\n", err)
					os.Exit(1)
				}
			}()
			var view *proto.ClusterView
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			sort.SliceStable(view.DataNodes, func(i, j int) bool {
				return view.DataNodes[i].ID < view.DataNodes[j].ID
			})
			stdout("[Data nodes]\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.DataNodes {
				if optFilterStatus != "" &&
					!strings.Contains(formatNodeStatus(node.Status), optFilterStatus) {
					continue
				}
				if optFilterWritable != "" &&
					!strings.Contains(formatYesNo(node.IsWritable), optFilterWritable) {
					continue
				}
				stdout("%v\n", formatNodeView(&node, true))
			}
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive")
	return cmd
}
