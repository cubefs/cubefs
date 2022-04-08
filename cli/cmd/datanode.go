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
	"sort"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDataNodeShort = "Manage data nodes"
	cmdDataNodeMigrateInfoShort = "Migrate partitions from a data node to the other node"
	dpMigrateMax = 50
)

func newDataNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliResourceDataNode,
		Short: cmdDataNodeShort,
	}
	cmd.AddCommand(
		newDataNodeListCmd(client),
		newDataNodeInfoCmd(client),
		newDataNodeDecommissionCmd(client),
		newDataNodeMigrateCmd(client),
	)
	return cmd
}

const (
	cmdDataNodeListShort             = "List information of data nodes"
	cmdDataNodeInfoShort             = "Show information of a data node"
	cmdDataNodeDecommissionInfoShort = "decommission partitions in a data node to others"
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
					errout("Error: %v", err)
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

func newDataNodeInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [{HOST}:{PORT}]",
		Short: cmdDataNodeInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var datanodeInfo *proto.DataNodeInfo
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			nodeAddr = args[0]
			if datanodeInfo, err = client.NodeAPI().GetDataNode(nodeAddr); err != nil {
				return
			}
			stdout("[Data node info]\n")
			stdout(formatDataNodeDetail(datanodeInfo, false))

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataNodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var optCount int
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [{HOST}:{PORT}]",
		Short: cmdDataNodeDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			nodeAddr = args[0]
			if optCount < 0 {
				stdout("Migrate dp count should >= 0\n")
				return
			}
			if err = client.NodeAPI().DataNodeDecommission(nodeAddr, optCount); err != nil {
				return
			}
			stdout("Decommission data node successfully\n")

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, "DataNode delete mp count")
	return cmd
}

func newDataNodeMigrateCmd(client *master.MasterClient) *cobra.Command {
	var optCount int
	var cmd = &cobra.Command{
		Use:   CliOpMigrate + " src[{HOST}:{PORT}] dst[{HOST}:{PORT}]",
		Short: cmdDataNodeMigrateInfoShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var src, dst string
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			src = args[0]
			dst = args[1]
			if optCount > dpMigrateMax || optCount <= 0{
				stdout("Migrate dp count should between [1-50]\n")
				return
			}

			if err = client.NodeAPI().DataNodeMigrate(src, dst, optCount); err != nil {
				return
			}
			stdout("Migrate data node successfully\n")

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, dpMigrateMax, "Migrate dp count,default 15")
	return cmd
}
