// Copyright 2018 The CubeFS Authors.
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
	cmdEcNodeShort = "Manage ec nodes"
)

func newEcNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliResourceEcNode,
		Short: cmdEcNodeShort,
	}
	cmd.AddCommand(
		newEcNodeListCmd(client),
		newEcNodeInfoCmd(client),
		newEcNodeDecommissionCmd(client),
		newEcNodeDiskDecommissionCmd(client),
		newEcNodeGetTaskStatus(client),
	)
	return cmd
}

const (
	cmdEcNodeListShort                 = "List information of ec nodes"
	cmdEcNodeInfoShort                 = "Show information of a ec node"
	cmdEcNodeDecommissionInfoShort     = "decommission partitions in a ec node to others"
	cmdEcNodeDiskDecommissionInfoShort = "decommission disk of partitions in a data node to others"
	cmdEcNodeGetTaskStatus             = "get all ec migrate Task detail"
)

func newEcNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdEcNodeListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("List cluster ec nodes failed: %v\n", err)
					os.Exit(1)
				}
			}()
			var view *proto.ClusterView
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			sort.SliceStable(view.EcNodes, func(i, j int) bool {
				return view.EcNodes[i].ID < view.EcNodes[j].ID
			})
			stdout("[EC nodes]\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.EcNodes {
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

func newEcNodeInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [NODE ADDRESS]",
		Short: cmdEcNodeInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var ecnodeInfo *proto.EcNodeInfo
			defer func() {
				if err != nil {
					errout("Show ec node info failed: %v\n", err)
					os.Exit(1)
				}
			}()
			nodeAddr = args[0]
			if ecnodeInfo, err = client.NodeAPI().GetEcNode(nodeAddr); err != nil {
				return
			}
			stdout("[EC node info]\n")
			stdout(formatEcNodeDetail(ecnodeInfo, false))

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newEcNodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [NODE ADDRESS]",
		Short: cmdEcNodeDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			defer func() {
				if err != nil {
					errout("decommission ec node failed, err[%v]\n", err)
					os.Exit(1)
				}
			}()
			nodeAddr = args[0]

			data, err := client.NodeAPI().EcNodeDecommission(nodeAddr)
			if err != nil {
				stdout("%v", err)
			}else {
				stdout("%v", string(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newEcNodeDiskDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommissionDisk + " [NODE ADDRESS]" + "[DISK PATH]",
		Short: cmdEcNodeDiskDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var diskAddr string
			defer func() {
				if err != nil {
					errout("decommission disk failed, err[%v]\n", err)
				}
			}()
			nodeAddr = args[0]
			diskAddr = args[1]
			data, err := client.NodeAPI().EcNodeDiskDecommission(nodeAddr, diskAddr)
			if err != nil {
				stdout("%v", err)
			}else {
				stdout("%v", string(data))
			}
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

func newEcNodeGetTaskStatus(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetTaskStatus,
		Short: cmdEcNodeGetTaskStatus,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err      error
				taskView = make([]*proto.MigrateTaskView, 0)
			)
			if taskView, err = client.NodeAPI().EcNodeGetTaskStatus(); err != nil {
				stdout("get EcNode task error:%v\n", err.Error())
				return
			}
			for _, task := range taskView {
				stdout("%+v\n", task)
			}
		},
	}
	return cmd
}
