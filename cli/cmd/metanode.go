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
	cmdMetaNodeUse   = "metanode [COMMAND]"
	cmdMetaNodeShort = "Manage meta nodes"
	mpMigrateMax = 15
)

func newMetaNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdMetaNodeUse,
		Short: cmdMetaNodeShort,
	}
	cmd.AddCommand(
		newMetaNodeListCmd(client),
		newMetaNodeInfoCmd(client),
		newMetaNodeDecommissionCmd(client),
		newMetaNodeMigrateCmd(client),
	)
	return cmd
}

const (
	cmdMetaNodeListShort             = "List information of meta nodes"
	cmdMetaNodeInfoShort             = "Show information of meta nodes"
	cmdMetaNodeDecommissionInfoShort = "Decommission partitions in a meta node to other nodes"
	cmdMetaNodeMigrateInfoShort      = "Migrate partitions from a meta node to the other node"
)

func newMetaNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdMetaNodeListShort,
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
			sort.SliceStable(view.MetaNodes, func(i, j int) bool {
				return view.MetaNodes[i].ID < view.MetaNodes[j].ID
			})
			stdout("[Meta nodes]\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.MetaNodes {
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
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter status [Active, Inactive")
	return cmd
}

func newMetaNodeInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [{HOST}:{PORT}]",
		Short: cmdMetaNodeInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var metanodeInfo *proto.MetaNodeInfo
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			nodeAddr = args[0]
			if metanodeInfo, err = client.NodeAPI().GetMetaNode(nodeAddr); err != nil {
				return
			}
			stdout("[Meta node info]\n")
			stdout(formatMetaNodeDetail(metanodeInfo, false))

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}
func newMetaNodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var optCount int
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [{HOST}:{PORT}]",
		Short: cmdMetaNodeDecommissionInfoShort,
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
				stdout("Migrate mp count should >= 0\n")
				return
			}
			if err = client.NodeAPI().MetaNodeDecommission(nodeAddr, optCount); err != nil {
				return
			}
			stdout("Decommission meta node successfully\n")

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, "MetaNode delete mp count")
	return cmd
}
func newMetaNodeMigrateCmd(client *master.MasterClient) *cobra.Command {
	var optCount int
	var cmd = &cobra.Command{
		Use:   CliOpMigrate + " src[{HOST}:{PORT}] dst[{HOST}:{PORT}]",
		Short: cmdMetaNodeMigrateInfoShort,
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
			if optCount > mpMigrateMax || optCount <= 0{
				stdout("Migrate mp count should between [1-15]\n")
				return
			}
			if err = client.NodeAPI().MetaNodeMigrate(src, dst, optCount); err != nil {
				return
			}
			stdout("Migrate meta node successfully\n")

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, mpMigrateMax, "Migrate mp count")
	return cmd
}
