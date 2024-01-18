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
	"sort"
	"strings"

	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDataNodeShort            = "Manage data nodes"
	cmdDataNodeMigrateInfoShort = "Migrate partitions from a data node to the other node"
	dpMigrateMax                = 50
)

func newDataNodeCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
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
	cmd := &cobra.Command{
		Use:     CliOpList,
		Short:   cmdDataNodeListShort,
		Aliases: []string{"ls"},
		RunE: func(cmd *cobra.Command, args []string) error {
			view, err := client.AdminAPI().GetCluster()
			if err != nil {
				return err
			}
			sort.SliceStable(view.DataNodes, func(i, j int) bool {
				return view.DataNodes[i].ID < view.DataNodes[j].ID
			})
			stdoutln("[Data nodes]")
			stdoutln(formatNodeViewTableHeader())
			for _, node := range view.DataNodes {
				if optFilterStatus != "" &&
					!strings.Contains(formatNodeStatus(node.IsActive), optFilterStatus) {
					continue
				}
				if optFilterWritable != "" &&
					!strings.Contains(formatYesNo(node.IsWritable), optFilterWritable) {
					continue
				}
				stdoutln(formatNodeView(&node, true))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive")
	return cmd
}

func newDataNodeInfoCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [{HOST}:{PORT}]",
		Short: cmdDataNodeInfoShort,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			datanodeInfo, err := client.NodeAPI().GetDataNode(args[0])
			if err != nil {
				return err
			}
			stdoutln("[Data node info]")
			stdoutln(formatDataNodeDetail(datanodeInfo, false))
			return nil
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
	var (
		optCount    int
		clientIDKey string
	)
	cmd := &cobra.Command{
		Use:   CliOpDecommission + " [{HOST}:{PORT}]",
		Short: cmdDataNodeDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if optCount < 0 {
				stdoutln("Migrate dp count should >= 0")
				return nil
			}
			if err := client.NodeAPI().DataNodeDecommission(args[0], optCount, clientIDKey); err != nil {
				return err
			}
			stdoutln("Decommission data node successfully")
			return nil
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, 0, "DataNode delete mp count")
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newDataNodeMigrateCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var optCount int
	cmd := &cobra.Command{
		Use:   CliOpMigrate + " src[{HOST}:{PORT}] dst[{HOST}:{PORT}]",
		Short: cmdDataNodeMigrateInfoShort,
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			src, dst := args[0], args[1]
			if optCount > dpMigrateMax || optCount <= 0 {
				stdoutln("Migrate dp count should between [1-50]")
				return nil
			}

			if err := client.NodeAPI().DataNodeMigrate(src, dst, optCount, clientIDKey); err != nil {
				return err
			}
			stdoutln("Migrate data node successfully")
			return nil
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, dpMigrateMax, "Migrate dp count,default 15")
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}
