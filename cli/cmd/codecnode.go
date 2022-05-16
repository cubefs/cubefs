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
	cmdCodecnodeShort = "Manage codecnode nodes"
)

func newCodEcnodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliResourceCodecnodeNode,
		Short: cmdCodecnodeShort,
	}
	cmd.AddCommand(
		newCodEcnodeListCmd(client),
		newCodEcnodeDecommissionCmd(client),
	)
	return cmd
}

const (
	cmdCodecnodeListShort             = "List information of codecnode nodes"
	cmdCodecnodeDecommissionInfoShort = "decommission a codecnode node"
)

func newCodEcnodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdCodecnodeListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("List cluster codecnode nodes failed: %v\n", err)
					os.Exit(1)
				}
			}()
			var view *proto.ClusterView
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			sort.SliceStable(view.CodEcnodes, func(i, j int) bool {
				return view.CodEcnodes[i].ID < view.CodEcnodes[j].ID
			})
			stdout("[CodEcnodes]\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.CodEcnodes {
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

func newCodEcnodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [NODE ADDRESS]",
		Short: cmdCodecnodeDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			defer func() {
				if err != nil {
					errout("decommission codecnode node failed, err[%v]\n", err)
					os.Exit(1)
				}
			}()
			nodeAddr = args[0]
			if err = client.NodeAPI().CodEcNodeDecommission(nodeAddr); err != nil {
				return
			}
			stdout("Decommission codecnode node successfully\n")

		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validCodEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func validCodEcNodes(client *master.MasterClient, toComplete string) []string {
	var (
		validCodEcNodes []string
		clusterView     *proto.ClusterView

		err error
	)
	if clusterView, err = client.AdminAPI().GetCluster(); err != nil {
		errout("Get ec node list failed:\n%v\n", err)
	}
	for _, en := range clusterView.CodEcnodes {
		validCodEcNodes = append(validCodEcNodes, en.Addr)
	}
	return validCodEcNodes
}
