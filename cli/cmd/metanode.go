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
	"fmt"
	"sort"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdMetaNodeUse   = "metanode [COMMAND]"
	cmdMetaNodeShort = "Manage meta nodes"
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
		newResetMetaNodeCmd(client),
	)
	return cmd
}

const (
	cmdMetaNodeListShort             = "List information of meta nodes"
	cmdMetaNodeInfoShort             = "Show information of meta nodes"
	cmdMetaNodeDecommissionInfoShort = "Decommission partitions in a meta node to other nodes"
	cmdResetMetaNodeShort            = "Reset corrupt meta partitions related to this node"
)

func newMetaNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var optShowDp bool
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdMetaNodeListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err           error
				nodeInfoSlice []*proto.MetaNodeInfo
				info          *proto.MetaNodeInfo
				header        string
				row           string
			)
			defer func() {
				if err != nil {
					errout("List cluster meta nodes failed: %v\n", err)
				}
			}()
			var view *proto.ClusterView
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			sort.SliceStable(view.MetaNodes, func(i, j int) bool {
				return view.MetaNodes[i].ID < view.MetaNodes[j].ID
			})
			if optShowDp {
				nodeInfoSlice = make([]*proto.MetaNodeInfo, len(view.MetaNodes))
				for index, node := range view.MetaNodes {
					if info, err = client.NodeAPI().GetMetaNode(node.Addr); err != nil {
						return
					}
					nodeInfoSlice[index] = info
				}
			}
			stdout("[Meta nodes]\n")
			if optShowDp {
				header = formatMetaNodeViewTableHeader()
			} else {
				header = formatNodeViewTableHeader()
			}
			stdout("%v\n", header)
			for index, node := range view.MetaNodes {
				if optFilterStatus != "" &&
					!strings.Contains(formatNodeStatus(node.Status), optFilterStatus) {
					continue
				}
				if optFilterWritable != "" &&
					!strings.Contains(formatYesNo(node.IsWritable), optFilterWritable) {
					continue
				}
				if optShowDp {
					info = nodeInfoSlice[index]
					row = fmt.Sprintf(metaNodeDetailViewTableRowPattern, node.ID, node.Addr, node.Version,
						formatYesNo(node.IsWritable), formatNodeStatus(node.Status), formatSize(info.Used), formatFloat(info.Ratio), info.ZoneName, info.MetaPartitionCount)
				} else {
					row = formatNodeView(&node, true)
				}
				stdout("%v\n", row)
			}
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter status [Active, Inactive")
	cmd.Flags().BoolVarP(&optShowDp, "detail", "d", false, "Show detail information")
	return cmd
}

func newMetaNodeInfoCmd(client *master.MasterClient) *cobra.Command {
	type dentryCount struct {
		partitionID uint64
		dentryCount uint64
	}
	var (
		optDentry       bool
		dentryCountList []*dentryCount
		mp              *proto.MetaPartitionInfo
	)
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [NODE ADDRESS]",
		Short: cmdMetaNodeInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			var metanodeInfo *proto.MetaNodeInfo
			defer func() {
				if err != nil {
					errout("Show meta node info failed: %v\n", err)
				}
			}()
			nodeAddr = args[0]
			if metanodeInfo, err = client.NodeAPI().GetMetaNode(nodeAddr); err != nil {
				return
			}
			stdout("[Meta node info]\n")
			stdout(formatMetaNodeDetail(metanodeInfo, false))
			if !optDentry {
				return
			}
			for _, mpId := range metanodeInfo.PersistenceMetaPartitions {
				if mp, err = client.ClientAPI().GetMetaPartition(mpId, ""); err != nil {
					continue
				}
				dentryCountList = append(dentryCountList, &dentryCount{partitionID: mp.PartitionID, dentryCount: mp.DentryCount})
			}
			sort.Slice(dentryCountList, func(i, j int) bool {
				return dentryCountList[i].dentryCount > dentryCountList[j].dentryCount
			})
			var sb = strings.Builder{}
			for i := range dentryCountList {
				sb.WriteString(fmt.Sprintf("%d:%d ", dentryCountList[i].partitionID, dentryCountList[i].dentryCount))
			}
			stdout("  Dentry Counts       : [%v]\n", sb.String())
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&optDentry, "dentry", "d", false, "Display meta partition dentry count")
	return cmd
}
func newMetaNodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [NODE ADDRESS]",
		Short: cmdMetaNodeDecommissionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var nodeAddr string
			defer func() {
				if err != nil {
					errout("decommission meta node failed: %v\n", err)
				}
			}()
			nodeAddr = args[0]
			if err = client.NodeAPI().MetaNodeDecommission(nodeAddr); err != nil {
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
	return cmd
}

func newResetMetaNodeCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [ADDRESS]",
		Short: cmdResetMetaNodeShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
not reach an agreement with one leader. In this case, you can use the "reset" command to fix the problem. This command
is used to reset all the corrupt partitions related to a chosen corrupt node. However this action may lead to data 
loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				address string
				confirm string
				err     error
			)
			defer func() {
				if err != nil {
					errout("Error:%v", err)
					OsExitWithLogFlush()
				}
			}()
			address = args[0]
			stdout(fmt.Sprintf("The action may risk the danger of losing data, please confirm(y/n):"))
			_, _ = fmt.Scanln(&confirm)
			if "y" != confirm && "yes" != confirm {
				return
			}
			if err = client.AdminAPI().ResetCorruptMetaNode(address); err != nil {
				return
			}
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
