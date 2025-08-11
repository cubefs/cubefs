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

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDataNodeShort              = "Manage data nodes"
	cmdDataNodeMigrateInfoShort   = "Migrate partitions from a data node to the other node"
	dpMigrateMax                  = 50
	lowPriorityDecommissionWeight = 2
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
		newDataNodeQueryDecommissionProgress(client),
		newDataNodeQueryDecommissionedDisk(client),
		newDataNodeQueryDecommissionSuccessDisk(client),
		newDataNodeCancelDecommissionCmd(client),
		// newDataNodeDiskOpCmd(client),
		// newDataNodeDpOpCmd(client),
	)
	return cmd
}

const (
	cmdDataNodeListShort                          = "List information of data nodes"
	cmdDataNodeInfoShort                          = "Show information of a data node"
	cmdDataNodeDecommissionInfoShort              = "decommission partitions in a data node to others"
	cmdDataNodeQueryDecommissionedDisksShort      = "query datanode decommissioned disks"
	cmdDataNodeQueryDecommissionSuccessDisksShort = "query datanode decommissionSuccess disks"
	cmdDataNodeCancelDecommissionedDisksShort     = "cancel decommission progress for datanode"
	cmdDataNodeQueryDecommissionProgress          = "query datanode decommission progress"
	// cmdDataNodeDiskOpShort                    = "Show Disk_op information of a data node"
	// cmdDataNodeDpOpShort                      = "Show Dp_op information of a data node"
)

func newDataNodeListCmd(client *master.MasterClient) *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	cmd := &cobra.Command{
		Use:     CliOpList,
		Short:   cmdDataNodeListShort,
		Aliases: []string{"ls"},
		RunE: func(cmd *cobra.Command, args []string) error {
			dataNodes, err := client.AdminAPI().GetClusterDataNodes()
			if err != nil {
				return err
			}
			sort.SliceStable(dataNodes, func(i, j int) bool {
				return dataNodes[i].ID < dataNodes[j].ID
			})
			stdoutln("[Data nodes]")
			stdoutln(formatNodeViewTableHeader())
			for _, node := range dataNodes {
				if optFilterStatus != "" &&
					!strings.Contains(formatNodeStatus(node.Status), optFilterStatus) {
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
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive]")
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
		optCount     int
		clientIDKey  string
		raftForceDel bool
		weight       int
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
			if err := client.NodeAPI().DataNodeDecommission(args[0], optCount, clientIDKey, raftForceDel, weight); err != nil {
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
	cmd.Flags().BoolVarP(&raftForceDel, CliFlagDecommissionRaftForce, "r", false, "true for raftForceDel")
	cmd.Flags().IntVar(&weight, CliFLagDecommissionWeight, lowPriorityDecommissionWeight, "decommission weight")
	return cmd
}

func newDataNodeMigrateCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var optCount int
	var weight int
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

			if err := client.NodeAPI().DataNodeMigrate(src, dst, optCount, weight, clientIDKey); err != nil {
				return err
			}
			stdoutln("Migrate data node successfully")
			return nil
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().IntVar(&optCount, CliFlagCount, dpMigrateMax, fmt.Sprintf("Migrate dp count,default %v", dpMigrateMax))
	cmd.Flags().IntVar(&weight, CliFLagDecommissionWeight, lowPriorityDecommissionWeight, "decommission weight")
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newDataNodeQueryDecommissionProgress(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryProgress + " [{HOST}:{PORT}]",
		Short: cmdDataNodeQueryDecommissionProgress,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			progress, err := client.NodeAPI().QueryDataNodeDecommissionProgress(args[0])
			if err != nil {
				stdout("%v", err)
				return err
			}
			stdout("%v", formatDataNodeDecommissionProgress(progress))
			return nil
		},
	}
	return cmd
}

func newDataNodeQueryDecommissionedDisk(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryDecommissionedDisk + " [{HOST}:{PORT}]",
		Short: cmdDataNodeQueryDecommissionedDisksShort,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			disks, err := client.NodeAPI().QueryDecommissionedDisks(args[0])
			if err != nil {
				stdout("%v", err)
				return err
			}
			stdoutln("[Decommissioned disks]")
			for _, disk := range disks.Disks {
				stdout("%v\n", disk)
			}
			return nil
		},
	}
	return cmd
}

func newDataNodeQueryDecommissionSuccessDisk(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryDecommissionSuccessDisk + " [{HOST}:{PORT}]",
		Short: cmdDataNodeQueryDecommissionSuccessDisksShort,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			disks, err := client.NodeAPI().QueryDecommissionSuccessDisks(args[0])
			if err != nil {
				stdout("%v", err)
				return err
			}
			stdoutln("[DecommissionSuccess disks]")
			for _, disk := range disks.Disks {
				stdout("%v\n", disk)
			}
			return nil
		},
	}
	return cmd
}

func newDataNodeCancelDecommissionCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpCancelDecommission + " [{HOST}:{PORT}]",
		Short: cmdDataNodeCancelDecommissionedDisksShort,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := client.NodeAPI().QueryCancelDecommissionedDataNode(args[0])
			if err != nil {
				stdout("%v, please exec curl -v http://masterAddr:17010/dataNode/queryDecommissionProgress?addr=dataAddr:17310 to check if the datanode has been canceled", err)
				return err
			}
			stdoutln(fmt.Sprintf("Cancel decommission for %v success", args[0]))
			return nil
		},
	}
	return cmd
}

// func newDataNodeDiskOpCmd(client *master.MasterClient) *cobra.Command {
// 	var filterOp string
// 	var diskName string
// 	var logNum int
// 	cmd := &cobra.Command{
// 		Use:   CliOpDiskOp + " [{HOST}:{PORT}]",
// 		Short: cmdDataNodeDiskOpShort,
// 		Args:  cobra.MinimumNArgs(1),
// 		RunE: func(cmd *cobra.Command, args []string) error {
// 			datanodeInfo, err := client.NodeAPI().GetDataNode(args[0])
// 			if err != nil {
// 				return err
// 			}
// 			stdoutln(fmt.Sprintf("%-30v %-20v %v", "DiskName", "OpType", "Count"))
// 			stdoutln(formatDataNodeDiskOp(datanodeInfo, logNum, diskName, filterOp))
// 			return nil
// 		},
// 		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
// 			if len(args) != 0 {
// 				return nil, cobra.ShellCompDirectiveNoFileComp
// 			}
// 			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
// 		},
// 	}
// 	cmd.Flags().IntVar(&logNum, "num", 50, "Number of logs to display")
// 	cmd.Flags().StringVar(&diskName, "disk", "", "Filter logs by disk name")
// 	cmd.Flags().StringVar(&filterOp, "filter-op", "", "Filter operations by type")
// 	return cmd
// }

// func newDataNodeDpOpCmd(client *master.MasterClient) *cobra.Command {
// 	var filterOp string
// 	var dpId string
// 	var logNum int
// 	cmd := &cobra.Command{
// 		Use:   CliOpDpOp + " [{HOST}:{PORT}]",
// 		Short: cmdDataNodeDpOpShort,
// 		Args:  cobra.MinimumNArgs(1),
// 		RunE: func(cmd *cobra.Command, args []string) error {
// 			datanodeInfo, err := client.NodeAPI().GetDataNode(args[0])
// 			if err != nil {
// 				return err
// 			}
// 			stdoutln(fmt.Sprintf("%-30v %-20v %v", "DpId", "OpType", "Count"))
// 			stdoutln(formatDataNodeDpOp(datanodeInfo, logNum, dpId, filterOp))
// 			return nil
// 		},
// 		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
// 			if len(args) != 0 {
// 				return nil, cobra.ShellCompDirectiveNoFileComp
// 			}
// 			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
// 		},
// 	}
// 	cmd.Flags().IntVar(&logNum, "num", 50, "Number of logs to display")
// 	cmd.Flags().StringVar(&dpId, "dp", "", "Filter logs by dp id")
// 	cmd.Flags().StringVar(&filterOp, "filter-op", "", "Filter operations by type")
// 	return cmd
// }
