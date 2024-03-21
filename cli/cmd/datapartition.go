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
	"fmt"
	"sort"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdDataPartitionUse   = "datapartition [COMMAND]"
	cmdDataPartitionShort = "Manage data partition"
)

func newDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdDataPartitionUse,
		Short: cmdDataPartitionShort,
	}
	cmd.AddCommand(
		newDataPartitionGetCmd(client),
		newListCorruptDataPartitionCmd(client),
		newDataPartitionDecommissionCmd(client),
		newDataPartitionReplicateCmd(client),
		newDataPartitionDeleteReplicaCmd(client),
		newDataPartitionGetDiscardCmd(client),
		newDataPartitionSetDiscardCmd(client),
		newDataPartitionQueryDecommissionProgress(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort                       = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort              = "Check and list unhealthy data partitions"
	cmdDataPartitionDecommissionShort              = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionReplicateShort                 = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort             = "Delete a replication of the data partition on a fixed address"
	cmdDataPartitionGetDiscardShort                = "Display all discard data partitions"
	cmdDataPartitionSetDiscardShort                = "Set discard flag for data partition"
	cmdDataPartitionQueryDecommissionProgressShort = "Query data partition decommission progress"
)

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [DATA PARTITION ID]",
		Short: cmdDataPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
				partition   *proto.DataPartitionInfo
			)
			defer func() {
				errout(err)
			}()
			if partitionID, err = strconv.ParseUint(args[0], 10, 64); err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				return
			}
			stdoutf("%v", formatDataPartitionInfo(partition))
		},
	}
	return cmd
}

func newListCorruptDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var ignoreDiscardDp bool
	var diff bool

	cmd := &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptDataPartitionShort,
		Long: `If the data nodes are marked as "Inactive", it means the nodes has been not available for a time. It is suggested to
eliminate the network, disk or other problems first. Once the bad nodes can never be "active", they are called corrupt
nodes. The "decommission" command can be used to discard the corrupt nodes. However, if more than half replicas of
a partition are on the corrupt nodes, the few remaining replicas can not reach an agreement with one leader. In this case,
you can use the "reset" command to fix the problem.The "reset" command may lead to data loss, be careful to do this.
The "reset" command will be released in next version`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				diagnosis *proto.DataPartitionDiagnosis
				dataNodes []*proto.DataNodeInfo
				err       error
			)
			defer func() {
				errout(err)
			}()
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(ignoreDiscardDp); err != nil {
				return
			}
			stdoutln("[Inactive Data nodes]:")
			stdoutlnf("%v", formatDataNodeDetailTableHeader())
			for _, addr := range diagnosis.InactiveDataNodes {
				var node *proto.DataNodeInfo
				if node, err = client.NodeAPI().GetDataNode(addr); err != nil {
					return
				}
				dataNodes = append(dataNodes, node)
			}
			sort.SliceStable(dataNodes, func(i, j int) bool {
				return dataNodes[i].ID < dataNodes[j].ID
			})
			for _, node := range dataNodes {
				stdoutln(formatDataNodeDetail(node, true))
			}
			stdoutln()
			stdoutln("[Corrupt data partitions](no leader):")
			stdoutln(partitionInfoTableHeader)
			sort.SliceStable(diagnosis.CorruptDataPartitionIDs, func(i, j int) bool {
				return diagnosis.CorruptDataPartitionIDs[i] < diagnosis.CorruptDataPartitionIDs[j]
			})
			for _, pid := range diagnosis.CorruptDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
				}
				stdoutln(formatDataPartitionInfoRow(partition))
			}

			stdoutln()
			stdoutln("[Partition lack replicas]:")
			stdoutln(partitionInfoTableHeader)
			sort.SliceStable(diagnosis.LackReplicaDataPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaDataPartitionIDs[i] < diagnosis.LackReplicaDataPartitionIDs[j]
			})
			for _, pid := range diagnosis.LackReplicaDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
				}
				if partition != nil {
					stdoutln(formatDataPartitionInfoRow(partition))
				}
			}

			stdoutln()
			stdoutln("[Bad data partitions(decommission not completed)]:")
			badPartitionTablePattern := "%-8v    %-10v    %-10v"
			stdoutlnf(badPartitionTablePattern, "PATH", "PARTITION ID", "REPAIR PROGRESS")
			for _, bdpv := range diagnosis.BadDataPartitionInfos {
				sort.SliceStable(bdpv.PartitionInfos, func(i, j int) bool {
					return bdpv.PartitionInfos[i].PartitionID < bdpv.PartitionInfos[j].PartitionID
				})
				for _, pinfo := range bdpv.PartitionInfos {
					percent := strconv.FormatFloat(pinfo.DecommissionRepairProgress*100, 'f', 2, 64) + "%"
					stdoutlnf(badPartitionTablePattern, bdpv.Path, pinfo.PartitionID, percent)
				}
			}

			stdoutln()
			stdoutln("[Partition has unavailable replica]:")
			stdoutln(badReplicaPartitionInfoTableHeader)
			sort.SliceStable(diagnosis.BadReplicaDataPartitionIDs, func(i, j int) bool {
				return diagnosis.BadReplicaDataPartitionIDs[i] < diagnosis.BadReplicaDataPartitionIDs[j]
			})

			for _, dpId := range diagnosis.BadReplicaDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", dpId); err != nil {
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
				}
				if partition != nil {
					stdoutln(formatBadReplicaDpInfoRow(partition))
				}
			}

			if diff {
				stdoutln()
				stdoutln("[Partition with replica file count differ significantly]:")
				stdoutln(RepFileCountDifferInfoTableHeader)
				sort.SliceStable(diagnosis.RepFileCountDifferDpIDs, func(i, j int) bool {
					return diagnosis.RepFileCountDifferDpIDs[i] < diagnosis.RepFileCountDifferDpIDs[j]
				})
				for _, dpId := range diagnosis.RepFileCountDifferDpIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", dpId); err != nil {
						err = fmt.Errorf("Partition not found, err:[%v] ", err)
						return
					}
					if partition != nil {
						stdoutln(formatReplicaFileCountDiffDpInfoRow(partition))
					}
				}

				stdoutln()
				stdoutln("[Partition with replica used size differ significantly]:")
				stdoutln(RepUsedSizeDifferInfoTableHeader)
				sort.SliceStable(diagnosis.RepUsedSizeDifferDpIDs, func(i, j int) bool {
					return diagnosis.RepUsedSizeDifferDpIDs[i] < diagnosis.RepUsedSizeDifferDpIDs[j]
				})
				for _, dpId := range diagnosis.RepUsedSizeDifferDpIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", dpId); err != nil {
						err = fmt.Errorf("Partition not found, err:[%v] ", err)
						return
					}
					if partition != nil {
						stdoutln(formatReplicaSizeDiffDpInfoRow(partition))
					}
				}
			} else {
				stdoutln()
				stdoutlnf("%v %v", "[Number of Partition with replica file count differ significantly]:",
					len(diagnosis.RepUsedSizeDifferDpIDs))

				stdoutln()
				stdoutlnf("%v %v", "[Number of Partition with replica used size differ significantly]:",
					len(diagnosis.RepUsedSizeDifferDpIDs))
			}

			stdoutln()
			stdoutln("[Partition with excessive replicas]:")
			stdoutln(partitionInfoTableHeader)
			sort.SliceStable(diagnosis.ExcessReplicaDpIDs, func(i, j int) bool {
				return diagnosis.ExcessReplicaDpIDs[i] < diagnosis.ExcessReplicaDpIDs[j]
			})
			for _, pid := range diagnosis.ExcessReplicaDpIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
				}
				if partition != nil {
					stdoutln(formatDataPartitionInfoRow(partition))
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&ignoreDiscardDp, "ignoreDiscard", "i", false, "true for not display discard dp")
	cmd.Flags().BoolVarP(&diff, "diff", "d", false, "true for display dp those replica file count count or size differ significantly")
	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var raftForceDel bool
	var clientIDKey string
	cmd := &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
			)
			defer func() {
				errout(err)
			}()
			address := args[0]
			partitionID, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			if err := client.AdminAPI().DecommissionDataPartition(partitionID, address, raftForceDel, clientIDKey); err != nil {
				stdout(fmt.Sprintf("failed:err(%v)\n", err.Error()))
				return
			}
			stdoutln("Decommission data partition successfully")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&raftForceDel, "raftForceDel", "r", false, "true for raftForceDel")
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newDataPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	cmd := &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
			)
			defer func() {
				errout(err)
			}()
			address := args[0]
			if partitionID, err = strconv.ParseUint(args[1], 10, 64); err != nil {
				return
			}
			if err = client.AdminAPI().AddDataReplica(partitionID, address, clientIDKey); err != nil {
				return
			}
			stdoutln("Add replication successfully")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newDataPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	cmd := &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
			)
			defer func() {
				errout(err)
			}()
			address := args[0]
			if partitionID, err = strconv.ParseUint(args[1], 10, 64); err != nil {
				return
			}
			if err = client.AdminAPI().DeleteDataReplica(partitionID, address, clientIDKey); err != nil {
				return
			}
			stdoutln("Delete replication successfully")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newDataPartitionGetDiscardCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpGetDiscard,
		Short: cmdDataPartitionGetDiscardShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				infos *proto.DiscardDataPartitionInfos
				err   error
			)

			defer func() {
				errout(err)
			}()

			if infos, err = client.AdminAPI().GetDiscardDataPartition(); err != nil {
				return
			}

			stdoutln()
			stdoutln("[Discard Partitions]:")
			stdoutln(partitionInfoTableHeader)
			sort.SliceStable(infos.DiscardDps, func(i, j int) bool {
				return infos.DiscardDps[i].PartitionID < infos.DiscardDps[j].PartitionID
			})
			for _, partition := range infos.DiscardDps {
				stdoutln(formatDataPartitionInfoRow(&partition))
			}
		},
	}
	return cmd
}

func newDataPartitionSetDiscardCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpSetDiscard + " [DATA PARTITION ID] [DISCARD]",
		Short: cmdDataPartitionSetDiscardShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err     error
				dpId    uint64
				discard bool
			)

			defer func() {
				errout(err)
			}()

			dpId, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			discard, err = strconv.ParseBool(args[1])
			if err != nil {
				return
			}
			if err = client.AdminAPI().SetDataPartitionDiscard(dpId, discard); err != nil {
				return
			}
			stdout("Discard %v successful", dpId)
		},
	}
	return cmd
}

func newDataPartitionQueryDecommissionProgress(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryProgress + "[DATA PARTITION ID]",
		Short: cmdDataPartitionQueryDecommissionProgressShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err  error
				dpId uint64
			)

			defer func() {
				errout(err)
			}()

			dpId, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}

			info, err := client.AdminAPI().QueryDataPartitionDecommissionStatus(dpId)
			if err != nil {
				return
			}

			stdout("%v", formatDataPartitionDecommissionProgress(info))
		},
	}
	return cmd
}
