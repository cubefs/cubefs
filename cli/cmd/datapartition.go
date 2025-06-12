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
		newDataPartitionResetRestoreStatusCmd(client),
		newDataPartitionQueryDiskDecommissionInfoStat(client),
		newDataPartitionQueryDataNodeDecommissionInfoStat(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort                               = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort                      = "Check and list unhealthy data partitions"
	cmdDataPartitionDecommissionShort                      = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionReplicateShort                         = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort                     = "Delete a replication of the data partition on a fixed address"
	cmdDataPartitionGetDiscardShort                        = "Display all discard data partitions"
	cmdDataPartitionSetDiscardShort                        = "Set discard flag for data partition"
	cmdDataPartitionQueryDecommissionProgressShort         = "Query data partition decommission progress"
	cmdDataPartitionResetRestoreStatusShort                = "Reset data partition restore status"
	cmdDataPartitionQueryDiskDecommissionInfoStatShort     = "Query data partition disk decommission info stat"
	cmdDataPartitionQueryDataNodeDecommissionInfoStatShort = "Query data partition datanode decommission info stat"
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
	var showSimplified bool
	var showInactiveNodes bool
	var showNoLeader bool
	var showLack bool
	var showBadDp bool
	var showUnavailable bool
	var showExcess bool
	var showDiskError bool
	var showDiscard bool
	var showDiff bool

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
				diagnosis    *proto.DataPartitionDiagnosis
				discardInfos *proto.DiscardDataPartitionInfos
				dataNodes    []*proto.DataNodeInfo
				err          error
			)
			defer func() {
				errout(err)
			}()

			showAll := !showInactiveNodes && !showNoLeader && !showLack && !showDiscard && !showBadDp &&
				!showDiff && !showUnavailable && !showExcess && !showDiskError && !showSimplified
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(true); err != nil {
				return
			}
			if !showSimplified && (showAll || showInactiveNodes) {
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
			} else {
				stdoutlnf("[Inactive Data nodes count]: %v", len(diagnosis.InactiveDataNodes))
			}

			stdoutln()
			if !showSimplified && (showAll || showNoLeader) {
				stdoutln("[Corrupt data partitions](no leader):")
				stdoutln(partitionInfoTableHeader)
				sort.SliceStable(diagnosis.CorruptDataPartitionIDs, func(i, j int) bool {
					return diagnosis.CorruptDataPartitionIDs[i] < diagnosis.CorruptDataPartitionIDs[j]
				})
				for _, pid := range diagnosis.CorruptDataPartitionIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
						err = fmt.Errorf("Partition not not found[%v], err:[%v] ", pid, err)
						return
					}
					stdoutln(formatDataPartitionInfoRow(partition))
				}
			} else {
				stdoutlnf("[Corrupt data partitions (no leader) count]: %v", len(diagnosis.CorruptDataPartitionIDs))
			}

			stdoutln()
			if !showSimplified && (showAll || showLack) {
				stdoutln("[Partition lack replicas]:")
				stdoutln(partitionInfoTableHeader)
				sort.SliceStable(diagnosis.LackReplicaDataPartitionIDs, func(i, j int) bool {
					return diagnosis.LackReplicaDataPartitionIDs[i] < diagnosis.LackReplicaDataPartitionIDs[j]
				})
				for _, pid := range diagnosis.LackReplicaDataPartitionIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
						err = fmt.Errorf("Partition not found[%v], err:[%v] ", pid, err)
						return
					}
					if partition != nil {
						stdoutln(formatDataPartitionInfoRow(partition))
					}
				}
			} else {
				stdoutlnf("[Partition lack replicas count]: %v", len(diagnosis.LackReplicaDataPartitionIDs))
			}

			stdoutln()
			if !showSimplified && (showAll || showBadDp) {
				typeGroups := make(map[uint32][]struct {
					Path      string
					Partition proto.DpRepairInfo
				})
				for _, bdpv := range diagnosis.BadDataPartitionInfos {
					for _, pinfo := range bdpv.PartitionInfos {
						decommissionType := pinfo.DecommissionType
						typeGroups[decommissionType] = append(typeGroups[decommissionType], struct {
							Path      string
							Partition proto.DpRepairInfo
						}{bdpv.Path, pinfo})
					}
				}
				typeOrder := []uint32{1, 2, 4, 5}
				typeNames := map[uint32]string{
					1: "ManualDecommission",
					2: "AutoDecommission",
					4: "AutoAddReplica",
					5: "ManualAddReplica",
				}
				stdoutln("[Bad data partitions(decommission not completed)]:")
				badPartitionTablePattern := "%-50v    %-10v    %-20v    %-20v"
				// stdoutlnf(badPartitionTablePattern, "PATH", "DP_ID", "REPAIR PROGRESS", "REPAIR STARTTIME")
				for _, dtype := range typeOrder {
					if group, ok := typeGroups[dtype]; ok {
						stdoutln("[" + typeNames[dtype] + "]:")
						stdoutlnf(badPartitionTablePattern, "PATH", "DP_ID", "REPAIR PROGRESS", "REPAIR STARTTIME")

						sort.SliceStable(group, func(i, j int) bool {
							return group[i].Partition.PartitionID < group[j].Partition.PartitionID
						})

						for _, item := range group {
							percent := strconv.FormatFloat(item.Partition.DecommissionRepairProgress*100, 'f', 2, 64) + "%"
							stdoutlnf(badPartitionTablePattern,
								item.Path,
								item.Partition.PartitionID,
								percent,
								item.Partition.RecoverStartTime)
						}
						delete(typeGroups, dtype)
					}
				}

				if len(typeGroups) > 0 {
					stdoutln("\n[Other]:")
					stdoutlnf(badPartitionTablePattern, "PATH", "DP_ID", "REPAIR PROGRESS", "REPAIR STARTTIME")

					var otherGroup []struct {
						Path      string
						Partition proto.DpRepairInfo
					}
					for _, group := range typeGroups {
						otherGroup = append(otherGroup, group...)
					}

					sort.SliceStable(otherGroup, func(i, j int) bool {
						return otherGroup[i].Partition.PartitionID < otherGroup[j].Partition.PartitionID
					})

					for _, item := range otherGroup {
						percent := strconv.FormatFloat(item.Partition.DecommissionRepairProgress*100, 'f', 2, 64) + "%"
						stdoutlnf(badPartitionTablePattern,
							item.Path,
							item.Partition.PartitionID,
							percent,
							item.Partition.RecoverStartTime)
					}
				}
			} else {
				badPatitionCount := 0
				for _, bdpv := range diagnosis.BadDataPartitionInfos {
					badPatitionCount += len(bdpv.PartitionInfos)
				}
				stdoutlnf("[Bad data partitions(decommission not completed) count]: %v", badPatitionCount)
			}

			stdoutln()
			if !showSimplified && (showAll || showUnavailable) {
				stdoutln("[Partition has unavailable replica]:")
				stdoutln(badReplicaPartitionInfoTableHeader)
				sort.SliceStable(diagnosis.BadReplicaDataPartitionIDs, func(i, j int) bool {
					return diagnosis.BadReplicaDataPartitionIDs[i] < diagnosis.BadReplicaDataPartitionIDs[j]
				})

				for _, dpId := range diagnosis.BadReplicaDataPartitionIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", dpId); err != nil {
						err = fmt.Errorf("Partition not found[%v], err:[%v] ", dpId, err)
						return
					}
					if partition != nil {
						stdoutln(formatBadReplicaDpInfoRow(partition))
					}
				}
			} else {
				stdoutlnf("[Partition has unavailable replica count]: %v", len(diagnosis.BadReplicaDataPartitionIDs))
			}

			if !showSimplified && showDiff {
				stdoutln()
				stdoutln("[Partition with replica file count differ significantly]:")
				stdoutln(RepFileCountDifferInfoTableHeader)
				sort.SliceStable(diagnosis.RepFileCountDifferDpIDs, func(i, j int) bool {
					return diagnosis.RepFileCountDifferDpIDs[i] < diagnosis.RepFileCountDifferDpIDs[j]
				})
				for _, dpId := range diagnosis.RepFileCountDifferDpIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", dpId); err != nil {
						err = fmt.Errorf("Partition not found[%v], err:[%v] ", dpId, err)
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
						err = fmt.Errorf("Partition not found[%v], err:[%v] ", dpId, err)
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
			if !showSimplified && (showAll || showExcess) {
				stdoutln("[Partition with excessive replicas]:")
				stdoutln(partitionInfoTableHeader)
				sort.SliceStable(diagnosis.ExcessReplicaDpIDs, func(i, j int) bool {
					return diagnosis.ExcessReplicaDpIDs[i] < diagnosis.ExcessReplicaDpIDs[j]
				})
				for _, pid := range diagnosis.ExcessReplicaDpIDs {
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
						err = fmt.Errorf("Partition not found[%v], err:[%v] ", pid, err)
						return
					}
					if partition != nil {
						stdoutln(formatDataPartitionInfoRow(partition))
					}
				}
			} else {
				stdoutlnf("[Partition with excessive replicas count]: %v", len(diagnosis.ExcessReplicaDpIDs))
			}

			stdoutln()
			if !showSimplified && (showAll || showDiskError) {
				stdoutln("[Partition with disk error replicas]:")
				stdoutln(diskErrorReplicaPartitionInfoTableHeader)
				for pid, infos := range diagnosis.DiskErrorDataPartitionInfos.DiskErrReplicas {
					// DiskErrNotAssociatedWithPartition equals to 0
					if pid == 0 {
						continue
					}
					var partition *proto.DataPartitionInfo
					if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
						err = fmt.Errorf("Partition not found[%v], err:[%v] ", pid, err)
						return
					}
					if partition != nil {
						diskErrorReplicaInfoRow := formatDiskErrorReplicaDpInfoRow(partition, infos)
						if diskErrorReplicaInfoRow != "" {
							stdoutln(diskErrorReplicaInfoRow)
						}
					}
				}
			} else {
				stdoutlnf("[Partition with disk error replicas count]: %v", len(diagnosis.DiskErrorDataPartitionInfos.DiskErrReplicas))
			}

			stdoutln()
			if discardInfos, err = client.AdminAPI().GetDiscardDataPartition(); err != nil {
				return
			}
			if !showSimplified && showDiscard {
				stdoutln("[Discard Partitions]:")
				stdoutln(partitionInfoTableHeader)
				sort.SliceStable(discardInfos.DiscardDps, func(i, j int) bool {
					return discardInfos.DiscardDps[i].PartitionID < discardInfos.DiscardDps[j].PartitionID
				})
				for _, partition := range discardInfos.DiscardDps {
					stdoutln(formatDataPartitionInfoRow(&partition))
				}
			} else {
				stdoutln("[Discard Partitions Count]:", len(discardInfos.DiscardDps))
			}
		},
	}

	cmd.Flags().BoolVarP(&showSimplified, "showSimplified", "s", false, "true for display Simplified version")
	cmd.Flags().BoolVarP(&showInactiveNodes, "showInactiveNodes", "i", false, "true for display inactive dataNodes")
	cmd.Flags().BoolVarP(&showNoLeader, "showNoLeader", "n", false, "true for display no-leader dp")
	cmd.Flags().BoolVarP(&showLack, "showLack", "l", false, "true for display lack replicas dp")
	cmd.Flags().BoolVarP(&showUnavailable, "showUnavailable", "u", false, "true for display dp with unavailable replicas")
	cmd.Flags().BoolVarP(&showBadDp, "showBadDp", "b", false, "true for display bad dp")
	cmd.Flags().BoolVarP(&showExcess, "showExcess", "e", false, "true for display dp with excess replicas")
	cmd.Flags().BoolVarP(&showDiskError, "showDiskError", "E", false, "true for display dp with disk error replicas")
	cmd.Flags().BoolVarP(&showDiscard, "showDiscard", "D", false, "true for display discard dp")
	cmd.Flags().BoolVarP(&showDiff, "showDiff", "d", false, "true for display dp those replica file count count or size differ significantly")
	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var raftForceDel bool
	var weight int
	var decommissionType string
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
			if err := client.AdminAPI().DecommissionDataPartition(partitionID, address, raftForceDel, weight, clientIDKey, decommissionType); err != nil {
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
	cmd.Flags().BoolVarP(&raftForceDel, CliFlagDecommissionRaftForce, "r", false, "true for raftForceDel")
	cmd.Flags().IntVar(&weight, CliFLagDecommissionWeight, lowPriorityDecommissionWeight, "decommission weight")
	cmd.Flags().StringVar(&decommissionType, "decommissionType", "1", "decommission type")
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
	var raftForceDel bool
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
			if err = client.AdminAPI().DeleteDataReplica(partitionID, address, clientIDKey, raftForceDel); err != nil {
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
	cmd.Flags().BoolVarP(&raftForceDel, "raftForceDel", "r", false, "true for raftForceDel")
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
	force := false
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
			if err = client.AdminAPI().SetDataPartitionDiscard(dpId, discard, force); err != nil {
				return
			}
			stdout("Set data partition %v discard to %v successful\n", dpId, discard)
		},
	}
	cmd.Flags().BoolVar(&force, CliFlagForce, false, "force set dp discard")
	return cmd
}

func newDataPartitionQueryDecommissionProgress(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryProgress + " [DATA PARTITION ID]",
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

func newDataPartitionResetRestoreStatusCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpResetRestoreStatus + " [DATA PARTITION ID]",
		Short: cmdDataPartitionResetRestoreStatusShort,
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

			ok, err := client.AdminAPI().ResetDataPartitionRestoreStatus(dpId)
			if err != nil {
				return
			}

			if ok {
				stdout("Reset data partition %v restore status to stop\n", dpId)
				return
			}
			stdout("No need to reset data partition %v restore status\n", dpId)
		},
	}
	return cmd
}

func newDataPartitionQueryDiskDecommissionInfoStat(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryDiskStat,
		Short: cmdDataPartitionQueryDiskDecommissionInfoStatShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error

			defer func() {
				errout(err)
			}()

			infos, err := client.AdminAPI().QueryDataPartitionDiskDecommissionInfoStat()
			if err != nil {
				return
			}

			stdout("%v", formatDataPartitionDecommissionInfoStat(infos))
		},
	}
	return cmd
}

func newDataPartitionQueryDataNodeDecommissionInfoStat(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpQueryNodeStat,
		Short: cmdDataPartitionQueryDataNodeDecommissionInfoStatShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error

			defer func() {
				errout(err)
			}()

			infos, err := client.AdminAPI().QueryDataPartitionDataNodeDecommissionInfoStat()
			if err != nil {
				return
			}

			stdout("%v", formatDataPartitionDecommissionInfoStat(infos))
		},
	}
	return cmd
}
