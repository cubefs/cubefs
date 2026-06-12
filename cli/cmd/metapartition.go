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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdMetaPartitionUse   = "metapartition [COMMAND]"
	cmdMetaPartitionShort = "Manage meta partition"
)

func newMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdMetaPartitionUse,
		Short: cmdMetaPartitionShort,
	}
	cmd.AddCommand(
		newMetaPartitionGetCmd(client),
		newListCorruptMetaPartitionCmd(client),
		newMetaPartitionDecommissionCmd(client),
		newMetaPartitionReplicateCmd(client),
		newMetaPartitionDeleteReplicaCmd(client),
		newMetaPartitionAddLearnerCmd(client),
		newMetaPartitionPromoteLearnerCmd(client),
		newMetaPartitionUpdateRegionCmd(client),
	)
	return cmd
}

const (
	cmdMetaPartitionGetShort            = "Display detail information of a meta partition"
	cmdCheckCorruptMetaPartitionShort   = "Check out corrupt meta partitions"
	cmdMetaPartitionDecommissionShort   = "Decommission a replication of the meta partition to a new address"
	cmdMetaPartitionReplicateShort      = "Add a replication of the meta partition on a new address"
	cmdMetaPartitionDeleteReplicaShort  = "Delete a replication of the meta partition on a fixed address"
	cmdMetaPartitionAddLearnerShort     = "Add a learner replica of the meta partition on a new address"
	cmdMetaPartitionPromoteLearnerShort = "Promote a learner replica to voter in the meta partition"
	cmdMetaPartitionUpdateRegionShort   = "Update meta partition region (region must be in the volume's allowed regions)"
)

func newMetaPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [META PARTITION ID]",
		Short: cmdMetaPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
				partition   *proto.MetaPartitionInfo
				pools       []*proto.StoragePoolInfo
				poolNameMap map[uint8]string
			)
			defer func() {
				errout(err)
			}()
			if partitionID, err = strconv.ParseUint(args[0], 10, 64); err != nil {
				return
			}
			if partition, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				return
			}
			// Get pool name map for better display
			if pools, err = client.AdminAPI().ListStoragePools(); err == nil {
				poolNameMap = make(map[uint8]string)
				for _, pool := range pools {
					poolNameMap[pool.Id] = pool.Name
				}
			} else {
				poolNameMap = make(map[uint8]string)
			}
			stdout("%v\n", formatMetaPartitionInfoWithPoolNames(partition, poolNameMap))
		},
	}
	return cmd
}

func newMetaPartitionUpdateRegionCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	cmd := &cobra.Command{
		Use:   "updateRegion [META_PARTITION_ID] [REGION]",
		Short: cmdMetaPartitionUpdateRegionShort,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			region := args[1]
			var mpInfo *proto.MetaPartitionInfo
			if mpInfo, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				return
			}
			volName := mpInfo.VolName
			var vv *proto.SimpleVolView
			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volName); err != nil {
				return
			}
			allowed := false
			for _, r := range vv.AllowedRegions {
				if r == region {
					allowed = true
					break
				}
			}
			if !allowed {
				err = fmt.Errorf("region %q is not in volume %q allowed regions %v", region, volName, vv.AllowedRegions)
				return
			}
			if err = client.AdminAPI().UpdateMetaPartitionRegion(partitionID, region, clientIDKey); err != nil {
				return
			}
			stdout("Update meta partition region successfully.\n")
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}

func newListCorruptMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	var printManual bool
	var printLeaseDetail bool
	cmd := &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptMetaPartitionShort,
		Long: `If the meta nodes are marked as "Inactive", it means the nodes has been not available for a long time. It is suggested to eliminate
the network, disk or other problems first. If the bad nodes can never be "active" again, they are called corrupt nodes. And the
"decommission" command can be used to discard the corrupt nodes. However, if more than half replicas of a partition are on
the corrupt nodes, the few remaining replicas can not reach an agreement with one leader. In this case, you can use the
"metapartition reset" command to fix the problem, however this action may lead to data loss, be careful to do this. The
"reset" command will be released in next version.`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				diagnosis *proto.MetaPartitionDiagnosisV1
				metaNodes []*proto.MetaNodeInfo
				err       error
			)
			if diagnosis, err = client.AdminAPI().DiagnoseMetaPartition(); err != nil {
				return
			}
			stdout("[Inactive Meta nodes]:\n")
			stdout("%v\n", formatMetaNodeDetailTableHeader())
			sort.SliceStable(diagnosis.InactiveMetaNodes, func(i, j int) bool {
				return diagnosis.InactiveMetaNodes[i] < diagnosis.InactiveMetaNodes[j]
			})
			for _, addr := range diagnosis.InactiveMetaNodes {
				var node *proto.MetaNodeInfo
				if node, err = client.NodeAPI().GetMetaNode(addr); err != nil {
					continue
				}
				metaNodes = append(metaNodes, node)
			}
			sort.SliceStable(metaNodes, func(i, j int) bool {
				return metaNodes[i].ID < metaNodes[j].ID
			})
			for _, node := range metaNodes {
				stdout("%v\n", formatMetaNodeDetail(node, true))
			}

			stdout("\n")
			stdout("[Corrupt meta partitions](no leader):\n")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.NoLeaderMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.NoLeaderMetaPartitionIDs[i] < diagnosis.NoLeaderMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.NoLeaderMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				stdout("%v\n", formatMetaPartitionInfoRow(partition))
			}

			stdout("\n")
			stdout("%v\n", "[Meta partition lack replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.LackReplicaMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaMetaPartitionIDs[i] < diagnosis.LackReplicaMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.LackReplicaMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Meta partition Abnormal Raft Info]:")
			stdout("%v\n", PeerAbnormalRaftPartitionInfoHeader)
			sort.SliceStable(diagnosis.AbnormalRaftIDs, func(i, j int) bool {
				return diagnosis.AbnormalRaftIDs[i] < diagnosis.AbnormalRaftIDs[j]
			})
			for _, pid := range diagnosis.AbnormalRaftIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionInfoRowWithRaft(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Meta partition learner flag mismatch]:")
			stdout("%v\n", partitionLearnerMismatchTableHeader)
			for _, pid := range diagnosis.LearnerFlagMismatchIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					if row := formatMetaPartitionLearnerMismatchRow(partition); row != "" {
						stdout("%v\n", row)
					}
				}
			}

			stdout("\n")
			// Display RecoverPairs
			if len(diagnosis.RecoverPairs) > 0 {
				stdout("%v\n", "[Meta Partition Recovery Pairs]:")
				recoverPairTablePattern := "%-12v    %-20v    %-20v    %-20v    %-12v    %-20v    %-10v    %-10v\n"
				stdout(recoverPairTablePattern, "PARTITION_ID", "RECOVER_SRC", "RECOVER_DST", "RECOVER_START", "RETRY_CNT", "RETRY_TIME", "STATE", "DECOMM_TYPE")

				sort.SliceStable(diagnosis.RecoverPairs, func(i, j int) bool {
					return diagnosis.RecoverPairs[i].RecoverPair.RecoverStart < diagnosis.RecoverPairs[j].RecoverPair.RecoverStart
				})

				for _, rpw := range diagnosis.RecoverPairs {
					rp := rpw.RecoverPair
					recoverStartTime := "N/A"
					if rp.RecoverStart > 0 {
						recoverStartTime = time.Unix(rp.RecoverStart, 0).Format("2006-01-02 15:04:05")
					}
					retryTime := "N/A"
					if rp.RecoverRetryTime > 0 {
						retryTime = time.Unix(rp.RecoverRetryTime, 0).Format("2006-01-02 15:04:05")
					}
					stateStr := rp.RecoverState.String()
					decommTypeStr := proto.FormatDecommissionType(rp.DecommissionType)
					stdout(recoverPairTablePattern,
						rpw.PartitionID,
						rp.RecoverSrc,
						rp.RecoverDst,
						recoverStartTime,
						rp.RecoverRetryCnt,
						retryTime,
						stateStr,
						decommTypeStr)
				}
			}

			stdout("\n")
			// Display LearnerRecoverPairs
			if len(diagnosis.LearnerRecoverPairs) > 0 {
				stdout("%v\n", "[Meta Partition Learner Recovery Pairs]:")
				learnerRecoverPairTablePattern := "%-12v    %-20v    %-20v    %-20v    %-12v    %-20v    %-10v    %-10v\n"
				stdout(learnerRecoverPairTablePattern, "PARTITION_ID", "RECOVER_SRC", "RECOVER_DST", "RECOVER_START", "RETRY_CNT", "RETRY_TIME", "STATE", "DECOMM_TYPE")

				sort.SliceStable(diagnosis.LearnerRecoverPairs, func(i, j int) bool {
					return diagnosis.LearnerRecoverPairs[i].RecoverPair.RecoverStart < diagnosis.LearnerRecoverPairs[j].RecoverPair.RecoverStart
				})

				for _, rpw := range diagnosis.LearnerRecoverPairs {
					rp := rpw.RecoverPair
					recoverStartTime := "N/A"
					if rp.RecoverStart > 0 {
						recoverStartTime = time.Unix(rp.RecoverStart, 0).Format("2006-01-02 15:04:05")
					}
					retryTime := "N/A"
					if rp.RecoverRetryTime > 0 {
						retryTime = time.Unix(rp.RecoverRetryTime, 0).Format("2006-01-02 15:04:05")
					}
					stateStr := rp.RecoverState.String()
					decommTypeStr := proto.FormatDecommissionType(rp.DecommissionType)
					stdout(learnerRecoverPairTablePattern,
						rpw.PartitionID,
						rp.RecoverSrc,
						rp.RecoverDst,
						recoverStartTime,
						rp.RecoverRetryCnt,
						retryTime,
						stateStr,
						decommTypeStr)
				}
			}

			stdout("\n")
			stdout("%v\n", "[Meta Partition has unavailable replica]:")
			stdout("%v\n", badMpReplicaPartitionInfoTableHeader)
			sort.SliceStable(diagnosis.UnavailableMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.UnavailableMetaPartitionIDs[i] < diagnosis.UnavailableMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.UnavailableMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					badReplicaMpInfoRow := formatBadReplicaMpInfoRow(partition)
					if "" != badReplicaMpInfoRow {
						stdout("%v\n", badReplicaMpInfoRow)
					}
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with replica inode count not equal]:")
			stdout("%v\n", inodeCountNotEqualInfoTableHeader)
			sort.SliceStable(diagnosis.InodeCountNotEqualIDs, func(i, j int) bool {
				return diagnosis.InodeCountNotEqualIDs[i] < diagnosis.InodeCountNotEqualIDs[j]
			})
			for _, pid := range diagnosis.InodeCountNotEqualIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionReplicaInodeNotEqualInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with replica max inode not equal]:")
			stdout("%v\n", maxInodeNotEqualInfoTableHeader)
			sort.SliceStable(diagnosis.MaxInodeNotEqualIDs, func(i, j int) bool {
				return diagnosis.MaxInodeNotEqualIDs[i] < diagnosis.MaxInodeNotEqualIDs[j]
			})
			for _, pid := range diagnosis.MaxInodeNotEqualIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionReplicaInodeNotEqualInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with replica dentry count not equal]:")
			stdout("%v\n", dentryCountNotEqualInfoTableHeader)
			sort.SliceStable(diagnosis.DentryCountNotEqualIDs, func(i, j int) bool {
				return diagnosis.DentryCountNotEqualIDs[i] < diagnosis.DentryCountNotEqualIDs[j]
			})
			for _, pid := range diagnosis.DentryCountNotEqualIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionReplicaDentryNotEqualInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with excessive replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.ExcessiveReplicaMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.ExcessiveReplicaMetaPartitionIDs[i] < diagnosis.ExcessiveReplicaMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.ExcessiveReplicaMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with failed recovery]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.FailedRecoveryMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.FailedRecoveryMetaPartitionIDs[i] < diagnosis.FailedRecoveryMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.FailedRecoveryMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with auto learner]:")
			stdout("%v\n", partitionLearnerTableHeader)
			sort.SliceStable(diagnosis.AutoLearnerMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.AutoLearnerMetaPartitionIDs[i] < diagnosis.AutoLearnerMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.AutoLearnerMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					continue
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionLearnerInfoRow(partition, false))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition with lease time exceeded threshold]:")
			stdout("count: %v\n", len(diagnosis.LeaseTimeExceededReplicas))
			if printLeaseDetail {
				stdout("%v\n", formatLeaseTimeExceededTableHeader())
				sort.SliceStable(diagnosis.LeaseTimeExceededReplicas, func(i, j int) bool {
					a, b := diagnosis.LeaseTimeExceededReplicas[i], diagnosis.LeaseTimeExceededReplicas[j]
					if a.VolName != b.VolName {
						return a.VolName < b.VolName
					}
					if a.PartitionID != b.PartitionID {
						return a.PartitionID < b.PartitionID
					}
					return a.ReplicaAddr < b.ReplicaAddr
				})
				for _, replica := range diagnosis.LeaseTimeExceededReplicas {
					stdout("%v\n", formatLeaseTimeExceededRow(replica))
				}
			}

			if printManual {
				stdout("\n")
				stdout("%v\n", "[Partition with manual learner]:")
				stdout("%v\n", partitionLearnerTableHeader)
				sort.SliceStable(diagnosis.ManualLearnerMetaPartitionIDs, func(i, j int) bool {
					return diagnosis.ManualLearnerMetaPartitionIDs[i] < diagnosis.ManualLearnerMetaPartitionIDs[j]
				})
				for _, pid := range diagnosis.ManualLearnerMetaPartitionIDs {
					var partition *proto.MetaPartitionInfo
					if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
						continue
					}
					if partition != nil {
						stdout("%v\n", formatMetaPartitionLearnerInfoRow(partition, true))
					}
				}
			}
		},
	}
	cmd.Flags().BoolVar(&printManual, "manual", false, "print manual learner partitions")
	cmd.Flags().BoolVarP(&printLeaseDetail, "lease-detail", "l", false, "print lease time exceeded replica details")
	return cmd
}

func newMetaPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var optStoreMode string
	cmd := &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionDecommissionShort,
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
			var storeMode proto.StoreMode
			storeMode, err = ParseStoreMode(optStoreMode)
			if err != nil {
				return
			}
			if err = client.AdminAPI().DecommissionMetaPartition(partitionID, address, clientIDKey, storeMode); err != nil {
				return
			}
			stdout("Decommission meta partition successfully\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	cmd.Flags().StringVar(&optStoreMode, CliFlagStoreMode, "memory", "specify volume default store mode: memory, rocksdb")
	return cmd
}

func newMetaPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var optStoreMode string
	cmd := &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionReplicateShort,
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

			var storeMode proto.StoreMode
			storeMode, err = ParseStoreMode(optStoreMode)
			if err != nil {
				return
			}
			if err = client.AdminAPI().AddMetaReplica(partitionID, address, clientIDKey, storeMode); err != nil {
				return
			}
			stdout("Add replication successfully\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	cmd.Flags().StringVar(&optStoreMode, CliFlagStoreMode, "memory", "specify volume default store mode: memory, rocksdb")
	return cmd
}

func newMetaPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var raftForceDel bool
	cmd := &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionDeleteReplicaShort,
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
			if err = client.AdminAPI().DeleteMetaReplica(partitionID, address, clientIDKey, raftForceDel); err != nil {
				return
			}
			stdout("Delete replication successfully\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	cmd.Flags().BoolVarP(&raftForceDel, CliFlagDecommissionRaftForce, "r", false, "true for raftForceDel")
	return cmd
}

func newMetaPartitionAddLearnerCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	var optStoreMode string
	var manualPromote bool
	cmd := &cobra.Command{
		Use:   CliOpAddLearner + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionAddLearnerShort,
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

			var storeMode proto.StoreMode
			storeMode, err = ParseStoreMode(optStoreMode)
			if err != nil {
				return
			}
			if err = client.AdminAPI().AddMetaPartitionLearner(partitionID, address, clientIDKey, storeMode, manualPromote); err != nil {
				return
			}
			stdout("Add learner replica successfully\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	cmd.Flags().StringVar(&optStoreMode, CliFlagStoreMode, "memory", "specify volume default store mode: memory, rocksdb")
	cmd.Flags().BoolVar(&manualPromote, "manualPromote", false, "if true, the learner can't be promoted or deleted automatically")
	return cmd
}

func newMetaPartitionPromoteLearnerCmd(client *master.MasterClient) *cobra.Command {
	var clientIDKey string
	cmd := &cobra.Command{
		Use:   CliOpPromoteLearner + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionPromoteLearnerShort,
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
			if err = client.AdminAPI().PromoteMetaReplica(partitionID, address, clientIDKey); err != nil {
				return
			}
			stdout("Promote learner replica to voter successfully\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&clientIDKey, CliFlagClientIDKey, client.ClientIDKey(), CliUsageClientIDKey)
	return cmd
}
