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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
)

const (
	cmdDataPartitionUse   = "datapartition [COMMAND]"
	cmdDataPartitionShort = "Manage data partition"
	defaultNodeTimeOutSec = 180
)

func newDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdDataPartitionUse,
		Short: cmdDataPartitionShort,
	}
	cmd.AddCommand(
		newDataPartitionGetCmd(client),
		newListCorruptDataPartitionCmd(client),
		newResetDataPartitionCmd(client),
		newDataPartitionDecommissionCmd(client),
		newDataPartitionReplicateCmd(client),
		newDataPartitionDeleteReplicaCmd(client),
		newDataPartitionAddLearnerCmd(client),
		newDataPartitionPromoteLearnerCmd(client),
		newDataPartitionCheckCommitCmd(client),
		newDataPartitionFreezeCmd(client),
		newDataPartitionUnfreezeCmd(client),
		newDataPartitionTransferCmd(client),
		newGetCanEcMigrateCmd(client),
		newGetCanEcDelCmd(client),
		newDelDpAlreadyEc(client),
		newMigrateEc(client),
		newStopMigratingByDataPartition(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort            = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort   = "Check and list unhealthy data partitions"
	cmdCheckCommitDataPartitionShort    = "Check the snapshot blocking by analyze commit id in data partitions"
	cmdResetDataPartitionShort          = "Reset corrupt data partition"
	cmdDataPartitionDecommissionShort   = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionReplicateShort      = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort  = "Delete a replication of the data partition on a fixed address"
	cmdDataPartitionAddLearnerShort     = "Add a learner of the data partition on a new address"
	cmdDataPartitionPromoteLearnerShort = "Promote the learner of the data partition on a fixed address"
	cmdDataPartitionFreezeShort          = "Freezes the DP and does not provide the write service. It is used only for smart Volumes"
	cmdDataPartitionUnFreezeLearnerShort = "Unfreeze the DP to provide write services. It is used only for smart Volumes"
	cmdGetCanEcMigrateShort             = "Display these partitions's detail information of can ec migrate"
	cmdGetCanEcDelShort                 = "Display these partitions's detail information of already finish ec"
	cmdDelDpAlreadyEc                   = "delete the datapartition of already finish ec migration"
	cmdMigrateEc                        = "start ec migration to using ecnode store data"
	cmdStopMigratingEcByDataPartition   = "stop migrating task by data partition"
)

func newDataPartitionTransferCmd(client *master.MasterClient) *cobra.Command {
	var (
		partitionId uint64
		address     string
		destAddress string
		err         error
		result      string
	)

	var cmd = &cobra.Command{
		Use:   CliOpTransfer + " [DATA PARTITION ID ADDRESS  DEST ADDRESS]",
		Short: "",
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			defer func() {
				if err != nil {
					errout("transfer data partition failed:%v\n", err.Error())
				}
			}()

			partitionId, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			address = args[1]
			destAddress = args[2]
			result, err = client.AdminAPI().DataPartitionTransfer(partitionId, address, destAddress)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	return cmd
}

func newDataPartitionUnfreezeCmd(client *master.MasterClient) *cobra.Command {
	var (
		volName     string
		partitionId uint64
		err         error
		result      string
	)
	var cmd = &cobra.Command{
		Use:   CliOpUnfreeze + " [VolName PARTITION ID]",
		Short: cmdDataPartitionUnFreezeLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				errout("%v", "both volName and partitionId must be present")
			}
			defer func() {
				if err != nil {
					errout("unfreeze data partition failed:%v\n", err.Error())
				}
			}()
			volName = args[0]
			partitionId, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			result, err = client.AdminAPI().UnfreezeDataPartition(volName, partitionId)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	return cmd
}

func newDataPartitionFreezeCmd(client *master.MasterClient) *cobra.Command {
	var (
		volName     string
		partitionId uint64
		err         error
		result      string
	)
	var cmd = &cobra.Command{
		Use:   CliOpFreeze + " [VolName PARTITION ID]",
		Short: cmdDataPartitionFreezeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				errout("%v", "both volName and partitionId must be present")
			}
			defer func() {
				if err != nil {
					errout("freeze data partition failed:%v\n", err.Error())
				}
			}()
			volName = args[0]
			partitionId, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			result, err = client.AdminAPI().FreezeDataPartition(volName, partitionId)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	return cmd
}

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var optRaft bool
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [DATA PARTITION ID]",
		Short: cmdDataPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.DataPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				return
			}
			stdout(formatDataPartitionInfo(partition))
			if optRaft {
				stdout("\n")
				stdout("RaftInfo :\n")
				stdout(fmt.Sprintf("%v\n", dataPartitionRaftTableHeaderInfo))
				for _, p := range partition.Peers {
					var dnPartition *proto.DNDataPartitionInfo
					addr := strings.Split(p.Addr, ":")[0]
					//check dataPartition by dataNode api
					for i := 0; i < 3; i++ {
						if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, partitionID); err == nil {
							break
						}
						time.Sleep(1 * time.Second)
					}
					if err != nil {
						continue
					}
					if dnPartition.RaftStatus == nil {
						stdout(fmt.Sprintf("%v   raft is stopped\n", p.ID))
						continue
					}
					stdout(fmt.Sprintf("%v\n", formatDataPartitionRaftTableInfo(dnPartition.RaftStatus)))
				}
			}
		},
	}
	cmd.Flags().BoolVar(&optRaft, CliFlagRaft, false, "show raft peer detail info")
	return cmd
}

func newListCorruptDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var optEnableAutoFullfill bool
	var optCheckAll bool
	var optDiffSizeThreshold int
	var optSpecifyDP uint64
	var cmd = &cobra.Command{
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
			if optSpecifyDP > 0 {
				outPut, isHealthy, _ := checkDataPartition("", optSpecifyDP, client, optDiffSizeThreshold)
				if !isHealthy {
					fmt.Printf(outPut)
				} else {
					fmt.Printf("partition is healthy")
				}
				return
			}
			if optCheckAll {
				err = checkAllDataPartitions(client, optDiffSizeThreshold)
				if err != nil {
					errout("%v\n", err)
				}
				return
			}
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("[Inactive Data nodes]:\n")
			stdout("%v\n", formatDataNodeDetailTableHeader())
			for _, addr := range diagnosis.InactiveDataNodes {
				var node *proto.DataNodeInfo
				node, err = client.NodeAPI().GetDataNode(addr)
				dataNodes = append(dataNodes, node)
			}
			sort.SliceStable(dataNodes, func(i, j int) bool {
				return dataNodes[i].ID < dataNodes[j].ID
			})
			for _, node := range dataNodes {
				stdout("%v\n", formatDataNodeDetail(node, true))
			}
			/*stdout("\n")
			stdout("[Corrupt data partitions](no leader):\n")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.CorruptDataPartitionIDs, func(i, j int) bool {
				return diagnosis.CorruptDataPartitionIDs[i] < diagnosis.CorruptDataPartitionIDs[j]
			})
			for _, pid := range diagnosis.CorruptDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				stdout("%v\n", formatDataPartitionInfoRow(partition))
			}*/

			stdout("\n")
			stdout("%v\n", "[Partition lack replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.LackReplicaDataPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaDataPartitionIDs[i] < diagnosis.LackReplicaDataPartitionIDs[j]
			})
			cv, _ := client.AdminAPI().GetCluster()
			dns := cv.DataNodes
			var sb = strings.Builder{}

			for _, pid := range diagnosis.LackReplicaDataPartitionIDs {
				var (
					partition     *proto.DataPartitionInfo
					leaderRps     map[uint64]*proto.ReplicaStatus
					canAutoRepair bool
					peerStrings   []string
				)
				canAutoRepair = true
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil || partition == nil {
					stdout("get partition error, err:[%v]", err)
					return
				}
				stdout("%v", formatDataPartitionInfoRow(partition))
				sort.Strings(partition.Hosts)
				if len(partition.MissingNodes) > 0 || partition.Status == -1 {
					stdoutRed(fmt.Sprintf("partition not ready to repair"))
					continue
				}
				for i, r := range partition.Replicas {
					var rps map[uint64]*proto.ReplicaStatus
					var dnPartition *proto.DNDataPartitionInfo
					var err error
					addr := strings.Split(r.Addr, ":")[0]
					if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, partition.PartitionID); err != nil {
						fmt.Printf(partitionInfoColorTablePattern+"\n",
							"", "", "", fmt.Sprintf("%v(hosts)", r.Addr), fmt.Sprintf("%v/%v", "nil", partition.ReplicaNum), "get partition info failed")
						continue
					}
					sort.Strings(dnPartition.Replicas)
					fmt.Printf(partitionInfoColorTablePattern+"\n",
						"", "", "", fmt.Sprintf("%v(hosts)", r.Addr), fmt.Sprintf("%v/%v", len(dnPartition.Replicas), partition.ReplicaNum), strings.Join(dnPartition.Replicas, "; "))

					if rps = dnPartition.RaftStatus.Replicas; rps != nil {
						leaderRps = rps
					}
					peers := convertPeersToArray(dnPartition.Peers)
					sort.Strings(peers)
					if i == 0 {
						peerStrings = peers
					} else {
						if !isEqualStrings(peers, peerStrings) {
							canAutoRepair = false
						}
					}
					fmt.Printf(partitionInfoColorTablePattern+"\n",
						"", "", "", fmt.Sprintf("%v(peers)", r.Addr), fmt.Sprintf("%v/%v", len(peers), partition.ReplicaNum), strings.Join(peers, ","))
				}
				if len(leaderRps) != 3 || len(partition.Hosts) != 2 {
					stdoutRed(fmt.Sprintf("raft peer number(expected is 3, but is %v) or replica number(expected is 2, but is %v) not match ", len(leaderRps), len(partition.Hosts)))
					continue
				}
				var lackAddr []string
				for _, dn := range dns {
					if _, ok := leaderRps[dn.ID]; ok {
						if !contains(partition.Hosts, dn.Addr) {
							lackAddr = append(lackAddr, dn.Addr)
						}
					}
				}
				if len(lackAddr) != 1 {
					stdoutRed(fmt.Sprintf("Not classic partition, please check and repair it manually"))
					continue
				}
				stdoutGreen(fmt.Sprintf(" The Lack Address is: %v", lackAddr))
				if canAutoRepair {
					sb.WriteString(fmt.Sprintf("cfs-cli datapartition add-replica %v %v\n", lackAddr[0], partition.PartitionID))
				}
				if optEnableAutoFullfill && canAutoRepair {
					stdoutGreen("     Auto Repair Begin:")
					if err = client.AdminAPI().AddDataReplica(partition.PartitionID, lackAddr[0], 0); err != nil {
						stdoutRed(fmt.Sprintf("%v err:%v", "     Failed.", err))
						continue
					}
					stdoutGreen("     Done.")
					time.Sleep(2 * time.Second)
				}
				stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
			}
			if !optEnableAutoFullfill {
				stdout(sb.String())
			}
			return
		},
	}
	cmd.Flags().Uint64Var(&optSpecifyDP, CliFlagId, 0, "check data partition by partitionID")
	cmd.Flags().IntVar(&optDiffSizeThreshold, CliFlagThreshold, 20, "if the diff size larger than this, report the volume")
	cmd.Flags().BoolVar(&optEnableAutoFullfill, CliFlagEnableAutoFill, false, "true - automatically full fill the missing replica")
	cmd.Flags().BoolVar(&optCheckAll, "all", false, "true - check all partitions; false - only check partitions which lack of replica")
	return cmd
}
func checkAllDataPartitions(client *master.MasterClient, optDiffSizeThreshold int) (err error) {
	var (
		volInfo          []*proto.VolInfo
		sizeNotEqualPids []uint64
		noLeaderPids     []uint64
	)
	if volInfo, err = client.AdminAPI().ListVols(""); err != nil {
		stdout("%v\n", err)
		return
	}
	stdout("\n")
	stdout("%v\n", "[Partition peer info not valid]:")
	stdout("%v\n", partitionInfoTableHeader)
	for _, vol := range volInfo {
		var (
			volView *proto.VolView
			volLock sync.Mutex
			wg      sync.WaitGroup
		)
		if volView, err = client.ClientAPI().GetVolume(vol.Name, calcAuthKey(vol.Owner)); err != nil {
			stdout("Found an invalid vol: %v\n", vol.Name)
			continue
		}
		/*		sort.SliceStable(volView.DataPartitions, func(i, j int) bool {
				return volView.DataPartitions[i].PartitionID < volView.DataPartitions[j].PartitionID
			})*/
		dpCh := make(chan bool, 20)
		for _, dp := range volView.DataPartitions {
			wg.Add(1)
			dpCh <- true
			go func(dp *proto.DataPartitionResponse) {
				defer func() {
					wg.Done()
					<-dpCh
				}()
				var outPut string
				var isHealthy bool
				outPut, isHealthy, _ = checkDataPartition(vol.Name, dp.PartitionID, client, optDiffSizeThreshold)
				if !isHealthy {
					volLock.Lock()
					if outPut == UsedSizeNotEqualErr {
						sizeNotEqualPids = append(sizeNotEqualPids, dp.PartitionID)
					} else if outPut == RaftNoLeader {
						noLeaderPids = append(noLeaderPids, dp.PartitionID)
					} else {
						fmt.Printf(outPut)
						//stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
						fmt.Printf(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
					}
					volLock.Unlock()
				}
			}(dp)
		}
		wg.Wait()
	}
	if len(noLeaderPids) > 0 {
		fmt.Printf("raft leader status get failed dps[%v]: %v\n", len(noLeaderPids), noLeaderPids)
	}
	if len(sizeNotEqualPids) > 0 {
		fmt.Printf("used size diff larger than %v percent not equal dps[%v]: %v\n", optDiffSizeThreshold, len(sizeNotEqualPids), sizeNotEqualPids)
	}
	return
}
func checkDataPartition(volName string, pid uint64, client *master.MasterClient, optDiffSizeThreshold int) (outPut string, isHealthy bool, err error) {
	var (
		partition    *proto.DataPartitionInfo
		errorReports []string
		leaderStatus *proto.Status
		sb           = strings.Builder{}
	)
	defer func() {
		isHealthy = true
		if len(errorReports) > 0 {
			isHealthy = false
			//mark \033[1;40;31m%-8v\033[0m\n
			if len(errorReports) == 1 && errorReports[0] == UsedSizeNotEqualErr {
				outPut = errorReports[0]
				return
			}
			if len(errorReports) == 1 && errorReports[0] == RaftNoLeader {
				outPut = errorReports[0]
				return
			}
			if len(errorReports) == 2 && errorReports[0] == UsedSizeNotEqualErr && errorReports[1] == RaftNoLeader {
				outPut = errorReports[0]
				return
			}
			for i, msg := range errorReports {
				sb.WriteString(fmt.Sprintf("%-8v\n", fmt.Sprintf("error %v: %v", i+1, msg)))
			}
		}
		outPut = sb.String()
	}()
	if partition, err = client.AdminAPI().GetDataPartition(volName, pid); err != nil || partition == nil {
		errorReports = append(errorReports, fmt.Sprintf("get partition error, err:[%v]", err))
		return
	}
	sb.WriteString(fmt.Sprintf("%v", formatDataPartitionInfoRow(partition)))
	sort.Strings(partition.Hosts)
	if len(partition.MissingNodes) > 0 || partition.Status == -1 || len(partition.Hosts) != int(partition.ReplicaNum) {
		errorReports = append(errorReports, PartitionNotHealthyInMaster)
	}
	if !checkUsedSizeDiff(partition.Replicas, optDiffSizeThreshold, partition.PartitionID) {
		errorReports = append(errorReports, UsedSizeNotEqualErr)
	}
	for _, r := range partition.Replicas {
		var dnPartition *proto.DNDataPartitionInfo
		var err1 error
		addr := strings.Split(r.Addr, ":")[0]
		//check dataPartition by dataNode api
		for i := 0; i < 3; i++ {
			if dnPartition, err1 = client.NodeAPI().DataNodeGetPartition(addr, partition.PartitionID); err1 == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if err1 != nil || dnPartition == nil {
			errorReports = append(errorReports, fmt.Sprintf("get partition[%v] failed in addr[%v], err:%v", partition.PartitionID, addr, err))
			continue
		}
		//RaftStatus Only exists on leader
		if dnPartition.RaftStatus != nil && dnPartition.RaftStatus.NodeID == dnPartition.RaftStatus.Leader {
			leaderStatus = dnPartition.RaftStatus
		}
		//print the hosts,peers,learners detail info
		peerStrings := convertPeersToArray(dnPartition.Peers)
		learnerStrings := convertLearnersToArray(dnPartition.Learners)
		sort.Strings(peerStrings)
		sort.Strings(dnPartition.Replicas)
		sort.Strings(learnerStrings)
		sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
			"", "", "", fmt.Sprintf("%-22v", r.Addr), fmt.Sprintf("%v/%v", len(dnPartition.Replicas), partition.ReplicaNum), "(hosts)"+strings.Join(dnPartition.Replicas, ",")))
		sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
			"", "", "", fmt.Sprintf("%-22v", ""), fmt.Sprintf("%v/%v", len(peerStrings), partition.ReplicaNum), "(peers)"+strings.Join(peerStrings, ",")))
		if len(dnPartition.Learners) > 0 {
			sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
				"", "", "", fmt.Sprintf("%-22v", ""), fmt.Sprintf("%v/%v", len(learnerStrings), len(partition.Learners)), "(learners)"+strings.Join(learnerStrings, ",")))
		}
		if !isEqualStrings(peerStrings, dnPartition.Replicas) || !isEqualStrings(partition.Hosts, peerStrings) || len(dnPartition.Replicas) != int(partition.ReplicaNum) || len(partition.Learners) != len(dnPartition.Learners) {
			errorReports = append(errorReports, fmt.Sprintf(ReplicaNotConsistent+" on host[%v]", r.Addr))
		}
	}
	if leaderStatus == nil || len(leaderStatus.Replicas) == 0 {
		errorReports = append(errorReports, RaftNoLeader)
	}
	return
}
func newResetDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var optManualResetAddrs string
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [DATA PARTITION ID]",
		Short: cmdResetDataPartitionShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
not reach an agreement with one leader. In this case, you can use the "reset" command to fix the problem, however 
this action may lead to data loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partitionID uint64
				confirm     string
				err         error
			)
			defer func() {
				if err != nil {
					errout("Error:%v", err)
					OsExitWithLogFlush()
				}
			}()
			partitionID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			stdout(fmt.Sprintf("The action may risk the danger of losing data, please confirm(y/n):"))
			_, _ = fmt.Scanln(&confirm)
			if "y" != confirm && "yes" != confirm {
				return
			}
			if "" != optManualResetAddrs {
				if err = client.AdminAPI().ManualResetDataPartition(partitionID, optManualResetAddrs); err != nil {
					return
				}
			} else {
				if err = client.AdminAPI().ResetDataPartition(partitionID); err != nil {
					return
				}
			}
		},
	}
	cmd.Flags().StringVar(&optManualResetAddrs, CliFlagAddress, "", "reset raft members according to the addr, split by ',' ")

	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [DATA PARTITION ID] [DestAddr] ",
		Short: cmdDataPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var destAddr string
			if len(args) >= 3 {
				destAddr = args[2]
			}
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DecommissionDataPartition(partitionID, address, destAddr); err != nil {
				stdout("%v\n", err)
				return
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

func newDataPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var optAddReplicaType string
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [DATA PARTITION ID] [ADDRESS]",
		Short: cmdDataPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var address string
			if len(args) == 1 && optAddReplicaType == "" {
				stdout("there must be at least 2 args or use add-replica-type flag\n")
				return
			}
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if len(args) >= 2 {
				address = args[1]
			}
			var addReplicaType proto.AddReplicaType
			if optAddReplicaType != "" {
				var addReplicaTypeUint uint64
				if addReplicaTypeUint, err = strconv.ParseUint(optAddReplicaType, 10, 64); err != nil {
					stdout("%v\n", err)
					return
				}
				addReplicaType = proto.AddReplicaType(addReplicaTypeUint)
				if addReplicaType != proto.AutoChooseAddrForQuorumVol && addReplicaType != proto.DefaultAddReplicaType {
					err = fmt.Errorf("region type should be %d(%s) or %d(%s)",
						proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol, proto.DefaultAddReplicaType, proto.DefaultAddReplicaType)
					stdout("%v\n", err)
					return
				}
				stdout("partitionID:%v add replica type:%s\n", partitionID, addReplicaType)
			}
			if err = client.AdminAPI().AddDataReplica(partitionID, address, addReplicaType); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}

	cmd.Flags().StringVar(&optAddReplicaType, CliFlagAddReplicaType, "",
		fmt.Sprintf("Set add replica type[%d(%s)]", proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol))
	return cmd
}

func newDataPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DeleteDataReplica(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
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

func checkUsedSizeDiff(replicas []*proto.DataReplica, percent int, id uint64) (isEqual bool) {
	if len(replicas) < 2 {
		return true
	}
	if percent < 1 {
		percent = 1
	}
	if percent > 99 {
		percent = 99
	}
	isEqual = true
	var sizeArr []int
	for _, r := range replicas {
		sizeArr = append(sizeArr, int(r.Used))
	}
	sort.Ints(sizeArr)
	diff := sizeArr[len(sizeArr)-1] - sizeArr[0]
	if diff*100/percent > sizeArr[len(sizeArr)-1] {
		isEqual = false
		log.LogDebugf("pid: %v diff:%v is larger than 1 percent, sizeArray:%v \n", id, diff, sizeArr)
	}
	return
}

func newDataPartitionAddLearnerCmd(client *master.MasterClient) *cobra.Command {
	var (
		optAutoPromote bool
		optThreshold   uint8
	)
	const defaultLearnThreshold uint8 = 90
	var cmd = &cobra.Command{
		Use:   CliOpAddLearner + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionAddLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				autoPromote bool
				threshold   uint8
			)
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if optAutoPromote {
				autoPromote = optAutoPromote
			}
			if optThreshold <= 0 || optThreshold > 100 {
				threshold = defaultLearnThreshold
			} else {
				threshold = optThreshold
			}
			if err = client.AdminAPI().AddDataLearner(partitionID, address, autoPromote, threshold); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint8VarP(&optThreshold, CliFlagThreshold, "t", 0, "Specify threshold of learner,(0,100],default 90")
	cmd.Flags().BoolVarP(&optAutoPromote, CliFlagAutoPromote, "a", false, "Auto promote learner to peers")
	return cmd
}

func newDataPartitionPromoteLearnerCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpPromoteLearner + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionPromoteLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().PromoteDataLearner(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
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
func newDataPartitionCheckCommitCmd(client *master.MasterClient) *cobra.Command {
	var optSpecifyDP uint64
	var cmd = &cobra.Command{
		Use:   CliOpCheckCommit,
		Short: cmdCheckCommitDataPartitionShort,
		Long:  `if the follower lack too much raft log from leader, the raft may be hang, we should check and resolve it `,
		Run: func(cmd *cobra.Command, args []string) {
			if optSpecifyDP > 0 {
				partition, err1 := client.AdminAPI().GetDataPartition("", optSpecifyDP)
				if err1 != nil {
					stdout("%v\n", err1)
					return
				}
				for _, r := range partition.Replicas {
					if r.IsLeader && time.Now().Unix()-r.ReportTime <= defaultNodeTimeOutSec  {
						isLack, lackID, active, next, firstIdx, err := checkDataPartitionCommit(r.Addr, partition.PartitionID)
						if err != nil {
							continue
						}
						if isLack {
							var host string
							for _, p := range partition.Peers {
								if p.ID == lackID {
									host = p.Addr
								}
							}
							fmt.Printf("Volume,Partition,BadPeerID,BadHost,IsActive,Next,FirstIndex\n")
							fmt.Printf("%v,%v,%v,%v,%v,%v,%v\n", partition.VolName, optSpecifyDP, lackID, host, active, next, firstIdx)
						}
					}
				}
			} else {
				checkCommit(client)
			}
		},
	}
	cmd.Flags().Uint64Var(&optSpecifyDP, CliFlagId, 0, "check data partition by partitionID")
	return cmd
}

func newGetCanEcMigrateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetCanEcMigrate,
		Short: cmdGetCanEcMigrateShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err        error
				partitions = make([]*proto.DataPartitionResponse, 0)
			)
			if partitions, err = client.AdminAPI().GetCanMigrateDataPartitions(); err != nil {
				return
			}
			for _, partition := range partitions {
				stdout(formatDataPartitionTableRow(partition))
				stdout("\n")
			}
		},
	}
	return cmd
}

func newGetCanEcDelCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetCanEcDel,
		Short: cmdGetCanEcDelShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err        error
				partitions = make([]*proto.DataPartitionResponse, 0)
			)
			if partitions, err = client.AdminAPI().GetCanDelDataPartitions(); err != nil {
				return
			}
			for _, partition := range partitions {
				stdout(formatDataPartitionTableRow(partition))
				stdout("\n")
			}
		},
	}
	return cmd
}

func newDelDpAlreadyEc(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelAleadyEcDp + " [PARTITION ID]",
		Short: cmdDelDpAlreadyEc,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", client.AdminAPI().DeleteDpAlreadyEc(partitionID))
		},
	}
	return cmd
}

func newMigrateEc(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpMigrateEc + " [PARTITION ID]",
		Short: cmdMigrateEc,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			test := false
			if len(args) == 2 && args[1] == "test" {
				test = true
			}
			stdout("%v\n", client.AdminAPI().MigrateEcById(partitionID, test))
		},
	}
	return cmd
}

func getDataPartitionCrc(dp *proto.DataPartitionInfo, dpCrc map[uint64]map[string]uint32, extentInfo []uint64, dpWg *sync.WaitGroup, client *master.MasterClient, printLog bool) {
	defer dpWg.Done()
	var hostmapLock sync.Mutex
	for _, extentId := range extentInfo {
		if printLog {
			stdout("DataPartition:%v Extent:%v start\n", dp.PartitionID, extentId)
		}
		var wg sync.WaitGroup
		hostmap := make(map[string]uint32)
		for _, host := range dp.Hosts {
			wg.Add(1)
			go func(host string) {
				defer wg.Done()
				var (
					crc uint32
					err error
				)
				arr := strings.Split(host, ":")
				if printLog {
					stdout("  from DataNode(%v) get crc\n", host)
				}
				if crc, err = client.NodeAPI().DataNodeGetExtentCrc(arr[0], dp.PartitionID, extentId); err != nil {
					if printLog {
						stdout("  DataNode(%v) GetExtentCrc err(%v)\n", host, err)
					}
					return
				}
				hostmapLock.Lock()
				hostmap[host] = crc
				hostmapLock.Unlock()
			}(host)
		}
		wg.Wait()
		dpCrc[extentId] = hostmap
	}
}

func newStopMigratingByDataPartition(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpStopMigratingEc + " [PARTITION ID]",
		Short: cmdStopMigratingEcByDataPartition,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", client.AdminAPI().StopMigratingByDataPartition(partitionID))
		},
	}
	return cmd
}

func checkCommit(client *master.MasterClient) (err error) {

	f, _ := os.OpenFile(fmt.Sprintf("check_commit_%v.csv", time.Now().Unix()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer f.Close()
	stat, _ := f.Stat()
	if stat.Size() == 0 {
		f.WriteString("Volume,Partition,BadPeerID,BadHost,IsActive,Next,FirstIndex\n")
	}
	var badDps sync.Map
	var partitionFunc = func(volumeName string, partition *proto.DataPartitionResponse) (err error){
		isLack, lackID, _, _, _, err := checkDataPartitionCommit(partition.LeaderAddr, partition.PartitionID)
		if err != nil {
			return
		}
		if isLack {
			badDps.Store(partition.PartitionID, lackID)
		}
		return
	}

	var volFunc = func(vol *proto.SimpleVolView) {
		//retry to check
		for i := 0; i < 4; i++ {
			count := 0
			badDps.Range(func(key, value interface{}) bool {
				count++
				id := key.(uint64)
				oldLackID := value.(uint64)
				partition, err1 := client.AdminAPI().GetDataPartition("", id)
				if err1 != nil {
					return true
				}
				for _, r := range partition.Replicas {
					if r.IsLeader {
						isLack, lackID, _, _, _, err2 := checkDataPartitionCommit(r.Addr, partition.PartitionID)
						if err2 != nil {
							continue
						}
						if !isLack {
							badDps.Delete(partition.PartitionID)
						} else if lackID != oldLackID {
							badDps.Store(partition.PartitionID, lackID)
						}
					}
				}
				return true
			})
			if count == 0 {
				break
			}
			time.Sleep(time.Minute)
		}

		//output
		badDps.Range(func(key, value interface{}) bool {
			id := key.(uint64)
			partition, err1 := client.AdminAPI().GetDataPartition("", id)
			if err1 != nil {
				return true
			}
			for _, r := range partition.Replicas {
				if r.IsLeader {
					isLack, lackID, active, next, first, err2 := checkDataPartitionCommit(r.Addr, partition.PartitionID)
					if err2 != nil {
						continue
					}
					if isLack {
						var host string
						for _, p := range partition.Peers {
							if p.ID == lackID {
								host = p.Addr
							}
						}
						f.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v\n", vol.Name, id, lackID, host, active, next, first))
					}
				}
			}
			return true
		})
		f.Sync()
	}
	vols := loadSpecifiedVolumes()
	ids := loadSpecifiedPartitions()
	rangeAllDataPartitions(20, vols, ids, volFunc, partitionFunc)

	fmt.Println("scan finish, result has been saved to local file")
	return
}

func checkDataPartitionCommit(leader string, pid uint64,) (lack bool, lackID uint64, active bool, next, firstIdx uint64, err error) {
	var dnPartition *proto.DNDataPartitionInfo
	addr := strings.Split(leader, ":")[0]
	//check dataPartition by dataNode api
	for i := 0; i < 3; i++ {
		if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, pid); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	if dnPartition.RaftStatus != nil && dnPartition.RaftStatus.Replicas != nil {
		for id, r := range dnPartition.RaftStatus.Replicas {
			if dnPartition.RaftStatus.Leader == id {
				continue
			}
			if r.Next < dnPartition.RaftStatus.Log.FirstIndex || !r.Active {
				lack = true
				lackID = id
				next = r.Next
				active = r.Active
				firstIdx = dnPartition.RaftStatus.Log.FirstIndex
			}
		}
	}
	return
}