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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
)

const (
	cmdMetaPartitionUse   = "metapartition [COMMAND]"
	cmdMetaPartitionShort = "Manage meta partition"
)

func newMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdMetaPartitionUse,
		Short: cmdMetaPartitionShort,
	}
	cmd.AddCommand(
		newMetaPartitionGetCmd(client),
		newListCorruptMetaPartitionCmd(client),
		newResetMetaPartitionCmd(client),
		newMetaPartitionDecommissionCmd(client),
		newMetaPartitionReplicateCmd(client),
		newMetaPartitionDeleteReplicaCmd(client),
		newMetaPartitionAddLearnerCmd(client),
		newMetaPartitionPromoteLearnerCmd(client),
		newMetaPartitionResetCursorCmd(client),
		newMetaPartitionListAllInoCmd(client),
		newMetaPartitionCheckSnapshot(client),
	)
	return cmd
}

const (
	cmdMetaPartitionGetShort            = "Display detail information of a meta partition"
	cmdCheckCorruptMetaPartitionShort   = "Check out corrupt meta partitions"
	cmdResetMetaPartitionShort          = "Reset corrupt meta partition"
	cmdMetaPartitionDecommissionShort   = "Decommission a replication of the meta partition to a new address"
	cmdMetaPartitionReplicateShort      = "Add a replication of the meta partition on a new address"
	cmdMetaPartitionDeleteReplicaShort  = "Delete a replication of the meta partition on a fixed address"
	cmdMetaPartitionAddLearnerShort     = "Add a learner of the meta partition on a new address"
	cmdMetaPartitionPromoteLearnerShort = "Promote the learner of the meta partition on a fixed address"
	cmdMetaPartitionResetCursorShort    = "Reset mp inode cursor"
	cmdMetaPartitionListAllInoShort     = "list mp all inodes id"
	cmdMetaPartitionCheckSnapshotShort  = "check snapshot is same by id"
)

func newMetaPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [META PARTITION ID]",
		Short: cmdMetaPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.MetaPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				return
			}
			stdout(formatMetaPartitionInfo(partition))
		},
	}
	return cmd
}

func newListCorruptMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	var optCheckAll bool
	var cmd = &cobra.Command{
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
				diagnosis *proto.MetaPartitionDiagnosis
				metaNodes []*proto.MetaNodeInfo
				err       error
			)
			if optCheckAll {
				err = checkAllMetaPartitions(client)
				if err != nil {
					errout("%v\n", err)
				}
				return
			}
			if diagnosis, err = client.AdminAPI().DiagnoseMetaPartition(); err != nil {
				stdout("%v\n", err)
			}
			stdout("[Inactive Meta nodes]:\n")
			stdout("%v\n", formatMetaNodeDetailTableHeader())
			sort.SliceStable(diagnosis.InactiveMetaNodes, func(i, j int) bool {
				return diagnosis.InactiveMetaNodes[i] < diagnosis.InactiveMetaNodes[j]
			})
			for _, addr := range diagnosis.InactiveMetaNodes {
				var node *proto.MetaNodeInfo
				node, err = client.NodeAPI().GetMetaNode(addr)
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
			sort.SliceStable(diagnosis.CorruptMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.CorruptMetaPartitionIDs[i] < diagnosis.CorruptMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.CorruptMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				stdout("%v", formatMetaPartitionInfoRow(partition))
			}

			stdout("\n")
			stdout("%v\n", "[Partition lack replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.LackReplicaMetaPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaMetaPartitionIDs[i] < diagnosis.LackReplicaMetaPartitionIDs[j]
			})
			for _, pid := range diagnosis.LackReplicaMetaPartitionIDs {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				if partition == nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				stdout("%v", formatMetaPartitionInfoRow(partition))
				sort.Strings(partition.Hosts)
				for _, r := range partition.Replicas {
					var mnPartition *proto.MNMetaPartitionInfo
					var err error
					addr := strings.Split(r.Addr, ":")[0]
					if mnPartition, err = client.NodeAPI().MetaNodeGetPartition(addr, partition.PartitionID); err != nil {
						fmt.Printf(partitionInfoColorTablePattern+"\n",
							"", "", "", r.Addr, fmt.Sprintf("%v/%v", 0, partition.ReplicaNum+partition.LearnerNum), "no data")
						continue
					}
					mnHosts := make([]string, 0)
					for _, peer := range mnPartition.Peers {
						mnHosts = append(mnHosts, peer.Addr)
					}
					sort.Strings(mnHosts)
					fmt.Printf(partitionInfoColorTablePattern+"\n",
						"", "", "", r.Addr, fmt.Sprintf("%v/%v", len(mnPartition.Peers), partition.ReplicaNum+partition.LearnerNum), strings.Join(mnHosts, "; "))
				}
				fmt.Printf("\033[1;40;32m%-8v\033[0m", strings.Repeat("_ ", len(partitionInfoTableHeader)/2+5)+"\n")
			}
			return
		},
	}
	cmd.Flags().BoolVar(&optCheckAll, "all", false, "true - check all partitions; false - only check partitions which lack of replica")
	return cmd
}
func checkAllMetaPartitions(client *master.MasterClient) (err error) {
	var volInfo []*proto.VolInfo
	if volInfo, err = client.AdminAPI().ListVols(""); err != nil {
		stdout("%v\n", err)
		return
	}
	stdout("\n")
	stdout("%v\n", "[Partition peer info not valid]:")
	stdout("%v\n", partitionInfoTableHeader)
	for _, vol := range volInfo {
		var (
			volView  *proto.VolView
			drawLock sync.Mutex
			wg       sync.WaitGroup
		)
		if volView, err = client.ClientAPI().GetVolume(vol.Name, calcAuthKey(vol.Owner)); err != nil {
			stdout("Found an invalid vol: %v\n", vol.Name)
			continue
		}
		/*		sort.SliceStable(volView.MetaPartitions, func(i, j int) bool {
				return volView.MetaPartitions[i].PartitionID < volView.MetaPartitions[j].PartitionID
			})*/
		mpCh := make(chan bool, 10)
		for _, mp := range volView.MetaPartitions {
			wg.Add(1)
			mpCh <- true
			go func(mp *proto.MetaPartitionView) {
				defer func() {
					wg.Done()
					<-mpCh
				}()
				var outPut string
				var isHealthy bool
				outPut, isHealthy, _ = checkMetaPartition(mp.PartitionID, client)
				if !isHealthy {
					drawLock.Lock()
					fmt.Printf(outPut)
					stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
					drawLock.Unlock()
				}
				time.Sleep(time.Millisecond * 10)
			}(mp)
		}
		wg.Wait()
	}
	return
}
func checkMetaPartition(pid uint64, client *master.MasterClient) (outPut string, isHealthy bool, err error) {
	var (
		partition    *proto.MetaPartitionInfo
		errorReports []string
		sb           = strings.Builder{}
	)
	defer func() {
		isHealthy = true
		if len(errorReports) > 0 {
			isHealthy = false
			for i, msg := range errorReports {
				sb.WriteString(fmt.Sprintf("%-8v\n", fmt.Sprintf("error %v: %v", i+1, msg)))
			}
		}
		outPut = sb.String()
	}()
	if partition, err = client.ClientAPI().GetMetaPartition(pid); err != nil || partition == nil {
		errorReports = append(errorReports, fmt.Sprintf("partition not found, err:[%v]", err))
		return
	}
	sb.WriteString(fmt.Sprintf("%v", formatMetaPartitionInfoRow(partition)))
	sort.Strings(partition.Hosts)
	if len(partition.MissNodes) > 0 || partition.Status == -1 || len(partition.Hosts) != int(partition.ReplicaNum+partition.LearnerNum) {
		errorReports = append(errorReports, fmt.Sprintf("partition is unhealthy in master"))
	}
	for _, r := range partition.Replicas {
		var mnPartition *proto.MNMetaPartitionInfo
		var err1 error
		addr := strings.Split(r.Addr, ":")[0]
		for i := 0; i < 3; i++ {
			if mnPartition, err1 = client.NodeAPI().MetaNodeGetPartition(addr, partition.PartitionID); err1 == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if err1 != nil || mnPartition == nil {
			errorReports = append(errorReports, fmt.Sprintf("get partition[%v] in addr[%v] failed, err:%v", partition.PartitionID, addr, err))
			continue
		}
		peerStrings := convertPeersToArray(mnPartition.Peers)
		learnerStrings := convertLearnersToArray(mnPartition.Learners)
		sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
			"", "", "", fmt.Sprintf("%-22v", r.Addr), fmt.Sprintf("%v/%v", len(peerStrings), partition.ReplicaNum+partition.LearnerNum), "(peer)"+strings.Join(peerStrings, ",")))
		if len(learnerStrings) > 0 {
			sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n",
				"", "", "", fmt.Sprintf("%-22v", r.Addr), fmt.Sprintf("%v/%v", len(learnerStrings), partition.LearnerNum), "(learner)"+strings.Join(learnerStrings, ",")))
		}
		sort.Strings(peerStrings)
		if !isEqualStrings(partition.Hosts, peerStrings) || len(peerStrings) != int(partition.ReplicaNum+partition.LearnerNum) || len(partition.Learners) != len(learnerStrings) {
			errorReports = append(errorReports, fmt.Sprintf(ReplicaNotConsistent+" on host[%v]", r.Addr))
		}
	}
	return
}
func newMetaPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [META PARTITION ID] [DestAddr] ",
		Short: cmdMetaPartitionDecommissionShort,
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
			if err = client.AdminAPI().DecommissionMetaPartition(partitionID, address, destAddr); err != nil {
				stdout("%v\n", err)
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

func newResetMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	var optManualResetAddrs string
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [META PARTITION ID]",
		Short: cmdResetMetaPartitionShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
not reach an agreement with one leader. In this case, you can use the "metapartition reset" command
to fix the problem, however this action may lead to data loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				confirm     string
				partitionID uint64
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
			stdout(fmt.Sprintf("The action may risk the danger of losing meta data, please confirm(y/n):"))
			_, _ = fmt.Scanln(&confirm)
			if "y" != confirm && "yes" != confirm {
				return
			}
			if "" != optManualResetAddrs {
				if err = client.AdminAPI().ManualResetMetaPartition(partitionID, optManualResetAddrs); err != nil {
					return
				}
			} else {
				if err = client.AdminAPI().ResetMetaPartition(partitionID); err != nil {
					return
				}
			}
		},
	}
	cmd.Flags().StringVar(&optManualResetAddrs, CliFlagAddress, "", "reset raft members according to the addr, split by ',' ")
	return cmd
}

func newMetaPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var optAddReplicaType string
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [META PARTITION ID] [ADDRESS]",
		Short: cmdMetaPartitionReplicateShort,
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
			if err = client.AdminAPI().AddMetaReplica(partitionID, address, addReplicaType); err != nil {
				stdout("%v\n", err)
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

	cmd.Flags().StringVar(&optAddReplicaType, CliFlagAddReplicaType, "",
		fmt.Sprintf("Set add replica type[%d(%s)]", proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol))
	return cmd
}

func newMetaPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DeleteMetaReplica(partitionID, address); err != nil {
				stdout("%v\n", err)
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

func newMetaPartitionAddLearnerCmd(client *master.MasterClient) *cobra.Command {
	var (
		optAutoPromote    bool
		optThreshold      uint8
		optAddReplicaType string
	)
	const defaultLearnerThreshold uint8 = 90
	var cmd = &cobra.Command{
		Use:   CliOpAddLearner + " [META PARTITION ID] [ADDRESS]",
		Short: cmdMetaPartitionAddLearnerShort,
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
			var (
				autoPromote bool
				threshold   uint8
			)
			if optAutoPromote {
				autoPromote = optAutoPromote
			}
			if optThreshold <= 0 || optThreshold > 100 {
				threshold = defaultLearnerThreshold
			} else {
				threshold = optThreshold
			}
			if err = client.AdminAPI().AddMetaReplicaLearner(partitionID, address, autoPromote, threshold, addReplicaType); err != nil {
				stdout("%v\n", err)
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
	cmd.Flags().Uint8VarP(&optThreshold, CliFlagThreshold, "t", 0, "Specify threshold of learner,(0,100],default 90")
	cmd.Flags().BoolVarP(&optAutoPromote, CliFlagAutoPromote, "a", false, "Auto promote learner to peers")
	cmd.Flags().StringVar(&optAddReplicaType, CliFlagAddReplicaType, "",
		fmt.Sprintf("Set add replica type[%d(%s)]", proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol))
	return cmd
}

func newMetaPartitionPromoteLearnerCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpPromoteLearner + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionPromoteLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().PromoteMetaReplicaLearner(partitionID, address); err != nil {
				stdout("%v\n", err)
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

func newMetaPartitionResetCursorCmd(client *master.MasterClient) *cobra.Command {
	var optForce bool
	var cmd = &cobra.Command{
		Use:   CliOpResetCursor + " [META PARTITION ID] [inode]",
		Short: cmdMetaPartitionResetCursorShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			inodeId := uint64(0)
			ip := ""
			var mp *proto.MetaPartitionInfo
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}

			if len(args) >= 2 {
				inodeId, err = strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					inodeId = 0
				}
			}
			if mp, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				stdout("%v\n", err)
				return
			}

			for _, replica := range mp.Replicas {
				if replica.IsLeader {
					ip = strings.Split(replica.Addr, ":")[0]
				}
			}
			ip += ":" + strconv.Itoa(int(client.MetaNodeProfPort))

			mtClient := meta.NewMetaHttpClient(ip, false)
			resp, err := mtClient.ResetCursor(partitionID, inodeId, optForce)
			if err != nil {
				errout("get resp err:%s\n", err.Error())
			}

			stdout("reset success, mp[%v], start: %v, end:%v, cursor:%v\n", partitionID, resp.Start, resp.End, resp.Cursor)
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVar(&optForce, "force", false, "force reset cursor through max inode is high")
	return cmd
}

func newMetaPartitionListAllInoCmd(client *master.MasterClient) *cobra.Command {
	var optDisplay bool
	var optMode uint32
	var optStTime int64
	var optEndTime int64
	var cmd = &cobra.Command{
		Use:   CliOpListMpAllInos + " [META PARTITION ID]",
		Short: cmdMetaPartitionListAllInoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ip := ""
			var mp *proto.MetaPartitionInfo
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}

			if mp, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				stdout("%v\n", err)
				return
			}

			for _, replica := range mp.Replicas {
				if replica.IsLeader {
					ip = strings.Split(replica.Addr, ":")[0]
				}
			}
			ip += ":" + strconv.Itoa(int(client.MetaNodeProfPort))

			mtClient := meta.NewMetaHttpClient(ip, false)
			resp, err := mtClient.ListAllInodesId(partitionID, optMode, optStTime, optEndTime)
			if err != nil {
				errout("get resp err:%s\n", err.Error())
			}

			stdout("list all inodes success, mp[%v], count:%v\n", partitionID, resp.Count)
			if optDisplay {
				for i, ino := range resp.Inodes {
					if i%5 == 0 {
						stdout("\n")
					}
					stdout("%d\t", ino)
				}
				stdout("\n")
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validMetaNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVar(&optDisplay, "display", false, "display all inode id")
	cmd.Flags().Uint32Var(&optMode, "mode", 0, "specify inode mode")
	cmd.Flags().Int64Var(&optStTime, "start", 0, "specify inode mtime > start")
	cmd.Flags().Int64Var(&optEndTime, "end", 0, "specify inode mtime < end")
	return cmd
}

func newMetaPartitionCheckSnapshot(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheckSnapshot + " [META PARTITION ID]",
		Short: cmdMetaPartitionCheckSnapshotShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.MetaPartitionInfo
				leaderCrc proto.SnapshotCrdResponse
				peersCrc  []proto.SnapshotCrdResponse
				resp      *http.Response
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			fmt.Printf("partitionID: %v\n", partitionID)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if partition, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				return
			}
			for index, peer := range partition.Peers {
				addr := strings.Split(peer.Addr, ":")[0]
				metaNodeProfPort := client.MetaNodeProfPort
				resp, err = http.Get(fmt.Sprintf("http://%s:%d%s?pid=%v", addr, metaNodeProfPort, proto.ClientMetaPartitionSnapshotCheck, partitionID))
				if err != nil {
					errout("get snapshotCheckSum info failed:\n%v\n", err)
					return
				}
				all, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					errout("read snapshotCheckSum info  failed:\n%v\n", err)
					return
				}
				value := make(map[string]interface{})
				err = json.Unmarshal(all, &value)
				if err != nil {
					errout("unmarshal snapshotCheckSum info  failed:\n%v\n", err)
					return
				}
				dataRaw, err := json.Marshal(value["data"])
				if err != nil {
					log.LogWarnf("unmarshal failed ,err: %v", err)
				}
				data := proto.SnapshotCrdResponse{}
				err = json.Unmarshal(dataRaw, &data)
				if err != nil {
					log.LogWarnf("err: %v", err)
				}
				if (partition.Replicas[index]).IsLeader == true {
					leaderCrc = data
				} else {
					log.LogWarnf("data.LastSnapshotStr: %v", data.LastSnapshotStr)
					peersCrc = append(peersCrc, data)
				}
			}
			if len(peersCrc) > 0 {
				stdout("%v", metaPartitionSnapshotCrcInfoTableHeader)
				if len(leaderCrc.LastSnapshotStr) > 0 {
					crcStr := strings.SplitN(leaderCrc.LastSnapshotStr, ",", 3)
					stdout(fmt.Sprintf(metaPartitionSnapshotCrcInfoTablePattern, leaderCrc.LocalAddr, "leader",
						crcStr[0], crcStr[1], crcStr[2]))
				}
				for _, peerCrc := range peersCrc {
					if peerCrc.LastSnapshotStr != "" {
						crcStr := strings.SplitN(peerCrc.LastSnapshotStr, ",", 3)
						stdout(fmt.Sprintf(metaPartitionSnapshotCrcInfoTablePattern, peerCrc.LocalAddr, "peer ",
							crcStr[0], crcStr[1], crcStr[2]))
					}
				}
			} else {
				fmt.Println("peersCrc is not created!")
			}
		},
	}
	return cmd
}
