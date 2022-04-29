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
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/tiglabs/raft"
)

const (
	cmdDataPartitionUse   = "datapartition [COMMAND]"
	cmdDataPartitionShort = "Manage data partition"
)

const (
	DataNodePprofPort string = "17320"
)

var (
	optShowRaftStatus bool
)

type PartitionStatus struct {
	VolName    string       `json:"volName"`
	ID         uint64       `json:"id"`
	Size       int          `json:"size"`
	Used       int          `json:"used"`
	Status     int          `json:"status"`
	Path       string       `json:"path"`
	FileCount  int          `json:"fileCount"`
	Replicas   []string     `json:"replicas"`
	RaftStatus *raft.Status `json:"raftStatus"`
}

type DataPartitionResponse struct {
	Code int             `json:"code"`
	Data PartitionStatus `json:"data"`
	Msg  string          `json:"msg"`
}

func newDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdDataPartitionUse,
		Short:   cmdDataPartitionShort,
		Aliases: []string{"dp"},
	}
	cmd.AddCommand(
		newDataPartitionGetCmd(client),
		newListCorruptDataPartitionCmd(client),
		newFixLackReplicaDataPartitionCmd(client),
		newDataPartitionDecommissionCmd(client),
		newDataPartitionBatchDecommissionCmd(client),
		newDataPartitionReplicateCmd(client),
		newDataPartitionDeleteReplicaCmd(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort               = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort      = "Check and list unhealthy data partitions"
	cmdFixLackReplicaDataPartitionShort    = "Check and fix data partitions lack of replica"
	cmdDataPartitionDecommissionShort      = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionBatchDecommissionShort = "Batch decommission a replication of the data partitions specified in a file separated by white space"
	cmdDataPartitionReplicateShort         = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort     = "Delete a replication of the data partition on a fixed address"
)

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
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
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if partitionID, err = strconv.ParseUint(args[0], 10, 64); err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				return
			}
			stdout(formatDataPartitionInfo(partition))
		},
	}
	cmd.Flags().BoolVarP(&optShowRaftStatus, "show-raft", "r", false, "Display data partition raft status")
	return cmd
}

func newListCorruptDataPartitionCmd(client *master.MasterClient) *cobra.Command {
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
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(); err != nil {
				return
			}
			stdout("[Inactive Data nodes]:\n")
			stdout("%v\n", formatDataNodeDetailTableHeader())
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
				stdout("%v\n", formatDataNodeDetail(node, true))
			}
			stdout("\n")
			stdout("[Corrupt data partitions](no leader):\n")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.CorruptDataPartitionIDs, func(i, j int) bool {
				return diagnosis.CorruptDataPartitionIDs[i] < diagnosis.CorruptDataPartitionIDs[j]
			})
			for _, pid := range diagnosis.CorruptDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
				}
				stdout("%v\n", formatDataPartitionInfoRow(partition))
			}

			stdout("\n")
			stdout("%v\n", "[Partition lack replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
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
					stdout("%v\n", formatDataPartitionInfoRow(partition))
					stdout("missing nodes(%v)\n", partition.MissingNodes)
				}
			}

			stdout("\n")
			stdout("%v\n", "[Bad data partitions(decommission not completed)]:")
			badPartitionTablePattern := "%-8v    %-10v\n"
			stdout(badPartitionTablePattern, "PATH", "PARTITION ID")
			for _, bdpv := range diagnosis.BadDataPartitionIDs {
				sort.SliceStable(bdpv.PartitionIDs, func(i, j int) bool {
					return bdpv.PartitionIDs[i] < bdpv.PartitionIDs[j]
				})
				for _, pid := range bdpv.PartitionIDs {
					stdout(badPartitionTablePattern, bdpv.Path, pid)
				}
			}
			return
		},
	}
	return cmd
}

func newFixLackReplicaDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpFix,
		Short: cmdFixLackReplicaDataPartitionShort,
		Long:  `Fix lack of replica data partitions`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view      *proto.ClusterView
				dataNodes map[uint64]string
				diagnosis *proto.DataPartitionDiagnosis
				err       error
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()

			// Get all the nodes and establish an id->addr map
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			dataNodes = make(map[uint64]string)
			for _, node := range view.DataNodes {
				dataNodes[node.ID] = node.Addr
			}

			// Get diagnosis results
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(); err != nil {
				return
			}
			sort.SliceStable(diagnosis.LackReplicaDataPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaDataPartitionIDs[i] < diagnosis.LackReplicaDataPartitionIDs[j]
			})

			// Fix dp
			for _, pid := range diagnosis.LackReplicaDataPartitionIDs {
				var (
					partition  *proto.DataPartitionInfo
					leaderAddr string
				)

				stdout("FIX [%v] ... ", pid)
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					err = fmt.Errorf("Failed to get partition, err:[%v] ", err)
					stdout("SKIP(%v)\n", err)
					return
				}
				if partition == nil {
					stdout("SKIP(partition is nil)\n")
					return
				}
				for _, replica := range partition.Replicas {
					if replica.IsLeader {
						leaderAddr = replica.Addr
					}
				}
				if leaderAddr == "" {
					stdout("SKIP(no leader)\n")
					return
				}

				var dataNodeResp *DataPartitionResponse
				dataNodeResp, err = getDataNodePartitionStatus(leaderAddr, pid)
				if err != nil {
					stdout("SKIP(failed to get partition status, err[%v])\n", err)
					return
				}

				var peers map[uint64]string
				peers = make(map[uint64]string)
				for _, peer := range partition.Peers {
					peers[peer.ID] = peer.Addr
				}

				var staleAddr string

				// Delete stale replica
				for id := range dataNodeResp.Data.RaftStatus.Replicas {
					var ok bool

					if _, ok = peers[id]; ok {
						continue
					}
					staleAddr, ok = dataNodes[id]
					if !ok {
						stdout("SKIP(no such node id: %v)\n", id)
						return
					}
					stdout("pid[%v] staleAddr[%v] ", pid, staleAddr)
					if err = client.AdminAPI().DeleteDataReplica(pid, staleAddr); err != nil {
						stdout("SKIP(failed to delete replica: %v)\n", err)
						return
					}
					break
				}

				// Add replica
				if len(partition.Hosts) <= 2 {
					var targetAddr string

					if staleAddr == "" {
						for id := range dataNodes {
							if _, ok := peers[id]; !ok {
								targetAddr = dataNodes[id]
							}
						}
					} else {
						targetAddr = staleAddr
					}
					stdout("targetAddr[%v] ", targetAddr)
					if err = client.AdminAPI().AddDataReplica(pid, targetAddr); err != nil {
						stdout("SKIP(failed to add replica: addr[%v] err[%v])", staleAddr, err)
						return
					}
				}

				stdout(" OK\n")
			}

			return
		},
	}
	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliOpDecommission + " [ADDRESS] [DATA PARTITION ID]",
		Aliases: []string{"decomm"},
		Short:   cmdDataPartitionDecommissionShort,
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			address := args[0]
			partitionID, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			if err = client.AdminAPI().DecommissionDataPartition(partitionID, address); err != nil {
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

func newDataPartitionBatchDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliOpBatchDecommission + " [ADDRESS] [FILE CONTAINS DP ID]",
		Aliases: []string{"b-decomm"},
		Short:   cmdDataPartitionBatchDecommissionShort,
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				data        []byte
				partitionID uint64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			address := args[0]
			dpFile := args[1]
			data, err = os.ReadFile(dpFile)
			if err != nil {
				return
			}
			dpStrings := strings.Split(strings.TrimSuffix(string(data), "\n"), " ")
			for _, dpString := range dpStrings {
				partitionID, err = strconv.ParseUint(dpString, 10, 64)
				stdout("Decommission [%v] ... ", partitionID)
				if err != nil {
					return
				}
				if err = client.AdminAPI().DecommissionDataPartition(partitionID, address); err != nil {
					return
				}
				stdout("OK\n")
			}
			stdout("Batch decommission finished!\n")
			return
		},
	}
	return cmd
}

func newDataPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			address := args[0]
			if partitionID, err = strconv.ParseUint(args[1], 10, 64); err != nil {
				return
			}
			if err = client.AdminAPI().AddDataReplica(partitionID, address); err != nil {
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

func newDataPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			address := args[0]
			if partitionID, err = strconv.ParseUint(args[1], 10, 64); err != nil {
				return
			}
			if err = client.AdminAPI().DeleteDataReplica(partitionID, address); err != nil {
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

func getDataNodePartitionStatus(addr string, pid uint64) (dataNodeResp *DataPartitionResponse, err error) {
	var (
		resp     *http.Response
		respData []byte
	)

	ip := strings.Split(addr, ":")
	if len(ip) != 2 {
		err = fmt.Errorf("invalid addr[%v]", addr)
		return
	}
	url := fmt.Sprintf("http://%s:%s/partition?id=%d", ip[0], DataNodePprofPort, pid)
	resp, err = http.Get(url)
	if err != nil {
		err = fmt.Errorf("failed to GET url[%v] err[%v]", url, err)
		return
	}
	respData, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		err = fmt.Errorf("read body failed, url[%v] err[%v]", url, err)
		return
	}
	if err = json.Unmarshal(respData, &dataNodeResp); err != nil {
		err = fmt.Errorf("unmarshal failed, url[%v] err[%v]", url, err)
		return
	}
	return
}
