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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
	"strconv"
	"strings"
)

const (
	cmdDataPartitionUse   = "datapartition [COMMAND]"
	cmdDataPartitionShort = "Manage data partition"
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
	)
	return cmd
}

const (
	cmdDataPartitionGetShort          = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort = "Check out corrupt data partitions"
	cmdResetDataPartitionShort        = "Reset corrupt data partition"
	cmdDataPartitionDecommissionShort = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionReplicateShort    = "Create a replication of the data partition on a new address"
)

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
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
		},
	}
	return cmd
}

func newListCorruptDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptDataPartitionShort,
		Long: `If the data nodes are marked as "Inactive", it means the nodes has been not available for a time. It is suggested to eliminate
				the network, disk or other problems first. Once the bad nodes can never be "active", they are called corrupt nodes. And the 
				"decommission" command can be used to discard the corrupt nodes. However, if more than half replicas of a partition are on 
				the corrupt nodes, the few remaining replicas can not reach an agreement with one leader. In this case, you can use the 
				"datapartition reset" command to fix the problem, however this action may lead to data loss, be careful to do this.`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view          *proto.ClusterView
				inactiveNodes []string
				partitionMap  map[uint64]uint8
				err           error
			)
			partitionMap = make(map[uint64]uint8)
			if view, err = client.AdminAPI().GetCluster(); err != nil {
				return
			}
			stdout("[Inactive Data nodes]:\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.DataNodes {
				if !strings.Contains(formatNodeStatus(node.Status), "Inactive") {
					continue
				}
				inactiveNodes = append(inactiveNodes, node.Addr)
				stdout("%v\n", formatNodeView(&node, true))
			}
			stdout("\n")
			stdout("[Corrupt data partitions](no leader):\n")

			for _, addr := range inactiveNodes {
				var nodeInfo *proto.DataNodeInfo
				if nodeInfo, err = client.NodeAPI().GetDataNode(addr); err != nil {
					stdout(fmt.Sprintf("node not found, err:[%v]", err))
					continue
				}
				for _, partition := range nodeInfo.PersistenceDataPartitions {
					partitionMap[partition] = partitionMap[partition] + 1
				}
			}

			stdout("%v\n", partitionInfoTableHeader)
			for partitionID, badNum := range partitionMap {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				if badNum > partition.ReplicaNum/2 {
					stdout("%v\n", formatDataPartitionInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partition lack replicas]:")
			for _, vol := range view.VolStatInfo {
				var dps *proto.DataPartitionsView
				if dps, err = client.ClientAPI().GetDataPartitions(vol.Name); err != nil {
					stdout("Get data partition failed, err:[%v]\n", err)
					return
				}
				for _, dp := range dps.DataPartitions {
					if dp.ReplicaNum > uint8(len(dp.Hosts)) {
						stdout("%v\n", formatDataPartitionTableRow(dp))
					}
				}
			}
		},
	}
	return cmd
}

func newResetDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [DATA PARTITION ID]",
		Short: cmdResetDataPartitionShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
				not reach an agreement with one leader. In this case, you can use the "reset" command to fix the 
				problem, however this action may lead to data loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.DataPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				stdout("%v\n", err)
				return
			}
			var inactiveNum uint8
			for _, host := range partition.Hosts {
				var dataNodeInfo *proto.DataNodeInfo
				if dataNodeInfo, err = client.NodeAPI().GetDataNode(host); err != nil {
					stdout("%v\n", err)
					return
				}
				if !dataNodeInfo.IsActive {
					inactiveNum = inactiveNum + 1
				}
			}
			if inactiveNum > partition.ReplicaNum/2 {
				client.AdminAPI().ResetDataPartition(partition.VolName, partitionID)
			} else {
				stdout("%v\n", "can not reset, active replicas are more than half of all")
			}
		},
	}
	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DecommissionDataPartition(partitionID, address); err != nil {
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
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().AddDataReplica(partitionID, address); err != nil {
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
