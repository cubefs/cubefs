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
	)
	return cmd
}

const (
	cmdMetaPartitionGetShort          = "Display detail information of a meta partition"
	cmdCheckCorruptMetaPartitionShort = "Check out corrupt meta partitions"
	cmdResetMetaPartitionShort        = "Reset corrupt meta partition"
	cmdMetaPartitionDecommissionShort = "Decommission a replication of the meta partition to a new address"
	cmdMetaPartitionReplicateShort    = "Create a replication of the meta partition on a new address"
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
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptMetaPartitionShort,
		Long: `If the meta nodes are marked as "Inactive", it means the nodes has been not available for a long time. It is suggested to eliminate
				the network, disk or other problems first. If the bad nodes can never be "active" again, they are called corrupt nodes. And the 
				"decommission" command can be used to discard the corrupt nodes. However, if more than half replicas of a partition are on 
				the corrupt nodes, the few remaining replicas can not reach an agreement with one leader. In this case, you can use the 
				"metapartition reset" command to fix the problem, however this action may lead to data loss, be careful to do this.`,
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
			stdout("[Inactive Meta nodes]:\n")
			stdout("%v\n", formatNodeViewTableHeader())
			for _, node := range view.MetaNodes {
				if !strings.Contains(formatNodeStatus(node.Status), "Inactive") {
					continue
				}
				inactiveNodes = append(inactiveNodes, node.Addr)
				stdout("%v\n", formatNodeView(&node, true))
			}
			stdout("\n")
			stdout("[Corrupt(no leader) meta partitions]:\n")

			for _, addr := range inactiveNodes {
				var nodeInfo *proto.MetaNodeInfo
				if nodeInfo, err = client.NodeAPI().GetMetaNode(addr); err != nil {
					stdout(fmt.Sprintf("node not found, err:[%v]", err))
					continue
				}
				for _, partition := range nodeInfo.PersistenceMetaPartitions {
					partitionMap[partition] = partitionMap[partition] + 1
				}
			}

			stdout("%v\n", partitionInfoTableHeader)
			for partitionID, badNum := range partitionMap {
				var partition *proto.MetaPartitionInfo
				if partition, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
					stdout("partition not found, err:[%v]", err)
					return
				}
				if badNum > partition.ReplicaNum/2 {
					stdout("%v\n", formatMetaPartitionInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Partitions lack replicas]:")
			for _, vol := range view.VolStatInfo {
				var mps []*proto.MetaPartitionView
				if mps, err = client.ClientAPI().GetMetaPartitions(vol.Name); err != nil {
					stdout("get meta partition failed, err:[%v]\n", err)
					return
				}
				var volume *proto.SimpleVolView
				if volume, err = client.AdminAPI().GetVolumeSimpleInfo(vol.Name); err != nil {
					stdout("get volume failed, err:[%v]\n", err)
					return
				}
				for _, mp := range mps {
					if volume.MpReplicaNum > uint8(len(mp.Members)) {
						stdout("%v\n", formatMetaPartitionTableRow(mp))
					}
				}
			}
		},
	}
	return cmd
}

func newResetMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [META PARTITION ID]",
		Short: cmdResetMetaPartitionShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
				not reach an agreement with one leader. In this case, you can use the "metapartition reset" command
				to fix the problem, however this action may lead to data loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.MetaPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if partition, err = client.ClientAPI().GetMetaPartition(partitionID); err != nil {
				stdout("%v\n", err)
				return
			}
			var inactiveNum uint8
			for _, host := range partition.Hosts {
				var metaNodeInfo *proto.MetaNodeInfo
				if metaNodeInfo, err = client.NodeAPI().GetMetaNode(host); err != nil {
					stdout("%v\n", err)
					return
				}
				if !metaNodeInfo.IsActive {
					inactiveNum = inactiveNum + 1
				}
			}
			if inactiveNum > partition.ReplicaNum/2 {
				if err = client.AdminAPI().ResetMetaPartition(partition.VolName, partitionID); err != nil {
					stdout("%v\n", err)
					return
				}
			} else {
				stdout("%v\n", "can not reset, active replicas are more than half of all")
			}
		},
	}
	return cmd
}

func newMetaPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DecommissionMetaPartition(partitionID, address); err != nil {
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

func newMetaPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().AddMetaReplica(partitionID, address); err != nil {
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
