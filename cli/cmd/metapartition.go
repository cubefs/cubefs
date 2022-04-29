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
	"strconv"

	"github.com/spf13/cobra"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

const (
	cmdMetaPartitionUse   = "metapartition [COMMAND]"
	cmdMetaPartitionShort = "Manage meta partition"
)

func newMetaPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdMetaPartitionUse,
		Short:   cmdMetaPartitionShort,
		Aliases: []string{"mp"},
	}
	cmd.AddCommand(
		newMetaPartitionGetCmd(client),
		newListCorruptMetaPartitionCmd(client),
		newMetaPartitionDecommissionCmd(client),
		newMetaPartitionReplicateCmd(client),
		newMetaPartitionDeleteReplicaCmd(client),
	)
	return cmd
}

const (
	cmdMetaPartitionGetShort           = "Display detail information of a meta partition"
	cmdCheckCorruptMetaPartitionShort  = "Check out corrupt meta partitions"
	cmdMetaPartitionDecommissionShort  = "Decommission a replication of the meta partition to a new address"
	cmdMetaPartitionReplicateShort     = "Add a replication of the meta partition on a new address"
	cmdMetaPartitionDeleteReplicaShort = "Delete a replication of the meta partition on a fixed address"
)

func newMetaPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [META PARTITION ID]",
		Short: cmdMetaPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err         error
				partitionID uint64
				partition   *proto.MetaPartitionInfo
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if partitionID, err = strconv.ParseUint(args[0], 10, 64); err != nil {
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
"metapartition reset" command to fix the problem, however this action may lead to data loss, be careful to do this. The 
"reset" command will be released in next version.`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				diagnosis *proto.MetaPartitionDiagnosis
				metaNodes []*proto.MetaNodeInfo
				err       error
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
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
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
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
					err = fmt.Errorf("Partition not found, err:[%v] ", err)
					return
				}
				if partition != nil {
					stdout("%v\n", formatMetaPartitionInfoRow(partition))
				}
			}

			stdout("\n")
			stdout("%v\n", "[Bad meta partitions(decommission not completed)]:")
			badPartitionTablePattern := "%-8v    %-10v\n"
			stdout(badPartitionTablePattern, "PATH", "PARTITION ID")
			for _, bmpv := range diagnosis.BadMetaPartitionIDs {
				sort.SliceStable(bmpv.PartitionIDs, func(i, j int) bool {
					return bmpv.PartitionIDs[i] < bmpv.PartitionIDs[j]
				})
				for _, pid := range bmpv.PartitionIDs {
					stdout(badPartitionTablePattern, bmpv.Path, pid)
				}
			}
			return
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
			if err = client.AdminAPI().DecommissionMetaPartition(partitionID, address); err != nil {
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
			if err = client.AdminAPI().AddMetaReplica(partitionID, address); err != nil {
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

func newMetaPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [META PARTITION ID]",
		Short: cmdMetaPartitionDeleteReplicaShort,
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
			partitionID, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			if err = client.AdminAPI().DeleteMetaReplica(partitionID, address); err != nil {
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
