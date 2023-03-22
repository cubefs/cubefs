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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
	"sort"
	"strconv"
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
		newDataPartitionDecommissionCmd(client),
		newDataPartitionReplicateCmd(client),
		newDataPartitionDeleteReplicaCmd(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort           = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort  = "Check and list unhealthy data partitions"
	cmdDataPartitionDecommissionShort  = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionReplicateShort     = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort = "Delete a replication of the data partition on a fixed address"
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

			stdout("\n")
			stdout("%v\n", "[Partition has unavailable replica]:")
			stdout("%v\n", badReplicaPartitionInfoTableHeader)
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
					stdout("%v\n", formatBadReplicaDpInfoRow(partition))
				}
			}
			return
		},
	}
	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var raftForceDel bool
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDecommissionShort,
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
			if err = client.AdminAPI().DecommissionDataPartition(partitionID, address, raftForceDel); err != nil {
				return
			}
			stdout("Decommission data partition successfully\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&raftForceDel, "raftForceDel", "r", false, "true for raftForceDel")
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
			stdout("Add replication successfully\n")
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
			stdout("Delete replication successfully\n")
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
