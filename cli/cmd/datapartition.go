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
	"time"
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
	)
	return cmd
}

const (
	cmdDataPartitionGetShort = "List information of data partitions"
)

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [VOLUME] [PARTITION ID]",
		Short: cmdDataPartitionGetShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.DataPartitionInfo
			)
			volName := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if partition, err = client.AdminAPI().GetDataPartition(volName, partitionID); err != nil {
				return
			}
			stdout(fmt.Sprintf("volume info: \n"))
			stdout(fmt.Sprintf("  volume name: %v\n", partition.VolName))
			stdout(fmt.Sprintf("  volume ID:   %v\n", partition.VolID))
			stdout("\n")
			stdout(fmt.Sprintf("partition info:\n"))
			stdout(fmt.Sprintf("  Replicas: \n"))
			for index, replica := range partition.Replicas {
				stdout(fmt.Sprintf("    -%v\n", index))
				stdout(formatReplica("      ", replica))
			}
			stdout(fmt.Sprintf("  PartitionID:    %v\n", partition.PartitionID))
			stdout(fmt.Sprintf("  Hosts:          %v\n", partition.Hosts))
			stdout(fmt.Sprintf("  Status:         %v\n", partition.Status))
			stdout(fmt.Sprintf("  LastLoadedTime: %v\n", time.Unix(partition.LastLoadedTime, 0).Format("2006-01-02 15:04:05")))
			stdout(fmt.Sprintf("  Peers: \n"))
			for index, peer := range partition.Peers {
				stdout(fmt.Sprintf("    -%v\n", index))
				stdout(formatPeer("      ", peer))
			}
			stdout(fmt.Sprintf("  Zones:          %v\n", partition.Zones))
			stdout(fmt.Sprintf("  MissingNodes: \n"))
			for partitionHost, id := range partition.MissingNodes {
				stdout(fmt.Sprintf("    %v %v\n", partitionHost, id))
			}
			stdout(fmt.Sprintf("  FilesWithMissingReplica: \n"))
			for file, id := range partition.FilesWithMissingReplica {
				stdout(fmt.Sprintf("    %v %v\n", file, id))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}
