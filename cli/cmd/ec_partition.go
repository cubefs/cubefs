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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
	"strconv"
)

const (
	cmdEcPartitionUse   = "ecpartition [COMMAND]"
	cmdEcPartitionShort = "Manage EC partition"
)

func newEcPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdEcPartitionUse,
		Short: cmdEcPartitionShort,
	}
	cmd.AddCommand(
		newEcPartitionGetCmd(client),
		newEcPartitionDecommissionCmd(client),
	)
	return cmd
}

const (
	cmdEcPartitionGetShort              = "Display detail information of a ec partition"
	cmdEcPartitionDecommissionShort     = "Decommission a replication of the ec partition to a new address"
)

func newEcPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [EC PARTITION ID]",
		Short: cmdEcPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.EcPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetEcPartition("", partitionID); err != nil {
				return
			}
			stdout(formatEcPartitionInfo(partition))
		},
	}
	return cmd
}

func newEcPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [EC PARTITION ID]",
		Short: cmdEcPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DecommissionEcPartition(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}


func validEcNodes(client *master.MasterClient, toComplete string) []string {
	var (
		validEcNodes []string
		clusterView    *proto.ClusterView

		err error
	)
	if clusterView, err = client.AdminAPI().GetCluster(); err != nil {
		errout("Get ec node list failed:\n%v\n", err)
	}
	for _, en := range clusterView.EcNodes {
		validEcNodes = append(validEcNodes, en.Addr)
	}
	return validEcNodes
}
