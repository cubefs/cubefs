// Copyright 2023 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdNodeSetUse   = "nodeset [COMMAND]"
	cmdNodeSetShort = "Manage nodeset"
)

func newNodeSetCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdNodeSetUse,
		Short: cmdNodeSetShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newNodeSetListCmd(client),
		newNodeSetInfoCmd(client),
		newNodeSetUpdateCmd(client),
	)
	return cmd
}

const (
	cmdNodeSetListShort   = "List cluster nodeSets"
	cmdGetNodeSetShort    = "Show nodeSet information"
	cmdUpdateNodeSetShort = "Update nodeSet"
)

func newNodeSetListCmd(client *master.MasterClient) *cobra.Command {
	var zoneName string
	cmd := &cobra.Command{
		Use:   CliOpList,
		Short: cmdNodeSetListShort,
		Run: func(cmd *cobra.Command, args []string) {
			var nodeSetStats []*proto.NodeSetStat
			var err error
			defer func() {
				errout(err)
			}()
			if nodeSetStats, err = client.AdminAPI().ListNodeSets(zoneName); err != nil {
				return
			}
			zoneTablePattern := "%-6v %-6v %-12v %-10v %-10v\n"
			stdout(zoneTablePattern, "ID", "Cap", "Zone", "MetaNum", "DataNum")
			zoneDataPattern := "%-6v %-6v %-12v %-10v %-10v\n"
			for _, nodeSet := range nodeSetStats {
				stdout(zoneDataPattern, nodeSet.ID, nodeSet.Capacity, nodeSet.Zone, nodeSet.MetaNodeNum, nodeSet.DataNodeNum)
			}
		},
	}

	cmd.Flags().StringVar(&zoneName, CliFlagZoneName, "", "List nodeSets in the specified zone")
	return cmd
}

func newNodeSetInfoCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdGetNodeSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var nodeSetStatInfo *proto.NodeSetStatInfo
			var err error
			defer func() {
				errout(err)
			}()

			nodeSetId := args[0]

			if nodeSetStatInfo, err = client.AdminAPI().GetNodeSet(nodeSetId); err != nil {
				return
			}
			stdout("%v", formatNodeSetView(nodeSetStatInfo))
		},
	}
	return cmd
}

func newNodeSetUpdateCmd(client *master.MasterClient) *cobra.Command {
	dataNodeSelector := ""
	metaNodeSelector := ""
	cmd := &cobra.Command{
		Use:   CliOpUpdate,
		Short: cmdUpdateNodeSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()

			nodeSetId := args[0]
			if err = client.AdminAPI().UpdateNodeSet(nodeSetId, dataNodeSelector, metaNodeSelector); err != nil {
				return
			}
			stdout("success to update nodeset %v\n", nodeSetId)
		},
	}
	cmd.Flags().StringVar(&dataNodeSelector, "dataNodeSelector", "", "Set the node select policy(datanode) for specify nodeset")
	cmd.Flags().StringVar(&metaNodeSelector, "metaNodeSelector", "", "Set the node select policy(metanode) for specify nodeset")
	return cmd
}
