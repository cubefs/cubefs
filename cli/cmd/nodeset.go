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
	"github.com/cubefs/cubefs/proto"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/spf13/cobra"
)

const (
	cmdNodeSetUse   = "nodeset [COMMAND]"
	cmdNodeSetShort = "Manage nodeset"
)

func newNodeSetCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdNodeSetUse,
		Short: cmdNodeSetShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newNodeSetListCmd(client),
	)
	return cmd
}

const (
	cmdNodeSetListShort = "List cluster nodeSets"
)

func newNodeSetListCmd(client *sdk.MasterClient) *cobra.Command {
	var zoneName string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdNodeSetListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var nodeSetStats []*proto.NodeSetStat
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			if nodeSetStats, err = client.AdminAPI().ListNodeSets(zoneName); err != nil {
				return
			}
			zoneTablePattern := "%-4v %-4v %-10v %-8v %-8v %12v %12v %12v %12v %12v %12v\n"
			stdout(zoneTablePattern, "ID", "Cap", "Zone", "MetaNum", "DataNum", "MetaTotal", "MetaUsed", "MetaAvail", "DataTotal", "DataUsed", "DataAvail")
			zoneDataPattern := "%-4v %-4v %-10v %-8v %-8v %10.2fGB %10.2fGB %10.2fGB %10.2fGB %10.2fGB %10.2fGB\n"
			for _, nodeSet := range nodeSetStats {
				stdout(zoneDataPattern, nodeSet.ID, nodeSet.Capacity, nodeSet.Zone, nodeSet.MetaNodeNum, nodeSet.DataNodeNum,
					float64(nodeSet.MetaTotal)/float64(util.GB),
					float64(nodeSet.MetaUsed)/float64(util.GB),
					float64(nodeSet.MetaAvail)/float64(util.GB),
					float64(nodeSet.DataTotal)/float64(util.GB),
					float64(nodeSet.DataUsed)/float64(util.GB),
					float64(nodeSet.DataAvail)/float64(util.GB),
				)
			}
			return
		},
	}

	cmd.Flags().StringVar(&zoneName, CliFlagZoneName, "", "List nodeSets in the specified zone")

	return cmd
}
