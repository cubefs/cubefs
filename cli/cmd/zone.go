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
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdZoneUse   = "zone [COMMAND]"
	cmdZoneShort = "Manage zone"
)

func newZoneCmd(client *sdk.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdZoneUse,
		Short: cmdZoneShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newZoneListCmd(client),
		newZoneInfoCmd(client),
		newZoneUpdateCmd(client),
	)
	return cmd
}

const (
	cmdZoneListShort   = "List cluster zones"
	cmdZoneInfoShort   = "Show zone information"
	cmdZoneUpdateShort = "Update zone settings"
)

func newZoneListCmd(client *sdk.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     CliOpList,
		Short:   cmdZoneListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var zones []*proto.ZoneView
			var err error
			defer func() {
				errout(err)
			}()
			if zones, err = client.AdminAPI().ListZones(); err != nil {
				return
			}
			stdoutln("[Zones]")
			zoneTablePattern := "%-12v    %-10v    %-15v    %-20v    %-20v    %-30v    %-12v\n"
			stdout(zoneTablePattern, "ZONE", "STATUS", "DATA_NODESET_SEL", "META_NODESET_SEL", "DATA_MEDIA_TYPE", "POOL", "META_REGION")
			for _, zone := range zones {
				dataNodesetSel := zone.DataNodesetSelector
				if dataNodesetSel == "" {
					dataNodesetSel = "-"
				}
				metaNodesetSel := zone.MetaNodesetSelector
				if metaNodesetSel == "" {
					metaNodesetSel = "-"
				}
				dataMediaType := zone.DataMediaType
				if dataMediaType == "" {
					dataMediaType = "-"
				}
				// Use zone's PoolId and PoolName directly
				poolInfoStr := "-"
				if zone.PoolId > 0 {
					poolInfoStr = fmt.Sprintf("%d", zone.PoolId)
					if zone.PoolName != "" {
						poolInfoStr = fmt.Sprintf("%d(%s)", zone.PoolId, zone.PoolName)
					}
				}
				// Get meta region, default to "default" if empty
				metaRegion := zone.Region
				if metaRegion == "" {
					metaRegion = "default"
				}
				stdout(zoneTablePattern, zone.Name, zone.Status, dataNodesetSel, metaNodesetSel, dataMediaType, poolInfoStr, metaRegion)
			}
		},
	}
	return cmd
}

func newZoneInfoCmd(client *sdk.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [NAME]",
		Short: cmdZoneInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var topo *proto.TopologyView
			var (
				err      error
				zoneName string
				zoneView *proto.ZoneView
			)
			defer func() {
				errout(err)
			}()
			zoneName = args[0]
			if topo, err = client.AdminAPI().Topo(); err != nil {
				return
			}

			for _, zone := range topo.Zones {
				if zoneName == zone.Name {
					zoneView = zone
				}
			}
			if zoneView == nil {
				err = fmt.Errorf("Zone[%v] not exists in cluster\n ", zoneName)
				return
			}
			stdout("%v", formatZoneView(zoneView))
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validZones(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newZoneUpdateCmd(client *sdk.MasterClient) *cobra.Command {
	enable := true
	dataNodesetSelector := ""
	metaNodesetSelector := ""
	dataNodeSelector := ""
	metaNodeSelector := ""
	cmd := &cobra.Command{
		Use:   CliOpUpdate + " [NAME]",
		Short: cmdZoneUpdateShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			zoneName := args[0]
			if err = client.AdminAPI().UpdateZone(zoneName, enable, dataNodesetSelector, metaNodesetSelector, dataNodeSelector, metaNodeSelector); err != nil {
				return
			}
			stdout(fmt.Sprintf("Zone %v has been update successfully!\n", zoneName))
		},
	}
	cmd.Flags().BoolVar(&enable, "enable", true, "Enable of disable specify zone")
	cmd.Flags().StringVar(&dataNodesetSelector, CliFlagDataNodesetSelector, "", "Set the nodeset select policy(datanode) for specify zone")
	cmd.Flags().StringVar(&metaNodesetSelector, CliFlagMetaNodesetSelector, "", "Set the nodeset select policy(metanode) for specify zone")
	cmd.Flags().StringVar(&dataNodeSelector, CliFlagDataNodeSelector, "", "Set the node select policy(datanode) for specify zone")
	cmd.Flags().StringVar(&metaNodeSelector, CliFlagMetaNodeSelector, "", "Set the node select policy(metanode) for specify zone")
	return cmd
}
