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
	"github.com/cubefs/cubefs/proto"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdZoneUse   = "zone [COMMAND]"
	cmdZoneShort = "Manage zone"
)

func newZoneCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdZoneUse,
		Short: cmdZoneShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newZoneListCmd(client),
		newZoneInfoCmd(client),
	)
	return cmd
}

const (
	cmdZoneListShort = "List cluster zones"
	cmdZoneInfoShort = "Show zone information"
)

func newZoneListCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdZoneListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var zones []*proto.ZoneView
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if zones, err = client.AdminAPI().ListZones(); err != nil {
				return
			}
			zoneTablePattern := "%-8v    %-10v\n"
			stdout(zoneTablePattern, "ZONE", "STATUS")
			for _, zone := range zones {
				stdout(zoneTablePattern, zone.Name, zone.Status)
			}
			return
		},
	}
	return cmd
}

func newZoneInfoCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
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
				if err != nil {
					errout("Error: %v", err)
				}
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
			stdout(formatZoneView(zoneView))
			return
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
