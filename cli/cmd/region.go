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
	"sort"
	"strings"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdRegionUse   = "region [COMMAND]"
	cmdRegionShort = "Manage regions"
)

func newRegionCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdRegionUse,
		Short: cmdRegionShort,
	}
	cmd.AddCommand(
		newRegionListCmd(client),
		newRegionInfoCmd(client),
	)
	return cmd
}

const (
	cmdRegionListShort = "List all regions"
	cmdRegionInfoShort = "Show region information"
)

func newRegionListCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     CliOpList,
		Short:   cmdRegionListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var regionViews []*proto.RegionView
			if regionViews, err = client.AdminAPI().ListRegions(); err != nil {
				return
			}
			stdout("[Regions]\n")
			regionTablePattern := "%-15v    %-10v\n"
			stdout(regionTablePattern, "REGION", "META COUNT")
			for _, region := range regionViews {
				stdout(regionTablePattern, region.Name, region.MetaCount)
			}
		},
	}
	return cmd
}

func newRegionInfoCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [REGION_NAME]",
		Short: cmdRegionInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			regionName := args[0]
			var regionView *proto.RegionView
			if regionView, err = client.AdminAPI().GetRegionInfo(regionName); err != nil {
				return
			}
			stdout("%v", formatRegionView(regionView))
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validRegions(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func formatRegionView(rv *proto.RegionView) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("[Region: %v]\n", rv.Name))
	sb.WriteString(fmt.Sprintf("  Meta Node Count: %v\n", rv.MetaCount))
	sb.WriteString("\n")

	// Sort zones by name
	sort.Slice(rv.MetaNodes, func(i, j int) bool {
		return rv.MetaNodes[i].ZoneName < rv.MetaNodes[j].ZoneName
	})

	for _, zoneView := range rv.MetaNodes {
		sb.WriteString(fmt.Sprintf("  [Zone: %v]\n", zoneView.ZoneName))
		sb.WriteString(fmt.Sprintf("    Meta Node Count: %v\n", len(zoneView.MetaNodes)))
		if len(zoneView.MetaNodes) > 0 {
			sb.WriteString(fmt.Sprintf("    %v\n", formatMetaNodeViewTableHeader()))
			// Sort nodes by ID
			sort.Slice(zoneView.MetaNodes, func(i, j int) bool {
				return zoneView.MetaNodes[i].ID < zoneView.MetaNodes[j].ID
			})
			for _, node := range zoneView.MetaNodes {
				sb.WriteString(fmt.Sprintf("    %v\n", formatMetaNodeView(&node, true)))
			}
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func validRegions(client *master.MasterClient, toComplete string) []string {
	regions, err := client.AdminAPI().ListRegions()
	if err != nil {
		return nil
	}
	var result []string
	for _, region := range regions {
		if strings.HasPrefix(region.Name, toComplete) {
			result = append(result, region.Name)
		}
	}
	return result
}
