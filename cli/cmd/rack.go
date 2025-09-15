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
	"fmt"
	"sort"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdRackUse   = "rack [COMMAND]"
	cmdRackShort = "Manage rack information"
)

// RackInfo represents aggregated rack information
type RackInfo struct {
	Name              string
	DataNodeCount     int
	DataWritableCount int
	MetaNodeCount     int
	MetaWritableCount int
	ZoneCount         int
	NodeSetCount      int
	DataNodes         []*proto.NodeView
	MetaNodes         []*proto.NodeView
}

func newRackCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdRackUse,
		Short: cmdRackShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newRackListCmd(client),
		newRackInfoCmd(client),
	)
	return cmd
}

const (
	cmdRackListShort = "List all racks in the cluster"
	cmdRackInfoShort = "Show detailed rack information"
)

func newRackListCmd(client *master.MasterClient) *cobra.Command {
	var zoneName string
	cmd := &cobra.Command{
		Use:   CliOpList,
		Short: cmdRackListShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()

			// Get all data nodes and meta nodes
			var dataNodes []proto.NodeView
			var metaNodes []proto.NodeView

			if dataNodes, err = client.AdminAPI().GetClusterDataNodes(); err != nil {
				return
			}
			if metaNodes, err = client.AdminAPI().GetClusterMetaNodes(); err != nil {
				return
			}

			// Filter by zone if specified
			if zoneName != "" {
				dataNodes = filterNodesByZone(dataNodes, zoneName)
				metaNodes = filterNodesByZone(metaNodes, zoneName)
			}

			// Aggregate rack information
			rackMap := make(map[string]*RackInfo)

			// Process data nodes
			for i, node := range dataNodes {
				rackName := node.Rack
				if rackName == "" {
					rackName = "default"
				}

				if rackMap[rackName] == nil {
					rackMap[rackName] = &RackInfo{
						Name:      rackName,
						DataNodes: make([]*proto.NodeView, 0),
						MetaNodes: make([]*proto.NodeView, 0),
					}
				}

				// Use pointer to the original node to avoid copy issues
				rackMap[rackName].DataNodes = append(rackMap[rackName].DataNodes, &dataNodes[i])
				rackMap[rackName].DataNodeCount++
				if node.IsWritable {
					rackMap[rackName].DataWritableCount++
				}
			}

			// Process meta nodes
			for i, node := range metaNodes {
				rackName := node.Rack
				if rackName == "" {
					rackName = "default"
				}

				if rackMap[rackName] == nil {
					rackMap[rackName] = &RackInfo{
						Name:      rackName,
						DataNodes: make([]*proto.NodeView, 0),
						MetaNodes: make([]*proto.NodeView, 0),
					}
				}

				// Use pointer to the original node to avoid copy issues
				rackMap[rackName].MetaNodes = append(rackMap[rackName].MetaNodes, &metaNodes[i])
				rackMap[rackName].MetaNodeCount++
				if node.IsWritable {
					rackMap[rackName].MetaWritableCount++
				}
			}

			// Calculate zone count and nodeset count for each rack
			for _, rack := range rackMap {
				zoneSet := make(map[string]bool)
				nodeSetSet := make(map[uint64]bool)

				// Count unique zones and nodesets from data nodes
				for _, node := range rack.DataNodes {
					if node.ZoneName != "" {
						zoneSet[node.ZoneName] = true
					}
					if node.NodeSetID != 0 {
						nodeSetSet[node.NodeSetID] = true
					}
				}

				// Count unique zones and nodesets from meta nodes
				for _, node := range rack.MetaNodes {
					if node.ZoneName != "" {
						zoneSet[node.ZoneName] = true
					}
					if node.NodeSetID != 0 {
						nodeSetSet[node.NodeSetID] = true
					}
				}

				rack.ZoneCount = len(zoneSet)
				rack.NodeSetCount = len(nodeSetSet)
			}

			// Convert map to slice and sort by rack name
			racks := make([]*RackInfo, 0, len(rackMap))
			for _, rack := range rackMap {
				racks = append(racks, rack)
			}
			sort.Slice(racks, func(i, j int) bool {
				return racks[i].Name < racks[j].Name
			})

			// Display rack list with center alignment
			rackTablePattern := "%12v %10v %12v %10v %10v %10v %12v\n"
			stdout(rackTablePattern, "RACK", "DATA_NUM", "DATA_WRITABLE", "META_NUM", "META_WRITABLE", "ZONE_CNT", "NODESET_CNT")
			for _, rack := range racks {
				stdout(rackTablePattern, rack.Name, rack.DataNodeCount, rack.DataWritableCount,
					rack.MetaNodeCount, rack.MetaWritableCount, rack.ZoneCount, rack.NodeSetCount)
			}
		},
	}

	cmd.Flags().StringVar(&zoneName, CliFlagZoneName, "", "List racks in the specified zone")
	return cmd
}

func newRackInfoCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdRackInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()

			rackName := args[0]

			// Get all data nodes and meta nodes
			var dataNodes []proto.NodeView
			var metaNodes []proto.NodeView

			if dataNodes, err = client.AdminAPI().GetClusterDataNodes(); err != nil {
				return
			}
			if metaNodes, err = client.AdminAPI().GetClusterMetaNodes(); err != nil {
				return
			}

			// Filter nodes by rack
			rackDataNodes := filterNodesByRack(dataNodes, rackName)
			rackMetaNodes := filterNodesByRack(metaNodes, rackName)

			if len(rackDataNodes) == 0 && len(rackMetaNodes) == 0 {
				err = fmt.Errorf("rack %s not found", rackName)
				return
			}

			// Display rack information
			stdout("%v", formatRackView(rackName, rackDataNodes, rackMetaNodes))
		},
	}
	return cmd
}

// filterNodesByZone filters nodes by zone name
func filterNodesByZone(nodes []proto.NodeView, zoneName string) []proto.NodeView {
	var filtered []proto.NodeView
	for _, node := range nodes {
		if node.ZoneName == zoneName {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

// filterNodesByRack filters nodes by rack name
func filterNodesByRack(nodes []proto.NodeView, rackName string) []proto.NodeView {
	var filtered []proto.NodeView
	for _, node := range nodes {
		nodeRack := node.Rack
		if nodeRack == "" {
			nodeRack = "default"
		}
		if nodeRack == rackName {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

// formatRackView formats rack information for display
func formatRackView(rackName string, dataNodes []proto.NodeView, metaNodes []proto.NodeView) string {
	sb := strings.Builder{}

	// Calculate statistics
	dataWritableCount := 0
	metaWritableCount := 0
	zoneSet := make(map[string]bool)
	nodeSetSet := make(map[uint64]bool)

	sort.Slice(dataNodes, func(i, j int) bool {
		return dataNodes[i].ID < dataNodes[j].ID
	})
	sort.Slice(metaNodes, func(i, j int) bool {
		return metaNodes[i].ID < metaNodes[j].ID
	})

	for _, node := range dataNodes {
		if node.IsWritable {
			dataWritableCount++
		}
		if node.ZoneName != "" {
			zoneSet[node.ZoneName] = true
		}
		if node.NodeSetID != 0 {
			nodeSetSet[node.NodeSetID] = true
		}
	}

	for _, node := range metaNodes {
		if node.IsWritable {
			metaWritableCount++
		}
		if node.ZoneName != "" {
			zoneSet[node.ZoneName] = true
		}
		if node.NodeSetID != 0 {
			nodeSetSet[node.NodeSetID] = true
		}
	}

	sb.WriteString(fmt.Sprintf("Rack Name:        %v\n", rackName))
	sb.WriteString(fmt.Sprintf("Data Node Count:  %v (Writable: %v)\n", len(dataNodes), dataWritableCount))
	sb.WriteString(fmt.Sprintf("Meta Node Count:  %v (Writable: %v)\n", len(metaNodes), metaWritableCount))
	sb.WriteString(fmt.Sprintf("Zone Count:       %v\n", len(zoneSet)))
	sb.WriteString(fmt.Sprintf("NodeSet Count:    %v\n", len(nodeSetSet)))
	sb.WriteString("\n")

	// Display data nodes
	if len(dataNodes) > 0 {
		sb.WriteString(fmt.Sprintf("DataNodes[%v]:\n", len(dataNodes)))
		sb.WriteString(fmt.Sprintf("  %v\n", formatRackNodeTableHeader()))
		for _, node := range dataNodes {
			sb.WriteString(fmt.Sprintf("  %v\n", formatRackNodeView(&node)))
		}
		sb.WriteString("\n")
	}

	// Display meta nodes
	if len(metaNodes) > 0 {
		sb.WriteString(fmt.Sprintf("MetaNodes[%v]:\n", len(metaNodes)))
		sb.WriteString(fmt.Sprintf("  %v\n", formatRackNodeTableHeader()))
		for _, node := range metaNodes {
			sb.WriteString(fmt.Sprintf("  %v\n", formatRackNodeView(&node)))
		}
	}

	return sb.String()
}

// formatRackNodeTableHeader returns the table header for rack node display
func formatRackNodeTableHeader() string {
	return fmt.Sprintf(rackNodeTableRowPattern, "ID", "ADDRESS", "WRITABLE", "STATUS", "ROCKSDB_WRITABLE", "ZONE", "NODESET_ID", "RACK")
}

// formatRackNodeView formats a single node for rack display
func formatRackNodeView(node *proto.NodeView) string {
	rackName := node.Rack
	if rackName == "" {
		rackName = "default"
	}
	zoneName := node.ZoneName
	if zoneName == "" {
		zoneName = "default"
	}
	return fmt.Sprintf(rackNodeTableRowPattern, node.ID, formatAddr(node.Addr, node.DomainAddr),
		formatYesNo(node.IsWritable), formatNodeStatus(node.Status),
		formatYesNo(node.IsRocksdbWritable), zoneName, node.NodeSetID, rackName)
}

// Table pattern for rack node display
var rackNodeTableRowPattern = "%-6v    %-65v    %-8v    %-8v    %-15v    %-10v    %-10v    %-10v"
