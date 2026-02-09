// Copyright 2026 The CubeFS Authors.
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

package clustermgr

import (
	"context"
	"errors"
	"net"
	"sort"
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util"
)

func addCmdNode(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "node",
		Help:     "node tools",
		LongHelp: "node tools for clustermgr",
	}
	cmd.AddCommand(command)

	// suggest node migration for EC N+margin safety
	command.AddCommand(&grumble.Command{
		Name: "migrate",
		Help: "suggest which nodes can be safely migrated together (EC N+margin alive)",
		LongHelp: `Suggest which nodes can be safely migrated together.

Node selection (priority: --nodes > --idc + --rack > auto-detect all):
  - --nodes: analyze specific nodes
  - --idc: analyze all nodes in the specified IDC (optional)
  - --rack: analyze all nodes in the specified Rack (optional)
  - neither: auto-detect all nodes in cluster

If output nodes match input nodes exactly, it means all input nodes can be migrated safely.

The --margin flag controls the safety redundancy level:
  - margin=0: require exactly N chunks (minimum for data recovery, risky)
  - margin=1: require N+1 chunks (default, one chunk redundancy)
  - margin=2: require N+2 chunks (more conservative)

Examples:
  cm disk migrate                           # auto-suggest from all nodes
  cm disk migrate --idc "idc1"              # analyze all nodes in idc1
  cm disk migrate --nodes "node1,node2"     # check if node1,node2 can migrate together
  cm disk migrate --max 3                   # suggest at most 3 nodes
  cm disk migrate --margin 0                # allow minimum N chunks (risky)
  cm disk migrate --margin 2                # require N+2 chunks (conservative)`,
		Run: cmdMigrate,
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
			f.IntL("max", 0, "max number of nodes to migrate together (0=find maximum)")
			f.StringL("nodes", "", "comma-separated node hosts to analyze, ip -> http://ip:8889")
			f.StringL("idc", "", "analyze all nodes in the specified IDC")
			f.StringL("rack", "", "analyze all nodes in the specified Rack")
			f.IntL("margin", 1, "safety margin above N (0=exact N, 1=N+1, 2=N+2, etc.)")
			f.BoolL("move_task", false, "auto create migrate tasks for blocking volume")
			f.BoolL("force", false, "auto create migrate tasks above 1000")
		},
	})
}

// volumeChunkInfo stores the chunk distribution info for a volume
type volumeChunkInfo struct {
	Vid          proto.Vid
	CodeMode     string
	N            int
	TotalChunks  int
	Tolerance    int            // max chunks that can be lost = Total - (N+margin)
	Margin       int            // safety margin (default 1)
	NodeChunkCnt map[string]int // host -> chunk count on this host
}

var zone_line string = common.Optimal.Sprintf("+ %s- +", strings.Repeat("- ", 30))

// nodeInfo stores node information with rack
type nodeInfo struct {
	Host string
	Rack string
	Idc  string
}

func cmdMigrate(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)
	verbose := config.Verbose() || flags.Verbose(c.Flags)
	maxNodes := c.Flags.Int("max")
	nodesArg := c.Flags.String("nodes")
	idcArg := c.Flags.String("idc")
	rackArg := c.Flags.String("rack")
	margin := c.Flags.Int("margin")
	moveTask := c.Flags.Bool("move_task")
	force := c.Flags.Bool("force")

	if margin == 0 {
		fmt.Println("WARNING: margin=0 means only N chunks required (minimum for recovery, no redundancy)")
	} else if margin < 0 {
		return errors.New("margin cannot be negative")
	}
	fmt.Printf("Safety requirement: N+%d chunks surviving\n", margin)

	// Step 1: Get all disks and build node info
	nodeDiskIDs := make(map[string][]proto.DiskID) // host -> disk IDs
	nodeInfoMap := make(map[string]*nodeInfo)      // host -> nodeInfo (with rack)

	var inputNodes []string
	if nodesArg != "" {
		for _, host := range strings.Split(nodesArg, ",") {
			host = strings.TrimSpace(host)
			scheme := "http://"
			if strings.HasPrefix(host, "https://") {
				scheme = "https://"
			}
			host = strings.TrimPrefix(strings.TrimPrefix(host, "http://"), "https://")
			if !strings.Contains(host, ":") {
				host += ":8889"
			}

			ipStr, portStr, err := net.SplitHostPort(host)
			if err != nil {
				return err
			}
			if net.ParseIP(ipStr) == nil {
				return fmt.Errorf("invalid ip %s", ipStr)
			}
			var port int
			if err = util.String2Any(portStr, &port); err != nil {
				return err
			}
			if port < 1 || port > 65535 {
				return fmt.Errorf("invalid port %s", portStr)
			}

			inputNodes = append(inputNodes, scheme+host)
		}
	}

	fmt.Println(zone_line)

	if len(inputNodes) > 0 {
		fmt.Printf("Analyzing %d specified nodes...\n", len(inputNodes))
		for _, host := range inputNodes {
			listOptionArgs := &clustermgr.ListOptionArgs{
				Host:   host,
				Marker: proto.DiskID(0),
			}
			for {
				disksOneQuery, err := cmClient.ListDisk(ctx, listOptionArgs)
				if err != nil {
					return fmt.Errorf("list disks for host %s failed: %w", host, err)
				}
				for _, disk := range disksOneQuery.Disks {
					nodeDiskIDs[host] = append(nodeDiskIDs[host], disk.DiskID)
					if nodeInfoMap[host] == nil {
						nodeInfoMap[host] = &nodeInfo{
							Host: host,
							Rack: disk.Rack,
							Idc:  disk.Idc,
						}
					}
				}
				listOptionArgs.Marker = disksOneQuery.Marker
				if listOptionArgs.Marker <= proto.InvalidDiskID {
					break
				}
			}
		}
	} else {
		fmt.Println("Fetching all nodes from cluster...")
		listOptionArgs := &clustermgr.ListOptionArgs{
			Idc:    idcArg,
			Rack:   rackArg,
			Marker: 0,
			Count:  100,
		}
		for {
			disksOneQuery, err := cmClient.ListDisk(ctx, listOptionArgs)
			if err != nil {
				return fmt.Errorf("list all disks failed: %w", err)
			}
			for _, disk := range disksOneQuery.Disks {
				if disk.Status >= proto.DiskStatusRepaired {
					continue
				}
				host := disk.Host
				nodeDiskIDs[host] = append(nodeDiskIDs[host], disk.DiskID)
				if nodeInfoMap[host] == nil {
					nodeInfoMap[host] = &nodeInfo{
						Host: host,
						Rack: disk.Rack,
						Idc:  disk.Idc,
					}
				}
			}
			listOptionArgs.Marker = disksOneQuery.Marker
			if listOptionArgs.Marker <= proto.InvalidDiskID || len(disksOneQuery.Disks) == 0 {
				break
			}
		}
	}
	if len(nodeDiskIDs) == 0 {
		return errors.New("no nodes found")
	}

	// Build sorted host list by Rack (same rack nodes are grouped together)
	allHosts := make([]string, 0, len(nodeDiskIDs))
	for host := range nodeDiskIDs {
		allHosts = append(allHosts, host)
	}
	// Sort by IDC, then Rack, then Host
	sort.Slice(allHosts, func(i, j int) bool {
		ni, nj := nodeInfoMap[allHosts[i]], nodeInfoMap[allHosts[j]]
		if ni.Idc != nj.Idc {
			return ni.Idc < nj.Idc
		}
		if ni.Rack != nj.Rack {
			return ni.Rack < nj.Rack
		}
		return ni.Host < nj.Host
	})

	fmt.Printf("Analyzing %d nodes for migration...\n", len(allHosts))
	if verbose {
		fmt.Println("Nodes by Rack:")
		currentRack := ""
		for _, host := range allHosts {
			ni := nodeInfoMap[host]
			rackKey := ni.Idc + "/" + ni.Rack
			if rackKey != currentRack {
				currentRack = rackKey
				fmt.Printf("  [%s]\n", rackKey)
			}
			fmt.Printf("    - %s\n", host)
		}
	}
	fmt.Println()

	totalDisks := 0
	for _, diskIDs := range nodeDiskIDs {
		totalDisks += len(diskIDs)
	}
	if verbose {
		fmt.Printf("Found %d disks across %d nodes\n", totalDisks, len(nodeDiskIDs))
	}

	// Build diskID -> host mapping for quick lookup
	diskToHost := make(map[proto.DiskID]string)
	for host, diskIDs := range nodeDiskIDs {
		for _, diskID := range diskIDs {
			diskToHost[diskID] = host
		}
	}

	// Step 2: Get volume info using ListVolume
	fmt.Println(zone_line)
	fmt.Println("Fetching volume information...")
	allVolumes := make(map[proto.Vid]*clustermgr.VolumeInfo)
	listVolumeArgs := &clustermgr.ListVolumeArgs{
		Marker: proto.Vid(0),
		Count:  10000,
	}
	for {
		volumes, err := cmClient.ListVolume(ctx, listVolumeArgs)
		if err != nil {
			return fmt.Errorf("list volumes failed: %w", err)
		}
		for _, vol := range volumes.Volumes {
			allVolumes[vol.Vid] = vol
		}
		if verbose && len(volumes.Volumes) > 0 {
			fmt.Printf("  Fetched %5d volumes (total: %8d)\n", len(volumes.Volumes), len(allVolumes))
		}
		listVolumeArgs.Marker = volumes.Marker
		if volumes.Marker <= proto.InvalidVid || len(volumes.Volumes) == 0 {
			break
		}
	}

	// Step 3: Parse volume units to find affected volumes and build chunk distribution
	fmt.Println(zone_line)
	fmt.Println("Analyzing volume distribution...")
	volumeInfos := make(map[proto.Vid]*volumeChunkInfo)
	for vid, volInfo := range allVolumes {
		nodeChunkCnt := make(map[string]int)
		hasTargetDisk := false
		for _, unit := range volInfo.Units {
			if host, ok := diskToHost[unit.DiskID]; ok {
				nodeChunkCnt[host]++
				hasTargetDisk = true
			}
		}
		if !hasTargetDisk {
			continue
		}

		tactic := volInfo.CodeMode.Tactic()
		totalChunks := len(volInfo.Units)
		required := tactic.N + margin
		tolerance := totalChunks - required

		volumeInfos[vid] = &volumeChunkInfo{
			Vid:          vid,
			CodeMode:     volInfo.CodeMode.String(),
			N:            tactic.N,
			TotalChunks:  totalChunks,
			Tolerance:    tolerance,
			Margin:       margin,
			NodeChunkCnt: nodeChunkCnt,
		}
	}
	if verbose {
		fmt.Printf("Found %s volumes with chunks on target nodes\n", common.Normal.Sprint(len(volumeInfos)))
	}

	// Step 4: Greedy algorithm to find maximum safe migration set
	fmt.Println(zone_line)
	fmt.Println("Greedy add node to find maximum...")
	canMigrate := func(selectedHosts map[string]bool) bool {
		for vid, info := range volumeInfos {
			lostChunks := 0
			for host := range selectedHosts {
				lostChunks += info.NodeChunkCnt[host]
			}
			if lostChunks > info.Tolerance {
				if verbose {
					fmt.Printf("  Volume %8d would lose %2d chunks (tolerance=%2d)\n", vid, lostChunks, info.Tolerance)
				}
				return false
			}
		}
		return true
	}

	type hostChunkCount struct {
		host       string
		rack       string
		idc        string
		chunkCount int
	}
	hostCounts := make([]hostChunkCount, 0, len(allHosts))
	for _, host := range allHosts {
		count := 0
		for _, info := range volumeInfos {
			count += info.NodeChunkCnt[host]
		}
		ni := nodeInfoMap[host]
		hostCounts = append(hostCounts, hostChunkCount{
			host:       host,
			rack:       ni.Rack,
			idc:        ni.Idc,
			chunkCount: count,
		})
	}
	// Sort by chunk count ascending (prefer nodes with fewer chunks)
	sort.Slice(hostCounts, func(i, j int) bool {
		return hostCounts[i].chunkCount < hostCounts[j].chunkCount
	})

	// Group hosts by rack
	rackHosts := make(map[string][]hostChunkCount) // rack -> hosts in this rack
	for _, hc := range hostCounts {
		rackKey := hc.idc + "/" + hc.rack
		rackHosts[rackKey] = append(rackHosts[rackKey], hc)
	}

	// Sort racks by number of hosts (descending) - prefer racks with more hosts
	rackOrder := make([]string, 0, len(rackHosts))
	for rackKey := range rackHosts {
		rackOrder = append(rackOrder, rackKey)
	}
	sort.Slice(rackOrder, func(i, j int) bool {
		return len(rackHosts[rackOrder[i]]) > len(rackHosts[rackOrder[j]])
	})

	// Greedy selection: round-robin across racks to maximize diversity
	selectedHosts := make(map[string]bool)
	selectedRacks := make(map[string]int) // rack -> count of selected nodes from this rack
	migrateOrder := make([]string, 0)

	// Keep selecting until no more can be added
	changed := true
	for changed {
		changed = false
		// Try to pick one node from each rack (round-robin)
		for _, rackKey := range rackOrder {
			if maxNodes > 0 && len(selectedHosts) >= maxNodes {
				break
			}
			hosts := rackHosts[rackKey]
			// Find the next unselected host in this rack
			for _, hc := range hosts {
				if selectedHosts[hc.host] {
					continue
				}
				// Try adding this host
				selectedHosts[hc.host] = true
				if canMigrate(selectedHosts) {
					migrateOrder = append(migrateOrder, hc.host)
					selectedRacks[rackKey]++
					changed = true
					break
				} else {
					delete(selectedHosts, hc.host)
					break
				}
			}
		}
	}

	// Step 5: Output results
	fmt.Println()
	fmt.Println(zone_line)
	fmt.Printf("=== Migration Result ===\n")
	fmt.Printf("Nodes analyzed      : %s\n", common.Normal.Sprint(len(allHosts)))
	fmt.Printf("Volumes affected    : %s\n", common.Normal.Sprint(len(volumeInfos)))
	fmt.Printf("Safety margin       : N+%s\n", common.Danger.Sprint(margin))
	fmt.Printf("Safe to migrate     : %s nodes\n", common.Loaded.Sprint(len(selectedHosts)))
	fmt.Println()

	// Check if all input nodes can be migrated (when --nodes was specified)
	if len(inputNodes) > 0 {
		allInputSafe := true
		for _, node := range inputNodes {
			if !selectedHosts[node] {
				allInputSafe = false
				break
			}
		}
		if allInputSafe && len(inputNodes) == len(selectedHosts) {
			fmt.Println("✓ CHECK OK! All specified nodes can be migrated together.")
		} else {
			fmt.Println("✗ CHECK FAILED! Not all specified nodes can be migrated together.")
			fmt.Println()

			blockingVolumes(ctx, cmClient, inputNodes, volumeInfos, allVolumes, diskToHost, moveTask, force)
		}
		fmt.Println()
	}

	if len(selectedHosts) == 0 {
		fmt.Println("✗ No nodes can be safely migrated!")
		fmt.Println("All nodes have critical chunks that would violate the safety requirement.")
		return nil
	}

	// Show nodes that can be migrated
	fmt.Println(zone_line)
	fmt.Println(common.Loaded.Sprint("Nodes that can be migrated together:"))
	fmt.Printf("%-5s %-20s %-30s %-15s\n", "No.", "Rack", "Host", "Chunks")
	fmt.Println(strings.Repeat("-", 75))

	for i, host := range migrateOrder {
		chunkCount := 0
		for _, info := range volumeInfos {
			chunkCount += info.NodeChunkCnt[host]
		}
		ni := nodeInfoMap[host]
		rackKey := ni.Idc + "/" + ni.Rack
		fmt.Printf("%-5d %-20s %-30s %-15d\n", i+1, rackKey, host, chunkCount)
	}

	// Show rack summary
	fmt.Println()
	fmt.Printf("Rack distribution: ")
	rackSummary := make([]string, 0)
	for rack, count := range selectedRacks {
		rackSummary = append(rackSummary, fmt.Sprintf("%s(%d)", rack, count))
	}
	fmt.Println(strings.Join(rackSummary, ", "))

	// Show remaining nodes that cannot be included
	remaining := make([]string, 0)
	for _, host := range allHosts {
		if !selectedHosts[host] {
			remaining = append(remaining, host)
		}
	}

	if len(remaining) > 0 {
		fmt.Println()
		fmt.Println(zone_line)
		fmt.Println(common.Danger.Sprintf("The following %d nodes CANNOT be migrated", len(remaining)))
		for _, host := range remaining {
			chunkCount := 0
			for _, info := range volumeInfos {
				chunkCount += info.NodeChunkCnt[host]
			}
			ni := nodeInfoMap[host]
			fmt.Printf("  - [%s/%s] %s (chunks: %d)\n", ni.Idc, ni.Rack, host, chunkCount)
		}
		fmt.Println()
		fmt.Println("Suggestion: Migrate in batches. After the first batch completes,")
		fmt.Println("run this command again to find the next batch.")
	}

	return nil
}

func blockingVolumes(ctx context.Context,
	cmClient *clustermgr.Client,
	inputNodes []string,
	volumeInfos map[proto.Vid]*volumeChunkInfo,
	allVolumes map[proto.Vid]*clustermgr.VolumeInfo,
	diskToHost map[proto.DiskID]string,
	moveTask, force bool,
) {
	// Build input node set
	inputNodeSet := make(map[string]bool)
	for _, node := range inputNodes {
		inputNodeSet[node] = true
	}

	// Find blocking volumes
	type blockingVol struct {
		info       *volumeChunkInfo
		lostChunks int
	}
	var blockingVolumes []blockingVol

	for _, info := range volumeInfos {
		lostChunks := 0
		for host, cnt := range info.NodeChunkCnt {
			if inputNodeSet[host] {
				lostChunks += cnt
			}
		}
		if lostChunks > info.Tolerance {
			blockingVolumes = append(blockingVolumes, blockingVol{info: info, lostChunks: lostChunks})
		}
	}
	if len(blockingVolumes) == 0 {
		return
	}

	// Sort by severity
	sort.Slice(blockingVolumes, func(i, j int) bool {
		ei := blockingVolumes[i].lostChunks - blockingVolumes[i].info.Tolerance
		ej := blockingVolumes[j].lostChunks - blockingVolumes[j].info.Tolerance
		return ei > ej
	})

	fmt.Println(zone_line)
	fmt.Printf("Blocking volumes: %s (need migrate)\n", common.Warn.Sprint(len(blockingVolumes)))
	fmt.Printf("%-12s %-15s %-6s %-8s %-10s %-8s %-8s\n",
		"Vid", "CodeMode", "N", "Total", "Tolerance", "Lost", "Exceed")
	fmt.Println(strings.Repeat("-", 75))

	showCount := len(blockingVolumes)
	if showCount > 30 {
		showCount = 30
	}
	totalExceed := 0
	for i, bv := range blockingVolumes {
		exceed := bv.lostChunks - bv.info.Tolerance
		totalExceed += exceed
		if i < showCount {
			fmt.Printf("%-12d %-15s %-6d %-8d %-10d %-8d %-8d\n",
				bv.info.Vid, bv.info.CodeMode, bv.info.N, bv.info.TotalChunks,
				bv.info.Tolerance, bv.lostChunks, exceed)
		}
	}
	if len(blockingVolumes) > showCount {
		fmt.Printf("... and %s more volumes\n", common.Warn.Sprint(len(blockingVolumes)-showCount))
	}

	fmt.Println()
	fmt.Printf("Summary: %d blocking volumes, need to pre-migrate at least %s chunks\n",
		len(blockingVolumes), common.Warn.Sprint(totalExceed))

	// Auto create migrate tasks if --move_task is set
	if !moveTask {
		return
	}

	fmt.Println()
	fmt.Println(zone_line)
	fmt.Printf("Preparing migrate tasks for %d blocking volumes...\n", len(blockingVolumes))

	schedulers, err := cmClient.GetService(ctx, clustermgr.GetServiceArgs{Name: proto.ServiceNameScheduler})
	if err != nil || len(schedulers.Nodes) == 0 {
		fmt.Println("List scheduler error:", err)
		return
	}
	clusterID := proto.ClusterID(schedulers.Nodes[0].ClusterID)
	schedulerCli := scheduler.New(&scheduler.Config{}, cmClient, clusterID)

	var vuidsToMigrate []proto.Vuid
	for _, bv := range blockingVolumes {
		volInfo := allVolumes[bv.info.Vid]
		if volInfo == nil {
			continue
		}
		exceed := bv.lostChunks - bv.info.Tolerance
		migrated := 0
		for _, unit := range volInfo.Units {
			if host, ok := diskToHost[unit.DiskID]; ok && inputNodeSet[host] {
				if migrated < exceed {
					vuidsToMigrate = append(vuidsToMigrate, unit.Vuid)
					migrated++
				}
			}
		}
	}

	fmt.Printf("Total VUIDs to migrate: %d\n", len(vuidsToMigrate))
	if len(vuidsToMigrate) > 1000 && !force {
		fmt.Println("too many migrate task (>1000)")
		return
	}
	for range [3]struct{}{} {
		if !common.Confirm(fmt.Sprintf("Create %s migrate tasks?", common.Danger.Sprint(len(vuidsToMigrate)))) {
			fmt.Println("Cancelled.")
			return
		}
	}

	successCnt, failCnt := 0, 0
	for _, vuid := range vuidsToMigrate {
		if err := schedulerCli.AddManualMigrateTask(ctx, &scheduler.AddManualMigrateArgs{
			Vuid:           vuid,
			DirectDownload: true,
		}); err != nil {
			fmt.Printf("  ✗ Failed to create task for VUID %d (vid=%d): %v\n", vuid, vuid.Vid(), err)
			failCnt++
		} else {
			fmt.Printf("  ✓ Created task for VUID %d (vid=%d)\n", vuid, vuid.Vid())
			successCnt++
		}
	}
	fmt.Printf("Migrate tasks created: %s success, %s failed\n",
		common.Loaded.Sprint(successCnt), common.Dead.Sprint(failCnt))
}
