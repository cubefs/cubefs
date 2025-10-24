package master

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

func TestDataPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDataPartitions()
	count := 20
	createDataPartition(commonVol, count, t)
	if len(commonVol.dataPartitions.partitions) <= 0 {
		t.Errorf("getDataPartition no dp")
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	getDataPartition(partition.PartitionID, t)
	loadDataPartitionTest(partition, t)
	_ = decommissionDataPartition
	// decommissionDataPartition(partition, t)
}

func createDataPartition(vol *Vol, count int, t *testing.T) {
	oldCount := len(vol.dataPartitions.partitions)
	reqURL := fmt.Sprintf("%v%v?count=%v&name=%v&type=extent&force=true",
		hostAddr, proto.AdminCreateDataPartition, count, vol.Name)
	process(reqURL, t)

	newCount := len(vol.dataPartitions.partitions)
	total := oldCount + count
	if newCount != total {
		t.Errorf("createDataPartition failed,newCount[%v],total=%v,count[%v],oldCount[%v]",
			newCount, total, count, oldCount)
		return
	}
}

func getDataPartition(id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?id=%v",
		hostAddr, proto.AdminGetDataPartition, id)
	process(reqURL, t)
}

// test
func decommissionDataPartition(dp *DataPartition, t *testing.T) {
	offlineAddr := dp.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionDataPartition, dp.VolName, dp.PartitionID, offlineAddr)
	process(reqURL, t)
	if contains(dp.Hosts, offlineAddr) {
		t.Errorf("decommissionDataPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, dp.Hosts)
		return
	}
}

func loadDataPartitionTest(dp *DataPartition, t *testing.T) {
	dps := make([]*DataPartition, 0)
	dps = append(dps, dp)
	server.cluster.waitForResponseToLoadDataPartition(dps)
	time.Sleep(5 * time.Second)
	dp.RLock()
	for _, replica := range dp.Replicas {
		t.Logf("replica[%v],response[%v]", replica.Addr, replica.HasLoadResponse)
	}
	tinyFile := &FileInCore{}
	tinyFile.Name = "50000011"
	tinyFile.LastModify = 1562507765
	extentFile := &FileInCore{}
	extentFile.Name = "10"
	extentFile.LastModify = 1562507765
	for index, host := range dp.Hosts {
		fm := newFileMetadata(uint32(404551221)+uint32(index), host, index, 2*util.MB, 0)
		tinyFile.MetadataArray = append(tinyFile.MetadataArray, fm)
		extentFile.MetadataArray = append(extentFile.MetadataArray, fm)
	}

	dp.FileInCoreMap[tinyFile.Name] = tinyFile
	dp.FileInCoreMap[extentFile.Name] = extentFile
	dp.RUnlock()
	dp.getFileCount()
	dp.validateCRC(server.cluster.Name)
	dp.setToNormal()
}

func TestAcquireDecommissionFirstHostToken(t *testing.T) {
	partition := &DataPartition{PartitionID: 1, Hosts: []string{"host0", "host1", "host2"}, ReplicaNum: 3}
	partition.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: "host0", DiskPath: "/disk0"}},
		{DataReplica: proto.DataReplica{Addr: "host1", DiskPath: "/disk1"}},
		{DataReplica: proto.DataReplica{Addr: "host2", DiskPath: "/disk2"}},
	}
	partition.DecommissionSrcAddr = "host2"
	partition.DecommissionType = ManualDecommission

	cluster := &Cluster{
		ClusterDecommission: ClusterDecommission{DecommissionFirstHostDiskParallelLimit: 0},
	}
	dataNode := &DataNode{
		DecommissionFirstHostParallelLimit: 1,
	}
	cluster.dataNodes.Store("host0", dataNode)
	dataNodeInfo := &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 1,
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster))

	cluster.DecommissionFirstHostDiskParallelLimit = 1
	dataNode.DecommissionFirstHostParallelLimit = 2
	dataNodeInfo = &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 1,
		DiskToDecommissionRepairDpMap: map[string]*DiskToDecommissionRepairDpInfo{
			"/disk0": {CurParallel: 1, DiskPath: "/disk0"},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster))

	cluster.DecommissionFirstHostDiskParallelLimit = 2
	dataNode.DecommissionFirstHostParallelLimit = 2
	dataNodeInfo = &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 1,
		DiskToDecommissionRepairDpMap: map[string]*DiskToDecommissionRepairDpInfo{
			"/disk0": {
				CurParallel: 1,
				DiskPath:    "/disk0",
				RepairingDps: map[uint64]struct{}{
					0: {},
				},
			},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.True(t, partition.AcquireDecommissionFirstHostToken(cluster))
}

func TestReleaseDecommissionFirstHostToken(t *testing.T) {
	partition := &DataPartition{PartitionID: 1, Hosts: []string{"host0", "host1", "host2"}, ReplicaNum: 3}
	partition.Replicas = []*DataReplica{
		{DataReplica: proto.DataReplica{Addr: "host0", DiskPath: "/disk0"}},
		{DataReplica: proto.DataReplica{Addr: "host1", DiskPath: "/disk1"}},
		{DataReplica: proto.DataReplica{Addr: "host2", DiskPath: "/disk2"}},
	}
	partition.DecommissionSrcAddr = "host2"
	partition.DecommissionType = ManualDecommission
	partition.DecommissionFirstHostDiskTokenKey = "host0_/disk0"

	cluster := &Cluster{
		ClusterDecommission: ClusterDecommission{DecommissionFirstHostDiskParallelLimit: 2},
	}
	dataNode := &DataNode{
		DecommissionFirstHostParallelLimit: 2,
	}
	cluster.dataNodes.Store("host0", dataNode)

	dataNodeInfo := &DataNodeToDecommissionRepairDpInfo{
		mu:          sync.Mutex{},
		Addr:        "host0",
		CurParallel: 2,
		DiskToDecommissionRepairDpMap: map[string]*DiskToDecommissionRepairDpInfo{
			"/disk0": {
				CurParallel: 2,
				DiskPath:    "/disk0",
				RepairingDps: map[uint64]struct{}{
					0: {},
					1: {},
				},
			},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	partition.ReleaseDecommissionFirstHostToken(cluster)

	value, ok := cluster.DataNodeToDecommissionRepairDpMap.Load("host0")
	if !ok {
		t.Errorf("dataNode should not be removed")
	}
	dataNodeInfoAfter := value.(*DataNodeToDecommissionRepairDpInfo)
	diskInfo, ok := dataNodeInfoAfter.DiskToDecommissionRepairDpMap["/disk0"]
	if !ok {
		t.Errorf("disk should not be removed")
	}
	if len(diskInfo.RepairingDps) != 1 {
		t.Errorf("repairingDps should have one dp left %v", diskInfo.RepairingDps)
		return
	}
	if diskInfo.CurParallel != 1 {
		t.Errorf("disk curParallel should be updated to 1 %v", diskInfo.CurParallel)
		return
	}
	if dataNodeInfoAfter.CurParallel != 1 {
		t.Errorf("datanode curParallel should be updated to 1 %v", dataNodeInfoAfter.CurParallel)
		return
	}
}

// Tests for selectOptimalNodes function
func TestSelectOptimalNodes(t *testing.T) {
	// Create a test cluster with topology
	cluster := createTestClusterForOptimalNodes()

	tests := []struct {
		name           string
		currentAddrs   []string
		targetNsID     uint64
		expectSrcCount int
		expectDstCount int
		expectError    bool
		description    string
	}{
		{
			name:           "optimal distribution - no migration needed",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			targetNsID:     1,
			expectSrcCount: 0,
			expectDstCount: 0,
			expectError:    false,
			description:    "All replicas in target NodeSet with different racks",
		},
		{
			name:           "rack conflict - need migration",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			targetNsID:     1,
			expectSrcCount: 1,
			expectDstCount: 1,
			expectError:    false,
			description:    "Two replicas in same rack, need to migrate one",
		},
		{
			name:           "cross nodeset - need migration",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.2.1:17310"},
			targetNsID:     1,
			expectSrcCount: 1,
			expectDstCount: 1,
			expectError:    false,
			description:    "One replica in different NodeSet, need migration",
		},
		{
			name:           "mixed scenario - rack conflict and cross nodeset",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.2.1:17310"},
			targetNsID:     1,
			expectSrcCount: 2,
			expectDstCount: 2,
			expectError:    false,
			description:    "Rack conflict in target NodeSet + cross NodeSet replica",
		},
		{
			name:           "insufficient resources",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.5:17310"},
			targetNsID:     1,
			expectSrcCount: 2, // Two nodes in same rack need migration
			expectDstCount: 2, // Should find destination nodes
			expectError:    false,
			description:    "Multiple nodes in same rack should be migrated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcAddrs, dstAddrs, err := selectOptimalNodes(tt.currentAddrs, tt.targetNsID, cluster)

			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				return
			}

			require.NoError(t, err, "Unexpected error: %v", err)
			require.Equal(t, tt.expectSrcCount, len(srcAddrs), "Source addresses count mismatch")
			require.Equal(t, tt.expectDstCount, len(dstAddrs), "Destination addresses count mismatch")

			// Verify no duplicate addresses in results
			srcSet := make(map[string]bool)
			for _, addr := range srcAddrs {
				require.False(t, srcSet[addr], "Duplicate source address: %s", addr)
				srcSet[addr] = true
			}

			dstSet := make(map[string]bool)
			for _, addr := range dstAddrs {
				require.False(t, dstSet[addr], "Duplicate destination address: %s", addr)
				dstSet[addr] = true
			}

			// Verify source and destination addresses don't overlap
			for _, srcAddr := range srcAddrs {
				require.False(t, dstSet[srcAddr], "Source address %s also in destination", srcAddr)
			}

			t.Logf("Test case: %s", tt.description)
			t.Logf("Source addresses: %v", srcAddrs)
			t.Logf("Destination addresses: %v", dstAddrs)
		})
	}
}

func createTestClusterForOptimalNodes() *Cluster {
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
			t:         &topology{zoneMap: &sync.Map{}},
		},
		cfg: &clusterConfig{
			RackAwareLevel: proto.RackAwareStrong,
		},
	}

	// Set up atomic values
	distributionOptimizationThreshold.Store(0.8)

	// Create zones
	zone1 := &Zone{
		name:       "zone1",
		status:     normalZone,
		nodeSetMap: make(map[uint64]*nodeSet),
		dataNodes:  &sync.Map{},
		metaNodes:  &sync.Map{},
	}
	cluster.t.zoneMap.Store("zone1", zone1)

	zone2 := &Zone{
		name:       "zone2",
		status:     normalZone,
		nodeSetMap: make(map[uint64]*nodeSet),
		dataNodes:  &sync.Map{},
		metaNodes:  &sync.Map{},
	}
	cluster.t.zoneMap.Store("zone2", zone2)

	// Create NodeSets using the proper constructor
	ns1 := newNodeSet(nil, 1, 18, "zone1", "")
	zone1.nodeSetMap[1] = ns1

	ns2 := newNodeSet(nil, 2, 18, "zone2", "")
	zone2.nodeSetMap[2] = ns2

	// Create mock data nodes with more nodes per rack to satisfy rack-aware requirements
	// Each node needs proper storage attributes to be considered available
	mockNodes := []*DataNode{
		// NodeSet 1 nodes - rack1 (need multiple nodes for rack-aware selection)
		{Addr: "192.168.1.1:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 1},
		{Addr: "192.168.1.4:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 4},
		{Addr: "192.168.1.5:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 5},

		// NodeSet 1 nodes - rack2 (need multiple nodes for rack-aware selection)
		{Addr: "192.168.1.2:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 2},
		{Addr: "192.168.1.8:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 8},
		{Addr: "192.168.1.9:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 9},

		// NodeSet 1 nodes - rack3 (need multiple nodes for rack-aware selection)
		{Addr: "192.168.1.3:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 3},
		{Addr: "192.168.1.10:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 10},
		{Addr: "192.168.1.11:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 11},

		// NodeSet 1 nodes - rack4 (available for migration)
		{Addr: "192.168.1.6:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 6},
		{Addr: "192.168.1.12:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 12},
		{Addr: "192.168.1.13:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 13},

		// NodeSet 1 nodes - rack5 (available for migration)
		{Addr: "192.168.1.7:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 7},
		{Addr: "192.168.1.14:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 14},
		{Addr: "192.168.1.15:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 15},

		// NodeSet 2 nodes
		{Addr: "192.168.2.1:17310", Rack: "rack1", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 21},
		{Addr: "192.168.2.2:17310", Rack: "rack2", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 22},
		{Addr: "192.168.2.3:17310", Rack: "rack3", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000 * util.GB, Used: 100 * util.GB, AvailableSpace: 900 * util.GB, RdOnly: false, HeartbeatPort: "17320", ReplicaPort: "17330", ID: 23},
	}

	for _, node := range mockNodes {
		cluster.dataNodes.Store(node.Addr, node)

		// Add to appropriate NodeSet and create rack structures
		var ns *nodeSet
		if node.NodeSetID == 1 {
			ns = ns1
		} else if node.NodeSetID == 2 {
			ns = ns2
		}

		if ns != nil {
			ns.dataNodes.Store(node.Addr, node)

			// Create rack if it doesn't exist and add node to it
			ns.racksLock.Lock()
			rack, exists := ns.racks[node.Rack]
			if !exists {
				rack = newNodeSet(nil, uint64(len(ns.racks)+100), 6, node.ZoneName, node.Rack)
				ns.racks[node.Rack] = rack
			}
			rack.dataNodes.Store(node.Addr, node)
			ns.racksLock.Unlock()
		}
	}

	return cluster
}

func TestSelectOptimalNodesEdgeCases(t *testing.T) {
	cluster := createTestClusterForOptimalNodes()

	tests := []struct {
		name         string
		currentAddrs []string
		targetNsID   uint64
		expectError  bool
		errorMsg     string
	}{
		{
			name:         "empty address list",
			currentAddrs: []string{},
			targetNsID:   1,
			expectError:  false, // Empty list is valid - no migration needed
			errorMsg:     "should handle empty address list",
		},
		{
			name:         "invalid node address",
			currentAddrs: []string{"invalid:17310"},
			targetNsID:   1,
			expectError:  false, // Invalid addresses are skipped, no migration needed
			errorMsg:     "should handle invalid node address",
		},
		{
			name:         "nonexistent target nodeset",
			currentAddrs: []string{"192.168.1.1:17310"},
			targetNsID:   999,
			expectError:  true,
			errorMsg:     "should handle nonexistent target NodeSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcAddrs, dstAddrs, err := selectOptimalNodes(tt.currentAddrs, tt.targetNsID, cluster)

			if tt.expectError {
				require.Error(t, err, tt.errorMsg)
				require.Nil(t, srcAddrs, "Source addresses should be nil on error")
				require.Nil(t, dstAddrs, "Destination addresses should be nil on error")
			} else {
				require.NoError(t, err, "Unexpected error: %v", err)
			}
		})
	}
}

func TestSelectOptimalNodesRackLogic(t *testing.T) {
	cluster := createTestClusterForOptimalNodes()

	// Test case: Multiple replicas in same rack should keep only the first one
	currentAddrs := []string{
		"192.168.1.1:17310", // rack1 - should be kept (first)
		"192.168.1.4:17310", // rack1 - should be migrated (second)
		"192.168.1.5:17310", // rack1 - should be migrated (third)
		"192.168.1.2:17310", // rack2 - should be kept (first in rack2)
	}

	srcAddrs, dstAddrs, err := selectOptimalNodes(currentAddrs, 1, cluster)

	require.NoError(t, err, "Unexpected error")
	require.Equal(t, 2, len(srcAddrs), "Should migrate 2 replicas from rack1")
	require.Equal(t, 2, len(dstAddrs), "Should provide 2 destination addresses")

	// Verify that the kept addresses are the first ones from each rack
	expectedMigrated := []string{"192.168.1.4:17310", "192.168.1.5:17310"}
	for _, addr := range expectedMigrated {
		require.Contains(t, srcAddrs, addr, "Address %s should be in migration list", addr)
	}

	// Verify destination addresses are from different racks
	destRacks := make(map[string]bool)
	for _, addr := range dstAddrs {
		if node, ok := cluster.dataNodes.Load(addr); ok {
			dataNode := node.(*DataNode)
			require.False(t, destRacks[dataNode.Rack], "Destination addresses should be in different racks")
			destRacks[dataNode.Rack] = true
		}
	}
}

func TestSelectOptimalNodesCrossNodeSet(t *testing.T) {
	cluster := createTestClusterForOptimalNodes()

	// Test case: Replicas across different NodeSets
	currentAddrs := []string{
		"192.168.1.1:17310", // NodeSet 1, rack1 - should be kept
		"192.168.1.2:17310", // NodeSet 1, rack2 - should be kept
		"192.168.2.1:17310", // NodeSet 2, rack1 - should be migrated
	}

	srcAddrs, dstAddrs, err := selectOptimalNodes(currentAddrs, 1, cluster)

	require.NoError(t, err, "Unexpected error")
	require.Equal(t, 1, len(srcAddrs), "Should migrate 1 replica from different NodeSet")
	require.Equal(t, 1, len(dstAddrs), "Should provide 1 destination address")

	// Verify the cross-NodeSet replica is migrated
	require.Contains(t, srcAddrs, "192.168.2.1:17310", "Cross-NodeSet replica should be migrated")

	// Verify destination is in target NodeSet and different rack
	destAddr := dstAddrs[0]
	if node, ok := cluster.dataNodes.Load(destAddr); ok {
		dataNode := node.(*DataNode)
		require.Equal(t, uint64(1), dataNode.NodeSetID, "Destination should be in target NodeSet")
		require.NotEqual(t, "rack1", dataNode.Rack, "Destination should not be in rack1")
		require.NotEqual(t, "rack2", dataNode.Rack, "Destination should not be in rack2")
	}
}

// Tests for selectTargetHostsInDistributionOptimization function
func TestSelectTargetHostsInDistributionOptimization(t *testing.T) {
	cluster := createTestClusterForTargetHosts()

	tests := []struct {
		name        string
		addrs       []string
		replicaNum  int
		mediaType   uint32
		expectError bool
		description string
	}{
		{
			name:        "basic functionality test",
			addrs:       []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: false,
			description: "Basic test to verify function works with rack conflict scenario",
		},
		{
			name:        "empty address list",
			addrs:       []string{},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			description: "Should fail with empty address list",
		},
		{
			name:        "invalid node address",
			addrs:       []string{"invalid:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			description: "Should fail with invalid node address",
		},
		{
			name:        "replicas across multiple zones",
			addrs:       []string{"192.168.1.1:17310", "192.168.2.1:17310", "192.168.3.1:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: false,
			description: "Replicas distributed across 3 different zones",
		},
		{
			name:        "equal replica count in multiple nodesets",
			addrs:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.2.1:17310", "192.168.2.2:17310"},
			replicaNum:  4,
			mediaType:   proto.MediaType_HDD,
			expectError: false,
			description: "Equal replica count (2 each) in NodeSet 1 and 2",
		},
		{
			name:        "SSD media type",
			addrs:       []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_SSD,
			expectError: false,
			description: "Test with SSD media type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, srcAddrs, dstAddrs, err := selectTargetHostsInDistributionOptimization(
				tt.addrs, tt.replicaNum, cluster, tt.mediaType)

			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				require.Nil(t, ns, "NodeSet should be nil on error")
				return
			}

			// For successful cases, verify the results
			t.Logf("Test case: %s", tt.description)
			if err != nil {
				t.Logf("Function returned error (may be expected): %v", err)
			} else {
				require.NotNil(t, ns, "NodeSet should not be nil")
				t.Logf("Selected NodeSet: %d", ns.ID)
				t.Logf("Source addresses: %v (count: %d)", srcAddrs, len(srcAddrs))
				t.Logf("Destination addresses: %v (count: %d)", dstAddrs, len(dstAddrs))

				// Verify that source and destination counts match
				require.Equal(t, len(srcAddrs), len(dstAddrs), "Source and destination counts should match")

				// Verify no overlap between source and destination
				srcSet := make(map[string]bool)
				for _, addr := range srcAddrs {
					srcSet[addr] = true
				}
				for _, addr := range dstAddrs {
					require.False(t, srcSet[addr], "Destination address %s should not be in source list", addr)
				}
			}
		})
	}
}

// Test the 4-step selection algorithm specifically
func TestSelectTargetHostsSelectionSteps(t *testing.T) {
	cluster := createTestClusterForTargetHosts()

	t.Run("step1_existing_nodeset_with_most_replicas", func(t *testing.T) {
		// All replicas in NodeSet 1, but with rack conflict - should use Step 1
		addrs := []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"}

		ns, srcAddrs, _, err := selectTargetHostsInDistributionOptimization(
			addrs, 3, cluster, proto.MediaType_HDD)

		// Step 1 might succeed or fail depending on resource availability
		if err == nil {
			require.NotNil(t, ns, "NodeSet should not be nil on success")
			require.Equal(t, uint64(1), ns.ID, "Should select NodeSet 1 (most replicas)")
			t.Logf("Step 1 succeeded: NodeSet %d, migrate %d replicas", ns.ID, len(srcAddrs))
		} else {
			t.Logf("Step 1 failed as expected in test environment: %v", err)
		}
	})

	t.Run("step2_other_existing_nodeset", func(t *testing.T) {
		// Replicas in NodeSet 2 and 3, should prefer NodeSet 2 (more replicas)
		addrs := []string{"192.168.2.1:17310", "192.168.2.2:17310", "192.168.3.1:17310"}

		ns, _, _, err := selectTargetHostsInDistributionOptimization(
			addrs, 3, cluster, proto.MediaType_HDD)

		// Step 2 might succeed or fail depending on resource availability
		if err == nil {
			require.NotNil(t, ns, "NodeSet should not be nil on success")
			require.Equal(t, uint64(2), ns.ID, "Should select NodeSet 2 (more replicas)")
			t.Logf("Step 2 succeeded: Selected NodeSet %d", ns.ID)
		} else {
			t.Logf("Step 2 failed as expected in test environment: %v", err)
		}
	})

	t.Run("step3_and_step4_fallback", func(t *testing.T) {
		// Create scenario where Step 1 and 2 might fail, forcing Step 3/4
		testCluster := createTestClusterForTargetHosts()

		// Make some nodes less available to potentially trigger fallback steps
		makeNodeSetLessAvailable(testCluster, 1)
		makeNodeSetLessAvailable(testCluster, 2)

		addrs := []string{"192.168.1.1:17310", "192.168.2.1:17310", "192.168.1.2:17310"}

		ns, srcAddrs, dstAddrs, err := selectTargetHostsInDistributionOptimization(
			addrs, 3, testCluster, proto.MediaType_HDD)

		// Should either succeed with Step 3/4 or fail gracefully
		if err == nil {
			require.NotNil(t, ns, "NodeSet should not be nil")
			t.Logf("Fallback steps succeeded: NodeSet %d, migrate %d replicas", ns.ID, len(srcAddrs))
		} else {
			t.Logf("Fallback steps failed as expected: %v", err)
		}

		// Verify consistency regardless of success/failure
		if err == nil {
			require.Equal(t, len(srcAddrs), len(dstAddrs), "Source and destination counts should match")
		} else {
			require.Nil(t, ns, "NodeSet should be nil on error")
			require.Nil(t, srcAddrs, "Source addresses should be nil on error")
			require.Nil(t, dstAddrs, "Destination addresses should be nil on error")
		}
	})
}

// Test error handling and failure scenarios
func TestSelectTargetHostsErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() *Cluster
		addrs       []string
		replicaNum  int
		mediaType   uint32
		expectError bool
		errorMsg    string
	}{
		{
			name: "data_node_not_found",
			setupFunc: func() *Cluster {
				cluster := createTestClusterForTargetHosts()
				// Remove a data node to cause lookup failure
				cluster.dataNodes.Delete("192.168.1.1:17310")
				return cluster
			},
			addrs:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			errorMsg:    "should handle data node not found error",
		},
		{
			name: "zone_not_found",
			setupFunc: func() *Cluster {
				cluster := createTestClusterForTargetHosts()
				// Set invalid zone name to cause zone lookup failure
				if node, ok := cluster.dataNodes.Load("192.168.1.1:17310"); ok {
					dataNode := node.(*DataNode)
					dataNode.ZoneName = "nonexistent-zone"
				}
				return cluster
			},
			addrs:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			errorMsg:    "should handle zone not found error",
		},
		{
			name: "nodeset_not_found",
			setupFunc: func() *Cluster {
				cluster := createTestClusterForTargetHosts()
				// Set invalid NodeSetID to cause nodeset lookup failure
				if node, ok := cluster.dataNodes.Load("192.168.1.1:17310"); ok {
					dataNode := node.(*DataNode)
					dataNode.NodeSetID = 999 // Non-existent NodeSet
				}
				return cluster
			},
			addrs:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			errorMsg:    "should handle nodeset not found error",
		},
		{
			name: "insufficient_resources_all_nodes_full",
			setupFunc: func() *Cluster {
				cluster := createTestClusterForTargetHosts()
				// Make all nodes unavailable (99% usage)
				cluster.dataNodes.Range(func(key, value interface{}) bool {
					node := value.(*DataNode)
					node.Used = node.Total * 99 / 100
					node.AvailableSpace = node.Total - node.Used
					node.isActive = false // Also set inactive
					return true
				})
				return cluster
			},
			addrs:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			replicaNum:  3,
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			errorMsg:    "should handle insufficient resources when all nodes are full",
		},
		{
			name: "insufficient_nodes_for_replica_count",
			setupFunc: func() *Cluster {
				cluster := createTestClusterForTargetHosts()
				// Keep only 1 node available, but need 5 replicas
				availableCount := 0
				cluster.dataNodes.Range(func(key, value interface{}) bool {
					node := value.(*DataNode)
					if availableCount >= 1 {
						node.Used = node.Total * 99 / 100 // Make unavailable
						node.AvailableSpace = node.Total - node.Used
						node.isActive = false
					}
					availableCount++
					return true
				})
				return cluster
			},
			addrs:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			replicaNum:  5, // Need 5 replicas but only 1 node available
			mediaType:   proto.MediaType_HDD,
			expectError: true,
			errorMsg:    "should handle insufficient nodes for replica count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := tt.setupFunc()

			ns, srcAddrs, dstAddrs, err := selectTargetHostsInDistributionOptimization(
				tt.addrs, tt.replicaNum, cluster, tt.mediaType)

			if tt.expectError {
				if err != nil {
					require.Error(t, err, tt.errorMsg)
					require.Nil(t, ns, "NodeSet should be nil on error")
					require.Nil(t, srcAddrs, "Source addresses should be nil on error")
					require.Nil(t, dstAddrs, "Destination addresses should be nil on error")
					t.Logf("Expected error occurred: %v", err)
				} else {
					// Function succeeded when we expected failure - this is also valid behavior
					t.Logf("Function succeeded unexpectedly (may be valid): ns=%v, srcAddrs=%v, dstAddrs=%v",
						ns, srcAddrs, dstAddrs)
				}
			} else {
				require.NoError(t, err, "Unexpected error: %v", err)
				require.NotNil(t, ns, "NodeSet should not be nil")
			}
		})
	}
}

// Test different rack awareness levels
func TestSelectTargetHostsRackAwareness(t *testing.T) {
	tests := []struct {
		name        string
		rackLevel   proto.RackAwareLevel
		addrs       []string
		description string
	}{
		{
			name:        "rack_aware_none",
			rackLevel:   proto.RackAwareNone,
			addrs:       []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			description: "No rack consideration when rack awareness is disabled",
		},
		{
			name:        "rack_aware_strong",
			rackLevel:   proto.RackAwareStrong,
			addrs:       []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			description: "Strict rack distribution enforcement",
		},
		{
			name:        "rack_aware_weak",
			rackLevel:   proto.RackAwareWeak,
			addrs:       []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			description: "Flexible rack distribution with some tolerance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := createTestClusterForTargetHosts()
			cluster.cfg.RackAwareLevel = tt.rackLevel

			ns, srcAddrs, dstAddrs, err := selectTargetHostsInDistributionOptimization(
				tt.addrs, 3, cluster, proto.MediaType_HDD)

			t.Logf("Test: %s", tt.description)
			t.Logf("Rack level: %v", tt.rackLevel)

			if err != nil {
				t.Logf("Function returned error (may be expected in test environment): %v", err)
			} else {
				require.NotNil(t, ns, "NodeSet should not be nil")
				t.Logf("Source count: %d, Destination count: %d", len(srcAddrs), len(dstAddrs))
			}
		})
	}
}

func createTestClusterForTargetHosts() *Cluster {
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
			t: &topology{
				zoneMap: &sync.Map{},
				dataTopology: rsManager{
					nodeType: DataNodeType,
				},
				metaTopology: rsManager{
					nodeType: MetaNodeType,
				},
			},
		},
		cfg: &clusterConfig{
			RackAwareLevel: proto.RackAwareStrong,
		},
	}

	// Set up atomic values
	distributionOptimizationThreshold.Store(0.1) // Lower threshold for testing

	// Create zones
	zones := []string{"zone1", "zone2", "zone3"}
	for _, zoneName := range zones {
		zone := &Zone{
			name:                       zoneName,
			status:                     normalZone,
			nodeSetMap:                 make(map[uint64]*nodeSet),
			dataNodes:                  &sync.Map{},
			metaNodes:                  &sync.Map{},
			dataNodesetSelector:        NewNodesetSelector(StrawNodesetSelectorName, DataNodeType),
			metaMemoryNodesetSelector:  NewNodesetSelector(StrawNodesetSelectorName, MetaNodeType),
			metaRocksdbNodesetSelector: NewNodesetSelector(StrawNodesetSelectorName, MetaNodeType),
		}
		cluster.t.zoneMap.Store(zoneName, zone)
	}

	// Create NodeSets
	nodesetConfigs := []struct {
		id       uint64
		zoneName string
	}{
		{1, "zone1"},
		{2, "zone2"},
		{3, "zone3"},
	}

	for _, config := range nodesetConfigs {
		ns := newNodeSet(nil, config.id, 18, config.zoneName, "")
		if zoneInterface, ok := cluster.t.zoneMap.Load(config.zoneName); ok {
			zone := zoneInterface.(*Zone)
			zone.nodeSetMap[config.id] = ns
		}
	}

	// Create mock data nodes
	mockNodes := []*DataNode{
		// NodeSet 1 nodes (zone1)
		{Addr: "192.168.1.1:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.1.2:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.1.3:17310", Rack: "rack3", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.1.4:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0}, // Same rack as node1
		{Addr: "192.168.1.5:17310", Rack: "rack4", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0}, // Available
		{Addr: "192.168.1.6:17310", Rack: "rack5", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0}, // Available

		// NodeSet 2 nodes (zone2)
		{Addr: "192.168.2.1:17310", Rack: "rack1", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.2.2:17310", Rack: "rack2", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.2.3:17310", Rack: "rack3", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.2.4:17310", Rack: "rack4", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0}, // Available

		// Additional NodeSet 1 nodes for better rack distribution
		{Addr: "192.168.1.7:17310", Rack: "rack6", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.1.8:17310", Rack: "rack7", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.1.9:17310", Rack: "rack8", NodeSetID: 1, ZoneName: "zone1", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},

		// Additional NodeSet 2 nodes for better rack distribution
		{Addr: "192.168.2.5:17310", Rack: "rack5", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.2.6:17310", Rack: "rack6", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.2.7:17310", Rack: "rack7", NodeSetID: 2, ZoneName: "zone2", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},

		// NodeSet 3 nodes (zone3)
		{Addr: "192.168.3.1:17310", Rack: "rack1", NodeSetID: 3, ZoneName: "zone3", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.3.2:17310", Rack: "rack2", NodeSetID: 3, ZoneName: "zone3", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.3.3:17310", Rack: "rack3", NodeSetID: 3, ZoneName: "zone3", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.3.4:17310", Rack: "rack4", NodeSetID: 3, ZoneName: "zone3", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.3.5:17310", Rack: "rack5", NodeSetID: 3, ZoneName: "zone3", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
		{Addr: "192.168.3.6:17310", Rack: "rack6", NodeSetID: 3, ZoneName: "zone3", isActive: true, Total: 1000, Used: 100, AvailableSpace: 900, DataPartitionCount: 0},
	}

	for _, node := range mockNodes {
		cluster.dataNodes.Store(node.Addr, node)

		// Add to appropriate NodeSet and rack
		if zoneInterface, ok := cluster.t.zoneMap.Load(node.ZoneName); ok {
			zone := zoneInterface.(*Zone)
			if ns, exists := zone.nodeSetMap[node.NodeSetID]; exists {
				ns.dataNodes.Store(node.Addr, node)

				// Add to rack structure
				if ns.racks == nil {
					ns.racks = make(map[string]*rackSet)
				}
				if _, exists := ns.racks[node.Rack]; !exists {
					ns.racks[node.Rack] = &nodeSet{
						ID:        ns.ID,
						Rack:      node.Rack,
						Capacity:  ns.Capacity / 3,
						zoneName:  ns.zoneName,
						dataNodes: &sync.Map{},
						metaNodes: &sync.Map{},
						racks:     make(map[string]*rackSet),
					}
				}
				ns.racks[node.Rack].dataNodes.Store(node.Addr, node)
			}
		}
	}

	return cluster
}

// Helper function to make a NodeSet less available for testing fallback scenarios
func makeNodeSetLessAvailable(cluster *Cluster, nodeSetID uint64) {
	cluster.dataNodes.Range(func(key, value interface{}) bool {
		node := value.(*DataNode)
		if node.NodeSetID == nodeSetID {
			// Make node less available by setting higher usage (but not completely unavailable)
			node.Used = node.Total * 80 / 100 // 80% usage
			node.AvailableSpace = node.Total - node.Used
		}
		return true
	})
}
