package master

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
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
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster, false))

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
	assert.False(t, partition.AcquireDecommissionFirstHostToken(cluster, false))

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
				IdToPriority: map[uint64]int{
					0: 2,
				},
			},
		},
	}
	cluster.DataNodeToDecommissionRepairDpMap.Store("host0", dataNodeInfo)
	assert.True(t, partition.AcquireDecommissionFirstHostToken(cluster, false))
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
				IdToPriority: map[uint64]int{
					0: 2,
					1: 2,
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
			name:           "rack conflict (two replicas) - need migration",
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
			name:           "rack conflict (three replicas)",
			currentAddrs:   []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.5:17310"},
			targetNsID:     1,
			expectSrcCount: 2,
			expectDstCount: 2,
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
			expectError:  true, // Empty list cannot find target NodeSet
			errorMsg:     "should return error for empty address list",
		},
		{
			name:         "invalid node address",
			currentAddrs: []string{"invalid:17310"},
			targetNsID:   1,
			expectError:  true, // Invalid addresses cannot find target NodeSet
			errorMsg:     "should return error for invalid node addresses",
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

// testLiveDataReplica builds a replica that passes isLive() for needReplicaMetaRestore / checkReplicaMeta paths.
func testLiveDataReplica(addr string, applyMemberChangeID uint64, isLeader bool, localPeers []proto.Peer) *DataReplica {
	dn := &DataNode{isActive: true, Addr: addr}
	return &DataReplica{
		DataReplica: proto.DataReplica{
			Addr:                addr,
			Status:              proto.ReadWrite,
			ReportTime:          time.Now().Unix(),
			IsLeader:            isLeader,
			ApplyMemberChangeID: applyMemberChangeID,
			LocalPeers:          localPeers,
		},
		dataNode: dn,
	}
}

func TestDataPartition_getLeaderApplyMemberChangeID(t *testing.T) {
	p := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h2", 7, false, nil),
			testLiveDataReplica("h1", 42, true, nil),
		},
	}
	id, ok := p.getLeaderApplyMemberChangeID()
	require.True(t, ok)
	require.EqualValues(t, 42, id)

	p2 := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 10, false, nil),
		},
	}
	id, ok = p2.getLeaderApplyMemberChangeID()
	require.False(t, ok)
	require.Zero(t, id)
}

func TestDataPartition_hasFollowerApplyMemberChangeAheadOfLeader(t *testing.T) {
	peers := []proto.Peer{
		{Addr: "h1", Type: raftProto.PeerNormal},
		{Addr: "h2", Type: raftProto.PeerNormal},
	}
	p := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 100, true, peers),
			testLiveDataReplica("h2", 100, false, peers),
		},
	}
	require.False(t, p.hasFollowerApplyMemberChangeAheadOfLeader())

	p.Replicas[1].ApplyMemberChangeID = 101
	require.True(t, p.hasFollowerApplyMemberChangeAheadOfLeader())

	pNoLeader := &DataPartition{
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, false, peers),
		},
	}
	require.False(t, pNoLeader.hasFollowerApplyMemberChangeAheadOfLeader())
}

// TestDataPartition_needReplicaMetaRestore_applyMemberChangeID covers e8f69a29: lagging followers must not
// drive replica-meta restore; if a follower reports higher ApplyMemberChangeID than leader, restore is skipped.
func TestDataPartition_needReplicaMetaRestore_applyMemberChangeID(t *testing.T) {
	c := &Cluster{cfg: newClusterConfig()}
	peers := []proto.Peer{
		{Addr: "h1", Type: raftProto.PeerNormal},
		{Addr: "h2", Type: raftProto.PeerNormal},
	}
	orphan := proto.Peer{Addr: "orphan", Type: raftProto.PeerNormal}
	peersWithOrphan := append(append([]proto.Peer(nil), peers...), orphan)

	// Leader caught up; follower still behind on member-change log but carries redundant local peer — ignore follower.
	dpLaggingFollower := &DataPartition{
		PartitionID: 9901,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, true, peers),
			testLiveDataReplica("h2", 50, false, peersWithOrphan),
		},
	}
	require.False(t, dpLaggingFollower.needReplicaMetaRestore(c), "lagging follower should not trigger restore")

	// Same topology but follower caught up to leader — redundant local peer should be detected.
	dpCaughtUp := &DataPartition{
		PartitionID: 9902,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 200, true, peers),
			testLiveDataReplica("h2", 200, false, peersWithOrphan),
		},
	}
	require.True(t, dpCaughtUp.needReplicaMetaRestore(c), "caught-up follower with redundant peers should need restore")

	// Follower ahead of leader (stale leader view): never restore.
	dpDivergent := &DataPartition{
		PartitionID: 9903,
		ReplicaNum:  2,
		Peers:       peers,
		Hosts:       []string{"h1", "h2"},
		Replicas: []*DataReplica{
			testLiveDataReplica("h1", 50, true, peers),
			testLiveDataReplica("h2", 100, false, peersWithOrphan),
		},
	}
	require.False(t, dpDivergent.needReplicaMetaRestore(c), "follower ahead of leader must skip restore")
}

// TestDataPartitionValue_replicaApplyMemberChangeIDJSON ensures raft-persisted DP replica value carries ApplyMemberChangeID (e8f69a29).
func TestDataPartitionValue_replicaApplyMemberChangeIDJSON(t *testing.T) {
	dp := &DataPartition{
		PartitionID: 88001,
		ReplicaNum:  1,
		VolName:     "vol_mc_json",
		VolID:       1,
		Hosts:       []string{"10.0.0.1:17310"},
		Replicas: []*DataReplica{
			{DataReplica: proto.DataReplica{Addr: "10.0.0.1:17310", DiskPath: "/data1", ApplyMemberChangeID: 888}},
		},
	}
	dpv := newDataPartitionValue(dp)
	require.Len(t, dpv.Replicas, 1)
	require.EqualValues(t, 888, dpv.Replicas[0].ApplyMemberChangeID)

	raw, err := json.Marshal(dpv)
	require.NoError(t, err)
	require.Contains(t, string(raw), "applyMemberChangeID")

	var decoded dataPartitionValue
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded.Replicas, 1)
	require.EqualValues(t, 888, decoded.Replicas[0].ApplyMemberChangeID)
}
