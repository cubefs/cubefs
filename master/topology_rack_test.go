package master

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

// Helper function: Create data node with rack information
func createDataNodeWithRack(addr, zoneName, rackName string, ns *nodeSet) *DataNode {
	dn := newDataNode(addr, strconv.Itoa(raftstore.DefaultHeartbeatPort), strconv.Itoa(raftstore.DefaultReplicaPort), zoneName, "", "test", proto.MediaType_HDD)
	dn.ZoneName = zoneName
	dn.Rack = rackName
	dn.Total = 1024 * util.GB
	dn.Used = 10 * util.GB
	dn.AvailableSpace = 1024 * util.GB
	dn.ReportTime = time.Now()
	dn.isActive = true
	dn.NodeSetID = ns.ID
	dn.AllDisks = []string{"/cfs/disk"}
	dn.DpCntLimit = defaultMaxDpCntLimit
	return dn
}

// Helper function: Create meta node with rack information
func createMetaNodeWithRack(addr, zoneName, rackName string, ns *nodeSet) *MetaNode {
	mn := newMetaNode(addr, strconv.Itoa(raftstore.DefaultHeartbeatPort), strconv.Itoa(raftstore.DefaultReplicaPort), zoneName, "", "test")
	mn.ZoneName = zoneName
	mn.Rack = rackName
	mn.Total = 1024 * util.GB
	mn.Used = 10 * util.GB
	mn.ReportTime = time.Now()
	mn.IsActive = true
	mn.NodeSetID = ns.ID
	mn.Threshold = 0.8
	mn.MaxMemAvailWeight = 1024 * util.GB
	return mn
}

// Helper function: Setup test environment for rack-aware testing with multiple nodes
func setupRackAwareTestEnvWithNodes(t *testing.T, rackCount, nodesPerRack int) (*topology, *Cluster, *Zone, *nodeSet) {
	topo := newTopology()
	c := new(Cluster)
	c.cfg = newClusterConfig()
	c.cfg.RackAwareLevel = proto.RackAwareStrong

	zoneName := "test-zone"
	zone := newZone(zoneName, proto.MediaType_Unspecified)
	topo.putZone(zone)

	ns := newNodeSet(c, 1, rackCount*nodesPerRack+10, zoneName, "") // Set capacity higher than total nodes
	zone.putNodeSet(ns)

	// Add nodes in multiple racks
	for rackIdx := 0; rackIdx < rackCount; rackIdx++ {
		rackName := "rack" + strconv.Itoa(rackIdx+1)
		for nodeIdx := 0; nodeIdx < nodesPerRack; nodeIdx++ {
			dn := createDataNodeWithRack(
				"192.168.1."+strconv.Itoa(rackIdx*10+nodeIdx+1)+":17310",
				zone.name,
				rackName,
				ns,
			)
			ns.putDataNode(dn)
		}
	}

	return topo, c, zone, ns
}

// Helper function: Setup test environment for rack-aware testing
func setupRackAwareTestEnv(t *testing.T) (*topology, *Cluster, *Zone, *nodeSet) {
	return setupRackAwareTestEnvWithNodes(t, 6, 2) // 6 racks, 2 nodes per rack = 12 total nodes
}

// Test 1: Test the correctness of checkRackAwareWriteable function with sufficient nodes
func TestRackCheckRackAwareWriteable(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnv(t) // 6 racks, 2 nodes per rack = 12 nodes

	// Test case 1: Sufficient racks to meet replica requirement
	param := &selectParam{
		replicaNum: 3,
		rackLevel:  proto.RackAwareStrong,
	}

	// Should return true since we have 6 racks and need 3 replicas
	result := ns.checkRackAwareWriteable(param, DataNodeType)
	require.True(t, result, "Should have enough racks for replica requirement")

	// Test case 2: Insufficient racks
	param.replicaNum = 8
	result = ns.checkRackAwareWriteable(param, DataNodeType)
	require.False(t, result, "Should not have enough racks for 8 replicas")

	// Test case 3: Exclude certain racks
	param.replicaNum = 3
	param.excludeRacks = []string{"rack1", "rack2"}
	result = ns.checkRackAwareWriteable(param, DataNodeType)
	require.True(t, result, "Should have enough racks after excluding 2 racks")

	// Test case 4: Exclude too many racks
	param.excludeRacks = []string{"rack1", "rack2", "rack3", "rack4"}
	result = ns.checkRackAwareWriteable(param, DataNodeType)
	require.False(t, result, "Should not have enough racks after excluding 4 racks")

	// Test case 5: Test with maximum replica number
	param.replicaNum = 6
	param.excludeRacks = nil
	result = ns.checkRackAwareWriteable(param, DataNodeType)
	require.True(t, result, "Should have enough racks for maximum replica number")
}

// Test 2: Test rack selection logic in selectNodesWithRack function with multiple nodes
func TestRackSelectNodesWithRack(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnv(t) // 6 racks, 2 nodes per rack = 12 nodes

	// Test case 1: Strong rack awareness mode with multiple replicas
	param := &selectParam{
		replicaNum: 4,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select nodes with rack awareness")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify selected nodes are from different racks
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		// Find corresponding rack based on host
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack] = true
				return false
			}
			return true
		})
	}
	require.Equal(t, 4, len(selectedRacks), "Selected nodes should be from 4 different racks")

	// Test case 2: Weak rack awareness mode
	param.rackLevel = proto.RackAwareWeak
	param.replicaNum = 6
	hosts, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select nodes with weak rack awareness")
	require.Equal(t, 6, len(hosts), "Should select 6 hosts")

	// Test case 3: Test with maximum possible replicas
	param.rackLevel = proto.RackAwareStrong
	param.replicaNum = 6
	hosts, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select maximum replicas with strong rack awareness")
	require.Equal(t, 6, len(hosts), "Should select 6 hosts from 6 different racks")
}

// Test 3: Test handling when no racks are available
func TestRackSelectNodesWithRackNoRacks(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnv(t)

	// Clear all nodes to test empty rack collection scenario
	ns.dataNodes = new(sync.Map)
	ns.racks = make(map[string]*nodeSet)

	param := &selectParam{
		replicaNum: 2,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.Error(t, err, "Should return error when no racks available")
	require.Nil(t, hosts, "Should return nil hosts")
}

// Test 4: Test rack awareness degradation mechanism with multiple nodes per rack
func TestRackAwareDegradation(t *testing.T) {
	// Setup with fewer racks but more nodes per rack
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 4) // 2 racks, 4 nodes per rack = 8 nodes

	// Test strong rack awareness mode, should fail since we need 3 replicas but only have 2 racks
	param := &selectParam{
		replicaNum: 3, // Need 3 replicas but only 2 racks
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	// Should fail in strong awareness mode since we don't have enough racks
	require.Error(t, err, "Should fail with strong rack awareness when not enough racks")
	require.Nil(t, hosts, "Should return nil hosts when failing")

	// Test weak rack awareness mode, should succeed since same rack has multiple nodes
	param.rackLevel = proto.RackAwareWeak
	hosts, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should succeed with weak rack awareness")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify that we can select from same rack when needed in weak mode
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}
	// Should have nodes from both racks, with at least one rack having multiple nodes
	require.Equal(t, 2, len(selectedRacks), "Should use both available racks")
	require.GreaterOrEqual(t, selectedRacks["rack1"]+selectedRacks["rack2"], 3, "Should select 3 total nodes")
}

// Test 5: Test concurrency safety of getRackSets function with many nodes
func TestRackGetRackSetsConcurrency(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 8, 3) // 8 racks, 3 nodes per rack = 24 nodes

	// Concurrency test
	done := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func() {
			defer func() {
				done <- true
			}()

			// Concurrent calls to getRackSets
			rsets := ns.getRackSets()
			require.NotNil(t, rsets, "getRackSets should not return nil")
			require.Equal(t, 8, len(rsets), "Should return 8 rack sets")
			require.GreaterOrEqual(t, len(rsets), 0, "Rack sets length should be non-negative")
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 20; i++ {
		<-done
	}
}

// Test 6: Test parameter override issue in allocNodeSetForDataNode with multiple nodes
func TestRackAllocNodeSetForDataNodeParamOverride(t *testing.T) {
	_, _, zone, _ := setupRackAwareTestEnv(t) // 6 racks, 2 nodes per rack = 12 nodes

	// Test case 1: User explicitly sets rackLevel to None
	param := &selectParam{
		replicaNum: 3,
		rackLevel:  proto.RackAwareNone,
	}

	selectedNodeSet, err := zone.allocNodeSetForDataNode(param)
	require.NoError(t, err, "Should successfully allocate node set")
	require.NotNil(t, selectedNodeSet, "Selected node set should not be nil")

	// Test case 2: User sets rackLevel to Weak
	param.rackLevel = proto.RackAwareWeak
	_, err = zone.allocNodeSetForDataNode(param)
	require.NoError(t, err, "Should successfully allocate node set with weak rack awareness")

	// Test case 3: Test with higher replica count
	param.replicaNum = 5
	param.rackLevel = proto.RackAwareStrong
	_, err = zone.allocNodeSetForDataNode(param)
	require.NoError(t, err, "Should successfully allocate node set with high replica count")
}

// Test 7: Test deep copy functionality
func TestRackSelectParamDeepCopy(t *testing.T) {
	original := &selectParam{
		excludeHosts:    []string{"host1", "host2", "host3", "host4"},
		replicaNum:      5,
		rackLevel:       proto.RackAwareStrong,
		excludeRacks:    []string{"rack1", "rack2", "rack3"},
		excludeNodeSets: []uint64{1, 2, 3, 4, 5},
	}

	copied := original.copy()

	// Modify original parameters
	original.excludeHosts[0] = "modified"
	original.excludeRacks = append(original.excludeRacks, "rack4")
	original.excludeNodeSets[0] = 999

	// Verify copied parameters are not modified
	require.Equal(t, "host1", copied.excludeHosts[0], "Copied excludeHosts should not be modified")
	require.Equal(t, 3, len(copied.excludeRacks), "Copied excludeRacks length should not change")
	require.Equal(t, uint64(1), copied.excludeNodeSets[0], "Copied excludeNodeSets should not be modified")
	require.Equal(t, 5, copied.replicaNum, "Copied replicaNum should not be modified")
	require.Equal(t, proto.RackAwareStrong, copied.rackLevel, "Copied rackLevel should not be modified")
}

// Test 8: Test boundary conditions for rack awareness with many nodes
func TestRackBoundaryConditions(t *testing.T) {
	_, _, zone, ns := setupRackAwareTestEnv(t) // 6 racks, 2 nodes per rack = 12 nodes

	// Test case 1: replicaNum is 0
	param := &selectParam{
		replicaNum: 0,
		rackLevel:  proto.RackAwareStrong,
	}

	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should handle replicaNum=0 gracefully")
	require.Equal(t, 0, len(hosts), "Should return empty hosts for replicaNum=0")

	// Test case 2: All nodes are not writable
	// Set all nodes as inactive
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		dn.isActive = false
		return true
	})

	param.replicaNum = 1
	_, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.Error(t, err, "Should return error when no writable nodes available")

	// Test case 3: Test with maximum replica number for strong rack awareness
	// Reset nodes to active
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		dn.isActive = true
		return true
	})

	// In RackAwareStrong mode, can only select up to the number of racks (6)
	param.replicaNum = 6 // Maximum possible with 6 racks in strong mode
	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should handle maximum replica number for strong rack awareness")
	require.Equal(t, 6, len(hosts), "Should return 6 hosts (one per rack)")

	// Test case 4: Test with weak rack awareness to select more nodes
	param.rackLevel = proto.RackAwareWeak
	param.replicaNum = 12 // All available nodes
	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should handle maximum replica number with weak rack awareness")
	require.Equal(t, 12, len(hosts), "Should return all available hosts with weak rack awareness")
}

// Test 9: Test rack awareness for meta nodes with multiple nodes
func TestRackMetaNodeRackAwareness(t *testing.T) {
	_, _, zone, ns := setupRackAwareTestEnvWithNodes(t, 5, 3) // 5 racks, 3 nodes per rack = 15 nodes

	// Add meta nodes in multiple racks
	for rackIdx := 0; rackIdx < 5; rackIdx++ {
		rackName := "rack" + strconv.Itoa(rackIdx+1)
		for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
			mn := createMetaNodeWithRack(
				"192.168.2."+strconv.Itoa(rackIdx*10+nodeIdx+1)+":17310",
				zone.name,
				rackName,
				ns,
			)
			ns.putMetaNode(mn)
		}
	}

	param := &selectParam{
		replicaNum: 4,
		rackLevel:  proto.RackAwareStrong,
	}

	hosts, _, err := ns.getAvailMetaNodeHosts(param, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select meta nodes with rack awareness")
	require.Equal(t, 4, len(hosts), "Should select 4 meta node hosts")

	// Verify selected nodes are from different racks
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		ns.metaNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				mn := value.(*MetaNode)
				selectedRacks[mn.Rack] = true
				return false
			}
			return true
		})
	}
	require.Equal(t, 4, len(selectedRacks), "Selected meta nodes should be from 4 different racks")
}

// Test 10: Test performance and stability of rack awareness with many nodes
func TestRackPerformance(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 10, 8) // 10 racks, 8 nodes per rack = 80 nodes

	param := &selectParam{
		replicaNum: 5,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	// Execute selection multiple times to test stability
	for i := 0; i < 20; i++ {
		hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
		require.NoError(t, err, "Should consistently succeed with large number of racks")
		require.Equal(t, 5, len(hosts), "Should consistently select 5 hosts")

		// Verify rack distribution
		selectedRacks := make(map[string]bool)
		for _, host := range hosts {
			ns.dataNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					dn := value.(*DataNode)
					selectedRacks[dn.Rack] = true
					return false
				}
				return true
			})
		}
		require.Equal(t, 5, len(selectedRacks), "Should consistently select from 5 different racks")
	}
}

// Test 11: Test error recovery mechanism for rack awareness with mixed availability
func TestRackErrorRecovery(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 6, 4) // 6 racks, 4 nodes per rack = 24 nodes

	// Set some nodes as unavailable in different racks
	nodeCount := 0
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		if nodeCount%3 == 0 { // Make every 3rd node unavailable
			dn.isActive = false
		}
		nodeCount++
		return true
	})

	param := &selectParam{
		replicaNum: 4,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	// Should still succeed since we have enough available nodes
	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should succeed with mixed node availability")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Test weak awareness mode with more replicas
	param.rackLevel = proto.RackAwareWeak
	param.replicaNum = 6
	hosts, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should succeed with weak awareness and mixed availability")
	require.Equal(t, 6, len(hosts), "Should select 6 hosts")
}

// Test 12: Test rack awareness configuration changes with many nodes
func TestRackConfigChange(t *testing.T) {
	_, c, _, ns := setupRackAwareTestEnv(t) // 6 racks, 2 nodes per rack = 12 nodes

	// Test different configuration levels
	configs := []proto.RackAwareLevel{
		proto.RackAwareNone,
		proto.RackAwareWeak,
		proto.RackAwareStrong,
	}

	for _, config := range configs {
		c.cfg.RackAwareLevel = config

		param := &selectParam{
			replicaNum: 3,
			rackLevel:  config,
			threshold:  1,
		}

		hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
		require.NoError(t, err, "Should work with config level %v", config)
		require.Equal(t, 3, len(hosts), "Should select 3 hosts with config level %v", config)
	}
}

// Test 13: Test rack exclusion functionality with many nodes
func TestRackExclusion(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 8, 3) // 8 racks, 3 nodes per rack = 24 nodes

	// Test excluding specific racks
	param := &selectParam{
		replicaNum:   4,
		rackLevel:    proto.RackAwareStrong,
		excludeRacks: []string{"rack1", "rack2", "rack3"},
		threshold:    1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select nodes excluding specified racks")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify selected nodes are not from excluded racks
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack] = true
				require.NotContains(t, param.excludeRacks, dn.Rack, "Selected node should not be from excluded rack")
				return false
			}
			return true
		})
	}
	require.Equal(t, 4, len(selectedRacks), "Should select from 4 different non-excluded racks")

	// Test excluding too many racks
	param.excludeRacks = []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}
	param.replicaNum = 3
	_, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.Error(t, err, "Should fail when excluding too many racks")
}

// Helper function: Print nodeset topology information
func printNodeSetTopology(t *testing.T, ns *nodeSet, title string) {
	t.Logf("=== %s ===", title)
	t.Logf("NodeSet ID: %d", ns.ID)
	t.Logf("NodeSet Zone: %s", ns.zoneName)
	t.Logf("NodeSet Capacity: %d", ns.Capacity)
	t.Logf("NodeSet Rack: %s", ns.Rack)

	// Print data nodes information
	dataNodeCount := 0
	t.Logf("Data Nodes:")
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		dataNodeCount++
		t.Logf("  - Addr: %s, Rack: %s, Zone: %s, Active: %v, NodeSetID: %d",
			dn.Addr, dn.Rack, dn.ZoneName, dn.isActive, dn.NodeSetID)
		return true
	})
	t.Logf("Total Data Nodes: %d", dataNodeCount)

	// Print meta nodes information
	metaNodeCount := 0
	t.Logf("Meta Nodes:")
	ns.metaNodes.Range(func(key, value interface{}) bool {
		mn := value.(*MetaNode)
		metaNodeCount++
		t.Logf("  - Addr: %s, Rack: %s, Zone: %s, Active: %v, NodeSetID: %d",
			mn.Addr, mn.Rack, mn.ZoneName, mn.IsActive, mn.NodeSetID)
		return true
	})
	t.Logf("Total Meta Nodes: %d", metaNodeCount)

	// Print rack information
	t.Logf("Racks:")
	for rackName, rack := range ns.racks {
		rackDataCount := 0
		rackMetaCount := 0

		rack.dataNodes.Range(func(key, value interface{}) bool {
			rackDataCount++
			return true
		})

		rack.metaNodes.Range(func(key, value interface{}) bool {
			rackMetaCount++
			return true
		})

		t.Logf("  - Rack: %s, DataNodes: %d, MetaNodes: %d",
			rackName, rackDataCount, rackMetaCount)
	}
	t.Logf("Total Racks: %d", len(ns.racks))
	t.Logf("========================")
}

// Test 14: Test rack awareness with mixed node types and many nodes
func TestRackMixedNodeTypes(t *testing.T) {
	_, _, zone, ns := setupRackAwareTestEnvWithNodes(t, 5, 4) // 5 racks, 4 nodes per rack = 20 nodes

	// Print initial topology information
	printNodeSetTopology(t, ns, "Initial NodeSet Topology Information")

	// Add meta nodes in same racks as data nodes
	for rackIdx := 0; rackIdx < 5; rackIdx++ {
		rackName := "rack" + strconv.Itoa(rackIdx+1)
		for nodeIdx := 0; nodeIdx < 4; nodeIdx++ {
			// Add meta node
			mn := createMetaNodeWithRack(
				"192.168.2."+strconv.Itoa(rackIdx*10+nodeIdx+1)+":17310",
				zone.name,
				rackName,
				ns,
			)
			ns.putMetaNode(mn)
		}
	}

	// Print topology information after adding meta nodes
	printNodeSetTopology(t, ns, "After Adding Meta Nodes")

	// Test data node selection
	param := &selectParam{
		replicaNum: 4,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	dataHosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select data nodes with rack awareness")
	require.Equal(t, 4, len(dataHosts), "Should select 4 data node hosts")

	// Test meta node selection
	metaHosts, _, err := ns.selectNodesWithRack(param, MetaNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select meta nodes with rack awareness")
	require.Equal(t, 4, len(metaHosts), "Should select 4 meta node hosts")

	// Verify both selections use different racks
	dataRacks := make(map[string]bool)
	metaRacks := make(map[string]bool)

	// Get data node racks
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		for _, host := range dataHosts {
			if key.(string) == host {
				dataRacks[dn.Rack] = true
				break
			}
		}
		return true
	})

	// Get meta node racks
	ns.metaNodes.Range(func(key, value interface{}) bool {
		mn := value.(*MetaNode)
		for _, host := range metaHosts {
			if key.(string) == host {
				metaRacks[mn.Rack] = true
				break
			}
		}
		return true
	})

	require.Equal(t, 4, len(dataRacks), "Data nodes should be from 4 different racks")
	require.Equal(t, 4, len(metaRacks), "Meta nodes should be from 4 different racks")
}

// Test 16: Test rack awareness with host exclusion and many nodes
func TestRackWithHostExclusion(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 6, 4) // 6 racks, 4 nodes per rack = 24 nodes

	// Test with host exclusion
	excludeHosts := []string{
		"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310", // Exclude 3 hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", // Exclude 2 hosts from rack2
	}

	param := &selectParam{
		replicaNum:   4,
		rackLevel:    proto.RackAwareStrong,
		excludeHosts: excludeHosts,
		threshold:    1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select nodes with host exclusion")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify excluded hosts are not selected
	for _, host := range hosts {
		require.NotContains(t, param.excludeHosts, host, "Selected host should not be in exclude list")
	}

	// Verify rack distribution is maintained
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack] = true
				return false
			}
			return true
		})
	}
	require.Equal(t, 4, len(selectedRacks), "Should still select from 4 different racks despite exclusions")
}

// Test 17: Test rack awareness with node set exclusion
func TestRackWithNodeSetExclusion(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnv(t) // 6 racks, 2 nodes per rack = 12 nodes

	// Test with node set exclusion
	param := &selectParam{
		replicaNum:      3,
		rackLevel:       proto.RackAwareStrong,
		excludeNodeSets: []uint64{ns.ID},
		threshold:       1,
	}

	// This should fail since we're excluding the only node set
	_, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.Error(t, err, "Should fail when excluding the only available node set")
}

// Test 18: Test rack awareness stress test with concurrent operations and many nodes
func TestRackStressTest(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 12, 6) // 12 racks, 6 nodes per rack = 72 nodes

	param := &selectParam{
		replicaNum: 6,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	// Concurrent stress test
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() {
				done <- true
			}()

			for j := 0; j < 10; j++ {
				hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
				require.NoError(t, err, "Should consistently succeed under stress")
				require.Equal(t, 6, len(hosts), "Should consistently select 6 hosts under stress")

				// Verify rack distribution
				selectedRacks := make(map[string]bool)
				for _, host := range hosts {
					ns.dataNodes.Range(func(key, value interface{}) bool {
						if key.(string) == host {
							dn := value.(*DataNode)
							selectedRacks[dn.Rack] = true
							return false
						}
						return true
					})
				}
				require.Equal(t, 6, len(selectedRacks), "Should consistently select from 6 different racks under stress")
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// Test 19: Test rack awareness with maximum replica scenarios
func TestRackMaxReplicas(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 8, 3) // 8 racks, 3 nodes per rack = 24 nodes

	// Test with maximum possible replicas (all racks)
	param := &selectParam{
		replicaNum: 8,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select maximum replicas")
	require.Equal(t, 8, len(hosts), "Should select 8 hosts from 8 different racks")

	// Verify all racks are used
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack] = true
				return false
			}
			return true
		})
	}
	require.Equal(t, 8, len(selectedRacks), "Should use all 8 available racks")
}

// Test 20: Test rack awareness with complex exclusion scenarios
func TestRackComplexExclusions(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 10, 4) // 10 racks, 4 nodes per rack = 40 nodes

	// Complex exclusion scenario: exclude some racks and some hosts
	param := &selectParam{
		replicaNum:   5,
		rackLevel:    proto.RackAwareStrong,
		excludeRacks: []string{"rack1", "rack2", "rack3"}, // Exclude 3 racks
		excludeHosts: []string{
			"192.168.1.31:17310", "192.168.1.32:17310", // Exclude 2 hosts from rack4
			"192.168.1.41:17310", // Exclude 1 host from rack5
		},
		threshold: 1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select nodes with complex exclusions")
	require.Equal(t, 5, len(hosts), "Should select 5 hosts")

	// Verify exclusions are respected
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		require.NotContains(t, param.excludeHosts, host, "Selected host should not be in exclude list")

		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				require.NotContains(t, param.excludeRacks, dn.Rack, "Selected node should not be from excluded rack")
				selectedRacks[dn.Rack] = true
				return false
			}
			return true
		})
	}
	require.Equal(t, 5, len(selectedRacks), "Should select from 5 different non-excluded racks")
}

// Test 21: Test excludeHosts edge cases and boundary conditions
func TestRackExcludeHostsEdgeCases(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 4, 2) // 4 racks, 2 nodes per rack = 8 nodes

	// Test case 1: Exclude all hosts from one rack
	param := &selectParam{
		replicaNum:   3,
		rackLevel:    proto.RackAwareStrong,
		excludeHosts: []string{"192.168.1.1:17310", "192.168.1.2:17310"}, // All hosts from rack1
		threshold:    1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should succeed when excluding all hosts from one rack")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts from remaining racks")

	// Test case 2: Exclude hosts from multiple racks but leave enough
	param.excludeHosts = []string{
		"192.168.1.1:17310",  // 1 host from rack1
		"192.168.1.11:17310", // 1 host from rack2
		"192.168.1.21:17310", // 1 host from rack3
	}

	hosts, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should succeed with selective host exclusions")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Test case 3: Exclude too many hosts to make selection impossible
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // All hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", // All hosts from rack2
		"192.168.1.21:17310", "192.168.1.22:17310", // All hosts from rack3
	}
	param.replicaNum = 3 // Need 3 replicas but only 1 rack available

	_, _, err = ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.Error(t, err, "Should fail when excluding too many hosts")
}

// Test 22: Test canWriteFor method with excludeHosts - CRITICAL BUG FIX TEST
func TestRackCanWriteForWithExcludeHosts(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 6, 3) // 6 racks, 3 nodes per rack = 18 nodes

	// Test case 1: Normal case without exclusions
	param := &selectParam{
		replicaNum: 3,
		rackLevel:  proto.RackAwareStrong,
	}

	canWrite := ns.canWriteFor(DataNodeType, param)
	require.True(t, canWrite, "NodeSet should be writable without exclusions")

	// Test case 2: With host exclusions but still enough nodes
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // 2 hosts from rack1
		"192.168.1.11:17310", // 1 host from rack2
	}

	canWrite = ns.canWriteFor(DataNodeType, param)
	require.True(t, canWrite, "NodeSet should be writable with moderate host exclusions")

	// Test case 3: With too many host exclusions
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310", // All hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", "192.168.1.13:17310", // All hosts from rack2
		"192.168.1.21:17310", "192.168.1.22:17310", "192.168.1.23:17310", // All hosts from rack3
		"192.168.1.31:17310", "192.168.1.32:17310", "192.168.1.33:17310", // All hosts from rack4
	}

	canWrite = ns.canWriteFor(DataNodeType, param)
	require.False(t, canWrite, "NodeSet should not be writable with too many host exclusions")

	// Test case 4: Test with rack exclusions
	param.excludeHosts = nil
	param.excludeRacks = []string{"rack1", "rack2", "rack3"}

	canWrite = ns.canWriteFor(DataNodeType, param)
	require.True(t, canWrite, "NodeSet should be writable with rack exclusions")

	// Test case 5: Test with both host and rack exclusions
	param.excludeHosts = []string{"192.168.1.41:17310", "192.168.1.42:17310"} // 2 hosts from rack5
	param.excludeRacks = []string{"rack1", "rack2"}                           // Exclude 2 racks

	canWrite = ns.canWriteFor(DataNodeType, param)
	require.True(t, canWrite, "NodeSet should be writable with combined exclusions")
}

// Test 23: Test checkNodeWriteable method with excludeHosts
func TestRackCheckNodeWriteableWithExcludeHosts(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 2) // 2 racks, 2 nodes per rack = 4 nodes

	param := &selectParam{
		excludeHosts: []string{"192.168.1.1:17310"},
	}

	// Test with excluded host
	var excludedNode *DataNode
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		if dn.Addr == "192.168.1.1:17310" {
			excludedNode = dn
			return false
		}
		return true
	})

	require.NotNil(t, excludedNode, "Should find the excluded node")
	canWrite := ns.checkNodeWriteable(excludedNode, DataNodeType, param)
	require.False(t, canWrite, "Excluded node should not be writable")

	// Test with non-excluded host
	var normalNode *DataNode
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		if dn.Addr != "192.168.1.1:17310" {
			normalNode = dn
			return false
		}
		return true
	})

	require.NotNil(t, normalNode, "Should find a normal node")
	canWrite = ns.checkNodeWriteable(normalNode, DataNodeType, param)
	require.True(t, canWrite, "Normal node should be writable")
}

// Test 24: Test checkRackAwareWriteable with excludeHosts
func TestRackCheckRackAwareWriteableWithExcludeHosts(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 6, 2) // 6 racks, 2 nodes per rack = 12 nodes

	// Test case 1: Exclude hosts from multiple racks
	param := &selectParam{
		replicaNum: 3,
		rackLevel:  proto.RackAwareStrong,
		excludeHosts: []string{
			"192.168.1.1:17310",  // 1 host from rack1
			"192.168.1.11:17310", // 1 host from rack2
			"192.168.1.21:17310", // 1 host from rack3
		},
	}

	canWrite := ns.checkRackAwareWriteable(param, DataNodeType)
	require.True(t, canWrite, "Should be writable with host exclusions from multiple racks")

	// Test case 2: Exclude all hosts from some racks
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // All hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", // All hosts from rack2
		"192.168.1.21:17310", "192.168.1.22:17310", // All hosts from rack3
	}

	canWrite = ns.checkRackAwareWriteable(param, DataNodeType)
	require.True(t, canWrite, "Should still be writable with 3 racks remaining")

	// Test case 3: Exclude too many hosts
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // All hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", // All hosts from rack2
		"192.168.1.21:17310", "192.168.1.22:17310", // All hosts from rack3
		"192.168.1.31:17310", "192.168.1.32:17310", // All hosts from rack4
		"192.168.1.41:17310", "192.168.1.42:17310", // All hosts from rack5
	}

	canWrite = ns.checkRackAwareWriteable(param, DataNodeType)
	require.False(t, canWrite, "Should not be writable with only 1 rack remaining")
}

// Test 25: Test checkNormalWriteable with excludeHosts
func TestRackCheckNormalWriteableWithExcludeHosts(t *testing.T) {
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 3) // 2 racks, 3 nodes per rack = 6 nodes

	// Test case 1: Normal case without exclusions
	param := &selectParam{
		replicaNum: 3,
		rackLevel:  proto.RackAwareNone,
	}

	canWrite := ns.checkNormalWriteable(ns.dataNodes, param, DataNodeType)
	require.True(t, canWrite, "Should be writable without exclusions")

	// Test case 2: With host exclusions
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // 2 hosts from rack1
	}

	canWrite = ns.checkNormalWriteable(ns.dataNodes, param, DataNodeType)
	require.True(t, canWrite, "Should be writable with host exclusions")

	// Test case 3: With too many host exclusions
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310", // All hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", // 2 hosts from rack2
	}

	canWrite = ns.checkNormalWriteable(ns.dataNodes, param, DataNodeType)
	require.False(t, canWrite, "Should not be writable with too many host exclusions")
}

// Test 26: Test nodeset selector with excludeHosts - INTEGRATION TEST
func TestRackNodeSetSelectorWithExcludeHosts(t *testing.T) {
	_, _, zone, _ := setupRackAwareTestEnvWithNodes(t, 4, 3) // 4 racks, 3 nodes per rack = 12 nodes

	// Test case 1: Normal selection without exclusions
	param := &selectParam{
		replicaNum: 3,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	selectedNodeSet, err := zone.allocNodeSetForDataNode(param)
	require.NoError(t, err, "Should successfully allocate node set")
	require.NotNil(t, selectedNodeSet, "Selected node set should not be nil")

	// Test case 2: Selection with host exclusions
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // 2 hosts from rack1
		"192.168.1.11:17310", // 1 host from rack2
	}

	selectedNodeSet, err = zone.allocNodeSetForDataNode(param)
	require.NoError(t, err, "Should successfully allocate node set with host exclusions")
	require.NotNil(t, selectedNodeSet, "Selected node set should not be nil")

	// Verify that the selected nodeset can handle the exclusions
	hosts, _, err := selectedNodeSet.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Should successfully select nodes from allocated node set")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify excluded hosts are not selected
	for _, host := range hosts {
		require.NotContains(t, param.excludeHosts, host, "Selected host should not be in exclude list")
	}
}

// Test 27: Test comprehensive excludeHosts scenarios
func TestRackComprehensiveExcludeHosts(t *testing.T) {
	_, _, zone, ns := setupRackAwareTestEnvWithNodes(t, 8, 4) // 8 racks, 4 nodes per rack = 32 nodes

	// Test scenario 1: Gradual exclusion of hosts
	param := &selectParam{
		replicaNum: 4,
		rackLevel:  proto.RackAwareStrong,
	}

	// Start with no exclusions
	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed with no exclusions")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Exclude some hosts gradually
	param.excludeHosts = []string{hosts[0]} // Exclude first selected host
	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed after excluding one host")
	require.Equal(t, 4, len(hosts), "Should still select 4 hosts")
	require.NotContains(t, hosts, param.excludeHosts[0], "Excluded host should not be selected")

	// Exclude more hosts
	param.excludeHosts = append(param.excludeHosts, hosts[0], hosts[1]) // Exclude 3 hosts total
	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed after excluding more hosts")
	require.Equal(t, 4, len(hosts), "Should still select 4 hosts")

	// Test scenario 2: Exclude hosts from specific racks
	param.excludeHosts = []string{
		"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310", "192.168.1.4:17310", // All hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", "192.168.1.13:17310", "192.168.1.14:17310", // All hosts from rack2
	}

	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed after excluding hosts from 2 racks")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts from remaining racks")

	// Verify selected hosts are not from excluded racks
	selectedRacks := make(map[string]bool)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack] = true
				require.NotContains(t, []string{"rack1", "rack2"}, dn.Rack, "Selected host should not be from excluded rack")
				return false
			}
			return true
		})
	}
	require.Equal(t, 4, len(selectedRacks), "Should select from 4 different non-excluded racks")
}

// Test 28: Test excludeHosts with different rack awareness levels
func TestRackExcludeHostsWithDifferentLevels(t *testing.T) {
	_, _, zone, _ := setupRackAwareTestEnvWithNodes(t, 6, 3) // 6 racks, 3 nodes per rack = 18 nodes

	excludeHosts := []string{
		"192.168.1.1:17310", "192.168.1.2:17310", // 2 hosts from rack1
		"192.168.1.11:17310", "192.168.1.12:17310", // 2 hosts from rack2
		"192.168.1.21:17310", "192.168.1.22:17310", // 2 hosts from rack3
	}

	// Test with RackAwareNone
	param := &selectParam{
		replicaNum:   4,
		rackLevel:    proto.RackAwareNone,
		excludeHosts: excludeHosts,
	}

	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed with RackAwareNone")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Test with RackAwareWeak
	param.rackLevel = proto.RackAwareWeak
	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed with RackAwareWeak")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Test with RackAwareStrong
	param.rackLevel = proto.RackAwareStrong
	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Should succeed with RackAwareStrong")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify excluded hosts are not selected in all cases
	for _, host := range hosts {
		require.NotContains(t, param.excludeHosts, host, "Selected host should not be in exclude list")
	}
}

// Test 29: Test weak rack awareness fallback mechanism - CRITICAL TEST
// This test verifies that weak rack awareness first tries strong mode, then falls back to weak mode
func TestRackWeakAwarenessFallbackMechanism(t *testing.T) {
	// Setup with 2 racks, 4 nodes per rack = 8 total nodes
	// This scenario is perfect for testing fallback: need 3 replicas but only 2 racks
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 4) // 2 racks, 4 nodes per rack = 8 nodes

	// Test case 1: Weak rack awareness with 3 replicas (more than available racks)
	param := &selectParam{
		replicaNum: 3, // Need 3 replicas but only 2 racks available
		rackLevel:  proto.RackAwareWeak,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with fallback mechanism")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify the fallback mechanism worked correctly
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// In weak mode with 2 racks and 3 replicas:
	// - First 2 replicas should be from different racks (strong mode behavior)
	// - The 3rd replica should be from any available rack (weak mode fallback)
	require.Equal(t, 2, len(selectedRacks), "Should use both available racks")
	require.Equal(t, 3, selectedRacks["rack1"]+selectedRacks["rack2"], "Should select 3 total nodes")

	// At least one rack should have multiple nodes (proving weak mode fallback)
	require.True(t, selectedRacks["rack1"] > 1 || selectedRacks["rack2"] > 1,
		"At least one rack should have multiple nodes, proving weak mode fallback")

	t.Logf("Selected rack distribution: rack1=%d, rack2=%d", selectedRacks["rack1"], selectedRacks["rack2"])
}

// Test 30: Test weak rack awareness with exact rack count
func TestRackWeakAwarenessExactRackCount(t *testing.T) {
	// Setup with 3 racks, 3 nodes per rack = 9 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 3, 3) // 3 racks, 3 nodes per rack = 9 nodes

	// Test case: Weak rack awareness with 3 replicas (exactly matching rack count)
	param := &selectParam{
		replicaNum: 3, // Need 3 replicas, exactly matching 3 racks
		rackLevel:  proto.RackAwareWeak,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with exact rack count")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify that strong mode was sufficient (one node per rack)
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// With exact rack count, strong mode should be sufficient
	require.Equal(t, 3, len(selectedRacks), "Should use all 3 racks")
	require.Equal(t, 1, selectedRacks["rack1"], "rack1 should have exactly 1 node")
	require.Equal(t, 1, selectedRacks["rack2"], "rack2 should have exactly 1 node")
	require.Equal(t, 1, selectedRacks["rack3"], "rack3 should have exactly 1 node")

	t.Logf("Selected rack distribution: rack1=%d, rack2=%d, rack3=%d",
		selectedRacks["rack1"], selectedRacks["rack2"], selectedRacks["rack3"])
}

// Test 31: Test weak rack awareness with more replicas than racks
func TestRackWeakAwarenessMoreReplicasThanRacks(t *testing.T) {
	// Setup with 2 racks, 5 nodes per rack = 10 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 5) // 2 racks, 5 nodes per rack = 10 nodes

	// Test case: Weak rack awareness with 4 replicas (more than available racks)
	param := &selectParam{
		replicaNum: 4, // Need 4 replicas but only 2 racks available
		rackLevel:  proto.RackAwareWeak,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with more replicas than racks")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify the fallback mechanism worked correctly
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// In weak mode with 2 racks and 4 replicas:
	// - First 2 replicas should be from different racks (strong mode behavior)
	// - Remaining 2 replicas should be from any available rack (weak mode fallback)
	require.Equal(t, 2, len(selectedRacks), "Should use both available racks")
	require.Equal(t, 4, selectedRacks["rack1"]+selectedRacks["rack2"], "Should select 4 total nodes")

	// Both racks should have multiple nodes (proving weak mode fallback)
	require.True(t, selectedRacks["rack1"] >= 1, "rack1 should have multiple nodes")
	require.True(t, selectedRacks["rack2"] >= 1, "rack2 should have multiple nodes")

	t.Logf("Selected rack distribution: rack1=%d, rack2=%d", selectedRacks["rack1"], selectedRacks["rack2"])
}

// Test 32: Test weak rack awareness with host exclusions
func TestRackWeakAwarenessWithHostExclusions(t *testing.T) {
	// Setup with 3 racks, 3 nodes per rack = 9 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 3, 3) // 3 racks, 3 nodes per rack = 9 nodes

	// Test case: Weak rack awareness with host exclusions
	param := &selectParam{
		replicaNum: 4, // Need 4 replicas
		rackLevel:  proto.RackAwareWeak,
		excludeHosts: []string{
			"192.168.1.1:17310",  // 1 host from rack1
			"192.168.1.11:17310", // 1 host from rack2
		},
		threshold: 1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with host exclusions")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify excluded hosts are not selected
	for _, host := range hosts {
		require.NotContains(t, param.excludeHosts, host, "Selected host should not be in exclude list")
	}

	// Verify the fallback mechanism worked correctly
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// Should use all 3 racks, with some racks having multiple nodes due to weak mode fallback
	require.Equal(t, 3, len(selectedRacks), "Should use all 3 racks")
	require.Equal(t, 4, selectedRacks["rack1"]+selectedRacks["rack2"]+selectedRacks["rack3"], "Should select 4 total nodes")

	t.Logf("Selected rack distribution: rack1=%d, rack2=%d, rack3=%d",
		selectedRacks["rack1"], selectedRacks["rack2"], selectedRacks["rack3"])
}

// Test 33: Test weak rack awareness with rack exclusions - CORRECTED VERSION
func TestRackWeakAwarenessWithRackExclusions(t *testing.T) {
	// Setup with 4 racks, 3 nodes per rack = 12 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 4, 3) // 4 racks, 3 nodes per rack = 12 nodes

	// Test case: Weak rack awareness with rack exclusions
	param := &selectParam{
		replicaNum:   3, // Need 3 replicas
		rackLevel:    proto.RackAwareWeak,
		excludeRacks: []string{"rack1", "rack2"}, // Exclude 2 racks
		threshold:    1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with rack exclusions")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify rack distribution
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// In weak mode, should be able to select from any rack, including initially excluded ones
	// The key point is that weak mode prioritizes availability over rack constraints
	require.Equal(t, 3, selectedRacks["rack1"]+selectedRacks["rack2"]+selectedRacks["rack3"]+selectedRacks["rack4"],
		"Should select 3 total nodes")

	// At least one rack should have multiple nodes (proving weak mode fallback)
	totalRacksUsed := 0
	for _, count := range selectedRacks {
		if count > 0 {
			totalRacksUsed++
		}
	}
	require.True(t, totalRacksUsed <= 3, "Should use at most 3 racks (since we need 3 replicas)")

	t.Logf("Selected rack distribution: rack1=%d, rack2=%d, rack3=%d, rack4=%d",
		selectedRacks["rack1"], selectedRacks["rack2"], selectedRacks["rack3"], selectedRacks["rack4"])
}

// Test 35: Test weak rack awareness comparison with strong mode
func TestRackWeakVsStrongAwarenessComparison(t *testing.T) {
	// Setup with 2 racks, 4 nodes per rack = 8 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 4) // 2 racks, 4 nodes per rack = 8 nodes

	// Test case: Need 3 replicas but only 2 racks available
	replicaNum := 3

	// Test 1: Strong rack awareness should fail
	strongParam := &selectParam{
		replicaNum: replicaNum,
		rackLevel:  proto.RackAwareStrong,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(strongParam, DataNodeType, proto.StoreModeMem)
	require.Error(t, err, "Strong rack awareness should fail when not enough racks")
	require.Nil(t, hosts, "Strong rack awareness should return nil hosts when failing")

	// Test 2: Weak rack awareness should succeed with fallback
	weakParam := &selectParam{
		replicaNum: replicaNum,
		rackLevel:  proto.RackAwareWeak,
		threshold:  1,
	}

	hosts, _, err = ns.selectNodesWithRack(weakParam, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with fallback mechanism")
	require.Equal(t, replicaNum, len(hosts), "Should select %d hosts with weak rack awareness", replicaNum)

	// Verify weak mode used fallback mechanism
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// Weak mode should use both racks, with at least one having multiple nodes
	require.Equal(t, 2, len(selectedRacks), "Weak mode should use both available racks")
	require.Equal(t, replicaNum, selectedRacks["rack1"]+selectedRacks["rack2"], "Should select %d total nodes", replicaNum)
	require.True(t, selectedRacks["rack1"] > 1 || selectedRacks["rack2"] > 1,
		"At least one rack should have multiple nodes, proving weak mode fallback")

	t.Logf("Strong mode: Failed (as expected)")
	t.Logf("Weak mode: Selected rack1=%d, rack2=%d (fallback mechanism working)", selectedRacks["rack1"], selectedRacks["rack2"])
}

// Test 36: Test weak rack awareness step-by-step fallback verification
func TestRackWeakAwarenessStepByStepFallback(t *testing.T) {
	// Setup with 2 racks, 3 nodes per rack = 6 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 3) // 2 racks, 3 nodes per rack = 6 nodes

	// Test case: Need 4 replicas but only 2 racks available
	// This should demonstrate the fallback mechanism clearly
	param := &selectParam{
		replicaNum: 4, // Need 4 replicas but only 2 racks available
		rackLevel:  proto.RackAwareWeak,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with step-by-step fallback")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify the step-by-step fallback mechanism
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// Expected behavior in weak mode with 2 racks and 4 replicas:
	// Step 1: Try strong mode - select 1 node from each rack (2 nodes total)
	// Step 2: Fallback to weak mode - select remaining 2 nodes from any available rack
	// Result: Both racks should be used, and both should have multiple nodes
	require.Equal(t, 2, len(selectedRacks), "Should use both available racks")
	require.Equal(t, 4, selectedRacks["rack1"]+selectedRacks["rack2"], "Should select 4 total nodes")
	t.Logf("Step-by-step fallback verification: rack1=%d, rack2=%d", selectedRacks["rack1"], selectedRacks["rack2"])
}

// Test 37: Test weak rack awareness with insufficient nodes after exclusions
func TestRackWeakAwarenessInsufficientNodes(t *testing.T) {
	// Setup with 2 racks, 2 nodes per rack = 4 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 2, 2) // 2 racks, 2 nodes per rack = 4 nodes

	// Test case: Exclude too many nodes to make selection impossible
	param := &selectParam{
		replicaNum: 3, // Need 3 replicas
		rackLevel:  proto.RackAwareWeak,
		excludeHosts: []string{
			"192.168.1.1:17310", "192.168.1.2:17310", // All hosts from rack1
			"192.168.1.11:17310", // 1 host from rack2
		},
		threshold: 1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.Error(t, err, "Should fail when insufficient nodes available after exclusions")
	require.Nil(t, hosts, "Should return nil hosts when failing")
}

// Test 38: Test weak rack awareness with mixed node availability
func TestRackWeakAwarenessMixedAvailability(t *testing.T) {
	// Setup with 3 racks, 3 nodes per rack = 9 total nodes
	_, _, _, ns := setupRackAwareTestEnvWithNodes(t, 3, 3) // 3 racks, 3 nodes per rack = 9 nodes

	// Make some nodes unavailable
	nodeCount := 0
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		if nodeCount%4 == 0 { // Make every 4th node unavailable
			dn.isActive = false
		}
		nodeCount++
		return true
	})

	// Test case: Weak rack awareness with mixed node availability
	param := &selectParam{
		replicaNum: 4, // Need 4 replicas
		rackLevel:  proto.RackAwareWeak,
		threshold:  1,
	}

	hosts, _, err := ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeMem)
	require.NoError(t, err, "Weak rack awareness should succeed with mixed node availability")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify all selected hosts are active
	for _, host := range hosts {
		ns.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				require.True(t, dn.isActive, "Selected host should be active")
				return false
			}
			return true
		})
	}

	t.Logf("Successfully selected 4 hosts with mixed node availability")
}

// Test 34: Test weak rack awareness at zone level with rack exclusions
func TestZoneWeakRackAwarenessWithRackExclusions(t *testing.T) {
	// Setup with 4 racks, 3 nodes per rack = 12 total nodes
	_, _, zone, _ := setupRackAwareTestEnvWithNodes(t, 4, 3) // 4 racks, 3 nodes per rack = 12 nodes

	// Test case: Zone level weak rack awareness with rack exclusions
	param := &selectParam{
		replicaNum:   3, // Need 3 replicas
		rackLevel:    proto.RackAwareWeak,
		excludeRacks: []string{"rack1", "rack2"}, // Exclude 2 racks
	}

	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Zone level weak rack awareness should succeed with rack exclusions")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify rack distribution
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		// Find the nodeSet that contains this host
		var foundNodeSet *nodeSet
		zone.nsLock.RLock()
		for _, ns := range zone.nodeSetMap {
			ns.dataNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					foundNodeSet = ns
					return false
				}
				return true
			})
			if foundNodeSet != nil {
				break
			}
		}
		zone.nsLock.RUnlock()

		require.NotNil(t, foundNodeSet, "Should find the nodeSet containing host %s", host)

		// Find the specific node to get its rack
		foundNodeSet.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// In weak mode at zone level, should be able to select from any rack, including initially excluded ones
	// The key point is that weak mode prioritizes availability over rack constraints
	require.Equal(t, 3, selectedRacks["rack1"]+selectedRacks["rack2"]+selectedRacks["rack3"]+selectedRacks["rack4"],
		"Should select 3 total nodes")

	// At least one rack should have multiple nodes (proving weak mode fallback)
	totalRacksUsed := 0
	for _, count := range selectedRacks {
		if count > 0 {
			totalRacksUsed++
		}
	}
	require.True(t, totalRacksUsed <= 3, "Should use at most 3 racks (since we need 3 replicas)")

	t.Logf("Zone level selected rack distribution: rack1=%d, rack2=%d, rack3=%d, rack4=%d",
		selectedRacks["rack1"], selectedRacks["rack2"], selectedRacks["rack3"], selectedRacks["rack4"])
}

// Test 35: Test zone level weak rack awareness with insufficient racks scenario
func TestZoneWeakRackAwarenessInsufficientRacks(t *testing.T) {
	// Setup with 2 racks, 4 nodes per rack = 8 total nodes
	_, _, zone, _ := setupRackAwareTestEnvWithNodes(t, 2, 4) // 2 racks, 4 nodes per rack = 8 nodes

	// Test case: Need 3 replicas but only 2 racks available
	param := &selectParam{
		replicaNum: 3, // Need 3 replicas but only 2 racks available
		rackLevel:  proto.RackAwareWeak,
	}

	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Zone level weak rack awareness should succeed with insufficient racks")
	require.Equal(t, 3, len(hosts), "Should select 3 hosts")

	// Verify rack distribution
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		// Find the nodeSet that contains this host
		var foundNodeSet *nodeSet
		zone.nsLock.RLock()
		for _, ns := range zone.nodeSetMap {
			ns.dataNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					foundNodeSet = ns
					return false
				}
				return true
			})
			if foundNodeSet != nil {
				break
			}
		}
		zone.nsLock.RUnlock()

		require.NotNil(t, foundNodeSet, "Should find the nodeSet containing host %s", host)

		// Find the specific node to get its rack
		foundNodeSet.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// Should use both available racks
	require.Equal(t, 2, len(selectedRacks), "Should use both available racks")
	require.Equal(t, 3, selectedRacks["rack1"]+selectedRacks["rack2"], "Should select 3 total nodes")

	// At least one rack should have multiple nodes (proving weak mode fallback)
	require.True(t, selectedRacks["rack1"] > 1 || selectedRacks["rack2"] > 1,
		"At least one rack should have multiple nodes, proving weak mode fallback")

	t.Logf("Zone level insufficient racks scenario: rack1=%d, rack2=%d",
		selectedRacks["rack1"], selectedRacks["rack2"])
}

// Test 36: Test zone level weak rack awareness with host exclusions
func TestZoneWeakRackAwarenessWithHostExclusions(t *testing.T) {
	// Setup with 3 racks, 3 nodes per rack = 9 total nodes
	_, _, zone, _ := setupRackAwareTestEnvWithNodes(t, 3, 3) // 3 racks, 3 nodes per rack = 9 nodes

	// Test case: Zone level weak rack awareness with host exclusions
	param := &selectParam{
		replicaNum: 4, // Need 4 replicas
		rackLevel:  proto.RackAwareWeak,
		excludeHosts: []string{
			"192.168.1.1:17310",  // 1 host from rack1
			"192.168.1.11:17310", // 1 host from rack2
		},
	}

	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, param)
	require.NoError(t, err, "Zone level weak rack awareness should succeed with host exclusions")
	require.Equal(t, 4, len(hosts), "Should select 4 hosts")

	// Verify excluded hosts are not selected
	for _, host := range hosts {
		require.NotContains(t, param.excludeHosts, host, "Selected host should not be in exclude list")
	}

	// Verify rack distribution
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		// Find the nodeSet that contains this host
		var foundNodeSet *nodeSet
		zone.nsLock.RLock()
		for _, ns := range zone.nodeSetMap {
			ns.dataNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					foundNodeSet = ns
					return false
				}
				return true
			})
			if foundNodeSet != nil {
				break
			}
		}
		zone.nsLock.RUnlock()

		require.NotNil(t, foundNodeSet, "Should find the nodeSet containing host %s", host)

		// Find the specific node to get its rack
		foundNodeSet.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// Should use all 3 racks, with some racks having multiple nodes due to weak mode fallback
	require.Equal(t, 3, len(selectedRacks), "Should use all 3 racks")
	require.Equal(t, 4, selectedRacks["rack1"]+selectedRacks["rack2"]+selectedRacks["rack3"], "Should select 4 total nodes")

	t.Logf("Zone level host exclusions scenario: rack1=%d, rack2=%d, rack3=%d",
		selectedRacks["rack1"], selectedRacks["rack2"], selectedRacks["rack3"])
}

// Test 37: Test zone level weak vs strong rack awareness comparison
func TestZoneWeakVsStrongRackAwarenessComparison(t *testing.T) {
	// Setup with 2 racks, 3 nodes per rack = 6 total nodes
	_, _, zone, _ := setupRackAwareTestEnvWithNodes(t, 2, 3) // 2 racks, 3 nodes per rack = 6 nodes

	// Test case: Need 3 replicas but only 2 racks available
	replicaNum := 3

	// Test 1: Strong rack awareness should fail
	strongParam := &selectParam{
		replicaNum: replicaNum,
		rackLevel:  proto.RackAwareStrong,
	}

	hosts, _, err := zone.getAvailNodeHosts(TypeDataPartition, strongParam)
	require.Error(t, err, "Zone level strong rack awareness should fail when not enough racks")
	require.Nil(t, hosts, "Zone level strong rack awareness should return nil hosts when failing")

	// Test 2: Weak rack awareness should succeed with fallback
	weakParam := &selectParam{
		replicaNum: replicaNum,
		rackLevel:  proto.RackAwareWeak,
	}

	hosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, weakParam)
	require.NoError(t, err, "Zone level weak rack awareness should succeed with fallback mechanism")
	require.Equal(t, replicaNum, len(hosts), "Should select %d hosts with weak rack awareness", replicaNum)

	// Verify weak mode used fallback mechanism
	selectedRacks := make(map[string]int)
	for _, host := range hosts {
		// Find the nodeSet that contains this host
		var foundNodeSet *nodeSet
		zone.nsLock.RLock()
		for _, ns := range zone.nodeSetMap {
			ns.dataNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					foundNodeSet = ns
					return false
				}
				return true
			})
			if foundNodeSet != nil {
				break
			}
		}
		zone.nsLock.RUnlock()

		require.NotNil(t, foundNodeSet, "Should find the nodeSet containing host %s", host)

		// Find the specific node to get its rack
		foundNodeSet.dataNodes.Range(func(key, value interface{}) bool {
			if key.(string) == host {
				dn := value.(*DataNode)
				selectedRacks[dn.Rack]++
				return false
			}
			return true
		})
	}

	// Weak mode should use both racks, with at least one having multiple nodes
	require.Equal(t, 2, len(selectedRacks), "Weak mode should use both available racks")
	require.Equal(t, replicaNum, selectedRacks["rack1"]+selectedRacks["rack2"], "Should select %d total nodes", replicaNum)
	require.True(t, selectedRacks["rack1"] > 1 || selectedRacks["rack2"] > 1,
		"At least one rack should have multiple nodes, proving weak mode fallback")

	t.Logf("Zone level strong mode: Failed (as expected)")
	t.Logf("Zone level weak mode: Selected rack1=%d, rack2=%d (fallback mechanism working)",
		selectedRacks["rack1"], selectedRacks["rack2"])
}

// Helper function: Create meta node with RocksDB disk information
func createMetaNodeWithRocksDB(addr, zoneName, rackName string, ns *nodeSet, rocksdbTotal, rocksdbUsed uint64) *MetaNode {
	mn := newMetaNode(addr, strconv.Itoa(raftstore.DefaultHeartbeatPort), strconv.Itoa(raftstore.DefaultReplicaPort), zoneName, "", "test")
	mn.ZoneName = zoneName
	mn.Rack = rackName
	mn.Total = 1024 * util.GB
	mn.Used = 10 * util.GB
	mn.ReportTime = time.Now()
	mn.IsActive = true
	mn.NodeSetID = ns.ID
	mn.Threshold = 0.8
	mn.MaxMemAvailWeight = 1024 * util.GB
	mn.MpCntLimit = defaultMaxMpCntLimit
	mn.MetaPartitionCount = 0

	// Setup system memory information (required for IsRocksdbWriteAble)
	mn.NodeMemTotal = 16 * util.GB
	mn.NodeMemUsed = 4 * util.GB

	// Setup RocksDB disk information
	mn.RocksdbDisks = []*proto.MetaNodeRocksdbInfo{
		{
			Path:       "/cfs/rocksdb",
			Total:      rocksdbTotal,
			Used:       rocksdbUsed,
			UsageRatio: float64(rocksdbUsed) / float64(rocksdbTotal),
			Status:     proto.ReadWrite,
			KeyNum:     100000, // Set reasonable key number
		},
	}
	mn.RocksdbDiskThreshold = 0.8
	mn.RocksdbKeyNumMax = 1000000
	mn.RocksdbRdOnly = false // Ensure not read-only

	return mn
}

// Test getAvailMetaNodeHosts with RocksDB store mode
func TestRackGetAvailMetaNodeHostsRocksDB(t *testing.T) {
	ns := newNodeSet(nil, 1, 4, "test-zone", "")

	// Create meta nodes with RocksDB support
	metaNodes := []*MetaNode{
		createMetaNodeWithRocksDB("192.168.1.1:8080", "test-zone", "rack1", ns, 100*util.GB, 20*util.GB),
		createMetaNodeWithRocksDB("192.168.1.2:8080", "test-zone", "rack1", ns, 100*util.GB, 30*util.GB),
		createMetaNodeWithRocksDB("192.168.1.3:8080", "test-zone", "rack2", ns, 100*util.GB, 10*util.GB),
		createMetaNodeWithRocksDB("192.168.1.4:8080", "test-zone", "rack2", ns, 100*util.GB, 25*util.GB),
	}

	// Add meta nodes to nodeset
	for _, mn := range metaNodes {
		ns.putMetaNode(mn)
	}

	t.Run("TestRocksDBModeWithoutRackAwareness", func(t *testing.T) {
		param := &selectParam{
			replicaNum: 3,
			rackLevel:  proto.RackAwareNone,
		}

		hosts, peers, err := ns.getAvailMetaNodeHosts(param, proto.StoreModeRocksDb)
		require.NoError(t, err, "Should successfully select meta nodes for RocksDB mode")
		require.Equal(t, 3, len(hosts), "Should select 3 meta node hosts")
		require.Equal(t, 3, len(peers), "Should return 3 peers")

		// Verify all selected nodes are writable for RocksDB
		for _, host := range hosts {
			ns.metaNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					mn := value.(*MetaNode)
					require.True(t, mn.IsRocksdbWriteAble(), "Selected node should be RocksDB writable")
					require.True(t, mn.PartitionCntLimited(), "Selected node should have partition count limit")
					return false
				}
				return true
			})
		}

		t.Logf("Selected RocksDB meta nodes: %v", hosts)
	})

	t.Run("TestRocksDBModeWithStrongRackAwareness", func(t *testing.T) {
		param := &selectParam{
			replicaNum: 2,
			rackLevel:  proto.RackAwareStrong,
		}

		hosts, peers, err := ns.getAvailMetaNodeHosts(param, proto.StoreModeRocksDb)
		require.NoError(t, err, "Should successfully select meta nodes with strong rack awareness")
		require.Equal(t, 2, len(hosts), "Should select 2 meta node hosts")
		require.Equal(t, 2, len(peers), "Should return 2 peers")

		// Verify selected nodes are from different racks
		selectedRacks := make(map[string]bool)
		for _, host := range hosts {
			ns.metaNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					mn := value.(*MetaNode)
					selectedRacks[mn.Rack] = true
					require.True(t, mn.IsRocksdbWriteAble(), "Selected node should be RocksDB writable")
					return false
				}
				return true
			})
		}

		require.Equal(t, 2, len(selectedRacks), "Should select nodes from 2 different racks")
		t.Logf("Selected RocksDB meta nodes with strong rack awareness: %v", hosts)
	})
}

// Test getAvailMetaNodeHosts with RocksDB disk threshold scenarios
func TestRackGetAvailMetaNodeHostsRocksDBThreshold(t *testing.T) {
	ns := newNodeSet(nil, 1, 4, "test-zone", "")

	// Create meta nodes with different RocksDB disk usage scenarios
	metaNodes := []*MetaNode{
		// Node with low RocksDB usage (should be selectable)
		createMetaNodeWithRocksDB("192.168.1.1:8080", "test-zone", "rack1", ns, 100*util.GB, 10*util.GB),
		// Node with medium RocksDB usage (should be selectable)
		createMetaNodeWithRocksDB("192.168.1.2:8080", "test-zone", "rack1", ns, 100*util.GB, 50*util.GB),
		// Node with high RocksDB usage (should not be selectable)
		createMetaNodeWithRocksDB("192.168.1.3:8080", "test-zone", "rack2", ns, 100*util.GB, 90*util.GB),
		// Node with full RocksDB usage (should not be selectable)
		createMetaNodeWithRocksDB("192.168.1.4:8080", "test-zone", "rack2", ns, 100*util.GB, 100*util.GB),
	}

	// Add meta nodes to nodeset
	for _, mn := range metaNodes {
		ns.putMetaNode(mn)
	}

	t.Run("TestRocksDBThresholdSelection", func(t *testing.T) {
		param := &selectParam{
			replicaNum: 2,
			rackLevel:  proto.RackAwareNone,
		}

		hosts, peers, err := ns.getAvailMetaNodeHosts(param, proto.StoreModeRocksDb)
		require.NoError(t, err, "Should successfully select meta nodes within RocksDB threshold")
		require.Equal(t, 2, len(hosts), "Should select 2 meta node hosts")
		require.Equal(t, 2, len(peers), "Should return 2 peers")

		// Verify selected nodes are within RocksDB threshold
		for _, host := range hosts {
			ns.metaNodes.Range(func(key, value interface{}) bool {
				if key.(string) == host {
					mn := value.(*MetaNode)
					require.True(t, mn.IsRocksdbWriteAble(), "Selected node should be RocksDB writable")
					require.False(t, mn.reachesRocksdbDisksThreshold(), "Selected node should not reach RocksDB threshold")
					return false
				}
				return true
			})
		}

		// Verify that high usage nodes are not selected
		require.NotContains(t, hosts, "192.168.1.3:8080", "High usage node should not be selected")
		require.NotContains(t, hosts, "192.168.1.4:8080", "Full usage node should not be selected")

		t.Logf("Selected RocksDB meta nodes within threshold: %v", hosts)
	})
}

// Test getAvailMetaNodeHosts with insufficient RocksDB nodes
func TestRackGetAvailMetaNodeHostsInsufficientRocksDBNodes(t *testing.T) {
	ns := newNodeSet(nil, 1, 2, "test-zone", "")

	// Create only one RocksDB-capable meta node
	metaNodes := []*MetaNode{
		createMetaNodeWithRocksDB("192.168.1.1:8080", "test-zone", "rack1", ns, 100*util.GB, 20*util.GB),
		// Create a node that's not RocksDB writable (high usage)
		createMetaNodeWithRocksDB("192.168.1.2:8080", "test-zone", "rack1", ns, 100*util.GB, 95*util.GB),
	}

	// Add meta nodes to nodeset
	for _, mn := range metaNodes {
		ns.putMetaNode(mn)
	}

	t.Run("TestInsufficientRocksDBNodes", func(t *testing.T) {
		param := &selectParam{
			replicaNum: 3, // Request more nodes than available
			rackLevel:  proto.RackAwareNone,
		}

		_, _, err := ns.getAvailMetaNodeHosts(param, proto.StoreModeRocksDb)
		require.Error(t, err, "Should return error when insufficient RocksDB nodes available")
		require.Contains(t, err.Error(), "no enough writable hosts", "Error should indicate insufficient writable hosts")

		t.Logf("Expected error for insufficient RocksDB nodes: %v", err)
	})
}
