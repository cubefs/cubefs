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

package master

import (
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestGetDpNodesetDistribution(t *testing.T) {
	// Create a test cluster with mock data nodes
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
		},
	}

	// Add mock data nodes
	mockNodes := map[string]*DataNode{
		"192.168.1.1:17310": {Addr: "192.168.1.1:17310", Rack: "rack1", ZoneName: "zone1", NodeSetID: 1},
		"192.168.1.2:17310": {Addr: "192.168.1.2:17310", Rack: "rack2", ZoneName: "zone1", NodeSetID: 1},
		"192.168.1.3:17310": {Addr: "192.168.1.3:17310", Rack: "rack3", ZoneName: "zone1", NodeSetID: 2},
		"192.168.1.4:17310": {Addr: "192.168.1.4:17310", Rack: "rack1", ZoneName: "zone1", NodeSetID: 3},
	}

	for addr, node := range mockNodes {
		cluster.dataNodes.Store(addr, node)
	}

	tests := []struct {
		name        string
		replicas    []*DataReplica
		expectCount map[string]map[uint64]int
		expectBal   bool
	}{
		{
			name: "single nodeset balanced",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
			},
			expectCount: map[string]map[uint64]int{
				"zone1": {1: 2},
			},
			expectBal: true,
		},
		{
			name: "multiple nodeset unbalanced",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
			},
			expectCount: map[string]map[uint64]int{
				"zone1": {1: 2, 2: 1},
			},
			expectBal: false,
		},
		{
			name: "three nodeset unbalanced",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: mockNodes["192.168.1.4:17310"]},
			},
			expectCount: map[string]map[uint64]int{
				"zone1": {1: 1, 2: 1, 3: 1},
			},
			expectBal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := &DataPartition{
				Replicas: tt.replicas,
			}

			count, balanced := getDpNodesetDistribution(dp)

			require.Equal(t, tt.expectCount, count, "NodeSet distribution count mismatch")
			require.Equal(t, tt.expectBal, balanced, "NodeSet balance status mismatch")
		})
	}
}

func TestGetRackConflictLevel(t *testing.T) {
	// Create a test cluster with mock data nodes
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
		},
	}

	// Add mock data nodes with different rack configurations
	mockNodes := map[string]*DataNode{
		"192.168.1.1:17310": {Addr: "192.168.1.1:17310", Rack: "rack1", ZoneName: "zone1"},
		"192.168.1.2:17310": {Addr: "192.168.1.2:17310", Rack: "rack2", ZoneName: "zone1"},
		"192.168.1.3:17310": {Addr: "192.168.1.3:17310", Rack: "rack3", ZoneName: "zone1"},
		"192.168.1.4:17310": {Addr: "192.168.1.4:17310", Rack: "rack1", ZoneName: "zone1"},
	}

	for addr, node := range mockNodes {
		cluster.dataNodes.Store(addr, node)
	}

	tests := []struct {
		name           string
		hosts          []string
		expectConflict bool
		expectLevel    int
	}{
		{
			name:           "no rack conflict",
			hosts:          []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			expectConflict: true, // noConflict = true
			expectLevel:    0,    // no conflict
		},
		{
			name:           "minor rack conflict",
			hosts:          []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.2:17310"},
			expectConflict: false, // noConflict = false
			expectLevel:    1,     // minor conflict (2 in same rack)
		},
		{
			name:           "major rack conflict",
			hosts:          []string{"192.168.1.1:17310", "192.168.1.4:17310", "192.168.1.1:17310"},
			expectConflict: false, // noConflict = false
			expectLevel:    2,     // major conflict (3 in same rack)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := &DataPartition{
				Hosts: tt.hosts,
			}

			level := getRackConflictLevel(dp.Hosts, cluster)
			noConflict := (level == 0)

			require.Equal(t, tt.expectConflict, noConflict, "Rack conflict detection mismatch")
			require.Equal(t, tt.expectLevel, level, "Rack conflict level mismatch")
		})
	}
}

func TestIsOptimalDistribution(t *testing.T) {
	// Create a test cluster
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
		},
		cfg: &clusterConfig{
			RackAwareLevel: proto.RackAwareStrong,
		},
	}

	// Add mock data nodes
	mockNodes := map[string]*DataNode{
		"192.168.1.1:17310": {Addr: "192.168.1.1:17310", Rack: "rack1", ZoneName: "zone1", NodeSetID: 1},
		"192.168.1.2:17310": {Addr: "192.168.1.2:17310", Rack: "rack2", ZoneName: "zone1", NodeSetID: 1},
		"192.168.1.3:17310": {Addr: "192.168.1.3:17310", Rack: "rack3", ZoneName: "zone1", NodeSetID: 2},
		"192.168.1.4:17310": {Addr: "192.168.1.4:17310", Rack: "rack1", ZoneName: "zone1", NodeSetID: 1},
		"192.168.2.1:17310": {Addr: "192.168.2.1:17310", Rack: "rack1", ZoneName: "zone2", NodeSetID: 3},
	}

	for addr, node := range mockNodes {
		cluster.dataNodes.Store(addr, node)
	}

	tests := []struct {
		name      string
		replicas  []*DataReplica
		rackLevel proto.RackAwareLevel
		expectOpt bool
	}{
		{
			name: "optimal: single nodeset, no rack conflict",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
			},
			rackLevel: proto.RackAwareStrong,
			expectOpt: true,
		},
		{
			name: "not optimal: multiple nodeset",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
			},
			rackLevel: proto.RackAwareStrong,
			expectOpt: false,
		},
		{
			name: "not optimal: single nodeset but rack conflict",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: mockNodes["192.168.1.4:17310"]},
			},
			rackLevel: proto.RackAwareStrong,
			expectOpt: false,
		},
		{
			name: "optimal: rack awareness disabled",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: mockNodes["192.168.1.4:17310"]},
			},
			rackLevel: proto.RackAwareNone,
			expectOpt: true,
		},
		{
			name: "cross-zone with intra-zone imbalance: 2 in zone1 (different nodesets) + 1 in zone2",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.2.1:17310"}, dataNode: mockNodes["192.168.2.1:17310"]},
			},
			rackLevel: proto.RackAwareStrong,
			expectOpt: false, // zone1 has 2 replicas in different nodesets, needs optimization
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster.cfg.RackAwareLevel = tt.rackLevel

			dp := &DataPartition{
				Replicas: tt.replicas,
			}

			_, optimal := isOptimalDistribution(dp, cluster)

			require.Equal(t, tt.expectOpt, optimal, "Optimal distribution check mismatch")
		})
	}
}

func TestHasRackConflict(t *testing.T) {
	tests := []struct {
		name           string
		replicas       []*DataReplica
		expectConflict bool
	}{
		{
			name: "no conflict - different racks",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: &DataNode{Addr: "192.168.1.1:17310", Rack: "rack1", ZoneName: "zone1"}},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: &DataNode{Addr: "192.168.1.2:17310", Rack: "rack2", ZoneName: "zone1"}},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: &DataNode{Addr: "192.168.1.3:17310", Rack: "rack3", ZoneName: "zone1"}},
			},
			expectConflict: false,
		},
		{
			name: "conflict - 2 in same rack",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: &DataNode{Addr: "192.168.1.1:17310", Rack: "rack1", ZoneName: "zone1"}},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: &DataNode{Addr: "192.168.1.4:17310", Rack: "rack1", ZoneName: "zone1"}},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: &DataNode{Addr: "192.168.1.2:17310", Rack: "rack2", ZoneName: "zone1"}},
			},
			expectConflict: true,
		},
		{
			name: "conflict - 3 in same rack",
			replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: &DataNode{Addr: "192.168.1.1:17310", Rack: "rack1", ZoneName: "zone1"}},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: &DataNode{Addr: "192.168.1.4:17310", Rack: "rack1", ZoneName: "zone1"}},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.5:17310"}, dataNode: &DataNode{Addr: "192.168.1.5:17310", Rack: "rack1", ZoneName: "zone1"}},
			},
			expectConflict: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dp := &DataPartition{
				Replicas:   tt.replicas,
				ReplicaNum: uint8(len(tt.replicas)),
			}

			conflict, _ := hasRackConflict(dp)

			require.Equal(t, tt.expectConflict, conflict, "Rack conflict detection mismatch")
		})
	}
}

func TestCountActiveDistributionOptimizationTasks(t *testing.T) {
	// Create a test cluster with volumes and data partitions
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
	}

	// Create test volume
	vol := &Vol{
		Name:           "test-vol",
		dataPartitions: newDataPartitionMap("test-vol"),
	}

	// Add test data partitions with different states
	partitions := []*DataPartition{
		{
			PartitionID:        1,
			DecommissionType:   proto.DistributionOptimization,
			DecommissionStatus: DecommissionRunning,
			IsDiscard:          false,
		},
		{
			PartitionID:        2,
			DecommissionType:   proto.DistributionOptimization,
			DecommissionStatus: DecommissionPrepare,
			IsDiscard:          false,
		},
		{
			PartitionID:        3,
			DecommissionType:   proto.DistributionOptimization,
			DecommissionStatus: DecommissionFail,
			IsDiscard:          false,
		},
		{
			PartitionID:        4,
			DecommissionType:   proto.ManualDecommission,
			DecommissionStatus: DecommissionRunning,
			IsDiscard:          false,
		},
		{
			PartitionID:        5,
			DecommissionType:   proto.DistributionOptimization,
			DecommissionStatus: DecommissionRunning,
			IsDiscard:          true, // discarded
		},
	}

	for _, dp := range partitions {
		vol.dataPartitions.put(dp)
	}

	cluster.vols["test-vol"] = vol

	// Test counting active tasks
	count := cluster.countActiveDistributionOptimizationTasks()

	// Should count partitions 1 and 2 (DistributionOptimization type, not failed, not discarded)
	require.Equal(t, 2, count, "Active distribution optimization task count mismatch")
}

func TestGetDistributionOptimizationStatus(t *testing.T) {
	// Create a test cluster
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
		ClusterTopoSubItem: ClusterTopoSubItem{
			dataNodes: sync.Map{},
		},
		cfg: &clusterConfig{
			RackAwareLevel: proto.RackAwareStrong,
		},
	}

	// Set up atomic values
	cluster.DistributionOptimizationConDpCnt.Store(100)
	distributionOptimizationThreshold.Store(0.8)
	cluster.EnableDistributionOptimization.Store(true)

	// Add mock data nodes (all in zone1, except one in zone2 for cross-zone testing)
	mockNodes := map[string]*DataNode{
		"192.168.1.1:17310": {Addr: "192.168.1.1:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1"},
		"192.168.1.2:17310": {Addr: "192.168.1.2:17310", Rack: "rack2", NodeSetID: 1, ZoneName: "zone1"},
		"192.168.1.3:17310": {Addr: "192.168.1.3:17310", Rack: "rack1", NodeSetID: 2, ZoneName: "zone1"},
		"192.168.2.1:17310": {Addr: "192.168.2.1:17310", Rack: "rack1", NodeSetID: 3, ZoneName: "zone2"},
	}

	for addr, node := range mockNodes {
		cluster.dataNodes.Store(addr, node)
	}

	// Create test volume with data partitions
	vol := &Vol{
		Name:           "test-vol",
		dataPartitions: newDataPartitionMap("test-vol"),
	}

	// Add more mock nodes for rack conflict testing
	mockNodes["192.168.1.4:17310"] = &DataNode{Addr: "192.168.1.4:17310", Rack: "rack1", NodeSetID: 1, ZoneName: "zone1"}
	cluster.dataNodes.Store("192.168.1.4:17310", mockNodes["192.168.1.4:17310"])

	partitions := []*DataPartition{
		// SSD partitions
		{
			PartitionID: 1,
			MediaType:   proto.MediaType_SSD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.2:17310", "192.168.1.3:17310"},
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
			},
			DecommissionType:   proto.DistributionOptimization,
			DecommissionStatus: DecommissionRunning,
			IsDiscard:          false,
		},
		{
			PartitionID: 2,
			MediaType:   proto.MediaType_SSD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.2:17310"}, // Same NodeSet, different racks - optimal
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 3,
			MediaType:   proto.MediaType_SSD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.4:17310"}, // Same NodeSet, same rack - rack conflict
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: mockNodes["192.168.1.4:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 4,
			MediaType:   proto.MediaType_SSD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.2.1:17310"}, // Cross-zone: zone1 + zone2
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.2.1:17310"}, dataNode: mockNodes["192.168.2.1:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 5,
			MediaType:   proto.MediaType_SSD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.3:17310", "192.168.2.1:17310"}, // 2 in zone1 (different nodesets) + 1 in zone2 - NodeSet unbalanced
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.2.1:17310"}, dataNode: mockNodes["192.168.2.1:17310"]},
			},
			IsDiscard: false,
		},
		// HDD partitions
		{
			PartitionID: 6,
			MediaType:   proto.MediaType_HDD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.2:17310"}, // Same NodeSet, different racks - optimal
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 7,
			MediaType:   proto.MediaType_HDD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.3:17310"}, // Different NodeSets - NodeSet unbalanced
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 8,
			MediaType:   proto.MediaType_HDD,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.4:17310"}, // Same NodeSet, same rack - rack conflict
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: mockNodes["192.168.1.4:17310"]},
			},
			IsDiscard: false,
		},
	}

	for _, dp := range partitions {
		vol.dataPartitions.put(dp)
	}

	cluster.vols["test-vol"] = vol

	// Test getting status
	status := cluster.getDistributionOptimizationStatus()

	require.NotNil(t, status, "Status should not be nil")
	require.Equal(t, int64(100), status.ConcurrentDpCount, "ConcurrentDpCount mismatch")
	require.Equal(t, 0.8, status.BalanceThreshold, "BalanceThreshold mismatch")
	require.True(t, status.EnableDistributionOptimization, "EnableDistributionOptimization mismatch")
	require.Equal(t, []uint64{1}, status.DecommissioningDPIDs, "DecommissioningDPIDs mismatch")

	// Verify SSDStats
	require.NotNil(t, status.SSDStats, "SSDStats should not be nil")
	// dp1: NodeSet unbalanced (NodeSet1 has 2, NodeSet2 has 1) + rack conflict (rack1 has 2) -> unbalanced
	// dp3: rack conflict (rack1 has 2) -> unbalanced
	// dp4: cross-zone but NodeSet balanced and no rack conflict -> not unbalanced
	// dp5: NodeSet unbalanced (zone1 has 2 in different NodeSets) -> unbalanced
	require.Equal(t, 3, status.SSDStats.TotalUnbalancedDPs, "SSD TotalUnbalancedDPs should be 3 (dp1, dp3, dp5)")
	require.Equal(t, 2, status.SSDStats.NodeSetUnbalancedDPs, "SSD NodeSetUnbalancedDPs should be 2 (dp1 and dp5)")
	// dp1: rack conflict (rack1 has 2 replicas in zone1)
	// dp3: rack conflict (rack1 has 2 replicas in zone1)
	// dp5: rack conflict (rack1 has 2 replicas in zone1)
	require.Equal(t, 3, status.SSDStats.RackConflictDPs, "SSD RackConflictDPs should be 3 (dp1, dp3, and dp5)")
	require.Equal(t, 2, status.SSDStats.CrossZoneDPs, "SSD CrossZoneDPs should be 2 (dp4 and dp5)")
	// dp2: SingleDomainDPs (NodeSet1 only in zone1)
	// dp3: SingleDomainDPs (NodeSet1 only in zone1, but has rack conflict)
	// dp4: SingleDomainDPs (each zone has 1 NodeSet: zone1 has NodeSet1, zone2 has NodeSet3)
	require.Equal(t, 3, status.SSDStats.DomainDistribution.SingleDomainDPs, "SSD SingleDomainDPs should be 3 (dp2, dp3, and dp4)")
	// dp1: TwoDomainDPs (NodeSet1 and NodeSet2 in zone1)
	// dp5: TwoDomainDPs (NodeSet1 and NodeSet2 in zone1, plus zone2)
	require.Equal(t, 2, status.SSDStats.DomainDistribution.TwoDomainDPs, "SSD TwoDomainDPs should be 2 (dp1 and dp5)")
	require.Equal(t, 0, status.SSDStats.DomainDistribution.ThreeDomainDPs, "SSD ThreeDomainDPs should be 0")
	// dp2: NoRackConflictDPs (different racks)
	// dp4: NoRackConflictDPs (different zones, no conflict)
	require.Equal(t, 2, status.SSDStats.RackDistribution.NoRackConflictDPs, "SSD NoRackConflictDPs should be 2 (dp2 and dp4)")
	// dp1: MinorRackConflictDPs (rack1 has 2 replicas)
	// dp3: MinorRackConflictDPs (rack1 has 2 replicas)
	// dp5: MinorRackConflictDPs (rack1 has 2 replicas in zone1)
	require.Equal(t, 3, status.SSDStats.RackDistribution.MinorRackConflictDPs, "SSD MinorRackConflictDPs should be 3 (dp1, dp3, and dp5)")
	require.Equal(t, 0, status.SSDStats.RackDistribution.MajorRackConflictDPs, "SSD MajorRackConflictDPs should be 0")

	// Verify HDDStats
	require.NotNil(t, status.HDDStats, "HDDStats should not be nil")
	// dp7: NodeSet unbalanced (NodeSet1 and NodeSet2) + rack conflict (rack1 has 2 replicas) -> unbalanced
	// dp8: rack conflict (rack1 has 2 replicas) -> unbalanced
	require.Equal(t, 2, status.HDDStats.TotalUnbalancedDPs, "HDD TotalUnbalancedDPs should be 2 (dp7 and dp8)")
	require.Equal(t, 1, status.HDDStats.NodeSetUnbalancedDPs, "HDD NodeSetUnbalancedDPs should be 1 (dp7)")
	// dp7: rack conflict (rack1 has 2 replicas in zone1)
	// dp8: rack conflict (rack1 has 2 replicas in zone1)
	require.Equal(t, 2, status.HDDStats.RackConflictDPs, "HDD RackConflictDPs should be 2 (dp7 and dp8)")
	require.Equal(t, 0, status.HDDStats.CrossZoneDPs, "HDD CrossZoneDPs should be 0")
	// dp6: SingleDomainDPs (NodeSet1 only)
	// dp8: SingleDomainDPs (NodeSet1 only, but has rack conflict)
	require.Equal(t, 2, status.HDDStats.DomainDistribution.SingleDomainDPs, "HDD SingleDomainDPs should be 2 (dp6 and dp8)")
	// dp7: TwoDomainDPs (NodeSet1 and NodeSet2 in zone1)
	require.Equal(t, 1, status.HDDStats.DomainDistribution.TwoDomainDPs, "HDD TwoDomainDPs should be 1 (dp7)")
	require.Equal(t, 0, status.HDDStats.DomainDistribution.ThreeDomainDPs, "HDD ThreeDomainDPs should be 0")
	// dp6: NoRackConflictDPs (different racks)
	require.Equal(t, 1, status.HDDStats.RackDistribution.NoRackConflictDPs, "HDD NoRackConflictDPs should be 1 (dp6)")
	// dp7: MinorRackConflictDPs (rack1 has 2 replicas)
	// dp8: MinorRackConflictDPs (rack1 has 2 replicas)
	require.Equal(t, 2, status.HDDStats.RackDistribution.MinorRackConflictDPs, "HDD MinorRackConflictDPs should be 2 (dp7 and dp8)")
	require.Equal(t, 0, status.HDDStats.RackDistribution.MajorRackConflictDPs, "HDD MajorRackConflictDPs should be 0")
}
