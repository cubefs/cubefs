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

	partitions := []*DataPartition{
		{
			PartitionID: 1,
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
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.2:17310"}, // Same NodeSet, different racks
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.2:17310"}, dataNode: mockNodes["192.168.1.2:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 3,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.1:17310"}, // Same NodeSet, same rack (conflict)
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.4:17310"}, dataNode: mockNodes["192.168.1.4:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 4,
			Hosts:       []string{"192.168.1.1:17310", "192.168.2.1:17310"}, // Cross-zone: zone1 + zone2
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.2.1:17310"}, dataNode: mockNodes["192.168.2.1:17310"]},
			},
			IsDiscard: false,
		},
		{
			PartitionID: 5,
			Hosts:       []string{"192.168.1.1:17310", "192.168.1.3:17310", "192.168.2.1:17310"}, // 2 in zone1 (different nodesets) + 1 in zone2
			Replicas: []*DataReplica{
				{DataReplica: proto.DataReplica{Addr: "192.168.1.1:17310"}, dataNode: mockNodes["192.168.1.1:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.1.3:17310"}, dataNode: mockNodes["192.168.1.3:17310"]},
				{DataReplica: proto.DataReplica{Addr: "192.168.2.1:17310"}, dataNode: mockNodes["192.168.2.1:17310"]},
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
	require.Equal(t, 2, status.CrossZoneDPs, "CrossZoneDPs should be 2 (dp4 and dp5 are cross-zone)")
	require.NotNil(t, status.DomainDistribution, "DomainDistribution should not be nil")
	require.NotNil(t, status.RackDistribution, "RackDistribution should not be nil")
}
