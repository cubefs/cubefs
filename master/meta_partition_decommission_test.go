// Copyright 2025 The CubeFS Authors.
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
)

// Helper function to create a test volume with proper initialization for decommission tests
func createTestVolForDecommission(name string) *Vol {
	vol := &Vol{
		Name:           name,
		MetaPartitions: make(map[uint64]*MetaPartition),
	}
	vol.mpsLock = newMpsLockManager(vol)
	return vol
}

func TestGetMetaPartitionDecommissionCount(t *testing.T) {
	// Create a test cluster
	cluster := &Cluster{}
	cluster.BadMetaPartitionIds = &sync.Map{}
	cluster.RecoverMetaPartitionIds = &sync.Map{}
	cluster.vols = make(map[string]*Vol)

	// Create test volumes and meta partitions
	vol := createTestVolForDecommission("test-vol")
	cluster.vols["test-vol"] = vol

	// Create meta partitions with different decommission types
	mp1 := &MetaPartition{
		PartitionID: 1,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.AutoAddReplica,
		},
	}
	mp2 := &MetaPartition{
		PartitionID: 2,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.AutoAddReplica,
		},
	}
	mp3 := &MetaPartition{
		PartitionID: 3,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.ManualDecommission,
		},
	}
	mp4 := &MetaPartition{
		PartitionID: 4,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.MpBalance,
		},
	}

	vol.MetaPartitions[1] = mp1
	vol.MetaPartitions[2] = mp2
	vol.MetaPartitions[3] = mp3
	vol.MetaPartitions[4] = mp4

	// Add meta partitions to BadMetaPartitionIds
	cluster.BadMetaPartitionIds.Store("node1", []uint64{1, 2})
	cluster.BadMetaPartitionIds.Store("node2", []uint64{3, 4})

	// Test counting AutoAddReplica type
	count := cluster.GetMetaPartitionDecommissionCount(proto.AutoAddReplica)
	if count != 2 {
		t.Errorf("Expected AutoAddReplica count to be 2, got %d", count)
	}

	// Test counting ManualDecommission type
	count = cluster.GetMetaPartitionDecommissionCount(proto.ManualDecommission)
	if count != 1 {
		t.Errorf("Expected ManualDecommission count to be 1, got %d", count)
	}

	// Test counting MpBalance type
	count = cluster.GetMetaPartitionDecommissionCount(proto.MpBalance)
	if count != 1 {
		t.Errorf("Expected MpBalance count to be 1, got %d", count)
	}

	// Test counting ManualAddReplica type (should be 0)
	count = cluster.GetMetaPartitionDecommissionCount(proto.ManualAddReplica)
	if count != 0 {
		t.Errorf("Expected ManualAddReplica count to be 0, got %d", count)
	}

	// Test with non-existent partition ID
	cluster.BadMetaPartitionIds.Store("node3", []uint64{999})
	count = cluster.GetMetaPartitionDecommissionCount(proto.AutoAddReplica)
	if count != 2 {
		t.Errorf("Expected count to remain 2 with non-existent partition, got %d", count)
	}
}

func TestCheckMetaPartitionDecommissionLimit(t *testing.T) {
	// Create a test cluster
	cluster := &Cluster{}
	cluster.BadMetaPartitionIds = &sync.Map{}
	cluster.RecoverMetaPartitionIds = &sync.Map{}
	cluster.vols = make(map[string]*Vol)

	// Set limits
	cluster.MetaAutoAddReplicaLimit.Store(3)
	cluster.MetaManualDecommissionLimit.Store(2)
	cluster.MetaBalanceLimit.Store(1)

	cluster.MetaManualAddReplicaLimit.Store(0) // 0 means no limit
	// Create test volume and meta partitions
	vol := createTestVolForDecommission("test-vol")
	cluster.vols["test-vol"] = vol

	// Create 2 AutoAddReplica partitions
	mp1 := &MetaPartition{
		PartitionID: 1,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.AutoAddReplica,
		},
	}
	mp2 := &MetaPartition{
		PartitionID: 2,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.AutoAddReplica,
		},
	}
	vol.MetaPartitions[1] = mp1
	vol.MetaPartitions[2] = mp2
	cluster.BadMetaPartitionIds.Store("node1", []uint64{1, 2})

	// Test: should pass (2 < 3)
	err := cluster.CheckMPDecommissionLimit(proto.AutoAddReplica)
	if err != nil {
		t.Errorf("Expected no error when count < limit, got: %v", err)
	}

	// Add one more AutoAddReplica partition
	mp3 := &MetaPartition{
		PartitionID: 3,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.AutoAddReplica,
		},
	}
	vol.MetaPartitions[3] = mp3
	cluster.BadMetaPartitionIds.Store("node2", []uint64{3})

	// Test: should fail (3 >= 3, limit reached)
	err = cluster.CheckMPDecommissionLimit(proto.AutoAddReplica)
	if err == nil {
		t.Error("Expected error when count >= limit, got nil")
	}

	// Add one more AutoAddReplica partition
	mp4 := &MetaPartition{
		PartitionID: 4,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.AutoAddReplica,
		},
	}
	vol.MetaPartitions[4] = mp4
	cluster.BadMetaPartitionIds.Store("node3", []uint64{4})

	// Test: should fail (4 > 3)
	err = cluster.CheckMPDecommissionLimit(proto.AutoAddReplica)
	if err == nil {
		t.Error("Expected error when count > limit, got nil")
	}

	// Test: no limit (0) should always pass
	mp5 := &MetaPartition{
		PartitionID: 5,
		volName:     "test-vol",
		RecoverPair: proto.RecoverPair{
			DecommissionType: proto.ManualAddReplica,
		},
	}
	vol.MetaPartitions[5] = mp5
	cluster.BadMetaPartitionIds.Store("node4", []uint64{5})

	err = cluster.CheckMPDecommissionLimit(proto.ManualAddReplica)
	if err != nil {
		t.Errorf("Expected no error when limit is 0 (no limit), got: %v", err)
	}
}

func TestSetAndGetMetaPartitionDecommissionLimit(t *testing.T) {
	// Create a test cluster
	cluster := &Cluster{}

	// Set different limits
	cluster.MetaAutoAddReplicaLimit.Store(5)
	cluster.MetaManualDecommissionLimit.Store(10)
	cluster.MetaBalanceLimit.Store(15)
	cluster.MetaManualAddReplicaLimit.Store(20)
	cluster.MetaManualLearnerLimit.Store(25)

	// Test getting AutoAddReplica limit
	limit := cluster.GetMetaPartitionDecommissionLimit(proto.AutoAddReplica)
	if limit != 5 {
		t.Errorf("Expected AutoAddReplica limit to be 5, got %d", limit)
	}

	// Test getting ManualDecommission limit
	limit = cluster.GetMetaPartitionDecommissionLimit(proto.ManualDecommission)
	if limit != 10 {
		t.Errorf("Expected ManualDecommission limit to be 10, got %d", limit)
	}

	// Test getting MpBalance limit
	limit = cluster.GetMetaPartitionDecommissionLimit(proto.MpBalance)
	if limit != 15 {
		t.Errorf("Expected MpBalance limit to be 15, got %d", limit)
	}

	// Test getting ManualAddReplica limit
	limit = cluster.GetMetaPartitionDecommissionLimit(proto.ManualAddReplica)
	if limit != 20 {
		t.Errorf("Expected ManualAddReplica limit to be 20, got %d", limit)
	}

	// Test getting MpManumalLearner limit
	limit = cluster.GetMetaPartitionDecommissionLimit(proto.MpManumalLearner)
	if limit != 25 {
		t.Errorf("Expected MpManumalLearner limit to be 25, got %d", limit)
	}

	// Test getting limit for unknown type
	limit = cluster.GetMetaPartitionDecommissionLimit(999)
	if limit != 0 {
		t.Errorf("Expected limit for unknown type to be 0, got %d", limit)
	}
}

func TestGetMetaPartitionDecommissionTypeName(t *testing.T) {
	tests := []struct {
		decommissionType uint32
		expectedName     string
	}{
		{proto.AutoAddReplica, "AutoAddReplica"},
		{proto.ManualDecommission, "ManualDecommission"},
		{proto.MpBalance, "MpBalance"},
		{proto.ManualAddReplica, "ManualAddReplica"},
		{proto.MpManumalLearner, "MpManumalLearner"},
		{999, "Unknown(999)"},
		{0, "Unknown(0)"},
	}

	for _, tt := range tests {
		name := GetMetaPartitionDecommissionTypeName(tt.decommissionType)
		if name != tt.expectedName {
			t.Errorf("Expected type name for %d to be %s, got %s", tt.decommissionType, tt.expectedName, name)
		}
	}
}
