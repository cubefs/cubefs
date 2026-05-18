// Copyright 2020 The CubeFS Authors.
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

package wrapper

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func makeDP(partitionID uint64, poolId uint8) *DataPartition {
	return &DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: partitionID,
			PoolId:      poolId,
			Hosts:       []string{"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"},
		},
		Metrics: new(DataPartitionMetrics),
	}
}

func TestDefaultRandomSelectorCountByPoolId(t *testing.T) {
	s := &DefaultRandomSelector{
		partitions:            make([]*DataPartition, 0),
		localLeaderPartitions: make([]*DataPartition, 0),
	}

	// Empty partitions
	if count := s.CountByPoolId(1); count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	// Add partitions with different poolIds
	dps := []*DataPartition{
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 1, PoolId: 1}},
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 2, PoolId: 1}},
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 3, PoolId: 2}},
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 4, PoolId: 0}},
	}
	s.partitions = dps

	if count := s.CountByPoolId(1); count != 2 {
		t.Fatalf("expected 2 for poolId=1, got %d", count)
	}
	if count := s.CountByPoolId(2); count != 1 {
		t.Fatalf("expected 1 for poolId=2, got %d", count)
	}
	if count := s.CountByPoolId(0); count != 1 {
		t.Fatalf("expected 1 for poolId=0, got %d", count)
	}
	if count := s.CountByPoolId(99); count != 0 {
		t.Fatalf("expected 0 for poolId=99, got %d", count)
	}
}

func TestKFasterRandomSelectorCountByPoolId(t *testing.T) {
	selector, err := newKFasterRandomSelector("80")
	if err != nil {
		t.Fatalf("init selector failed: %v", err)
	}
	s := selector.(*KFasterRandomSelector)

	// Empty partitions
	if count := s.CountByPoolId(1); count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	dps := []*DataPartition{
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 1, PoolId: 1}, Metrics: new(DataPartitionMetrics)},
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 2, PoolId: 1}, Metrics: new(DataPartitionMetrics)},
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 3, PoolId: 2}, Metrics: new(DataPartitionMetrics)},
		{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 4, PoolId: 0}, Metrics: new(DataPartitionMetrics)},
	}
	s.partitions = dps

	if count := s.CountByPoolId(1); count != 2 {
		t.Fatalf("expected 2 for poolId=1, got %d", count)
	}
	if count := s.CountByPoolId(2); count != 1 {
		t.Fatalf("expected 1 for poolId=2, got %d", count)
	}
	if count := s.CountByPoolId(0); count != 1 {
		t.Fatalf("expected 1 for poolId=0, got %d", count)
	}
	if count := s.CountByPoolId(99); count != 0 {
		t.Fatalf("expected 0 for poolId=99, got %d", count)
	}
}

func TestRefreshDpSelectorUpdatePolicy(t *testing.T) {
	w := &Wrapper{}
	w.Lock = sync.RWMutex{}

	// Set up selector with old partitions across multiple pools
	oldDps := []*DataPartition{
		makeDP(1, 1),
		makeDP(2, 1),
		makeDP(3, 1),
		makeDP(4, 2),
		makeDP(5, 2),
	}
	s := &DefaultRandomSelector{
		partitions:            oldDps,
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s
	w.dpSelectorChanged = false

	// New partitions are fewer — only 1 per pool, so backfill from oldDps will happen
	newDps := []*DataPartition{
		makeDP(10, 1),
		makeDP(11, 2),
	}

	w.refreshDpSelector(UpdateDpPolicy, newDps)

	result := s.GetAllDp()
	// After refresh, partitions should contain new + backfilled old dps
	if len(result) < 2 {
		t.Fatalf("expected at least 2 partitions after refresh, got %d", len(result))
	}

	// Verify new partitions are present
	found10, found11 := false, false
	for _, dp := range result {
		if dp.PartitionID == 10 {
			found10 = true
		}
		if dp.PartitionID == 11 {
			found11 = true
		}
	}
	if !found10 || !found11 {
		t.Fatalf("expected new dps 10 and 11 in result, found10=%v found11=%v", found10, found11)
	}
}

func TestRefreshDpSelectorUpdatePolicyNoBackfill(t *testing.T) {
	w := &Wrapper{}
	w.Lock = sync.RWMutex{}

	// Old partitions
	oldDps := []*DataPartition{
		makeDP(1, 1),
		makeDP(2, 1),
	}
	s := &DefaultRandomSelector{
		partitions:            oldDps,
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s
	w.dpSelectorChanged = false

	// New partitions are same size, no backfill needed
	newDps := []*DataPartition{
		makeDP(10, 1),
		makeDP(11, 1),
	}

	w.refreshDpSelector(UpdateDpPolicy, newDps)

	result := s.GetAllDp()
	if len(result) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(result))
	}
}

func TestRefreshDpSelectorUpdatePolicyDuplicatedMerge(t *testing.T) {
	w := &Wrapper{}
	w.Lock = sync.RWMutex{}

	// Old partitions with 3 dps in pool 1
	oldDps := []*DataPartition{
		makeDP(1, 1),
		makeDP(2, 1),
		makeDP(3, 1),
	}
	s := &DefaultRandomSelector{
		partitions:            oldDps,
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s
	w.dpSelectorChanged = false

	// Only 1 new dp in pool 1, minDpCount = refreshMinDpCount(3) = 2, so backfill needed
	newDps := []*DataPartition{
		makeDP(4, 1),
	}

	w.refreshDpSelector(UpdateDpPolicy, newDps)

	result := s.GetAllDp()
	// Should have at least 2 dps after backfill
	if len(result) < 2 {
		t.Fatalf("expected at least 2 partitions after backfill, got %d", len(result))
	}
}

func TestRefreshDpSelectorUpdatePolicyPoolWithNoNewDps(t *testing.T) {
	w := &Wrapper{}
	w.Lock = sync.RWMutex{}

	// Old partitions across pool 1 and pool 2
	oldDps := []*DataPartition{
		makeDP(1, 1),
		makeDP(2, 1),
		makeDP(3, 1),
		makeDP(4, 2),
		makeDP(5, 2),
	}
	s := &DefaultRandomSelector{
		partitions:            oldDps,
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s
	w.dpSelectorChanged = false

	// New dps only in pool 1, no new dps in pool 2
	// Pool 2 should get backfilled from old dps
	newDps := []*DataPartition{
		makeDP(10, 1),
	}

	w.refreshDpSelector(UpdateDpPolicy, newDps)

	result := s.GetAllDp()
	// Should include pool 2 backfill
	if len(result) < 3 {
		t.Fatalf("expected at least 3 partitions (pool1 + pool2 backfill), got %d", len(result))
	}
}

func TestRefreshMinDpCount(t *testing.T) {
	w := &Wrapper{}

	// refreshMinDpCount = oldDpCount * 2 / 3
	if count := w.refreshMinDpCount(3); count != 2 {
		t.Fatalf("expected 2 for input 3, got %d", count)
	}
	if count := w.refreshMinDpCount(6); count != 4 {
		t.Fatalf("expected 4 for input 6, got %d", count)
	}
	if count := w.refreshMinDpCount(1); count != 0 {
		t.Fatalf("expected 0 for input 1, got %d", count)
	}
	if count := w.refreshMinDpCount(0); count != 0 {
		t.Fatalf("expected 0 for input 0, got %d", count)
	}
}

func TestWrapperRemoveDataPartitionForWrite(t *testing.T) {
	w := &Wrapper{}
	w.Lock = sync.RWMutex{}

	// Test: not enough data partitions for the given poolId
	s := &DefaultRandomSelector{
		partitions: []*DataPartition{
			{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 1, PoolId: 1}},
		},
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s

	err := w.RemoveDataPartitionForWrite(1, 1)
	if err == nil || err.Error() != "not enough data partitions" {
		t.Fatalf("expected 'not enough data partitions' error, got %v", err)
	}

	// Test: zero partitions for the poolId
	s2 := &DefaultRandomSelector{
		partitions: []*DataPartition{
			{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 1, PoolId: 2}},
			{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 2, PoolId: 2}},
		},
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s2

	err = w.RemoveDataPartitionForWrite(1, 1)
	if err == nil || err.Error() != "not enough data partitions" {
		t.Fatalf("expected 'not enough data partitions' error for poolId with 0 dps, got %v", err)
	}

	// Test: enough partitions for the poolId, remove succeeds
	s3 := &DefaultRandomSelector{
		partitions: []*DataPartition{
			{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 1, PoolId: 1}},
			{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 2, PoolId: 1}},
			{DataPartitionResponse: proto.DataPartitionResponse{PartitionID: 3, PoolId: 2}},
		},
		localLeaderPartitions: make([]*DataPartition, 0),
	}
	w.dpSelector = s3

	err = w.RemoveDataPartitionForWrite(1, 1)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Verify the partition was removed
	if s3.Count() != 2 {
		t.Fatalf("expected 2 partitions after removal, got %d", s3.Count())
	}
}

func TestKmin(t *testing.T) {
	partitions := make([]*DataPartition, 0)

	rand.Seed(time.Now().UnixNano())
	length := rand.Intn(100) + 2

	for i := 0; i < length; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Int63n(100)

		dp := new(DataPartition)
		dp.Metrics = new(DataPartitionMetrics)
		dp.Metrics.AvgWriteLatencyNano = i
		partitions = append(partitions, dp)
	}
	fmt.Printf("%-20s", "origin partitions:")
	for _, v := range partitions {
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	kth := selectKminDataPartition(partitions, (length-1)*80/100+1)

	kmin := partitions[kth].GetAvgWrite()

	fmt.Printf("%-20s%v/%v", "kth of length:", kth, length)
	fmt.Println()

	fmt.Printf("%-20s%v", "kmin:", kmin)
	fmt.Println()

	fmt.Printf("%-20s", "faster partitions:")
	for _, v := range partitions[:kth] {
		if v.GetAvgWrite() > kmin {
			fmt.Println()
			fmt.Println("select error!")
			t.Fail()
		}
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	fmt.Printf("%-20s", "slower partitions:")
	for _, v := range partitions[kth:] {
		fmt.Printf("%v ", v.GetAvgWrite())
		if v.GetAvgWrite() < kmin {
			fmt.Println()
			fmt.Println("select error!")
			t.Fail()
		}
	}
	fmt.Println()
}
