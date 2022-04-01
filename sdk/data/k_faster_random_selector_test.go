package data

import (
	"testing"

	"github.com/chubaofs/chubaofs/proto"
)

func TestKFaster(t *testing.T) {
	//var selectorNil DataPartitionSelector
	var selector1 DataPartitionSelector

	_, e := newKFasterRandomSelector(&DpSelectorParam{kValue: "", quorum: 0})
	if e == nil {
		t.Fatalf("expected err, but nil")
	}

	_, e = newKFasterRandomSelector(&DpSelectorParam{kValue: "0", quorum: 0})
	if e == nil {
		t.Fatalf("expected err, but nil")
	}

	_, e = newKFasterRandomSelector(&DpSelectorParam{kValue: "100", quorum: 0})
	if e == nil {
		t.Fatalf("expected err, but nil")
	}

	selectorParams := []string{"1", "99", "50"}
	for _, s := range selectorParams {
		selector, e := newKFasterRandomSelector(&DpSelectorParam{kValue: s, quorum: 0})
		if e != nil {
			t.Fatalf("newKFasterRandomSelector %v", e)
		}
		selector1 = selector
	}

	if selector1.Name() == "" {
		t.Fatalf("get Name failed")
	}

	dpr1 := proto.DataPartitionResponse{PartitionID: uint64(1), Hosts: []string{"1"}}
	metrics1 := proto.DataPartitionMetrics{AvgWriteLatencyNano: int64(50)}
	dp1 := DataPartition{DataPartitionResponse: dpr1, Metrics: &metrics1}
	dpr2 := proto.DataPartitionResponse{PartitionID: uint64(2), Hosts: []string{"2"}}
	metrics2 := proto.DataPartitionMetrics{AvgWriteLatencyNano: int64(70)}
	dp2 := DataPartition{DataPartitionResponse: dpr2, Metrics: &metrics2}
	dpr3 := proto.DataPartitionResponse{PartitionID: uint64(3), Hosts: []string{"3"}}
	metrics3 := proto.DataPartitionMetrics{AvgWriteLatencyNano: int64(9)}
	dp3 := DataPartition{DataPartitionResponse: dpr3, Metrics: &metrics3}
	dpr4 := proto.DataPartitionResponse{PartitionID: uint64(4), Hosts: []string{"4"}}
	metrics4 := proto.DataPartitionMetrics{AvgWriteLatencyNano: int64(100)}
	dp4 := DataPartition{DataPartitionResponse: dpr4, Metrics: &metrics4}
	dpr5 := proto.DataPartitionResponse{PartitionID: uint64(5), Hosts: []string{"5"}}
	metrics5 := proto.DataPartitionMetrics{AvgWriteLatencyNano: int64(20)}
	dp5 := DataPartition{DataPartitionResponse: dpr5, Metrics: &metrics5}

	multiPartitions := [][]*DataPartition{}
	multiPartitions = append(multiPartitions, []*DataPartition{})
	//multiPartitions = append(multiPartitions, []*DataPartition{&dp1})
	multiPartitions = append(multiPartitions, []*DataPartition{&dp1, &dp2, &dp3, &dp4, &dp5})
	//fmt.Println("dp1", dp1, "\ndp2", dp2, "\ndp3", dp3, "\ndp4", dp4, "\ndp5", dp5)
	for _, partitions := range multiPartitions {
		err := selector1.Refresh(partitions)
		if err != nil {
			t.Fatalf("%v", err)
		}
	}

	excludes := make([]map[string]struct{}, 1)
	m1 := make(map[string]struct{})
	m1["1"] = struct{}{}
	m1["2"] = struct{}{}
	m1["3"] = struct{}{}
	m2 := make(map[string]struct{}) //next to last
	m2["1"] = struct{}{}
	m2["2"] = struct{}{}
	m2["3"] = struct{}{}
	m2["5"] = struct{}{}
	m3 := make(map[string]struct{}) //last
	m3["1"] = struct{}{}
	m3["2"] = struct{}{}
	m3["3"] = struct{}{}
	m3["4"] = struct{}{}
	m3["5"] = struct{}{}
	m4 := make(map[string]struct{})
	m4["4"] = struct{}{}
	m4["5"] = struct{}{}
	excludes = append(excludes, m1)
	excludes = append(excludes, m2)
	//excludes = append(excludes, m3)
	excludes = append(excludes, m4)
	for _, exclude := range excludes {
		dp, err2 := selector1.Select(exclude)
		if err2 != nil {
			t.Fatalf("Select falied, dp %v err %v", dp, err2)
		}
	}

	dp, err := selector1.Select(m3)
	if err == nil {
		t.Fatalf("expected Select falied, but success, dp %v err %v", dp, err)
	}

	partitionIds := make([]uint64, 2)
	partitionIds[0] = 0
	partitionIds[1] = 1
	for _, p := range partitionIds {
		selector1.RemoveDP(p)
	}

	err = selector1.Refresh(multiPartitions[0])
	if err != nil {
		t.Fatalf("Refresh failed, err %v", err)
	}
	dp, err2 := selector1.Select(excludes[0])
	if err2 == nil {
		t.Fatalf("%v", err2)
	}
}
