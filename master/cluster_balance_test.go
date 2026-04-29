package master

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	raftstore "github.com/cubefs/cubefs/raftstore"
	raftstore_db "github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/stretchr/testify/require"
)

// Cluster balance tests focus on deterministic planning helpers first, then
// cover the heavier handler-like flows with narrow fixtures. Most tests build
// only the fields read by the function under test, which keeps them independent
// from a fully running master cluster.

func TestGetMigrateDestAddr(t *testing.T) {
	// Construct test parameters
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	param := &GetMigrateAddrParam{
		ZoneName:   "testZone",
		NodeSetID:  1,
		RequestNum: 2,
		Excludes:   []string{"192.168.1.1"},
		LeastSize:  1024,
		Topo: map[string]*proto.ZonePressureView{
			"testZone": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.1.1", ID: 1, NodeSetID: 1, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.1.2", ID: 2, NodeSetID: 1, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.1.3", ID: 3, NodeSetID: 1, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}

	// Call the function under test
	find, address := GetMigrateDestAddr(param)

	// Verify the result
	if !find {
		t.Errorf("Expected to find addresses, but got find=false")
	}

	if len(address) != param.RequestNum {
		t.Errorf("Expected %d addresses, but got %d", param.RequestNum, len(address))
	}

	for _, addr := range address {
		if addr.Destination == "192.168.1.1" {
			t.Errorf("Excluded address found in results: %s", addr.Destination)
		}
	}
}

func TestFreezeEmptyMetaPartition_NoLeader(t *testing.T) {
	c := &Cluster{}
	mp := &MetaPartition{
		PartitionID: 123,
		Replicas:    []*MetaReplica{{Addr: "node1", IsLeader: false}},
	}
	err := c.FreezeEmptyMetaPartition(mp, true)
	if err == nil {
		t.Errorf("expected error when no leader, got nil")
	}
}

func TestFreezeEmptyMetaPartition_MetaNodeNotFound(t *testing.T) {
	// build cluster with leader replica but meta node map empty
	c := &Cluster{}
	leaderNode := &MetaNode{ID: 1, Addr: "node1"}
	mp := &MetaPartition{
		PartitionID: 123,
		Replicas:    []*MetaReplica{{Addr: leaderNode.Addr, IsLeader: true, metaNode: leaderNode}},
	}
	// c.metaNodes not loaded with node1 => c.metaNode() returns error
	err := c.FreezeEmptyMetaPartition(mp, true)
	if err == nil {
		t.Errorf("expected error when meta node not found, got nil")
	}
}

func TestFreezeEmptyMetaPartition_SendFail(t *testing.T) {
	// start a local TCP listener to accept but not respond correctly, forcing ReadFromConnWithVer to fail
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			// immediately close to cause send/read error
			conn.Close()
		}
	}()

	addr := ln.Addr().String()
	c := &Cluster{}
	// ensure direct dialing (no pool) to our listener
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mn := &MetaNode{ID: 1, Addr: addr, Sender: newAdminTaskManager(addr, "test-cluster")}
	c.metaNodes.Store(addr, mn)
	mp := &MetaPartition{
		PartitionID: 123,
		Replicas:    []*MetaReplica{{Addr: addr, IsLeader: true, metaNode: mn}},
	}

	err = c.FreezeEmptyMetaPartition(mp, true)
	if err == nil {
		t.Errorf("expected error when syncSendAdminTask fails, got nil")
	}
}

func TestFreezeEmptyMetaPartition_Success(t *testing.T) {
	// Spin up a mock meta server that acks admin tasks
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// read packet then echo minimal valid OpOk response
		// Use proto.Packet helpers to read and write
		// Because we are in master package test, we can reuse proto directly here
		p := proto.NewPacket()
		_ = p.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime)
		p.ResultCode = proto.OpOk
		p.Data = []byte("ok")
		p.Size = uint32(len(p.Data))
		_ = p.WriteToConn(conn)
		conn.Close()
	}()

	addr := ln.Addr().String()
	c := &Cluster{}
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mn := &MetaNode{ID: 1, Addr: addr, Sender: newAdminTaskManager(addr, "test-cluster")}
	c.metaNodes.Store(addr, mn)
	mp := &MetaPartition{
		PartitionID: 123,
		Replicas:    []*MetaReplica{{Addr: addr, IsLeader: true, metaNode: mn}},
	}

	if err := c.FreezeEmptyMetaPartition(mp, true); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	<-done
}

func TestGetMigrateAddrExcludeNodeSet(t *testing.T) {
	// Construct test parameters
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	param := &GetMigrateAddrParam{
		ZoneName:   "testZone",
		NodeSetID:  1,
		RequestNum: 2,
		Excludes:   []string{"192.168.1.1"},
		LeastSize:  1024,
		Topo: map[string]*proto.ZonePressureView{
			"testZone": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.1.1", ID: 1, NodeSetID: 1, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.1.2", ID: 2, NodeSetID: 1, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.1.3", ID: 3, NodeSetID: 1, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
						},
					},
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							4: {Addr: "192.168.1.4", ID: 4, NodeSetID: 2, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
							5: {Addr: "192.168.1.5", ID: 5, NodeSetID: 2, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
							6: {Addr: "192.168.1.6", ID: 6, NodeSetID: 2, ZoneName: "testZone", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}

	// Call the function under test
	find, address := GetMigrateAddrExcludeNodeSet(param)

	// Verify the result
	if !find {
		t.Errorf("Expected to find addresses, but got find=false")
	}

	if len(address) != param.RequestNum {
		t.Errorf("Expected %d addresses, but got %d", param.RequestNum, len(address))
	}

	for _, addr := range address {
		if addr.Destination == "192.168.1.1" {
			t.Errorf("Excluded address found in results: %s", addr.Destination)
		}
	}

	// Verify if the address comes from a different NodeSet
	for _, addr := range address {
		if addr.DstNodeSetId == param.NodeSetID {
			t.Errorf("Address from excluded NodeSet found in results: %s", addr.Destination)
		}
	}
}

func TestGetMigrateAddrExcludeZone(t *testing.T) {
	// Construct test parameters
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	param := &GetMigrateAddrParam{
		ZoneName:   "testZone1",
		NodeSetID:  1,
		RequestNum: 2,
		Excludes:   []string{"192.168.1.1"},
		LeastSize:  1024,
		Topo: map[string]*proto.ZonePressureView{
			"testZone1": {
				ZoneName: "testZone1",
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.1.1", ID: 1, NodeSetID: 1, ZoneName: "testZone1", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.1.2", ID: 2, NodeSetID: 1, ZoneName: "testZone1", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.1.3", ID: 3, NodeSetID: 1, ZoneName: "testZone1", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
			"testZone2": {
				ZoneName: "testZone2",
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							4: {Addr: "192.168.1.4", ID: 4, NodeSetID: 2, ZoneName: "testZone2", Free: freeSize, NodeMemFree: freeSize},
							5: {Addr: "192.168.1.5", ID: 5, NodeSetID: 2, ZoneName: "testZone2", Free: freeSize, NodeMemFree: freeSize},
							6: {Addr: "192.168.1.6", ID: 6, NodeSetID: 2, ZoneName: "testZone2", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}

	// Call the function under test
	find, address := GetMigrateAddrExcludeZone(param)

	// Verify the result
	if !find {
		t.Errorf("Expected to find addresses, but got find=false")
	}

	if len(address) != param.RequestNum {
		t.Errorf("Expected %d addresses, but got %d", param.RequestNum, len(address))
	}

	for _, addr := range address {
		if addr.Destination == "192.168.1.1" {
			t.Errorf("Excluded address found in results: %s", addr.Destination)
		}
		if addr.DstZoneName == param.ZoneName {
			t.Errorf("Address from excluded zone found in results: %s", addr.Destination)
		}
	}
}

func TestSrcIsPlaned(t *testing.T) {
	// Construct test parameters
	mpPlan := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1", SrcMemSize: 2048, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
			{Source: "192.168.1.2", SrcMemSize: 3072, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
			{Source: "192.168.1.3", SrcMemSize: 1024, SrcNodeSetId: 2, SrcZoneName: "testZone2"},
		},
	}

	// Test case 1: existing source address
	srcAddr := "192.168.1.2"
	index, bExist := SrcIsPlaned(mpPlan, srcAddr)
	if !bExist {
		t.Errorf("Expected source %s to exist, but it does not", srcAddr)
	}
	if index != 1 {
		t.Errorf("Expected index 1 for source %s, but got %d", srcAddr, index)
	}

	// Test case 2: non-existent source address
	srcAddr = "192.168.1.4"
	index, bExist = SrcIsPlaned(mpPlan, srcAddr)
	if bExist {
		t.Errorf("Expected source %s to not exist, but it does", srcAddr)
	}
	if index != -1 {
		t.Errorf("Expected index -1 for non-existent source %s, but got %d", srcAddr, index)
	}

	// Test case 3: empty Plan list
	mpPlanEmpty := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{},
	}
	srcAddr = "192.168.1.1"
	index, bExist = SrcIsPlaned(mpPlanEmpty, srcAddr)
	if bExist {
		t.Errorf("Expected source %s to not exist in empty Plan, but it does", srcAddr)
	}
	if index != -1 {
		t.Errorf("Expected index -1 for non-existent source %s in empty Plan, but got %d", srcAddr, index)
	}
}

func TestUpdateLowPressureNodeTopo(t *testing.T) {
	// Construct test parameters
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"testZone2": {
				ZoneName: "testZone2",
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							4: {Addr: "192.168.1.4", ID: 4, NodeSetID: 2, ZoneName: "testZone2", Total: freeSize, Used: 0, Free: freeSize},
							5: {Addr: "192.168.1.5", ID: 5, NodeSetID: 2, ZoneName: "testZone2", Total: freeSize, Used: 0, Free: freeSize},
							6: {Addr: "192.168.1.6", ID: 6, NodeSetID: 2, ZoneName: "testZone2", Total: freeSize, Used: 0, Free: freeSize},
						},
					},
				},
			},
		},
	}

	newPlan := &proto.MrBalanceInfo{
		Source:       "192.168.1.1",
		SrcMemSize:   2048,
		SrcNodeSetId: 1,
		SrcZoneName:  "testZone1",
		Destination:  "192.168.1.4",
		DstId:        4,
		DstNodeSetId: 2,
		DstZoneName:  "testZone2",
	}

	// Call the function under test
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// Verify the result
	if err != nil {
		t.Errorf("Expected no error, but got: %s", err.Error())
	}

	// Verify metaNode update
	zone, ok := migratePlan.Low["testZone2"]
	if !ok {
		t.Errorf("Expected zone testZone2 to exist, but it does not")
	}

	nodeSet, ok := zone.NodeSet[2]
	if !ok {
		t.Errorf("Expected node set 2 to exist, but it does not")
	}

	metaNode, ok := nodeSet.MetaNodes[4]
	if !ok {
		t.Errorf("Expected meta node 4 to exist, but it does not")
	}

	if metaNode.Used != 2048*metaNodeMemoryRatio {
		t.Errorf("Expected meta node 4 Used to be %d, but got %d", 2048*metaNodeMemoryRatio, metaNode.Used)
	}

	if metaNode.Free != metaNode.Total-metaNode.Used {
		t.Errorf("Expected meta node 4 Free to be %d, but got %d", metaNode.Total-metaNode.Used, metaNode.Free)
	}

	if metaNode.Ratio != float64(metaNode.Used)/float64(metaNode.Total) {
		t.Errorf("Expected meta node 4 Ratio to be %f, but got %f", float64(metaNode.Used)/float64(metaNode.Total), metaNode.Ratio)
	}

	// Verify if metaNode is deleted
	if metaNode.Ratio >= gConfig.metaNodeMemMidPer {
		if _, exists := nodeSet.MetaNodes[metaNode.ID]; exists {
			t.Errorf("Expected meta node 4 to be deleted, but it still exists")
		}
		if nodeSet.Number != 2 {
			t.Errorf("Expected node set 2 Number to be 2, but got %d", nodeSet.Number)
		}
	}
}

func TestUpdateLowPressureNodeTopo_ZoneNotFound(t *testing.T) {
	// Construct test parameters
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{},
	}

	newPlan := &proto.MrBalanceInfo{
		Source:       "192.168.1.1",
		SrcMemSize:   2048,
		SrcNodeSetId: 1,
		SrcZoneName:  "testZone1",
		Destination:  "192.168.1.4",
		DstId:        4,
		DstNodeSetId: 2,
		DstZoneName:  "testZone2",
	}

	// Call the function under test
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// Verify the result
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}

	expectedErr := fmt.Sprintf("Error to get destination zone: %s", newPlan.DstZoneName)
	if err.Error() != expectedErr {
		t.Errorf("Expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestUpdateLowPressureNodeTopo_NodeSetNotFound(t *testing.T) {
	// Construct test parameters
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"testZone2": {
				ZoneName: "testZone2",
				NodeSet:  map[uint64]*proto.NodeSetPressureView{},
			},
		},
	}

	newPlan := &proto.MrBalanceInfo{
		Source:       "192.168.1.1",
		SrcMemSize:   2048,
		SrcNodeSetId: 1,
		SrcZoneName:  "testZone1",
		Destination:  "192.168.1.4",
		DstId:        4,
		DstNodeSetId: 2,
		DstZoneName:  "testZone2",
	}

	// Call the function under test
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// Verify the result
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}

	expectedErr := fmt.Sprintf("Error to get node set %d", newPlan.DstNodeSetId)
	if err.Error() != expectedErr {
		t.Errorf("Expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestUpdateLowPressureNodeTopo_MetaNodeNotFound(t *testing.T) {
	// Construct test parameters
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"testZone2": {
				ZoneName: "testZone2",
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{},
					},
				},
			},
		},
	}

	newPlan := &proto.MrBalanceInfo{
		Source:       "192.168.1.1",
		SrcMemSize:   2048,
		SrcNodeSetId: 1,
		SrcZoneName:  "testZone1",
		Destination:  "192.168.1.4",
		DstId:        4,
		DstNodeSetId: 2,
		DstZoneName:  "testZone2",
	}

	// Call the function under test
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// Verify the result
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}

	expectedErr := fmt.Sprintf("Error to get meta node %d", newPlan.DstId)
	if err.Error() != expectedErr {
		t.Errorf("Expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestFillExcludeAddrIntoGetParam(t *testing.T) {
	// Construct test parameters
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1", SrcMemSize: 2048, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
			{Source: "192.168.1.2", SrcMemSize: 3072, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
		},
		Plan: []*proto.MrBalanceInfo{
			{Destination: "192.168.1.4", DstId: 4, DstNodeSetId: 2, DstZoneName: "testZone2"},
			{Destination: "192.168.1.5", DstId: 5, DstNodeSetId: 2, DstZoneName: "testZone2"},
		},
	}

	getParam := &GetMigrateAddrParam{
		Excludes: []string{},
	}

	// Call the function under test
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// Verify the result
	expectedExcludes := []string{
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.4",
		"192.168.1.5",
	}

	if len(getParam.Excludes) != len(expectedExcludes) {
		t.Errorf("Expected %d excludes, but got %d", len(expectedExcludes), len(getParam.Excludes))
	}

	for i, exclude := range getParam.Excludes {
		if exclude != expectedExcludes[i] {
			t.Errorf("Expected exclude %s at index %d, but got %s", expectedExcludes[i], i, exclude)
		}
	}
}

func TestFillExcludeAddrIntoGetParam_EmptyOriginal(t *testing.T) {
	// Construct test parameters
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{},
		Plan: []*proto.MrBalanceInfo{
			{Destination: "192.168.1.4", DstId: 4, DstNodeSetId: 2, DstZoneName: "testZone2"},
			{Destination: "192.168.1.5", DstId: 5, DstNodeSetId: 2, DstZoneName: "testZone2"},
		},
	}

	getParam := &GetMigrateAddrParam{
		Excludes: []string{},
	}

	// Call the function under test
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// Verify the result
	expectedExcludes := []string{
		"192.168.1.4",
		"192.168.1.5",
	}

	if len(getParam.Excludes) != len(expectedExcludes) {
		t.Errorf("Expected %d excludes, but got %d", len(expectedExcludes), len(getParam.Excludes))
	}

	for i, exclude := range getParam.Excludes {
		if exclude != expectedExcludes[i] {
			t.Errorf("Expected exclude %s at index %d, but got %s", expectedExcludes[i], i, exclude)
		}
	}
}

func TestFillExcludeAddrIntoGetParam_EmptyPlan(t *testing.T) {
	// Construct test parameters
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1", SrcMemSize: 2048, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
			{Source: "192.168.1.2", SrcMemSize: 3072, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
		},
		Plan: []*proto.MrBalanceInfo{},
	}

	getParam := &GetMigrateAddrParam{
		Excludes: []string{},
	}

	// Call the function under test
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// Verify the result
	expectedExcludes := []string{
		"192.168.1.1",
		"192.168.1.2",
	}

	if len(getParam.Excludes) != len(expectedExcludes) {
		t.Errorf("Expected %d excludes, but got %d", len(expectedExcludes), len(getParam.Excludes))
	}

	for i, exclude := range getParam.Excludes {
		if exclude != expectedExcludes[i] {
			t.Errorf("Expected exclude %s at index %d, but got %s", expectedExcludes[i], i, exclude)
		}
	}
}

func TestFillExcludeAddrIntoGetParam_EmptyBoth(t *testing.T) {
	// Construct test parameters
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{},
		Plan:     []*proto.MrBalanceInfo{},
	}

	getParam := &GetMigrateAddrParam{
		Excludes: []string{},
	}

	// Call the function under test
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// Verify the result
	expectedExcludes := []string{}

	if len(getParam.Excludes) != len(expectedExcludes) {
		t.Errorf("Expected %d excludes, but got %d", len(expectedExcludes), len(getParam.Excludes))
	}

	for i, exclude := range getParam.Excludes {
		if exclude != expectedExcludes[i] {
			t.Errorf("Expected exclude %s at index %d, but got %s", expectedExcludes[i], i, exclude)
		}
	}
}

func TestMigratePlanOverLoadToDest(t *testing.T) {
	// Create test data
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"testZone": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		OverLoad: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1"},
			{Source: "192.168.1.2"},
		},
	}
	dests := []*proto.MrBalanceInfo{
		{Destination: "192.168.2.1", DstNodeSetId: 1, DstId: 1, DstZoneName: "testZone"},
		{Destination: "192.168.2.2", DstNodeSetId: 1, DstId: 2, DstZoneName: "testZone"},
	}

	// Call the function under test
	err := MigratePlanOverLoadToDest(migratePlan, mpPlan, dests, false)
	// Verify the result
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestMigratePlanOriginalToDest(t *testing.T) {
	// Create test data
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"testZone": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1"},
			{Source: "192.168.1.2"},
		},
	}
	dests := []*proto.MrBalanceInfo{
		{Destination: "192.168.2.1", DstNodeSetId: 1, DstId: 1, DstZoneName: "testZone"},
		{Destination: "192.168.2.2", DstNodeSetId: 1, DstId: 2, DstZoneName: "testZone"},
	}

	// Call the function under test
	err := MigratePlanOriginalToDest(migratePlan, mpPlan, dests, false)
	// Verify the result
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestFillMigratePlanArray(t *testing.T) {
	// Create test data
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"testZone": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 1, ZoneName: "testZone", Free: metaNodeReserveMemorySize + 1024},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1", SrcMemSize: 1024, SrcNodeSetId: 1, SrcZoneName: "zone1", Destination: "192.168.2.1", DstId: 3},
		},
	}
	srcNode := []*proto.MrBalanceInfo{
		{Source: "192.168.1.1", SrcMemSize: 1024, SrcNodeSetId: 1, SrcZoneName: "zone1"},
		{Source: "192.168.1.2", SrcMemSize: 2048, SrcNodeSetId: 2, SrcZoneName: "zone2"},
	}
	dests := []*proto.MrBalanceInfo{
		{Destination: "192.168.2.1", DstId: 1, DstNodeSetId: 1, DstZoneName: "testZone"},
		{Destination: "192.168.2.2", DstId: 2, DstNodeSetId: 1, DstZoneName: "testZone"},
	}

	// Call the function under test
	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests, false)
	// Verify the result
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	// Verify mpPlan.Plan content
	expectedPlan := []*proto.MrBalanceInfo{
		{Source: "192.168.1.1", Destination: "192.168.2.1", DstId: 1, DstNodeSetId: 1, DstZoneName: "testZone", SrcMemSize: 1024, SrcNodeSetId: 1, SrcZoneName: "zone1", Status: PlanTaskInit},
		{Source: "192.168.1.2", Destination: "192.168.2.2", DstId: 2, DstNodeSetId: 1, DstZoneName: "testZone", SrcMemSize: 2048, SrcNodeSetId: 2, SrcZoneName: "zone2", Status: PlanTaskInit},
	}

	if len(mpPlan.Plan) != len(expectedPlan) {
		t.Errorf("Expected %d items in mpPlan.Plan, got %d", len(expectedPlan), len(mpPlan.Plan))
	}

	for i := range expectedPlan {
		if mpPlan.Plan[i].Source != expectedPlan[i].Source ||
			mpPlan.Plan[i].Destination != expectedPlan[i].Destination ||
			mpPlan.Plan[i].DstId != expectedPlan[i].DstId ||
			mpPlan.Plan[i].DstNodeSetId != expectedPlan[i].DstNodeSetId ||
			mpPlan.Plan[i].DstZoneName != expectedPlan[i].DstZoneName ||
			mpPlan.Plan[i].SrcNodeSetId != expectedPlan[i].SrcNodeSetId ||
			mpPlan.Plan[i].SrcZoneName != expectedPlan[i].SrcZoneName {
			t.Errorf("Mismatch at index %d: expected %v, got %v", i, expectedPlan[i], mpPlan.Plan[i])
		}
	}
}

func TestCreateMigratePlanExcludeNodeSet(t *testing.T) {
	// Mock data
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"zone1": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		// Mock MetaPartitionPlan data
	}
	srcNode := []*proto.MrBalanceInfo{
		{
			SrcMemSize:   1024,
			SrcZoneName:  "zone1",
			SrcNodeSetId: 1,
		},
		{
			SrcMemSize:   2048,
			SrcZoneName:  "zone1",
			SrcNodeSetId: 1,
		},
	}

	// Test case
	err := CreateMigratePlanExcludeNodeSet(migratePlan, mpPlan, srcNode)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestGetSameNodeSetArray(t *testing.T) {
	// Mock data
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{
			{
				SrcNodeSetId: 1,
				// Other fields can be set as needed
			},
			{
				SrcNodeSetId: 2,
				// Other fields can be set as needed
			},
			{
				SrcNodeSetId: 1,
				// Other fields can be set as needed
			},
		},
	}
	mrRec := &proto.MrBalanceInfo{
		SrcNodeSetId: 1,
		// Other fields can be set as needed
	}

	// Expected result
	expected := []*proto.MrBalanceInfo{
		mpPlan.Original[0],
		mpPlan.Original[2],
	}

	// Test case
	result := GetSameNodeSetArray(mpPlan, mrRec)
	if len(result) != len(expected) {
		t.Errorf("Expected %d elements, got %d", len(expected), len(result))
	}
	for i := range expected {
		if result[i].SrcNodeSetId != expected[i].SrcNodeSetId {
			t.Errorf("Element %d: expected SrcNodeSetId %d, got %d", i, expected[i].SrcNodeSetId, result[i].SrcNodeSetId)
		}
	}
}

func TestCreateMigratePlanInNodeSet(t *testing.T) {
	// Mock data
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"zone1": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		// Mock MetaPartitionPlan data
	}
	srcNode := []*proto.MrBalanceInfo{
		{
			Source:       "192.168.1.10",
			SrcMemSize:   1024,
			SrcZoneName:  "zone1",
			SrcNodeSetId: 2,
		},
		{
			Source:       "192.168.1.20",
			SrcMemSize:   2048,
			SrcZoneName:  "zone1",
			SrcNodeSetId: 2,
		},
	}

	// Test case
	err := CreateMigratePlanInNodeSet(migratePlan, mpPlan, srcNode)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test case with no srcNode
	err = CreateMigratePlanInNodeSet(migratePlan, mpPlan, []*proto.MrBalanceInfo{})
	if err != nil {
		t.Errorf("Expected no error for empty srcNode, got %v", err)
	}
}

func TestGetOverLoadNodeArray(t *testing.T) {
	// Mock data
	mpPlan := &proto.MetaBalancePlan{
		OverLoad: []*proto.MrBalanceInfo{
			{
				SrcNodeSetId: 1,
				// Other fields can be set as needed
			},
			{
				SrcNodeSetId: 2,
				// Other fields can be set as needed
			},
			{
				SrcNodeSetId: 1,
				// Other fields can be set as needed
			},
		},
	}
	mrRec := &proto.MrBalanceInfo{
		SrcNodeSetId: 1,
		// Other fields can be set as needed
	}

	// Expected result
	expected := []*proto.MrBalanceInfo{
		mpPlan.OverLoad[0],
		mpPlan.OverLoad[2],
	}

	// Test case
	result := GetOverLoadNodeArray(mpPlan, mrRec)
	if len(result) != len(expected) {
		t.Errorf("Expected %d elements, got %d", len(expected), len(result))
	}
	for i := range expected {
		if result[i].SrcNodeSetId != expected[i].SrcNodeSetId {
			t.Errorf("Element %d: expected SrcNodeSetId %d, got %d", i, expected[i].SrcNodeSetId, result[i].SrcNodeSetId)
		}
	}

	// Test case with no matching SrcNodeSetId
	mrRecNoMatch := &proto.MrBalanceInfo{
		SrcNodeSetId: 3,
		// Other fields can be set as needed
	}
	resultNoMatch := GetOverLoadNodeArray(mpPlan, mrRecNoMatch)
	if len(resultNoMatch) != 0 {
		t.Errorf("Expected 0 elements, got %d", len(resultNoMatch))
	}
}

func TestFindMigrateDestRetainZone(t *testing.T) {
	// Mock data
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"zone1": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{
			{
				SrcNodeSetId: 1,
				Source:       "192.168.1.10",
				SrcZoneName:  "zone1",
				SrcMemSize:   1024,
				// Other fields can be set as needed
			},
			{
				SrcNodeSetId: 1,
				Source:       "192.168.1.20",
				SrcZoneName:  "zone1",
				SrcMemSize:   1024,
				// Other fields can be set as needed
			},
		},
		OverLoad: []*proto.MrBalanceInfo{
			{
				SrcNodeSetId: 1,
				Source:       "192.168.1.10",
				SrcZoneName:  "zone1",
				SrcMemSize:   1024,
				// Other fields can be set as needed
			},
		},
		Plan: []*proto.MrBalanceInfo{},
	}

	// Test case
	err := FindMigrateDestRetainZone(migratePlan, mpPlan)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestFindMigrateDestination(t *testing.T) {
	cluster := &Cluster{}
	// Mock data
	freeSize := uint64(metaNodeReserveMemorySize + 1024)
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"zone1": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					2: {
						NodeSetID: 2,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							1: {Addr: "192.168.2.1", ID: 1, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							2: {Addr: "192.168.2.2", ID: 2, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
							3: {Addr: "192.168.2.3", ID: 3, NodeSetID: 2, ZoneName: "zone1", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
			"zone2": {
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					20: {
						NodeSetID: 20,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							11: {Addr: "192.168.2.11", ID: 11, NodeSetID: 20, ZoneName: "zone2", Free: freeSize, NodeMemFree: freeSize},
							12: {Addr: "192.168.2.12", ID: 12, NodeSetID: 20, ZoneName: "zone2", Free: freeSize, NodeMemFree: freeSize},
							13: {Addr: "192.168.2.13", ID: 13, NodeSetID: 20, ZoneName: "zone2", Free: freeSize, NodeMemFree: freeSize},
						},
					},
				},
			},
		},
	}

	migratePlan.Plan = []*proto.MetaBalancePlan{
		{
			ID:        1000,
			CrossZone: false,
			Original: []*proto.MrBalanceInfo{
				{
					SrcNodeSetId: 1,
					Source:       "192.168.1.10",
					SrcZoneName:  "zone2",
					SrcMemSize:   1024,
					// Other fields can be set as needed
				},
			},
			OverLoad: []*proto.MrBalanceInfo{
				{
					SrcNodeSetId: 1,
					Source:       "192.168.1.10",
					SrcZoneName:  "zone2",
					SrcMemSize:   1024,
					// Other fields can be set as needed
				},
			},
			Plan: []*proto.MrBalanceInfo{},
		},
	}

	// Test case
	err := cluster.FindMigrateDestination(migratePlan)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	migratePlan.Plan = []*proto.MetaBalancePlan{
		{
			ID:        1000,
			CrossZone: true,
			Original: []*proto.MrBalanceInfo{
				{
					SrcNodeSetId: 1,
					Source:       "192.168.1.10",
					SrcZoneName:  "zone1",
					SrcMemSize:   1024,
					// Other fields can be set as needed
				},
			},
			OverLoad: []*proto.MrBalanceInfo{
				{
					SrcNodeSetId: 1,
					Source:       "192.168.1.10",
					SrcZoneName:  "zone1",
					SrcMemSize:   1024,
					// Other fields can be set as needed
				},
			},
			Plan: []*proto.MrBalanceInfo{},
		},
	}

	// Test case where FindMigrateDestRetainZone returns an error
	err = cluster.FindMigrateDestination(migratePlan)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestUpdateMetaReplicaPlanCount(t *testing.T) {
	// Create test data
	mpPlan := &proto.MetaBalancePlan{
		OverLoad: []*proto.MrBalanceInfo{
			{
				Source: "node1",
			},
		},
		InodeCount: 100,
	}

	overLoadNodes := []*proto.MetaNodeBalanceInfo{
		{
			Addr:       "node1",
			InodeCount: 50,
			Total:      1024,
			Used:       1024,
			PlanCnt:    0,
		},
	}

	// Call the function
	err := UpdateMetaReplicaPlanCount(mpPlan, overLoadNodes)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Check if PlanCnt is incremented
	if overLoadNodes[0].PlanCnt != 1 {
		t.Errorf("Expected PlanCnt to be 1, got %d", overLoadNodes[0].PlanCnt)
	}

	// Check if SrcMemSize is updated correctly
	expectedSrcMemSize := uint64(float64(mpPlan.InodeCount) / float64(overLoadNodes[0].InodeCount) * float64(overLoadNodes[0].Used))
	if mpPlan.OverLoad[0].SrcMemSize != expectedSrcMemSize {
		t.Errorf("Expected SrcMemSize to be %d, got %d", expectedSrcMemSize, mpPlan.OverLoad[0].SrcMemSize)
	}
}

func TestGetVolumeCrossZone(t *testing.T) {
	// Create test data
	vols := map[string]*Vol{
		"vol1": {
			TopoSubItem: TopoSubItem{
				crossZone: true,
			},
			MetaPartitions: map[uint64]*MetaPartition{
				1: {
					PartitionID: 1,
				},
			},
		},
		"vol2": {
			TopoSubItem: TopoSubItem{
				crossZone: false,
			},
			MetaPartitions: map[uint64]*MetaPartition{
				2: {
					PartitionID: 2,
				},
			},
		},
	}

	mpPlan1 := &proto.MetaBalancePlan{
		ID: 1,
	}

	mpPlan2 := &proto.MetaBalancePlan{
		ID: 2,
	}

	mpPlan3 := &proto.MetaBalancePlan{
		ID: 3,
	}

	// Test case 1: PartitionID 1 should return true
	result := GetVolumeCrossZone(vols, mpPlan1)
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// Test case 2: PartitionID 2 should return false
	result = GetVolumeCrossZone(vols, mpPlan2)
	if result != false {
		t.Errorf("Expected false, got %v", result)
	}

	// Test case 3: PartitionID 3 should return false
	result = GetVolumeCrossZone(vols, mpPlan3)
	if result != false {
		t.Errorf("Expected false, got %v", result)
	}
}

func TestCheckMetaReplicaIsOverLoad(t *testing.T) {
	// Create test data
	mr1 := &MetaReplica{
		Addr: "node1",
	}

	mr2 := &MetaReplica{
		Addr: "node3",
	}

	overLoadNodes := []*proto.MetaNodeBalanceInfo{
		{
			Addr: "node1",
		},
		{
			Addr: "node2",
		},
	}

	// Test case 1: mr1 should return true
	result := CheckMetaReplicaIsOverLoad(mr1, overLoadNodes)
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// Test case 2: mr2 should return false
	result = CheckMetaReplicaIsOverLoad(mr2, overLoadNodes)
	if result != false {
		t.Errorf("Expected false, got %v", result)
	}
}

func TestCheckMetaPartitionInPlan(t *testing.T) {
	// Create test data
	mp1 := &MetaPartition{
		PartitionID: 1,
	}

	mp2 := &MetaPartition{
		PartitionID: 3,
	}

	migratePlan := &proto.ClusterPlan{
		Plan: []*proto.MetaBalancePlan{
			{
				ID: 1,
			},
			{
				ID: 2,
			},
		},
	}

	// Test case 1: mp1 should return true
	result := CheckMetaPartitionInPlan(mp1, migratePlan)
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// Test case 2: mp2 should return false
	result = CheckMetaPartitionInPlan(mp2, migratePlan)
	if result != false {
		t.Errorf("Expected false, got %v", result)
	}
}

func TestGetMetaReplicaRecord(t *testing.T) {
	// Create test data
	metaNode := &MetaNode{
		Addr:      "node1",
		NodeSetID: 1,
		ZoneName:  "zone1",
	}

	// Call the function
	result := GetMetaReplicaRecord(metaNode)

	// Check if the result matches the expected values
	if result.Source != metaNode.Addr {
		t.Errorf("Expected Source to be %s, got %s", metaNode.Addr, result.Source)
	}

	if result.SrcNodeSetId != metaNode.NodeSetID {
		t.Errorf("Expected SrcNodeSetId to be %d, got %d", metaNode.NodeSetID, result.SrcNodeSetId)
	}

	if result.SrcZoneName != metaNode.ZoneName {
		t.Errorf("Expected SrcZoneName to be %s, got %s", metaNode.ZoneName, result.SrcZoneName)
	}

	if result.Status != PlanTaskInit {
		t.Errorf("Expected Status to be %s, got %s", PlanTaskInit, result.Status)
	}
}

func TestCleanEmptyMetaPartition_NoReplicas(t *testing.T) {
	c := &Cluster{}
	mp := &MetaPartition{PartitionID: 1, Replicas: []*MetaReplica{}}
	err := c.CleanEmptyMetaPartition(mp)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestCleanEmptyMetaPartition_MetaNodeNotFound(t *testing.T) {
	c := &Cluster{}
	// one replica, but cluster has no corresponding MetaNode
	mp := &MetaPartition{PartitionID: 2, Replicas: []*MetaReplica{{Addr: "127.0.0.1:12345"}}}
	err := c.CleanEmptyMetaPartition(mp)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestCleanEmptyMetaPartition_SendFail(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	addr := ln.Addr().String()
	c := &Cluster{}
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mn := &MetaNode{ID: 11, Addr: addr, Sender: newAdminTaskManager(addr, "test-cluster")}
	c.metaNodes.Store(addr, mn)
	mp := &MetaPartition{
		PartitionID: 3,
		Replicas:    []*MetaReplica{{Addr: addr, metaNode: mn}},
	}
	// even if send fails, CleanEmptyMetaPartition should return nil
	err = c.CleanEmptyMetaPartition(mp)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestCleanEmptyMetaPartition_Success(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// serve two sequential connections (for two replicas)
	serves := 2
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < serves; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			p := proto.NewPacket()
			_ = p.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime)
			p.ResultCode = proto.OpOk
			p.Data = []byte("ok")
			p.Size = uint32(len(p.Data))
			_ = p.WriteToConn(conn)
			conn.Close()
		}
	}()

	addr := ln.Addr().String()
	c := &Cluster{}
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()

	mn := &MetaNode{ID: 12, Addr: addr, Sender: newAdminTaskManager(addr, "test-cluster")}
	c.metaNodes.Store(addr, mn)
	mp := &MetaPartition{
		PartitionID: 4,
		Replicas:    []*MetaReplica{{Addr: addr, metaNode: mn}, {Addr: addr, metaNode: mn}},
	}

	if err := c.CleanEmptyMetaPartition(mp); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	<-done
}

// helper: build and store a balance plan into raft store for RunMetaPartitionBalanceTask
func putPlanToStore(t *testing.T, db *raftstore_db.RocksDBStore, plan *proto.ClusterPlan) {
	data, err := json.Marshal(plan)
	require.NoError(t, err)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err = gz.Write(data)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	_, err = db.Put(string([]byte(balanceTaskKey)), buf.Bytes(), true)
	require.NoError(t, err)
}

func newTestClusterWithStore(t *testing.T) *Cluster {
	// minimal cluster with in-memory rocksdb store and nil raft partition
	dir, err := os.MkdirTemp("", "cfs-raftstore")
	require.NoError(t, err)
	db, err := raftstore_db.NewRocksDBStoreAndRecovery(dir, LRUCacheSize, WriteBufferSize)
	require.NoError(t, err)
	return &Cluster{
		fsm:   &MetadataFsm{store: db},
		stopc: make(chan bool, 1),
	}
}

func TestRunMetaPartitionBalanceTask_NoPlan(t *testing.T) {
	c := newTestClusterWithStore(t)
	// loadBalanceTask should return ErrNoMpMigratePlan -> propagate error
	err := c.RunMetaPartitionBalanceTask()
	if err == nil {
		t.Errorf("expected error when no plan, got nil")
	}
}

func TestRunMetaPartitionBalanceTask_StartsGoroutine(t *testing.T) {
	c := newTestClusterWithStore(t)
	// prepare a simple plan in store
	plan := &proto.ClusterPlan{Status: PlanTaskInit}
	putPlanToStore(t, c.fsm.store, plan)
	// make partition non-nil and set IsRaftLeader true via stub
	c.partition = &mockPartition{isLeader: true}
	err := c.RunMetaPartitionBalanceTask()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// the goroutine sets PlanRun=true; give a brief wait then stop
	time.Sleep(10 * time.Millisecond)
	c.stopc <- true
}

// mockPartition to satisfy raftstore.Partition with minimal methods used
type mockPartition struct{ isLeader bool }

func (m *mockPartition) Submit([]byte) (interface{}, error) { return nil, nil }
func (m *mockPartition) ChangeMember(_ raftproto.ConfChangeType, _ raftproto.Peer, _ []byte) (interface{}, error) {
	return nil, nil
}
func (m *mockPartition) Stop() error                        { return nil }
func (m *mockPartition) Delete() error                      { return nil }
func (m *mockPartition) Status() *raftstore.PartitionStatus { return nil }
func (m *mockPartition) IsRestoring() bool                  { return false }
func (m *mockPartition) LeaderTerm() (uint64, uint64)       { return 0, 0 }
func (m *mockPartition) IsRaftLeader() bool                 { return m.isLeader }
func (m *mockPartition) AppliedIndex() uint64               { return 0 }
func (m *mockPartition) CommittedIndex() uint64             { return 0 }
func (m *mockPartition) Truncate(uint64)                    {}
func (m *mockPartition) TryToLeader(uint64) error           { return nil }
func (m *mockPartition) IsOfflinePeer() bool                { return true }
func (m *mockPartition) CloseAndBackup() error              { return nil }
func (m *mockPartition) Closed() bool                       { return false }

// no extra stub needed for raftstore.PartitionStatus alias

func TestHandleMetaReplicaPlan_StatusTransitions(t *testing.T) {
	c := &Cluster{stopc: make(chan bool, 1), partition: &mockPartition{isLeader: true}}
	addr := "127.0.0.1:17210"
	addr2 := "127.0.0.2:17210"
	mn := &MetaNode{ID: 1, Addr: addr, IsActive: true, MaxMemAvailWeight: gConfig.metaNodeReservedMem * 2}
	c.metaNodes.Store(addr, mn)
	mp := &MetaPartition{PartitionID: 200, Replicas: []*MetaReplica{{Addr: addr, metaNode: mn, IsLeader: false}, {Addr: addr2, metaNode: mn, IsLeader: true}}}
	plan := &proto.ClusterPlan{
		Type: AutoPlan,
		Mode: proto.StoreModeMem,
	}
	mpPlan := &proto.MetaBalancePlan{ID: 200}
	mrPlan := &proto.MrBalanceInfo{Source: addr, Destination: addr, Status: PlanTaskInit}
	c.SetClusterPlanRunning()
	// since destination equals existing host, expect error from doMetaPartitionMigrate
	err := c.handleMetaReplicaPlan(plan, mpPlan, mp, mrPlan)
	if err == nil {
		t.Errorf("expected error due to invalid destination, got nil")
	}
	// status should have transitioned to PlanTaskRun before failing
	require.Equal(t, PlanTaskRun, mrPlan.Status)
}

func TestWaitForMetaPartitionMigrateDone_TimeoutThenStop(t *testing.T) {
	c := &Cluster{stopc: make(chan bool, 1)}
	mp := &MetaPartition{PartitionID: 300, Replicas: []*MetaReplica{{Addr: "nodeX", IsLeader: true}}}
	// make CheckRaftStatus always false by not setting metaNode/Sender; and trigger stop via stopc
	go func() {
		time.Sleep(5 * time.Millisecond)
		c.stopc <- true
	}()
	err := c.WaitForMetaPartitionMigrateDone(mp, "nodeX")
	if err == nil {
		t.Errorf("expected error due to cluster stopping or timeout, got nil")
	}
}

func TestFillOffLineAddrToPlan_NoSuchNode(t *testing.T) {
	c := &Cluster{}
	plan := &proto.ClusterPlan{}
	err := c.FillOffLineAddrToPlan("node-not-exist", plan)
	if err == nil {
		t.Errorf("expected error when metanode not found")
	}
}

func TestCreateOfflineMetaNodePlan_FillError(t *testing.T) {
	// topology empty, metaNodes empty -> FillOffLineAddrToPlan fails
	c := &Cluster{ClusterTopoSubItem: ClusterTopoSubItem{t: &topology{zones: []*Zone{}, zoneMap: new(sync.Map)}}}
	plan, err := c.CreateOfflineMetaNodePlan("nodeX")
	if err == nil {
		t.Errorf("expected error due to FillOffLineAddrToPlan failure, got nil plan=%+v", plan)
	}
}

func TestChangeAndCheckMetaPartitionLeader_AlreadyDifferent(t *testing.T) {
	c := &Cluster{}
	mp := &MetaPartition{
		PartitionID: 1,
		Replicas: []*MetaReplica{
			{Addr: "src", IsLeader: false},
			{Addr: "other", IsLeader: true},
		},
	}
	mrPlan := &proto.MrBalanceInfo{Source: "src"}
	mpPlan := &proto.MetaBalancePlan{}
	err := c.changeAndCheckMetaPartitionLeader(mrPlan, mpPlan, mp)
	if err != nil {
		t.Errorf("expected nil when leader already not source, got %v", err)
	}
}

func TestSelectOneLeaderAddr(t *testing.T) {
	mp := &MetaPartition{
		Replicas: []*MetaReplica{{Addr: "a"}, {Addr: "b"}, {Addr: "c"}},
	}
	mpPlan := &proto.MetaBalancePlan{Plan: []*proto.MrBalanceInfo{{Source: "a"}}}
	got := selectOneLeaderAddr(&proto.MrBalanceInfo{Source: "a"}, mpPlan, mp, []string{"b"})
	if got == "" || got == "a" || got == "b" {
		t.Errorf("unexpected addr: %s", got)
	}
}

func TestWaitForMetaPartitionReady_LeaderExist(t *testing.T) {
	c := &Cluster{stopc: make(chan bool, 1)}
	mp := &MetaPartition{PartitionID: 2, Replicas: []*MetaReplica{{Addr: "x", IsLeader: true}}}
	if err := c.waitForMetaPartitionReady(mp); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestGetMpCountByMetaNode(t *testing.T) {
	c := &Cluster{}
	c.ClusterVolSubItem.vols = map[string]*Vol{
		"v1": {Name: "v1", MetaPartitions: map[uint64]*MetaPartition{1: {PartitionID: 1, Hosts: []string{"n1"}}}, mpsLock: new(mpsLockManager)},
		"v2": {Name: "v2", MetaPartitions: map[uint64]*MetaPartition{2: {PartitionID: 2, Hosts: []string{"n2", "n1"}}}, mpsLock: new(mpsLockManager)},
	}
	if cnt := c.GetMpCountByMetaNode("n1"); cnt != 2 {
		t.Errorf("expected 2, got %d", cnt)
	}
}

func TestCalculateMetaPartitionFreezeCount(t *testing.T) {
	c := &Cluster{}
	v := &Vol{Name: "v", MetaPartitions: map[uint64]*MetaPartition{
		1: {PartitionID: 1, Freeze: proto.FreezeMetaPartitionInit},
		2: {PartitionID: 2, Freeze: proto.FreezingMetaPartition},
		3: {PartitionID: 3, Freeze: proto.FreezedMetaPartition},
	}, mpsLock: new(mpsLockManager)}
	c.ClusterVolSubItem.vols = map[string]*Vol{"v": v}
	got, err := c.CalculateMetaPartitionFreezeCount("v")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.UnFreeze != 1 || got.Freezing != 1 || got.Freezed != 1 {
		t.Errorf("unexpected counts: %+v", got)
	}
}

func TestCheckMetaReplicasIsRocksdb(t *testing.T) {
	mp := &MetaPartition{Replicas: []*MetaReplica{{StoreMode: proto.StoreModeMem}, {StoreMode: proto.StoreModeRocksDb}}}
	if !checkMetaReplicasIsRocksdb(mp) {
		t.Errorf("expected true")
	}
}

func TestIsRocksdbDiskUsageLow(t *testing.T) {
	low := &MetaNode{RocksdbDisks: []*proto.MetaNodeRocksdbInfo{{UsageRatio: gConfig.metaNodeMemLowPer - 0.01}}}
	if !IsRocksdbDiskUsageLow(low) {
		t.Errorf("expected true for low usage")
	}
	high := &MetaNode{RocksdbDisks: []*proto.MetaNodeRocksdbInfo{{UsageRatio: gConfig.metaNodeMemLowPer + 0.01}}}
	if IsRocksdbDiskUsageLow(high) {
		t.Errorf("expected false for high usage")
	}
}

func TestGetMetaPartitionMemorySize(t *testing.T) {
	// zero count
	mp := &MetaPartition{
		InodeCount:  100,
		DentryCount: 100,
	}
	memSize := GetMetaPartitionMemorySize(mp)
	require.Equal(t, uint64(MetaPartitionMemMin), memSize)

	mp.InodeCount = 1000000
	mp.DentryCount = 1000000
	memSize = GetMetaPartitionMemorySize(mp)
	checkValue := mp.InodeCount*MetaPartitionInodeSize + mp.DentryCount*MetaPartitionDentrySize
	require.Equal(t, checkValue, memSize)
}

func TestCalcuMetaPartitionReadyMaxRetry(t *testing.T) {
	mp := &MetaPartition{InodeCount: MaxInodePerMp}
	if v := CalcuMetaPartitionReadyMaxRetry(mp); v != RetryCheckStatusNum {
		t.Errorf("expected %d, got %d", RetryCheckStatusNum, v)
	}
	mp = &MetaPartition{InodeCount: MaxInodePerMp * 2}
	expected := int(mp.InodeCount / MaxInodePerMp * RetryCheckStatusNum)
	if v := CalcuMetaPartitionReadyMaxRetry(mp); v != expected {
		t.Errorf("expected %d, got %d", expected, v)
	}
}

func TestGetAllMetaPartitions(t *testing.T) {
	c := &Cluster{}
	c.ClusterVolSubItem.vols = map[string]*Vol{
		"v1": {Name: "v1", MetaPartitions: map[uint64]*MetaPartition{1: {PartitionID: 1}}, mpsLock: new(mpsLockManager)},
		"v2": {Name: "v2", MetaPartitions: map[uint64]*MetaPartition{2: {PartitionID: 2}, 3: {PartitionID: 3}}, mpsLock: new(mpsLockManager)},
	}
	got := c.getAllMetaPartitions()
	if len(got) != 3 || got[1] == nil || got[2] == nil || got[3] == nil {
		t.Errorf("unexpected result: %+v", got)
	}
}

func TestDoMetaPartitionMigrate_ErrPlanStopped(t *testing.T) {
	c := &Cluster{}
	c.SetClusterPlanIdle()
	mp := &MetaPartition{PartitionID: 10, Replicas: []*MetaReplica{}}
	mpPlan := &proto.MetaBalancePlan{ID: 10}
	plan := &proto.ClusterPlan{}
	mrPlan := &proto.MrBalanceInfo{Source: "s", Destination: "d"}
	if err := c.doMetaPartitionMigrate(plan, mpPlan, mrPlan, mp); err == nil {
		t.Errorf("expected error when plan stopped")
	}
}

func TestDoMetaPartitionMigrate_ErrNotLeader(t *testing.T) {
	c := &Cluster{partition: &mockPartition{isLeader: false}}
	c.SetClusterPlanRunning()
	mp := &MetaPartition{PartitionID: 10, Replicas: []*MetaReplica{}}
	mpPlan := &proto.MetaBalancePlan{ID: 10}
	plan := &proto.ClusterPlan{}
	mrPlan := &proto.MrBalanceInfo{Source: "s", Destination: "d"}
	if err := c.doMetaPartitionMigrate(plan, mpPlan, mrPlan, mp); err == nil {
		t.Errorf("expected error when not raft leader")
	}
}

func TestIsRetryMigrateMpError(t *testing.T) {
	for _, msg := range []string{"no leader", "try again", "deadline exceeded", "connection refused", "no route to host", "downreplicas so donnot offline"} {
		if !IsRetryMigrateMpError(fmt.Errorf(msg)) {
			t.Errorf("expected retryable for %q", msg)
		}
	}
	if IsRetryMigrateMpError(fmt.Errorf("some fatal error")) {
		t.Errorf("expected non-retryable")
	}
}

func TestCalculateMetaNodeEstimate(t *testing.T) {
	// Estimate calculation determines how many meta partitions should be moved
	// away from each overloaded node. The table covers normal input, invalid
	// ratio input, and the minimum-estimate clamp.
	tests := []struct {
		name             string
		overLoadNodes    []*proto.MetaNodeBalanceInfo
		expectedError    error
		expectedEstimate []int
	}{
		{
			name: "Valid MetaNodeRec",
			overLoadNodes: []*proto.MetaNodeBalanceInfo{
				{Ratio: 0.8, MpCount: 100, NodeMemRatio: 0.7},
			},
			expectedError:    nil,
			expectedEstimate: []int{1},
		},
		{
			name: "MetaNodeRec with Ratio <= 0",
			overLoadNodes: []*proto.MetaNodeBalanceInfo{
				{Ratio: -0.1, MpCount: 100, NodeMemRatio: 0.2},
			},
			expectedError:    fmt.Errorf("The meta node ratio (-0.100000) is <= 0"),
			expectedEstimate: nil,
		},
		{
			name: "MetaNodeRec with Estimate <= 0",
			overLoadNodes: []*proto.MetaNodeBalanceInfo{
				{Ratio: 0.01, MpCount: 1, NodeMemRatio: 0.2},
			},
			expectedError:    nil,
			expectedEstimate: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// CalculateMetaNodeEstimate mutates each node record in place by
			// filling the Estimate field.
			err := CalculateMetaNodeEstimate(tt.overLoadNodes)
			if tt.expectedError != nil {
				if err == nil || err.Error() != tt.expectedError.Error() {
					t.Errorf("CalculateMetaNodeEstimate() error = %v, wantErr %v", err, tt.expectedError)
				}
			} else if err != nil {
				t.Errorf("CalculateMetaNodeEstimate() error = %v, wantErr %v", err, tt.expectedError)
			}

			for i, metaNode := range tt.overLoadNodes {
				if tt.expectedError != nil {
					continue
				}
				// Successful cases should leave the expected migration estimate
				// on the corresponding overloaded node.
				if metaNode.Estimate != tt.expectedEstimate[i] {
					t.Errorf("CalculateMetaNodeEstimate() Estimate = %v, want %v", metaNode.Estimate, tt.expectedEstimate[i])
				}
			}
		})
	}
}

func TestGetLowMemPressureTopology(t *testing.T) {
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	// Create a Cluster instance with one zone and one nodeset containing two
	// low-pressure metanodes.
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			t: &topology{
				zones: []*Zone{
					{
						name: "zone1",
						nodeSetMap: map[uint64]*nodeSet{
							1: {
								ID:        1,
								metaNodes: new(sync.Map),
							},
						},
					},
				},
				zoneMap: new(sync.Map),
			},
		},
	}
	// The nodes are active and have enough reserved memory, so both should appear
	// in the low-pressure topology.
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.1, IsActive: true, MaxMemAvailWeight: size10GB})
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node2", &MetaNode{ID: 102, Ratio: 0.2, IsActive: true, MaxMemAvailWeight: size10GB})
	cluster.t.zoneMap.Store(cluster.t.zones[0].name, cluster.t.zones[0])

	// GetLowMemPressureTopology writes eligible memory and rocksdb candidates
	// into these maps.
	migratePlan := &proto.ClusterPlan{
		Low:        make(map[string]*proto.ZonePressureView),
		RocksdbLow: make(map[string]*proto.ZonePressureView),
	}

	// Call the function under test
	err := cluster.GetLowMemPressureTopology(migratePlan)
	if err != nil {
		t.Errorf("Expect no error. but get: %s", err.Error())
	}

	// Verify the result. The assertion focuses on node count because the full
	// MetaNodeBalanceInfo records contain many fields unrelated to this test.
	expectedZoneView := &proto.ZonePressureView{
		ZoneName: "zone1",
		NodeSet: map[uint64]*proto.NodeSetPressureView{
			1: {
				NodeSetID: 1,
				MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
					101: {ID: 101},
					102: {ID: 102},
				},
				Number: 2,
			},
		},
	}

	actualZoneView, ok := migratePlan.Low["zone1"]
	if !ok {
		t.Errorf("Expect ok == true, but it is false.")
	}
	if expectedZoneView.NodeSet[1].Number != actualZoneView.NodeSet[1].Number {
		t.Errorf("expect %d not equal actual %d", expectedZoneView.NodeSet[1].Number, actualZoneView.NodeSet[1].Number)
	}
}

func TestVerifyMetaReplicaPlanNotAllInit(t *testing.T) {
	// Test case 1: all statuses are PlanTaskInit, so there is no in-flight or
	// completed replica work yet.
	mpPlan1 := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Status: PlanTaskInit},
			{Status: PlanTaskInit},
		},
	}
	if VerifyMetaReplicaPlanNotAllInit(mpPlan1) {
		t.Errorf("Expected false, got true")
	}

	// Test case 2: one status is not PlanTaskInit, which indicates that the plan
	// has started progressing.
	mpPlan2 := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Status: PlanTaskInit},
			{Status: PlanTaskRun}, // Assuming PlanTaskRunning is a valid status
		},
	}
	if !VerifyMetaReplicaPlanNotAllInit(mpPlan2) {
		t.Errorf("Expected true, got false")
	}

	// Test case 3: empty Plan slice should be treated as "all init" because no
	// replica has progressed.
	mpPlan3 := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{},
	}
	if VerifyMetaReplicaPlanNotAllInit(mpPlan3) {
		t.Errorf("Expected false, got true")
	}
}

func TestVerifyMetaNodeExceedMemMid(t *testing.T) {
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	// Test case 1: node1 is above the mid watermark and should be treated as
	// exceeding the target load.
	cluster := &Cluster{}
	cluster.metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.8, IsActive: true, MaxMemAvailWeight: size10GB})
	cluster.metaNodes.Store("node2", &MetaNode{ID: 102, Ratio: 0.5, IsActive: true, MaxMemAvailWeight: size10GB})

	result1, err1 := cluster.VerifyMetaNodeExceedMemMid("node1", proto.StoreModeMem)
	if err1 != nil || !result1 {
		t.Errorf("Expected true, got %v, err: %v", result1, err1)
	}

	// Test case 2: node2 is below the mid watermark and should not be considered
	// overloaded for this check.
	result2, err2 := cluster.VerifyMetaNodeExceedMemMid("node2", proto.StoreModeMem)
	if err2 != nil || result2 {
		t.Errorf("Expected false, got %v, err: %v", result2, err2)
	}

	// Test case 3: missing metanodes return an error and a false result.
	result3, err3 := cluster.VerifyMetaNodeExceedMemMid("node3", proto.StoreModeMem)
	if err3 == nil || result3 {
		t.Errorf("Expected error, got %v, result: %v", err3, result3)
	}
}

func TestUpdateMigrateDestination(t *testing.T) {
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	// Test case 1: all methods succeed. The topology has enough low-pressure
	// nodes in the same zone for destination assignment.
	totalSize := uint64(metaNodeReserveMemorySize * 2)
	cluster := &Cluster{
		ClusterTopoSubItem: ClusterTopoSubItem{
			t: &topology{
				zones: []*Zone{
					{
						name: "zone1",
						nodeSetMap: map[uint64]*nodeSet{
							1: {
								ID:        1,
								metaNodes: new(sync.Map),
							},
						},
					},
				},
				zoneMap: new(sync.Map),
			},
		},
	}
	// Populate the topology with eligible destination nodes.
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node1", &MetaNode{
		ID: 101, Addr: "node1", Ratio: 0.1, Total: totalSize,
		NodeMemTotal: totalSize, ZoneName: "zone1", NodeSetID: 1,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node2", &MetaNode{
		ID: 102, Addr: "node2", Ratio: 0.2, Total: totalSize,
		NodeMemTotal: totalSize, ZoneName: "zone1", NodeSetID: 1,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node3", &MetaNode{
		ID: 103, Addr: "node3", Ratio: 0.2, Total: totalSize,
		NodeMemTotal: totalSize, ZoneName: "zone1", NodeSetID: 1,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zoneMap.Store(cluster.t.zones[0].name, cluster.t.zones[0])
	// UpdateMigrateDestination expects the low-pressure maps to be initialized.
	migratePlan := &proto.ClusterPlan{
		Low:        map[string]*proto.ZonePressureView{},
		RocksdbLow: map[string]*proto.ZonePressureView{},
	}
	// CrossZone=true drives the retain-zone destination path.
	mpPlan := &proto.MetaBalancePlan{
		CrossZone: true,
		OverLoad: []*proto.MrBalanceInfo{
			{
				Source:       "node4",
				SrcZoneName:  "zone1",
				SrcNodeSetId: 1,
			},
		},
	}
	err1 := cluster.UpdateMigrateDestination(migratePlan, mpPlan)
	if err1 != nil {
		t.Errorf("Expected no error, got %v", err1)
	}

	// Test case 2: CrossZone=false drives the one-nodeset path. The low-pressure
	// topology still has enough candidates to satisfy the request.
	mpPlan = &proto.MetaBalancePlan{
		CrossZone: false,
		Original: []*proto.MrBalanceInfo{
			{
				Source:       "node4",
				SrcZoneName:  "zone2",
				SrcNodeSetId: 10,
			},
			{
				Source:       "node5",
				SrcZoneName:  "zone2",
				SrcNodeSetId: 10,
			},
			{
				Source:       "node6",
				SrcZoneName:  "zone2",
				SrcNodeSetId: 10,
			},
		},
		OverLoad: []*proto.MrBalanceInfo{
			{
				Source:       "node4",
				SrcZoneName:  "zone2",
				SrcNodeSetId: 10,
			},
		},
	}
	err1 = cluster.UpdateMigrateDestination(migratePlan, mpPlan)
	if err1 != nil {
		t.Errorf("Expected no error, got %v", err1)
	}
}

func TestFindMigrateDestInOneNodeSet(t *testing.T) {
	// Mock data: low-pressure candidates exist in the same zone and nodeset as
	// the overloaded source, which should let the helper build a plan directly.
	freeSize := uint64(metaNodeReserveMemorySize * 2)
	migratePlan := &proto.ClusterPlan{
		Low: map[string]*proto.ZonePressureView{
			"zone1": {
				ZoneName: "zone1",
				NodeSet: map[uint64]*proto.NodeSetPressureView{
					1: {
						NodeSetID: 1,
						Number:    3,
						MetaNodes: map[uint64]*proto.MetaNodeBalanceInfo{
							101: {
								ID:          101,
								Addr:        "node1",
								NodeSetID:   1,
								ZoneName:    "zone1",
								Free:        freeSize,
								NodeMemFree: freeSize,
							},
							102: {
								ID:          102,
								Addr:        "node2",
								NodeSetID:   1,
								ZoneName:    "zone1",
								Free:        freeSize,
								NodeMemFree: freeSize,
							},
							103: {
								ID:          103,
								Addr:        "node3",
								NodeSetID:   1,
								ZoneName:    "zone1",
								Free:        freeSize,
								NodeMemFree: freeSize,
							},
						},
					},
				},
			},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		CrossZone: false,
		Original: []*proto.MrBalanceInfo{
			{
				Source:       "node4",
				SrcZoneName:  "zone1",
				SrcNodeSetId: 2,
			},
			{
				Source:       "node5",
				SrcZoneName:  "zone1",
				SrcNodeSetId: 2,
			},
			{
				Source:       "node6",
				SrcZoneName:  "zone1",
				SrcNodeSetId: 2,
			},
		},
		OverLoad: []*proto.MrBalanceInfo{
			{
				Source:       "node4",
				SrcZoneName:  "zone1",
				SrcNodeSetId: 2,
			},
		},
	}

	// Test
	err := FindMigrateDestInOneNodeSet(migratePlan, mpPlan)
	if err != nil {
		t.Errorf("FindMigrateDestInOneNodeSet failed: %v", err)
	}
}

func TestAddMetaPartitionIntoPlan(t *testing.T) {
	// The cluster fixture contains one volume with two meta partitions on the
	// same overloaded source node. This is the minimal shape needed for
	// AddMetaPartitionIntoPlan to scan volume metadata and create candidate plans.
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{
				"vol1": {
					Name: "vol1",
					MetaPartitions: map[uint64]*MetaPartition{
						1000: {
							PartitionID: 1000,
							InodeCount:  10,
							Hosts:       []string{"node1"},
							Replicas: []*MetaReplica{
								{
									Addr:      "node1",
									StoreMode: proto.StoreModeMem,
								},
							},
						},
						1001: {
							PartitionID: 1001,
							InodeCount:  20,
							Hosts:       []string{"node1"},
							Replicas: []*MetaReplica{
								{
									Addr:      "node1",
									StoreMode: proto.StoreModeMem,
								},
							},
						},
					},
					mpsLock: new(mpsLockManager),
				},
			},
		},
	}
	// The source metanode must exist in the cluster cache because
	// AddMetaPartitionIntoPlan enriches replica records from metanode metadata.
	cluster.metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.8})
	metaNode := &proto.MetaNodeBalanceInfo{
		Addr:     "node1",
		Estimate: 1,
	}
	// overLoads provides the memory baseline used to estimate how much each
	// source replica contributes to the overloaded node.
	migratePlan := &proto.ClusterPlan{}
	overLoads := []*proto.MetaNodeBalanceInfo{
		{
			Addr:       "node1",
			Total:      metaNodeReserveMemorySize * 2,
			Ratio:      0.8,
			InodeCount: 30000,
		},
	}

	// The assertion is intentionally broad here: the test verifies that the
	// minimal fixture is sufficient to build plan entries without error.
	err := cluster.AddMetaPartitionIntoPlan(metaNode, migratePlan, overLoads)
	if err != nil {
		t.Errorf("AddMetaPartitionIntoPlan failed: %v", err)
	}
}

func TestCreateMetaPartitionMigratePlan(t *testing.T) {
	// Create a cluster instance with one high-pressure metanode. The function
	// under test reads c.metaNodes and converts eligible nodes into a migrate
	// plan skeleton.
	cluster := &Cluster{}
	cluster.metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.8, NodeMemTotal: 1000000, NodeMemUsed: 900000, MetaPartitionCount: 1000})

	// The migrate plan starts empty; CreateMetaPartitionMigratePlan is expected
	// to populate it from the current cluster pressure view.
	migratePlan := &proto.ClusterPlan{}

	// Normal case: a high-pressure node exists, so plan creation should not
	// return an error for this compact fixture.
	err := cluster.CreateMetaPartitionMigratePlan(migratePlan)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func PrintMigratePlan(plan *proto.ClusterPlan) {
	// This helper is used only while debugging test fixtures. It prints both the
	// low-pressure topology and the generated plan in a stable JSON form.
	body, err := json.MarshalIndent(plan.Low, "", "    ")
	if err != nil {
		fmt.Println("Error to encode as json:", err.Error())
		return
	}
	fmt.Println("low memory node topology:", string(body))

	body, err = json.MarshalIndent(plan, "", "    ")
	if err != nil {
		fmt.Println("Error to encode as json:", err.Error())
		return
	}
	fmt.Println("create migrate plan:", string(body))
}

func TestGetMetaNodePressureView(t *testing.T) {
	// The pressure view test builds a small two-zone topology. zone2 contains
	// one overloaded source replica and several low-pressure destinations.
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: map[string]*Vol{
				"vol1": {
					Name: "vol1",
					MetaPartitions: map[uint64]*MetaPartition{
						1000: {
							PartitionID: 1000,
							InodeCount:  10,
							Hosts:       []string{"node4", "node5", "node6"},
							Replicas: []*MetaReplica{
								{
									Addr:      "node4",
									StoreMode: proto.StoreModeMem,
								},
								{
									Addr:      "node5",
									StoreMode: proto.StoreModeMem,
								},
								{
									Addr:      "node6",
									StoreMode: proto.StoreModeMem,
								},
							},
						},
					},
					mpsLock: new(mpsLockManager),
				},
			},
		},
		ClusterTopoSubItem: ClusterTopoSubItem{
			t: &topology{
				zones: []*Zone{
					{
						name: "zone1",
						nodeSetMap: map[uint64]*nodeSet{
							1: {
								ID:        1,
								metaNodes: new(sync.Map),
							},
						},
					},
					{
						name: "zone2",
						nodeSetMap: map[uint64]*nodeSet{
							20: {
								ID:        20,
								metaNodes: new(sync.Map),
							},
							30: {
								ID:        30,
								metaNodes: new(sync.Map),
							},
						},
					},
				},
				zoneMap: new(sync.Map),
			},
		},
	}
	totalSize := uint64(metaNodeReserveMemorySize * 2)
	// node4 is the overloaded source; node5 and node6 are healthy peers in the
	// same partition and should be considered original replicas, not targets.
	cluster.metaNodes.Store("node4", &MetaNode{
		ID: 201, Addr: "node4", NodeSetID: 20,
		MetaPartitionCount: 10, ZoneName: "zone2", Ratio: 0.8,
		NodeMemTotal: totalSize, NodeMemUsed: 8192,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.metaNodes.Store("node5", &MetaNode{
		ID: 202, Addr: "node5", NodeSetID: 20,
		MetaPartitionCount: 10, ZoneName: "zone2", Ratio: 0.0001,
		NodeMemTotal: totalSize, NodeMemUsed: 8192,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.metaNodes.Store("node6", &MetaNode{
		ID: 203, Addr: "node6", NodeSetID: 20,
		MetaPartitionCount: 10, ZoneName: "zone2", Ratio: 0.0001,
		NodeMemTotal: totalSize, NodeMemUsed: 8192,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})

	// node10 is a low-pressure node in the same nodeset as the overloaded source,
	// so it exercises the first destination preference path.
	cluster.t.zones[1].nodeSetMap[20].metaNodes.Store("node10", &MetaNode{
		ID: 110, Addr: "node10", Ratio: 0.1,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 20,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})

	// node7, node8, and node9 provide additional low-pressure capacity in a
	// different nodeset in the same zone for fallback selection.
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Store("node7", &MetaNode{
		ID: 107, Addr: "node7", Ratio: 0.1,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 30,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Store("node8", &MetaNode{
		ID: 108, Addr: "node8", Ratio: 0.1,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 30,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Store("node9", &MetaNode{
		ID: 109, Addr: "node9", Ratio: 0.1,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 30,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})

	// zone1 nodes make the cross-zone fallback topology non-empty.
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node1", &MetaNode{
		ID: 101, Addr: "node1", Ratio: 0.1,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone1", NodeSetID: 1,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node2", &MetaNode{
		ID: 102, Addr: "node2", Ratio: 0.2,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone1", NodeSetID: 1,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node3", &MetaNode{
		ID: 103, Addr: "node3", Ratio: 0.2,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone1", NodeSetID: 1,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})

	cluster.t.zoneMap.Store(cluster.t.zones[0].name, cluster.t.zones[0])
	cluster.t.zoneMap.Store(cluster.t.zones[1].name, cluster.t.zones[1])

	// Case 1: find meta node under the same node set.
	result, err := cluster.GetMetaNodePressureView()
	// A non-empty plan proves the pressure scan found the overloaded source and
	// at least one suitable destination.
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}
	if len(result.Plan) <= 0 {
		t.Errorf("GetMetaNodePressureView returned an empty plan")
	}
	if len(result.Plan[0].Plan) <= 0 {
		t.Errorf("GetMetaNodePressureView returned an empty size")
	}
	for _, mrPlan := range result.Plan[0].Plan {
		if mrPlan.SrcNodeSetId != mrPlan.DstNodeSetId {
			t.Errorf("GetMetaNodePressureView returned an unexpected plan. src(%s) srcNodeSet(%d) dst(%s) dstNodeSet(%d)",
				mrPlan.Source, mrPlan.SrcNodeSetId, mrPlan.Destination, mrPlan.DstNodeSetId)
		}
	}

	// Case 2: find meta node under different node set under the same zone.
	cluster.t.zones[1].nodeSetMap[20].metaNodes.Delete("node10")
	_, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}

	// Case 3: find meta node in different zone.
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Delete("node7")
	_, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}

	// Case 4: test CrossZone == true. Find meta node under the same node set.
	cluster.vols["vol1"].crossZone = true
	cluster.t.zones[1].nodeSetMap[20].metaNodes.Store("node10", &MetaNode{
		ID: 110, Addr: "node10", Ratio: 0.1,
		Total: metaNodeReserveMemorySize * 2, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 20,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Store("node7", &MetaNode{
		ID: 107, Addr: "node7", Ratio: 0.1,
		Total: metaNodeReserveMemorySize * 2, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 30,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})
	cluster.metaNodes.Delete("node6")
	cluster.metaNodes.Store("node6", &MetaNode{
		ID: 203, Addr: "node6", NodeSetID: 50,
		MetaPartitionCount: 10, ZoneName: "zone3",
		Ratio: 0.0001, NodeMemTotal: totalSize, NodeMemUsed: 8192,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})

	result, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}
	if len(result.Plan) <= 0 || len(result.Plan[0].Plan) <= 0 {
		t.Errorf("GetMetaNodePressureView returned an unexpected plan")
	}

	for _, mrPlan := range result.Plan[0].Plan {
		if mrPlan.SrcNodeSetId != mrPlan.DstNodeSetId {
			t.Errorf("GetMetaNodePressureView returned an unexpected plan. src(%s) srcNodeSet(%d) dst(%s) dstNodeSet(%d)",
				mrPlan.Source, mrPlan.SrcNodeSetId, mrPlan.Destination, mrPlan.DstNodeSetId)
		}
	}

	// Case 5: test CrossZone == true. Find meta node under the same zone.
	cluster.t.zones[1].nodeSetMap[20].metaNodes.Delete("node10")

	result, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}
	if len(result.Plan) <= 0 || len(result.Plan[0].Plan) <= 1 {
		t.Errorf("GetMetaNodePressureView returned an unexpected plan")
	}

	for _, mrPlan := range result.Plan[0].Plan {
		if mrPlan.SrcZoneName != mrPlan.DstZoneName || mrPlan.SrcNodeSetId == mrPlan.DstNodeSetId {
			t.Errorf("GetMetaNodePressureView returned an unexpected plan. src(%s) srcNodeSet(%d) dst(%s) dstNodeSet(%d)",
				mrPlan.Source, mrPlan.SrcNodeSetId, mrPlan.Destination, mrPlan.DstNodeSetId)
		}
	}

	// Case 6: test CrossZone == true. Not find low memory usage meta node.
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Delete("node7")
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Delete("node8")

	_, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err == nil {
		t.Errorf("GetMetaNodePressureView returned an unexpected plan")
	}
}

func TestCheckPlanSourceChanged(t *testing.T) {
	// The plan's Original list should match the current replica set. If any
	// planned source disappears from the partition, migration should be treated
	// as stale.
	mp := &MetaPartition{
		Replicas: []*MetaReplica{
			{Addr: "node1"},
			{Addr: "node2"},
			{Addr: "node3"},
		},
	}
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{
			{Source: "node1"},
			{Source: "node2"},
			{Source: "node3"},
		},
	}
	ret := checkPlanSourceChanged(mpPlan, mp)
	require.False(t, ret)

	// Changing one planned source to a node not present in replicas simulates a
	// partition membership change after the plan was created.
	mpPlan.Original[0].Source = "node4"
	ret = checkPlanSourceChanged(mpPlan, mp)
	require.True(t, ret)
}

func TestVerifyDestinationInMetaReplicas(t *testing.T) {
	// verifyDestinationInMetaReplicas is a simple guard against migrating to a
	// destination that is already a partition member.
	mp := &MetaPartition{
		Replicas: []*MetaReplica{
			{Addr: "node1"},
			{Addr: "node2"},
			{Addr: "node3"},
		},
	}
	ret := verifyDestinationInMetaReplicas(mp, "node1")
	require.True(t, ret)

	ret = verifyDestinationInMetaReplicas(mp, "node4")
	require.False(t, ret)
}

func TestHandleMetaPartitionPlan_BasicFlow(t *testing.T) {
	// Setup a cluster with just enough state for handleMetaPartitionPlan to find
	// the target partition and send a successful admin task.
	c := &Cluster{stopc: make(chan bool, 1)}
	// The local listener acts as a fake metanode and returns an OpOk packet for
	// the admin task sent by the migration handler.
	ln, _ := net.Listen("tcp", ":0")
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			p := proto.NewPacket()
			_ = p.ReadFromConnWithVer(conn, proto.SyncSendTaskDeadlineTime)
			p.ResultCode = proto.OpOk
			p.Data = []byte("ok")
			p.Size = uint32(len(p.Data))
			_ = p.WriteToConn(conn)
			conn.Close()
		}
	}()
	addr := ln.Addr().String()
	// Disable connection pooling so the test talks directly to the listener
	// above and does not depend on shared connection state.
	prev := useConnPool
	useConnPool = false
	defer func() { useConnPool = prev }()
	mn := &MetaNode{ID: 1, Addr: addr, Sender: newAdminTaskManager(addr, "test-cluster")}
	c.metaNodes.Store(addr, mn)

	// The partition has a single leader replica on the fake metanode. This keeps
	// the flow focused on handler status transitions rather than leader changes.
	mp := &MetaPartition{
		PartitionID: 100,
		Replicas:    []*MetaReplica{{Addr: addr, IsLeader: true, metaNode: mn}},
	}
	// Add the partition into a volume so getMetaPartitionByID can locate it.
	c.ClusterVolSubItem.vols = map[string]*Vol{
		"v": {Name: "v", MetaPartitions: map[uint64]*MetaPartition{100: mp}, mpsLock: new(mpsLockManager)},
	}
	// The plan contains one source-to-destination pair; both addresses are the
	// fake metanode so the admin task can complete inside this test.
	plan := &proto.ClusterPlan{}
	mpPlan := &proto.MetaBalancePlan{ID: 100, Plan: []*proto.MrBalanceInfo{{Source: addr, Destination: addr}}}
	// handleMetaPartitionPlan requires the cluster plan state to be running.
	c.SetClusterPlanRunning()
	err := c.handleMetaPartitionPlan(plan, mpPlan)
	require.NoError(t, err)
}

// These helper tests cover learner and ready-replica accounting without running
// a migration. The replica metadata is enough to exercise zone, nodeset, tag,
// and store-mode selection branches.
func TestMetaPartitionLearnerAndReadyReplicaHelpers(t *testing.T) {
	// Peers define learner membership, while Replicas carry node metadata used
	// for ready-replica counting by zone, nodeset, tag, and store mode.
	mp := &MetaPartition{
		Peers: []proto.Peer{
			{Addr: "addr-1", Type: raftproto.PeerNormal},
			{Addr: "addr-2", Type: raftproto.PeerLearner},
			{Addr: "addr-3", Type: raftproto.PeerLearner},
		},
		Replicas: []*MetaReplica{
			{
				Addr:      "addr-1",
				StoreMode: proto.StoreModeMem,
				metaNode:  &MetaNode{ZoneName: "zone-1", NodeSetID: 1, Tag: "tag-a"},
			},
			{
				Addr:      "addr-2",
				StoreMode: proto.StoreModeMem,
				metaNode:  &MetaNode{ZoneName: "zone-1", NodeSetID: 2, Tag: "tag-b"},
			},
			{
				Addr:      "addr-3",
				StoreMode: proto.StoreModeRocksDb,
				metaNode:  &MetaNode{ZoneName: "zone-2", NodeSetID: 3, Tag: "tag-a"},
			},
		},
	}

	// Learner helpers read from Peers and should ignore normal voters.
	require.Equal(t, []string{"addr-2", "addr-3"}, GetMetaPartitionLearnerList(mp))
	require.Equal(t, 2, GetMetaPartitionLearnerCount(mp))
	require.True(t, IsMetaReplicaLearner(mp, "addr-2"))
	require.False(t, IsMetaReplicaLearner(mp, "addr-1"))

	// Ready-replica counts are driven by ClusterPlan selection mode. Each
	// assertion below pins one selection branch.
	require.Equal(t, 2, GetMetaPartitionReadyReplicaCount(&proto.ClusterPlan{
		SelectType: SelectTypeZoneName,
		ZoneName:   "zone-1",
	}, mp))
	require.Equal(t, 1, GetMetaPartitionReadyReplicaCount(&proto.ClusterPlan{
		SelectType: SelectTypeNodeSetId,
		NodeSetID:  2,
	}, mp))
	require.Equal(t, 2, GetMetaPartitionReadyReplicaCount(&proto.ClusterPlan{
		SelectType: SelectTypeNodeAddrs,
		Tag:        "tag-a",
	}, mp))
	require.Equal(t, 1, GetMetaPartitionReadyReplicaCount(&proto.ClusterPlan{
		Mode: proto.StoreModeRocksDb,
	}, mp))
}

// SelectOneReplicaToDelete first tries the strict selector and then falls back
// to any non-excluded replica. The fixture keeps one replica outside each
// requested placement rule so the strict path is visible.
func TestSelectOneReplicaToDelete(t *testing.T) {
	// addr-3 is intentionally outside zone-1, nodeset 1, tag-a, and memory mode,
	// making it the strict deletion candidate for all selector variants.
	mp := &MetaPartition{
		Replicas: []*MetaReplica{
			{
				Addr:      "addr-1",
				StoreMode: proto.StoreModeMem,
				metaNode:  &MetaNode{ZoneName: "zone-1", NodeSetID: 1, Tag: "tag-a"},
			},
			{
				Addr:      "addr-2",
				StoreMode: proto.StoreModeMem,
				metaNode:  &MetaNode{ZoneName: "zone-1", NodeSetID: 1, Tag: "tag-a"},
			},
			{
				Addr:      "addr-3",
				StoreMode: proto.StoreModeRocksDb,
				metaNode:  &MetaNode{ZoneName: "zone-2", NodeSetID: 2, Tag: "tag-b"},
			},
		},
	}

	// Strict selection should prefer the replica that violates the requested
	// placement rule.
	require.Equal(t, "addr-3", SelectOneReplicaStrickly(mp, nil, &MetaPartitionPlanUserParams{
		SelectType: SelectTypeZoneName,
		ZoneName:   "zone-1",
	}))
	require.Equal(t, "addr-3", SelectOneReplicaStrickly(mp, nil, &MetaPartitionPlanUserParams{
		SelectType: SelectTypeNodeSetId,
		NodeSetID:  1,
	}))
	require.Equal(t, "addr-3", SelectOneReplicaStrickly(mp, nil, &MetaPartitionPlanUserParams{
		SelectType: SelectTypeNodeAddrs,
		Tag:        "tag-a",
	}))
	require.Equal(t, "addr-3", SelectOneReplicaStrickly(mp, nil, &MetaPartitionPlanUserParams{
		Mode: proto.StoreModeMem,
	}))

	// Once the strict candidate is excluded, SelectOneReplicaToDelete falls back
	// to the first non-excluded replica.
	require.Empty(t, SelectOneReplicaStrickly(mp, []string{"addr-3"}, &MetaPartitionPlanUserParams{
		SelectType: SelectTypeZoneName,
		ZoneName:   "zone-1",
	}))
	require.Equal(t, "addr-1", SelectOneReplicaToDelete(mp, []string{"addr-3"}, &MetaPartitionPlanUserParams{
		SelectType: SelectTypeZoneName,
		ZoneName:   "zone-1",
	}))
	// If every replica is excluded, no deletion source can be selected.
	require.Empty(t, SelectOneReplicaToDelete(mp, []string{"addr-1", "addr-2", "addr-3"}, &MetaPartitionPlanUserParams{}))
}

// FillLearnerPlanDestination mutates destination records when auto-promote is
// enabled. These assertions pin both the selected source address and the copied
// source metadata used later by the migration executor.
func TestFillLearnerPlanDestination(t *testing.T) {
	cluster := &Cluster{}
	// Destination records are produced earlier by target selection. Auto-promote
	// fills in the source side of these records.
	dest := []*proto.MrBalanceInfo{
		{Destination: "dst-1", DstId: 11},
		{Destination: "dst-2", DstId: 12},
	}
	// Original records carry source metadata that must be copied onto the chosen
	// destination records for later execution.
	mpPlan := &proto.MetaBalancePlan{
		ID: 100,
		Original: []*proto.MrBalanceInfo{
			{Source: "addr-1", SrcMemSize: 1, SrcNodeSetId: 1, SrcZoneName: "zone-1", SrcRack: "rack-1"},
			{Source: "addr-2", SrcMemSize: 2, SrcNodeSetId: 1, SrcZoneName: "zone-1", SrcRack: "rack-2"},
			{Source: "addr-3", SrcMemSize: 3, SrcNodeSetId: 2, SrcZoneName: "zone-2", SrcRack: "rack-3"},
		},
	}
	// addr-3 is outside the requested zone and should be chosen first by the
	// strict deletion selector.
	mp := &MetaPartition{
		PartitionID: 100,
		Replicas: []*MetaReplica{
			{Addr: "addr-1", metaNode: &MetaNode{ZoneName: "zone-1", NodeSetID: 1, Tag: "tag-a"}},
			{Addr: "addr-2", metaNode: &MetaNode{ZoneName: "zone-1", NodeSetID: 1, Tag: "tag-a"}},
			{Addr: "addr-3", metaNode: &MetaNode{ZoneName: "zone-2", NodeSetID: 2, Tag: "tag-b"}},
		},
	}
	param := &MetaPartitionPlanUserParams{
		SelectType: SelectTypeZoneName,
		ZoneName:   "zone-1",
	}

	// Auto-promote mutates the destination records by selecting source replicas
	// and copying their source metadata.
	err := cluster.FillLearnerPlanDestination(&proto.ClusterPlan{AutoPromote: true}, mpPlan, dest, param, mp)
	require.NoError(t, err)
	require.Len(t, mpPlan.Plan, 2)
	require.Equal(t, "addr-3", mpPlan.Plan[0].Source)
	require.Equal(t, uint64(3), mpPlan.Plan[0].SrcMemSize)
	require.Equal(t, uint64(2), mpPlan.Plan[0].SrcNodeSetId)
	require.Equal(t, "zone-2", mpPlan.Plan[0].SrcZoneName)
	require.Equal(t, "rack-3", mpPlan.Plan[0].SrcRack)
	require.Equal(t, "addr-1", mpPlan.Plan[1].Source)
	require.Equal(t, uint64(1), mpPlan.Plan[1].SrcMemSize)

	// Without AutoPromote, the function should preserve the destination records
	// exactly as supplied.
	manualPlan := &proto.MetaBalancePlan{ID: 101}
	manualDest := []*proto.MrBalanceInfo{{Destination: "manual-dst"}}
	err = cluster.FillLearnerPlanDestination(&proto.ClusterPlan{}, manualPlan, manualDest, param, mp)
	require.NoError(t, err)
	require.Same(t, manualDest[0], manualPlan.Plan[0])

	// Empty destination lists are invalid because there is nothing to fill.
	err = cluster.FillLearnerPlanDestination(&proto.ClusterPlan{AutoPromote: true}, &proto.MetaBalancePlan{ID: 102}, nil, param, mp)
	require.Error(t, err)
}

func TestCopyMd5SumToChecksumInfo(t *testing.T) {
	cluster := &Cluster{}
	// Two fresh checksum responses share the same apply ID and md5. A third stale
	// response proves LastApplyID filtering is honored.
	mp := &MetaPartition{
		PartitionID: 100,
		LoadResponse: []*proto.MetaPartitionLoadResponse{
			{Addr: "addr-1", Md5ApplyId: 10, Md5Sum: "same-md5"},
			{Addr: "addr-2", Md5ApplyId: 10, Md5Sum: "same-md5"},
			{Addr: "addr-3", Md5ApplyId: 8, Md5Sum: "stale-md5"},
		},
	}
	// Only addr-1 and addr-2 are expected by the checksum plan.
	checksumInfo := &proto.MetaPartitionChecksumInfo{
		LastApplyID: 9,
		Replicas: []*proto.MetaReplicaChecksumInfo{
			{Addr: "addr-1"},
			{Addr: "addr-2"},
		},
	}

	// Successful copy should fill each requested replica with the fresh checksum
	// result.
	require.NoError(t, cluster.CopyMd5SumToChecksumInfo(mp, checksumInfo))
	require.Equal(t, uint64(10), checksumInfo.Replicas[0].ApplyID)
	require.Equal(t, "same-md5", checksumInfo.Replicas[0].Md5Sum)
	require.Equal(t, uint64(10), checksumInfo.Replicas[1].ApplyID)
	require.Equal(t, "same-md5", checksumInfo.Replicas[1].Md5Sum)

	// Missing expected replicas should fail because the checksum plan requires a
	// complete result set.
	require.Error(t, cluster.CopyMd5SumToChecksumInfo(mp, &proto.MetaPartitionChecksumInfo{}))
	require.Error(t, cluster.CopyMd5SumToChecksumInfo(mp, &proto.MetaPartitionChecksumInfo{
		LastApplyID: 9,
		Replicas:    []*proto.MetaReplicaChecksumInfo{{Addr: "addr-1"}, {Addr: "missing"}},
	}))

	// All replicas must report the same apply ID.
	mismatchApplyID := &MetaPartition{
		PartitionID: 101,
		LoadResponse: []*proto.MetaPartitionLoadResponse{
			{Addr: "addr-1", Md5ApplyId: 10, Md5Sum: "same-md5"},
			{Addr: "addr-2", Md5ApplyId: 11, Md5Sum: "same-md5"},
		},
	}
	require.Error(t, cluster.CopyMd5SumToChecksumInfo(mismatchApplyID, &proto.MetaPartitionChecksumInfo{
		Replicas: []*proto.MetaReplicaChecksumInfo{{Addr: "addr-1"}, {Addr: "addr-2"}},
	}))

	// All replicas must also report the same md5 sum.
	mismatchMd5 := &MetaPartition{
		PartitionID: 102,
		LoadResponse: []*proto.MetaPartitionLoadResponse{
			{Addr: "addr-1", Md5ApplyId: 10, Md5Sum: "md5-a"},
			{Addr: "addr-2", Md5ApplyId: 10, Md5Sum: "md5-b"},
		},
	}
	require.Error(t, cluster.CopyMd5SumToChecksumInfo(mismatchMd5, &proto.MetaPartitionChecksumInfo{
		Replicas: []*proto.MetaReplicaChecksumInfo{{Addr: "addr-1"}, {Addr: "addr-2"}},
	}))
}

func TestCreateTaskToCalculateCheckSum(t *testing.T) {
	// The checksum task should encode the partition ID both in the request body
	// and in the task metadata used by task tracking.
	mp := &MetaPartition{
		PartitionID: 12345,
		volName:     "vol-test",
	}

	// Building the task is pure; no metanode or network fixture is required.
	task, err := mp.createTaskToCalculateCheckSum("leader-addr")
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, proto.OpCalcMetaPartitionMd5Sum, task.OpCode)
	require.Equal(t, "leader-addr", task.OperatorAddr)
	require.Equal(t, mp.PartitionID, task.PartitionID)
	require.Contains(t, task.ID, "pid[12345]")

	// The request payload is what the metanode receives to start checksum
	// calculation.
	req, ok := task.Request.(*proto.CalcMetaPartitionMd5SumRequest)
	require.True(t, ok)
	require.Equal(t, mp.PartitionID, req.PartitionID)
}

func TestIsRocksdbDirInMetaPartition(t *testing.T) {
	// LoadResponse carries the last reported rocksdb directory per replica.
	mp := &MetaPartition{
		LoadResponse: []*proto.MetaPartitionLoadResponse{
			{Addr: "addr-1", RocksdbDir: "/disk-a/rocksdb"},
			{Addr: "addr-2", RocksdbDir: "/disk-b/rocksdb"},
		},
	}

	// A match requires both the replica address and rocksdb directory to match.
	require.True(t, IsRocksdbDirInMetaPartition(mp, "addr-1", "/disk-a/rocksdb"))
	require.False(t, IsRocksdbDirInMetaPartition(mp, "addr-1", "/disk-b/rocksdb"))
	require.False(t, IsRocksdbDirInMetaPartition(mp, "addr-missing", "/disk-a/rocksdb"))
}
