package master

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestGetMigrateDestAddr(t *testing.T) {
	// 构造测试参数
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

	// 调用被测试函数
	find, address := GetMigrateDestAddr(param)

	// 验证结果
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

func TestGetMigrateAddrExcludeNodeSet(t *testing.T) {
	// 构造测试参数
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

	// 调用被测试函数
	find, address := GetMigrateAddrExcludeNodeSet(param)

	// 验证结果
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

	// 验证地址是否来自不同的 NodeSet
	for _, addr := range address {
		if addr.DstNodeSetId == param.NodeSetID {
			t.Errorf("Address from excluded NodeSet found in results: %s", addr.Destination)
		}
	}
}

func TestGetMigrateAddrExcludeZone(t *testing.T) {
	// 构造测试参数
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

	// 调用被测试函数
	find, address := GetMigrateAddrExcludeZone(param)

	// 验证结果
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
	// 构造测试参数
	mpPlan := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Source: "192.168.1.1", SrcMemSize: 2048, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
			{Source: "192.168.1.2", SrcMemSize: 3072, SrcNodeSetId: 1, SrcZoneName: "testZone1"},
			{Source: "192.168.1.3", SrcMemSize: 1024, SrcNodeSetId: 2, SrcZoneName: "testZone2"},
		},
	}

	// 测试用例1: 存在的源地址
	srcAddr := "192.168.1.2"
	index, bExist := SrcIsPlaned(mpPlan, srcAddr)
	if !bExist {
		t.Errorf("Expected source %s to exist, but it does not", srcAddr)
	}
	if index != 1 {
		t.Errorf("Expected index 1 for source %s, but got %d", srcAddr, index)
	}

	// 测试用例2: 不存在的源地址
	srcAddr = "192.168.1.4"
	index, bExist = SrcIsPlaned(mpPlan, srcAddr)
	if bExist {
		t.Errorf("Expected source %s to not exist, but it does", srcAddr)
	}
	if index != -1 {
		t.Errorf("Expected index -1 for non-existent source %s, but got %d", srcAddr, index)
	}

	// 测试用例3: 空的 Plan 列表
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
	// 构造测试参数
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

	// 调用被测试函数
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// 验证结果
	if err != nil {
		t.Errorf("Expected no error, but got: %s", err.Error())
	}

	// 验证 metaNode 的更新
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

	// 验证 metaNode 是否被删除
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
	// 构造测试参数
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

	// 调用被测试函数
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// 验证结果
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}

	expectedErr := fmt.Sprintf("Error to get destination zone: %s", newPlan.DstZoneName)
	if err.Error() != expectedErr {
		t.Errorf("Expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestUpdateLowPressureNodeTopo_NodeSetNotFound(t *testing.T) {
	// 构造测试参数
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

	// 调用被测试函数
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// 验证结果
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}

	expectedErr := fmt.Sprintf("Error to get node set %d", newPlan.DstNodeSetId)
	if err.Error() != expectedErr {
		t.Errorf("Expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestUpdateLowPressureNodeTopo_MetaNodeNotFound(t *testing.T) {
	// 构造测试参数
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

	// 调用被测试函数
	err := UpdateLowPressureNodeTopo(migratePlan, newPlan)
	// 验证结果
	if err == nil {
		t.Errorf("Expected error, but got nil")
	}

	expectedErr := fmt.Sprintf("Error to get meta node %d", newPlan.DstId)
	if err.Error() != expectedErr {
		t.Errorf("Expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestFillExcludeAddrIntoGetParam(t *testing.T) {
	// 构造测试参数
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

	// 调用被测试函数
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// 验证结果
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
	// 构造测试参数
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

	// 调用被测试函数
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// 验证结果
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
	// 构造测试参数
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

	// 调用被测试函数
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// 验证结果
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
	// 构造测试参数
	mpPlan := &proto.MetaBalancePlan{
		Original: []*proto.MrBalanceInfo{},
		Plan:     []*proto.MrBalanceInfo{},
	}

	getParam := &GetMigrateAddrParam{
		Excludes: []string{},
	}

	// 调用被测试函数
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// 验证结果
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
	// 创建测试数据
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

	// 调用被测试函数
	err := MigratePlanOverLoadToDest(migratePlan, mpPlan, dests)
	// 验证结果
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestMigratePlanOriginalToDest(t *testing.T) {
	// 创建测试数据
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

	// 调用被测试函数
	err := MigratePlanOriginalToDest(migratePlan, mpPlan, dests)
	// 验证结果
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestFillMigratePlanArray(t *testing.T) {
	// 创建测试数据
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

	// 调用被测试函数
	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests)
	// 验证结果
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	// 验证 mpPlan.Plan 的内容
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
	err := FindMigrateDestination(migratePlan)
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
	err = FindMigrateDestination(migratePlan)
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

func TestCalculateMetaNodeEstimate(t *testing.T) {
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
			expectedEstimate: []int{7},
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
				if metaNode.Estimate != tt.expectedEstimate[i] {
					t.Errorf("CalculateMetaNodeEstimate() Estimate = %v, want %v", metaNode.Estimate, tt.expectedEstimate[i])
				}
			}
		})
	}
}

func TestGetLowMemPressureTopology(t *testing.T) {
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	// 创建一个Cluster实例
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
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.1, IsActive: true, MaxMemAvailWeight: size10GB})
	cluster.t.zones[0].nodeSetMap[1].metaNodes.Store("node2", &MetaNode{ID: 102, Ratio: 0.2, IsActive: true, MaxMemAvailWeight: size10GB})
	cluster.t.zoneMap.Store(cluster.t.zones[0].name, cluster.t.zones[0])

	migratePlan := &proto.ClusterPlan{
		Low: make(map[string]*proto.ZonePressureView),
	}

	// 调用被测试的方法
	err := cluster.GetLowMemPressureTopology(migratePlan)
	if err != nil {
		t.Errorf("Expect no error. but get: %s", err.Error())
	}

	// 验证结果
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
	// 测试用例1: 所有状态都是PlanTaskInit
	mpPlan1 := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Status: PlanTaskInit},
			{Status: PlanTaskInit},
		},
	}
	if VerifyMetaReplicaPlanNotAllInit(mpPlan1) {
		t.Errorf("Expected false, got true")
	}

	// 测试用例2: 存在一个状态不是PlanTaskInit
	mpPlan2 := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{
			{Status: PlanTaskInit},
			{Status: PlanTaskRun}, // 假设PlanTaskRunning是一个存在的状态
		},
	}
	if !VerifyMetaReplicaPlanNotAllInit(mpPlan2) {
		t.Errorf("Expected true, got false")
	}

	// 测试用例3: 空的Plan切片
	mpPlan3 := &proto.MetaBalancePlan{
		Plan: []*proto.MrBalanceInfo{},
	}
	if VerifyMetaReplicaPlanNotAllInit(mpPlan3) {
		t.Errorf("Expected false, got true")
	}
}

func TestVerifyMetaNodeExceedMemMid(t *testing.T) {
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	// 测试用例1: Ratio 大于等于 metaNodeMemMidPer
	cluster := &Cluster{}
	cluster.metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.8, IsActive: true, MaxMemAvailWeight: size10GB})
	cluster.metaNodes.Store("node2", &MetaNode{ID: 102, Ratio: 0.5, IsActive: true, MaxMemAvailWeight: size10GB})

	result1, err1 := cluster.VerifyMetaNodeExceedMemMid("node1")
	if err1 != nil || !result1 {
		t.Errorf("Expected true, got %v, err: %v", result1, err1)
	}

	// 测试用例2: Ratio 小于 metaNodeMemMidPer
	result2, err2 := cluster.VerifyMetaNodeExceedMemMid("node2")
	if err2 != nil || result2 {
		t.Errorf("Expected false, got %v, err: %v", result2, err2)
	}

	// 测试用例3: 获取metaNode失败
	result3, err3 := cluster.VerifyMetaNodeExceedMemMid("node3")
	if err3 == nil || result3 {
		t.Errorf("Expected error, got %v, result: %v", err3, result3)
	}
}

func TestUpdateMigrateDestination(t *testing.T) {
	size10GB := uint64(10 * 1024 * 1024 * 1024)
	// 测试用例1: 所有方法成功
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
	migratePlan := &proto.ClusterPlan{}
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

	// 测试用例3: FindMigrateDestRetainZone 失败
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
	// Mock data
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
									Addr: "node1",
								},
							},
						},
						1001: {
							PartitionID: 1001,
							InodeCount:  20,
							Hosts:       []string{"node1"},
							Replicas: []*MetaReplica{
								{
									Addr: "node1",
								},
							},
						},
					},
					mpsLock: new(mpsLockManager),
				},
			},
		},
	}
	cluster.metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.8})
	metaNode := &proto.MetaNodeBalanceInfo{
		Addr:     "node1",
		Estimate: 1,
	}
	migratePlan := &proto.ClusterPlan{}
	overLoads := []*proto.MetaNodeBalanceInfo{
		{
			Addr:       "node1",
			Total:      metaNodeReserveMemorySize * 2,
			Ratio:      0.8,
			InodeCount: 30000,
		},
	}

	err := cluster.AddMetaPartitionIntoPlan(metaNode, migratePlan, overLoads)
	if err != nil {
		t.Errorf("AddMetaPartitionIntoPlan failed: %v", err)
	}
}

func TestCreateMetaPartitionMigratePlan(t *testing.T) {
	// Create a cluster instance
	cluster := &Cluster{}
	cluster.metaNodes.Store("node1", &MetaNode{ID: 101, Ratio: 0.8, NodeMemTotal: 1000000, NodeMemUsed: 900000, MetaPartitionCount: 1000})

	// Create a mock migrate plan
	migratePlan := &proto.ClusterPlan{}

	// Test case 1: Normal case
	err := cluster.CreateMetaPartitionMigratePlan(migratePlan)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func PrintMigratePlan(plan *proto.ClusterPlan) {
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
									Addr: "node4",
								},
								{
									Addr: "node5",
								},
								{
									Addr: "node6",
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

	cluster.t.zones[1].nodeSetMap[20].metaNodes.Store("node10", &MetaNode{
		ID: 110, Addr: "node10", Ratio: 0.1,
		Total: totalSize, NodeMemTotal: totalSize,
		NodeMemUsed: 8192, ZoneName: "zone2", NodeSetID: 20,
		IsActive: true, MaxMemAvailWeight: size10GB,
	})

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
	// Check for errors
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
	result, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}
	if len(result.Plan) <= 0 || len(result.Plan[0].Plan) <= 2 {
		t.Errorf("GetMetaNodePressureView returned an unexpected plan")
	}
	for _, mrPlan := range result.Plan[0].Plan {
		if mrPlan.SrcZoneName != mrPlan.DstZoneName || mrPlan.SrcNodeSetId == mrPlan.DstNodeSetId {
			t.Errorf("GetMetaNodePressureView returned an unexpected plan")
		}
	}

	// Case 3: find meta node in different zone.
	cluster.t.zones[1].nodeSetMap[30].metaNodes.Delete("node7")
	result, err = cluster.GetMetaNodePressureView()
	// Check for errors
	if err != nil {
		t.Errorf("GetMetaNodePressureView returned an error: %v", err)
	}
	if len(result.Plan) <= 0 || len(result.Plan[0].Plan) <= 2 {
		t.Errorf("GetMetaNodePressureView returned an unexpected plan")
	}

	for _, mrPlan := range result.Plan[0].Plan {
		if mrPlan.SrcZoneName == mrPlan.DstZoneName || mrPlan.SrcNodeSetId == mrPlan.DstNodeSetId {
			t.Errorf("GetMetaNodePressureView returned an unexpected plan. src(%s) srcNodeSet(%d) dst(%s) dstNodeSet(%d)",
				mrPlan.Source, mrPlan.SrcNodeSetId, mrPlan.Destination, mrPlan.DstNodeSetId)
		}
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

	mpPlan.Original[0].Source = "node4"
	ret = checkPlanSourceChanged(mpPlan, mp)
	require.True(t, ret)
}

func TestVerifyDestinationInMetaReplicas(t *testing.T) {
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
