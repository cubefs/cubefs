// Copyright 2023 The CubeFS Authors.
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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/master/mocktest"
	"github.com/cubefs/cubefs/util"
)

const loopNodeSelectorTestCount = 100

func writeDataNode(sb *strings.Builder, node *DataNode) {
	sb.WriteString(fmt.Sprintf("Data Node %v\n", node.ID))
	sb.WriteString(fmt.Sprintf("\tTotal Space:%v MB\n", node.Total/util.MB))
	sb.WriteString(fmt.Sprintf("\tAvaliable Space:%v MB\n", node.AvailableSpace/util.MB))
}

func writeMetaNode(sb *strings.Builder, node *MetaNode) {
	sb.WriteString(fmt.Sprintf("Meta Node %v\n", node.ID))
	sb.WriteString(fmt.Sprintf("\tTotal Space:%v MB\n", node.Total/util.MB))
	sb.WriteString(fmt.Sprintf("\tAvaliable Space:%v MB\n", (node.Total-node.Used)/util.MB))
}

func printDataNode(t *testing.T, node *DataNode) {
	sb := strings.Builder{}
	writeDataNode(&sb, node)
	mocktest.Log(t, sb.String())
}

func printMetaNode(t *testing.T, node *MetaNode) {
	sb := strings.Builder{}
	writeMetaNode(&sb, node)
	mocktest.Log(t, sb.String())
}

func printNodesetAndDataNodes(t *testing.T, nset *nodeSet) {
	printNodeset(t, nset)
	nset.dataNodes.Range(func(key, value interface{}) bool {
		printDataNode(t, value.(*DataNode))
		return true
	})
}

func printNodesetAndMetaNodes(t *testing.T, nset *nodeSet) {
	printNodeset(t, nset)
	nset.metaNodes.Range(func(key, value interface{}) bool {
		printMetaNode(t, value.(*MetaNode))
		return true
	})
}

func getAllDataNodesForTest(t *testing.T, selectZone string) (nodes []*DataNode) {
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Errorf("nodeset count could not be 0")
		return
	}
	nset := nsc[0]
	if nset.dataNodeLen() == 0 {
		t.Errorf("datanode count could not be 0")
		return
	}
	nodes = make([]*DataNode, 0, nset.dataNodeLen())
	nset.dataNodes.Range(func(key, value interface{}) bool {
		node := value.(*DataNode)
		nodes = append(nodes, node)
		return true
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return
}

func getFirstDataNodeForTest(t *testing.T, selectZone string) (node *DataNode) {
	nodes := getAllDataNodesForTest(t, selectZone)
	if nodes == nil {
		return
	}
	node = nodes[0]
	return
}

func getAllMetaNodesForTest(t *testing.T, selectZone string) (nodes []*MetaNode) {
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return
	}
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Errorf("nodeset count could not be 0")
		return
	}
	nset := nsc[0]
	if nset.metaNodeLen() == 0 {
		t.Errorf("metanode count could not be 0")
		return
	}
	nodes = make([]*MetaNode, 0, nset.metaNodeLen())
	nset.metaNodes.Range(func(key, value interface{}) bool {
		node := value.(*MetaNode)
		nodes = append(nodes, node)
		return true
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return
}

func getFirstMetaNodeForTest(t *testing.T, selectZone string) (node *MetaNode) {
	nodes := getAllMetaNodesForTest(t, selectZone)
	if nodes == nil {
		return
	}
	node = nodes[0]
	return
}

func DataNodeSelectorTest(t *testing.T, selector NodeSelector, expectedNode *DataNode) *DataNode {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return nil
	}
	mocktest.Log(t, "List nodesets of zone")
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Errorf("nodeset count could not be 0")
		return nil
	}
	nset := nsc[0]
	mocktest.Log(t, "List datanodes of nodeset", nset.ID)
	printNodesetAndDataNodes(t, nset)
	_, peer, err := selector.Select(nset, nil, 1)
	if err != nil {
		t.Errorf("%v failed to select nodes %v", selector.GetName(), err)
		return nil
	}
	mocktest.Log(t, "List selected nodes:")
	for i := 0; i < len(peer); i++ {
		nodeVal, ok := nset.dataNodes.Load(peer[i].Addr)
		if !ok {
			t.Errorf("%v select wrong node", selector.GetName())
			return nil
		}
		node := nodeVal.(*DataNode)
		printDataNode(t, node)
	}
	nodeVal, ok := nset.dataNodes.Load(peer[0].Addr)
	if !ok {
		t.Errorf("%v failed to select nodes", selector.GetName())
		return nil
	}
	node := nodeVal.(*DataNode)
	if expectedNode != nil && node.ID != expectedNode.ID {
		t.Errorf("%v select wrong node, expected: %v actually: %v", selector.GetName(), expectedNode.ID, node.ID)
		return nil
	}
	return node
}

func MetaNodeSelectorTest(t *testing.T, selector NodeSelector, expectedNode *MetaNode) *MetaNode {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return nil
	}
	mocktest.Log(t, "List nodesets of zone")
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	nset := nsc[0]
	mocktest.Log(t, "List metanodes of nodeset", nset.ID)
	printNodesetAndMetaNodes(t, nset)
	_, peer, err := selector.Select(nset, nil, 1)
	if err != nil {
		t.Errorf("%v failed to select nodes %v", selector.GetName(), err)
		return nil
	}
	mocktest.Log(t, "List selected nodes:")
	for i := 0; i < len(peer); i++ {
		nodeVal, ok := nset.metaNodes.Load(peer[i].Addr)
		if !ok {
			t.Errorf("%v select wrong node", selector.GetName())
			return nil
		}
		node := nodeVal.(*MetaNode)
		printMetaNode(t, node)
	}
	nodeVal, ok := nset.metaNodes.Load(peer[0].Addr)
	if !ok {
		t.Errorf("%v failed to select nodes", selector.GetName())
		return nil
	}
	node := nodeVal.(*MetaNode)
	if expectedNode != nil && node.ID != expectedNode.ID {
		t.Errorf("%v select wrong node, expected: %v actually: %v", selector.GetName(), expectedNode.ID, node.ID)
		return nil
	}
	return node
}

func printNodeSelectTimes(t *testing.T, times map[uint64]int) {
	sb := strings.Builder{}
	for id, time := range times {
		sb.WriteString(fmt.Sprintf("Node %v select times %v\n", id, time))
	}
	mocktest.Log(t, sb.String())
}

func TestCarryWeightNodeSelector(t *testing.T) {
	// get first node
	dataNode := getFirstDataNodeForTest(t, testZone2)
	metaNode := getFirstMetaNodeForTest(t, testZone2)
	dataSelectTimes := make(map[uint64]int)
	metaSelectTimes := make(map[uint64]int)
	// prepare for datanode
	tmp := dataNode.AvailableSpace
	dataNode.Total += dataNode.AvailableSpace
	dataNode.AvailableSpace *= 2
	// select test
	selector := NewCarryWeightNodeSelector(DataNodeType)
	for i := 0; i != loopNodeSelectorTestCount; i++ {
		expected := dataNode
		if i != 0 {
			expected = nil
		}
		node := DataNodeSelectorTest(t, selector, expected)
		if node == nil {
			return
		}
		count, _ := dataSelectTimes[node.ID]
		count += 1
		dataSelectTimes[node.ID] = count
	}
	t.Logf("%v data node select times:\n", selector.GetName())
	printNodeSelectTimes(t, dataSelectTimes)
	count, _ := dataSelectTimes[dataNode.ID]
	for _, c := range dataSelectTimes {
		if count < c {
			t.Errorf("%v failed to select data nodes", selector.GetName())
			return
		}
	}
	// restore status
	dataNode.Total -= tmp
	dataNode.AvailableSpace = tmp

	// prepare for metanode
	tmp = metaNode.Total
	metaNode.Total *= 2
	// select test
	selector = NewCarryWeightNodeSelector(MetaNodeType)
	for i := 0; i != loopNodeSelectorTestCount; i++ {
		expected := metaNode
		if i != 0 {
			expected = nil
		}
		node := MetaNodeSelectorTest(t, selector, expected)
		if node == nil {
			return
		}
		count, _ := metaSelectTimes[node.ID]
		count += 1
		metaSelectTimes[node.ID] = count
	}
	t.Logf("%v meta node select times:\n", selector.GetName())
	printNodeSelectTimes(t, metaSelectTimes)
	count, _ = metaSelectTimes[metaNode.ID]
	for _, c := range metaSelectTimes {
		if count < c {
			t.Errorf("%v failed to select meta nodes", selector.GetName())
			return
		}
	}
	// restore status
	metaNode.Total = tmp
}

func TestRoundRobinNodeSelector(t *testing.T) {
	dataNodes := getAllDataNodesForTest(t, testZone2)
	if dataNodes == nil {
		return
	}
	metaNodes := getAllMetaNodesForTest(t, testZone2)
	if metaNodes == nil {
		return
	}
	selector := NewRoundRobinNodeSelector(DataNodeType)
	for i, node := range dataNodes {
		mocktest.Log(t, "Select DataNode Round", i)
		if DataNodeSelectorTest(t, selector, node) == nil {
			return
		}
	}
	selector = NewRoundRobinNodeSelector(MetaNodeType)
	for i, node := range metaNodes {
		mocktest.Log(t, "Select MetaNode Round", i)
		if MetaNodeSelectorTest(t, selector, node) == nil {
			return
		}
	}
}

func TestAvailableSpaceFirstNodeSelector(t *testing.T) {
	// get first node
	dataNode := getFirstDataNodeForTest(t, testZone2)
	metaNode := getFirstMetaNodeForTest(t, testZone2)

	// prepare for datanode
	tmp := dataNode.AvailableSpace
	dataNode.Total += dataNode.AvailableSpace
	dataNode.AvailableSpace *= 2
	// select test
	selector := NewAvailableSpaceFirstNodeSelector(DataNodeType)
	if DataNodeSelectorTest(t, selector, dataNode) == nil {
		return
	}
	// restore status
	dataNode.Total -= tmp
	dataNode.AvailableSpace = tmp

	// prepare for metanode
	tmp = metaNode.Total
	metaNode.Total *= 2
	// select test
	selector = NewAvailableSpaceFirstNodeSelector(MetaNodeType)
	if MetaNodeSelectorTest(t, selector, metaNode) == nil {
		return
	}
	// restore status
	metaNode.Total = tmp
}

func TestStrawNodeSelector(t *testing.T) {
	// get first node
	dataNode := getFirstDataNodeForTest(t, testZone2)
	metaNode := getFirstMetaNodeForTest(t, testZone2)
	dataSelectTimes := make(map[uint64]int)
	metaSelectTimes := make(map[uint64]int)
	// prepare for datanode
	tmp := dataNode.AvailableSpace
	dataNode.Total += dataNode.AvailableSpace
	dataNode.AvailableSpace *= 2
	// select test
	selector := NewStrawNodeSelector(DataNodeType)
	for i := 0; i != loopNodeSelectorTestCount; i++ {
		node := DataNodeSelectorTest(t, selector, nil)
		if node == nil {
			return
		}
		count, _ := dataSelectTimes[node.ID]
		count += 1
		dataSelectTimes[node.ID] = count
	}
	t.Logf("%v data node select times:\n", selector.GetName())
	printNodeSelectTimes(t, dataSelectTimes)
	count, _ := dataSelectTimes[dataNode.ID]
	for _, c := range dataSelectTimes {
		if count < c {
			t.Errorf("%v failed to select data nodes", selector.GetName())
			return
		}
	}
	// restore status
	dataNode.Total -= tmp
	dataNode.AvailableSpace = tmp

	// prepare for metanode
	tmp = metaNode.Total
	metaNode.Total *= 2
	// select test
	selector = NewStrawNodeSelector(MetaNodeType)
	for i := 0; i != loopNodeSelectorTestCount; i++ {
		node := MetaNodeSelectorTest(t, selector, nil)
		if node == nil {
			return
		}
		count, _ := metaSelectTimes[node.ID]
		count += 1
		metaSelectTimes[node.ID] = count
	}
	t.Logf("%v meta node select times:\n", selector.GetName())
	printNodeSelectTimes(t, metaSelectTimes)
	count, _ = metaSelectTimes[metaNode.ID]
	for _, c := range metaSelectTimes {
		if count < c {
			t.Errorf("%v failed to select meta nodes", selector.GetName())
			return
		}
	}
	// restore status
	metaNode.Total = tmp
}

func prepareDataNodesForBench(count int, initTotal uint64, grow uint64) (ns *nodeSet) {
	ns = &nodeSet{
		ID:               1,
		Capacity:         4,
		zoneName:         testZone1,
		metaNodes:        new(sync.Map),
		dataNodes:        new(sync.Map),
		dataNodeSelector: NewNodeSelector(DefaultNodeSelectorName, DataNodeType),
		metaNodeSelector: NewNodeSelector(DefaultNodeSelectorName, MetaNodeType),
	}
	for i := 0; i < count; i++ {
		space := initTotal + uint64(i)*grow
		node := &DataNode{
			ID:             uint64(i),
			Addr:           fmt.Sprintf("Datanode: %v", i),
			Total:          space,
			AvailableSpace: space,
			isActive:       true,
		}
		ns.putDataNode(node)
	}
	return
}

func prepareMetaNodesForBench(count int, initTotal uint64, grow uint64) (ns *nodeSet) {
	ns = &nodeSet{
		ID:               1,
		Capacity:         4,
		zoneName:         testZone1,
		metaNodes:        new(sync.Map),
		dataNodes:        new(sync.Map),
		dataNodeSelector: NewNodeSelector(DefaultNodeSelectorName, DataNodeType),
		metaNodeSelector: NewNodeSelector(DefaultNodeSelectorName, MetaNodeType),
	}
	for i := 0; i < count; i++ {
		space := initTotal + uint64(i)*grow
		node := &MetaNode{
			ID:                uint64(i),
			Addr:              fmt.Sprintf("Metanode: %v", i),
			Used:              0,
			Total:             space,
			IsActive:          true,
			MaxMemAvailWeight: math.MaxUint64,
		}
		ns.putMetaNode(node)
	}
	return
}

func nodeSelectorBench(selector NodeSelector, nset *nodeSet, onSelect func(addr string)) (map[uint64]int, error) {
	times := make(map[uint64]int)
	for i := 0; i < loopNodeSelectorTestCount; i++ {
		_, peers, err := selector.Select(nset, nil, 1)
		if err != nil {
			return nil, err
		}
		for _, peer := range peers {
			count, _ := times[peer.ID]
			count += 1
			times[peer.ID] = count
			if onSelect != nil {
				onSelect(peer.Addr)
			}
		}
	}
	return times, nil
}

func dataNodeSelectorBench(t *testing.T, selector NodeSelector) error {
	nset := prepareDataNodesForBench(4, 100*util.GB, 100*util.GB)
	random := rand.New(rand.NewSource(time.Now().Unix()))
	times, err := nodeSelectorBench(selector, nset, func(addr string) {
		val, _ := nset.dataNodes.Load(addr)
		node := val.(*DataNode)
		node.AvailableSpace -= uint64(random.Float64() * util.GB * 10)
	})
	if err != nil {
		t.Errorf("%v failed to Bench %v", selector.GetName(), err)
		return err
	}
	printNodeSelectTimes(t, times)
	nset.dataNodes.Range(func(key, value interface{}) bool {
		node := value.(*DataNode)
		printDataNode(t, node)
		return true
	})
	return nil
}

func metaNodeSelectorBench(t *testing.T, selector NodeSelector) error {
	nset := prepareMetaNodesForBench(4, 100*util.GB, 100*util.GB)
	random := rand.New(rand.NewSource(time.Now().Unix()))
	times, err := nodeSelectorBench(selector, nset, func(addr string) {
		val, _ := nset.metaNodes.Load(addr)
		node := val.(*MetaNode)
		node.Used += uint64(random.Float64() * util.GB * 10)
	})
	if err != nil {
		t.Errorf("%v failed to Bench %v", selector.GetName(), err)
		return err
	}
	printNodeSelectTimes(t, times)
	nset.metaNodes.Range(func(key, value interface{}) bool {
		node := value.(*MetaNode)
		printMetaNode(t, node)
		return true
	})
	return nil
}

func TestBenchCarryWeightNodeSelector(t *testing.T) {
	selector := NewCarryWeightNodeSelector(DataNodeType)
	dataNodeSelectorBench(t, selector)
	selector = NewCarryWeightNodeSelector(MetaNodeType)
	metaNodeSelectorBench(t, selector)
}

func TestBenchStrawNodeSelector(t *testing.T) {
	selector := NewStrawNodeSelector(DataNodeType)
	dataNodeSelectorBench(t, selector)
	selector = NewStrawNodeSelector(MetaNodeType)
	metaNodeSelectorBench(t, selector)
}
