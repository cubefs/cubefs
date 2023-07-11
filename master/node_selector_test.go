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
	"sort"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/util"
)

func writeDataNode(sb *strings.Builder, node *DataNode) {
	sb.WriteString(fmt.Sprintf("Data Node %v\n", node.ID))
	sb.WriteString(fmt.Sprintf("\tTotal Space:%v MB\n", node.Total/util.MB))
	sb.WriteString(fmt.Sprintf("\tAvaliable Space:%v MB\n", node.AvailableSpace/util.MB))
}

func writeMetaNode(sb *strings.Builder, node *MetaNode) {
	sb.WriteString(fmt.Sprintf("Meta Node %v\n", node.ID))
	sb.WriteString(fmt.Sprintf("\tTotal Space:%v MB\n", node.Total/util.MB))
	sb.WriteString(fmt.Sprintf("\tAvaliable Space:%v MB\n", node.Total-node.Used/util.MB))
}

func printDataNode(t *testing.T, node *DataNode) {
	sb := strings.Builder{}
	writeDataNode(&sb, node)
	t.Log(sb.String())
}

func printMetaNode(t *testing.T, node *MetaNode) {
	sb := strings.Builder{}
	writeMetaNode(&sb, node)
	t.Log(sb.String())
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

func DataNodeSelectorTest(t *testing.T, selector NodeSelector, expectedNode *DataNode) {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return
	}
	t.Log("List nodesets of zone")
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	if nsc.Len() == 0 {
		t.Errorf("nodeset count could not be 0")
		return
	}
	nset := nsc[0]
	t.Logf("List datanodes of nodeset %v", nset.ID)
	printNodesetAndDataNodes(t, nset)
	_, peer, err := selector.Select(nset, nil, 1)
	if err != nil {
		t.Errorf("%v failed to select nodes %v", selector.GetName(), err)
		return
	}
	t.Log("List selected nodes:")
	for i := 0; i < len(peer); i++ {
		nodeVal, ok := nset.dataNodes.Load(peer[i].Addr)
		if !ok {
			t.Errorf("%v select wrong node", selector.GetName())
			return
		}
		node := nodeVal.(*DataNode)
		printDataNode(t, node)
	}
	nodeVal, ok := nset.dataNodes.Load(peer[0].Addr)
	if !ok {
		t.Errorf("%v failed to select nodes", selector.GetName())
		return
	}
	node := nodeVal.(*DataNode)
	if expectedNode != nil && node.ID != expectedNode.ID {
		t.Errorf("%v select wrong node, expected: %v actually: %v", selector.GetName(), expectedNode.ID, node.ID)
		return
	}
}

func MetaNodeSelectorTest(t *testing.T, selector NodeSelector, expectedNode *MetaNode) {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return
	}
	t.Log("List nodesets of zone")
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	nset := nsc[0]
	t.Logf("List metanodes of nodeset %v", nset.ID)
	printNodesetAndMetaNodes(t, nset)
	_, peer, err := selector.Select(nset, nil, 1)
	if err != nil {
		t.Errorf("%v failed to select nodes %v", selector.GetName(), err)
		return
	}
	t.Log("List selected nodes:")
	for i := 0; i < len(peer); i++ {
		nodeVal, ok := nset.metaNodes.Load(peer[i].Addr)
		if !ok {
			t.Errorf("%v select wrong node", selector.GetName())
			return
		}
		node := nodeVal.(*MetaNode)
		printMetaNode(t, node)
	}
	nodeVal, ok := nset.metaNodes.Load(peer[0].Addr)
	if !ok {
		t.Errorf("%v failed to select nodes", selector.GetName())
		return
	}
	node := nodeVal.(*MetaNode)
	if expectedNode != nil && node.ID != expectedNode.ID {
		t.Errorf("%v select wrong node, expected: %v actually: %v", selector.GetName(), expectedNode.ID, node.ID)
		return
	}
}

func TestCarryWeightNodeSelector(t *testing.T) {
	// get first node
	dataNode := getFirstDataNodeForTest(t, testZone2)
	metaNode := getFirstMetaNodeForTest(t, testZone2)

	// prepare for datanode
	tmp := dataNode.AvailableSpace
	dataNode.Total += dataNode.AvailableSpace
	dataNode.AvailableSpace *= 2
	// select test
	selector := NewCarryWeightNodeSelector(DataNodeType)
	DataNodeSelectorTest(t, selector, dataNode)
	// restore status
	dataNode.Total -= tmp
	dataNode.AvailableSpace = tmp

	// prepare for metanode
	tmp = metaNode.Total
	metaNode.Total *= 2
	// select test
	selector = NewCarryWeightNodeSelector(MetaNodeType)
	MetaNodeSelectorTest(t, selector, metaNode)
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
		t.Logf("Select DataNode Round %v", i)
		DataNodeSelectorTest(t, selector, node)
	}
	selector = NewRoundRobinNodeSelector(MetaNodeType)
	for i, node := range metaNodes {
		t.Logf("Select MetaNode Round %v", i)
		MetaNodeSelectorTest(t, selector, node)
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
	DataNodeSelectorTest(t, selector, dataNode)
	// restore status
	dataNode.Total -= tmp
	dataNode.AvailableSpace = tmp

	// prepare for metanode
	tmp = metaNode.Total
	metaNode.Total *= 2
	// select test
	selector = NewAvailableSpaceFirstNodeSelector(MetaNodeType)
	MetaNodeSelectorTest(t, selector, metaNode)
	// restore status
	metaNode.Total = tmp
}
