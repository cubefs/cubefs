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

func DataNodeSelectorTest(t *testing.T, selector NodeSelector) {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
	}
	t.Log("List nodesets of zone")
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	nset := nsc[0]
	t.Logf("List datanodes of nodeset %v", nset.ID)
	printNodesetAndDataNodes(t, nset)
	_, peer, err := selector.Select(nset, nil, 3)
	if err != nil {
		t.Errorf("%v failed to select nodes %v", selector.GetName(), err)
	}
	t.Log("List selected nodes:")
	for i := 0; i < len(peer); i++ {
		nodeVal, ok := nset.dataNodes.Load(peer[i].Addr)
		if !ok {
			t.Errorf("%v select wrong node", selector.GetName())
		}
		node := nodeVal.(*DataNode)
		printDataNode(t, node)
	}
}

func MetaNodeSelectorTest(t *testing.T, selector NodeSelector) {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
	}
	t.Log("List nodesets of zone")
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	nset := nsc[0]
	t.Logf("List metanodes of nodeset %v", nset.ID)
	printNodesetAndMetaNodes(t, nset)
	_, peer, err := selector.Select(nset, nil, 3)
	if err != nil {
		t.Errorf("%v failed to select nodes %v", selector.GetName(), err)
	}
	t.Log("List selected nodes:")
	for i := 0; i < len(peer); i++ {
		nodeVal, ok := nset.metaNodes.Load(peer[i].Addr)
		if !ok {
			t.Errorf("%v select wrong node", selector.GetName())
		}
		node := nodeVal.(*MetaNode)
		printMetaNode(t, node)
	}
}

func TestCarryWeightNodeSelector(t *testing.T) {
	selector := NewCarryWeightNodeSelector(DataNodeType)
	DataNodeSelectorTest(t, selector)
	selector = NewCarryWeightNodeSelector(MetaNodeType)
	MetaNodeSelectorTest(t, selector)
}

func TestRoundRobinNodeSelector(t *testing.T) {
	selector := NewRoundRobinNodeSelector(DataNodeType)
	DataNodeSelectorTest(t, selector)
	selector = NewRoundRobinNodeSelector(MetaNodeType)
	MetaNodeSelectorTest(t, selector)
}

func TestAvailableSpaceFirstNodeSelector(t *testing.T) {
	selector := NewAvailableSpaceFirstNodeSelector(DataNodeType)
	DataNodeSelectorTest(t, selector)
	selector = NewAvailableSpaceFirstNodeSelector(MetaNodeType)
	MetaNodeSelectorTest(t, selector)
}
