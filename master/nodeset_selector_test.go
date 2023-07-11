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

func writeNodeset(sb *strings.Builder, nset *nodeSet) {
	sb.WriteString(fmt.Sprintf("Nodeset %v\n", nset.ID))
	sb.WriteString(fmt.Sprintf("\tTotal Data Space:%v GB\n", nset.getDataNodeTotalSpace()/util.GB))
	sb.WriteString(fmt.Sprintf("\tTotal Meta Space:%v GB\n", nset.getMetaNodeTotalSpace()/util.GB))
	sb.WriteString(fmt.Sprintf("\tTotal Data Available Space:%v GB\n", nset.getDataNodeTotalAvailableSpace()/util.GB))
	sb.WriteString(fmt.Sprintf("\tTotal Meta Available Space:%v GB\n", nset.getMetaNodeTotalAvailableSpace()/util.GB))
}

func printNodesetsOfZone(t *testing.T, zone *Zone) {
	nsc := zone.getAllNodeSet()
	for i := 0; i < nsc.Len(); i++ {
		nset := nsc[i]
		printNodeset(t, nset)
	}
}

func printNodeset(t *testing.T, nset *nodeSet) {
	sb := strings.Builder{}
	writeNodeset(&sb, nset)
	t.Logf(sb.String())
}

func NodesetSelectorTest(t *testing.T, selector NodesetSelector) {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
		return
	}
	printNodesetsOfZone(t, zone)
	nsc := zone.getAllNodeSet()
	ns, err := selector.Select(nsc, nil, 1)
	if err != nil {
		t.Errorf("%v failed to select nodeset %v", selector.GetName(), err)
		return
	}
	zone.nsLock.Lock()
	defer zone.nsLock.Unlock()
	if _, ok := zone.nodeSetMap[ns.ID]; !ok {
		t.Errorf("%v select a wrong nodeset", selector.GetName())
		return
	}
	t.Logf("%v select nodeset %v", selector.GetName(), ns.ID)
	printNodeset(t, ns)
}

func TestRoundRobinNodesetSelector(t *testing.T) {
	selector := NewRoundRobinNodesetSelector(DataNodeType)
	NodesetSelectorTest(t, selector)
	selector = NewRoundRobinNodesetSelector(MetaNodeType)
	NodesetSelectorTest(t, selector)
}

func TestCarryWeightNodesetSelector(t *testing.T) {
	selector := NewCarryWeightNodesetSelector(DataNodeType)
	NodesetSelectorTest(t, selector)
	selector = NewCarryWeightNodesetSelector(MetaNodeType)
	NodesetSelectorTest(t, selector)
}

func TestAvailableSpaceFirstNodesetSelector(t *testing.T) {
	selector := NewAvailableSpaceFirstNodesetSelector(DataNodeType)
	NodesetSelectorTest(t, selector)
	selector = NewAvailableSpaceFirstNodesetSelector(MetaNodeType)
	NodesetSelectorTest(t, selector)
}
