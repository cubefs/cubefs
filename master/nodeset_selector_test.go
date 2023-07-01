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

import "testing"

func NodesetSelectorTest(t *testing.T, selector NodesetSelector) {
	selectZone := testZone2
	zone, err := server.cluster.t.getZone(selectZone)
	if err != nil {
		t.Errorf("failed to get zone %v", err)
	}
	nsc := zone.getAllNodeSet()
	ns, err := selector.Select(nsc, nil, 1)
	if err != nil {
		t.Errorf("%v failed to select nodeset %v", selector.GetName(), err)
	}
	zone.nsLock.Lock()
	defer zone.nsLock.Unlock()
	if _, ok := zone.nodeSetMap[ns.ID]; !ok {
		t.Errorf("%v select a wrong nodeset", selector.GetName())
	}
	t.Logf("%v select nodeset %v", selector.GetName(), ns.ID)
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
