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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/master/mocktest"
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
	mocktest.Log(t, sb.String())
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
	zone.nsLock.RLock()
	defer zone.nsLock.RUnlock()
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

func TestStrawNodesetSelector(t *testing.T) {
	selector := NewStrawNodesetSelector(DataNodeType)
	NodesetSelectorTest(t, selector)
	selector = NewStrawNodesetSelector(MetaNodeType)
	NodesetSelectorTest(t, selector)
}

func prepareDataNodesetForBench(count int, initTotal uint64, grow uint64) (nsc nodeSetCollection) {
	nsc = make(nodeSetCollection, 0, count)
	for i := 0; i < count; i++ {
		ns := prepareDataNodesForBench(1, initTotal+grow*uint64(i), 0)
		ns.ID = uint64(i) + 1
		nsc = append(nsc, ns)
	}
	return
}

func prepareMetaNodesetForBench(count int, initTotal uint64, grow uint64) (nsc nodeSetCollection) {
	nsc = make(nodeSetCollection, 0, count)
	for i := 0; i < count; i++ {
		ns := prepareMetaNodesForBench(1, initTotal+grow*uint64(i), 0)
		ns.ID = uint64(i) + 1
		nsc = append(nsc, ns)
	}
	return
}

const loopNodesetSelectorTestCount = 100

func nodesetSelectorBench(selector NodesetSelector, nsc nodeSetCollection, onSelect func(id uint64)) (map[uint64]int, error) {
	times := make(map[uint64]int)
	for i := 0; i < loopNodeSelectorTestCount; i++ {
		ns, err := selector.Select(nsc, nil, 1)
		if err != nil {
			return nil, err
		}
		count, _ := times[ns.ID]
		count += 1
		times[ns.ID] = count
		if onSelect != nil {
			onSelect(ns.ID)
		}
	}
	return times, nil
}

func printNodesetSelectTimes(t *testing.T, times map[uint64]int) {
	for id, count := range times {
		mocktest.Log(t, fmt.Sprintf("Nodeset %v select %v times", id, count))
	}
}

func dataNodesetSelectorBench(t *testing.T, selector NodesetSelector) error {
	nsc := prepareDataNodesetForBench(4, 100*util.GB, 100*util.GB)
	random := rand.New(rand.NewSource(time.Now().Unix()))
	times, err := nodesetSelectorBench(selector, nsc, func(id uint64) {
		for _, ns := range nsc {
			if ns.ID == id {
				decrase := uint64(random.Float64() * util.GB * 10)
				ns.dataNodes.Range(func(key, value interface{}) bool {
					node := value.(*DataNode)
					tmp := decrase
					if tmp > node.AvailableSpace {
						tmp = node.AvailableSpace
					}
					node.AvailableSpace -= tmp
					decrase -= tmp
					return decrase > 0
				})
			}
		}
	})
	if err != nil {
		t.Errorf("%v failed to Bench %v", selector.GetName(), err)
		return err
	}
	t.Logf("%v Nodeset Select times:", selector.GetName())
	printNodesetSelectTimes(t, times)
	for _, ns := range nsc {
		printNodeset(t, ns)
	}
	return nil
}

func metaNodesetSelectorBench(t *testing.T, selector NodesetSelector) error {
	nsc := prepareMetaNodesetForBench(4, 100*util.GB, 100*util.GB)
	random := rand.New(rand.NewSource(time.Now().Unix()))
	times, err := nodesetSelectorBench(selector, nsc, func(id uint64) {
		for _, ns := range nsc {
			if ns.ID == id {
				decrase := uint64(random.Float64() * util.GB * 10)
				ns.metaNodes.Range(func(key, value interface{}) bool {
					node := value.(*MetaNode)
					tmp := decrase
					if tmp+node.Used > node.Total {
						tmp = node.Total - node.Used
					}
					node.Used += tmp
					decrase -= tmp
					return decrase > 0
				})
			}
		}
	})
	if err != nil {
		t.Errorf("%v failed to Bench %v", selector.GetName(), err)
		return err
	}
	t.Logf("%v Nodeset Select times:", selector.GetName())
	printNodesetSelectTimes(t, times)
	for _, ns := range nsc {
		printNodeset(t, ns)
	}
	return nil
}

func TestBenchmarkCarryWeightNodesetSelector(t *testing.T) {
	selector := NewCarryWeightNodesetSelector(DataNodeType)
	err := dataNodesetSelectorBench(t, selector)
	if err != nil {
		t.Errorf("%v nodeset selector failed to benchmark %v", selector.GetName(), err)
		return
	}
	selector = NewCarryWeightNodesetSelector(MetaNodeType)
	err = metaNodesetSelectorBench(t, selector)
	if err != nil {
		t.Errorf("%v nodeset selector failed to benchmark %v", selector.GetName(), err)
		return
	}
}

func TestBenchmarkStrawNodesetSelector(t *testing.T) {
	selector := NewStrawNodesetSelector(DataNodeType)
	err := dataNodesetSelectorBench(t, selector)
	if err != nil {
		t.Errorf("%v nodeset selector failed to benchmark %v", selector.GetName(), err)
		return
	}
	selector = NewStrawNodesetSelector(MetaNodeType)
	err = metaNodesetSelectorBench(t, selector)
	if err != nil {
		t.Errorf("%v nodeset selector failed to benchmark %v", selector.GetName(), err)
		return
	}
}
