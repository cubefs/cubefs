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
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
)

const RoundRobinNodesetSelectorName = "RoundRobin"

const CarryWeightNodesetSelectorName = "CarryWeight"

const AvailableSpaceFirstNodesetSelectorName = "AvailableSpaceFirst"

const StrawNodesetSelectorName = "Straw"

const DefaultNodesetSelectorName = RoundRobinNodeSelectorName

func (ns *nodeSet) getDataNodeTotalSpace() (toalSpace uint64) {
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		toalSpace += dataNode.Total
		return true
	})
	return
}

func (ns *nodeSet) getMetaNodeTotalSpace() (toalSpace uint64) {
	ns.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		toalSpace += metaNode.Total
		return true
	})
	return
}

func (ns *nodeSet) getDataNodeTotalAvailableSpace() (space uint64) {
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if !dataNode.ToBeOffline {
			space += dataNode.AvailableSpace
		}
		return true
	})
	return
}

func (ns *nodeSet) getMetaNodeTotalAvailableSpace() (space uint64) {
	ns.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if !metaNode.ToBeOffline {
			space += metaNode.Total - metaNode.Used
		}
		return true
	})
	return
}

func (ns *nodeSet) canWriteFor(nodeType NodeType, replica int) bool {
	switch nodeType {
	case DataNodeType:
		return ns.canWriteForDataNode(replica)
	case MetaNodeType:
		return ns.canWriteForMetaNode(replica)
	default:
		panic("unknow node type")
	}
}

func (ns *nodeSet) getTotalSpaceOf(nodeType NodeType) uint64 {
	switch nodeType {
	case DataNodeType:
		return ns.getDataNodeTotalSpace()
	case MetaNodeType:
		return ns.getMetaNodeTotalSpace()
	default:
		panic("unknow node type")
	}
}

func (ns *nodeSet) getTotalAvailableSpaceOf(nodeType NodeType) uint64 {
	switch nodeType {
	case DataNodeType:
		return ns.getDataNodeTotalAvailableSpace()
	case MetaNodeType:
		return ns.getMetaNodeTotalAvailableSpace()
	default:
		panic("unknow node type")
	}
}

type NodesetSelector interface {
	GetName() string
	Select(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error)
}

type RoundRobinNodesetSelector struct {
	index int

	nodeType NodeType
}

func (s *RoundRobinNodesetSelector) Select(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	// sort nodesets by id, so we can get a node list that is as stable as possible
	sort.Slice(nsc, func(i, j int) bool {
		return nsc[i].ID < nsc[j].ID
	})
	for i := 0; i < len(nsc); i++ {

		if s.index >= len(nsc) {
			s.index = 0
		}

		ns = nsc[s.index]
		s.index++

		if containsID(excludeNodeSets, ns.ID) {
			continue
		}
		if ns.canWriteFor(s.nodeType, int(replicaNum)) {
			return
		}
	}

	switch s.nodeType {
	case DataNodeType:
		err = errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
	case MetaNodeType:
		err = errors.NewError(proto.ErrNoNodeSetToCreateMetaPartition)
	default:
		panic("unknow node type")
	}
	return
}

func (s *RoundRobinNodesetSelector) GetName() string {
	return RoundRobinNodesetSelectorName
}

func NewRoundRobinNodesetSelector(nodeType NodeType) *RoundRobinNodesetSelector {
	return &RoundRobinNodesetSelector{
		nodeType: nodeType,
	}
}

type CarryWeightNodesetSelector struct {
	carrys map[uint64]float64

	nodeType NodeType
}

func (s *CarryWeightNodesetSelector) GetName() string {
	return CarryWeightNodesetSelectorName
}

func (s *CarryWeightNodesetSelector) getMaxTotal(nsc nodeSetCollection) uint64 {
	total := uint64(0)
	for i := 0; i < nsc.Len(); i++ {
		tmp := nsc[i].getTotalSpaceOf(s.nodeType)
		if tmp > total {
			total = tmp
		}
	}
	return total
}

func (s *CarryWeightNodesetSelector) prepareCarry(nsc nodeSetCollection, total uint64) {
	for _, nodeset := range nsc {
		id := nodeset.ID
		if _, ok := s.carrys[id]; !ok {
			// use total available space to calculate initial weight
			s.carrys[id] = float64(nodeset.getTotalAvailableSpaceOf(s.nodeType)) / float64(total)
		}
	}
}

func (s *CarryWeightNodesetSelector) getAvailNodesets(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (newNsc nodeSetCollection) {
	newNsc = make(nodeSetCollection, 0, nsc.Len())
	for i := 0; i < nsc.Len(); i++ {
		ns := nsc[i]
		if ns.canWriteFor(s.nodeType, int(replicaNum)) && !containsID(excludeNodeSets, ns.ID) {
			newNsc = append(newNsc, ns)
		}
	}
	return
}

func (s *CarryWeightNodesetSelector) getCarryCount(nsc nodeSetCollection) (count int) {
	for i := 0; i < nsc.Len(); i++ {
		ns := nsc[i]
		if s.carrys[ns.ID] >= 1.0 {
			count += 1
		}
	}
	return
}

func (s *CarryWeightNodesetSelector) setNodesetCarry(nsc nodeSetCollection, total uint64) int {
	count := s.getCarryCount(nsc)
	for count < 1 {
		count = 0
		for i := 0; i < nsc.Len(); i++ {
			nset := nsc[i]
			weight := float64(nset.getTotalAvailableSpaceOf(s.nodeType)) / float64(total)
			s.carrys[nset.ID] += weight
			if s.carrys[nset.ID] >= 1.0 {
				count += 1
			}
			// limit the max value of weight
			if s.carrys[nset.ID] > 10.0 {
				s.carrys[nset.ID] = 10.0
			}
		}
	}
	return count
}

func (s *CarryWeightNodesetSelector) Select(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	total := s.getMaxTotal(nsc)
	// prepare weight of evert nodesets
	s.prepareCarry(nsc, total)
	nsc = s.getAvailNodesets(nsc, excludeNodeSets, replicaNum)
	avaliCount := 0
	if len(nsc) < 1 {
		goto err
	}
	avaliCount = s.setNodesetCarry(nsc, total)
	// sort nodesets by weight
	sort.Slice(nsc, func(i, j int) bool {
		return s.carrys[nsc[i].ID] > s.carrys[nsc[j].ID]
	})
	// pick the first nodeset than has N writable node
	for i := 0; i < avaliCount; i++ {
		ns = nsc[i]
		if ns.canWriteFor(s.nodeType, int(replicaNum)) && !containsID(excludeNodeSets, ns.ID) {
			break
		}
	}
	if ns != nil {
		if !ns.canWriteFor(s.nodeType, int(replicaNum)) || containsID(excludeNodeSets, ns.ID) {
			goto err
		}
		s.carrys[ns.ID] -= 1.0
	}
	return
err:
	switch s.nodeType {
	case DataNodeType:
		err = errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
	case MetaNodeType:
		err = errors.NewError(proto.ErrNoNodeSetToCreateMetaPartition)
	default:
		panic("unknow node type")
	}
	return
}

func NewCarryWeightNodesetSelector(nodeType NodeType) *CarryWeightNodesetSelector {
	return &CarryWeightNodesetSelector{
		carrys:   make(map[uint64]float64),
		nodeType: nodeType,
	}
}

type AvailableSpaceFirstNodesetSelector struct {
	nodeType NodeType
}

func (s *AvailableSpaceFirstNodesetSelector) GetName() string {
	return AvailableSpaceFirstNodesetSelectorName
}

func (s *AvailableSpaceFirstNodesetSelector) Select(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	// sort nodesets by available space
	sort.Slice(nsc, func(i, j int) bool {
		return nsc[i].getTotalAvailableSpaceOf(s.nodeType) > nsc[j].getTotalAvailableSpaceOf(s.nodeType)
	})
	// pick the first nodeset that has N writable nodes
	for i := 0; i < nsc.Len(); i++ {
		ns = nsc[i]
		if ns.canWriteFor(s.nodeType, int(replicaNum)) && !containsID(excludeNodeSets, ns.ID) {
			return
		}
	}
	switch s.nodeType {
	case DataNodeType:
		err = errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
	case MetaNodeType:
		err = errors.NewError(proto.ErrNoNodeSetToCreateMetaPartition)
	default:
		panic("unknow node type")
	}
	return
}

func NewAvailableSpaceFirstNodesetSelector(nodeType NodeType) *AvailableSpaceFirstNodesetSelector {
	return &AvailableSpaceFirstNodesetSelector{
		nodeType: nodeType,
	}
}

const (
	StrawNodesetSelectorRandMax = 65536
)

// NOTE: this nodeset selector inspired by Straw2 algorithm, which is widely used in ceph
type StrawNodesetSelector struct {
	nodeType NodeType
	rand     *rand.Rand
}

func (s *StrawNodesetSelector) GetName() string {
	return StrawNodesetSelectorName
}

func (s *StrawNodesetSelector) getWeight(ns *nodeSet) float64 {
	return float64(ns.getTotalAvailableSpaceOf(s.nodeType) / util.GB)
}

func (s *StrawNodesetSelector) Select(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	tmp := make(nodeSetCollection, 0)
	for _, nodeset := range nsc {
		if nodeset.canWriteFor(s.nodeType, int(replicaNum)) && !containsID(excludeNodeSets, nodeset.ID) {
			tmp = append(tmp, nodeset)
		}
	}
	nsc = tmp
	if len(nsc) < 1 {
		switch s.nodeType {
		case DataNodeType:
			err = errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
		case MetaNodeType:
			err = errors.NewError(proto.ErrNoNodeSetToCreateMetaPartition)
		default:
			panic("unknow node type")
		}
		return
	}
	maxStraw := float64(0)
	for _, nodeset := range nsc {
		straw := float64(s.rand.Intn(StrawNodesetSelectorRandMax))
		straw = math.Log(straw/float64(StrawNodesetSelectorRandMax)) / s.getWeight(nodeset)
		if ns == nil || straw > maxStraw {
			ns = nodeset
			maxStraw = straw
		}
	}
	return
}

func NewStrawNodesetSelector(nodeType NodeType) *StrawNodesetSelector {
	return &StrawNodesetSelector{
		nodeType: nodeType,
		rand:     rand.New(rand.NewSource(time.Now().Unix())),
	}
}

func NewNodesetSelector(name string, nodeType NodeType) NodesetSelector {
	switch name {
	case CarryWeightNodesetSelectorName:
		return NewCarryWeightNodesetSelector(nodeType)
	case RoundRobinNodesetSelectorName:
		return NewRoundRobinNodesetSelector(nodeType)
	case AvailableSpaceFirstNodesetSelectorName:
		return NewAvailableSpaceFirstNodesetSelector(nodeType)
	case StrawNodesetSelectorName:
		return NewStrawNodesetSelector(nodeType)
	default:
		return NewRoundRobinNodesetSelector(nodeType)
	}
}
