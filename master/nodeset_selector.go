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
	"sort"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
)

const RoundRobinNodesetSelectorName = "RoundRobin"

const CarryWeightNodesetSelectorName = "CarryWeight"

const AvailableSpaceFirstNodesetSelectorName = "AvailableSpaceFirst"

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
			s.carrys[id] = float64(nodeset.getTotalAvailableSpaceOf(s.nodeType)) / float64(total)
		}
	}
}

func (s *CarryWeightNodesetSelector) Select(nsc nodeSetCollection, excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	total := s.getMaxTotal(nsc)
	s.prepareCarry(nsc, total)
	sort.Slice(nsc, func(i, j int) bool {
		return s.carrys[nsc[i].ID] > s.carrys[nsc[j].ID]
	})
	for i := 0; i < nsc.Len(); i++ {
		nset := nsc[i]
		if nset.canWriteFor(s.nodeType, int(replicaNum)) {
			if i != 0 {
				nsc[i], nsc[0] = nsc[0], nsc[i]
			}
			ns = nset
			break
		}
	}
	for i := 1; i < nsc.Len(); i++ {
		nset := nsc[i]
		weight := float64(nset.getTotalAvailableSpaceOf(s.nodeType)) / float64(total)
		s.carrys[nset.ID] += weight
		if s.carrys[nset.ID] > 10.0 {
			s.carrys[nset.ID] = 10.0
		}
	}
	if ns != nil {
		s.carrys[ns.ID] -= 1.0
		if s.carrys[ns.ID] < 0 {
			s.carrys[ns.ID] = 0
		}
		return
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
	sort.Slice(nsc, func(i, j int) bool {
		return nsc[i].getTotalAvailableSpaceOf(s.nodeType) > nsc[j].getTotalAvailableSpaceOf(s.nodeType)
	})
	for i := 0; i < nsc.Len(); i++ {
		if nsc[i].canWriteFor(s.nodeType, int(replicaNum)) {
			ns = nsc[i]
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

func NewNodesetSelector(name string, nodeType NodeType) NodesetSelector {
	switch name {
	case CarryWeightNodesetSelectorName:
		return NewCarryWeightNodesetSelector(nodeType)
	case RoundRobinNodesetSelectorName:
		return NewRoundRobinNodesetSelector(nodeType)
	case AvailableSpaceFirstNodesetSelectorName:
		return NewAvailableSpaceFirstNodesetSelector(nodeType)
	}
	return NewRoundRobinNodesetSelector(nodeType)
}
