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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
)

const RoundRobinNodesetSelectorName = "RoundRobin"

const CarryWeightNodesetSelectorName = "CarryWeight"

const WeightRoundRobinNodesetSelectorName = "WeightRoundRobin"

const DefaultNodesetSelectorName = RoundRobinNodesetSelectorName

type NodesetSelector interface {
	GetName() string

	Select(excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error)

	SetCandidates(nsc nodeSetCollection)
}

type RoundRobinNodesetSelector struct {
	index int

	Candidates nodeSetCollection
}

func (s *RoundRobinNodesetSelector) SetCandidates(nsc nodeSetCollection) {
	s.Candidates = nsc
}

func (s *RoundRobinNodesetSelector) Select(excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	nset := s.Candidates
	for i := 0; i < len(nset); i++ {

		if s.index >= len(nset) {
			s.index = 0
		}

		ns = nset[s.index]
		s.index++

		if containsID(excludeNodeSets, ns.ID) {
			continue
		}

		if ns.canWriteForDataNode(int(replicaNum)) {
			return
		}
	}

	err = errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
	return
}

func (s *RoundRobinNodesetSelector) GetName() string {
	return RoundRobinNodesetSelectorName
}

func NewRoundRobinNodesetSelector() *RoundRobinNodesetSelector {
	return &RoundRobinNodesetSelector{}
}

func NewNodesetSelector(name string) NodesetSelector {
	switch name {
	case CarryWeightNodesetSelectorName:
	case WeightRoundRobinNodesetSelectorName:
	}
	return NewRoundRobinNodesetSelector()
}
