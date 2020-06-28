// Copyright 2018 The Chubao Authors.
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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"sort"
	"sync"
	"time"
)

const (
	selectDataNode = 0
	selectMetaNode = 1
	selectEcNode   = 2
)

type weightedNode struct {
	Carry  float64
	Weight float64
	Ptr    Node
	ID     uint64
}

// Node defines an interface that needs to be implemented by weightedNode
type Node interface {
	isWriteAble() bool
	isAvailCarryNode() bool
	SetCarry(carry float64)
	SelectNodeForWrite()
	GetID() uint64
	GetAddr() string
	IsOnline() bool
	GetTotal() uint64
	GetUsed() uint64
	GetAvail() uint64
	GetCarry() float64
	GetReportTime() time.Time
}

// SortedWeightedNodes defines an array sorted by carry
type SortedWeightedNodes []*weightedNode

func (nodes SortedWeightedNodes) Len() int {
	return len(nodes)
}

func (nodes SortedWeightedNodes) Less(i, j int) bool {
	return nodes[i].Carry > nodes[j].Carry
}

func (nodes SortedWeightedNodes) Swap(i, j int) {
	nodes[i], nodes[j] = nodes[j], nodes[i]
}

func (nodes SortedWeightedNodes) setNodeCarry(availCarryCount, replicaNum int) {
	if availCarryCount >= replicaNum {
		return
	}
	for availCarryCount < replicaNum {
		availCarryCount = 0
		for _, nt := range nodes {
			carry := nt.Carry + nt.Weight
			if carry > 10.0 {
				carry = 10.0
			}
			nt.Carry = carry
			nt.Ptr.SetCarry(carry)
			if carry > 1.0 {
				availCarryCount++
			}
		}
	}
}

type GetMaxTotal func(nodes *sync.Map) (maxTotal uint64)

func getNodeMaxTotal(nodes *sync.Map) (maxTotal uint64) {
	nodes.Range(func(key, value interface{}) bool {
		node, ok := value.(Node)
		if !ok {
			return true
		}
		if node.GetTotal() > maxTotal {
			maxTotal = node.GetTotal()
		}
		return true
	})
	return
}

type GetCarryNodes func(maxTotal uint64, excludeHosts []string, nodes *sync.Map) (weightedNodes SortedWeightedNodes, availCount int)

func getAvailCarryNodeTab(maxTotal uint64, excludeHosts []string, nodes *sync.Map) (nodeTabs SortedWeightedNodes, availCount int) {
	nodeTabs = make(SortedWeightedNodes, 0)
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		if contains(excludeHosts, node.GetAddr()) == true {
			log.LogDebugf("contains return")
			return true
		}
		if node.isWriteAble() == false {
			log.LogDebugf("isWritable return")
			return true
		}
		if node.isAvailCarryNode() == true {
			availCount++
		}
		nt := new(weightedNode)
		nt.Carry = node.GetCarry()
		if node.GetAvail() < 0 {
			nt.Weight = 0.0
		} else {
			nt.Weight = float64(node.GetAvail()) / float64(maxTotal)
		}
		nt.Ptr = node
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

func getAvailHosts(nodes *sync.Map, excludeHosts []string, replicaNum int, selectType int) (newHosts []string, peers []proto.Peer, err error) {
	var (
		maxTotalFunc      GetMaxTotal
		getCarryNodesFunc GetCarryNodes
	)
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if replicaNum == 0 {
		return
	}
	maxTotalFunc = getNodeMaxTotal
	switch selectType {
	case selectDataNode, selectMetaNode:
		getCarryNodesFunc = getAvailCarryNodeTab
	case selectEcNode:
		getCarryNodesFunc = getAvailCarryEcNodeTab
	default:
		return nil, nil, fmt.Errorf("invalid selectType[%v]", selectType)
	}
	maxTotal := maxTotalFunc(nodes)
	weightedNodes, availCarryCnt := getCarryNodesFunc(maxTotal, excludeHosts, nodes)
	if len(weightedNodes) < replicaNum {
		err = fmt.Errorf("action[getAvailHosts] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v  ",
			replicaNum, len(weightedNodes))
		return
	}
	weightedNodes.setNodeCarry(availCarryCnt, replicaNum)
	sort.Sort(weightedNodes)

	for i := 0; i < replicaNum; i++ {
		node := weightedNodes[i].Ptr
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.GetAddr())
		peer := proto.Peer{ID: node.GetID(), Addr: node.GetAddr()}
		peers = append(peers, peer)
	}

	if newHosts, err = reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf("action[getAvailHosts] err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func (ns *nodeSet) getAvailMetaNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	return getAvailHosts(ns.metaNodes, excludeHosts, replicaNum, selectMetaNode)
}
