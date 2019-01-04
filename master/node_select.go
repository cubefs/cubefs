// Copyright 2018 The Container File System Authors.
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

/* TODO change the file name to node_selector.go */

package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"math/rand"
	"sort"
	"time"
)

type WeightedNode struct {
	Carry  float64
	Weight float64
	Ptr    Node
	ID     uint64
}

// Node defines an interface that needs to be implemented by WeightedNode
type Node interface {
	SetCarry(carry float64)
	SelectNodeForWrite()
}

// SortedWeightedNodes defines an array sorted by carry
type SortedWeightedNodes []*WeightedNode

func (nodes SortedWeightedNodes) Len() int {
	return len(nodes)
}

func (nodes SortedWeightedNodes) Less(i, j int) bool {
	return nodes[i].Carry > nodes[j].Carry
}

func (nodes SortedWeightedNodes) Swap(i, j int) {
	nodes[i], nodes[j] = nodes[j], nodes[i]
}

func (nodes SortedWeightedNodes) setNodeTabCarry(availCarryCount, replicaNum int) {
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

func (ns *nodeSet) getMetaNodeMaxTotal() (maxTotal uint64) {
	ns.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if metaNode.Total > maxTotal {
			maxTotal = metaNode.Total
		}
		return true
	})
	return
}

func (ns *nodeSet) getAvailMetaNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := ns.getMetaNodeMaxTotal()
	nodeTabs, availCarryCount := ns.getAvailCarryMetaNode(maxTotal, excludeHosts)
	if len(nodeTabs) < replicaNum {
		err = fmt.Errorf(getAvailMetaNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			noMetaNodeToWriteErr, ns.metaNodeLen, len(nodeTabs))
		return
	}

	nodeTabs.setNodeTabCarry(availCarryCount, replicaNum)
	sort.Sort(nodeTabs)

	for i := 0; i < replicaNum; i++ {
		node := nodeTabs[i].Ptr.(*MetaNode)
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.Addr)
		peer := proto.Peer{ID: node.ID, Addr: node.Addr}
		peers = append(peers, peer)
	}

	if newHosts, err = ns.reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf(getAvailMetaNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

// TODO find a better name for getAvailCarryMetaNode
func (ns *nodeSet) getAvailCarryMetaNode(maxTotal uint64, excludeHosts []string) (nodes SortedWeightedNodes, availCount int) {
	nodes = make(SortedWeightedNodes, 0)
	ns.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if contains(excludeHosts, metaNode.Addr) == true {
			return true
		}
		if metaNode.isWriteAble() == false {
			return true
		}
		if metaNode.isAvailCarryNode() == true {
			availCount++
		}
		nt := new(WeightedNode)
		nt.Carry = metaNode.Carry
		if metaNode.Used < 0 {
			nt.Weight = 1.0
		} else {
			nt.Weight = (float64)(maxTotal - metaNode.Used) / (float64)(maxTotal)
		}
		nt.Ptr = metaNode
		nodes = append(nodes, nt)

		return true
	})

	return
}

func (ns *nodeSet) reshuffleHosts(oldHosts []string) (newHosts []string, err error) {
	var (
		newCurrPos int
	)

	if oldHosts == nil || len(oldHosts) == 0 {
		log.LogError(fmt.Sprintf("action[reshuffleHosts],err:%v", reshuffleArrayErr))
		err = reshuffleArrayErr
		return
	}

	lenOldHosts := len(oldHosts)
	newHosts = make([]string, lenOldHosts)
	if lenOldHosts == 1 {
		copy(newHosts, oldHosts)
		return
	}

	for randCount := 0; randCount < lenOldHosts; randCount++ {
		remainCount := lenOldHosts - randCount
		rand.Seed(time.Now().UnixNano())
		oCurrPos := rand.Intn(remainCount)
		newHosts[newCurrPos] = oldHosts[oCurrPos]
		newCurrPos++
		oldHosts[oCurrPos] = oldHosts[remainCount-1]
	}

	return
}
