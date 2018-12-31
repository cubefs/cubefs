// Copyright 2018 The CFS Authors.
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

// node_selector.go -> 选择node 列表

package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"math/rand"
	"sort"
	"time"
)

// TODO why it is called NodeTab?
// NodeTab 定义带权重的node -> weightedNode
type NodeTab struct {
	Carry  float64
	Weight float64
	Ptr    Node
	ID     uint64
}

// TODO why define such interface?
// Node defines an interface that needs to be implemented by NodeTab
type Node interface {
	SetCarry(carry float64)
	SelectNodeForWrite()
}

// TODO find a better name for Nodetab
// NodeTabArrSorterByCarry defines an array sorted by carry
type NodeTabArrSorterByCarry []*NodeTab

func (nodeTabs NodeTabArrSorterByCarry) Len() int {
	return len(nodeTabs)
}

// TODO remove unused functions
func (nodeTabs NodeTabArrSorterByCarry) Less(i, j int) bool {
	return nodeTabs[i].Carry > nodeTabs[j].Carry
}

// TODO remove unused functions
func (nodeTabs NodeTabArrSorterByCarry) Swap(i, j int) {
	nodeTabs[i], nodeTabs[j] = nodeTabs[j], nodeTabs[i]
}

func (nodeTabs NodeTabArrSorterByCarry) setNodeTabCarry(availCarryCount, replicaNum int) {
	if availCarryCount >= replicaNum {
		return
	}
	for availCarryCount < replicaNum {
		availCarryCount = 0
		for _, nt := range nodeTabs {
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
	nodeTabs, availCarryCount := ns.getAvailCarryMetaNodeTab(maxTotal, excludeHosts)
	if len(nodeTabs) < replicaNum {
		err = fmt.Errorf(getAvailMetaNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			errNoHaveAnyMetaNodeToWrite, ns.metaNodeLen, len(nodeTabs))
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

	if newHosts, err = ns.disOrderArray(orderHosts); err != nil {
		err = fmt.Errorf(getAvailMetaNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

// TODO find a better name for getAvailCarryMetaNodeTab
// carry meta node -> getAvailCarryMetaNode
func (ns *nodeSet) getAvailCarryMetaNodeTab(maxTotal uint64, excludeHosts []string) (nodeTabs NodeTabArrSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabArrSorterByCarry, 0)
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
		nt := new(NodeTab)
		nt.Carry = metaNode.Carry
		if metaNode.Used < 0 {
			nt.Weight = 1.0
		} else {
			nt.Weight = (float64)(maxTotal-metaNode.Used) / (float64)(maxTotal)
		}
		nt.Ptr = metaNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

// TODO what is disOrderArray? disorder array?
// 把数组乱序   shuffleArray
func (ns *nodeSet) disOrderArray(oldHosts []string) (newHosts []string, err error) {
	var (
		newCurrPos int
	)

	if oldHosts == nil || len(oldHosts) == 0 {
		log.LogError(fmt.Sprintf("action[disOrderArray],err:%v", errDisOrderArray))
		err = errDisOrderArray
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
