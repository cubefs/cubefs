// Copyright 2018 The CubeFS Authors.
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
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const RoundRobinNodeSelectorName = "RoundRobin"

const CarryWeightNodeSelectorName = "CarryWeight"

const AvailableSpaceFirstNodeSelectorName = "AvailableSpaceFirst"

const StrawNodeSelectorName = "Straw"

const DefaultNodeSelectorName = CarryWeightNodeSelectorName

func (ns *nodeSet) getNodes(nodeType NodeType) *sync.Map {
	switch nodeType {
	case DataNodeType:
		return ns.dataNodes
	case MetaNodeType:
		return ns.metaNodes
	default:
		panic("unknown node type")
	}
}

type NodeSelector interface {
	GetName() string
	Select(ns *nodeSet, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error)
}

type weightedNode struct {
	Carry  float64
	Weight float64
	Ptr    Node
	ID     uint64
}

// Node defines an interface that needs to be implemented by weightedNode
type Node interface {
	SelectNodeForWrite()
	GetID() uint64
	GetAddr() string
	GetHeartbeatPort() string
	GetReplicaPort() string
	PartitionCntLimited() bool
	IsActiveNode() bool
	IsWriteAble() bool
	GetPartitionLimitCnt() uint64
	GetTotal() uint64
	GetUsed() uint64
	GetAvailableSpace() uint64
	GetStorageInfo() string
	IsOffline() bool
	GetZoneName() string
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

func canAllocPartition(node Node) bool {
	return node.IsWriteAble() && node.PartitionCntLimited()
}

func asNodeWrap(node interface{}, nodeType NodeType) Node {
	switch nodeType {
	case DataNodeType:
		dataNode := node.(*DataNode)
		return dataNode
	case MetaNodeType:
		metaNode := node.(*MetaNode)
		return metaNode
	default:
		panic("unknown node type")
	}
}

type CarryWeightNodeSelector struct {
	nodeType NodeType

	carry map[uint64]float64
}

func (s *CarryWeightNodeSelector) GetName() string {
	return CarryWeightNodeSelectorName
}

func (s *CarryWeightNodeSelector) prepareCarry(nodes *sync.Map, total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		if _, ok := s.carry[node.GetID()]; !ok {
			// use available space to calculate initial weight
			s.carry[node.GetID()] = float64(node.GetAvailableSpace()) / float64(total)
		}
		return true
	})
}

func (s *CarryWeightNodeSelector) getTotalMax(nodes *sync.Map) (total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		dataNode := value.(Node)
		if dataNode.GetTotal() > total {
			total = dataNode.GetTotal()
		}
		return true
	})
	return
}

func (s *CarryWeightNodeSelector) getCarryNodes(nset *nodeSet, maxTotal uint64, excludeHosts []string) (SortedWeightedNodes, int) {
	var nodes *sync.Map
	switch s.nodeType {
	case DataNodeType:
		nodes = nset.dataNodes
	case MetaNodeType:
		nodes = nset.metaNodes
	default:
		panic("unknown node type")
	}

	nodeTabs := make(SortedWeightedNodes, 0)
	availCount := 0
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		if contains(excludeHosts, node.GetAddr()) {
			// log.LogDebugf("[getAvailCarryDataNodeTab] dataNode [%v] is excludeHosts", dataNode.Addr)
			return true
		}
		if node.IsOffline() {
			log.LogWarnf("[getCarryDataNodes] nodeType (%v) storage info (%v)  exclude hosts(%v) is offline",
				s.nodeType, node.GetStorageInfo(), excludeHosts)
			return true
		}
		if !canAllocPartition(node) {
			log.LogWarnf("[getCarryDataNodes] nodeType (%v) storage info (%v)  exclude hosts(%v)", s.nodeType,
				node.GetStorageInfo(), excludeHosts)
			return true
		}
		if s.carry[node.GetID()] >= 1.0 {
			availCount++
		}

		nt := new(weightedNode)
		nt.Carry = s.carry[node.GetID()]
		nt.Weight = float64(node.GetTotal()-node.GetUsed()) / float64(maxTotal)
		nt.Ptr = node
		nodeTabs = append(nodeTabs, nt)
		return true
	})
	return nodeTabs, availCount
}

func (s *CarryWeightNodeSelector) setNodeCarry(nodes SortedWeightedNodes, availCarryCount, replicaNum int) {
	for availCarryCount < replicaNum {
		availCarryCount = 0
		for _, nt := range nodes {
			carry := nt.Carry + nt.Weight
			// limit the max value of weight
			// prevent subsequent selections make node overloading
			if carry > 10.0 {
				carry = 10.0
			}
			nt.Carry = carry
			s.carry[nt.Ptr.GetID()] = carry
			if carry > 1.0 {
				availCarryCount++
			}
		}
	}
}

func (s *CarryWeightNodeSelector) selectNodeForWrite(node Node) {
	node.SelectNodeForWrite()
	// decrease node weight
	s.carry[node.GetID()] -= 1.0
}

func (s *CarryWeightNodeSelector) Select(ns *nodeSet, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	nodes := ns.getNodes(s.nodeType)
	total := s.getTotalMax(nodes)
	// prepare carry for every nodes
	s.prepareCarry(nodes, total)
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	// if replica == 0, return
	if replicaNum == 0 {
		return
	}
	// if we cannot get enough writable nodes, return error
	weightedNodes, count := s.getCarryNodes(ns, total, excludeHosts)
	if len(weightedNodes) < replicaNum {
		err = fmt.Errorf("action[%s NodeSelector-Select] no enough writable hosts,replicaNum: %d MatchNodeCount:%d",
			s.GetName(), replicaNum, len(weightedNodes))
		return
	}
	// create enough carry nodes
	// we say a node is "carry node", when its carry >= 1.0
	s.setNodeCarry(weightedNodes, count, replicaNum)
	// sort nodes by weight
	sort.Sort(weightedNodes)
	// pick first N nodes
	for i := 0; i < replicaNum; i++ {
		node := weightedNodes[i].Ptr
		s.selectNodeForWrite(node)
		orderHosts = append(orderHosts, node.GetAddr())
		peer := proto.Peer{ID: node.GetID(), Addr: node.GetAddr(), ReplicaPort: node.GetReplicaPort(), HeartbeatPort: node.GetHeartbeatPort()}
		peers = append(peers, peer)
	}
	log.LogInfof("action[%vNodeSelector-Select] peers[%v]", s.GetName(), peers)
	// reshuffle for primary-backup replication
	if newHosts, err = reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf("action[%vNodeSelector-Select] err:%v  orderHosts is nil", s.GetName(), err.Error())
		return
	}
	return
}

func NewCarryWeightNodeSelector(nodeType NodeType) *CarryWeightNodeSelector {
	return &CarryWeightNodeSelector{
		carry:    make(map[uint64]float64),
		nodeType: nodeType,
	}
}

type AvailableSpaceFirstNodeSelector struct {
	nodeType NodeType
}

func (s *AvailableSpaceFirstNodeSelector) getNodeAvailableSpace(node interface{}) uint64 {
	return node.(Node).GetAvailableSpace()
}

func (s *AvailableSpaceFirstNodeSelector) GetName() string {
	return AvailableSpaceFirstNodeSelectorName
}

func (s *AvailableSpaceFirstNodeSelector) Select(ns *nodeSet, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	// if replica == 0, return
	if replicaNum == 0 {
		return
	}
	orderHosts := make([]string, 0)
	nodes := ns.getNodes(s.nodeType)
	sortedNodes := make([]Node, 0)
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		if contains(excludeHosts, node.GetAddr()) {
			return true
		}
		if !canAllocPartition(node) {
			return true
		}
		sortedNodes = append(sortedNodes, node)
		return true
	})

	// if we cannot get enough nodes, return error
	if len(sortedNodes) < replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), replicaNum, len(sortedNodes))
		return
	}
	// sort nodes by available space
	sort.Slice(sortedNodes, func(i, j int) bool {
		return s.getNodeAvailableSpace(sortedNodes[i]) > s.getNodeAvailableSpace(sortedNodes[j])
	})

	// select replica number of nodes, try to avoid multiple replicas of a partition locate on same machine
	// If raftPartitionCanUsingDifferentPort is enabled, sortedNodes may contain nodes with same ip

	// Consider the below case：
	//	Machine1 has 3 dn process (IP1:17310, IP1:18310, IP1:19310)
	//	Machine2 has 2 dn process (IP2:17310, IP2:18310)
	//	Machine3 has 1 dn process (IP3:17310)
	//	1) When create 3 replicas dp, we will select IP1:17310, IP2:17310, IP3:17310, inner loop only be executed once
	//	2) When create 5 replicas dp, we will select IP1:17310, IP2:17310, IP3:17310,IP1:18310, IP2:18310
	//	inner loop be executed twice, first loop select out IP1:17310, IP2:17310, IP3:17310
	//	because nodes with distinct ip can’t satisfy the replica requirement, need second loop , and select out IP1:18310, IP2:18310
	excludedNodes := make([]Node, 0)
	distinctIpSet := make(map[string]struct{})
	// outer loop: select until we get replica number of nodes
	for len(orderHosts) < replicaNum {
		// for each execution of inner loop, select nodes with distinct ip, try to avoid multiple replicas of a partition locate on same machine
		for i := 0; i < len(sortedNodes); i++ {
			node := sortedNodes[i]
			addr := node.GetAddr()
			ipAndPort := strings.Split(addr, ":")
			ip := ipAndPort[0]
			if _, exist := distinctIpSet[ip]; exist {
				excludedNodes = append(excludedNodes, node)
				continue
			}

			distinctIpSet[ip] = struct{}{}
			node.SelectNodeForWrite()
			orderHosts = append(orderHosts, node.GetAddr())
			peer := proto.Peer{ID: node.GetID(), Addr: node.GetAddr(), ReplicaPort: node.GetReplicaPort(), HeartbeatPort: node.GetHeartbeatPort()}
			peers = append(peers, peer)

			if len(orderHosts) == replicaNum {
				break
			}
		}

		// number of nodes with distinct ip can not satisfy replica requirement
		sortedNodes = excludedNodes
		distinctIpSet = make(map[string]struct{})
	}
	// if we cannot get enough writable nodes, return error
	if len(orderHosts) < replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), replicaNum, len(orderHosts))
		return
	}
	log.LogInfof("action[%vNodeSelector-Select] peers[%v]", s.GetName(), peers)
	// reshuffle for primary-backup replication
	if newHosts, err = reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf("action[%vNodeSelector-Select] err:%v  orderHosts is nil", s.GetName(), err.Error())
		return
	}
	return
}

func NewAvailableSpaceFirstNodeSelector(nodeType NodeType) *AvailableSpaceFirstNodeSelector {
	return &AvailableSpaceFirstNodeSelector{
		nodeType: nodeType,
	}
}

type RoundRobinNodeSelector struct {
	index int

	nodeType NodeType
}

func (s *RoundRobinNodeSelector) GetName() string {
	return RoundRobinNodeSelectorName
}

func (s *RoundRobinNodeSelector) Select(ns *nodeSet, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	// if replica == 0, return
	if replicaNum == 0 {
		return
	}
	orderHosts := make([]string, 0)
	nodes := ns.getNodes(s.nodeType)
	sortedNodes := make([]Node, 0)
	nodes.Range(func(key, value interface{}) bool {
		sortedNodes = append(sortedNodes, value.(Node))
		return true
	})
	// if we cannot get enough nodes, return error
	if len(sortedNodes) < replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), replicaNum, len(sortedNodes))
		return
	}
	// sort nodes by id, so we can get a node list that is as stable as possible
	sort.Slice(sortedNodes, func(i, j int) bool {
		return sortedNodes[i].GetID() < sortedNodes[j].GetID()
	})
	nodeIndex := 0
	// pick first N nodes
	for i := 0; i < replicaNum && nodeIndex < len(sortedNodes); i++ {
		selectedIndex := len(sortedNodes)
		// loop until we get a writable node
		for nodeIndex < len(sortedNodes) {
			node := sortedNodes[(nodeIndex+s.index)%len(sortedNodes)]
			nodeIndex += 1
			if canAllocPartition(node) {
				if excludeHosts == nil || !contains(excludeHosts, node.GetAddr()) {
					selectedIndex = nodeIndex - 1
					break
				}
			}
		}
		// if we get a writable node, append it to host list
		if selectedIndex != len(sortedNodes) {
			node := sortedNodes[(selectedIndex+s.index)%len(sortedNodes)]
			orderHosts = append(orderHosts, node.GetAddr())
			node.SelectNodeForWrite()
			peer := proto.Peer{ID: node.GetID(), Addr: node.GetAddr(), ReplicaPort: node.GetReplicaPort(), HeartbeatPort: node.GetHeartbeatPort()}
			peers = append(peers, peer)
		}
	}
	// if we cannot get enough writable nodes, return error
	if len(orderHosts) < replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), replicaNum, len(orderHosts))
		return
	}
	// move the index of selector
	s.index += nodeIndex
	log.LogInfof("action[%vNodeSelector-Select] peers[%v]", s.GetName(), peers)
	// reshuffle for primary-backup replication
	if newHosts, err = reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf("action[%vNodeSelector-Select] err:%v  orderHosts is nil", s.GetName(), err.Error())
		return
	}
	return
}

func NewRoundRobinNodeSelector(nodeType NodeType) *RoundRobinNodeSelector {
	return &RoundRobinNodeSelector{
		nodeType: nodeType,
	}
}

const (
	StrawNodeSelectorRandMax = 65536
)

// NOTE: this node selector inspired by Straw2 algorithm, which is widely used in ceph
type StrawNodeSelector struct {
	rand     *rand.Rand
	nodeType NodeType
}

func (s *StrawNodeSelector) GetName() string {
	return StrawNodeSelectorName
}

func (s *StrawNodeSelector) getWeight(node Node) float64 {
	return float64(node.GetAvailableSpace()) / util.GB
}

// select a node with max straw and it's ip didn't exist in excludedIpSet
func (s *StrawNodeSelector) selectOneNode(nodes []Node, excludedIpSet map[string]struct{}) (index int, maxNode Node) {
	maxStraw := float64(0)
	maxStrawNodeIp := ""
	index = -1
	for i, node := range nodes {
		addr := node.GetAddr()
		ipAndPort := strings.Split(addr, ":")
		ip := ipAndPort[0]

		if _, ok := excludedIpSet[ip]; ok {
			continue
		}

		straw := float64(s.rand.Intn(StrawNodeSelectorRandMax))
		straw = math.Log(straw/float64(StrawNodeSelectorRandMax)) / s.getWeight(node)
		if index == -1 || straw > maxStraw {
			maxStraw = straw
			maxNode = node
			index = i
			maxStrawNodeIp = ip
		}
	}
	if index != -1 {
		excludedIpSet[maxStrawNodeIp] = struct{}{}
	}

	return
}

func (s *StrawNodeSelector) Select(ns *nodeSet, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	nodes := make([]Node, 0)
	ns.getNodes(s.nodeType).Range(func(key, value interface{}) bool {
		node := asNodeWrap(value, s.nodeType)
		if contains(excludeHosts, node.GetAddr()) {
			return true
		}
		if !canAllocPartition(node) {
			return true
		}
		nodes = append(nodes, node)
		return true
	})

	if len(nodes) < replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), replicaNum, len(nodes))
		return
	}

	distinctIpSet := make(map[string]struct{})
	orderHosts := make([]string, 0)

	// select replica number of nodes, try to avoid multiple replicas of a partition locate on same machine
	// If raftPartitionCanUsingDifferentPort is enabled, candidate nodes may contain nodes with same ip

	// outer loop: select until we get replica number of nodes
	for len(orderHosts) < replicaNum {
		// for each execution of inner loop, select nodes with distinct ip, try to avoid multiple replicas of a partition locate on same machine
		for {
			index, node := s.selectOneNode(nodes, distinctIpSet)
			if index == -1 {
				break
			}

			if index != 0 {
				nodes[0], nodes[index] = node, nodes[0]
			}
			nodes = nodes[1:]

			orderHosts = append(orderHosts, node.GetAddr())
			node.SelectNodeForWrite()
			peer := proto.Peer{ID: node.GetID(), Addr: node.GetAddr(), ReplicaPort: node.GetReplicaPort(), HeartbeatPort: node.GetHeartbeatPort()}
			peers = append(peers, peer)
			if len(orderHosts) == replicaNum {
				break
			}
		}
		// number of nodes with distinct ip can not satisfy replica requirement
		distinctIpSet = make(map[string]struct{})
	}

	// if we cannot get enough writable nodes, return error
	if len(orderHosts) < replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), replicaNum, len(orderHosts))
		return
	}
	log.LogInfof("action[%vNodeSelector-Select] peers[%v]", s.GetName(), peers)
	// reshuffle for primary-backup replication
	if newHosts, err = reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf("action[%vNodeSelector-Select] err:%v  orderHosts is nil", s.GetName(), err.Error())
		return
	}
	return
}

func NewStrawNodeSelector(nodeType NodeType) *StrawNodeSelector {
	return &StrawNodeSelector{
		rand:     rand.New(rand.NewSource(time.Now().UnixMicro())),
		nodeType: nodeType,
	}
}

func NewNodeSelector(name string, nodeType NodeType) NodeSelector {
	switch name {
	case RoundRobinNodeSelectorName:
		return NewRoundRobinNodeSelector(nodeType)
	case CarryWeightNodeSelectorName:
		return NewCarryWeightNodeSelector(nodeType)
	case AvailableSpaceFirstNodeSelectorName:
		return NewAvailableSpaceFirstNodeSelector(nodeType)
	case StrawNodeSelectorName:
		return NewStrawNodeSelector(nodeType)
	default:
		return NewCarryWeightNodeSelector(nodeType)
	}
}

func (ns *nodeSet) getAvailMetaNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	ns.nodeSelectLock.Lock()
	defer ns.nodeSelectLock.Unlock()
	// we need a read lock to block the modification of node selector
	ns.metaNodeSelectorLock.RLock()
	defer ns.metaNodeSelectorLock.RUnlock()
	return ns.metaNodeSelector.Select(ns, excludeHosts, replicaNum)
}

func (ns *nodeSet) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	ns.nodeSelectLock.Lock()
	defer ns.nodeSelectLock.Unlock()
	// we need a read lock to block the modification of node selector
	ns.dataNodeSelectorLock.Lock()
	defer ns.dataNodeSelectorLock.Unlock()
	return ns.dataNodeSelector.Select(ns, excludeHosts, replicaNum)
}
