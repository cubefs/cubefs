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
	case MetaNodeType, RocksdbType:
		return ns.metaNodes
	default:
		panic("unknown node type")
	}
}

type NodeSelector interface {
	GetName() string
	Select(ns *nodeSet, param *selectParam) (newHosts []string, peers []proto.Peer, err error)
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
	PartitionCntLimitedEx(threshold float64) bool
	IsActiveNode() bool
	IsWriteAbleEx(threshold float64) bool
	GetPartitionLimitCnt() uint64
	GetTotal() uint64
	GetUsed() uint64
	GetAvailableSpace() uint64
	GetStorageInfo() string
	IsOffline() bool
	GetZoneName() string
	GetSelectTag() string
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

func canAllocPartition(node Node, nodeType NodeType, threshold float64) bool {
	if nodeType == RocksdbType {
		metaNode := node.(*MetaNode)
		return metaNode.IsRocksdbWriteAble() && metaNode.PartitionCntLimitedEx(threshold)
	}
	return node.IsWriteAbleEx(threshold) && node.PartitionCntLimitedEx(threshold)
}

func asNodeWrap(node interface{}, nodeType NodeType) Node {
	switch nodeType {
	case DataNodeType:
		dataNode := node.(*DataNode)
		return dataNode
	case MetaNodeType, RocksdbType:
		metaNode := node.(*MetaNode)
		return metaNode
	default:
		panic("unknown node type")
	}
}

type CarryWeightNodeSelector struct {
	nodeType NodeType

	carry map[uint64]float64
	sync.RWMutex
}

func (s *CarryWeightNodeSelector) GetName() string {
	return CarryWeightNodeSelectorName
}

func (s *CarryWeightNodeSelector) prepareCarry(nodes *sync.Map, total uint64) {
	switch s.nodeType {
	case DataNodeType:
		s.prepareCarryForDataNodes(nodes, total)
	case MetaNodeType, RocksdbType:
		s.prepareCarryForMetaNodes(nodes, total)
	default:
	}
}

func (s *CarryWeightNodeSelector) getTotalMax(nodes *sync.Map) (total uint64) {
	switch s.nodeType {
	case DataNodeType:
		total = s.getTotalMaxForDataNodes(nodes)
	case MetaNodeType, RocksdbType:
		total = s.getTotalMaxForMetaNodes(nodes)
	default:
	}
	return
}

func (s *CarryWeightNodeSelector) getCarryNodes(nset *nodeSet, maxTotal uint64, param *selectParam) (SortedWeightedNodes, int) {
	var nodes *sync.Map
	switch s.nodeType {
	case DataNodeType:
		nodes = nset.dataNodes
	case MetaNodeType, RocksdbType:
		nodes = nset.metaNodes
	default:
		panic("unknown node type")
	}

	nodeTabs := make(SortedWeightedNodes, 0)
	availCount := 0
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		if contains(param.excludeHosts, node.GetAddr()) {
			// log.LogDebugf("[getAvailCarryDataNodeTab] dataNode [%v] is excludeHosts", dataNode.Addr)
			return true
		}
		if node.IsOffline() {
			log.LogWarnf("[getCarryDataNodes] nodeType (%v) storage info (%v)  exclude hosts(%v)", s.nodeType, node.GetStorageInfo(), param.excludeHosts)
			return true
		}

		if !canAllocPartition(node, s.nodeType, param.threshold) {
			log.LogWarnf("[getCarryDataNodes] nodeType (%v) storage info (%v)  exclude hosts(%v)", s.nodeType,
				node.GetStorageInfo(), param.excludeHosts)
			return true
		}

		if param.selectType == proto.SelectTypeTag && param.selectTag != node.GetSelectTag() {
			return true
		}

		s.RLock()
		if s.carry[node.GetID()] >= 1.0 {
			availCount++
		}
		s.RUnlock()

		nt := new(weightedNode)
		s.RLock()
		nt.Carry = s.carry[node.GetID()]
		s.RUnlock()
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
			s.Lock()
			s.carry[nt.Ptr.GetID()] = carry
			s.Unlock()
			if carry > 1.0 {
				availCarryCount++
			}
		}
	}
}

func (s *CarryWeightNodeSelector) selectNodeForWrite(node Node) {
	node.SelectNodeForWrite()
	// decrease node weight
	s.Lock()
	s.carry[node.GetID()] -= 1.0
	s.Unlock()
}

func (s *CarryWeightNodeSelector) Select(ns *nodeSet, param *selectParam) (newHosts []string, peers []proto.Peer, err error) {
	nodes := ns.getNodes(s.nodeType)
	total := s.getTotalMax(nodes)
	// prepare carry for every nodes
	s.prepareCarry(nodes, total)
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	// if replica == 0, return
	if param.replicaNum == 0 {
		return
	}
	// if we cannot get enough writable nodes, return error
	weightedNodes, count := s.getCarryNodes(ns, total, param)
	if len(weightedNodes) < param.replicaNum {
		err = fmt.Errorf("action[%s NodeSelector-Select] no enough writable hosts,replicaNum: %d MatchNodeCount:%d, selectTag: %s",
			s.GetName(), param.replicaNum, len(weightedNodes), param.selectTag)
		return
	}
	// create enough carry nodes
	// we say a node is "carry node", when its carry >= 1.0
	s.setNodeCarry(weightedNodes, count, param.replicaNum)
	// sort nodes by weight
	sort.Sort(weightedNodes)
	// pick first N nodes
	for i := 0; i < param.replicaNum; i++ {
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
	switch s.nodeType {
	case DataNodeType:
		dataNode := node.(*DataNode)
		return dataNode.AvailableSpace
	case MetaNodeType:
		metaNode := node.(*MetaNode)
		return metaNode.Total - metaNode.Used
	case RocksdbType:
		metaNode := node.(*MetaNode)
		return metaNode.GetRocksdbTotal() - metaNode.GetRocksdbUsed()
	default:
		panic("unknown node type")
	}
}

func (s *AvailableSpaceFirstNodeSelector) GetName() string {
	return AvailableSpaceFirstNodeSelectorName
}

func (s *AvailableSpaceFirstNodeSelector) Select(ns *nodeSet, param *selectParam) (newHosts []string, peers []proto.Peer, err error) {
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	// if replica == 0, return
	if param.replicaNum == 0 {
		return
	}
	orderHosts := make([]string, 0)
	nodes := ns.getNodes(s.nodeType)
	sortedNodes := make([]Node, 0)
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		if contains(param.excludeHosts, node.GetAddr()) {
			return true
		}
		if !canAllocPartition(node, s.nodeType, param.threshold) {
			return true
		}
		if param.selectType == proto.SelectTypeTag && param.selectTag != node.GetSelectTag() {
			return true
		}
		sortedNodes = append(sortedNodes, node)
		return true
	})

	// if we cannot get enough nodes, return error
	if len(sortedNodes) < param.replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough hosts,replicaNum:%v  MatchNodeCount:%v  ",
			s.GetName(), param.replicaNum, len(sortedNodes))
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
	for len(orderHosts) < param.replicaNum {
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

			if len(orderHosts) == param.replicaNum {
				break
			}
		}

		// number of nodes with distinct ip can not satisfy replica requirement
		sortedNodes = excludedNodes
		distinctIpSet = make(map[string]struct{})
	}
	// if we cannot get enough writable nodes, return error
	if len(orderHosts) < param.replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v selectTag: %s",
			s.GetName(), param.replicaNum, len(orderHosts), param.selectTag)
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

func (s *RoundRobinNodeSelector) Select(ns *nodeSet, param *selectParam) (newHosts []string, peers []proto.Peer, err error) {
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	// if replica == 0, return
	if param.replicaNum == 0 {
		return
	}
	orderHosts := make([]string, 0)
	nodes := ns.getNodes(s.nodeType)
	sortedNodes := make([]Node, 0)
	nodes.Range(func(key, value interface{}) bool {
		if param.selectType == proto.SelectTypeTag && param.selectTag != value.(Node).GetSelectTag() {
			return true
		}
		sortedNodes = append(sortedNodes, value.(Node))
		return true
	})
	// if we cannot get enough nodes, return error
	if len(sortedNodes) < param.replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v selectTag: %s",
			s.GetName(), param.replicaNum, len(sortedNodes), param.selectTag)
		return
	}
	// sort nodes by id, so we can get a node list that is as stable as possible
	sort.Slice(sortedNodes, func(i, j int) bool {
		return sortedNodes[i].GetID() < sortedNodes[j].GetID()
	})
	nodeIndex := 0
	// pick first N nodes
	for i := 0; i < param.replicaNum && nodeIndex < len(sortedNodes); i++ {
		selectedIndex := len(sortedNodes)
		// loop until we get a writable node
		for nodeIndex < len(sortedNodes) {
			node := sortedNodes[(nodeIndex+s.index)%len(sortedNodes)]
			nodeIndex += 1
			if canAllocPartition(node, s.nodeType, param.threshold) {
				if param.excludeHosts == nil || !contains(param.excludeHosts, node.GetAddr()) {
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
	if len(orderHosts) < param.replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v selectTag: %s",
			s.GetName(), param.replicaNum, len(orderHosts), param.selectTag)
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
	switch s.nodeType {
	case DataNodeType:
		dataNode := node.(*DataNode)
		return float64(dataNode.AvailableSpace) / util.GB
	case MetaNodeType:
		metaNode := node.(*MetaNode)
		return float64(metaNode.Total-metaNode.Used) / util.GB
	case RocksdbType:
		metaNode := node.(*MetaNode)
		return float64(metaNode.GetRocksdbTotal()-metaNode.GetRocksdbUsed()) / util.GB
	default:
		panic("unknown node type")
	}
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

func (s *StrawNodeSelector) Select(ns *nodeSet, param *selectParam) (newHosts []string, peers []proto.Peer, err error) {
	nodes := make([]Node, 0)
	ns.getNodes(s.nodeType).Range(func(key, value interface{}) bool {
		node := asNodeWrap(value, s.nodeType)
		if contains(param.excludeHosts, node.GetAddr()) {
			return true
		}
		if !canAllocPartition(node, s.nodeType, param.threshold) {
			return true
		}
		if param.selectType == proto.SelectTypeTag && param.selectTag != node.GetSelectTag() {
			return true
		}
		nodes = append(nodes, node)
		return true
	})

	if len(nodes) < param.replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v selectTag: %s",
			s.GetName(), param.replicaNum, len(nodes), param.selectTag)
		return
	}

	distinctIpSet := make(map[string]struct{})
	orderHosts := make([]string, 0)

	// select replica number of nodes, try to avoid multiple replicas of a partition locate on same machine
	// If raftPartitionCanUsingDifferentPort is enabled, candidate nodes may contain nodes with same ip

	// outer loop: select until we get replica number of nodes
	for len(orderHosts) < param.replicaNum {
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
			if len(orderHosts) == param.replicaNum {
				break
			}
		}
		// number of nodes with distinct ip can not satisfy replica requirement
		distinctIpSet = make(map[string]struct{})
	}

	// if we cannot get enough writable nodes, return error
	if len(orderHosts) < param.replicaNum {
		err = fmt.Errorf("action[%vNodeSelector-Select] no enough writable hosts,replicaNum:%v  MatchNodeCount:%v selectTag: %s",
			s.GetName(), param.replicaNum, len(orderHosts), param.selectTag)
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

func (ns *nodeSet) getRackSets() nodeSetCollection {
	ns.racksLock.RLock()
	defer ns.racksLock.RUnlock()

	rsets := make(nodeSetCollection, 0, len(ns.racks))
	for _, rack := range ns.racks {
		rsets = append(rsets, rack)
	}
	return rsets
}

func (ns *nodeSet) getAvailMetaNodeHosts(param *selectParam, storeMode proto.StoreMode) (newHosts []string, peers []proto.Peer, err error) {
	ns.nodeSelectLock.Lock()
	defer ns.nodeSelectLock.Unlock()
	// we need a read lock to block the modification of node selector
	ns.metaNodeSelectorLock.RLock()
	defer ns.metaNodeSelectorLock.RUnlock()

	nodeType := MetaNodeType
	if storeMode == proto.StoreModeRocksDb {
		nodeType = RocksdbType
	}

	switch param.thresholdType {
	case proto.SelectType_Normal:
		param.threshold = 1
	case proto.SelectType_DistributionOptimization:
		param.threshold = getDistributionOptimizationThreshold()
	}

	// If rack isolation is not enabled, use non-rack-aware selector directly
	if param.rackLevel == proto.RackAwareNone {
		return ns.getNodeSelector(nodeType, storeMode).Select(ns, param)
	}

	// Rack isolation enabled, prioritize strong constraint mode
	return ns.selectNodesWithRack(param, nodeType, storeMode)
}

// selectMetaNodesWithRack selects meta nodes with rack awareness
func (ns *nodeSet) selectNodesWithRack(param *selectParam, nodeType NodeType, storeMode proto.StoreMode) (newHosts []string, peers []proto.Peer, err error) {
	rsets := ns.getRackSets()

	paramCopy := param.copy()
	// First attempt with strong rack awareness
	paramCopy.rackLevel = proto.RackAwareStrong
	paramCopy.replicaNum = 1

	for {

		rack, err := ns.getRackSelector(nodeType).Select(rsets, paramCopy)
		if err != nil {
			// if rack aware is not enabled or alreay weak aware, return error
			if param.rackLevel == proto.RackAwareStrong || paramCopy.rackLevel == proto.RackAwareWeak {
				return nil, nil, fmt.Errorf("strong rack aware selection failed for rack[%v], param[%v], err: %v",
					rack, paramCopy.String(), err)
			}

			log.LogWarnf("action[getAvailMetaNodeHosts] weak rack aware selection failed for rack[%v], param %v, err: %v",
				rack, paramCopy.String(), err.Error())
			paramCopy.rackLevel = proto.RackAwareWeak
			continue
		}

		// Select nodes
		selector := rack.getNodeSelector(nodeType, storeMode)

		rhosts, rpeers, err := selector.Select(rack, paramCopy)
		if err != nil {
			log.LogErrorf("action[getAvailMetaNodeHosts] node selection failed for rack[%v], param[%v], err: %v",
				rack, paramCopy.String(), err.Error())
			return nil, nil, fmt.Errorf("node selection failed for rack[%v], param[%v], err: %v",
				rack.Rack, paramCopy.String(), err.Error())
		}

		// Update results
		newHosts = append(newHosts, rhosts...)
		peers = append(peers, rpeers...)
		paramCopy.excludeHosts = append(paramCopy.excludeHosts, rhosts...)
		paramCopy.excludeRacks = append(paramCopy.excludeRacks, rack.Rack)

		// Check if replica number requirement is met
		if len(newHosts) >= param.replicaNum {
			return newHosts, peers, nil
		}
	}
}

func (ns *nodeSet) getAvailDataNodeHosts(param *selectParam) (hosts []string, peers []proto.Peer, err error) {
	ns.nodeSelectLock.Lock()
	defer ns.nodeSelectLock.Unlock()
	// we need a read lock to block the modification of node selector
	ns.dataNodeSelectorLock.Lock()
	defer ns.dataNodeSelectorLock.Unlock()

	switch param.thresholdType {
	case proto.SelectType_Normal:
		param.threshold = 1
	case proto.SelectType_DistributionOptimization:
		param.threshold = getDistributionOptimizationThreshold()
	}

	if param.rackLevel == proto.RackAwareNone {
		return ns.dataNodeSelector.Select(ns, param)
	}

	return ns.selectNodesWithRack(param, DataNodeType, proto.StoreModeDef)
}

func (s *CarryWeightNodeSelector) prepareCarryForDataNodes(nodes *sync.Map, total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		node := value.(Node)
		s.Lock()
		if _, ok := s.carry[node.GetID()]; !ok {
			// use available space to calculate initial weight
			s.carry[node.GetID()] = float64(node.GetAvailableSpace()) / float64(total)
		}
		s.Unlock()
		return true
	})
}

func (s *CarryWeightNodeSelector) prepareCarryForMetaNodeMemory(nodes *sync.Map, total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		s.Lock()
		if _, ok := s.carry[metaNode.ID]; !ok {
			// use available space to calculate initial weight
			s.carry[metaNode.ID] = float64(metaNode.Total-metaNode.Used) / float64(total)
		}
		s.Unlock()
		return true
	})
}

func (s *CarryWeightNodeSelector) prepareCarryForMetaNodeRocksdb(nodes *sync.Map, total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		s.Lock()
		if _, ok := s.carry[metaNode.ID]; !ok {
			// use available space to calculate initial weight
			s.carry[metaNode.ID] = float64(metaNode.GetRocksdbTotal()-metaNode.GetRocksdbUsed()) / float64(total)
		}
		s.Unlock()
		return true
	})
}

func (s *CarryWeightNodeSelector) prepareCarryForMetaNodes(nodes *sync.Map, total uint64) {
	switch s.nodeType {
	case MetaNodeType:
		s.prepareCarryForMetaNodeMemory(nodes, total)
	case RocksdbType:
		s.prepareCarryForMetaNodeRocksdb(nodes, total)
	default:
	}
}

func (s *CarryWeightNodeSelector) getTotalMaxForMetaNodeMemory(nodes *sync.Map) (total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		dataNode := value.(Node)
		if dataNode.GetTotal() > total {
			total = dataNode.GetTotal()
		}
		return true
	})
	return
}

func (s *CarryWeightNodeSelector) getTotalMaxForMetaNodeRocksdb(nodes *sync.Map) (total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		nodeTotal := metaNode.GetRocksdbTotal()
		if nodeTotal > total {
			total = nodeTotal
		}
		return true
	})
	return
}

func (s *CarryWeightNodeSelector) getTotalMaxForMetaNodes(nodes *sync.Map) (total uint64) {
	switch s.nodeType {
	case MetaNodeType:
		return s.getTotalMaxForMetaNodeMemory(nodes)
	case RocksdbType:
		return s.getTotalMaxForMetaNodeRocksdb(nodes)
	}
	return
}

func (s *CarryWeightNodeSelector) getTotalMaxForDataNodes(nodes *sync.Map) (total uint64) {
	nodes.Range(func(key, value interface{}) bool {
		dataNode := value.(Node)
		if dataNode.GetTotal() > total {
			total = dataNode.GetTotal()
		}
		return true
	})
	return
}
