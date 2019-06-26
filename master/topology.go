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
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"sort"
	"sync"
)

type topology struct {
	setIndex   int
	dataNodes  sync.Map
	metaNodes  sync.Map
	nodeSetMap map[uint64]*nodeSet
	nsLock     sync.RWMutex
}

func newTopology() (t *topology) {
	t = new(topology)
	t.nodeSetMap = make(map[uint64]*nodeSet)
	return
}

type topoDataNode struct {
	*DataNode
	setID uint64
}

func newTopoDataNode(dataNode *DataNode, setID uint64) *topoDataNode {
	return &topoDataNode{
		DataNode: dataNode,
		setID:    setID,
	}
}

type topoMetaNode struct {
	*MetaNode
	setID uint64
}

func newTopoMetaNode(metaNode *MetaNode, setID uint64) *topoMetaNode {
	return &topoMetaNode{
		MetaNode: metaNode,
		setID:    setID,
	}
}

func (t *topology) replaceDataNode(dataNode *DataNode) {
	if oldRack, err := t.getRack(dataNode); err == nil {
		oldRack.putDataNode(dataNode)
	}
	topoNode, ok := t.dataNodes.Load(dataNode.Addr)
	if ok {
		node := topoNode.(*topoDataNode)
		node.RackName = dataNode.RackName
		t.putDataNodeToCache(node)
	}
}

func (t *topology) putDataNode(dataNode *DataNode) (err error) {

	if _, ok := t.dataNodes.Load(dataNode.Addr); ok {
		return
	}
	var ns *nodeSet
	if ns, err = t.getNodeSet(dataNode.NodeSetID); err != nil {
		log.LogErrorf("action[putDataNode] nodeSet[%v] not found", dataNode.NodeSetID)
		return
	}
	ns.putDataNode(dataNode)
	node := newTopoDataNode(dataNode, ns.ID)
	t.putDataNodeToCache(node)
	return
}

func (t *topology) putDataNodeToCache(dataNode *topoDataNode) {
	t.dataNodes.Store(dataNode.Addr, dataNode)
}

func (t *topology) deleteDataNode(dataNode *DataNode) {
	t.dataNodes.Delete(dataNode.Addr)
	ns, err := t.getNodeSet(dataNode.NodeSetID)
	if err != nil {
		return
	}
	ns.deleteDataNode(dataNode)
}

func (t *topology) getRack(dataNode *DataNode) (rack *Rack, err error) {
	topoNode, ok := t.dataNodes.Load(dataNode.Addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(dataNode.Addr), "%v not found", dataNode.Addr)
	}
	node := topoNode.(*topoDataNode)
	ns, err := t.getNodeSet(node.setID)
	if err != nil {
		return
	}
	return ns.getRack(node.RackName)
}

func (t *topology) getAvailNodeSetForDataNode() (nset *nodeSet) {
	allNodeSet := t.getAllNodeSet()
	for _, ns := range allNodeSet {
		if ns.dataNodeLen < ns.Capacity {
			nset = ns
			return
		}
	}
	return
}

func (t *topology) putMetaNode(metaNode *MetaNode) (err error) {
	if _, ok := t.metaNodes.Load(metaNode.Addr); ok {
		return
	}
	var ns *nodeSet
	if ns, err = t.getNodeSet(metaNode.NodeSetID); err != nil {
		return
	}
	ns.putMetaNode(metaNode)
	node := newTopoMetaNode(metaNode, ns.ID)
	t.putMetaNodeToCache(node)
	return
}

func (t *topology) deleteMetaNode(metaNode *MetaNode) {
	t.metaNodes.Delete(metaNode.Addr)
	ns, err := t.getNodeSet(metaNode.NodeSetID)
	if err != nil {
		return
	}
	ns.deleteMetaNode(metaNode)
}

func (t *topology) putMetaNodeToCache(metaNode *topoMetaNode) {
	t.metaNodes.Store(metaNode.Addr, metaNode)
}

func (t *topology) getAvailNodeSetForMetaNode() (nset *nodeSet) {
	allNodeSet := t.getAllNodeSet()
	sort.Sort(sort.Reverse(allNodeSet))
	for _, ns := range allNodeSet {
		if ns.metaNodeLen < ns.Capacity {
			nset = ns
			return
		}
	}
	return
}

func (t *topology) createNodeSet(c *Cluster) (ns *nodeSet, err error) {
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		return
	}
	ns = newNodeSet(id, c.cfg.nodeSetCapacity)
	if err = c.syncAddNodeSet(ns); err != nil {
		return
	}
	t.putNodeSet(ns)
	return
}

func (t *topology) putNodeSet(ns *nodeSet) {
	t.nsLock.Lock()
	defer t.nsLock.Unlock()
	t.nodeSetMap[ns.ID] = ns
}

func (t *topology) getNodeSet(setID uint64) (ns *nodeSet, err error) {
	t.nsLock.RLock()
	defer t.nsLock.RUnlock()
	ns, ok := t.nodeSetMap[setID]
	if !ok {
		return nil, errors.NewErrorf("set %v not found", setID)
	}
	return
}

func (t *topology) getAllNodeSet() (nsc nodeSetCollection) {
	t.nsLock.RLock()
	defer t.nsLock.RUnlock()
	nsc = make(nodeSetCollection, 0)
	for _, ns := range t.nodeSetMap {
		nsc = append(nsc, ns)
	}
	return
}

func (t *topology) allocNodeSetForDataNode(replicaNum uint8) (ns *nodeSet, err error) {
	nset := t.getAllNodeSet()
	if nset == nil {
		return nil, proto.ErrNoNodeSetToCreateDataPartition
	}
	for i := 0; i < len(nset); i++ {
		if t.setIndex >= len(nset) {
			t.setIndex = 0
		}
		ns = nset[t.setIndex]
		t.setIndex++
		if ns.canWriteForDataNode(int(replicaNum)) {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[allocNodeSetForDataNode],err:%v", proto.ErrNoNodeSetToCreateDataPartition))
	return nil, proto.ErrNoNodeSetToCreateDataPartition
}

func (t *topology) allocNodeSetForMetaNode(replicaNum uint8) (ns *nodeSet, err error) {
	nset := t.getAllNodeSet()
	if nset == nil {
		return nil, proto.ErrNoNodeSetToCreateMetaPartition
	}
	for i := 0; i < len(nset); i++ {
		if t.setIndex >= len(nset) {
			t.setIndex = 0
		}
		ns = nset[t.setIndex]
		t.setIndex++
		if ns.canWriteForMetaNode(int(replicaNum)) {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[allocNodeSetForMetaNode],err:%v", proto.ErrNoNodeSetToCreateMetaPartition))
	return nil, proto.ErrNoNodeSetToCreateMetaPartition
}

type nodeSetCollection []*nodeSet

func (nsc nodeSetCollection) Len() int {
	return len(nsc)
}

func (nsc nodeSetCollection) Less(i, j int) bool {
	return nsc[i].metaNodeLen < nsc[j].metaNodeLen
}

func (nsc nodeSetCollection) Swap(i, j int) {
	nsc[i], nsc[j] = nsc[j], nsc[i]
}

type nodeSet struct {
	ID          uint64
	Capacity    int
	rackIndex   int
	rackMap     map[string]*Rack
	racks       []string
	rackLock    sync.RWMutex
	dataNodeLen int
	metaNodeLen int
	metaNodes   sync.Map
	dataNodes   sync.Map
	sync.RWMutex
}

func newNodeSet(id uint64, cap int) *nodeSet {
	ns := &nodeSet{
		ID:       id,
		Capacity: cap,
	}
	ns.rackMap = make(map[string]*Rack)
	ns.racks = make([]string, 0)
	return ns
}

func (ns *nodeSet) increaseDataNodeLen() {
	ns.Lock()
	defer ns.Unlock()
	ns.dataNodeLen++
}

func (ns *nodeSet) decreaseDataNodeLen() {
	ns.Lock()
	defer ns.Unlock()
	ns.dataNodeLen--
}

func (ns *nodeSet) increaseMetaNodeLen() {
	ns.Lock()
	defer ns.Unlock()
	ns.metaNodeLen++
}

func (ns *nodeSet) decreaseMetaNodeLen() {
	ns.Lock()
	defer ns.Unlock()
	ns.metaNodeLen--
}

func (ns *nodeSet) putMetaNode(metaNode *MetaNode) {
	ns.metaNodes.Store(metaNode.Addr, metaNode)
}

func (ns *nodeSet) deleteMetaNode(metaNode *MetaNode) {
	ns.metaNodes.Delete(metaNode.Addr)
}

func (ns *nodeSet) canWriteForDataNode(replicaNum int) bool {
	log.LogInfof("canWriteForDataNode dataLen[%v] replicaNum[%v]", ns.dataNodeLen, replicaNum)
	if ns.dataNodeLen < int(replicaNum) {
		return false
	}
	var count int
	ns.dataNodes.Range(func(key, value interface{}) bool {
		node := value.(*DataNode)
		if node.isWriteAble() {
			count++
		}
		if count >= replicaNum {
			return false
		}
		return true
	})
	log.LogInfof("canWriteForDataNode count[%v] replicaNum[%v]", count, replicaNum)
	return count >= replicaNum
}

func (ns *nodeSet) canWriteForMetaNode(replicaNum int) bool {
	log.LogInfof("canWriteForMetaNode metaLen[%v] replicaNum[%v]", ns.metaNodeLen, replicaNum)
	if ns.metaNodeLen < replicaNum {
		return false
	}
	var count int
	ns.metaNodes.Range(func(key, value interface{}) bool {
		node := value.(*MetaNode)
		if node.isWritable() {
			count++
		}
		if count >= replicaNum {
			return false
		}
		return true
	})
	log.LogInfof("canWriteForMetaNode count[%v] replicaNum[%v]", count, replicaNum)
	return count >= replicaNum
}

func (ns *nodeSet) isSingleRack() bool {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	return len(ns.rackMap) == 1
}

func (ns *nodeSet) getRack(name string) (rack *Rack, err error) {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	rack, ok := ns.rackMap[name]
	if !ok {
		return nil, errors.Trace(rackNotFound(name), "%v not found", name)
	}
	return
}

func (ns *nodeSet) putRack(rack *Rack) *Rack {
	ns.rackLock.Lock()
	defer ns.rackLock.Unlock()
	oldRack, ok := ns.rackMap[rack.name]
	if ok {
		return oldRack
	}
	ns.rackMap[rack.name] = rack
	if ok := ns.isExist(rack.name); !ok {
		ns.racks = append(ns.racks, rack.name)
	}
	return rack
}

func (ns *nodeSet) isExist(rackName string) (ok bool) {
	for _, name := range ns.racks {
		if name == rackName {
			ok = true
			return
		}
	}
	return
}

func (ns *nodeSet) removeRack(name string) {
	ns.rackLock.Lock()
	defer ns.rackLock.Unlock()
	delete(ns.rackMap, name)
}

func (ns *nodeSet) putDataNode(dataNode *DataNode) {
	dataNode.RackName = DefaultRackName
	rack, err := ns.getRack(dataNode.RackName)
	if err != nil {
		rack = newRack(dataNode.RackName)
		rack = ns.putRack(rack)
	}
	rack.putDataNode(dataNode)
	ns.dataNodes.Store(dataNode.Addr, dataNode)
}

func (ns *nodeSet) deleteDataNode(dataNode *DataNode) {
	ns.dataNodes.Delete(dataNode.Addr)
	rack, err := ns.getRack(dataNode.RackName)
	if err != nil {
		return
	}
	rack.dataNodes.Delete(dataNode.Addr)
}

func (ns *nodeSet) getAllRacks() (racks []*Rack) {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	racks = make([]*Rack, 0)
	for _, rack := range ns.rackMap {
		racks = append(racks, rack)
	}
	return
}

func (ns *nodeSet) getRackNameByIndex(index int) (rName string) {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	rName = ns.racks[index]
	return
}

func (ns *nodeSet) allocRacks(replicaNum int, excludeRack []string) (racks []*Rack, err error) {
	racks = make([]*Rack, 0)
	if excludeRack == nil {
		excludeRack = make([]string, 0)
	}
	racks = ns.getAllRacks()
	if ns.isSingleRack() {
		return racks, nil
	}

	for i := 0; i < len(racks); i++ {
		if ns.rackIndex >= len(racks) {
			ns.rackIndex = 0
		}
		rName := ns.getRackNameByIndex(ns.rackIndex)
		if contains(excludeRack, rName) {
			ns.rackIndex++
			continue
		}
		var rack *Rack
		if rack, err = ns.getRack(ns.racks[ns.rackIndex]); err != nil {
			ns.rackIndex++
			continue
		}
		ns.rackIndex++

		if rack.canWrite(1) {
			racks = append(racks, rack)
		}
		if len(racks) >= int(replicaNum) {
			break
		}
	}
	if len(racks) == 0 {
		log.LogError(fmt.Sprintf("action[allocRacks],err:%v", proto.ErrNoRackToCreateDataPartition))
		return nil, proto.ErrNoRackToCreateDataPartition
	}
	if len(racks) > int(replicaNum) {
		racks = racks[:int(replicaNum)]
	}
	err = nil
	return
}

// Rack stores all the rack related information
type Rack struct {
	name      string
	dataNodes sync.Map
	sync.RWMutex
}

func newRack(name string) (rack *Rack) {
	return &Rack{name: name}
}

func (rack *Rack) putDataNode(dataNode *DataNode) {
	rack.dataNodes.Store(dataNode.Addr, dataNode)
}

func (rack *Rack) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := rack.dataNodes.Load(addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
	}
	dataNode = value.(*DataNode)
	return
}
func (rack *Rack) removeDataNode(addr string) {
	rack.dataNodes.Delete(addr)
}

func (rack *Rack) canWrite(replicaNum uint8) (can bool) {
	rack.RLock()
	defer rack.RUnlock()
	var leastAlive uint8
	rack.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.isActive == true && dataNode.isWriteAble() == true {
			leastAlive++
		}
		if leastAlive >= replicaNum {
			can = true
			return false
		}
		return true
	})
	return
}

func (rack *Rack) getDataNodeMaxTotal() (maxTotal uint64) {
	rack.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (rack *Rack) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := rack.getDataNodeMaxTotal()
	nodeTabs, availCarryCount := rack.getAvailCarryDataNodeTab(maxTotal, excludeHosts, replicaNum)
	if len(nodeTabs) < replicaNum {
		err = proto.ErrNoDataNodeToWrite
		err = fmt.Errorf(getAvailDataNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			proto.ErrNoDataNodeToWrite, rack.dataNodeCount(), len(nodeTabs))
		return
	}

	nodeTabs.setNodeCarry(availCarryCount, replicaNum)
	sort.Sort(nodeTabs)

	for i := 0; i < replicaNum; i++ {
		node := nodeTabs[i].Ptr.(*DataNode)
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.Addr)
		peer := proto.Peer{ID: node.ID, Addr: node.Addr}
		peers = append(peers, peer)
	}

	if newHosts, err = reshuffleHosts(orderHosts); err != nil {
		err = fmt.Errorf(getAvailDataNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func (rack *Rack) getAvailCarryDataNodeTab(maxTotal uint64, excludeHosts []string, replicaNum int) (nodeTabs SortedWeightedNodes, availCount int) {
	nodeTabs = make(SortedWeightedNodes, 0)
	rack.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if contains(excludeHosts, dataNode.Addr) == true {
			log.LogDebugf("contains return")
			return true
		}
		if dataNode.isWriteAble() == false {
			log.LogDebugf("isWritable return")
			return true
		}
		if dataNode.isAvailCarryNode() == true {
			availCount++
		}
		nt := new(weightedNode)
		nt.Carry = dataNode.Carry
		if dataNode.AvailableSpace < 0 {
			nt.Weight = 0.0
		} else {
			nt.Weight = float64(dataNode.AvailableSpace) / float64(maxTotal)
		}
		nt.Ptr = dataNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

func (rack *Rack) dataNodeCount() (len int) {

	rack.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}
