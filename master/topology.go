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
	setIndexForDataNode int
	setIndexForMetaNode int
	dataNodes           sync.Map
	metaNodes           sync.Map
	nodeSetMap          map[uint64]*nodeSet
	nsLock              sync.RWMutex
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

func (t *topology) clear() {
	t.dataNodes.Range(func(key, value interface{}) bool {
		t.dataNodes.Delete(key)
		return true
	})
	t.metaNodes.Range(func(key, value interface{}) bool {
		t.metaNodes.Delete(key)
		return true
	})
}

func (t *topology) replaceDataNode(dataNode *DataNode) {
	if oldCell, err := t.getCell(dataNode); err == nil {
		oldCell.putDataNode(dataNode)
	}
	topoNode, ok := t.dataNodes.Load(dataNode.Addr)
	if ok {
		node := topoNode.(*topoDataNode)
		node.CellName = dataNode.CellName
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

func (t *topology) getCell(dataNode *DataNode) (cell *Cell, err error) {
	topoNode, ok := t.dataNodes.Load(dataNode.Addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(dataNode.Addr), "%v not found", dataNode.Addr)
	}
	node := topoNode.(*topoDataNode)
	ns, err := t.getNodeSet(node.setID)
	if err != nil {
		return
	}
	return ns.getCell(node.CellName)
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

func (t *topology) allocNodeSetForDataNode(excludeNodeSet *nodeSet, replicaNum uint8) (ns *nodeSet, err error) {
	nset := t.getAllNodeSet()
	if nset == nil {
		return nil, proto.ErrNoNodeSetToCreateDataPartition
	}
	t.nsLock.Lock()
	defer t.nsLock.Unlock()
	for i := 0; i < len(nset); i++ {
		if t.setIndexForDataNode >= len(nset) {
			t.setIndexForDataNode = 0
		}
		ns = nset[t.setIndexForDataNode]
		t.setIndexForDataNode++
		if excludeNodeSet != nil && excludeNodeSet.ID == ns.ID {
			continue
		}
		if ns.canWriteForDataNode(int(replicaNum)) {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[allocNodeSetForDataNode],err:%v", proto.ErrNoNodeSetToCreateDataPartition))
	return nil, proto.ErrNoNodeSetToCreateDataPartition
}

func (t *topology) allocNodeSetForMetaNode(excludeNodeSet *nodeSet, replicaNum uint8) (ns *nodeSet, err error) {
	nset := t.getAllNodeSet()
	if nset == nil {
		return nil, proto.ErrNoNodeSetToCreateMetaPartition
	}
	t.nsLock.Lock()
	defer t.nsLock.Unlock()
	for i := 0; i < len(nset); i++ {
		if t.setIndexForMetaNode >= len(nset) {
			t.setIndexForMetaNode = 0
		}
		ns = nset[t.setIndexForMetaNode]
		t.setIndexForMetaNode++
		if excludeNodeSet != nil && ns.ID == excludeNodeSet.ID {
			continue
		}
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
	cellIndex   int
	cellMap     map[string]*Cell
	cells       []string
	cellLock    sync.RWMutex
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
	ns.cellMap = make(map[string]*Cell)
	ns.cells = make([]string, 0)
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

func (ns *nodeSet) isSingleCell() bool {
	ns.cellLock.RLock()
	defer ns.cellLock.RUnlock()
	return len(ns.cellMap) == 1
}

func (ns *nodeSet) getCell(name string) (cell *Cell, err error) {
	ns.cellLock.RLock()
	defer ns.cellLock.RUnlock()
	cell, ok := ns.cellMap[name]
	if !ok {
		return nil, errors.Trace(cellNotFound(name), "%v not found", name)
	}
	return
}

func (ns *nodeSet) putCell(cell *Cell) *Cell {
	ns.cellLock.Lock()
	defer ns.cellLock.Unlock()
	oldCell, ok := ns.cellMap[cell.name]
	if ok {
		return oldCell
	}
	ns.cellMap[cell.name] = cell
	if ok := ns.isExist(cell.name); !ok {
		ns.cells = append(ns.cells, cell.name)
	}
	return cell
}

func (ns *nodeSet) isExist(cellName string) (ok bool) {
	for _, name := range ns.cells {
		if name == cellName {
			ok = true
			return
		}
	}
	return
}

func (ns *nodeSet) removeCell(name string) {
	ns.cellLock.Lock()
	defer ns.cellLock.Unlock()
	delete(ns.cellMap, name)
}

func (ns *nodeSet) putDataNode(dataNode *DataNode) {
	cell, err := ns.getCell(dataNode.CellName)
	if err != nil {
		cell = newCell(dataNode.CellName)
		cell = ns.putCell(cell)
	}
	cell.putDataNode(dataNode)
	ns.dataNodes.Store(dataNode.Addr, dataNode)
}

func (ns *nodeSet) deleteDataNode(dataNode *DataNode) {
	ns.dataNodes.Delete(dataNode.Addr)
	cell, err := ns.getCell(dataNode.CellName)
	if err != nil {
		return
	}
	cell.dataNodes.Delete(dataNode.Addr)
}

func (ns *nodeSet) getAllCells() (cells []*Cell) {
	ns.cellLock.RLock()
	defer ns.cellLock.RUnlock()
	cells = make([]*Cell, 0)
	for _, cell := range ns.cellMap {
		cells = append(cells, cell)
	}
	return
}

func (ns *nodeSet) getCellNameByIndex(index int) (rName string) {
	ns.cellLock.RLock()
	defer ns.cellLock.RUnlock()
	rName = ns.cells[index]
	return
}

func (ns *nodeSet) allocCells(replicaNum int, excludeCell []string) (cells []*Cell, err error) {

	cells = ns.getAllCells()
	if ns.isSingleCell() {
		return cells, nil
	}
	if excludeCell == nil {
		excludeCell = make([]string, 0)
	}
	candidateCells := make([]*Cell, 0)
	for i := 0; i < len(cells); i++ {
		if ns.cellIndex >= len(cells) {
			ns.cellIndex = 0
		}
		rName := ns.getCellNameByIndex(ns.cellIndex)
		if contains(excludeCell, rName) {
			ns.cellIndex++
			continue
		}
		var cell *Cell
		if cell, err = ns.getCell(ns.cells[ns.cellIndex]); err != nil {
			ns.cellIndex++
			continue
		}
		ns.cellIndex++

		if cell.canWrite(uint8(replicaNum)) {
			candidateCells = append(candidateCells, cell)
		}
		if len(candidateCells) >= int(replicaNum) {
			break
		}
	}
	if len(candidateCells) == 0 {
		log.LogError(fmt.Sprintf("action[allocCells],err:%v", proto.ErrNoCellToCreateDataPartition))
		return nil, proto.ErrNoCellToCreateDataPartition
	}
	cells = candidateCells
	err = nil
	return
}

func (ns *nodeSet) getAvailDataNodeHosts(excludeCell *Cell, excludeHosts []string, replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr  []string
		addrs       []string
		cells       []*Cell
		cell        *Cell
		masterPeers []proto.Peer
		slavePeers  []proto.Peer
	)
	hosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if excludeHosts == nil {
		excludeHosts = make([]string, 0)
	}
	if ns.isSingleCell() {
		if excludeCell != nil && excludeCell.name == ns.cells[0] {
			log.LogErrorf("ns[%v] no cell to createDataPartition after exclude cell[%v]", ns.ID, excludeCell.name)
			return nil, nil, proto.ErrNoCellToCreateDataPartition
		}
		if cell, err = ns.getCell(ns.cells[0]); err != nil {
			return nil, nil, errors.NewError(err)
		}
		if hosts, peers, err = cell.getAvailDataNodeHosts(excludeHosts, replicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		return
	}
	excludeCells := make([]string, 0)
	if excludeCell != nil {
		excludeCells = append(excludeCells, excludeCell.name)
	}
	if cells, err = ns.allocCells(replicaNum, excludeCells); err != nil {
		return nil, nil, errors.NewError(err)
	}
	if len(cells) == replicaNum {
		for index := 0; index < replicaNum; index++ {
			cell := cells[index]
			var selectPeers []proto.Peer
			if addrs, selectPeers, err = cell.getAvailDataNodeHosts(excludeHosts, 1); err != nil {
				return nil, nil, errors.NewError(err)
			}
			hosts = append(hosts, addrs...)
			peers = append(peers, selectPeers...)
			excludeHosts = append(excludeHosts, addrs...)
		}
		return
	}
	// the number of cells less than replica number,We're only dealing with one cell and two cells
	if len(cells) == 1 {
		if cell, err = ns.getCell(ns.cells[0]); err != nil {
			return nil, nil, errors.NewError(err)
		}
		if hosts, peers, err = cell.getAvailDataNodeHosts(excludeHosts, replicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		return
	} else if len(cells) >= 2 {
		masterCell := cells[0]
		slaveCell := cells[1]
		masterReplicaNum := replicaNum/2 + 1
		slaveReplicaNum := replicaNum - masterReplicaNum
		if masterAddr, masterPeers, err = masterCell.getAvailDataNodeHosts(excludeHosts, masterReplicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		hosts = append(hosts, masterAddr...)
		peers = append(peers, masterPeers...)
		excludeHosts = append(excludeHosts, masterAddr...)
		if addrs, slavePeers, err = slaveCell.getAvailDataNodeHosts(excludeHosts, slaveReplicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		hosts = append(hosts, addrs...)
		peers = append(peers, slavePeers...)
	}
	if len(hosts) != replicaNum {
		return nil, nil, proto.ErrNoDataNodeToCreateDataPartition
	}
	return
}

// Cell stores all the cell related information
type Cell struct {
	name      string
	dataNodes sync.Map
	sync.RWMutex
}

func newCell(name string) (cell *Cell) {
	return &Cell{name: name}
}

func (cell *Cell) putDataNode(dataNode *DataNode) {
	cell.dataNodes.Store(dataNode.Addr, dataNode)
}

func (cell *Cell) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := cell.dataNodes.Load(addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
	}
	dataNode = value.(*DataNode)
	return
}
func (cell *Cell) removeDataNode(addr string) {
	cell.dataNodes.Delete(addr)
}

func (cell *Cell) canWrite(replicaNum uint8) (can bool) {
	cell.RLock()
	defer cell.RUnlock()
	var leastAlive uint8
	cell.dataNodes.Range(func(addr, value interface{}) bool {
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

func (cell *Cell) getDataNodeMaxTotal() (maxTotal uint64) {
	cell.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (cell *Cell) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := cell.getDataNodeMaxTotal()
	nodeTabs, availCarryCount := cell.getAvailCarryDataNodeTab(maxTotal, excludeHosts, replicaNum)
	if len(nodeTabs) < replicaNum {
		err = proto.ErrNoDataNodeToWrite
		err = fmt.Errorf(getAvailDataNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			proto.ErrNoDataNodeToWrite, cell.dataNodeCount(), len(nodeTabs))
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

func (cell *Cell) getAvailCarryDataNodeTab(maxTotal uint64, excludeHosts []string, replicaNum int) (nodeTabs SortedWeightedNodes, availCount int) {
	nodeTabs = make(SortedWeightedNodes, 0)
	cell.dataNodes.Range(func(key, value interface{}) bool {
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

func (cell *Cell) dataNodeCount() (len int) {

	cell.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}
