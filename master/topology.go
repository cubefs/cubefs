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
	dataNodes            *sync.Map
	metaNodes            *sync.Map
	cellMap              *sync.Map
	cellIndexForDataNode int
	cellIndexForMetaNode int
	cells                []*Cell
	cellLock             sync.RWMutex
}

func newTopology() (t *topology) {
	t = new(topology)
	t.cellMap = new(sync.Map)
	t.dataNodes = new(sync.Map)
	t.metaNodes = new(sync.Map)
	t.cells = make([]*Cell, 0)
	return
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

func (t *topology) putCell(cell *Cell) (err error) {
	t.cellLock.Lock()
	defer t.cellLock.Unlock()
	if _, ok := t.cellMap.Load(cell.name); ok {
		return fmt.Errorf("cell[%v] has exist", cell.name)
	}
	t.cellMap.Store(cell.name, cell)
	t.cells = append(t.cells, cell)
	return
}

func (t *topology) putCellIfAbsent(cell *Cell) (beStoredCell *Cell) {
	t.cellLock.Lock()
	defer t.cellLock.Unlock()
	oldCell, ok := t.cellMap.Load(cell.name)
	if ok {
		return oldCell.(*Cell)
	}
	t.cellMap.Store(cell.name, cell)
	t.cells = append(t.cells, cell)
	beStoredCell = cell
	return
}

func (t *topology) getCell(name string) (cell *Cell, err error) {
	t.cellMap.Range(func(cellName, value interface{}) bool {
		if cellName != name {
			return true
		}
		cell = value.(*Cell)
		return true
	})
	if cell == nil {
		return nil, fmt.Errorf("cell[%v] is not found", name)
	}
	return
}

func (t *topology) putDataNode(dataNode *DataNode) (err error) {

	if _, ok := t.dataNodes.Load(dataNode.Addr); ok {
		return
	}
	cell, err := t.getCell(dataNode.CellName)
	if err != nil {
		return
	}

	cell.putDataNode(dataNode)
	t.putDataNodeToCache(dataNode)
	return
}

func (t *topology) putDataNodeToCache(dataNode *DataNode) {
	t.dataNodes.Store(dataNode.Addr, dataNode)
}

func (t *topology) deleteDataNode(dataNode *DataNode) {
	cell, err := t.getCell(dataNode.CellName)
	if err != nil {
		return
	}
	cell.deleteDataNode(dataNode)
	t.dataNodes.Delete(dataNode.Addr)
}

func (t *topology) getCellByDataNode(dataNode *DataNode) (cell *Cell, err error) {
	_, ok := t.dataNodes.Load(dataNode.Addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(dataNode.Addr), "%v not found", dataNode.Addr)
	}

	return t.getCell(dataNode.CellName)
}

func (t *topology) putMetaNode(metaNode *MetaNode) (err error) {
	if _, ok := t.metaNodes.Load(metaNode.Addr); ok {
		return
	}
	cell, err := t.getCell(metaNode.CellName)
	if err != nil {
		return
	}
	cell.putMetaNode(metaNode)
	t.putMetaNodeToCache(metaNode)
	return
}

func (t *topology) deleteMetaNode(metaNode *MetaNode) {
	t.metaNodes.Delete(metaNode.Addr)
	cell, err := t.getCell(metaNode.CellName)
	if err != nil {
		return
	}
	cell.deleteMetaNode(metaNode)
}

func (t *topology) putMetaNodeToCache(metaNode *MetaNode) {
	t.metaNodes.Store(metaNode.Addr, metaNode)
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
	dataNodeLen int
	metaNodeLen int
	cellName    string
	metaNodes   *sync.Map
	dataNodes   *sync.Map
	sync.RWMutex
}

func newNodeSet(id uint64, cap int, cellName string) *nodeSet {
	ns := &nodeSet{
		ID:        id,
		Capacity:  cap,
		cellName:  cellName,
		metaNodes: new(sync.Map),
		dataNodes: new(sync.Map),
	}
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
	ns.decreaseMetaNodeLen()
}

func (ns *nodeSet) canWriteForDataNode(replicaNum int) bool {
	log.LogInfof("canWriteForDataNode cell[%v], ns[%v],dataLen[%v], replicaNum[%v]",
		ns.cellName, ns.ID, ns.dataNodeLen, replicaNum)
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
	log.LogInfof("canWriteForDataNode cell[%v], ns[%v],count[%v], replicaNum[%v]",
		ns.cellName, ns.ID, count, replicaNum)
	return count >= replicaNum
}

func (ns *nodeSet) canWriteForMetaNode(replicaNum int) bool {
	log.LogInfof("canWriteForMetaNode cell[%v], ns[%v],metaLen[%v] replicaNum[%v]",
		ns.cellName, ns.ID, ns.metaNodeLen, replicaNum)
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
	log.LogInfof("canWriteForMetaNode cell[%v], ns[%v],count[%v] replicaNum[%v]",
		ns.cellName, ns.ID, count, replicaNum)
	return count >= replicaNum
}

func (ns *nodeSet) putDataNode(dataNode *DataNode) {
	ns.dataNodes.Store(dataNode.Addr, dataNode)
}

func (ns *nodeSet) deleteDataNode(dataNode *DataNode) {
	ns.dataNodes.Delete(dataNode.Addr)
	ns.decreaseDataNodeLen()
}

func (t *topology) isSingleCell() bool {
	t.cellLock.RLock()
	defer t.cellLock.RUnlock()
	var cellLen int
	t.cellMap.Range(func(cellName, value interface{}) bool {
		cellLen++
		return true
	})
	return cellLen == 1
}

func (t *topology) getAllCells() (cells []*Cell) {
	t.cellLock.RLock()
	defer t.cellLock.RUnlock()
	cells = make([]*Cell, 0)
	t.cellMap.Range(func(cellName, value interface{}) bool {
		cell := value.(*Cell)
		cells = append(cells, cell)
		return true
	})
	return
}

func (t *topology) getCellByIndex(index int) (cell *Cell) {
	t.cellLock.RLock()
	defer t.cellLock.RUnlock()
	return t.cells[index]
}

func (t *topology) allocCellsForMetaNode(cellNum, replicaNum int, excludeCell []string) (cells []*Cell, err error) {
	cells = t.getAllCells()
	if t.isSingleCell() {
		return cells, nil
	}
	if excludeCell == nil {
		excludeCell = make([]string, 0)
	}
	candidateCells := make([]*Cell, 0)
	for i := 0; i < len(cells); i++ {
		if t.cellIndexForMetaNode >= len(cells) {
			t.cellIndexForMetaNode = 0
		}
		cell := t.getCellByIndex(t.cellIndexForMetaNode)
		t.cellIndexForMetaNode++
		if contains(excludeCell, cell.name) {
			continue
		}
		if cell.canWriteForMetaNode(uint8(replicaNum)) {
			candidateCells = append(candidateCells, cell)
		}
		if len(candidateCells) >= cellNum {
			break
		}
	}
	if len(candidateCells) < cellNum {
		log.LogError(fmt.Sprintf("action[allocCellsForMetaNode],err:%v", proto.ErrCodeNoCellToCreateDataPartition))
		return nil, proto.ErrNoCellToCreateDataPartition
	}
	cells = candidateCells
	err = nil
	return
}

func (t *topology) allocCellsForDataNode(cellNum, replicaNum int, excludeCell []string) (cells []*Cell, err error) {
	cells = t.getAllCells()
	fmt.Printf("len(cells) = %v \n", len(cells))
	if t.isSingleCell() {
		return cells, nil
	}
	if excludeCell == nil {
		excludeCell = make([]string, 0)
	}
	candidateCells := make([]*Cell, 0)
	for i := 0; i < len(cells); i++ {
		if t.cellIndexForDataNode >= len(cells) {
			t.cellIndexForDataNode = 0
		}
		cell := t.getCellByIndex(t.cellIndexForDataNode)
		t.cellIndexForDataNode++
		if contains(excludeCell, cell.name) {
			continue
		}
		if cell.canWriteForDataNode(uint8(replicaNum)) {
			candidateCells = append(candidateCells, cell)
		}
		if len(candidateCells) >= cellNum {
			break
		}
	}
	if len(candidateCells) < cellNum {
		log.LogError(fmt.Sprintf("action[allocCellsForDataNode],err:%v", proto.ErrNoCellToCreateDataPartition))
		return nil, errors.NewError(proto.ErrNoCellToCreateDataPartition)
	}
	cells = candidateCells
	err = nil
	return
}

func (ns *nodeSet) dataNodeCount() int {
	var count int
	ns.dataNodes.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func (ns *nodeSet) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	return getAvailHosts(ns.dataNodes, excludeHosts, replicaNum, selectDataNode)
}

// Cell stores all the cell related information
type Cell struct {
	name                string
	setIndexForDataNode int
	setIndexForMetaNode int
	dataNodes           *sync.Map
	metaNodes           *sync.Map
	nodeSetMap          map[uint64]*nodeSet
	nsLock              sync.RWMutex
	sync.RWMutex
}

func newCell(name string) (cell *Cell) {
	cell = &Cell{name: name}
	cell.dataNodes = new(sync.Map)
	cell.metaNodes = new(sync.Map)
	cell.nodeSetMap = make(map[uint64]*nodeSet)
	return
}

func (cell *Cell) isSingleNodeSet() bool {
	cell.RLock()
	defer cell.RUnlock()
	return len(cell.nodeSetMap) == 1
}

func (cell *Cell) getNodeSet(setID uint64) (ns *nodeSet, err error) {
	cell.nsLock.RLock()
	defer cell.nsLock.RUnlock()
	ns, ok := cell.nodeSetMap[setID]
	if !ok {
		return nil, errors.NewErrorf("set %v not found", setID)
	}
	return
}

func (cell *Cell) putNodeSet(ns *nodeSet) (err error) {
	cell.nsLock.Lock()
	defer cell.nsLock.Unlock()
	if _, ok := cell.nodeSetMap[ns.ID]; ok {
		return fmt.Errorf("nodeSet [%v] has exist", ns.ID)
	}
	cell.nodeSetMap[ns.ID] = ns
	return
}

func (cell *Cell) createNodeSet(c *Cluster) (ns *nodeSet, err error) {
	id, err := c.idAlloc.allocateCommonID()
	if err != nil {
		return
	}
	ns = newNodeSet(id, c.cfg.nodeSetCapacity, cell.name)
	if err = c.syncAddNodeSet(ns); err != nil {
		return
	}
	if err = cell.putNodeSet(ns); err != nil {
		return
	}
	return
}

func (cell *Cell) getAllNodeSet() (nsc nodeSetCollection) {
	cell.nsLock.RLock()
	defer cell.nsLock.RUnlock()
	nsc = make(nodeSetCollection, 0)
	for _, ns := range cell.nodeSetMap {
		nsc = append(nsc, ns)
	}
	return
}

func (cell *Cell) getAvailNodeSetForMetaNode() (nset *nodeSet) {
	allNodeSet := cell.getAllNodeSet()
	sort.Sort(sort.Reverse(allNodeSet))
	for _, ns := range allNodeSet {
		if ns.metaNodeLen < ns.Capacity {
			nset = ns
			return
		}
	}
	return
}

func (cell *Cell) getAvailNodeSetForDataNode() (nset *nodeSet) {
	allNodeSet := cell.getAllNodeSet()
	for _, ns := range allNodeSet {
		if ns.dataNodeLen < ns.Capacity {
			nset = ns
			return
		}
	}
	return
}

func (cell *Cell) putDataNode(dataNode *DataNode) (err error) {
	var ns *nodeSet
	if ns, err = cell.getNodeSet(dataNode.NodeSetID); err != nil {
		log.LogErrorf("action[putDataNode] nodeSet[%v] not found", dataNode.NodeSetID)
		return
	}
	ns.putDataNode(dataNode)
	cell.dataNodes.Store(dataNode.Addr, dataNode)
	return
}

func (cell *Cell) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := cell.dataNodes.Load(addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
	}
	dataNode = value.(*DataNode)
	return
}
func (cell *Cell) deleteDataNode(dataNode *DataNode) {
	ns, err := cell.getNodeSet(dataNode.NodeSetID)
	if err != nil {
		log.LogErrorf("action[cellDeleteDataNode] nodeSet[%v] not found", dataNode.NodeSetID)
		return
	}
	ns.deleteDataNode(dataNode)
	cell.dataNodes.Delete(dataNode.Addr)
}

func (cell *Cell) putMetaNode(metaNode *MetaNode) (err error) {
	var ns *nodeSet
	if ns, err = cell.getNodeSet(metaNode.NodeSetID); err != nil {
		log.LogErrorf("action[cellPutMetaNode] nodeSet[%v] not found", metaNode.NodeSetID)
		return
	}
	ns.putMetaNode(metaNode)
	cell.metaNodes.Store(metaNode.Addr, metaNode)
	return
}

func (cell *Cell) deleteMetaNode(metaNode *MetaNode) (err error) {
	ns, err := cell.getNodeSet(metaNode.NodeSetID)
	if err != nil {
		log.LogErrorf("action[cellDeleteMetaNode] nodeSet[%v] not found", metaNode.NodeSetID)
		return
	}
	ns.deleteMetaNode(metaNode)
	cell.metaNodes.Delete(metaNode.Addr)
	return
}

func (cell *Cell) allocNodeSetForDataNode(excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	nset := cell.getAllNodeSet()
	if nset == nil {
		return nil, errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
	}
	cell.nsLock.Lock()
	defer cell.nsLock.Unlock()
	for i := 0; i < len(nset); i++ {
		if cell.setIndexForDataNode >= len(nset) {
			cell.setIndexForDataNode = 0
		}
		ns = nset[cell.setIndexForDataNode]
		cell.setIndexForDataNode++
		if containsID(excludeNodeSets, ns.ID) {
			continue
		}
		if ns.canWriteForDataNode(int(replicaNum)) {
			return
		}
	}
	log.LogErrorf("action[allocNodeSetForDataNode],nset len[%v] err:%v", nset.Len(), proto.ErrNoNodeSetToCreateDataPartition)
	return nil, errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
}

func (cell *Cell) allocNodeSetForMetaNode(excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	nset := cell.getAllNodeSet()
	if nset == nil {
		return nil, proto.ErrNoNodeSetToCreateMetaPartition
	}
	cell.nsLock.Lock()
	defer cell.nsLock.Unlock()
	for i := 0; i < len(nset); i++ {
		if cell.setIndexForMetaNode >= len(nset) {
			cell.setIndexForMetaNode = 0
		}
		ns = nset[cell.setIndexForMetaNode]
		cell.setIndexForMetaNode++
		if containsID(excludeNodeSets, ns.ID) {
			continue
		}
		if ns.canWriteForMetaNode(int(replicaNum)) {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[allocNodeSetForMetaNode],err:%v", proto.ErrNoNodeSetToCreateMetaPartition))
	return nil, proto.ErrNoNodeSetToCreateMetaPartition
}

func (cell *Cell) canWriteForDataNode(replicaNum uint8) (can bool) {
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
	fmt.Printf("canWriteForDataNode leastAlive[%v],replicaNum[%v],count[%v]\n", leastAlive, replicaNum, cell.dataNodeCount())
	return
}

func (cell *Cell) canWriteForMetaNode(replicaNum uint8) (can bool) {
	cell.RLock()
	defer cell.RUnlock()
	var leastAlive uint8
	cell.metaNodes.Range(func(addr, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if metaNode.IsActive == true && metaNode.isWritable() == true {
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

func (cell *Cell) getAvailDataNodeHosts(excludeNodeSets []uint64, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	if replicaNum == 0 {
		return
	}
	ns, err := cell.allocNodeSetForDataNode(excludeNodeSets, uint8(replicaNum))
	if err != nil {
		return nil, nil, errors.Trace(err, "cell[%v] alloc node set,replicaNum[%v]", cell.name, replicaNum)
	}
	return ns.getAvailDataNodeHosts(excludeHosts, replicaNum)
}

func (cell *Cell) getAvailMetaNodeHosts(excludeNodeSets []uint64, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	if replicaNum == 0 {
		return
	}
	ns, err := cell.allocNodeSetForMetaNode(excludeNodeSets, uint8(replicaNum))
	if err != nil {
		return
	}
	return ns.getAvailMetaNodeHosts(excludeHosts, replicaNum)

}

func (cell *Cell) dataNodeCount() (len int) {

	cell.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}
