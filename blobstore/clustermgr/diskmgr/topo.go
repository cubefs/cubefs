package diskmgr

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func newTopoMgr() *topoMgr {
	return &topoMgr{
		allNodeSets: make(map[proto.DiskType]nodeSetMap),
	}
}

type nodeSetMap map[proto.NodeSetID]*nodeSetItem

type topoMgr struct {
	curNodeSetID proto.NodeSetID
	curDiskSetID proto.DiskSetID
	allNodeSets  map[proto.DiskType]nodeSetMap

	lock sync.RWMutex
}

func (t *topoMgr) SetNodeSetID(id proto.NodeSetID) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.curNodeSetID = id
}

func (t *topoMgr) SetDiskSetID(id proto.DiskSetID) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.curDiskSetID = id
}

func (t *topoMgr) AllocNodeSetID(ctx context.Context, info *blobnode.NodeInfo, config CopySetConfig, rackAware bool) proto.NodeSetID {
	span := trace.SpanFromContextSafe(ctx)
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.allNodeSets[info.DiskType]; !ok {
		t.allNodeSets[info.DiskType] = make(nodeSetMap)
	}

	var retryMode bool
	nodeSetCap := config.NodeSetCap
	nodeSetIdcCap := config.NodeSetIdcCap
	nodeSetRackCap := config.NodeSetRackCap

RETRY:
	for nodeSetID, nodeSet := range t.allNodeSets[info.DiskType] {
		if nodeSetLen := nodeSet.getNodeSetLen(); nodeSetLen >= nodeSetCap {
			continue
		}
		nodeSetIdcLen, nodeSetRackLen := nodeSet.getNodeSetIDCAndRackLen(info.Idc, info.Rack)
		if nodeSetIdcLen >= nodeSetIdcCap {
			continue
		}
		// omit rack diff when retry
		if rackAware && nodeSetRackLen >= nodeSetRackCap && !retryMode {
			continue
		}
		span.Debugf("nodeSetID %d is chosen, nodeSetLen:%d, nodeSetIdcLen:%d, nodeSetRackLen:%d", nodeSetID, nodeSetIdcLen, nodeSetRackLen)
		return nodeSetID
	}
	if rackAware && !retryMode {
		span.Warn("retry without rackAware")
		retryMode = true
		goto RETRY
	}

	t.curNodeSetID += 1
	return t.curNodeSetID
}

func (t *topoMgr) AllocDiskSetID(ctx context.Context, diskInfo *blobnode.DiskInfo, nodeInfo *blobnode.NodeInfo, config CopySetConfig) proto.DiskSetID {
	span := trace.SpanFromContextSafe(ctx)
	nodeSet := t.getNodeSet(nodeInfo.DiskType, nodeInfo.NodeSetID)

	t.lock.Lock()
	defer t.lock.Unlock()

	diskSetCap := config.DiskSetCap
	diskCountPerNode := config.DiskCountPerNodeInDiskSet
	for diskSetID, diskSet := range nodeSet.diskSets {
		diskSetLen, diskCount := diskSet.getDiskSetLen(diskInfo.NodeID)
		if diskSetLen > diskSetCap || diskCount > diskCountPerNode {
			continue
		}
		span.Debugf("diskSetID %d is chosen, diskSetLen:%d, diskCount:%d", diskSetID, diskSetLen, diskCount)
		return diskSetID
	}

	t.curDiskSetID += 1
	return t.curDiskSetID
}

func (t *topoMgr) AddNodeToNodeSet(node *nodeItem) {
	t.lock.Lock()

	info := node.info
	nodeSetsOfDiskType, ok := t.allNodeSets[info.DiskType]
	if !ok {
		nodeSetsOfDiskType = make(nodeSetMap)
		t.allNodeSets[info.DiskType] = nodeSetsOfDiskType
	}
	nodeSet, ok := nodeSetsOfDiskType[info.NodeSetID]
	if !ok {
		nodeSet = &nodeSetItem{
			nodeSetID: info.NodeSetID,
			diskSets:  make(map[proto.DiskSetID]*diskSetItem),
			nodes:     make(map[proto.NodeID]*nodeItem),
		}
		nodeSetsOfDiskType[info.NodeSetID] = nodeSet
	}

	t.lock.Unlock()

	nodeSet.addNode(node)
}

func (t *topoMgr) RemoveNodeFromNodeSet(node *nodeItem) {
	info := node.info
	nodeSet := t.getNodeSet(info.DiskType, info.NodeSetID)

	nodeSet.removeNode(node.nodeID)
}

func (t *topoMgr) AddDiskToDiskSet(diskType proto.DiskType, nodeSetID proto.NodeSetID, disk *diskItem) {
	nodeSet := t.getNodeSet(diskType, nodeSetID)
	nodeSet.addDisk(disk)
}

func (t *topoMgr) RemoveDiskFromDiskSet(diskType proto.DiskType, nodeSetID proto.NodeSetID, disk *diskItem) {
	nodeSet := t.getNodeSet(diskType, nodeSetID)
	nodeSet.removeDisk(disk)
}

func (t *topoMgr) ValidateNodeSetID(ctx context.Context, diskType proto.DiskType, nodeSetID proto.NodeSetID) error {
	span := trace.SpanFromContext(ctx)

	t.lock.RLock()
	defer t.lock.RUnlock()

	curNodeSetID := t.curNodeSetID
	if curNodeSetID < nodeSetID || nodeSetID == ECNodeSetID {
		span.Warn("invalid node set id")
		return apierrors.ErrIllegalArguments
	}

	if _, ok := t.allNodeSets[diskType]; !ok {
		span.Warnf("node set disk type not exist, disk type: %d", diskType)
		return apierrors.ErrCMNodeSetNotFound
	}
	if _, ok := t.allNodeSets[diskType][nodeSetID]; !ok {
		span.Warnf("node set id not exist, node set id: %d", nodeSetID)
		return apierrors.ErrCMNodeSetNotFound
	}

	return nil
}

func (t *topoMgr) GetAllNodeSets(ctx context.Context) map[proto.DiskType][]*nodeSetItem {
	t.lock.RLock()
	defer t.lock.RUnlock()

	ret := make(map[proto.DiskType][]*nodeSetItem)
	for diskType, m := range t.allNodeSets {
		ret[diskType] = make([]*nodeSetItem, 0, len(m))
		for _, nodeSetItem := range m {
			ret[diskType] = append(ret[diskType], nodeSetItem)
		}
	}

	return ret
}

func (t *topoMgr) getNodeSet(diskType proto.DiskType, nodeSetID proto.NodeSetID) *nodeSetItem {
	t.lock.RLock()
	nodeSet := t.allNodeSets[diskType][nodeSetID]
	t.lock.RUnlock()
	return nodeSet
}

func (t *topoMgr) getNodeNum(diskType proto.DiskType, id proto.NodeSetID) int {
	nodeSet := t.getNodeSet(diskType, id)
	return nodeSet.getNodeNum()
}

type nodeSetItem struct {
	nodeSetID proto.NodeSetID
	diskSets  map[proto.DiskSetID]*diskSetItem
	nodes     map[proto.NodeID]*nodeItem
	// nodes     map[string]map[string]nodeMap // idc <--> (rack <--> nodeMap)

	sync.RWMutex
}

func (n *nodeSetItem) GetDiskSets() []*diskSetItem {
	n.RLock()
	defer n.RUnlock()

	ret := make([]*diskSetItem, 0, len(n.diskSets))
	for _, diskSetItem := range n.diskSets {
		ret = append(ret, diskSetItem)
	}

	return ret
}

func (n *nodeSetItem) ID() proto.NodeSetID {
	return n.nodeSetID
}

func (n *nodeSetItem) getNodeNum() int {
	return len(n.nodes)
}

func (n *nodeSetItem) addNode(node *nodeItem) {
	n.Lock()
	defer n.Unlock()

	n.nodes[node.nodeID] = node
}

func (n *nodeSetItem) removeNode(nodeID proto.NodeID) {
	n.Lock()
	defer n.Unlock()

	delete(n.nodes, nodeID)
}

func (n *nodeSetItem) addDisk(disk *diskItem) {
	n.Lock()

	diskInfo := disk.info
	diskSet, ok := n.diskSets[diskInfo.DiskSetID]
	if !ok {
		diskSet = &diskSetItem{
			diskSetID:     diskInfo.DiskSetID,
			disks:         make(map[proto.DiskID]*diskItem),
			nodeDiskCount: make(map[proto.NodeID]int),
		}
		n.diskSets[diskInfo.DiskSetID] = diskSet
	}

	n.Unlock()

	diskSet.add(disk)
}

func (n *nodeSetItem) removeDisk(disk *diskItem) {
	n.RLock()
	diskSet := n.diskSets[disk.info.DiskSetID]
	n.RUnlock()

	diskSet.remove(disk)
}

func (n *nodeSetItem) getNodeSetLen() int {
	n.RLock()
	ret := len(n.nodes)
	n.RUnlock()

	return ret
}

func (n *nodeSetItem) getNodeSetIDCAndRackLen(idc, rack string) (int, int) {
	var nodeSetIdcLen, nodeSetRackLen int

	for _, node := range n.nodes {
		if node.info.Idc == idc {
			nodeSetIdcLen += 1
		}
		if node.info.Rack == rack {
			nodeSetRackLen += 1
		}
	}

	return nodeSetIdcLen, nodeSetRackLen
}

type diskSetItem struct {
	diskSetID proto.DiskSetID
	// disks     map[proto.NodeID]map[proto.DiskID]*diskItem // nodeID <--> diskMap
	disks         map[proto.DiskID]*diskItem
	nodeDiskCount map[proto.NodeID]int

	sync.RWMutex
}

func (d *diskSetItem) GetDisks() []*diskItem {
	d.RLock()
	defer d.RUnlock()

	ret := make([]*diskItem, 0, len(d.disks))
	for i := range d.disks {
		ret = append(ret, d.disks[i])
	}
	return ret
}

func (d *diskSetItem) ID() proto.DiskSetID {
	return d.diskSetID
}

func (d *diskSetItem) add(disk *diskItem) {
	d.Lock()
	defer d.Unlock()

	d.disks[disk.diskID] = disk
	d.nodeDiskCount[disk.info.NodeID] += 1
}

func (d *diskSetItem) remove(disk *diskItem) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.disks[disk.diskID]; ok {
		delete(d.disks, disk.diskID)
		d.nodeDiskCount[disk.info.NodeID] -= 1
	}
}

func (d *diskSetItem) getDiskSetLen(nodeID proto.NodeID) (int, int) {
	return len(d.disks), d.nodeDiskCount[nodeID]
}