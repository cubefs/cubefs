// Copyright 2022 The CubeFS Authors.
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
// implieb. See the License for the specific language governing
// permissions and limitations under the License.

package cluster

import (
	"context"
	"encoding/json"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// maxHeap are used to build max heap, and we can calculate writable space that satisfied with stripe count
type maxHeap []int64

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *maxHeap) Push(x interface{}) {
	*h = append(*h, x.(int64))
}

func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// refresh use for refreshing storage allocator info and cluster statistic info
func (b *BlobNodeManager) refresh(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	// space and disk stat info
	spaceStatInfos := make(map[proto.DiskType]*clustermgr.SpaceStatInfo)
	// generate diskType -> nodeSet -> diskSet -> idc -> rack -> blobnode storage and statInfo
	nodeSetAllocators := make(map[proto.DiskType]nodeSetAllocatorMap)
	diskSetAllocators := make(map[proto.DiskType]diskSetAllocatorMap)

	ecDiskSet := make(map[proto.DiskType][]*diskItem)
	nodeSetsMap := b.topoMgr.GetAllNodeSets(ctx)

	for diskType, nodeSets := range nodeSetsMap {
		if _, ok := nodeSetAllocators[diskType]; !ok {
			nodeSetAllocators[diskType] = make(nodeSetAllocatorMap)
		}
		if _, ok := diskSetAllocators[diskType]; !ok {
			diskSetAllocators[diskType] = make(diskSetAllocatorMap)
		}
		if _, ok := spaceStatInfos[diskType]; !ok {
			spaceStatInfos[diskType] = &clustermgr.SpaceStatInfo{}
		}
		spaceStatInfo := spaceStatInfos[diskType]
		diskStatInfo := make(map[string]*clustermgr.DiskStatInfo)
		for i := range b.cfg.IDC {
			diskStatInfo[b.cfg.IDC[i]] = &clustermgr.DiskStatInfo{IDC: b.cfg.IDC[i]}
		}

		for _, nodeSet := range nodeSets {
			nodeSetAllocator := newNodeSetAllocator(nodeSet.ID())
			for _, diskSet := range nodeSet.GetDiskSets() {
				disks := diskSet.GetDisks()
				// ecDiskSet[diskType] = append(ecDiskSet[diskType], disks...)
				idcAllocators, diskSetFreeChunk := b.generateDiskSetStorage(ctx, disks, spaceStatInfo, diskStatInfo)
				diskSetAllocator := newDiskSetAllocator(diskSet.ID(), diskSetFreeChunk, idcAllocators)
				diskSetAllocators[diskType][diskSet.ID()] = diskSetAllocator
				nodeSetAllocator.addDiskSet(diskSetAllocator)
			}
			nodeSetAllocators[diskType][nodeSet.ID()] = nodeSetAllocator
		}

		for idc := range diskStatInfo {
			spaceStatInfo.DisksStatInfos = append(spaceStatInfo.DisksStatInfos, *diskStatInfo[idc])
		}

		spaceStatInfo.TotalBlobNode = int64(b.topoMgr.GetNodeNum(diskType))
	}

	// compatible
	allDisks := b.getAllDisk()
	span.Debugf("get all disks, len:%d", len(allDisks))
	diskTypeDisks := make(map[proto.DiskType][]*diskItem)
	for _, disk := range allDisks {
		diskType := b.getDiskType(disk)
		diskTypeDisks[diskType] = append(diskTypeDisks[diskType], disk)
	}

	for diskType := range diskTypeDisks {
		if _, ok := nodeSetAllocators[diskType]; !ok {
			nodeSetAllocators[diskType] = make(nodeSetAllocatorMap)
		}
		if _, ok := diskSetAllocators[diskType]; !ok {
			diskSetAllocators[diskType] = make(diskSetAllocatorMap)
		}
		if _, ok := spaceStatInfos[diskType]; !ok {
			spaceStatInfos[diskType] = &clustermgr.SpaceStatInfo{}
		}

		ecDiskSet[diskType] = diskTypeDisks[diskType]
		ecSpaceStateInfo := &clustermgr.SpaceStatInfo{}
		diskStatInfo := make(map[string]*clustermgr.DiskStatInfo)
		for i := range b.cfg.IDC {
			diskStatInfo[b.cfg.IDC[i]] = &clustermgr.DiskStatInfo{IDC: b.cfg.IDC[i]}
		}

		ecIdcAllocators, ecFreeChunk := b.generateDiskSetStorage(ctx, ecDiskSet[diskType], ecSpaceStateInfo, diskStatInfo)

		// initial ec allocator
		diskSetAllocator := newDiskSetAllocator(ecDiskSetID, ecFreeChunk, ecIdcAllocators)
		diskSetAllocators[diskType][ecDiskSetID] = diskSetAllocator
		nodeSetAllocator := newNodeSetAllocator(ecNodeSetID)
		nodeSetAllocator.addDiskSet(diskSetAllocator)
		nodeSetAllocators[diskType][ecNodeSetID] = nodeSetAllocator
		span.Debugf("add ec nodeset")

		// update space state info
		for idc := range diskStatInfo {
			ecSpaceStateInfo.DisksStatInfos = append(ecSpaceStateInfo.DisksStatInfos, *diskStatInfo[idc])
		}
		// set blobnode space info and disk stat info by ec statistic
		// TODO: calculate writable space by replicate code mode and ec code mode ratio
		spaceStatInfos[diskType] = ecSpaceStateInfo
		spaceStatInfos[diskType].TotalBlobNode = int64(b.topoMgr.GetNodeNum(diskType))
	}

	b.allocator.Store(newAllocator(allocatorConfig{
		nodeSets: nodeSetAllocators,
		diskSets: diskSetAllocators,
		dg:       b,
		tg:       b.topoMgr,
		diffHost: b.cfg.HostAware,
		diffRack: b.cfg.RackAware,
	}))

	b.spaceStatInfo.Store(spaceStatInfos)
}

func (b *BlobNodeManager) checkDroppingNode(ctx context.Context) {
	if !b.raftServer.IsLeader() {
		return
	}

	span := trace.SpanFromContextSafe(ctx)
	droppingNodeDBs, err := b.nodeTbl.GetAllDroppingNode()
	if err != nil {
		span.Warnf("get dropping nodes failed:%v", err)
		return
	}

	var diskItems []*diskItem
	for _, nodeID := range droppingNodeDBs {
		node, _ := b.getNode(nodeID)
		node.withRLocked(func() error {
			// copy diskIDs of node, avoid nested node and disk lock
			diskItems = make([]*diskItem, 0, len(node.disks))
			for _, di := range node.disks {
				diskItems = append(diskItems, di)
			}
			return nil
		})
		// check disk status
		for _, di := range diskItems {
			err = di.withRLocked(func() error {
				if di.needFilter() {
					return errors.New("node has disk in use")
				}
				return nil
			})
			if err != nil {
				break
			}
		}

		if err != nil {
			span.Debugf("checkDroppingNode node: %d has disk in use", node.nodeID)
			continue
		}

		args := &clustermgr.NodeInfoArgs{NodeID: nodeID}
		data, err := json.Marshal(args)
		if err != nil {
			span.Errorf("checkDroppingNode json marshal failed, args: %v, error: %v", args, err)
			return
		}
		proposeInfo := base.EncodeProposeInfo(b.GetModuleName(), OperTypeDroppedNode, data, base.ProposeContext{ReqID: span.TraceID()})
		err = b.raftServer.Propose(ctx, proposeInfo)
		if err != nil {
			span.Errorf("checkDroppingNode dropped node: %d failed: %v", node.nodeID, err)
			return
		}
		span.Debugf("checkDroppingNode dropped node: %d success", node.nodeID)
	}
}
