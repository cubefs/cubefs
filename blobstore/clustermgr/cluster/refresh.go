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
	"container/heap"
	"context"
	"encoding/json"
	"math"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// maxHeap are use to build max heap, and we can calculate writable space that satisfied with stripe count
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
		// no copy set register, set space info and disk stat info by ec statistic
		if len(nodeSetsMap) == 0 {
			spaceStatInfos[diskType] = ecSpaceStateInfo
			continue
		}

		// TODO: calculate writable space by replicate code mode and ec code mode ratio
		spaceStatInfos[diskType].WritableSpace = ecSpaceStateInfo.WritableSpace
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

func (b *BlobNodeManager) generateDiskSetStorage(ctx context.Context, disks []*diskItem, spaceStatInfo *clustermgr.SpaceStatInfo,
	diskStatInfosM map[string]*clustermgr.DiskStatInfo,
) (ret map[string]*idcAllocator, freeChunk int64) {
	span := trace.SpanFromContextSafe(ctx)
	blobNodeStgs := make(map[string]*nodeAllocator)
	idcFreeChunks := make(map[string]int64)
	idcRackStgs := make(map[string]map[string]*rackAllocator)
	idcBlobNodeStgs := make(map[string][]*nodeAllocator)
	rackBlobNodeStgs := make(map[string][]*nodeAllocator)
	rackFreeChunks := make(map[string]int64)

	var (
		free, diskFreeChunk int64
		idc, rack, host     string
	)
	for _, disk := range disks {
		// read one disk info
		disk.withRLocked(func() error {
			idc = disk.info.Idc
			rack = disk.info.Rack
			host = disk.info.Host
			if node, ok := b.getNode(disk.info.NodeID); ok {
				idc = node.info.Idc
				rack = node.info.Rack
				host = node.info.Host
			}
			heartbeatInfo := disk.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
			diskFreeChunk = heartbeatInfo.FreeChunkCnt
			maxChunk := heartbeatInfo.MaxChunkCnt
			readonly := disk.info.Readonly
			size := heartbeatInfo.Size
			free = heartbeatInfo.Free
			status := disk.info.Status
			// rack can be the same in different idc, so we make rack string with idc
			rack = idc + "-" + rack
			spaceStatInfo.TotalDisk += 1
			// idc disk status num calculate
			if diskStatInfosM[idc] == nil {
				diskStatInfosM[idc] = &clustermgr.DiskStatInfo{IDC: idc}
			}
			diskStatInfosM[idc].Total += 1
			diskStatInfosM[idc].TotalChunk += maxChunk
			diskStatInfosM[idc].TotalFreeChunk += diskFreeChunk
			if readonly {
				diskStatInfosM[idc].Readonly += 1
			}
			switch status {
			case proto.DiskStatusBroken:
				diskStatInfosM[idc].Broken += 1
			case proto.DiskStatusRepairing:
				diskStatInfosM[idc].Repairing += 1
			case proto.DiskStatusRepaired:
				diskStatInfosM[idc].Repaired += 1
			case proto.DiskStatusDropped:
				diskStatInfosM[idc].Dropped += 1
			default:
			}
			if disk.dropping {
				diskStatInfosM[idc].Dropping += 1
			}
			// filter abnormal disk
			if disk.info.Status != proto.DiskStatusNormal {
				return nil
			}
			spaceStatInfo.TotalSpace += size
			if readonly { // include dropping disk
				spaceStatInfo.ReadOnlySpace += free
				return nil
			}
			spaceStatInfo.FreeSpace += free
			diskStatInfosM[idc].Available += 1

			// filter expired disk
			if disk.isExpire() {
				diskStatInfosM[idc].Expired += 1
				return nil
			}

			return nil
		})

		// build for idcRackStorage
		if _, ok := idcRackStgs[idc]; !ok {
			idcRackStgs[idc] = make(map[string]*rackAllocator)
		}
		if _, ok := idcRackStgs[idc][rack]; !ok {
			idcRackStgs[idc][rack] = &rackAllocator{rack: rack}
		}
		// build for idcAllocator
		if _, ok := idcBlobNodeStgs[idc]; !ok {
			idcBlobNodeStgs[idc] = make([]*nodeAllocator, 0)
			idcFreeChunks[idc] = 0
		}
		idcFreeChunks[idc] += diskFreeChunk
		// build for rackAllocator
		if _, ok := rackBlobNodeStgs[rack]; !ok {
			rackBlobNodeStgs[rack] = make([]*nodeAllocator, 0)
			rackFreeChunks[rack] = 0
		}
		rackFreeChunks[rack] += diskFreeChunk
		// build for nodeAllocator
		if _, ok := blobNodeStgs[host]; !ok {
			blobNodeStgs[host] = &nodeAllocator{host: host, disks: make([]*diskItem, 0)}
			// append idc data node
			idcBlobNodeStgs[idc] = append(idcBlobNodeStgs[idc], blobNodeStgs[host])
			// append rack data node
			rackBlobNodeStgs[rack] = append(rackBlobNodeStgs[rack], blobNodeStgs[host])
		}
		blobNodeStgs[host].disks = append(blobNodeStgs[host].disks, disk)
		blobNodeStgs[host].weight += diskFreeChunk
		blobNodeStgs[host].free += free
	}

	span.Debugf("all blobNodeStgs: %+v", blobNodeStgs)
	for _, rackStgs := range idcRackStgs {
		for rack := range rackStgs {
			rackStgs[rack].weight = rackFreeChunks[rack]
			rackStgs[rack].blobNodeStorages = rackBlobNodeStgs[rack]
		}
	}
	for idc := range idcBlobNodeStgs {
		span.Infof("%s idcBlobNodeStgs length: %d", idc, len(idcBlobNodeStgs[idc]))
	}

	spaceStatInfo.UsedSpace = spaceStatInfo.TotalSpace - spaceStatInfo.FreeSpace - spaceStatInfo.ReadOnlySpace

	if len(idcRackStgs) > 0 {
		ret = make(map[string]*idcAllocator)
		for i := range b.cfg.IDC {
			spaceStatInfo.TotalBlobNode += int64(len(idcBlobNodeStgs[b.cfg.IDC[i]]))
			ret[b.cfg.IDC[i]] = &idcAllocator{
				idc:              b.cfg.IDC[i],
				weight:           idcFreeChunks[b.cfg.IDC[i]],
				diffRack:         b.cfg.RackAware,
				diffHost:         b.cfg.HostAware,
				rackStorages:     idcRackStgs[b.cfg.IDC[i]],
				blobNodeStorages: idcBlobNodeStgs[b.cfg.IDC[i]],
			}
			freeChunk += idcFreeChunks[b.cfg.IDC[i]]
		}
		spaceStatInfo.WritableSpace += b.calculateWritable(idcBlobNodeStgs)
	}

	return
}

func (b *BlobNodeManager) calculateWritable(idcBlobNodeStgs map[string][]*nodeAllocator) int64 {
	// writable space statistic
	codeMode, suCount := b.getMaxSuCount()
	idcSuCount := suCount / len(b.cfg.IDC)
	if b.cfg.HostAware && len(idcBlobNodeStgs) > 0 {
		// calculate minimum idc writable chunk num
		calIDCWritableFunc := func(stgs []*nodeAllocator) int64 {
			stripe := make([]int64, idcSuCount)
			lefts := make(maxHeap, 0)
			n := int64(0)
			for _, v := range stgs {
				count := v.free / b.cfg.ChunkSize
				if count > 0 {
					lefts = append(lefts, count)
				}
			}

			heap.Init(&lefts)
			for {
				if lefts.Len() < idcSuCount {
					break
				}
				for i := 0; i < idcSuCount; i++ {
					stripe[i] = heap.Pop(&lefts).(int64)
				}
				// set minimum stripe count to 10 with more random selection, optimize writable space accuracy
				min := int64(10)
				n += min
				for i := 0; i < idcSuCount; i++ {
					stripe[i] -= min
					if stripe[i] > 0 {
						heap.Push(&lefts, stripe[i])
					}
				}
			}
			return n
		}
		minimumStripeCount := int64(math.MaxInt64)
		for idc := range idcBlobNodeStgs {
			n := calIDCWritableFunc(idcBlobNodeStgs[idc])
			if n < minimumStripeCount {
				minimumStripeCount = n
			}
		}
		return minimumStripeCount * int64(codeMode.Tactic().N) * b.cfg.ChunkSize
	}

	if len(idcBlobNodeStgs) > 0 {
		minimumChunkNum := int64(math.MaxInt64)
		for idc := range idcBlobNodeStgs {
			idcChunkNum := int64(0)
			for i := range idcBlobNodeStgs[idc] {
				idcChunkNum += idcBlobNodeStgs[idc][i].free / b.cfg.ChunkSize
			}
			if idcChunkNum < minimumChunkNum {
				minimumChunkNum = idcChunkNum
			}
		}
		return minimumChunkNum / int64(idcSuCount) * int64(codeMode.Tactic().N) * b.cfg.ChunkSize
	}

	return 0
}

func (b *BlobNodeManager) getMaxSuCount() (codemode.CodeMode, int) {
	suCount := 0
	idx := 0
	for i := range b.cfg.CodeModes {
		codeModeInfo := b.cfg.CodeModes[i].Tactic()
		if codeModeInfo.N+codeModeInfo.M+codeModeInfo.L > suCount {
			idx = i
			suCount = codeModeInfo.N + codeModeInfo.M + codeModeInfo.L
		}
	}
	return b.cfg.CodeModes[idx], suCount
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
