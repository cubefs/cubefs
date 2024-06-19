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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package diskmgr

import (
	"container/heap"
	"context"
	"math"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
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
func (d *DiskMgr) refresh(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	// generate nodeRole -> diskType -> nodeSet -> diskSet -> idc -> rack -> blobnode storage and statInfo
	nodeSetAllocators := make(map[proto.NodeRole]map[proto.DiskType]nodeSetAllocatorMap)
	diskSetAllocators := make(map[proto.NodeRole]map[proto.DiskSetID]*diskSetAllocator)

	// space and disk stat info
	spaceStatInfos := make(map[proto.NodeRole]map[proto.DiskType]*clustermgr.SpaceStatInfo)

	for nodeRole, mgr := range d.topoMgrs {
		if _, ok := nodeSetAllocators[nodeRole]; !ok {
			nodeSetAllocators[nodeRole] = make(map[proto.DiskType]nodeSetAllocatorMap)
		}
		if _, ok := diskSetAllocators[nodeRole]; !ok {
			diskSetAllocators[nodeRole] = make(map[proto.DiskSetID]*diskSetAllocator)
		}
		if _, ok := spaceStatInfos[nodeRole]; !ok {
			spaceStatInfos[nodeRole] = make(map[proto.DiskType]*clustermgr.SpaceStatInfo)
		}

		ecDiskSet := make(map[proto.DiskType][]*diskItem)
		nodeSetsMap := mgr.GetAllNodeSets(ctx)

		for diskType, nodeSets := range nodeSetsMap {
			if _, ok := nodeSetAllocators[nodeRole][diskType]; !ok {
				nodeSetAllocators[nodeRole][diskType] = make(nodeSetAllocatorMap)
			}
			if _, ok := spaceStatInfos[nodeRole][diskType]; !ok {
				spaceStatInfos[nodeRole][diskType] = &clustermgr.SpaceStatInfo{}
			}
			spaceStatInfo := spaceStatInfos[nodeRole][diskType]
			diskStatInfo := make(map[string]*clustermgr.DiskStatInfo)
			for i := range d.IDC {
				diskStatInfo[d.IDC[i]] = &clustermgr.DiskStatInfo{IDC: d.IDC[i]}
			}

			for _, nodeSet := range nodeSets {
				nodeSetAllocator := newNodeSetAllocator(nodeSet.ID())
				for _, diskSet := range nodeSet.GetDiskSets() {
					disks := diskSet.GetDisks()
					// ecDiskSet[diskType] = append(ecDiskSet[diskType], disks...)
					idcAllocators, diskSetFreeChunk := d.generateDiskSetStorage(ctx, disks, spaceStatInfo, diskStatInfo)
					diskSetAllocator := newDiskSetAllocator(diskSet.ID(), diskSetFreeChunk, idcAllocators)
					diskSetAllocators[nodeRole][diskSet.ID()] = diskSetAllocator
					nodeSetAllocator.addDiskSet(diskSetAllocator)
				}
				nodeSetAllocators[nodeRole][diskType][nodeSet.ID()] = nodeSetAllocator
			}

			for idc := range diskStatInfo {
				spaceStatInfo.DisksStatInfos = append(spaceStatInfo.DisksStatInfos, *diskStatInfo[idc])
			}
		}

		// compatible
		if nodeRole == proto.NodeRoleBlobNode {
			allDisks := d.getAllDisk()
			span.Debugf("get all disks, len:%d", len(allDisks))
			diskTypeDisks := make(map[proto.DiskType][]*diskItem)
			for _, disk := range allDisks {
				diskType := d.getDiskType(disk)
				diskTypeDisks[diskType] = append(diskTypeDisks[diskType], disk)
			}

			for diskType := range diskTypeDisks {
				ecDiskSet[diskType] = diskTypeDisks[diskType]
				ecSpaceStateInfo := &clustermgr.SpaceStatInfo{}
				diskStatInfo := make(map[string]*clustermgr.DiskStatInfo)
				for i := range d.IDC {
					diskStatInfo[d.IDC[i]] = &clustermgr.DiskStatInfo{IDC: d.IDC[i]}
				}

				ecIdcAllocators, ecFreeChunk := d.generateDiskSetStorage(ctx, ecDiskSet[diskType], ecSpaceStateInfo, diskStatInfo)

				// initial ec allocator
				diskSetAllocator := newDiskSetAllocator(ECDiskSetID, ecFreeChunk, ecIdcAllocators)
				diskSetAllocators[nodeRole][ECDiskSetID] = diskSetAllocator
				nodeSetAllocator := newNodeSetAllocator(ECNodeSetID)
				nodeSetAllocator.addDiskSet(diskSetAllocator)
				if _, ok := nodeSetAllocators[nodeRole][diskType]; !ok {
					nodeSetAllocators[nodeRole][diskType] = make(nodeSetAllocatorMap)
				}
				nodeSetAllocators[nodeRole][diskType][ECNodeSetID] = nodeSetAllocator
				span.Debugf("add ec nodeset")

				// update space state info
				for idc := range diskStatInfo {
					ecSpaceStateInfo.DisksStatInfos = append(ecSpaceStateInfo.DisksStatInfos, *diskStatInfo[idc])
				}
				// no copy set register, set space info and disk stat info by ec statistic
				if len(nodeSetsMap) == 0 {
					spaceStatInfos[nodeRole][diskType] = ecSpaceStateInfo
					continue
				}

				// TODO: calculate writable space by replicate code mode and ec code mode ratio
				spaceStatInfos[nodeRole][diskType].WritableSpace = ecSpaceStateInfo.WritableSpace
			}
		}

		if d.allocators[nodeRole] == nil {
			d.allocators[nodeRole] = &atomic.Value{}
		}

		d.allocators[nodeRole].Store(newAllocator(allocatorConfig{
			nodeSets: nodeSetAllocators[nodeRole],
			diskSets: diskSetAllocators[nodeRole],
			dg:       d,
			tg:       d.topoMgrs[nodeRole],
			diffHost: d.HostAware,
			diffRack: d.RackAware,
		}))
	}

	d.spaceStatInfo.Store(spaceStatInfos)
}

func (d *DiskMgr) generateDiskSetStorage(ctx context.Context, disks []*diskItem, spaceStatInfo *clustermgr.SpaceStatInfo,
	diskStatInfosM map[string]*clustermgr.DiskStatInfo,
) (ret map[string]*idcAllocator, freeChunk int64) {
	span := trace.SpanFromContextSafe(ctx)
	blobNodeStgs := make(map[string]*blobNodeAllocator)
	idcFreeChunks := make(map[string]int64)
	idcRackStgs := make(map[string]map[string]*rackAllocator)
	idcBlobNodeStgs := make(map[string][]*blobNodeAllocator)
	rackBlobNodeStgs := make(map[string][]*blobNodeAllocator)
	rackFreeChunks := make(map[string]int64)

	for _, disk := range disks {
		// read one disk info
		disk.lock.RLock()
		idc := disk.info.Idc
		rack := disk.info.Rack
		host := disk.info.Host
		if node, ok := d.getNode(disk.info.NodeID); ok {
			idc = node.info.Idc
			rack = node.info.Rack
			host = node.info.Host
		}
		freeChunk := disk.info.FreeChunkCnt
		maxChunk := disk.info.MaxChunkCnt
		readonly := disk.info.Readonly
		size := disk.info.Size
		free := disk.info.Free
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
		diskStatInfosM[idc].TotalFreeChunk += freeChunk
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
			disk.lock.RUnlock()
			continue
		}
		spaceStatInfo.TotalSpace += size
		if readonly { // include dropping disk
			spaceStatInfo.ReadOnlySpace += free
			disk.lock.RUnlock()
			continue
		}
		spaceStatInfo.FreeSpace += free
		diskStatInfosM[idc].Available += 1

		// filter expired disk
		if disk.isExpire() {
			disk.lock.RUnlock()
			diskStatInfosM[idc].Expired += 1
			continue
		}
		disk.lock.RUnlock()

		// build for idcRackStorage
		if _, ok := idcRackStgs[idc]; !ok {
			idcRackStgs[idc] = make(map[string]*rackAllocator)
		}
		if _, ok := idcRackStgs[idc][rack]; !ok {
			idcRackStgs[idc][rack] = &rackAllocator{rack: rack}
		}
		// build for idcAllocator
		if _, ok := idcBlobNodeStgs[idc]; !ok {
			idcBlobNodeStgs[idc] = make([]*blobNodeAllocator, 0)
			idcFreeChunks[idc] = 0
		}
		idcFreeChunks[idc] += freeChunk
		// build for rackAllocator
		if _, ok := rackBlobNodeStgs[rack]; !ok {
			rackBlobNodeStgs[rack] = make([]*blobNodeAllocator, 0)
			rackFreeChunks[rack] = 0
		}
		rackFreeChunks[rack] += freeChunk
		// build for blobNodeAllocator
		if _, ok := blobNodeStgs[host]; !ok {
			blobNodeStgs[host] = &blobNodeAllocator{host: host, disks: make([]*diskItem, 0)}
			// append idc data node
			idcBlobNodeStgs[idc] = append(idcBlobNodeStgs[idc], blobNodeStgs[host])
			// append rack data node
			rackBlobNodeStgs[rack] = append(rackBlobNodeStgs[rack], blobNodeStgs[host])
		}
		blobNodeStgs[host].disks = append(blobNodeStgs[host].disks, disk)
		blobNodeStgs[host].freeChunk += freeChunk
		blobNodeStgs[host].free += free
	}

	span.Debugf("all blobNodeStgs: %+v", blobNodeStgs)
	for _, rackStgs := range idcRackStgs {
		for rack := range rackStgs {
			rackStgs[rack].freeChunk = rackFreeChunks[rack]
			rackStgs[rack].blobNodeStorages = rackBlobNodeStgs[rack]
		}
	}
	for idc := range idcBlobNodeStgs {
		span.Infof("%s idcBlobNodeStgs length: %d", idc, len(idcBlobNodeStgs[idc]))
	}

	spaceStatInfo.UsedSpace = spaceStatInfo.TotalSpace - spaceStatInfo.FreeSpace - spaceStatInfo.ReadOnlySpace

	if len(idcRackStgs) > 0 {
		ret = make(map[string]*idcAllocator)
		for i := range d.IDC {
			spaceStatInfo.TotalBlobNode += int64(len(idcBlobNodeStgs[d.IDC[i]]))
			ret[d.IDC[i]] = &idcAllocator{
				idc:              d.IDC[i],
				freeChunk:        idcFreeChunks[d.IDC[i]],
				diffRack:         d.RackAware,
				diffHost:         d.HostAware,
				rackStorages:     idcRackStgs[d.IDC[i]],
				blobNodeStorages: idcBlobNodeStgs[d.IDC[i]],
			}
			freeChunk += idcFreeChunks[d.IDC[i]]
		}
		spaceStatInfo.WritableSpace += d.calculateWritable(idcBlobNodeStgs)
	}

	return
}

func (d *DiskMgr) calculateWritable(idcBlobNodeStgs map[string][]*blobNodeAllocator) int64 {
	// writable space statistic
	codeMode, suCount := d.getMaxSuCount()
	idcSuCount := suCount / len(d.IDC)
	if d.HostAware && len(idcBlobNodeStgs) > 0 {
		// calculate minimum idc writable chunk num
		calIDCWritableFunc := func(stgs []*blobNodeAllocator) int64 {
			stripe := make([]int64, idcSuCount)
			lefts := make(maxHeap, 0)
			n := int64(0)
			for _, v := range stgs {
				count := v.free / d.ChunkSize
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
		return minimumStripeCount * int64(codeMode.Tactic().N) * d.ChunkSize
	}

	if len(idcBlobNodeStgs) > 0 {
		minimumChunkNum := int64(math.MaxInt64)
		for idc := range idcBlobNodeStgs {
			idcChunkNum := int64(0)
			for i := range idcBlobNodeStgs[idc] {
				idcChunkNum += idcBlobNodeStgs[idc][i].free / d.ChunkSize
			}
			if idcChunkNum < minimumChunkNum {
				minimumChunkNum = idcChunkNum
			}
		}
		return minimumChunkNum / int64(idcSuCount) * int64(codeMode.Tactic().N) * d.ChunkSize
	}

	return 0
}

func (d *DiskMgr) getMaxSuCount() (codemode.CodeMode, int) {
	suCount := 0
	idx := 0
	for i := range d.CodeModes {
		codeModeInfo := d.CodeModes[i].Tactic()
		if codeModeInfo.N+codeModeInfo.M+codeModeInfo.L > suCount {
			idx = i
			suCount = codeModeInfo.N + codeModeInfo.M + codeModeInfo.L
		}
	}
	return d.CodeModes[idx], suCount
}
