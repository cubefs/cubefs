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
	// space stat info
	spaceStatInfo := &clustermgr.SpaceStatInfo{}

	diskStatInfosM := make(map[string]*clustermgr.DiskStatInfo)
	for i := range d.IDC {
		diskStatInfosM[d.IDC[i]] = &clustermgr.DiskStatInfo{
			IDC: d.IDC[i],
		}
	}

	allDisks := d.getAllDisk()
	span.Info("all disk length: ", len(allDisks))

	blobNodeStgs := make(map[string]*blobNodeStorage)

	idcFreeChunks := make(map[string]int64)
	idcRackStgs := make(map[string]map[string]*rackStorage)
	idcBlobNodeStgs := make(map[string][]*blobNodeStorage)

	rackBlobNodeStgs := make(map[string][]*blobNodeStorage)
	rackFreeChunks := make(map[string]int64)

	// generate idc->rack->blobnode storage and statInfo
	for _, disk := range allDisks {
		// read one disk info
		disk.lock.RLock()
		idc := disk.info.Idc
		rack := disk.info.Rack
		host := disk.info.Host
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
		}
		if disk.dropping {
			diskStatInfosM[idc].Dropping += 1
		}
		// filter unavailable disk
		if !disk.isAvailable() {
			disk.lock.RUnlock()
			continue
		}
		diskStatInfosM[idc].Available += 1
		spaceStatInfo.TotalSpace += size
		spaceStatInfo.FreeSpace += free

		// filter expired disk
		if disk.isExpire() {
			disk.lock.RUnlock()
			diskStatInfosM[idc].Expired += 1
			continue
		}
		disk.lock.RUnlock()

		// build for idcRackStorage
		if _, ok := idcRackStgs[idc]; !ok {
			idcRackStgs[idc] = make(map[string]*rackStorage)
		}
		if _, ok := idcRackStgs[idc][rack]; !ok {
			idcRackStgs[idc][rack] = &rackStorage{rack: rack}
		}
		// build for idcStorage
		if _, ok := idcBlobNodeStgs[idc]; !ok {
			idcBlobNodeStgs[idc] = make([]*blobNodeStorage, 0)
			idcFreeChunks[idc] = 0
		}
		idcFreeChunks[idc] += freeChunk
		// build for rackStorage
		if _, ok := rackBlobNodeStgs[rack]; !ok {
			rackBlobNodeStgs[rack] = make([]*blobNodeStorage, 0)
			rackFreeChunks[rack] = 0
		}
		rackFreeChunks[rack] += freeChunk
		// build for blobNodeStorage
		if _, ok := blobNodeStgs[host]; !ok {
			blobNodeStgs[host] = &blobNodeStorage{host: host, disks: make([]*diskItem, 0)}
			// append idc data node
			idcBlobNodeStgs[idc] = append(idcBlobNodeStgs[idc], blobNodeStgs[host])
			// append rack data node
			rackBlobNodeStgs[rack] = append(rackBlobNodeStgs[rack], blobNodeStgs[host])
		}
		blobNodeStgs[host].disks = append(blobNodeStgs[host].disks, disk)
		blobNodeStgs[host].freeChunk += freeChunk
		blobNodeStgs[host].free += free
	}
	span.Debug("all blobNodeStgs: ", blobNodeStgs)

	for _, rackStgs := range idcRackStgs {
		for rack := range rackStgs {
			rackStgs[rack].freeChunk = rackFreeChunks[rack]
			rackStgs[rack].blobNodeStorages = rackBlobNodeStgs[rack]
		}
	}
	for idc := range idcBlobNodeStgs {
		span.Infof("%s idcBlobNodeStgs length: %d", idc, len(idcBlobNodeStgs[idc]))
	}
	// writable space statistic
	d.calculateWritable(spaceStatInfo, idcBlobNodeStgs)

	if len(idcRackStgs) > 0 {
		// atomic store idc allocator
		for i := range d.IDC {
			spaceStatInfo.TotalBlobNode += int64(len(idcBlobNodeStgs[d.IDC[i]]))
			d.allocators[d.IDC[i]].Store(&idcStorage{idc: d.IDC[i], freeChunk: idcFreeChunks[d.IDC[i]], diffRack: d.RackAware, diffHost: d.HostAware, rackStorages: idcRackStgs[d.IDC[i]], blobNodeStorages: idcBlobNodeStgs[d.IDC[i]]})
		}
	}
	for idc := range diskStatInfosM {
		spaceStatInfo.DisksStatInfos = append(spaceStatInfo.DisksStatInfos, *diskStatInfosM[idc])
	}
	spaceStatInfo.UsedSpace = spaceStatInfo.TotalSpace - spaceStatInfo.FreeSpace
	d.spaceStatInfo.Store(spaceStatInfo)
}

func (d *DiskMgr) calculateWritable(spaceStatInfo *clustermgr.SpaceStatInfo, idcBlobNodeStgs map[string][]*blobNodeStorage) {
	// writable space statistic
	codeMode, suCount := d.getMaxSuCount()
	idcSuCount := suCount / len(d.IDC)
	if d.HostAware && len(idcBlobNodeStgs) > 0 {
		// calculate minimum idc writable chunk num
		calIDCWritableFunc := func(stgs []*blobNodeStorage) int64 {
			stripe := make([]int64, idcSuCount)
			lefts := make(maxHeap, 0)
			n := int64(0)
			for _, v := range idcBlobNodeStgs[d.IDC[0]] {
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
		spaceStatInfo.WritableSpace = minimumStripeCount * int64(codeMode.Tactic().N) * d.ChunkSize
		return
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
		spaceStatInfo.WritableSpace = minimumChunkNum / int64(idcSuCount) * int64(codeMode.Tactic().N) * d.ChunkSize
	}
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
