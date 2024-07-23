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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// refresh use for refreshing storage allocator info and cluster statistic info
func (s *ShardNodeManager) refresh(ctx context.Context) {
	// space and disk stat info
	spaceStatInfos := make(map[proto.DiskType]*clustermgr.SpaceStatInfo)
	// generate diskType -> nodeSet -> diskSet -> idc -> rack -> shardnode storage and statInfo
	nodeSetAllocators := make(map[proto.DiskType]nodeSetAllocatorMap)
	diskSetAllocators := make(map[proto.DiskType]diskSetAllocatorMap)

	nodeSetsMap := s.topoMgr.GetAllNodeSets(ctx)

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
		for i := range s.cfg.IDC {
			diskStatInfo[s.cfg.IDC[i]] = &clustermgr.DiskStatInfo{IDC: s.cfg.IDC[i]}
		}

		for _, nodeSet := range nodeSets {
			nodeSetAllocator := newNodeSetAllocator(nodeSet.ID())
			for _, diskSet := range nodeSet.GetDiskSets() {
				disks := diskSet.GetDisks()
				idcAllocators, diskSetFreeShard := s.generateDiskSetStorage(ctx, disks, spaceStatInfo, diskStatInfo)
				diskSetAllocator := newDiskSetAllocator(diskSet.ID(), int64(diskSetFreeShard), idcAllocators)
				diskSetAllocators[diskType][diskSet.ID()] = diskSetAllocator
				nodeSetAllocator.addDiskSet(diskSetAllocator)
			}
			nodeSetAllocators[diskType][nodeSet.ID()] = nodeSetAllocator
		}
		for idc := range diskStatInfo {
			spaceStatInfo.DisksStatInfos = append(spaceStatInfo.DisksStatInfos, *diskStatInfo[idc])
		}
		spaceStatInfo.TotalShardNode = int64(s.topoMgr.GetNodeNum(diskType))
	}

	s.allocator.Store(newAllocator(allocatorConfig{
		nodeSets: nodeSetAllocators,
		diskSets: diskSetAllocators,
		dg:       s,
		tg:       s.topoMgr,
		diffHost: s.cfg.HostAware,
		diffRack: s.cfg.RackAware,
	}))

	s.spaceStatInfo.Store(spaceStatInfos)
}
