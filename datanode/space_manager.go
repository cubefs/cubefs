// Copyright 2018 The CFS Authors.
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

package datanode

import (
	"fmt"
	"sync"
	"time"

	"os"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/log"
)

// SpaceManager manages the disk space
type SpaceManager struct {
	clusterID         string
	disks             map[string]*Disk
	partitions        map[uint64]*DataPartition
	raftStore         raftstore.RaftStore
	nodeID            uint64
	diskMu            sync.RWMutex  // TODO change the name to dMutex
	partitionMu       sync.RWMutex  // TODO change the name to pMutex
	stats             *Stats
	stopC             chan bool
	chooseIndex       int
	diskList          []string
	createPartitionMu sync.RWMutex
}

// NewSpaceManager creates a new space manager.
func NewSpaceManager(rack string) *SpaceManager {
	var space *SpaceManager
	space = &SpaceManager{}
	space.disks = make(map[string]*Disk)
	space.diskList = make([]string, 0)
	space.partitions = make(map[uint64]*DataPartition)
	space.stats = NewStats(rack)
	space.stopC = make(chan bool, 0)

	go space.statUpdateScheduler()
	go space.flushDeleteScheduler()

	return space
}

func (space *SpaceManager) Stop() {
	defer func() {
		recover()
	}()
	close(space.stopC)
}

func (space *SpaceManager) SetNodeID(nodeID uint64) {
	space.nodeID = nodeID
}

func (space *SpaceManager) GetNodeID() (nodeID uint64) {
	return space.nodeID
}

func (space *SpaceManager) SetClusterID(clusterID string) {
	space.clusterID = clusterID
}

func (space *SpaceManager) GetClusterID() (clusterID string) {
	return space.clusterID
}

func (space *SpaceManager) SetRaftStore(raftStore raftstore.RaftStore) {
	space.raftStore = raftStore
}
func (space *SpaceManager) GetRaftStore() (raftStore raftstore.RaftStore) {
	return space.raftStore
}

func (space *SpaceManager) RangePartitions(f func(partition *DataPartition) bool) {
	if f == nil {
		return
	}
	space.partitionMu.RLock()
	partitions := make([]*DataPartition, 0)
	for _, dp := range space.partitions {
		partitions = append(partitions, dp)
	}
	space.partitionMu.RUnlock()

	for _, partition := range partitions {
		if !f(partition) {
			break
		}
	}
}

func (space *SpaceManager) GetDisks() (disks []*Disk) {
	space.diskMu.RLock()
	defer space.diskMu.RUnlock()
	disks = make([]*Disk, 0)
	for _, disk := range space.disks {
		disks = append(disks, disk)
	}
	return
}

func (space *SpaceManager) Stats() *Stats {
	return space.stats
}

func (space *SpaceManager) LoadDisk(path string, restSize uint64, maxErrs int) (err error) {
	var (
		disk    *Disk
		visitor PartitionVisitor
	)
	log.LogDebugf("action[LoadDisk] load disk from path(%v).", path)
	visitor = func(dp *DataPartition) {
		space.partitionMu.Lock()
		defer space.partitionMu.Unlock()
		if _, has := space.partitions[dp.ID()]; !has {
			space.partitions[dp.ID()] = dp
			log.LogDebugf("action[LoadDisk] put partition(%v) to space manager.", dp.ID())
		}
	}
	if _, err = space.GetDisk(path); err != nil {

		disk = NewDisk(path, restSize, maxErrs, space)
		disk.RestorePartition(visitor)
		space.putDisk(disk)
		err = nil
	}
	return
}

func (space *SpaceManager) GetDisk(path string) (d *Disk, err error) {
	space.diskMu.RLock()
	defer space.diskMu.RUnlock()
	disk, has := space.disks[path]
	if has && disk != nil {
		d = disk
		return
	}
	err = fmt.Errorf("disk(%v) not exsit", path)
	return
}

func (space *SpaceManager) putDisk(d *Disk) {
	space.diskMu.Lock()
	space.disks[d.Path] = d
	space.diskList = append(space.diskList, d.Path)
	space.diskMu.Unlock()

}

func (space *SpaceManager) updateMetrics() {
	space.diskMu.RLock()
	var (
		total, used, available                                   uint64
		createdPartitionWeights, remainWeightsForCreatePartition uint64
		maxWeightsForCreatePartition, partitionCnt               uint64
	)
	maxWeightsForCreatePartition = 0
	for _, d := range space.disks {
		total += d.Total
		used += d.Used
		available += d.Available
		createdPartitionWeights += d.Allocated
		remainWeightsForCreatePartition += d.Unallocated
		partitionCnt += uint64(d.PartitionCount())
		if maxWeightsForCreatePartition < d.Unallocated {
			maxWeightsForCreatePartition = d.Unallocated
		}
	}
	space.diskMu.RUnlock()
	log.LogDebugf("action[updateMetrics] total(%v) used(%v) available(%v) createdPartitionWeights(%v)  remainWeightsForCreatePartition(%v) "+
		"partitionCnt(%v) maxWeightsForCreatePartition(%v) ", total, used, available, createdPartitionWeights, remainWeightsForCreatePartition, partitionCnt, maxWeightsForCreatePartition)
	space.stats.updateMetrics(total, used, available, createdPartitionWeights,
		remainWeightsForCreatePartition, maxWeightsForCreatePartition, partitionCnt)
}

func (space *SpaceManager) getMinPartitionCntDisk() (d *Disk) {
	space.diskMu.Lock()
	defer space.diskMu.Unlock()
	var path string
	if space.chooseIndex >= len(space.diskList) {
		space.chooseIndex = 0
	}

	path = space.diskList[space.chooseIndex]
	d = space.disks[path]
	space.chooseIndex++

	return
}

func (space *SpaceManager) flushDeleteScheduler() {
	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		for {
			select {
			case <-ticker.C:
				space.flushDelete()
			case <-space.stopC:
				ticker.Stop()
				return
			}
		}
	}()
}

func (space *SpaceManager) statUpdateScheduler() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				space.updateMetrics()
			case <-space.stopC:
				ticker.Stop()
				return
			}
		}
	}()
}

func (space *SpaceManager) flushDelete() {
	partitions := make([]*DataPartition, 0)
	space.RangePartitions(func(dp *DataPartition) bool {
		partitions = append(partitions, dp)
		return true
	})
	for _, partition := range partitions {
		partition.FlushDelete()
	}
}

func (space *SpaceManager) Partition(partitionID uint64) (dp *DataPartition) {
	space.partitionMu.RLock()
	defer space.partitionMu.RUnlock()
	dp = space.partitions[partitionID]

	return
}

func (space *SpaceManager) putPartition(dp *DataPartition) {
	space.partitionMu.Lock()
	defer space.partitionMu.Unlock()
	space.partitions[dp.ID()] = dp
	return
}

func (space *SpaceManager) CreatePartition(request *proto.CreateDataPartitionRequest) (dp *DataPartition, err error) {
	space.partitionMu.Lock()
	defer space.partitionMu.Unlock()
	dpCfg := &dataPartitionCfg{
		PartitionID:   request.PartitionId,
		VolName:       request.VolumeId,
		Peers:         request.Members,
		RaftStore:     space.raftStore,
		NodeID:        space.nodeID,
		ClusterID:     space.clusterID,
		PartitionSize: request.PartitionSize,
		RandomWrite:   request.RandomWrite,
	}
	dp = space.partitions[dpCfg.PartitionID]
	if dp != nil {
		return
	}
	var (
		disk *Disk
	)
	for i := 0; i < len(space.disks); i++ {
		disk = space.getMinPartitionCntDisk()
		if disk.Available < uint64(dpCfg.PartitionSize) {
			disk = nil
			continue
		}
		break
	}
	if disk == nil {
		return nil, ErrNoSpaceToCreatePartition
	}
	if dp, err = CreateDataPartition(dpCfg, disk); err != nil {
		return
	}

	space.partitions[dp.ID()] = dp
	go dp.ForceLoadHeader()

	return
}

// DeletePartition deletes a partition based on the partition id.
func (space *SpaceManager) DeletePartition(dpID uint64) {
	dp := space.Partition(dpID)
	if dp == nil {
		return
	}
	space.partitionMu.Lock()
	delete(space.partitions, dpID)
	space.partitionMu.Unlock()
	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	os.RemoveAll(dp.Path())
}

// TODO change the name to initHeartbeatResponse or newHeartbeatResponse ?
func (s *DataNode) buildHeartBeatResponse(response *proto.DataNodeHeartBeatResponse) {
	response.Status = proto.TaskSuccess
	stat := s.space.Stats()
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.CreatedPartitionWeights = stat.CreatedPartitionWeights
	response.MaxWeightsForCreatePartition = stat.MaxWeightsForCreatePartition
	response.RemainWeightsForCreatePartition = stat.RemainingWeightsForCreatePartition
	stat.Unlock()

	response.RackName = s.rackName
	response.PartitionInfo = make([]*proto.PartitionReport, 0)
	space := s.space
	space.RangePartitions(func(partition *DataPartition) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		vr := &proto.PartitionReport{
			PartitionID:     uint64(partition.ID()),
			PartitionStatus: partition.Status(),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
			DiskPath:        partition.Disk().Path,
			IsLeader:        isLeader,
			ExtentCount:     partition.ExtentStore().ExtentCount(),
			NeedCompare:     partition.LoadExtentHeaderStatus() == FinishLoadingExtentHeader,
		}
		log.LogDebugf("action[Heartbeats] dpid[%v], status[%v] total[%v] used[%v] leader[%v] b[%v].", vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader)
		response.PartitionInfo = append(response.PartitionInfo, vr)
		return true
	})
}
