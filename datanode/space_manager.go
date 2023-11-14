// Copyright 2018 The CubeFS Authors.
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
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/async"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/tiglabs/raft/util"
)

// SpaceManager manages the disk space.
type SpaceManager struct {
	clusterID            string
	disks                map[string]*Disk
	partitions           map[uint64]*DataPartition
	raftStore            raftstore.RaftStore
	nodeID               uint64
	diskMutex            sync.RWMutex
	partitionMutex       sync.RWMutex
	stats                *Stats
	stopC                chan bool
	selectedIndex        int // TODO what is selected index
	diskList             []string
	dataNode             *DataNode
	createPartitionMutex sync.RWMutex // 该锁用于控制Partition创建的并发，保证同一时间只处理一个Partition的创建操作

	// Parallel task limits on disk
	fixTinyDeleteRecordLimitOnDisk uint64
	repairTaskLimitOnDisk          uint64
	normalExtentDeleteExpireTime   uint64
	flushFDIntervalSec             uint32
	flushFDParallelismOnDisk       uint64

	limiter *multirate.MultiLimiter
}

// NewSpaceManager creates a new space manager.
func NewSpaceManager(dataNode *DataNode) *SpaceManager {
	var space = &SpaceManager{
		disks:                          make(map[string]*Disk),
		diskList:                       make([]string, 0),
		partitions:                     make(map[uint64]*DataPartition),
		stats:                          NewStats(dataNode.zoneName),
		stopC:                          make(chan bool, 0),
		dataNode:                       dataNode,
		fixTinyDeleteRecordLimitOnDisk: DefaultFixTinyDeleteRecordLimitOnDisk,
		repairTaskLimitOnDisk:          DefaultRepairTaskLimitOnDisk,
		normalExtentDeleteExpireTime:   DefaultNormalExtentDeleteExpireTime,
		limiter:                        dataNode.limiter,
	}
	async.RunWorker(space.statUpdateScheduler, func(i interface{}) {
		log.LogCriticalf("SPCMGR: stat update scheduler occurred panic: %v\nCallstack:\n%v",
			i, string(debug.Stack()))
	})
	return space
}

func (manager *SpaceManager) AsyncLoadExtent() {

	var disks = manager.GetDisks()
	var wg = new(sync.WaitGroup)
	if log.IsInfoEnabled() {
		log.LogInfof("SPCMGR: lazy load start")
	}
	var start = time.Now()
	for _, disk := range disks {
		wg.Add(1)
		go func(disk *Disk) {
			defer wg.Done()
			disk.AsyncLoadExtent(DefaultLazyLoadParallelismPerDisk)
		}(disk)
	}
	wg.Wait()
	if log.IsInfoEnabled() {
		log.LogInfof("SPCMGR: lazy load complete, elapsed %v", time.Now().Sub(start))
	}
	gHasLoadDataPartition = true
}

func (manager *SpaceManager) Stop() {
	defer func() {
		recover()
	}()
	close(manager.stopC)
	// 并行关闭所有Partition并释放空间, 并行度为64
	const maxParallelism = 128
	var parallelism = int(math.Min(float64(maxParallelism), float64(len(manager.partitions))))
	wg := sync.WaitGroup{}
	partitionC := make(chan *DataPartition, parallelism)
	wg.Add(1)
	go func(c chan<- *DataPartition) {
		defer wg.Done()
		manager.WalkPartitions(func(partition *DataPartition) bool {
			c <- partition
			return true
		}) // WalkPartitions 方法内部采用局部读锁结构，既不会产生map线程安全问题由不会长时间占用锁.
		close(c)
	}(partitionC)

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(c <-chan *DataPartition) {
			defer wg.Done()
			var partition *DataPartition
			for {
				if partition = <-c; partition == nil {
					return
				}
				partition.Stop()
			}
		}(partitionC)
	}
	wg.Wait()
}

func (manager *SpaceManager) SetNodeID(nodeID uint64) {
	manager.nodeID = nodeID
}

func (manager *SpaceManager) GetNodeID() (nodeID uint64) {
	return manager.nodeID
}

func (manager *SpaceManager) SetClusterID(clusterID string) {
	manager.clusterID = clusterID
}

func (manager *SpaceManager) GetClusterID() (clusterID string) {
	return manager.clusterID
}

func (manager *SpaceManager) SetRaftStore(raftStore raftstore.RaftStore) {
	manager.raftStore = raftStore
}
func (manager *SpaceManager) GetRaftStore() (raftStore raftstore.RaftStore) {
	return manager.raftStore
}

func (manager *SpaceManager) GetPartitions() (partitions []*DataPartition) {
	manager.partitionMutex.RLock()
	partitions = make([]*DataPartition, 0, len(manager.partitions))
	for _, dp := range manager.partitions {
		partitions = append(partitions, dp)
	}
	manager.partitionMutex.RUnlock()
	return
}

func (manager *SpaceManager) WalkPartitions(visitor func(partition *DataPartition) bool) {
	if visitor == nil {
		return
	}
	for _, partition := range manager.GetPartitions() {
		if !visitor(partition) {
			break
		}
	}
}

func (manager *SpaceManager) GetDisks() (disks []*Disk) {
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	disks = make([]*Disk, 0)
	for _, disk := range manager.disks {
		disks = append(disks, disk)
	}
	return
}

func (manager *SpaceManager) WalkDisks(visitor func(*Disk) bool) {
	if visitor == nil {
		return
	}
	for _, disk := range manager.GetDisks() {
		if !visitor(disk) {
			break
		}
	}
}

func (manager *SpaceManager) Stats() *Stats {
	return manager.stats
}

func (manager *SpaceManager) LoadDisk(path *DiskPath, expired CheckExpired) (err error) {
	var (
		disk *Disk
	)
	if _, exists := manager.GetDisk(path); !exists {
		var config = &DiskConfig{
			Reserved:          path.Reserved(),
			UsableRatio:       unit.NewRatio(DefaultDiskUseRatio),
			MaxErrCnt:         DefaultDiskMaxErr,
			MaxFDLimit:        DiskMaxFDLimit,
			ForceFDEvictRatio: DiskForceEvictFDRatio,

			FixTinyDeleteRecordLimit: manager.fixTinyDeleteRecordLimitOnDisk,
			RepairTaskLimit:          manager.repairTaskLimitOnDisk,
		}
		var startTime = time.Now()
		if disk, err = OpenDisk(path.Path(), config, manager, DiskLoadPartitionParallelism, manager.limiter, expired); err != nil {
			return
		}
		manager.putDisk(disk)
		var count int
		disk.WalkPartitions(func(u uint64, partition *DataPartition) bool {
			manager.AttachPartition(partition)
			count++
			return true
		})
		log.LogInfof("Disk %v: load compete: partitions=%v, elapsed=%v", path, count, time.Since(startTime))
		err = nil
	}
	deleteSysStartTimeFile()
	_ = initSysStartTimeFile()
	return
}

func (manager *SpaceManager) StartPartitions() {
	var err error
	partitions := make([]*DataPartition, 0)
	manager.partitionMutex.RLock()
	for _, partition := range manager.partitions {
		partitions = append(partitions, partition)
	}
	manager.partitionMutex.RUnlock()

	var (
		wg sync.WaitGroup
	)
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer wg.Done()
			if err = dp.Start(); err != nil {
				manager.partitionMutex.Lock()
				delete(manager.partitions, dp.ID())
				manager.partitionMutex.Unlock()
				dp.Disk().DetachDataPartition(dp)
				msg := fmt.Sprintf("partition [id: %v, disk: %v] start failed: %v", dp.ID(), dp.Disk().Path, err)
				log.LogErrorf(msg)
				exporter.Warning(msg)
			}
		}(dp)
	}
	wg.Wait()
}

func (manager *SpaceManager) GetDisk(path *DiskPath) (d *Disk, exists bool) {
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	disk, has := manager.disks[path.Path()]
	if has && disk != nil {
		d = disk
		exists = true
		return
	}
	return
}

func (manager *SpaceManager) putDisk(d *Disk) {
	manager.diskMutex.Lock()
	manager.disks[d.Path] = d
	manager.diskList = append(manager.diskList, d.Path)
	manager.diskMutex.Unlock()

}

func (manager *SpaceManager) updateMetrics() {
	manager.diskMutex.RLock()
	var (
		total, used, available                                 uint64
		totalPartitionSize, remainingCapacityToCreatePartition uint64
		maxCapacityToCreatePartition, partitionCnt             uint64
	)
	maxCapacityToCreatePartition = 0
	for _, d := range manager.disks {
		total += d.Total
		used += d.Used
		available += d.Available
		totalPartitionSize += d.Allocated
		remainingCapacityToCreatePartition += d.Unallocated
		partitionCnt += uint64(d.PartitionCount())
		if maxCapacityToCreatePartition < d.Unallocated {
			maxCapacityToCreatePartition = d.Unallocated
		}
	}
	manager.diskMutex.RUnlock()
	log.LogDebugf("action[updateMetrics] total(%v) used(%v) available(%v) totalPartitionSize(%v)  remainingCapacityToCreatePartition(%v) "+
		"partitionCnt(%v) maxCapacityToCreatePartition(%v) ", total, used, available, totalPartitionSize, remainingCapacityToCreatePartition, partitionCnt, maxCapacityToCreatePartition)
	manager.stats.updateMetrics(total, used, available, totalPartitionSize,
		remainingCapacityToCreatePartition, maxCapacityToCreatePartition, partitionCnt)
}

func (manager *SpaceManager) minPartitionCnt() (d *Disk) {
	manager.diskMutex.Lock()
	defer manager.diskMutex.Unlock()
	var (
		minWeight     float64
		minWeightDisk *Disk
	)
	minWeight = math.MaxFloat64
	for _, disk := range manager.disks {
		if disk.Available <= 5*unit.GB || disk.Status != proto.ReadWrite {
			continue
		}
		diskWeight := disk.getSelectWeight()
		if diskWeight < minWeight {
			minWeight = diskWeight
			minWeightDisk = disk
		}
	}
	if minWeightDisk == nil {
		return
	}
	if minWeightDisk.Available <= 5*unit.GB || minWeightDisk.Status != proto.ReadWrite {
		return
	}
	d = minWeightDisk
	return d
}
func (manager *SpaceManager) statUpdateScheduler() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				manager.updateMetrics()
			case <-manager.stopC:
				ticker.Stop()
				return
			}
		}
	}()
}

func (manager *SpaceManager) Partition(partitionID uint64) (dp *DataPartition) {
	manager.partitionMutex.RLock()
	defer manager.partitionMutex.RUnlock()
	dp = manager.partitions[partitionID]

	return
}

func (manager *SpaceManager) AttachPartition(dp *DataPartition) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()
	manager.partitions[dp.ID()] = dp
}

// DetachDataPartition removes a data partition from the partition map.
func (manager *SpaceManager) DetachDataPartition(partitionID uint64) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()
	delete(manager.partitions, partitionID)
}

func (manager *SpaceManager) CreatePartition(request *proto.CreateDataPartitionRequest) (dp *DataPartition, err error) {
	// 保证同一时间只处理一个Partition的创建操作
	manager.createPartitionMutex.Lock()
	defer manager.createPartitionMutex.Unlock()
	dpCfg := &dataPartitionCfg{
		PartitionID:   request.PartitionId,
		VolName:       request.VolumeId,
		Peers:         request.Members,
		Hosts:         request.Hosts,
		Learners:      request.Learners,
		RaftStore:     manager.raftStore,
		NodeID:        manager.nodeID,
		ClusterID:     manager.clusterID,
		PartitionSize: request.PartitionSize,
		ReplicaNum:    request.ReplicaNum,

		VolHAType: request.VolumeHAType,
	}
	dp = manager.Partition(dpCfg.PartitionID)
	if dp != nil {
		if err = dp.IsEquareCreateDataPartitionRequst(request); err != nil {
			return nil, err
		}
		return
	}
	disk := manager.minPartitionCnt()
	if disk == nil {
		return nil, ErrNoSpaceToCreatePartition
	}
	if dp, err = disk.createPartition(dpCfg, request); err != nil {
		return
	}
	disk.AttachDataPartition(dp)
	manager.AttachPartition(dp)
	if err = dp.Start(); err != nil {
		return
	}
	return
}

// DeletePartition deletes a partition based on the partition id.
func (manager *SpaceManager) DeletePartition(dpID uint64) {
	dp := manager.Partition(dpID)
	if dp == nil {
		return
	}
	manager.partitionMutex.Lock()
	delete(manager.partitions, dpID)
	manager.partitionMutex.Unlock()
	dp.Delete()
}

// ExpiredPartition marks specified partition as expired.
// It renames data path to a new name which add 'expired_' as prefix and operation timestamp as suffix.
// (e.g. '/disk0/datapartition_1_128849018880' to '/disk0/deleted_datapartition_1_128849018880_1600054521')
func (manager *SpaceManager) ExpiredPartition(partitionID uint64) {
	dp := manager.Partition(partitionID)
	if dp == nil {
		return
	}
	manager.partitionMutex.Lock()
	delete(manager.partitions, partitionID)
	manager.partitionMutex.Unlock()
	dp.Expired()
}

func (manager *SpaceManager) LoadPartition(d *Disk, partitionID uint64, partitionPath string) (err error) {
	var partition *DataPartition
	if err = d.RestoreOnePartition(partitionPath); err != nil {
		return
	}

	defer func() {
		if err != nil {
			manager.DetachDataPartition(partitionID)
			partition.Disk().DetachDataPartition(partition)
			msg := fmt.Sprintf("partition [id: %v, disk: %v] start failed: %v", partition.ID(), partition.Disk().Path, err)
			log.LogErrorf(msg)
			exporter.Warning(msg)
		}
	}()

	partition = manager.Partition(partitionID)
	if partition == nil {
		return fmt.Errorf("partition not exist")
	}

	if err = partition.ChangeCreateType(proto.DecommissionedCreateDataPartition); err != nil {
		return
	}

	// start raft
	if err = partition.Start(); err != nil {
		return
	}
	async.RunWorker(partition.ExtentStore().Load, func(i interface{}) {
		log.LogCriticalf("SPCMGR: DP %v: lazy load occurred panic: %v\nCallStack:\n%v",
			partition.ID(), i, string(debug.Stack()))
	})
	return
}

func (manager *SpaceManager) SyncPartitionReplicas(partitionID uint64, hosts []string) {
	dp := manager.Partition(partitionID)
	if dp == nil {
		return
	}
	dp.SyncReplicaHosts(hosts)
	return
}

// DeletePartition deletes a partition from cache based on the partition id.
func (manager *SpaceManager) DeletePartitionFromCache(dpID uint64) {
	dp := manager.Partition(dpID)
	if dp == nil {
		return
	}
	manager.partitionMutex.Lock()
	delete(manager.partitions, dpID)
	manager.partitionMutex.Unlock()
	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
}

func (s *DataNode) buildHeartBeatResponse(response *proto.DataNodeHeartbeatResponse) {
	response.Status = proto.TaskSucceeds
	response.Version = DataNodeLatestVersion
	stat := s.space.Stats()
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.TotalPartitionSize = stat.TotalPartitionSize
	response.MaxCapacity = stat.MaxCapacityToCreatePartition
	response.RemainingCapacity = stat.RemainingCapacityToCreatePartition
	response.BadDisks = make([]string, 0)
	response.DiskInfos = make(map[string]*proto.DiskInfo, 0)
	stat.Unlock()

	response.HttpPort = s.httpPort
	response.ZoneName = s.zoneName
	response.PartitionReports = make([]*proto.PartitionReport, 0)
	space := s.space
	space.WalkPartitions(func(partition *DataPartition) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		vr := &proto.PartitionReport{
			VolName:         partition.volumeID,
			PartitionID:     partition.ID(),
			PartitionStatus: partition.Status(),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
			DiskPath:        partition.Disk().Path,
			IsLeader:        isLeader,
			ExtentCount:     partition.GetExtentCount(),
			NeedCompare:     true,
			IsLearner:       partition.IsRaftLearner(),
			IsRecover:       partition.DataPartitionCreateType == proto.DecommissionedCreateDataPartition,
		}
		log.LogDebugf("action[Heartbeats] dpid(%v), status(%v) total(%v) used(%v) leader(%v) isLeader(%v) isLearner(%v).",
			vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader, vr.IsLearner)
		response.PartitionReports = append(response.PartitionReports, vr)
		return true
	})

	disks := space.GetDisks()
	var usageRatio float64
	for _, d := range disks {
		usageRatio = 0
		if d.Total != 0 {
			usageRatio = float64(d.Used) / float64(d.Total)
		}
		dInfo := &proto.DiskInfo{Total: d.Total, Used: d.Used, ReservedSpace: d.ReservedSpace, Status: d.Status, Path: d.Path, UsageRatio: usageRatio}
		response.DiskInfos[d.Path] = dInfo
		if d.Status == proto.Unavailable {
			response.BadDisks = append(response.BadDisks, d.Path)
		}
	}
}

func (manager *SpaceManager) SetDiskFixTinyDeleteRecordLimit(newValue uint64) {
	if newValue > 0 && manager.fixTinyDeleteRecordLimitOnDisk != newValue {
		log.LogInfof("action[spaceManager] change DiskFixTinyDeleteRecordLimit from(%v) to(%v)", manager.fixTinyDeleteRecordLimitOnDisk, newValue)
		manager.fixTinyDeleteRecordLimitOnDisk = newValue
		manager.diskMutex.Lock()
		for _, disk := range manager.disks {
			disk.SetFixTinyDeleteRecordLimitOnDisk(newValue)
		}
		manager.diskMutex.Unlock()
	}
	return
}

func (manager *SpaceManager) SetForceFlushFDParallelismOnDisk(newValue uint64) {
	if newValue == 0 {
		newValue = DefaultForceFlushFDParallelismOnDisk
	}
	if newValue > 0 && manager.flushFDParallelismOnDisk != newValue {
		log.LogInfof("change ForceFlushFDParallelismOnDisk from %v  to %v", manager.flushFDParallelismOnDisk, newValue)
		manager.flushFDParallelismOnDisk = newValue
		manager.diskMutex.Lock()
		for _, disk := range manager.disks {
			disk.SetForceFlushFDParallelism(newValue)
		}
		manager.diskMutex.Unlock()
	}
}

func (manager *SpaceManager) SetPartitionConsistencyMode(mode proto.ConsistencyMode) {
	if !mode.Valid() {
		return
	}
	manager.partitionMutex.RLock()
	for _, partition := range manager.partitions {
		partition.SetConsistencyMode(mode)
	}
	manager.partitionMutex.RUnlock()
}

const (
	MaxDiskRepairTaskLimit                 = 256
	DefaultForceFlushFDSecond              = 10
	DefaultForceFlushFDParallelismOnDisk   = 5
	DefaultForceFlushDataSizeOnEachHDDDisk = 2 * util.MB
	DefaultForceFlushDataSizeOnEachSSDDisk = 10 * util.MB
	DefaultDeletionConcurrencyOnDisk       = 2
	DefaultIssueFixConcurrencyOnDisk       = 16
)

func (manager *SpaceManager) SetDiskRepairTaskLimit(newValue uint64) {
	if newValue == 0 {
		newValue = MaxDiskRepairTaskLimit
	}
	if newValue > 0 && manager.repairTaskLimitOnDisk != newValue {
		log.LogInfof("action[spaceManager] change DiskRepairTaskLimit from(%v) to(%v)", manager.repairTaskLimitOnDisk, newValue)
		manager.repairTaskLimitOnDisk = newValue
		manager.diskMutex.Lock()
		for _, disk := range manager.disks {
			disk.SetRepairTaskLimitOnDisk(newValue)
		}
		manager.diskMutex.Unlock()
	}
}

func (manager *SpaceManager) SetForceFlushFDInterval(newValue uint32) {
	if newValue == 0 {
		newValue = DefaultForceFlushFDSecond
	}
	if newValue > 0 && manager.flushFDIntervalSec != newValue {
		log.LogInfof("action[spaceManager] change ForceFlushFDInterval from(%v) to(%v)", manager.flushFDIntervalSec, newValue)
		manager.flushFDIntervalSec = newValue
	}
}

func (manager *SpaceManager) SetSyncWALOnUnstableEnableState(enableState bool) {
	if enableState == manager.raftStore.IsSyncWALOnUnstable() {
		return
	}
	log.LogInfof("action[spaceManager] change SyncWALOnUnstableEnableState from(%v) to(%v)", manager.raftStore.IsSyncWALOnUnstable(), enableState)
	manager.raftStore.SetSyncWALOnUnstable(enableState)
}
func (manager *SpaceManager) SetNormalExtentDeleteExpireTime(newValue uint64) {
	if newValue == 0 {
		newValue = DefaultNormalExtentDeleteExpireTime
	}
	if newValue > 0 && manager.normalExtentDeleteExpireTime != newValue {
		log.LogInfof("action[spaceManager] change normalExtentDeleteExpireTime from(%v) to(%v)", manager.normalExtentDeleteExpireTime, newValue)
		manager.normalExtentDeleteExpireTime = newValue
	}
}

func (s *DataNode) buildHeartBeatResponsePb(response *proto.DataNodeHeartbeatResponsePb) {
	response.Status = proto.TaskSucceeds
	response.Version = DataNodeLatestVersion
	stat := s.space.Stats()
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.TotalPartitionSize = stat.TotalPartitionSize
	response.MaxCapacity = stat.MaxCapacityToCreatePartition
	response.RemainingCapacity = stat.RemainingCapacityToCreatePartition
	response.BadDisks = make([]string, 0)
	response.DiskInfos = make(map[string]*proto.DiskInfoPb, 0)
	stat.Unlock()

	response.HttpPort = s.httpPort
	response.ZoneName = s.zoneName
	response.PartitionReports = make([]*proto.PartitionReportPb, 0)
	space := s.space
	space.WalkPartitions(func(partition *DataPartition) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		vr := &proto.PartitionReportPb{
			VolName:         partition.volumeID,
			PartitionID:     partition.ID(),
			PartitionStatus: int32(partition.Status()),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
			DiskPath:        partition.Disk().Path,
			IsLeader:        isLeader,
			ExtentCount:     int32(partition.GetExtentCount()),
			NeedCompare:     true,
			IsLearner:       partition.IsRaftLearner(),
			IsRecover:       partition.DataPartitionCreateType == proto.DecommissionedCreateDataPartition,
		}
		log.LogDebugf("action[Heartbeats] dpid(%v), status(%v) total(%v) used(%v) leader(%v) isLeader(%v) isLearner(%v).",
			vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader, vr.IsLearner)
		response.PartitionReports = append(response.PartitionReports, vr)
		return true
	})

	disks := space.GetDisks()
	var usageRatio float64
	for _, d := range disks {
		usageRatio = 0
		if d.Total != 0 {
			usageRatio = float64(d.Used) / float64(d.Total)
		}
		dInfo := &proto.DiskInfoPb{Total: d.Total, Used: d.Used, ReservedSpace: d.ReservedSpace, Status: int32(d.Status), Path: d.Path, UsageRatio: float32(usageRatio)}
		response.DiskInfos[d.Path] = dInfo
		if d.Status == proto.Unavailable {
			response.BadDisks = append(response.BadDisks, d.Path)
		}
	}
}
