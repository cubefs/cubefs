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
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	syslog "log"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/strutil"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/disk"

	opstat "github.com/cubefs/cubefs/util/stat"
)

const DefaultStopDpLimit = 4

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
	createPartitionMutex sync.RWMutex
	rand                 *rand.Rand
	currentLoadDpCount   int
	currentStopDpCount   int
	diskUtils            map[string]*atomicutil.Float64
	samplerDone          chan struct{}
	allDisksLoaded       bool
	dataNodeIDs          map[string]uint64
	dataNodeIDsMutex     sync.RWMutex
}

const diskSampleDuration = 1 * time.Second

// NewSpaceManager creates a new space manager.
func NewSpaceManager(dataNode *DataNode) *SpaceManager {
	space := &SpaceManager{}
	space.disks = make(map[string]*Disk)
	space.diskList = make([]string, 0)
	space.partitions = make(map[uint64]*DataPartition)
	space.stats = NewStats(dataNode.zoneName)
	space.stopC = make(chan bool)
	space.dataNode = dataNode
	space.rand = rand.New(rand.NewSource(time.Now().Unix()))
	space.currentLoadDpCount = DefaultCurrentLoadDpLimit
	space.currentStopDpCount = DefaultStopDpLimit
	space.diskUtils = make(map[string]*atomicutil.Float64)
	space.dataNodeIDs = make(map[string]uint64)
	go space.statUpdateScheduler()

	return space
}

func (manager *SpaceManager) SetCurrentLoadDpLimit(limit int) {
	if limit != 0 {
		manager.currentLoadDpCount = limit
	}
}

func (manager *SpaceManager) SetCurrentStopDpLimit(limit int) {
	if limit != 0 {
		manager.currentStopDpCount = limit
	}
}

func (manager *SpaceManager) Stop() {
	begin := time.Now()
	defer func() {
		msg := fmt.Sprintf("[Stop] stop space manager using time(%v)", time.Since(begin))
		log.LogInfo(msg)
		syslog.Print(msg)
		auditlog.LogDataNodeOp("SpaceManager", msg, nil)
	}()
	defer func() {
		recover()
	}()
	close(manager.stopC)
	close(manager.samplerDone)

	// Close raft store.
	partitions := manager.getPartitions()
	for _, partition := range partitions {
		partition.stopRaft()
	}

	disks := manager.GetDisks()
	var wg sync.WaitGroup
	for _, d := range disks {
		dps := make([]*DataPartition, 0)

		for _, dp := range partitions {
			if dp.disk == d {
				dps = append(dps, dp)
			}
		}
		wg.Add(1)
		go func(d *Disk, dps []*DataPartition) {
			begin := time.Now()
			defer func() {
				log.LogInfof("[Stop] stop disk(%v) using time(%v) dp cnt(%v)", d.Path, time.Since(begin), len(dps))
			}()

			defer wg.Done()

			var stopWg sync.WaitGroup
			defer func() {
				stopWg.Wait()
			}()

			stopCh := make(chan *DataPartition, manager.currentStopDpCount)
			defer close(stopCh)

			for i := 0; i < manager.currentStopDpCount; i++ {
				stopWg.Add(1)
				go func() {
					defer stopWg.Done()
					for {
						dp, ok := <-stopCh
						if !ok {
							return
						}
						dp.Stop()
					}
				}()
			}

			for _, dp := range dps {
				stopCh <- dp
			}
		}(d, dps)
	}
	wg.Wait()
}

func (manager *SpaceManager) GetAllDiskPartitions() []*disk.PartitionStat {
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	partitions := make([]*disk.PartitionStat, 0, len(manager.disks))
	for _, disk := range manager.disks {
		partition := disk.GetDiskPartition()
		if partition != nil {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (manager *SpaceManager) FillIoUtils(samples map[string]loadutil.DiskIoSample) {
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	for _, sample := range samples {
		util := manager.diskUtils[sample.GetPartition().Device]
		if util != nil {
			util.Store(sample.GetIoUtilPercent())
		}
	}
}

func (manager *SpaceManager) StartDiskSample() {
	manager.samplerDone = make(chan struct{})
	go func() {
		for {
			select {
			case <-manager.samplerDone:
				return
			default:
				partitions := manager.GetAllDiskPartitions()
				samples, err := loadutil.GetDisksIoSample(partitions, diskSampleDuration)
				if err != nil {
					log.LogErrorf("failed to sample disk %v\n", err.Error())
					return
				}
				manager.FillIoUtils(samples)
			}
		}
	}()
}

func (manager *SpaceManager) GetDiskUtils() map[string]float64 {
	utils := make(map[string]float64)
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	for device, used := range manager.diskUtils {
		utils[device] = used.Load()
	}
	return utils
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

func (manager *SpaceManager) RangePartitions(f func(partition *DataPartition, testID string) bool, reqID string) {
	if f == nil {
		return
	}
	begin := time.Now()
	partitions := manager.getPartitions()
	testID := uuid.New().String()
	log.LogDebugf("RangePartitions req(%v) get lock cost %v testID %v", reqID, time.Now().Sub(begin), testID)

	var wg sync.WaitGroup
	partitionsCh := make(chan *DataPartition)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partition := range partitionsCh {
				if !f(partition, testID) {
					break
				}
			}
		}()
	}
	for _, partition := range partitions {
		partitionsCh <- partition
	}
	close(partitionsCh)
	wg.Wait()
	log.LogDebugf("RangePartitions req(%v) traverse dps %v cost %v testID %v", reqID, len(partitions), time.Now().Sub(begin), testID)
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

func (manager *SpaceManager) Stats() *Stats {
	return manager.stats
}

func (manager *SpaceManager) LoadDisk(path string, reservedSpace, diskRdonlySpace uint64, maxErrCnt int,
	diskEnableReadRepairExtentLimit bool,
) (err error) {
	var (
		disk    *Disk
		visitor PartitionVisitor
	)

	if diskRdonlySpace < reservedSpace {
		diskRdonlySpace = reservedSpace
	}

	log.LogDebugf("action[LoadDisk] load disk from path(%v).", path)
	visitor = func(dp *DataPartition) {
		// do noting here, dp is attached to space manager in RestorePartition
	}

	if _, err = manager.GetDisk(path); err != nil {
		disk, err = NewDisk(path, reservedSpace, diskRdonlySpace, maxErrCnt, manager, diskEnableReadRepairExtentLimit)
		if err != nil {
			log.LogErrorf("NewDisk fail err:[%v]", err)
			return
		}
		err = disk.RestorePartition(visitor)
		if err != nil {
			log.LogErrorf("RestorePartition fail err:[%v]", err)
			return
		}
		manager.putDisk(disk)
		err = nil
		go disk.doBackendTask()
	}
	return
}

func (manager *SpaceManager) LoadBrokenDisk(path string, reservedSpace, diskRdonlySpace uint64, maxErrCnt int, diskEnableReadRepairExtentLimit bool) (err error) {
	var disk *Disk

	if diskRdonlySpace < reservedSpace {
		diskRdonlySpace = reservedSpace
	}

	log.LogDebugf("action[LoadBrokenDisk] load broken disk from path(%v).", path)

	if _, err = manager.GetDisk(path); err != nil {
		disk = NewBrokenDisk(path, reservedSpace, diskRdonlySpace, maxErrCnt, manager, diskEnableReadRepairExtentLimit)
		manager.putDisk(disk)
	}
	return
}

func (manager *SpaceManager) GetDisk(path string) (d *Disk, err error) {
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	disk, has := manager.disks[path]
	if has && disk != nil {
		d = disk
		return
	}
	err = fmt.Errorf("disk(%v) not exsit", path)
	return
}

func (manager *SpaceManager) putDisk(d *Disk) {
	manager.diskMutex.Lock()
	manager.disks[d.Path] = d
	manager.diskList = append(manager.diskList, d.Path)
	if d.GetDiskPartition() != nil {
		manager.diskUtils[d.GetDiskPartition().Device] = &atomicutil.Float64{}
		manager.diskUtils[d.GetDiskPartition().Device].Store(0)
	}
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
		if d.Status == proto.Unavailable {
			log.LogInfof("disk is broken, not stat disk useage, diskpath %s", d.Path)
			continue
		}

		if d.Used > d.Total {
			total += d.Used
		} else {
			total += d.Total
		}

		used += d.Used
		if !d.GetDecommissionStatus() {
			available += d.Available
		} else {
			log.LogInfof("[updateMetrics] disk(%v) is decommissioned, avaliable space(%v) raw(%v)", d.Path, strutil.FormatSize(d.Available), d.Available)
		}
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

const DiskSelectMaxStraw = 65536

func (manager *SpaceManager) selectDisk(decommissionedDisks []string) (d *Disk) {
	manager.diskMutex.Lock()
	defer manager.diskMutex.Unlock()
	decommissionedDiskMap := make(map[string]struct{})
	for _, disk := range decommissionedDisks {
		decommissionedDiskMap[disk] = struct{}{}
	}
	maxStraw := float64(0)
	for _, disk := range manager.disks {
		if _, ok := decommissionedDiskMap[disk.Path]; ok {
			log.LogInfof("action[minPartitionCnt] exclude decommissioned disk[%v]", disk.Path)
			continue
		}
		if disk.Status != proto.ReadWrite {
			log.LogInfof("[minPartitionCnt] disk(%v) is not writable", disk.Path)
			continue
		}

		straw := float64(manager.rand.Intn(DiskSelectMaxStraw))
		straw = math.Log(straw/float64(DiskSelectMaxStraw)) / (float64(atomic.LoadUint64(&disk.Available)) / util.GB)
		if d == nil || straw > maxStraw {
			maxStraw = straw
			d = disk
		}
	}
	if d != nil && d.Status != proto.ReadWrite {
		d = nil
		return
	}
	return d
}

func (manager *SpaceManager) statUpdateScheduler() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				manager.updateMetrics()
				manager.updateDataNodeIDs()
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
	begin := time.Now()
	manager.partitionMutex.Lock()
	defer func() {
		log.LogInfof("[AttachPartition] load dp(%v) attach using time(%v)", dp.info(), time.Now().Sub(begin))
		manager.partitionMutex.Unlock()
	}()
	if _, has := manager.partitions[dp.partitionID]; !has {
		manager.partitions[dp.partitionID] = dp
		log.LogDebugf("action[AttachPartition] put partition(%v) to manager.", dp.partitionID)
	}
}

// DetachDataPartition removes a data partition from the partition map.
func (manager *SpaceManager) DetachDataPartition(partitionID uint64) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()
	delete(manager.partitions, partitionID)
}

func (manager *SpaceManager) CreatePartition(request *proto.CreateDataPartitionRequest) (dp *DataPartition, err error) {
	dpCfg := &dataPartitionCfg{
		PartitionID:              request.PartitionId,
		VolName:                  request.VolumeId,
		Peers:                    request.Members,
		Hosts:                    request.Hosts,
		RaftStore:                manager.raftStore,
		NodeID:                   manager.nodeID,
		ClusterID:                manager.clusterID,
		PartitionSize:            request.PartitionSize,
		PartitionType:            int(request.PartitionTyp),
		ReplicaNum:               request.ReplicaNum,
		VerSeq:                   request.VerSeq,
		CreateType:               request.CreateType,
		Forbidden:                false,
		IsEnableSnapshot:         manager.dataNode.clusterEnableSnapshot,
		ForbidWriteOpOfProtoVer0: false,
	}
	log.LogInfof("action[CreatePartition] dp %v dpCfg.Peers %v request.Members %v",
		dpCfg.PartitionID, dpCfg.Peers, request.Members)
	dp = manager.Partition(dpCfg.PartitionID)
	if dp != nil {
		if err = dp.IsEquareCreateDataPartitionRequst(request); err != nil {
			return nil, err
		}
		return
	}
	disk := manager.selectDisk(request.DecommissionedDisks)
	if disk == nil {
		log.LogErrorf("[CreatePartition] dp(%v) failed to select disk", dpCfg.PartitionID)
		return nil, ErrNoSpaceToCreatePartition
	}
	defer func() {
		msg := fmt.Sprintf("dp %v request.Members %v on disk %v",
			dpCfg.PartitionID, request.Members, disk.Path)
		auditlog.LogDataNodeOp("DataPartitionCreate", msg, err)
	}()

	if dp, err = CreateDataPartition(dpCfg, disk, request); err != nil {
		return
	}
	manager.partitionMutex.Lock()
	manager.partitions[dp.partitionID] = dp
	manager.partitionMutex.Unlock()
	return
}

// DeletePartition deletes a partition based on the partition id.
func (manager *SpaceManager) DeletePartition(dpID uint64, force bool) (err error) {
	manager.partitionMutex.Lock()

	dp := manager.partitions[dpID]
	if dp == nil {
		manager.partitionMutex.Unlock()
		// maybe dp not loaded when triggered disk error, need to remove disk root dir
		err = manager.deleteDataPartitionNotLoaded(dpID, force)
		return err
	}

	delete(manager.partitions, dpID)
	manager.partitionMutex.Unlock()
	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	if err := dp.RemoveAll(force); err != nil {
		return err
	}
	return nil
}

func (s *DataNode) buildHeartBeatResponse(response *proto.DataNodeHeartbeatResponse,
	forbiddenVols map[string]struct{}, dpRepairBlockSize map[string]uint64, reqID string,
) {
	response.Status = proto.TaskSucceeds
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
	response.BadDiskStats = make([]proto.BadDiskStat, 0)
	response.StartTime = s.startTime
	stat.Unlock()

	response.ZoneName = s.zoneName
	response.ReceivedForbidWriteOpOfProtoVer0 = s.nodeForbidWriteOpOfProtoVer0
	response.PartitionReports = make([]*proto.DataPartitionReport, 0)
	space := s.space
	begin := time.Now()
	var respLock sync.Mutex
	space.RangePartitions(func(partition *DataPartition, testID string) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		begin2 := time.Now()
		vr := &proto.DataPartitionReport{
			VolName:                    partition.volumeID,
			PartitionID:                uint64(partition.partitionID),
			PartitionStatus:            partition.Status(),
			Total:                      uint64(partition.Size()),
			Used:                       uint64(partition.Used()),
			DiskPath:                   partition.Disk().Path,
			IsLeader:                   isLeader,
			ExtentCount:                partition.GetExtentCountWithoutLock(),
			NeedCompare:                true,
			DecommissionRepairProgress: partition.decommissionRepairProgress,
			LocalPeers:                 partition.config.Peers,
			TriggerDiskError:           atomic.LoadUint64(&partition.diskErrCnt) > 0,
		}
		log.LogDebugf("action[Heartbeats] dpid(%v), status(%v) total(%v) used(%v) leader(%v) isLeader(%v) "+
			"TriggerDiskError(%v) reqId(%v) testID(%v) cost(%v).",
			vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader, vr.TriggerDiskError,
			reqID, testID, time.Now().Sub(begin2))
		respLock.Lock()
		response.PartitionReports = append(response.PartitionReports, vr)
		respLock.Unlock()
		begin2 = time.Now()

		if len(forbiddenVols) != 0 {
			if _, ok := forbiddenVols[partition.volumeID]; ok {
				partition.SetForbidden(true)
			} else {
				partition.SetForbidden(false)
			}
		}

		oldVal := partition.IsForbidWriteOpOfProtoVer0()
		VolsForbidWriteOpOfProtoVer0 := s.VolsForbidWriteOpOfProtoVer0
		if _, ok := VolsForbidWriteOpOfProtoVer0[partition.volumeID]; ok {
			partition.SetForbidWriteOpOfProtoVer0(true)
		} else {
			partition.SetForbidWriteOpOfProtoVer0(false)
		}
		newVal := partition.IsForbidWriteOpOfProtoVer0()
		if oldVal != newVal {
			log.LogWarnf("[Heartbeats] vol(%v) dpId(%v) IsForbidWriteOpOfProtoVer0 change to %v",
				partition.volumeID, partition.partitionID, newVal)
		}

		size := uint64(proto.DefaultDpRepairBlockSize)
		if len(dpRepairBlockSize) != 0 {
			var ok bool
			if size, ok = dpRepairBlockSize[partition.volumeID]; !ok {
				size = proto.DefaultDpRepairBlockSize
			}
		}
		log.LogDebugf("action[Heartbeats] volume(%v) dp(%v) repair block size(%v) current size(%v) reqId(%v) testID(%v) nodeForbidWriteOpOfProtoVer0(%v) cost(%v)",
			partition.volumeID, partition.partitionID, size, partition.GetRepairBlockSize(), reqID, testID, partition.IsForbidWriteOpOfProtoVer0(), time.Now().Sub(begin2))
		if partition.GetRepairBlockSize() != size {
			partition.SetRepairBlockSize(size)
		}
		return true
	}, reqID)

	if opstat.DpStat.IsSendMaster() {
		response.DiskOpLog = s.getDiskOpLog()
	}
	if opstat.DiskStat.IsSendMaster() {
		response.DpOpLog = s.getDpOpLog()
	}

	log.LogDebugf("buildHeartBeatResponse range dp req(%v) cost %v", reqID, time.Now().Sub(begin))
	disks := space.GetDisks()
	for _, d := range disks {
		response.AllDisks = append(response.AllDisks, d.Path)
		brokenDpsCnt := d.GetDiskErrPartitionCount()
		brokenDps := d.GetDiskErrPartitionList()
		log.LogInfof("[buildHeartBeatResponse] disk(%v) status(%v) broken dp len(%v)", d.Path, d.Status, brokenDpsCnt)
		if d.Status == proto.Unavailable || brokenDpsCnt != 0 {
			response.BadDisks = append(response.BadDisks, d.Path)
			bds := proto.BadDiskStat{
				DiskPath:             d.Path,
				TotalPartitionCnt:    d.PartitionCount(),
				DiskErrPartitionList: brokenDps,
			}
			response.BadDiskStats = append(response.BadDiskStats, bds)
			log.LogErrorf("[buildHeartBeatResponse] disk(%v) total(%v) broken dp len(%v) %v",
				d.Path, bds.TotalPartitionCnt, brokenDpsCnt, brokenDps)
		}
		response.BackupDataPartitions = append(response.BackupDataPartitions, d.GetBackupPartitionDirList()...)
	}
}

func (manager *SpaceManager) getPartitionIds() []uint64 {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()
	res := make([]uint64, 0)
	for id := range manager.partitions {
		res = append(res, id)
	}
	return res
}

func (manager *SpaceManager) deleteDataPartitionNotLoaded(id uint64, force bool) error {
	if !manager.dataNode.checkAllDiskLoaded() {
		return errors.NewErrorf("Disks on data node %v are not loaded completed", manager.dataNode.localServerAddr)
	}
	disks := manager.GetDisks()
	for _, d := range disks {
		if d.HasDiskErrPartition(id) {
			// delete it from DiskErrPartitionSet, not report to master any more
			d.DiskErrPartitionSet.Delete(id)
			// remove dp root dir
			fileInfoList, err := os.ReadDir(d.Path)
			if err != nil {
				log.LogErrorf("[deleteDataPartitionNotLoaded] disk(%v)load file list err %v",
					d.Path, err)
				return err
			}
			for _, fileInfo := range fileInfoList {
				filename := fileInfo.Name()
				if !d.isPartitionDir(filename) {
					log.LogWarnf("[deleteDataPartitionNotLoaded] disk(%v)ignore file %v",
						d.Path, filename)
					continue
				}
				var partitionID uint64
				if partitionID, _, err = unmarshalPartitionName(filename); err != nil {
					log.LogErrorf("action[deleteDataPartitionNotLoaded] unmarshal partitionName(%v) from disk(%v) err(%v) ",
						filename, d.Path, err.Error())
					continue
				} else {
					if partitionID == id {
						rootPath := path.Join(d.Path, filename)
						if force {
							newPath := fmt.Sprintf("%v-%v", path.Join(d.Path, BackupPartitionPrefix+filename), time.Now().Format("20060102150405"))
							//_, err := os.Stat(newPath)
							//if err == nil {
							//	newPathWithTimestamp := fmt.Sprintf("%v-%v", newPath, time.Now().Format("20060102150405"))
							//	err = os.Rename(newPath, newPathWithTimestamp)
							//	if err != nil {
							//		log.LogWarnf("action[deleteDataPartitionNotLoaded]: rename dir from %v to %v,err %v", newPath, newPathWithTimestamp, err)
							//		return err
							//	}
							//}
							err = os.Rename(rootPath, newPath)
							if err == nil {
								d.AddBackupPartitionDir(id)
							}
							log.LogInfof("action[deleteDataPartitionNotLoaded] disk(%v) rename root dir (%v)"+
								"to %v  err(%v) ", d.Path, rootPath, newPath, err)
						} else {
							err = os.RemoveAll(rootPath)
							log.LogInfof("action[deleteDataPartitionNotLoaded] disk(%v) remove root dir (%v) failed err(%v) ",
								d.Path, rootPath, err)
						}
						return err
					}
				}
			}
		}
	}
	return nil
}

func (manager *SpaceManager) fetchDataNodesFromMaster() (nodes []proto.NodeView, err error) {
	retry := 0
	for {
		if nodes, err = MasterClient.AdminAPI().GetClusterDataNodes(); err != nil {
			retry++
			if retry > 5 {
				return
			}
		} else {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	return
}

func (manager *SpaceManager) updateDataNodeIDs() {
	dataNodes, err := manager.fetchDataNodesFromMaster()
	if err != nil {
		log.LogErrorf("action[updateDataNodeID] fetch dataNodes from master failed err(%v) ", err.Error())
		return
	}
	// lear old
	manager.dataNodeIDsMutex.Lock()
	defer manager.dataNodeIDsMutex.Unlock()
	for key := range manager.dataNodeIDs {
		delete(manager.dataNodeIDs, key)
	}
	for _, dn := range dataNodes {
		manager.dataNodeIDs[dn.Addr] = dn.ID
	}
}

type DataNodeID struct {
	Addr string
	ID   uint64
}

func (manager *SpaceManager) getDataNodeIDs() []DataNodeID {
	manager.dataNodeIDsMutex.RLock()
	defer manager.dataNodeIDsMutex.RUnlock()
	var ids []DataNodeID
	for addr, id := range manager.dataNodeIDs {
		ids = append(ids, DataNodeID{Addr: addr, ID: id})
	}
	return ids
}

func (manager *SpaceManager) getPartitions() []*DataPartition {
	partitions := make([]*DataPartition, 0)
	manager.partitionMutex.RLock()
	defer manager.partitionMutex.RUnlock()
	for _, dp := range manager.partitions {
		partitions = append(partitions, dp)
	}
	return partitions
}
