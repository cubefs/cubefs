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
	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/strutil"
	"github.com/shirou/gopsutil/disk"
)

const DefaultStopDpLimit = 4

// SpaceManager manages the disk space.
type SpaceManager struct {
	clusterID          string
	disks              map[string]*Disk
	partitions         map[uint64]*DataPartition
	raftStore          raftstore.RaftStore
	nodeID             uint64
	diskMutex          sync.RWMutex
	partitionMutex     sync.RWMutex
	stats              *Stats
	stopC              chan bool
	diskList           []string
	dataNode           *DataNode
	rand               *rand.Rand
	currentLoadDpCount int
	currentStopDpCount int
	diskUtils          map[string]*atomicutil.Float64
	samplerDone        chan struct{}
	allDisksLoaded     bool
	dataNodeIDs        map[string]uint64
	dataNodeIDsMutex   sync.RWMutex
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
	}()
	defer func() {
		recover()
	}()
	close(manager.stopC)
	close(manager.samplerDone)

	// Close raft store.
	for _, partition := range manager.partitions {
		partition.stopRaft()
	}

	var wg sync.WaitGroup
	for _, d := range manager.disks {
		dps := make([]*DataPartition, 0)

		for _, dp := range manager.partitions {
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

func (manager *SpaceManager) GetDiskUtil(disk *Disk) (util float64) {
	manager.diskMutex.RLock()
	defer manager.diskMutex.RUnlock()
	util = manager.diskUtils[disk.diskPartition.Device].Load()
	return
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

func (manager *SpaceManager) RangePartitions(f func(partition *DataPartition) bool) {
	if f == nil {
		return
	}
	manager.partitionMutex.RLock()
	partitions := make([]*DataPartition, 0)
	for _, dp := range manager.partitions {
		partitions = append(partitions, dp)
	}
	manager.partitionMutex.RUnlock()

	for _, partition := range partitions {
		if !f(partition) {
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
	defer func() {
		log.LogInfof("[AttachPartition] load dp(%v) attach using time(%v)", dp.partitionID, time.Since(begin))
	}()
	manager.partitionMutex.Lock()
	if loadedDp, has := manager.partitions[dp.partitionID]; !has {
		manager.partitions[dp.partitionID] = dp
		manager.partitionMutex.Unlock()
		log.LogDebugf("action[AttachPartition] put partition(%v) to manager.", dp.partitionID)
	} else {
		manager.partitionMutex.Unlock()

		if loadedDp.disk.Path == dp.disk.Path {
			log.LogWarnf("[AttachPartition] dp(%v) is loaded, to load(%v), but disk path is the same",
				loadedDp.info(), dp.info())
			return
		}

		log.LogWarnf("action[AttachPartition] dp(%v) is loaded, to load(%v).", loadedDp.info(), dp.info())
		_, _, infos, err := dp.fetchReplicasFromMaster()
		if err != nil {
			manager.DetachDataPartition(loadedDp.partitionID)
			loadedDp.Stop()
			loadedDp.Disk().DetachDataPartition(loadedDp)
			log.LogErrorf("action[LoadDisk] dp(%v) is detached,due to get dp info failed(%v).",
				loadedDp.info(), err)
		} else {
			var correctReplica ReplicaInfo
			for _, replica := range infos {
				if replica.Addr == manager.dataNode.localServerAddr {
					correctReplica = replica
					break
				}
			}
			if correctReplica.Disk == "" && correctReplica.Addr == "" {
				loadedDp.Stop()
				loadedDp.Disk().DetachDataPartition(loadedDp)
				log.LogErrorf("action[LoadDisk] dp(%v) is detached,due to data node not contains in replicas"+
					"from master.", loadedDp.info())
			} else {
				if loadedDp.disk.Path == correctReplica.Disk {
					dp.Stop()
					dp.Disk().DetachDataPartition(dp)
					if err := dp.RemoveAll(true); err != nil {
						log.LogErrorf("action[LoadDisk]failed to remove dp(%v) dir(%v), err(%v)",
							dp.partitionID, dp.Path(), err)
					}
				} else {
					// detach loaded dp
					loadedDp.Stop()
					loadedDp.Disk().DetachDataPartition(loadedDp)
					if err := loadedDp.RemoveAll(true); err != nil {
						log.LogErrorf("action[LoadDisk]failed to remove dp(%v) dir(%v), err(%v)",
							loadedDp.partitionID, loadedDp.Path(), err)
					}
					dp.Stop()
					dp.Disk().DetachDataPartition(dp)
					_, err := LoadDataPartition(dp.path, dp.disk)
					if err != nil {
						log.LogErrorf("action[LoadDisk]failed to load dp %v (%v_%v), err(%v)",
							dp.partitionID, dp.path, dp.disk, err)
					}
				}
			}
		}
	}
}

// DetachDataPartition removes a data partition from the partition map.
func (manager *SpaceManager) DetachDataPartition(partitionID uint64) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()
	delete(manager.partitions, partitionID)
}

func (manager *SpaceManager) CreatePartition(request *proto.CreateDataPartitionRequest) (dp *DataPartition, err error) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()
	dpCfg := &dataPartitionCfg{
		PartitionID:   request.PartitionId,
		VolName:       request.VolumeId,
		Peers:         request.Members,
		Hosts:         request.Hosts,
		RaftStore:     manager.raftStore,
		NodeID:        manager.nodeID,
		ClusterID:     manager.clusterID,
		PartitionSize: request.PartitionSize,
		PartitionType: int(request.PartitionTyp),
		ReplicaNum:    request.ReplicaNum,
		VerSeq:        request.VerSeq,
		CreateType:    request.CreateType,
		Forbidden:     false,
	}
	log.LogInfof("action[CreatePartition] dp %v dpCfg.Peers %v request.Members %v",
		dpCfg.PartitionID, dpCfg.Peers, request.Members)
	dp = manager.partitions[dpCfg.PartitionID]
	if dp != nil {
		if err = dp.IsEqualCreateDataPartitionRequest(request); err != nil {
			return nil, err
		}
		return
	}
	disk := manager.selectDisk(request.DecommissionedDisks)
	if disk == nil {
		log.LogErrorf("[CreatePartition] dp(%v) failed to select disk", dpCfg.PartitionID)
		return nil, ErrNoSpaceToCreatePartition
	}
	if dp, err = CreateDataPartition(dpCfg, disk, request); err != nil {
		return
	}
	manager.partitions[dp.partitionID] = dp
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
	volNames map[string]struct{}, dpRepairBlockSize map[string]uint64) {
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
	response.DiskStats = make([]proto.DiskStat, 0)
	response.StartTime = s.startTime
	stat.Unlock()

	response.ZoneName = s.zoneName
	response.PartitionReports = make([]*proto.DataPartitionReport, 0)
	space := s.space
	space.RangePartitions(func(partition *DataPartition) bool {
		leaderAddr, isLeader := partition.IsRaftLeader()
		vr := &proto.DataPartitionReport{
			VolName:                    partition.volumeID,
			PartitionID:                uint64(partition.partitionID),
			PartitionStatus:            partition.Status(),
			Total:                      uint64(partition.Size()),
			Used:                       uint64(partition.Used()),
			DiskPath:                   partition.Disk().Path,
			IsLeader:                   isLeader,
			ExtentCount:                partition.GetExtentCount(),
			NeedCompare:                true,
			DecommissionRepairProgress: partition.decommissionRepairProgress,
			LocalPeers:                 partition.config.Peers,
			TriggerDiskError:           atomic.LoadUint64(&partition.diskErrCnt) > 0,
		}
		log.LogDebugf("action[Heartbeats] dpid(%v), status(%v) total(%v) used(%v) leader(%v) isLeader(%v) TriggerDiskError(%v).",
			vr.PartitionID, vr.PartitionStatus, vr.Total, vr.Used, leaderAddr, vr.IsLeader, vr.TriggerDiskError)
		response.PartitionReports = append(response.PartitionReports, vr)

		if len(volNames) != 0 {
			if _, ok := volNames[partition.volumeID]; ok {
				partition.SetForbidden(true)
			} else {
				partition.SetForbidden(false)
			}
		}
		size := uint64(proto.DefaultDpRepairBlockSize)
		if len(dpRepairBlockSize) != 0 {
			var ok bool
			if size, ok = dpRepairBlockSize[partition.volumeID]; !ok {
				size = proto.DefaultDpRepairBlockSize
			}
		}
		log.LogDebugf("action[Heartbeats] volume(%v) dp(%v) repair block size(%v) current size(%v)",
			partition.volumeID, partition.partitionID, size, partition.GetRepairBlockSize())
		if partition.GetRepairBlockSize() != size {
			partition.SetRepairBlockSize(size)
		}
		return true
	})

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
	res := make([]uint64, 0)
	for id := range manager.partitions {
		res = append(res, id)
	}
	return res
}

func (manager *SpaceManager) deleteDataPartitionNotLoaded(id uint64, decommissionType uint32, force bool) error {
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
							newPath := path.Join(d.Path, BackupPartitionPrefix+filename)
							_, err := os.Stat(newPath)
							if err == nil {
								newPathWithTimestamp := fmt.Sprintf("%v-%v", newPath, time.Now().Format("20060102150405"))
								err = os.Rename(newPath, newPathWithTimestamp)
								if err != nil {
									log.LogWarnf("action[deleteDataPartitionNotLoaded]: rename dir from %v to %v,err %v", newPath, newPathWithTimestamp, err)
									return err
								}
							}
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
