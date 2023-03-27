// Copyright 2020 The CubeFS Authors.
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

package ecnode

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const (
	ReadFlag  = 1
	WriteFlag = 2
)

type SpaceManager struct {
	clusterID            string
	disks                map[string]*Disk
	partitions           map[uint64]*EcPartition
	ecNode               *EcNode
	nodeID               uint64
	diskMutex            sync.RWMutex
	partitionMutex       sync.RWMutex
	stats                *Stats
	stopC                chan bool
	diskList             []string
	createPartitionMutex sync.RWMutex
}

// Creates a new space manager.
func NewSpaceManager(cell string) *SpaceManager {
	space := &SpaceManager{}
	space.disks = make(map[string]*Disk)
	space.diskList = make([]string, 0)
	space.partitions = make(map[uint64]*EcPartition)
	space.stats = NewStats(cell)
	space.stopC = make(chan bool, 0)

	go space.statUpdateScheduler()

	return space
}

func (manager *SpaceManager) Stop() {
	defer func() {
		recover()
	}()
	for _, disk := range manager.GetDisks() {
		disk.ecDb.CloseEcDb()
		disk.Stop()
	}
	close(manager.stopC)
}

func (manager *SpaceManager) SetNodeID(nodeID uint64) {
	manager.nodeID = nodeID
}

func (manager *SpaceManager) SetClusterID(clusterID string) {
	manager.clusterID = clusterID
}

func (manager *SpaceManager) Stats() *Stats {
	return manager.stats
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
	manager.diskMutex.Unlock()
}

func (d *Disk) autoComputeEcTinyExtentCrc() {
	partitions := make([]*EcPartition, 0)
	d.RLock()
	for _, ep := range d.partitionMap {
		if ep.ecMigrateStatus == proto.FinishEC || ep.ecMigrateStatus == proto.OnlyEcExist {
			partitions = append(partitions, ep)
		}
	}
	d.RUnlock()
	for _, ep := range partitions {
		ep.extentStore.AutoComputeEcExtentCrc(ep.Hosts, ep.ecNode.localServerAddr, ep.EcDataNum, ep.EcParityNum)
	}
}

func (manager *SpaceManager) LoadDisk(path string, reservedSpace uint64, maxErrCnt int) (err error) {
	diskFDLimit := EcFdLimit{
		MaxFDLimit:      DiskMaxFDLimit,
		ForceEvictRatio: DiskForceEvictFDRatio,
	}
	visitor := func(ep *EcPartition) {
		manager.partitionMutex.Lock()
		defer manager.partitionMutex.Unlock()

		if _, has := manager.partitions[ep.PartitionID]; !has {
			manager.partitions[ep.PartitionID] = ep
			log.LogDebugf("action[LoadDisk] put partition(%v) to manager manager.", ep.PartitionID)
		}
	}

	log.LogDebugf("action[LoadDisk] load disk from path(%v).", path)

	_, err = manager.GetDisk(path)
	if err != nil {
		log.LogDebugf("action[LoadDisk] find unload disk, load it from path(%v).", path)
		disk := NewDisk(path, reservedSpace, maxErrCnt, diskFDLimit, manager)
		if err = disk.startEcRocksdb(); err != nil {
			return
		}
		disk.RestorePartition(visitor)
		manager.putDisk(disk)
		err = nil
		// TODO CRC check
		go disk.ecScheduleScrub()
	}

	return
}

func (manager *SpaceManager) RangePartitions(f func(partition *EcPartition) bool) {
	if f == nil {
		return
	}

	manager.partitionMutex.RLock()
	partitions := make([]*EcPartition, 0)
	for _, ep := range manager.partitions {
		partitions = append(partitions, ep)
	}
	manager.partitionMutex.RUnlock()

	for _, partition := range partitions {
		if !f(partition) {
			break
		}
	}
}

func (manager *SpaceManager) Partition(partitionID uint64) (ep *EcPartition) {
	manager.partitionMutex.RLock()
	defer manager.partitionMutex.RUnlock()
	ep = manager.partitions[partitionID]

	return
}

func (manager *SpaceManager) AttachPartition(ep *EcPartition) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()

	manager.partitions[ep.PartitionID] = ep
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

	log.LogDebugf("action[updateMetrics] total(%v) used(%v) available(%v) totalPartitionSize(%v) "+
		"remainingCapacityToCreatePartition(%v) "+"partitionCnt(%v) maxCapacityToCreatePartition(%v) ",
		total, used, available, totalPartitionSize, remainingCapacityToCreatePartition, partitionCnt,
		maxCapacityToCreatePartition)

	manager.stats.UpdateMetrics(total, used, available, totalPartitionSize,
		remainingCapacityToCreatePartition, maxCapacityToCreatePartition, partitionCnt)
}

func (manager *SpaceManager) statUpdateScheduler() {
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
}

func (e *EcNode) buildHeartbeatResponse(response *proto.EcNodeHeartbeatResponse) {
	response.Status = proto.TaskSucceeds
	stat := e.space.Stats()

	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.MaxCapacity = stat.MaxCapacityToCreatePartition
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.TotalPartitionSize = stat.TotalPartitionSize
	response.BadDisks = make([]string, 0)
	response.Version = EcNodeLatestVersion
	stat.Unlock()

	response.HttpPort = e.httpPort
	response.CellName = e.cellName
	response.PartitionReports = make([]*proto.EcPartitionReport, 0)
	space := e.space
	space.RangePartitions(func(partition *EcPartition) bool {
		vr := &proto.EcPartitionReport{
			VolName:         partition.VolumeID,
			PartitionID:     partition.PartitionID,
			PartitionStatus: partition.Status(),
			Total:           partition.Size(),
			Used:            partition.Used(),
			DiskPath:        partition.Disk().Path,
			ExtentCount:     partition.extentStore.GetExtentCount(),
			NeedCompare:     true,
			NodeIndex:       partition.NodeIndex,
		}
		vr.IsRecover = partition.isRecover
		response.PartitionReports = append(response.PartitionReports, vr)
		return true
	})

	disks := space.GetDisks()
	for _, d := range disks {
		if d.Status == proto.Unavailable {
			response.BadDisks = append(response.BadDisks, d.Path)
		}
	}
}

func (e *EcNode) startSpaceManager(cfg *config.Config) (err error) {
	e.space = NewSpaceManager(e.cellName)
	if len(strings.TrimSpace(e.port)) == 0 {
		err = ErrNewSpaceManagerFailed
		return
	}

	e.space.SetNodeID(e.nodeID)
	e.space.SetClusterID(e.clusterID)
	e.space.ecNode = e

	var wg sync.WaitGroup
	for _, d := range cfg.GetSlice(ConfigKeyDisks) {
		log.LogDebugf("action[startSpaceManager] load disk raw config(%v).", d)

		diskArr := strings.Split(d.(string), ":")
		if len(diskArr) != 2 {
			return errors.New("Invalid disk configuration. Example: PATH:RESERVE_SIZE")
		}
		path := diskArr[0]
		fileInfo, err := os.Stat(path)
		if err != nil {
			return errors.New(fmt.Sprintf("Stat disk path error: %v", err))
		}
		if !fileInfo.IsDir() {
			return errors.New("Disk path is not dir")
		}
		reservedSpace, err := strconv.ParseUint(diskArr[1], 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid disk reserved space. Error: %v", err))
		}

		if reservedSpace < DefaultDiskRetainMin {
			reservedSpace = DefaultDiskRetainMin
		}
		wg.Add(1)
		go func(wg *sync.WaitGroup, path string, reservedSpace uint64) {
			defer wg.Done()
			e.space.LoadDisk(path, reservedSpace, DefaultDiskMaxErr)
		}(&wg, path, reservedSpace)
	}
	wg.Wait()
	return nil
}

func (manager *SpaceManager) getMinUsageDisk(partitionSize uint64) (d *Disk) {
	manager.diskMutex.Lock()
	defer manager.diskMutex.Unlock()

	minWeight := math.MaxFloat64
	for _, disk := range manager.disks {
		if disk.Available <= 5*unit.GB || disk.Status != proto.ReadWrite || disk.Unallocated < partitionSize {
			continue
		}
		diskWeight := float64(atomic.LoadUint64(&disk.Allocated)) / float64(disk.Total)
		if diskWeight < minWeight {
			minWeight = diskWeight
			d = disk
		}
	}
	return
}

func (manager *SpaceManager) ExpiredEcPartition(ep *EcPartition) (err error) {
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("EcPartition(%v) Expired panic(%v)", ep.PartitionID, r)
			log.LogWarnf(mesg)
		}
	}()
	manager.partitionMutex.Lock()
	delete(manager.partitions, ep.PartitionID)
	manager.partitionMutex.Unlock()

	ep.extentStore.Close()
	ep.disk.DetachEcPartition(ep)

	var currentPath = path.Clean(ep.path)
	var newPath = path.Join(path.Dir(currentPath),
		ExpiredPartitionPrefix+path.Base(currentPath)+"_"+strconv.FormatInt(time.Now().Unix(), 10))
	if err = os.Rename(currentPath, newPath); err != nil {
		log.LogErrorf("ExpiredEcPartion: mark expired partition fail: volume(%v) partitionID(%v) path(%v) newPath(%v) err(%v)",
			ep.VolumeID,
			ep.PartitionID,
			ep.path,
			newPath,
			err)
		return
	}
	ep.Stop()
	log.LogInfof("ExpiredEcPartion: mark expired partition: volume(%v) partitionID(%v) path(%v) newPath(%v)",
		ep.VolumeID,
		ep.PartitionID,
		ep.path,
		newPath)
	return
}
func (manager *SpaceManager) CreatePartition(request *proto.CreateEcPartitionRequest) (ep *EcPartition, err error) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()

	md := &EcPartitionMetaData{
		PartitionID:      request.PartitionID,
		PartitionSize:    request.PartitionSize,
		VolumeID:         request.VolumeID,
		EcDataNum:        request.DataNodeNum,
		EcParityNum:      request.ParityNodeNum,
		NodeIndex:        request.NodeIndex,
		Hosts:            request.Hosts,
		EcMaxUnitSize:    request.EcMaxUnitSize,
		CreateTime:       time.Now().Format(TimeLayout),
		FailScrubExtents: make(map[uint64]uint64),
	}

	ep = manager.partitions[md.PartitionID]
	if ep != nil {
		return
	}
	disk := manager.getMinUsageDisk(md.PartitionSize)
	if disk == nil {
		err = errors.New("no disk space to create a data partition")
		return nil, err
	}
	log.LogDebugf("createPartition_pre:disk-total:%v, used:%v, avalible:%v, allocted:%v, remain:%v, partition size:%v\n",
		disk.Total, disk.Used, disk.Available, disk.Allocated, disk.Unallocated, md.PartitionSize)
	ep, err = CreateEcPartition(md, disk)
	log.LogDebugf("createPartition:disk-total:%v, used:%v, avalible:%v, allocted:%v, remain:%v, partition size:%v\n",
		disk.Total, disk.Used, disk.Available, disk.Allocated, disk.Unallocated, ep.PartitionSize)
	if err != nil {
		return
	}
	ep.ecNode = manager.ecNode
	manager.partitions[md.PartitionID] = ep
	return
}

func (e *EcNode) incDiskErrCnt(partitionID uint64, err error, flag uint8) {
	if err == nil {
		return
	}

	ep := e.space.Partition(partitionID)
	if ep == nil {
		return
	}

	d := ep.Disk()
	if d == nil {
		return
	}

	if !IsDiskErr(err.Error()) {
		return
	}

	if flag == WriteFlag {
		d.incWriteErrCnt()
	} else if flag == ReadFlag {
		d.incReadErrCnt()
	}
	return
}
