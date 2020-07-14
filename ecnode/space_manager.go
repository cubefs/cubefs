// Copyright 2020 The Chubao Authors.
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	ReadFlag  = 1
	WriteFlag = 2
)

type SpaceManager struct {
	clusterID            string
	disks                map[string]*Disk
	partitions           map[uint64]*EcPartition
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

	close(manager.stopC)
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

func (manager *SpaceManager) LoadDisk(path string, reservedSpace uint64, maxErrCnt int) (err error) {
	visitor := func(ep *EcPartition) {
		manager.partitionMutex.Lock()
		defer manager.partitionMutex.Unlock()

		if _, has := manager.partitions[ep.partitionID]; !has {
			manager.partitions[ep.partitionID] = ep
			log.LogDebugf("action[LoadDisk] put partition(%v) to manager manager.", ep.partitionID)
		}
	}

	log.LogDebugf("action[LoadDisk] load disk from path(%v).", path)

	_, err = manager.GetDisk(path)
	if err != nil {
		log.LogDebugf("action[LoadDisk] find unload disk, load it from path(%v).", path)
		disk := NewDisk(path, reservedSpace, maxErrCnt, manager)
		disk.RestorePartition(visitor)
		manager.putDisk(disk)
		err = nil
		// TODO CRC check
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

	manager.partitions[ep.partitionID] = ep
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
	response.RemainingCapacity = stat.RemainingCapacityToCreatePartition
	stat.Unlock()

	response.CellName = e.cellName
	response.PartitionReports = make([]*proto.PartitionReport, 0)
	space := e.space
	space.RangePartitions(func(partition *EcPartition) bool {
		vr := &proto.PartitionReport{
			VolName:         partition.volumeID,
			PartitionID:     uint64(partition.partitionID),
			PartitionStatus: partition.Status(),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
			DiskPath:        partition.Disk().Path,
			ExtentCount:     partition.GetExtentCount(),
			NeedCompare:     true,
		}
		response.PartitionReports = append(response.PartitionReports, vr)
		return true
	})

	disks := space.GetDisks()
	for _, d := range disks {
		if d.Status == proto.Unavailable {
			// TODO Add bad disk list
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

	var wg sync.WaitGroup
	for _, d := range cfg.GetArray(ConfigKeyDisks) {
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

func (manager *SpaceManager) getMinUsageDisk() (d *Disk) {
	manager.diskMutex.Lock()
	defer manager.diskMutex.Unlock()

	minWeight := math.MaxFloat64
	for _, disk := range manager.disks {
		if disk.Available <= 5*util.GB || disk.Status != proto.ReadWrite {
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

func (manager *SpaceManager) CreatePartition(request *proto.CreateEcPartitionRequest) (ep *EcPartition, err error) {
	manager.partitionMutex.Lock()
	defer manager.partitionMutex.Unlock()

	epCfg := &EcPartitionCfg{
		ClusterID: manager.clusterID,

		VolName:        request.VolumeID,
		PartitionID:    request.PartitionID,
		PartitionSize:  request.PartitionSize,
		StripeUnitSize: request.StripeBlockSize,

		DataNodeNum:   request.DataNodeNum,
		ParityNodeNum: request.ParityNodeNum,
		NodeIndex:     request.NodeIndex,
		DataNodes:     request.DataNodes,
		ParityNodes:   request.ParityNodes,
	}

	ep = manager.partitions[epCfg.PartitionID]
	if ep != nil {
		return
	}

	disk := manager.getMinUsageDisk()
	if disk == nil {
		err = errors.New("No disk space to create a data partition")
		return nil, err
	}

	ep, err = CreateEcPartition(epCfg, disk, request)
	if err != nil {
		return
	}
	manager.partitions[epCfg.PartitionID] = ep
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
