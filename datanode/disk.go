// Copyright 2018 The Chubao Authors.
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
	"io/ioutil"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/storage"

	"os"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	// RegexpDataPartitionDir validates the directory name of a data partition.
	RegexpDataPartitionDir, _ = regexp.Compile("^datapartition_(\\d)+_(\\d)+$")
)

const ExpiredPartitionPrefix = "expired_"

type FDLimit struct {
	MaxFDLimit      uint64  // 触发强制FD淘汰策略的阈值
	ForceEvictRatio float64 // 强制FD淘汰比例
}

// Disk represents the structure of the disk
type Disk struct {
	sync.RWMutex
	Path        string
	ReadErrCnt  uint64 // number of read errors
	WriteErrCnt uint64 // number of write errors

	Total       uint64
	Used        uint64
	Available   uint64
	Unallocated uint64
	Allocated   uint64

	MaxErrCnt     int // maximum number of errors
	Status        int // disk status such as READONLY
	ReservedSpace uint64

	RejectWrite  bool
	partitionMap map[uint64]*DataPartition
	space        *SpaceManager

	// Parallel limit control
	fixTinyDeleteRecordLimit     uint64 // Limit for parallel fix tiny delete record tasks
	executingFixTinyDeleteRecord uint64 // Count of executing fix tiny delete record tasks
	repairTaskLimit              uint64 // Limit for parallel data repair tasks
	executingRepairTask          uint64 // Count of executing data repair tasks
	limitLock                    sync.Mutex

	// Runtime statistics
	fdCount int64
	fdLimit FDLimit
}

type PartitionVisitor func(dp *DataPartition)

func NewDisk(path string, reservedSpace uint64, maxErrCnt int, fdLimit FDLimit, space *SpaceManager) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.ReservedSpace = reservedSpace
	d.MaxErrCnt = maxErrCnt
	d.RejectWrite = false
	d.space = space
	d.partitionMap = make(map[uint64]*DataPartition)
	d.fixTinyDeleteRecordLimit = space.fixTinyDeleteRecordLimitOnDisk
	d.repairTaskLimit = space.repairTaskLimitOnDisk
	d.fdCount = 0
	d.fdLimit = fdLimit
	d.computeUsage()
	d.updateSpaceInfo()
	d.startScheduler()
	return
}

func (d *Disk) IncreaseFDCount() {
	atomic.AddInt64(&d.fdCount, 1)
}

func (d *Disk) DecreaseFDCount() {
	atomic.AddInt64(&d.fdCount, -1)
}

// PartitionCount returns the number of partitions in the partition map.
func (d *Disk) PartitionCount() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.partitionMap)
}

func (d *Disk) canRepairOnDisk() bool {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.repairTaskLimit <= 0 {
		return false
	}
	if d.executingRepairTask >= d.repairTaskLimit {
		return false
	}
	d.executingRepairTask++
	return true
}

func (d *Disk) finishRepairTask() {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.executingRepairTask > 0 {
		d.executingRepairTask--
	}
}

// Compute the disk usage
func (d *Disk) computeUsage() (err error) {
	d.RLock()
	defer d.RUnlock()
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(d.Path, &fs)
	if err != nil {
		return
	}

	//  total := math.Max(0, int64(fs.Blocks*uint64(fs.Bsize) - d.ReservedSpace))
	total := int64(fs.Blocks*uint64(fs.Bsize) - d.ReservedSpace)
	if total < 0 {
		total = 0
	}
	d.Total = uint64(total)

	//  available := math.Max(0, int64(fs.Bavail*uint64(fs.Bsize) - d.ReservedSpace))
	available := int64(fs.Bavail*uint64(fs.Bsize) - d.ReservedSpace)
	if available < 0 {
		available = 0
	}
	d.Available = uint64(available)

	//  used := math.Max(0, int64(total - available))
	used := int64(total - available)
	if used < 0 {
		used = 0
	}
	d.Used = uint64(used)

	allocatedSize := int64(0)
	for _, dp := range d.partitionMap {
		allocatedSize += int64(dp.Size())
	}
	atomic.StoreUint64(&d.Allocated, uint64(allocatedSize))
	//  unallocated = math.Max(0, total - allocatedSize)
	unallocated := total - allocatedSize
	if unallocated < 0 {
		unallocated = 0
	}
	if d.Available <= 0 {
		d.RejectWrite = true
	} else {
		d.RejectWrite = false
	}
	d.Unallocated = uint64(unallocated)

	log.LogDebugf("action[computeUsage] disk(%v) all(%v) available(%v) used(%v)",
		d.Path, d.Total, d.Available, d.Used)

	return
}

func (d *Disk) incReadErrCnt() {
	atomic.AddUint64(&d.ReadErrCnt, 1)
}

func (d *Disk) incWriteErrCnt() {
	atomic.AddUint64(&d.WriteErrCnt, 1)
}

func (d *Disk) startScheduler() {
	go func() {
		var (
			updateSpaceInfoTicker = time.NewTicker(5 * time.Second)
			checkStatusTicker     = time.NewTicker(time.Minute * 2)
			evictFDTicker         = time.NewTicker(time.Minute * 5)
			forceEvictFDTicker    = time.NewTicker(time.Second * 10)
		)
		defer func() {
			updateSpaceInfoTicker.Stop()
			checkStatusTicker.Stop()
			evictFDTicker.Stop()
			forceEvictFDTicker.Stop()
		}()
		for {
			select {
			case <-updateSpaceInfoTicker.C:
				d.computeUsage()
				d.updateSpaceInfo()
			case <-checkStatusTicker.C:
				d.checkDiskStatus()
				d.updateTaskExecutionLimit()
			case <-evictFDTicker.C:
				d.evictExpiredFileDescriptor()
			case <-forceEvictFDTicker.C:
				d.forceEvictFileDescriptor()
			}
		}
	}()
}

func (d *Disk) autoComputeExtentCrc() {
	for {
		partitions := make([]*DataPartition, 0)
		d.RLock()
		for _, dp := range d.partitionMap {
			partitions = append(partitions, dp)
		}
		d.RUnlock()
		for _, dp := range partitions {
			dp.extentStore.AutoComputeExtentCrc()
		}
		time.Sleep(time.Minute)
	}
}

func (d *Disk) updateTaskExecutionLimit() {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.fixTinyDeleteRecordLimit != d.space.fixTinyDeleteRecordLimitOnDisk {
		log.LogInfof("action[updateTaskExecutionLimit] disk(%v) change fixTinyDeleteRecordLimit from(%v) to(%v)", d.Path, d.fixTinyDeleteRecordLimit, d.space.fixTinyDeleteRecordLimitOnDisk)
		d.fixTinyDeleteRecordLimit = d.space.fixTinyDeleteRecordLimitOnDisk
	}
	if d.repairTaskLimit != d.space.repairTaskLimitOnDisk {
		log.LogInfof("action[updateTaskExecutionLimit] disk(%v) change repairTaskLimit from(%v) to(%v)", d.Path, d.repairTaskLimit, d.space.repairTaskLimitOnDisk)
		d.repairTaskLimit = d.space.repairTaskLimitOnDisk
	}
}

const (
	DiskStatusFile = ".diskStatus"
)

func (d *Disk) checkDiskStatus() {
	path := path.Join(d.Path, DiskStatusFile)
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		d.triggerDiskError(err)
		return
	}
	defer fp.Close()
	data := []byte(DiskStatusFile)
	_, err = fp.WriteAt(data, 0)
	if err != nil {
		d.triggerDiskError(err)
		return
	}
	if err = fp.Sync(); err != nil {
		d.triggerDiskError(err)
		return
	}
	if _, err = fp.ReadAt(data, 0); err != nil {
		d.triggerDiskError(err)
		return
	}
}

func (d *Disk) triggerDiskError(err error) {
	if err == nil {
		return
	}
	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", d.Path, LocalIP)
		exporter.Warning(mesg)
		log.LogErrorf(mesg)
		d.ForceExitRaftStore()
		d.Status = proto.Unavailable
	}
	return
}

func (d *Disk) updateSpaceInfo() (err error) {
	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(d.Path, &statsInfo); err != nil {
		d.incReadErrCnt()
	}
	if d.Status == proto.Unavailable {
		mesg := fmt.Sprintf("disk path %v error on %v", d.Path, LocalIP)
		log.LogErrorf(mesg)
		exporter.Warning(mesg)
		d.ForceExitRaftStore()
	} else if d.Available <= 0 {
		d.Status = proto.ReadOnly
	} else {
		d.Status = proto.ReadWrite
	}
	log.LogDebugf("action[updateSpaceInfo] disk(%v) total(%v) available(%v) remain(%v) "+
		"restSize(%v) maxErrs(%v) readErrs(%v) writeErrs(%v) status(%v)", d.Path,
		d.Total, d.Available, d.Unallocated, d.ReservedSpace, d.MaxErrCnt, d.ReadErrCnt, d.WriteErrCnt, d.Status)
	return
}

// AttachDataPartition adds a data partition to the partition map.
func (d *Disk) AttachDataPartition(dp *DataPartition) {
	d.Lock()
	d.partitionMap[dp.partitionID] = dp
	d.Unlock()

	d.computeUsage()
}

// DetachDataPartition removes a data partition from the partition map.
func (d *Disk) DetachDataPartition(dp *DataPartition) {
	d.Lock()
	delete(d.partitionMap, dp.partitionID)
	d.Unlock()

	d.computeUsage()
}

// GetDataPartition returns the data partition based on the given partition ID.
func (d *Disk) GetDataPartition(partitionID uint64) (partition *DataPartition) {
	d.RLock()
	defer d.RUnlock()
	return d.partitionMap[partitionID]
}

func (d *Disk) ForceExitRaftStore() {
	partitionList := d.DataPartitionList()
	for _, partitionID := range partitionList {
		partition := d.GetDataPartition(partitionID)
		partition.partitionStatus = proto.Unavailable
		partition.stopRaft()
	}
}

// DataPartitionList returns a list of the data partitions
func (d *Disk) DataPartitionList() (partitionIDs []uint64) {
	d.Lock()
	defer d.Unlock()
	partitionIDs = make([]uint64, 0, len(d.partitionMap))
	for _, dp := range d.partitionMap {
		partitionIDs = append(partitionIDs, dp.partitionID)
	}
	return
}

func unmarshalPartitionName(name string) (partitionID uint64, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error DataPartition name(%v)", name)
		return
	}
	if partitionID, err = strconv.ParseUint(arr[1], 10, 64); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	return
}

func (d *Disk) isPartitionDir(filename string) (isPartitionDir bool) {
	isPartitionDir = RegexpDataPartitionDir.MatchString(filename)
	return
}

// RestorePartition reads the files stored on the local disk and restores the data partitions.
func (d *Disk) RestorePartition(visitor PartitionVisitor, parallelism int) {
	var convert = func(node *proto.DataNodeInfo) *DataNodeInfo {
		result := &DataNodeInfo{}
		result.Addr = node.Addr
		result.PersistenceDataPartitions = node.PersistenceDataPartitions
		return result
	}
	var dataNode *proto.DataNodeInfo
	var err error
	for i := 0; i < 3; i++ {
		dataNode, err = MasterClient.NodeAPI().GetDataNode(d.space.dataNode.localServerAddr)
		if err != nil {
			log.LogErrorf("action[RestorePartition]: getDataNode error %v", err)
			continue
		}
		break
	}
	dinfo := convert(dataNode)
	if len(dinfo.PersistenceDataPartitions) == 0 {
		log.LogWarnf("action[RestorePartition]: length of PersistenceDataPartitions is 0, ExpiredPartition check " +
			"without effect")
	}

	var (
		partitionID uint64
	)

	fileInfoList, err := ioutil.ReadDir(d.Path)
	if err != nil {
		log.LogErrorf("action[RestorePartition] read dir(%v) err(%v).", d.Path, err)
		return
	}

	if parallelism < 1 {
		parallelism = 1
	}
	var loadWaitGroup = new(sync.WaitGroup)
	var filenameCh = make(chan string, parallelism)
	for i := 0; i < parallelism; i++ {
		loadWaitGroup.Add(1)
		go func() {
			defer loadWaitGroup.Done()
			var (
				filename  string
				partition *DataPartition
				loadErr   error
			)
			for {
				if filename = <-filenameCh; len(filename) == 0 {
					return
				}
				partitionFullPath := path.Join(d.Path, filename)
				startTime := time.Now()
				if partition, loadErr = LoadDataPartition(partitionFullPath, d); loadErr != nil {
					msg := fmt.Sprintf("load partition(%v) failed: %v",
						partitionFullPath, loadErr)
					log.LogError(msg)
					exporter.Warning(msg)
					return
				}
				log.LogInfof("partition(%v) load complete cost(%v)",
					partitionFullPath, time.Since(startTime))
				if visitor != nil {
					visitor(partition)
				}
			}
		}()
	}
	go func() {
		for _, fileInfo := range fileInfoList {
			filename := fileInfo.Name()
			if !d.isPartitionDir(filename) {
				continue
			}

			if partitionID, _, err = unmarshalPartitionName(filename); err != nil {
				log.LogErrorf("action[RestorePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
					filename, d.Path, err.Error())
				continue
			}
			if isExpiredPartition(partitionID, dinfo.PersistenceDataPartitions) {
				log.LogErrorf("action[RestorePartition]: find expired partition[%s], rename it and you can delete it "+
					"manually", filename)
				oldName := path.Join(d.Path, filename)
				newName := path.Join(d.Path, ExpiredPartitionPrefix+filename)
				_ = os.Rename(oldName, newName)
				continue
			}
			filenameCh <- filename
		}
		close(filenameCh)
	}()

	loadWaitGroup.Wait()
}

// RestoreOnePartition restores the data partition.
func (d *Disk) RestoreOnePartition(visitor PartitionVisitor, partitionPath string) (err error) {
	var (
		partitionID uint64
		partition   *DataPartition
		dInfo       *DataNodeInfo
	)
	if len(partitionPath) == 0 {
		err = fmt.Errorf("action[RestoreOnePartition] partition path is empty")
		return
	}
	partitionFullPath := path.Join(d.Path, partitionPath)
	_, err = os.Stat(partitionFullPath)
	if err != nil {
		err = fmt.Errorf("action[RestoreOnePartition] read dir(%v) err(%v)", partitionFullPath, err)
		return
	}
	if !d.isPartitionDir(partitionPath) {
		err = fmt.Errorf("action[RestoreOnePartition] invalid partition path")
		return
	}

	if partitionID, _, err = unmarshalPartitionName(partitionPath); err != nil {
		err = fmt.Errorf("action[RestoreOnePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
			partitionPath, d.Path, err.Error())
		return
	}

	dInfo, err = d.getPersistPartitionsFromMaster()
	if err != nil {
		return
	}
	if len(dInfo.PersistenceDataPartitions) == 0 {
		log.LogWarnf("action[RestoreOnePartition]: length of PersistenceDataPartitions is 0, ExpiredPartition check " +
			"without effect")
	}

	if isExpiredPartition(partitionID, dInfo.PersistenceDataPartitions) {
		log.LogErrorf("action[RestoreOnePartition]: find expired partition[%s], rename it and you can delete it "+
			"manually", partitionPath)
		newName := path.Join(d.Path, ExpiredPartitionPrefix+partitionPath)
		_ = os.Rename(partitionFullPath, newName)
		return
	}

	startTime := time.Now()
	if partition, err = LoadDataPartition(partitionFullPath, d); err != nil {
		msg := fmt.Sprintf("load partition(%v) failed: %v",
			partitionFullPath, err)
		log.LogError(msg)
		exporter.Warning(msg)
		return
	}
	log.LogInfof("partition(%v) load complete cost(%v)",
		partitionFullPath, time.Since(startTime))
	if visitor != nil {
		visitor(partition)
	}
	return
}

func (d *Disk) getPersistPartitionsFromMaster() (dInfo *DataNodeInfo, err error) {
	var dataNode *proto.DataNodeInfo
	var convert = func(node *proto.DataNodeInfo) *DataNodeInfo {
		result := &DataNodeInfo{}
		result.Addr = node.Addr
		result.PersistenceDataPartitions = node.PersistenceDataPartitions
		return result
	}
	for i := 0; i < 3; i++ {
		dataNode, err = MasterClient.NodeAPI().GetDataNode(d.space.dataNode.localServerAddr)
		if err != nil {
			log.LogErrorf("action[RestorePartition]: getDataNode error %v", err)
			continue
		}
		break
	}
	if err != nil {
		return
	}
	dInfo = convert(dataNode)
	return
}

func (d *Disk) AddSize(size uint64) {
	atomic.AddUint64(&d.Allocated, size)
}

func (d *Disk) getSelectWeight() float64 {
	return float64(atomic.LoadUint64(&d.Allocated)) / float64(d.Total)
}

// isExpiredPartition return whether one partition is expired
// if one partition does not exist in master, we decided that it is one expired partition
func isExpiredPartition(id uint64, partitions []uint64) bool {
	if len(partitions) == 0 {
		return true
	}

	for _, existId := range partitions {
		if existId == id {
			return false
		}
	}
	return true
}

func (d *Disk) canFinTinyDeleteRecord() bool {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.executingFixTinyDeleteRecord >= d.fixTinyDeleteRecordLimit {
		return false
	}
	d.executingFixTinyDeleteRecord++
	return true
}

func (d *Disk) finishFixTinyDeleteRecord() {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.executingFixTinyDeleteRecord > 0 {
		d.executingFixTinyDeleteRecord--
	}
}

func (d *Disk) evictExpiredFileDescriptor() {
	d.RLock()
	var partitions = make([]*DataPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.RUnlock()

	for _, partition := range partitions {
		partition.EvictExpiredFileDescriptor()
	}
}

func (d *Disk) forceEvictFileDescriptor() {
	var count = atomic.LoadInt64(&d.fdCount)
	log.LogDebugf("action[forceEvictFileDescriptor] disk(%v) current FD count(%v)",
		d.Path, count)
	//if d.fdLimit.MaxFDLimit == 0 || uint64(count) <= d.fdLimit.MaxFDLimit {
	//	return
	//}

	d.RLock()
	var partitions = make([]*DataPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.RUnlock()
	var ratio = storage.NewRatio(d.fdLimit.ForceEvictRatio)
	var flushedCount int
	for _, partition := range partitions {
		partition.ForceEvictFileDescriptor(ratio)
		flushedCount += partition.ForceFlushAllFD()
	}
	log.LogDebugf("action[forceEvictFileDescriptor] disk(%v) evicted FD count [%v -> %v],flushed count(%v)",
		d.Path, count, atomic.LoadInt64(&d.fdCount), flushedCount)
}
