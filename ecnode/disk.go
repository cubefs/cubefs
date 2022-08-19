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
	"fmt"
	"github.com/chubaofs/chubaofs/ecstorage"
	"github.com/chubaofs/chubaofs/util/exporter"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	// RegexpEcPartitionDir validates the directory name of a data partition.
	RegexpEcPartitionDir, _ = regexp.Compile("^ecpartition_(\\d)+_(\\d)+$")
)

const (
	DiskStatusFile         = ".diskStatus"
	ExpiredPartitionPrefix = "expired_"
)

type EcFdLimit struct {
	MaxFDLimit      uint64  // 触发强制FD淘汰策略的阈值
	ForceEvictRatio float64 // 强制FD淘汰比例
}

type ScrubInfo struct {
	partitionId   uint64
	scrubExtentId uint64
}

type EcNodeInfo struct {
	Addr                    string
	PersistenceEcPartitions []uint64
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
	stopC         chan bool
	RejectWrite   bool
	partitionMap  map[uint64]*EcPartition
	space         *SpaceManager
	ecDb          *ecstorage.EcRocksDbInfo

	// Runtime statistics
	fdCount int64
	fdLimit EcFdLimit
}

func NewDisk(path string, reservedSpace uint64, maxErrCnt int, fdLimit EcFdLimit, space *SpaceManager) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.ReservedSpace = reservedSpace
	d.MaxErrCnt = maxErrCnt
	d.RejectWrite = false
	d.space = space
	d.stopC = make(chan bool, 0)
	d.partitionMap = make(map[uint64]*EcPartition)
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

func (d *Disk) startEcRocksdb() (err error) {
	newDbDir := d.Path + "/ecdb"
	log.LogDebugf("startEcRocksdb dir:%v", newDbDir)
	d.ecDb, err = ecstorage.NewEcRocksdbHandle(newDbDir)
	if err != nil {
		log.LogErrorf("startEcRocksdb err:%v", err)
	}
	return
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

	used := total - available
	if used < 0 {
		used = 0
	}
	d.Used = uint64(used)

	allocatedSize := int64(0)
	allEpfinishEc := true
	for _, ep := range d.partitionMap {
		if proto.IsEcFinished(ep.ecMigrateStatus) {
			allocatedSize += int64(ep.Used())//partition used 会比预占空间小，因为包含已经打洞的空间，如果有打洞会比真实使用空间大
		}else {
			allocatedSize += int64(ep.Size())
			allEpfinishEc = false
		}
	}
	if allEpfinishEc {
		allocatedSize = int64(d.Used)
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

	log.LogDebugf("action[computeUsage] disk(%v) all(%v) available(%v) used(%v)", d.Path, d.Total, d.Available, d.Used)

	return
}

func (d *Disk) incReadErrCnt() {
	atomic.AddUint64(&d.ReadErrCnt, 1)
}

func (d *Disk) incWriteErrCnt() {
	atomic.AddUint64(&d.WriteErrCnt, 1)
}

// PartitionCount returns the number of partitions in the partition map.
func (d *Disk) PartitionCount() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.partitionMap)
}

func (d *Disk) updateSpaceInfo() (err error) {
	var statsInfo syscall.Statfs_t

	err = syscall.Statfs(d.Path, &statsInfo)
	if err != nil {
		d.incReadErrCnt()
	}

	if d.Status == proto.Unavailable {
		mesg := fmt.Sprintf("disk path %v error on %v", d.Path, localIP)
		log.LogErrorf(mesg)
		exporter.Warning(mesg)
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

func IsDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, syscall.EIO.Error()) || strings.Contains(errMsg, syscall.EROFS.Error()) {
		return true
	}

	return false
}

func (d *Disk) triggerDiskError(err error) {
	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", d.Path, localIP)
		exporter.Warning(mesg)
		log.LogErrorf(mesg)
		//d.ForceExitRaftStore()
		d.Status = proto.Unavailable
	}
	return
}

func (d *Disk) Stop() {
	if d.stopC != nil {
		close(d.stopC)
	}
	partitions := make([]*EcPartition, 0)
	d.RangePartitions(func(ep *EcPartition) bool {
		partitions = append(partitions, ep)
		return true
	})
	for _, ep := range partitions {
		ep.Stop()
	}

}

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

func (d *Disk) startScheduler() {
	go func() {
		updateSpaceInfoTicker := time.NewTicker(5 * time.Second)
		checkStatusTicker := time.NewTicker(time.Minute * 2)
		evictFDTicker := time.NewTicker(time.Minute * 5)
		forceEvictFDTicker := time.NewTicker(time.Second * 10)
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
			case <-evictFDTicker.C:
				d.evictExpiredFileDescriptor()
			case <-forceEvictFDTicker.C:
				d.forceEvictFileDescriptor()
			}
		}
	}()
}

func (d *Disk) evictExpiredFileDescriptor() {
	d.RLock()
	var partitions = make([]*EcPartition, 0, len(d.partitionMap))
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
	log.LogDebugf("action[forceEvictFileDescriptor] disk [%v] current FD count [%v]",
		d.Path, count)
	if d.fdLimit.MaxFDLimit == 0 || uint64(count) <= d.fdLimit.MaxFDLimit {
		return
	}

	d.RLock()
	var partitions = make([]*EcPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.RUnlock()
	var ratio = ecstorage.NewRatio(d.fdLimit.ForceEvictRatio)
	for _, partition := range partitions {
		partition.ForceEvictFileDescriptor(ratio)
	}
	log.LogDebugf("action[forceEvictFileDescriptor] disk [%v] evicted FD count [%v -> %v]",
		d.Path, count, atomic.LoadInt64(&d.fdCount))
}

func (d *Disk) isPartitionDir(fileName string) (isPartitionDir bool) {
	isPartitionDir = RegexpEcPartitionDir.MatchString(fileName)
	return
}

func unmarshalPartitionName(name string) (partitionID uint32, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error DataPartition name(%v)", name)
		return
	}

	pID, err := strconv.Atoi(arr[1])
	if err != nil {
		return
	}
	partitionSize, err = strconv.Atoi(arr[2])
	if err != nil {
		return
	}
	partitionID = uint32(pID)
	return
}

func (d *Disk) RangePartitions(f func(partition *EcPartition) bool) {
	if f == nil {
		return
	}

	d.RLock()
	partitions := make([]*EcPartition, 0)
	for _, ep := range d.partitionMap {
		partitions = append(partitions, ep)
	}
	d.RUnlock()

	for _, partition := range partitions {
		if !f(partition) {
			break
		}
	}
}

func (d *Disk) ecScheduleScrub() {
	scrubTimer := time.NewTimer(time.Minute * 0)
	computeCrcTimer := time.NewTimer(time.Minute * 0)
	for {
		select {
		case <-scrubTimer.C:
			d.ecCheckScrub()
			scrubTimer.Reset(time.Minute * defaultScrubCheckInterval)
		case <-computeCrcTimer.C:
			d.autoComputeEcTinyExtentCrc()
			computeCrcTimer.Reset(time.Minute * defaultComputeCrcInterval)
		case <-d.stopC:
			scrubTimer.Stop()
			computeCrcTimer.Stop()
			return
		}
	}
}

func (ep *EcPartition) checkScrubNextPeriodCome(ecNode *EcNode) (come bool) {
	come = true
	scrubPeriod := int64(ecNode.scrubPeriod) * defaultScrubPeriodTime
	if time.Now().Unix()-ep.ScrubStartTime < scrubPeriod {
		come = false
	}
	return
}

func (d *Disk) ecCheckScrub() {
	if d.Status == proto.Unavailable {
		return
	}
	ecNode := d.space.ecNode
	scrubEnable := ecNode.scrubEnable
	if !scrubEnable {
		return
	}
	partitions := make([]*EcPartition, 0)
	d.RangePartitions(func(ep *EcPartition) bool {
		if proto.IsEcFinished(ep.ecMigrateStatus) {
			partitions = append(partitions, ep)
		}
		return true
	})
	if len(partitions) == 0 {
		return
	}
	for _, ep := range partitions {
		localMaxExtentId, _ := ep.extentStore.GetMaxExtentIDAndPartitionSize()
		if len(ep.FailScrubExtents) != 0 {
			ep.scrubFailPartitionExtents(ecNode)
		}
		if ep.MaxScrubDoneId >= localMaxExtentId { // all partition extents scrub finished
			if come := ep.checkScrubNextPeriodCome(ecNode); !come { //wait next scrub period come handle
				continue
			}
			ep.MaxScrubDoneId = 0
			ep.FailScrubExtents = make(map[uint64]uint64)
			ep.ScrubStartTime = time.Now().Unix()
			if err := ep.PersistMetaData(); err != nil {
				log.LogErrorf("partition[%v] persist err[%v]", ep.PartitionID, err)
				continue
			}
		}
		if err := ep.scrubPartitionData(ecNode); err != nil {
			log.LogWarnf("scrubPartitionData err[%v]", err) //wait next schedule handle
		}
	}
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

func (d *Disk) RestorePartition(visitor func(ep *EcPartition)) {
	var (
		partitionID   uint32
		partitionSize int
	)
	var convert = func(node *proto.EcNodeInfo) *EcNodeInfo {
		result := &EcNodeInfo{}
		result.Addr = node.Addr
		result.PersistenceEcPartitions = node.PersistenceDataPartitions
		return result
	}
	fileInfoList, err := ioutil.ReadDir(d.Path)
	if err != nil {
		log.LogErrorf("action[RestorePartition] read dir(%v) err(%v).", d.Path, err)
		return
	}
	var ecNode *proto.EcNodeInfo
	for i := 0; i < 3; i++ {
		ecNode, err = MasterClient.NodeAPI().GetEcNode(d.space.ecNode.localServerAddr)
		if err != nil {
			log.LogErrorf("action[RestorePartition]: getDataNode error %v", err)
			continue
		}
		break
	}
	ecInfo := convert(ecNode)
	if len(ecInfo.PersistenceEcPartitions) == 0 {
		log.LogWarnf("action[RestorePartition]: length of PersistenceEcPartitions is 0, ExpiredPartition check " +
			"without effect")
	}

	var wg sync.WaitGroup
	for _, fileInfo := range fileInfoList {
		fileName := fileInfo.Name()
		if !d.isPartitionDir(fileName) {
			continue
		}

		partitionID, partitionSize, err = unmarshalPartitionName(fileName)
		if err != nil {
			log.LogErrorf("action[RestorePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
				fileName, d.Path, err.Error())
			continue
		}
		if isExpiredPartition(uint64(partitionID), ecInfo.PersistenceEcPartitions) {
			log.LogErrorf("action[RestorePartition]: find expired partition[%s], rename it and you can delete it "+
				"manually", fileName)
			oldName := path.Join(d.Path, fileName)
			newName := path.Join(d.Path, ExpiredPartitionPrefix+fileName)
			_ = os.Rename(oldName, newName)
			continue
		}
		log.LogDebugf("acton[RestorePartition] disk(%v) path(%v) PartitionID(%v) partitionSize(%v).",
			d.Path, fileInfo.Name(), partitionID, partitionSize)

		wg.Add(1)
		go func(partitionID uint32, filename string) {
			var (
				ep  *EcPartition
				err error
			)
			defer wg.Done()
			if ep, err = LoadEcPartition(path.Join(d.Path, filename), d); err != nil {
				mesg := fmt.Sprintf("action[RestorePartition] new partition(%v) err(%v) ",
					partitionID, err.Error())
				log.LogError(mesg)
				exporter.Warning(mesg)
				return
			}
			if visitor != nil {
				visitor(ep)
			}

		}(partitionID, fileName)
	}
}

func (d *Disk) AddSize(size uint64) {
	atomic.AddUint64(&d.Allocated, size)
}

// DetachEcPartition remove a partition from the partition map.
func (d *Disk) DetachEcPartition(ep *EcPartition) {
	d.Lock()
	delete(d.partitionMap, ep.PartitionID)
	d.Unlock()

	d.computeUsage()
}

// AttachEcPartition adds a partition to the partition map.
func (d *Disk) AttachEcPartition(ep *EcPartition) {
	d.Lock()
	d.partitionMap[ep.PartitionID] = ep
	d.Unlock()

	d.computeUsage()
}
