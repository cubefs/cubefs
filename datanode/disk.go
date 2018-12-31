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
	"io/ioutil"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
)

var (
	// RegexpDataPartitionDir validates the directory name of a data partition.
	RegexpDataPartitionDir, _ = regexp.Compile("^datapartition_(\\d)+_(\\d)+$")
)

// DiskUsage includes different statistics of the disk usage.
// TODO it seems that these fields are duplicated with the ones in Disk
type DiskUsage struct {
	Total       uint64
	Used        uint64
	Available   uint64
	Unallocated uint64
	Allocated   uint64
}

// Disk represents the structure of the disk
type Disk struct {
	sync.RWMutex
	Path         string
	ReadErrCnt     uint64 // number of read errors
	WriteErrCnt    uint64 // number of write errors

	// TODO we have the following in DiskUsage already
	Total        uint64
	Used         uint64
	Available    uint64
	Unallocated  uint64
	Allocated    uint64

	MaxErrCnt      int // maximum number of errors TODO shouldn't this value be a constant ?
	Status       int // TODo disk status such as XXX
	RestSize     uint64 // TODO what is reset size?

	// TODO will dpMap sound better?
	partitionMap map[uint64]*DataPartition
	space        *SpaceManager
}

type PartitionVisitor func(dp *DataPartition)

func NewDisk(path string, restSize uint64, maxErrCnt int, space *SpaceManager) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.RestSize = restSize

	// TODO why maxErrs has been set twice here?
	d.MaxErrCnt = maxErrCnt

	d.space = space
	d.partitionMap = make(map[uint64]*DataPartition)
	d.RestSize = restSize

	// TODO why maxErrs has been set twice here?
	d.MaxErrCnt = 2000

	// TODO do we need to call computeUsage here? It will be called in startScheduleTasks as well
	d.computeUsage()

	d.startScheduleTasks()
	return
}

// PartitionCount returns the number of partitions in the partition map.
func (d *Disk) PartitionCount() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.partitionMap)
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

	// TODO how about:
	//  total := math.Max(0, int64(fs.Blocks*uint64(fs.Bsize) - d.RestSize))
	total := int64(fs.Blocks*uint64(fs.Bsize) - d.RestSize)
	if total < 0 {
		total = 0
	}
	d.Total = uint64(total)

	// TODO how about:
	// available := math.Max(0, int64(fs.Bavail*uint64(fs.Bsize) - d.RestSize))
	available := int64(fs.Bavail*uint64(fs.Bsize) - d.RestSize)
	if available < 0 {
		available = 0
	}
	d.Available = uint64(available)

	// TODO how about:
	// used := math.Max(0, int64(total - available))
	used := int64(total - available)
	if used < 0 {
		used = 0
	}
	d.Used = uint64(used)

	allocatedSize := int64(0)
	for _, dp := range d.partitionMap {
		allocatedSize += int64(dp.Size())
	}
	d.Allocated = uint64(allocatedSize)

	// TODO how about:
	// unallocated = math.Max(0, total - allocatedSize)
	unallocated := total - allocatedSize
	if unallocated < 0 {
		unallocated = 0
	}
	d.Unallocated = uint64(unallocated)

	log.LogDebugf("action[computeUsage] disk(%v) all(%v) available(%v) used(%v)", d.Path, d.Total, d.Available, d.Used)

	return
}

// TODO do we really need this wrapper?
func (d *Disk) updateReadErrCnt() {
	atomic.AddUint64(&d.ReadErrCnt, 1)
}

// TODO do we really need this wrapper?
func (d *Disk) updateWriteErrCnt() {
	atomic.AddUint64(&d.WriteErrCnt, 1)
}

// TODO we need to find a better name for this fucntion, or juts inline it. There is no "task" in this function.
func (d *Disk) startScheduleTasks() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer func() {
			ticker.Stop()
		}()
		for {
			select {
			case <-ticker.C:
				d.computeUsage()

				// TODO we can just inline this function here
				d.updateSpaceInfo()
			}
		}
	}()
}

// TODO we can just inline this function
func (d *Disk) updateSpaceInfo() (err error) {
	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(d.Path, &statsInfo); err != nil {
		d.updateReadErrCnt()
	}
	currErrs := d.ReadErrCnt + d.WriteErrCnt
	if currErrs >= uint64(d.MaxErrCnt) {
		d.Status = proto.Unavaliable
	} else if d.Available <= 0 {
		d.Status = proto.ReadOnly
	} else {
		d.Status = proto.ReadWrite
	}
	log.LogDebugf("action[updateSpaceInfo] disk[%v] total[%v] available[%v] remain[%v] "+
		"restSize[%v] maxErrs[%v] readErrs[%v] writeErrs[%v] status[%v]", d.Path,
		d.Total, d.Available, d.Unallocated, d.RestSize, d.MaxErrCnt, d.ReadErrCnt, d.WriteErrCnt, d.Status)
	return
}

// AttachDataPartition adds a data partition to the partition map.
func (d *Disk) AttachDataPartition(dp *DataPartition) {
	d.Lock()
	d.partitionMap[dp.ID()] = dp
	d.Unlock()
	d.computeUsage()
}

// DetachDataPartition removes a data partition from the partition map.
func (d *Disk) DetachDataPartition(dp *DataPartition) {
	d.Lock()
	delete(d.partitionMap, dp.ID())
	d.Unlock()
	d.computeUsage()
}

// GetDataPartition returns the data partition based on the given partition ID.
func (d *Disk) GetDataPartition(partitionID uint64) (partition *DataPartition) {
	d.RLock()
	defer d.RUnlock()
	return d.partitionMap[partitionID]
}

// TODO why call it "forced"? why not just call it "LoadPartitionHeaders" ?
func (d *Disk) ForceLoadPartitionHeader() {
	partitionList := d.DataPartitionList()
	for _, partitionID := range partitionList {
		partition := d.GetDataPartition(partitionID)
		partition.ForceLoadHeader()
	}
}

// DataPartitionList returns a list of the data partitions
func (d *Disk) DataPartitionList() (partitionIDs []uint64) {
	d.Lock()
	defer d.Unlock()
	partitionIDs = make([]uint64, 0, len(d.partitionMap))
	for _, dp := range d.partitionMap {
		partitionIDs = append(partitionIDs, dp.ID())
	}
	return
}

func unmarshalPartitionName(name string) (partitionID uint32, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error DataPartition name(%v)", name)
		return
	}
	var (
		pID int
	)
	if pID, err = strconv.Atoi(arr[1]); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	partitionID = uint32(pID)
	return
}

// TODO why we need this wrapper? It is just one line of code.
func (d *Disk) isPartitionDir(filename string) (is bool) {
	is = RegexpDataPartitionDir.MatchString(filename)
	return
}

// RestorePartition reads the files stored on the local disk and restores the data partitions.
func (d *Disk) RestorePartition(visitor PartitionVisitor) {
	var (
		partitionID   uint32
		partitionSize int
	)

	fileInfoList, err := ioutil.ReadDir(d.Path)
	if err != nil {
		log.LogErrorf("action[RestorePartition] read dir(%v) err(%v).", d.Path, err)
		return
	}

	var wg sync.WaitGroup
	for _, fileInfo := range fileInfoList {
		filename := fileInfo.Name()
		if !d.isPartitionDir(filename) {
			continue
		}

		if partitionID, partitionSize, err = unmarshalPartitionName(filename); err != nil {
			log.LogErrorf("action[RestorePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
				filename, d.Path, err.Error())
			continue
		}
		log.LogDebugf("acton[RestorePartition] disk(%v) path(%v) partitionID(%v) partitionSize(%v).",
			d.Path, fileInfo.Name(), partitionID, partitionSize)
		wg.Add(1)

		go func(partitionID uint32, filename string) {
			var (
				dp  *DataPartition
				err error
			)
			defer wg.Done()
			if dp, err = LoadDataPartition(path.Join(d.Path, filename), d); err != nil {
				log.LogError(fmt.Sprintf("action[RestorePartition] new partition(%v) err(%v) ",
					partitionID, err.Error()))
				log.LogFlush()
				panic(err.Error())
				return
			}
			if visitor != nil {
				visitor(dp)
			}

		}(partitionID, filename)
	}
	wg.Wait()
	go d.ForceLoadPartitionHeader()
}
