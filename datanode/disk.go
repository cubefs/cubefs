// Copyright 2018 The Containerfs Authors.
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

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

type CompactTask struct {
	partitionId uint32
	blobfileId  int
	isLeader    bool
}

func (t *CompactTask) toString() (m string) {
	return fmt.Sprintf("dataPartition(%v)_blobfile(%v)_isLeader(%v)", t.partitionId, t.blobfileId, t.isLeader)
}

const (
	CompactThreadNum = 4
)

var (
	ErrDiskCompactChanFull = errors.New("disk compact chan is full")
)

var (
	// Regexp pattern for data partition dir name validate.
	RegexpDataPartitionDir, _ = regexp.Compile("^datapartition_(\\d)+_(\\d)+$")
)

type DiskUsage struct {
	Total       uint64
	Used        uint64
	Available   uint64
	Unallocated uint64
	Allocated   uint64
}

type Disk struct {
	sync.RWMutex
	Path            string
	ReadErrs        uint64
	WriteErrs       uint64
	Total           uint64
	Used            uint64
	Available       uint64
	Unallocated     uint64
	Allocated       uint64
	MaxErrs         int
	Status          int
	RestSize        uint64
	partitionMap    map[uint32]DataPartition
	compactCh       chan *CompactTask
	space           SpaceManager
	compactTasks    map[string]*CompactTask
	compactTaskLock sync.RWMutex
}

type PartitionVisitor func(dp DataPartition)

func NewDisk(path string, restSize uint64, maxErrs int) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.RestSize = restSize
	d.MaxErrs = maxErrs
	d.partitionMap = make(map[uint32]DataPartition)
	d.RestSize = util.GB * 1
	d.MaxErrs = 2000
	d.compactTasks = make(map[string]int)
	d.compactCh = make(chan *CompactTask, CompactThreadNum)
	for i := 0; i < CompactThreadNum; i++ {
		go d.compact()
	}
	d.computeUsage()

	d.startScheduleTasks()
	return
}

func (d *Disk) PartitionCount() int {
	return len(d.partitionMap)
}

func (d *Disk) computeUsage() (err error) {
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(d.Path, &fs)
	if err != nil {
		return
	}

	// total
	total := int64(fs.Blocks*uint64(fs.Bsize) - d.RestSize)
	if total < 0 {
		total = 0
	}
	d.Total = uint64(total)

	// available
	available := int64(fs.Bavail*uint64(fs.Bsize) - d.RestSize)
	if available < 0 {
		available = 0
	}
	d.Available = uint64(available)

	// used
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

	unallocated := total - allocatedSize
	if unallocated < 0 {
		unallocated = 0
	}
	d.Unallocated = uint64(unallocated)

	log.LogDebugf("action[computeUsage] disk(%v) all(%v) available(%v) used(%v)", d.Path, d.Total, d.Available, d.Used)

	return
}

func (d *Disk) addTask(t *CompactTask) (err error) {
	select {
	case d.compactCh <- t:
		return
	default:
		return errors.Annotatef(ErrDiskCompactChanFull, "diskPath:(%v) partitionId(%v)", d.Path, t.partitionId)
	}
}

func (d *Disk) addReadErr() {
	atomic.AddUint64(&d.ReadErrs, 1)
}

func (d *Disk) compact() {
	for {
		select {
		case t := <-d.compactCh:
			dp := d.space.GetPartition(t.partitionId)
			if dp == nil {
				continue
			}
			err, release := dp.GetBlobStore().DoCompactWork(t.blobfileId)
			if err != nil {
				log.LogErrorf("action[compact] task(%v) compact error(%v)", t.toString(), err.Error())
			} else {
				log.LogInfof("action[compact] task(%v) compact success Release (%v)", t.toString(), release)
			}
		}
	}
}

func (d *Disk) addWriteErr() {
	atomic.AddUint64(&d.WriteErrs, 1)
}

func (d *Disk) startScheduleTasks() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				d.computeUsage()
				d.updateSpaceInfo()
			}
		}
	}()
}

func (d *Disk) updateSpaceInfo() (err error) {
	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(d.Path, &statsInfo); err != nil {
		d.addReadErr()
	}
	currErrs := d.ReadErrs + d.WriteErrs
	if currErrs >= uint64(d.MaxErrs) {
		d.Status = proto.Unavaliable
	} else if d.Available <= 0 {
		d.Status = proto.ReadOnly
	} else {
		d.Status = proto.ReadWrite
	}
	log.LogDebugf("action[updateSpaceInfo] disk(%v) total(%v) available(%v) remain(%v) "+
		"restSize(%v) maxErrs(%v) readErrs(%v) writeErrs(%v) status(%v)", d.Path,
		d.Total, d.Available, d.Unallocated, d.RestSize, d.MaxErrs, d.ReadErrs, d.WriteErrs, d.Status)
	return
}

func (d *Disk) AttachDataPartition(dp DataPartition) {
	d.Lock()
	defer d.Unlock()
	d.partitionMap[dp.ID()] = dp
	d.computeUsage()
}

func (d *Disk) hasExsitCompactTask(id string) (ok bool) {
	d.compactTaskLock.RLock()
	_, ok = d.compactTasks[id]
	d.compactTaskLock.RUnlock()
	return
}

func (d *Disk) putCompactTask(task *CompactTask) (err error) {
	select {
	case d.compactCh <- task:
		d.compactTaskLock.Lock()
		d.compactTasks[task.toString()] = task
		d.compactTaskLock.Unlock()
	default:
		return fmt.Errorf("cannot add compactTask(%v) to disk(%v)", task.toString(), d.Path)

	}

}

func (d *Disk) deleteCompactTask(id string) {
	d.compactTaskLock.Lock()
	delete(d.compactTasks, id)
	d.compactTaskLock.Unlock()
}

func (d *Disk) DetachDataPartition(dp DataPartition) {
	d.Lock()
	defer d.Unlock()
	delete(d.partitionMap, dp.ID())
	d.computeUsage()
}

func (d *Disk) DataPartitionList() (partitionIds []uint32) {
	d.Lock()
	defer d.Unlock()
	partitionIds = make([]uint32, 0, len(d.partitionMap))
	for _, dp := range d.partitionMap {
		partitionIds = append(partitionIds, dp.ID())
	}
	return
}

func unmarshalPartitionName(name string) (partitionId uint32, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error dataPartition name(%v)", name)
		return
	}
	var (
		pId int
	)
	if pId, err = strconv.Atoi(arr[1]); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	partitionId = uint32(pId)
	return
}

func (d *Disk) isPartitionDir(filename string) (is bool) {
	is = RegexpDataPartitionDir.MatchString(filename)
	return
}

func (d *Disk) RestorePartition(visitor PartitionVisitor) {
	var (
		partitionId   uint32
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

		if partitionId, partitionSize, err = unmarshalPartitionName(filename); err != nil {
			log.LogErrorf("action[RestorePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
				filename, d.Path, err.Error())
			continue
		}
		log.LogDebugf("acton[RestorePartition] disk(%v) path(%v) partitionId(%v) partitionSize(%v).",
			d.Path, fileInfo.Name(), partitionId, partitionSize)
		wg.Add(1)
		go func(partitionId uint32, filename string) {
			var (
				dp  DataPartition
				err error
			)
			defer wg.Done()
			if dp, err = LoadDataPartition(path.Join(d.Path, filename), d); err != nil {
				log.LogError(fmt.Sprintf("action[RestorePartition] new partition(%v) err(%v) ",
					partitionId, err.Error()))
				return
			}
			if visitor != nil {
				visitor(dp)
			}

		}(partitionId, filename)
	}
	wg.Wait()
}
