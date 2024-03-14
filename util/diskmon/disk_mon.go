// Copyright 2024 The CubeFS Authors.
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

package diskmon

import (
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/shirou/gopsutil/disk"
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)

const (
	DiskStatusFile           = ".diskStatus"
	DiskHangCnt              = 2
	DefaultMAXFsUsedFactor   = 0.6
	DefReservedSpaceMaxRatio = 0.05
)

type FsCapMon struct {
	sync.RWMutex
	Path          string
	IsRocksDBDisk bool
	ReservedSpace uint64
	Total         float64
	Used          float64
	Available     float64
	Status        int8
	lastUpdate    time.Time
}

func GetDiskTotal(path string) (total uint64, err error) {
	var (
		usageStat = new(disk.UsageStat)
	)

	if usageStat, err = disk.Usage(path); err != nil {
		return
	}

	total = usageStat.Total
	return
}

func NewFsMon(path string, isRocksDBDisk bool, reservedSpace uint64) (d *FsCapMon) {
	d = new(FsCapMon)
	d.Path = path
	d.IsRocksDBDisk = isRocksDBDisk

	d.ComputeUsage()
	if isRocksDBDisk {
		reservedSpace = uint64(math.Min(float64(reservedSpace), d.Total*DefReservedSpaceMaxRatio))
	} else {
		reservedSpace = 0
	}
	d.ReservedSpace = reservedSpace

	d.Status = ReadWrite
	d.lastUpdate = time.Now()
	return
}

func (d *FsCapMon) GetStatus() int8 {
	d.RLock()
	defer d.RUnlock()
	return d.Status
}

func (d *FsCapMon) UpdateReversedSpace(space uint64) {
	d.RLock()
	defer d.RUnlock()
	if !d.IsRocksDBDisk {
		return
	}
	reservedSpace := uint64(math.Min(float64(space*util.MB), d.Total*DefReservedSpaceMaxRatio))
	d.ReservedSpace = reservedSpace
}

// Compute the disk usage
func (d *FsCapMon) ComputeUsage() (err error) {
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(d.Path, &fs)
	if err != nil {
		return
	}

	d.Total = float64(fs.Blocks) * float64(fs.Bsize)
	d.Available = float64(fs.Bavail) * float64(fs.Bsize)
	d.Used = d.Total - d.Available
	log.LogDebugf("action[computeUsage] disk(%v) all(%v) available(%v) used(%v)", d.Path, d.Total, d.Available, d.Used)

	return
}

func (d *FsCapMon) isDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, syscall.EIO.Error()) || strings.Contains(errMsg, syscall.EROFS.Error()) || strings.Contains(errMsg, "write disk hang") {
		return true
	}

	return false
}

func (d *FsCapMon) TriggerDiskError(err error) {
	if err == nil {
		return
	}
	if d.isDiskErr(err.Error()) {
		d.Status = Unavailable
	}
}

func (d *FsCapMon) cleanDiskError() {
	if d.Status != ReadWrite {
		log.LogWarnf("clean disk(%s) status:%d--->%d", d.Path, d.Status, ReadWrite)
	}
	d.Status = ReadWrite
}

func (d *FsCapMon) updateCheckTick() {
	d.RLock()
	defer d.RUnlock()

	d.cleanDiskError()
	d.lastUpdate = time.Now()
}

func (d *FsCapMon) UpdateDiskTick() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("update space info panic, recover: %v", r)
			exporter.WarningAppendKey("RecoverPanic", "update space info panic")
		}
	}()
	var err error
	var fp *os.File
	defer func() {
		if err != nil {
			d.TriggerDiskError(err)
		}
	}()

	path := path.Join(d.Path, DiskStatusFile)
	fp, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		return
	}
	defer fp.Close()
	data := []byte(DiskStatusFile)
	_, err = fp.WriteAt(data, 0)
	if err != nil {
		return
	}
	if err = fp.Sync(); err != nil {
		return
	}
	if _, err = fp.ReadAt(data, 0); err != nil {
		return
	}
	d.updateCheckTick()
}

func (d *FsCapMon) CheckDiskStatus(interval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("update space info panic, recover: %v", r)
			exporter.WarningAppendKey("RecoverPanic", "update space info panic")
		}
	}()
	d.RLock()
	defer d.RUnlock()
	timeOutCnt := time.Since(d.lastUpdate) / interval
	// twice not update, will set unavailable, and alarm
	if timeOutCnt > DiskHangCnt {
		d.TriggerDiskError(fmt.Errorf("write disk hang, last update:%v, now:%v, cnt:%d", d.lastUpdate, time.Now(), timeOutCnt))
	}
}

type DiskStat struct {
	Path           string
	Total          uint64
	Available      uint64
	PartitionCount int
}

func (s *DiskStat) GetScore(totalAvailable uint64, totalPartition int) (score float64) {
	// NOTE:
	//	available ratio = available / total available
	//	count ratio = partition count / total partition count
	//	score = 1 / (1+exp(1 - free/allfree)) +  1 / (1 + exp(num/allnum))

	avaliableRatio := float64(s.Available) / float64(totalAvailable)
	countRatio := float64(0)
	if totalPartition != 0 {
		countRatio = float64(s.PartitionCount) / float64(totalPartition)
	}
	left := 1 / (1 + math.Exp(1-avaliableRatio))
	right := 1 / (1 + math.Exp(countRatio))
	score = left + right
	return
}

func NewDiskStat(dir string) (s DiskStat, err error) {
	fs := syscall.Statfs_t{}
	if err = syscall.Statfs(dir, &fs); err != nil {
		log.LogErrorf("[NewDiskStat] statfs dir(%v) has err(%v)", dir, err.Error())
		return
	}
	s.Path = dir
	s.Total = fs.Blocks * uint64(fs.Bsize)
	s.Available = fs.Bavail * uint64(fs.Bsize)
	return
}

func SelectDisk(stats []DiskStat, fsUsedFactor float64) (disk DiskStat, err error) {
	if fsUsedFactor <= 0 || fsUsedFactor > 1 {
		fsUsedFactor = DefaultMAXFsUsedFactor
	}
	result := make([]DiskStat, 0, len(stats))

	var (
		sumAvail uint64
		sumCount int
	)

	for _, stat := range stats {
		space := float64(stat.Total) * fsUsedFactor
		if (stat.Total - stat.Available) > uint64(space) {
			log.LogWarnf("[SelectDiskByStat] dir(%v) not enough space(%v) of disk, skip", stat.Path, stat.Available)
			continue
		}

		result = append(result, stat)

		sumAvail += uint64(stat.Available)
		sumCount += stat.PartitionCount
	}

	if len(result) == 0 {
		err = fmt.Errorf("select disk got 0 result")
		return
	}

	maxScore := float64(0)
	for _, ds := range result {
		score := ds.GetScore(sumAvail, sumCount)
		if score > maxScore {
			disk = ds
			maxScore = score
		}
	}

	return
}
