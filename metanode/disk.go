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

package metanode

import (
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/diskmon"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	UpdateDiskSpaceInterval = 10 * time.Second
	CheckDiskStatusInterval = 1 * time.Minute
)

// Compute the disk usage
func (m *MetaNode) startScheduleToUpdateSpaceInfo() {
	go func() {
		updateSpaceInfoTicker := time.NewTicker(UpdateDiskSpaceInterval)
		checkStatusTicker := time.NewTicker(CheckDiskStatusInterval)
		defer func() {
			updateSpaceInfoTicker.Stop()
			checkStatusTicker.Stop()
		}()
		for {
			select {
			case <-m.diskStopCh:
				log.LogInfof("[startScheduleToUpdateSpaceInfo] stop disk stat  \n")
				return
			case <-updateSpaceInfoTicker.C:

				for _, d := range m.disks {
					err := d.ComputeUsage()
					if err != nil {
						log.LogErrorf("[startScheduleToUpdateSpaceInfo] failed to compute usage for disk(%v), err(%v)", d.Path, err)
					}
				}
			case <-checkStatusTicker.C:
				for _, d := range m.disks {
					d.UpdateDiskTick()
				}
			}
		}
	}()
}

func (m *MetaNode) startScheduleToCheckDiskStatus() {
	go func() {
		checkStatusTicker := time.NewTicker(CheckDiskStatusInterval)
		defer func() {
			checkStatusTicker.Stop()
		}()
		for {
			select {
			case <-m.diskStopCh:
				log.LogInfof("[startScheduleToCheckDiskStatus] stop disk stat")
				return
			case <-checkStatusTicker.C:
				for _, d := range m.disks {
					d.CheckDiskStatus(CheckDiskStatusInterval)
				}
			}
		}
	}()
}

func (m *MetaNode) addDisk(path string, isRocksDBDisk bool, reservedSpace uint64) *diskmon.FsCapMon {
	if len(path) == 0 {
		return nil
	}

	if !fileutil.ExistDir(path) {
		// NOTE: dir may create after add mon, just add to map
		log.LogWarnf("[addDisk] disk dir(%v) not exists", path)
	}

	disk := m.disks[path]
	if disk == nil {
		disk = diskmon.NewFsMon(path, isRocksDBDisk, reservedSpace)
		m.disks[path] = disk
		log.LogInfof("[addDisk] add disk(%v) to diskmon", path)
		return disk
	}

	log.LogInfof("[addDisk] found disk(%v) in diskmon ", path)
	return disk
}

func (m *MetaNode) startDiskStat() error {
	m.disks = make(map[string]*diskmon.FsCapMon)
	foundRootDir := false
	for _, rocksDir := range m.rocksDirs {
		if strings.HasPrefix(rocksDir, m.metadataDir) {
			foundRootDir = true
		}
		m.addDisk(rocksDir, true, m.diskReservedSpace)
	}
	if !foundRootDir {
		m.addDisk(m.metadataDir, false, m.diskReservedSpace)
	}
	m.addDisk(m.raftDir, false, 0)

	m.startScheduleToUpdateSpaceInfo()
	m.startScheduleToCheckDiskStatus()
	return nil
}

func (m *MetaNode) stopDiskStat() {
	close(m.diskStopCh)
}

func (m *MetaNode) getRocksDBDiskStat() []*proto.MetaNodeRocksdbInfo {
	disks := make([]*proto.MetaNodeRocksdbInfo, 0, len(m.disks))
	for _, d := range m.disks {
		if !d.IsRocksDBDisk {
			continue
		}
		var ratio float64 = 0
		total := uint64(d.Total) - d.ReservedSpace
		if d.Used > 0 && d.Used <= float64(total) {
			ratio = d.Used / d.Total
		} else if d.Used > d.Total {
			ratio = 1
		}
		count, err := m.rocksdbManager.GetPartitionCount(d.Path)
		if err != nil {
			log.LogErrorf("[getRocksDBDiskStat] failed to get partition count on disk(%v), err(%v)", d.Path, err)
			err = nil
			count = 0
		}
		disks = append(disks, &proto.MetaNodeRocksdbInfo{
			Path:           d.Path,
			Total:          total,
			Used:           uint64(d.Used),
			UsageRatio:     ratio,
			Status:         d.Status,
			PartitionCount: count,
		})
	}
	return disks
}
