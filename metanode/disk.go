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

package metanode

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/diskusage"
	"github.com/chubaofs/chubaofs/util/log"
	"os"
	"time"
)

const (
	UpdateDiskSpaceInterval = 10 * time.Second
	CheckDiskStatusInterval = 1 * time.Minute
)

// Compute the disk usage

//
//msg := fmt.Sprintf("disk path %v error(%s) on %v", d.Path, err.Error(), d.nodeInfo.localAddr)
//exporter.Warning(msg)
//log.LogErrorf(msg)

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
				log.LogInfof("[MetaNode]stop disk stat  \n")
				return
			case <-updateSpaceInfoTicker.C:

				for _, d := range m.disks {
					d.ComputeUsage()
				}

				break
			case <-checkStatusTicker.C:
				for _, d := range m.disks {
					d.UpdateDiskTick()
				}
				break
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
				log.LogInfof("[MetaNode]stop disk stat  \n")
				return
			case <-checkStatusTicker.C:
				for _, d := range m.disks {
					d.CheckDiskStatus(CheckDiskStatusInterval)
				}
				break
			}
		}
	}()
}

func (m *MetaNode) addDisk(path string, isRocksDBDisk bool, reservedSpace uint64) *diskusage.FsCapMon {
	//add disk when node start, can not add
	if len(path) == 0 {
		return nil
	}

	if _, err := os.Stat(path); err != nil {
		log.LogInfof("add disk failed, no such dir/file:%v", path)
		//dir may create after add mon, just add to map
		//return nil
	}

	disk, ok := m.disks[path]
	if disk == nil || !ok {
		disk = diskusage.NewFsMon(path, isRocksDBDisk, reservedSpace)
		m.disks[path] = disk
		log.LogInfof("add disk:%v", disk)
		return disk
	}

	log.LogInfof("already add disk:%v ", disk)
	return disk
}

func (m *MetaNode) startDiskStat() error {
	rootDirIsRocksDBDisk := len(m.rocksDirs) == 0 || contains(m.rocksDirs, m.metadataDir)
	m.disks = make(map[string]*diskusage.FsCapMon)
	m.diskStopCh = make(chan struct{})
	m.addDisk(m.metadataDir, rootDirIsRocksDBDisk, m.diskReservedSpace)
	m.addDisk(m.raftDir, false, 0)
	for _, rocksDir := range m.rocksDirs {
		m.addDisk(rocksDir, true, m.diskReservedSpace)
	}

	m.startScheduleToUpdateSpaceInfo()
	m.startScheduleToCheckDiskStatus()
	return nil
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}

func (m *MetaNode) stopDiskStat() {
	close(m.diskStopCh)
}

func (m *MetaNode) getDisks() []*diskusage.FsCapMon {
	disks := make([]*diskusage.FsCapMon, 0, len(m.disks))
	for _, d := range m.disks {
		disks = append(disks, d)
	}
	return disks
}

func (m *MetaNode) getRocksDBDiskStat() []*proto.MetaNodeDiskInfo {
	disks := make([]*proto.MetaNodeDiskInfo, 0, len(m.disks))
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
		disks = append(disks, &proto.MetaNodeDiskInfo{
			Path:       d.Path,
			Total:      total,
			Used:       uint64(d.Used),
			UsageRatio: ratio,
			Status:     d.Status,
			MPCount:    d.MPCount,
		})
	}
	return disks
}

func (m *MetaNode) getSingleDiskStat(path string) *diskusage.FsCapMon {
	disk, ok := m.disks[path]
	if !ok {
		return nil
	}

	return disk
}
