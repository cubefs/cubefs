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
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	StrawDiskSelectorName = "Straw"
	HashDiskSelectorName  = "Hash"
	NormalizeSelectorName = "Normalize"
)

func NewDiskSelector(name string, capacityWeight, partitionWeight, affinityWeight float64) DiskSelector {
	switch name {
	case StrawDiskSelectorName:
		return NewStrawDiskSelector()
	case HashDiskSelectorName:

		return NewHashDiskSelector()
	case NormalizeSelectorName:
		return NewNormalizeDiskSelector(capacityWeight, partitionWeight, affinityWeight)
	default:
		return NewNormalizeDiskSelector(capacityWeight, partitionWeight, affinityWeight)
	}
}

type DiskSelector interface {
	GetName() string
	Select(volumeID string, partitionID uint64, disks map[string]*Disk, decommissionedDisks map[string]struct{}) *Disk
}

type StrawDiskSelector struct {
	rand *rand.Rand
}

func NewStrawDiskSelector() *StrawDiskSelector {
	return &StrawDiskSelector{
		rand: rand.New(rand.NewSource(time.Now().Unix())),
	}
}

func (s *StrawDiskSelector) GetName() string {
	return StrawDiskSelectorName
}

func (s *StrawDiskSelector) Select(volumeID string, partitionID uint64, disks map[string]*Disk, decommissionedDisks map[string]struct{}) *Disk {
	var d *Disk
	maxStraw := float64(0)

	for _, disk := range disks {
		if _, ok := decommissionedDisks[disk.Path]; ok {
			log.LogInfof("StrawDiskSelector exclude decommissioned disk[%v]", disk.Path)
			continue
		}

		if disk.Status != proto.ReadWrite {
			log.LogInfof("StrawDiskSelector disk(%v) is not writable", disk.Path)
			continue
		}

		if disk.isLost {
			log.LogInfof("StrawDiskSelector disk(%v) is lost", disk.Path)
			continue
		}

		straw := float64(s.rand.Intn(DiskSelectMaxStraw))
		straw = math.Log(straw/float64(DiskSelectMaxStraw)) / (float64(atomic.LoadUint64(&disk.Available)) / util.GB)
		if d == nil || straw > maxStraw {
			maxStraw = straw
			d = disk
		}
	}
	if d != nil && d.Status != proto.ReadWrite {
		log.LogWarnf("StrawDiskSelector no available disks(%v)", disks)
		return nil
	}
	return d
}

type HashDiskSelector struct{}

func NewHashDiskSelector() *HashDiskSelector {
	return &HashDiskSelector{}
}

func (s *HashDiskSelector) GetName() string {
	return HashDiskSelectorName
}

func (s *HashDiskSelector) Select(volumeID string, partitionID uint64, disks map[string]*Disk, decommissionedDisks map[string]struct{}) *Disk {
	cnt := 0
	availableDisks := make([]string, len(disks))

	for _, disk := range disks {
		if _, ok := decommissionedDisks[disk.Path]; ok {
			log.LogInfof("HashDiskSelector exclude decommissioned disk[%v]", disk.Path)
			continue
		}

		if disk.Status != proto.ReadWrite {
			log.LogInfof("HashDiskSelector disk(%v) is not writable", disk.Path)
			continue
		}

		if disk.isLost {
			log.LogInfof("HashDiskSelector disk(%v) is lost", disk.Path)
			continue
		}
		availableDisks[cnt] = disk.Path
		cnt++
	}
	if cnt == 0 {
		log.LogWarnf("HashDiskSelector no available disks(%v)", disks)
		return nil
	}

	availableDisks = availableDisks[:cnt]
	sort.Strings(availableDisks)
	index := partitionID % (uint64)(cnt)
	path := availableDisks[index]
	disk, ok := disks[path]
	if ok {
		return disk
	}

	return nil
}

type NormalizeDiskSelector struct {
	capacityWeight  float64
	partitionWeight float64
	affinityWeight  float64
}

func NewNormalizeDiskSelector(capacityWeight, partitionWeight, affinityWeight float64) *NormalizeDiskSelector {
	return &NormalizeDiskSelector{
		capacityWeight:  capacityWeight,
		partitionWeight: partitionWeight,
		affinityWeight:  affinityWeight,
	}
}

func (s *NormalizeDiskSelector) GetName() string {
	return NormalizeSelectorName
}

func (s *NormalizeDiskSelector) Select(volumeID string, partitionID uint64, disks map[string]*Disk, decommissionedDisks map[string]struct{}) *Disk {
	totalPartitionCnt := 0
	availableDiskCnt := 0

	for _, disk := range disks {
		if _, ok := decommissionedDisks[disk.Path]; ok {
			log.LogInfof("NormalizeDiskSelector exclude decommissioned disk[%v]", disk.Path)
			continue
		}
		if disk.isLost {
			log.LogInfof("NormalizeDiskSelector disk(%v) is lost", disk.Path)
			continue
		}
		availableDiskCnt++
		totalPartitionCnt += len(disk.partitionMap)
	}

	var ret *Disk
	var weight float64 = 0
	avgPartitionCnt := float64(totalPartitionCnt) / float64(availableDiskCnt)
	for _, disk := range disks {
		if _, ok := decommissionedDisks[disk.Path]; ok {
			continue
		}
		if disk.Status != proto.ReadWrite {
			continue
		}
		if disk.isLost {
			continue
		}
		w := s.calculateDiskWeight(disk, volumeID, avgPartitionCnt)
		if w > weight {
			weight = w
			ret = disk
		}
	}

	return ret
}

func (s *NormalizeDiskSelector) calculateDiskWeight(disk *Disk, volumeID string, avgPartitionCnt float64) float64 {
	capacityRatio := float64(disk.Available) / float64(disk.Total)

	var partitionRatio float64 = 1.0
	if avgPartitionCnt > 0 {
		partitionRatio = math.Max(1.0, avgPartitionCnt/float64(1+len(disk.partitionMap)))
	}

	affinityRatio := 1 / math.Pow(math.E/2, float64(disk.GetVolumePartitionCount(volumeID)))

	return capacityRatio*s.capacityWeight + partitionRatio*s.partitionWeight + affinityRatio*s.affinityWeight
}
