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

package datanode

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func prepareDisksForSelectDiskTest(t *testing.T, sm *SpaceManager, cnt int) {
	for i := 0; i < cnt; i++ {
		size := uint64(i+1) * 1000 * util.GB
		diskPath := fmt.Sprintf("/cfs/disk_%v", strconv.FormatInt(int64(i), 10))
		disk := &Disk{
			Total:     size,
			Available: size,
			Used:      0,
			Allocated: 0,
			Status:    proto.ReadWrite,
			Path:      diskPath,
		}
		sm.disks[diskPath] = disk
		t.Logf("disk(%v) space(%v) GB", diskPath, size/util.GB)
	}
}

func prepareFixedDisksForSelectDiskTest(t *testing.T, sm *SpaceManager, cnt int, size uint64) {
	for i := 0; i < cnt; i++ {
		size := size * 1024 * util.GB
		diskPath := fmt.Sprintf("/cfs/disk_%v", strconv.FormatInt(int64(i), 10))
		disk := &Disk{
			Total:        size,
			Available:    size,
			Used:         0,
			Allocated:    0,
			Status:       proto.ReadWrite,
			Path:         diskPath,
			partitionMap: make(map[uint64]*DataPartition),
		}
		sm.disks[diskPath] = disk
		t.Logf("disk(%v) space(%v) GB", diskPath, size/util.GB)
	}
}

func TestSelectDisk(t *testing.T) {
	sm := &SpaceManager{
		disks:        make(map[string]*Disk),
		diskSelector: NewDiskSelector(StrawDiskSelectorName, 0, 0, 0),
	}
	// prepare disks
	prepareDisksForSelectDiskTest(t, sm, 4)
	const testCount = 1500
	selectTimes := make(map[string]int)
	for _, disk := range sm.disks {
		selectTimes[disk.Path] = 0
	}
	decommissionDisk := []string{}
	for i := 0; i < testCount; i++ {
		disk := sm.selectDisk("vol", uint64(i), decommissionDisk)
		require.NotNil(t, disk)
		selectTimes[disk.Path] += 1
		used := rand.Float64() * util.GB * 10
		disk.Allocated += uint64(used)
		disk.Available -= uint64(used)
		disk.Used += uint64(used)
	}
	for disk, times := range selectTimes {
		t.Logf("disk(%v) select times(%v)", disk, times)
	}
	for _, disk := range sm.disks {
		t.Logf("disk(%v) left space(%v) GB", disk.Path, disk.Available/util.GB)
	}
	// NOTE: check for space
	ratio := make([]float64, 0)
	for _, disk := range sm.disks {
		r := float64(disk.Available) / float64(disk.Total)
		ratio = append(ratio, r)
		t.Logf("disk(%v) ratio(%v)", disk.Path, r)
	}
	sort.Slice(ratio, func(i, j int) bool {
		return ratio[i] < ratio[j]
	})
	require.Less(t, ratio[len(ratio)-1]-ratio[0], 0.2)
}

func TestSelectDiskForSmallDp(t *testing.T) {
	sm := &SpaceManager{
		disks:        make(map[string]*Disk),
		diskSelector: NewDiskSelector(StrawDiskSelectorName, 0, 0, 0),
	}
	// prepare disks
	prepareDisksForSelectDiskTest(t, sm, 4)
	const testCount = 1500
	selectTimes := make(map[string]int)
	for _, disk := range sm.disks {
		selectTimes[disk.Path] = 0
	}
	decommissionDisk := []string{}
	for i := 0; i < testCount; i++ {
		disk := sm.selectDisk("vol", uint64(i), decommissionDisk)
		require.NotNil(t, disk)
		selectTimes[disk.Path] += 1
	}
	for disk, times := range selectTimes {
		t.Logf("disk(%v) select times(%v)", disk, times)
	}
	for _, disk := range sm.disks {
		t.Logf("disk(%v) left space(%v) GB", disk.Path, disk.Available/util.GB)
	}
}

func TestHashSelectDisk(t *testing.T) {
	sm := &SpaceManager{
		disks:        make(map[string]*Disk),
		diskSelector: NewDiskSelector(HashDiskSelectorName, 0, 0, 0),
	}
	// prepare disks
	prepareFixedDisksForSelectDiskTest(t, sm, 24, 10)
	const testCount = 1500
	decommissionDisk := []string{}
	for i := 0; i < testCount; i++ {
		disk := sm.selectDisk("vol", uint64(i), decommissionDisk)
		require.NotNil(t, disk)
		used := util.GB * 10
		disk.Allocated += uint64(used)
		disk.Available -= uint64(used)
		disk.Used += uint64(used)
		disk.AttachDataPartition(&DataPartition{
			volumeID:    "vol",
			partitionID: uint64(i),
		})
	}
	for _, disk := range sm.disks {
		t.Logf("disk(%v) left space(%v) GB, data partition count(%d)", disk.Path, disk.Available/util.GB, len(disk.partitionMap))
	}
	// check space ratio
	spaceRatio := make([]float64, 0)
	for _, disk := range sm.disks {
		r := float64(disk.Available) / float64(disk.Total)
		spaceRatio = append(spaceRatio, r)
		t.Logf("disk(%v) ratio(%v)", disk.Path, r)
	}
	sort.Slice(spaceRatio, func(i, j int) bool {
		return spaceRatio[i] < spaceRatio[j]
	})
	require.Less(t, spaceRatio[len(spaceRatio)-1]-spaceRatio[0], 0.01)
	// check partition count
	partitionCnt := make([]int, 0)
	for _, disk := range sm.disks {
		partitionCnt = append(partitionCnt, len(disk.partitionMap))
		t.Logf("disk(%v) partition(%v)", disk.Path, len(disk.partitionMap))
	}
	sort.Ints(partitionCnt)
	require.Less(t, partitionCnt[len(partitionCnt)-1]-partitionCnt[0], 2)
}

func TestNormalizeSelectDisk(t *testing.T) {
	sm := &SpaceManager{
		disks:        make(map[string]*Disk),
		diskSelector: NewDiskSelector(NormalizeSelectorName, 0.5, 0.3, 0.2),
	}
	// prepare disks
	prepareFixedDisksForSelectDiskTest(t, sm, 24, 18)
	const testCount = 1500
	volumes := []string{"vol1", "vol2", "vol3", "vol4", "vol5"}
	decommissionDisk := []string{}
	for i := 0; i < testCount; i++ {
		r := rand.Int()
		vol := volumes[r%len(volumes)]
		disk := sm.selectDisk(vol, uint64(i), decommissionDisk)
		require.NotNil(t, disk)
		used := rand.Intn(10) * util.GB
		disk.Allocated += uint64(used)
		disk.Available -= uint64(used)
		disk.Used += uint64(used)
		disk.AttachDataPartition(&DataPartition{
			volumeID:    vol,
			partitionID: uint64(i),
		})
	}
	for _, disk := range sm.disks {
		t.Logf("disk(%v) left space(%v) GB, data partition count(%d)", disk.Path, disk.Available/util.GB, len(disk.partitionMap))
	}
	// check space ratio
	spaceRatio := make([]float64, 0)
	for _, disk := range sm.disks {
		r := float64(disk.Available) / float64(disk.Total)
		spaceRatio = append(spaceRatio, r)
		t.Logf("disk(%v) ratio(%v)", disk.Path, r)
	}
	sort.Slice(spaceRatio, func(i, j int) bool {
		return spaceRatio[i] < spaceRatio[j]
	})
	t.Logf("disk space ratio %v", spaceRatio[len(spaceRatio)-1]-spaceRatio[0])
	require.Less(t, spaceRatio[len(spaceRatio)-1]-spaceRatio[0], 0.2)
	// check partition count
	partitionCnt := make([]int, 0)
	for _, disk := range sm.disks {
		partitionCnt = append(partitionCnt, len(disk.partitionMap))
	}
	sort.Ints(partitionCnt)
	partitionRatio := float64(partitionCnt[len(partitionCnt)-1]-partitionCnt[0]) / float64(partitionCnt[0])
	t.Logf("disk partition ratio %v", partitionRatio)
	require.Less(t, partitionRatio, 1.1)
	// check volume partition count
	r := rand.Int()
	vol := volumes[r%len(volumes)]
	volumePartitionCnt := make([]int, 0)
	for _, disk := range sm.disks {
		volumePartitionCnt = append(volumePartitionCnt, disk.GetVolumePartitionCount(vol))
		t.Logf("disk(%v), volume(%s), partition(%v)", disk.Path, vol, disk.GetVolumePartitionCount(vol))
	}
	sort.Ints(volumePartitionCnt)
	volPartitionRatio := float64(volumePartitionCnt[len(volumePartitionCnt)-1]-volumePartitionCnt[0]) / float64(volumePartitionCnt[0])
	t.Logf("disk volume ratio %v", volPartitionRatio)
	require.Less(t, volPartitionRatio, 1.5)
}
