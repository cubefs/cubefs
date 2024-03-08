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
	"time"

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

func TestSelectDisk(t *testing.T) {
	sm := &SpaceManager{
		disks: make(map[string]*Disk),
		rand:  rand.New(rand.NewSource(time.Now().Unix())),
	}
	// prepare disks
	prepareDisksForSelectDiskTest(t, sm, 4)
	const testCount = 1500
	selectTimes := make(map[string]int)
	for _, disk := range sm.disks {
		selectTimes[disk.Path] = 0
	}
	decommsionDisk := []string{}
	for i := 0; i < testCount; i++ {
		disk := sm.selectDisk(decommsionDisk)
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
	require.Less(t, ratio[len(ratio)-1]-ratio[0], 0.1)
}

func TestSelectDiskForSmallDp(t *testing.T) {
	sm := &SpaceManager{
		disks: make(map[string]*Disk),
		rand:  rand.New(rand.NewSource(time.Now().Unix())),
	}
	// prepare disks
	prepareDisksForSelectDiskTest(t, sm, 4)
	const testCount = 1500
	selectTimes := make(map[string]int)
	for _, disk := range sm.disks {
		selectTimes[disk.Path] = 0
	}
	decommsionDisk := []string{}
	for i := 0; i < testCount; i++ {
		disk := sm.selectDisk(decommsionDisk)
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
