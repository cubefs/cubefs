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

package diskmon_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/diskmon"
	"github.com/stretchr/testify/require"
)

func TestDiskMon(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	diskMon := diskmon.NewFsMon(tempDir, true, 1*util.GB)
	err = diskMon.ComputeUsage()
	require.NoError(t, err)
	require.NotEqual(t, 0, diskMon.Used)
}

func TestSelectDisk(t *testing.T) {
	const selectCount = 2000
	const diskNum = 4
	disks := make([]diskmon.DiskStat, 0)
	for i := 0; i < diskNum; i++ {
		size := uint64(i+1) * util.TB
		disks = append(disks, diskmon.DiskStat{
			Path:           fmt.Sprintf("/disk/%v", i),
			Total:          size,
			Available:      size,
			PartitionCount: 0,
		})
	}
	randGen := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < selectCount; i++ {
		disk, err := diskmon.SelectDisk(disks, 1)
		require.NoError(t, err)

		for i := 0; i < len(disks); i++ {
			stat := &disks[i]
			if stat.Path == disk.Path {
				stat.PartitionCount += 1
				used := randGen.Uint64() % (10 * util.GB)
				stat.Available -= used
				break
			}
		}
	}

	for _, disk := range disks {
		ratio := float64(disk.Available) / float64(disk.Total)
		t.Logf("Disk %v avaliable %v GB total %v GB ratio %v mp count %v", disk.Path, disk.Available/util.GB, disk.Total/util.GB, ratio, disk.PartitionCount)
	}
}
