// Copyright 2022 The CubeFS Authors.
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

package normaldb

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/assert"
)

func TestDiskDropTbl(t *testing.T) {
	tmpDBPath := "/tmp/tmpdiskdropnormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath, false)
	assert.NoError(t, err)
	defer db.Close()

	diskDropTbl, err := OpenDroppedDiskTable(db)
	assert.NoError(t, err)

	dropList, err := diskDropTbl.GetAllDroppingDisk()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(dropList))

	diskID1 := proto.DiskID(1)
	diskID2 := proto.DiskID(2)

	// add dropping disk and check list result
	{
		err = diskDropTbl.AddDroppingDisk(diskID1)
		assert.NoError(t, err)

		droppingList, err := diskDropTbl.GetAllDroppingDisk()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(droppingList))
		assert.Equal(t, []proto.DiskID{diskID1}, droppingList)

		err = diskDropTbl.AddDroppingDisk(diskID2)
		assert.NoError(t, err)

		droppingList, err = diskDropTbl.GetAllDroppingDisk()
		assert.NoError(t, err)
		assert.Equal(t, []proto.DiskID{diskID1, diskID2}, droppingList)
	}

	// dropping disk
	{
		droppingList, _ := diskDropTbl.GetAllDroppingDisk()
		t.Log("dropping list: ", droppingList)
		exist, err := diskDropTbl.IsDroppingDisk(diskID1)
		assert.NoError(t, err)
		assert.Equal(t, true, exist)

		exist, err = diskDropTbl.IsDroppingDisk(diskID2)
		assert.NoError(t, err)
		assert.Equal(t, true, exist)

		exist, err = diskDropTbl.IsDroppingDisk(proto.InvalidDiskID)
		assert.NoError(t, err)
		assert.Equal(t, false, exist)

		err = diskDropTbl.DroppedDisk(diskID1)
		assert.NoError(t, err)

		exist, err = diskDropTbl.IsDroppingDisk(diskID1)
		assert.NoError(t, err)
		assert.Equal(t, false, exist)

		droppingList, err = diskDropTbl.GetAllDroppingDisk()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(droppingList))
		assert.Equal(t, []proto.DiskID{diskID2}, droppingList)
	}
}
