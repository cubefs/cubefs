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
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/assert"
)

var dr1 = DiskInfoRecord{
	Version:      DiskInfoVersionNormal,
	DiskID:       proto.DiskID(1),
	ClusterID:    proto.ClusterID(1),
	Idc:          "z0",
	Rack:         "rack1",
	Host:         "127.0.0.1",
	Path:         "",
	Status:       proto.DiskStatusNormal,
	Readonly:     false,
	UsedChunkCnt: 0,
	CreateAt:     time.Now(),
	LastUpdateAt: time.Now(),
	Used:         0,
	Size:         100000,
	Free:         100000,
	MaxChunkCnt:  10,
	FreeChunkCnt: 10,
}

var dr2 = DiskInfoRecord{
	Version:      DiskInfoVersionNormal,
	DiskID:       proto.DiskID(2),
	ClusterID:    proto.ClusterID(1),
	Idc:          "z0",
	Rack:         "rack2",
	Host:         "127.0.0.2",
	Path:         "",
	Status:       proto.DiskStatusBroken,
	Readonly:     false,
	UsedChunkCnt: 0,
	CreateAt:     time.Now(),
	LastUpdateAt: time.Now(),
	Used:         0,
	Size:         100000,
	Free:         100000,
	MaxChunkCnt:  10,
	FreeChunkCnt: 10,
}

func TestDiskTbl(t *testing.T) {
	tmpDBPath := "/tmp/tmpdisknormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath, false)
	assert.NoError(t, err)
	defer db.Close()

	diskTbl, err := OpenDiskTable(db, true)
	assert.NoError(t, err)

	// get all disk/ add disk / delete disk
	{
		diskList, err := diskTbl.GetAllDisks()
		assert.NoError(t, err)
		assert.Equal(t, 0, len(diskList))

		err = diskTbl.AddDisk(&dr1)
		assert.NoError(t, err)

		err = diskTbl.AddDisk(&dr2)
		assert.NoError(t, err)

		diskList, err = diskTbl.GetAllDisks()
		assert.NoError(t, err)
		assert.Equal(t, 2, len(diskList))

		err = diskTbl.DeleteDisk(dr2.DiskID)
		assert.NoError(t, err)
		diskList, err = diskTbl.GetAllDisks()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(diskList))
	}

	// get disk and update disk
	{
		diskInfo, err := diskTbl.GetDisk(dr1.DiskID)
		assert.NoError(t, err)
		t.Log(diskInfo.CreateAt.String())
		t.Log(dr1.CreateAt.String())
		assert.EqualValues(t, diskInfo.CreateAt.Unix(), dr1.CreateAt.Unix())

		diskInfo.Readonly = true
		err = diskTbl.UpdateDisk(dr1.DiskID, diskInfo)
		assert.NoError(t, err)
		diskInfo, err = diskTbl.GetDisk(dr1.DiskID)
		assert.NoError(t, err)
		assert.Equal(t, true, diskInfo.Readonly)

		err = diskTbl.UpdateDiskStatus(dr1.DiskID, proto.DiskStatusRepairing)
		assert.NoError(t, err)
		diskInfo, err = diskTbl.GetDisk(dr1.DiskID)
		assert.NoError(t, err)
		assert.Equal(t, proto.DiskStatusRepairing, diskInfo.Status)
	}

	// list disk
	{
		diskList, err := diskTbl.ListDisk(&clustermgr.ListOptionArgs{Host: dr1.Host, Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(diskList))
		assert.Equal(t, dr1.DiskID, diskList[0].DiskID)

		err = diskTbl.AddDisk(&dr2)
		assert.NoError(t, err)

		diskList, err = diskTbl.ListDisk(&clustermgr.ListOptionArgs{Host: dr2.Host, Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(diskList))
		assert.Equal(t, dr2.DiskID, diskList[0].DiskID)

		diskList, err = diskTbl.ListDisk(&clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(diskList))
		assert.Equal(t, dr2.DiskID, diskList[0].DiskID)

		diskList, err = diskTbl.ListDisk(&clustermgr.ListOptionArgs{Status: proto.DiskStatusBroken, Marker: dr2.DiskID, Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(diskList))

		diskList, err = diskTbl.ListDisk(&clustermgr.ListOptionArgs{Marker: dr2.DiskID, Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(diskList))

		diskList, err = diskTbl.ListDisk(&clustermgr.ListOptionArgs{Idc: dr1.Idc, Rack: dr1.Rack, Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(diskList))

		diskList, err = diskTbl.ListDisk(&clustermgr.ListOptionArgs{Count: 10})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(diskList))
	}
}
