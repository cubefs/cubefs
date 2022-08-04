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

package proto_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestSchedulerAll(t *testing.T) {
	require.True(t, proto.TaskTypeBlobDelete.Valid())
	require.True(t, proto.TaskTypeDiskRepair.Valid())
	require.False(t, proto.TaskType("").Valid())
	require.False(t, proto.TaskType("nothing-xxx").Valid())
	require.Equal(t, "nothing-xxx", proto.TaskType("nothing-xxx").String())

	var locs []proto.VunitLocation
	require.False(t, proto.CheckVunitLocations(locs))
	locs = append(locs, proto.VunitLocation{})
	require.False(t, proto.CheckVunitLocations(locs))
	locs[0].Vuid = 1
	require.False(t, proto.CheckVunitLocations(locs))
	locs[0].Host = "localhost"
	require.False(t, proto.CheckVunitLocations(locs))
	locs[0].DiskID = 10
	require.True(t, proto.CheckVunitLocations(locs))

	vi := proto.VolumeInspectRet{}
	require.NoError(t, vi.Err())
	vi.InspectErrStr = "has error"
	require.Error(t, vi.Err())

	sr := proto.ShardRepairTask{}
	require.False(t, sr.IsValid())
	sr.CodeMode = 1
	require.False(t, sr.IsValid())
}

func TestSchedulerMigrateTask(t *testing.T) {
	sVuid, _ := proto.NewVuid(111, 1, 1)
	dVuid, _ := proto.NewVuid(222, 2, 2)
	mt := proto.MigrateTask{
		TaskID:   "task_id",
		TaskType: proto.TaskTypeBalance,
		State:    proto.MigrateStateWorkCompleted,

		SourceIDC:    "z0",
		SourceDiskID: 11,
		SourceVuid:   sVuid,
		Sources:      []proto.VunitLocation{},

		Destination: proto.VunitLocation{
			Vuid:   dVuid,
			Host:   "dest_host",
			DiskID: 22,
		},
	}

	require.Equal(t, proto.Vid(111), mt.Vid())
	require.Equal(t, dVuid, mt.GetDestination().Vuid)
	require.Equal(t, proto.DiskID(11), mt.GetSourceDiskID())
	require.Equal(t, proto.DiskID(22), mt.DestinationDiskID())
	require.Empty(t, mt.GetSources())
	require.True(t, mt.Running())
	require.Equal(t, mt, *(mt.Copy()))

	mt.SetDestination(proto.VunitLocation{DiskID: 33})
	require.Equal(t, proto.DiskID(33), mt.DestinationDiskID())
}

func TestSchedulerTaskProgress(t *testing.T) {
	{
		tp := proto.NewTaskProgress()
		require.Equal(t, proto.TaskStatistics{}, tp.Done())

		tp.Total(0, 10)
		require.True(t, tp.Done().Progress >= 100)
		tp.Do(10, 10)
		require.True(t, tp.Done().Progress >= 100)
	}

	tp := proto.NewTaskProgress()
	tp.Total(100, 10)
	require.False(t, tp.Done().Progress >= 100)

	for range [9]struct{}{} {
		tp.Do(11, 1)
		require.False(t, tp.Done().Progress >= 100)
	}
	for range [3]struct{}{} {
		tp.Do(11, 1)
		require.True(t, tp.Done().Progress >= 100)
	}
}
