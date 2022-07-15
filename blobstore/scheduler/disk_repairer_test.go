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

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func newMockVolInfoMap() map[proto.Vid]*client.VolumeInfoSimple {
	return map[proto.Vid]*client.VolumeInfoSimple{
		1: MockGenVolInfo(1, codemode.EC6P6, proto.VolumeStatusIdle),
		2: MockGenVolInfo(2, codemode.EC6P10L2, proto.VolumeStatusIdle),
		3: MockGenVolInfo(3, codemode.EC6P10L2, proto.VolumeStatusActive),
		4: MockGenVolInfo(4, codemode.EC6P6, proto.VolumeStatusLock),
		5: MockGenVolInfo(5, codemode.EC6P6, proto.VolumeStatusLock),

		6: MockGenVolInfo(6, codemode.EC6P6, proto.VolumeStatusLock),
		7: MockGenVolInfo(7, codemode.EC6P6, proto.VolumeStatusLock),
	}
}

func newDiskRepairer(t *testing.T) *DiskRepairMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	taskLogger := mocks.NewMockRecordLogEncoder(ctr)
	conf := &MigrateConfig{
		TaskCommonConfig: base.TaskCommonConfig{
			CollectTaskIntervalS: 1,
			CheckTaskIntervalS:   1,
		},
	}
	return NewDiskRepairMgr(clusterMgr, taskSwitch, taskLogger, conf)
}

func TestDiskRepairerLoad(t *testing.T) {
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return(nil, nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasks(any, any).Return([]*proto.MigrateTask{{}, {}}, nil)
		err := mgr.Load()
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{}, {}}, nil)
		err := mgr.Load()
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		err := mgr.Load()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskRepairer(t) // vid proto.Vid, state proto.MigrateState, diskID proto.DiskID, volInfoMap map[proto.Vid]*client.VolumeInfoSimple
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateFinishedInAdvance, newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1}, nil)
		err := mgr.Load()
		require.Error(t, err)

		t2 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateFinished, newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t2}, nil)
		err = mgr.Load()
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateInited, newMockVolInfoMap())
		t2 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 2, proto.MigrateStatePrepared, newMockVolInfoMap())
		t3 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 4, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1, t2, t3}, nil)
		err := mgr.Load()
		require.NoError(t, err)
	}
	{
		// same volume task prepared
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		t2 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1, t2}, nil)
		err := mgr.Load()
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateState(111), newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: &client.DiskInfoSimple{DiskID: proto.DiskID(1)}}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1}, nil)
		err := mgr.Load()
		require.Error(t, err)
	}
}

func TestDiskRepairerRun(t *testing.T) {
	mgr := newDiskRepairer(t)
	defer mgr.Close()

	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().AnyTimes().Return(true)

	mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).AnyTimes().Return(nil, errMock)
	require.True(t, mgr.Enabled())
	mgr.hasRevised = true
	mgr.repairingDiskID = proto.DiskID(1)

	mgr.Run()
	time.Sleep(1 * time.Second)
}

func TestDiskRepairerCollectTask(t *testing.T) {
	{
		mgr := newDiskRepairer(t)
		mgr.hasRevised = false
		mgr.repairingDiskID = proto.DiskID(1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.collectTask()
	}
	{
		mgr := newDiskRepairer(t)
		mgr.hasRevised = false
		mgr.repairingDiskID = proto.DiskID(1)
		// genDiskRepairTasks failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(
			&client.DiskInfoSimple{DiskID: mgr.repairingDiskID, Status: proto.DiskStatusBroken}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		mgr.collectTask()

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(
			&client.DiskInfoSimple{DiskID: mgr.repairingDiskID, Status: proto.DiskStatusBroken}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.collectTask()

		// gen task success
		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigrateTask(any, any).AnyTimes().Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(
			&client.DiskInfoSimple{DiskID: mgr.repairingDiskID, Status: proto.DiskStatusBroken}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskRepairing(any, any).Return(nil)
		mgr.collectTask()
		todo, doing := mgr.prepareQueue.StatsTasks()
		require.Equal(t, 12, todo+doing)
		require.Equal(t, true, mgr.hasRevised)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.hasRevised = true
		mgr.repairingDiskID = proto.DiskID(0)

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, errMock)
		mgr.collectTask()
		require.False(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{}, {}}, nil)
		require.Panics(t, func() {
			mgr.collectTask()
		})

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenDisks(any, any).Return(nil, errMock)
		mgr.collectTask()
		require.False(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenDisks(any, any).Return(nil, nil)
		mgr.collectTask()
		require.False(t, mgr.hasRepairingDisk())

		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusBroken,
			DiskID:       proto.DiskID(1),
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenDisks(any, any).Return([]*client.DiskInfoSimple{disk1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		mgr.collectTask()
		require.False(t, mgr.hasRepairingDisk())
	}
	{
		mgr := newDiskRepairer(t)
		mgr.hasRevised = true
		mgr.repairingDiskID = base.EmptyDiskID

		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusBroken,
			DiskID:       proto.DiskID(1),
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenDisks(any, any).Return([]*client.DiskInfoSimple{disk1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(errMock)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigrateTask(any, any).AnyTimes().Return(nil)

		// collect failed
		mgr.collectTask()
		require.False(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenDisks(any, any).Return([]*client.DiskInfoSimple{disk1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskRepairing(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(nil)

		mgr.collectTask()
		todo, doing := mgr.prepareQueue.StatsTasks()
		require.True(t, mgr.hasRepairingDisk())
		require.Equal(t, disk1.DiskID, mgr.repairingDiskID)
		require.Equal(t, 12, todo+doing)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.hasRevised = true
		mgr.repairingDiskID = proto.DiskID(0)

		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusBroken,
			DiskID:       proto.DiskID(1),
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		t1 := &proto.MigrateTask{
			TaskID:     client.GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), volume.Vid),
			TaskType:   proto.TaskTypeDiskRepair,
			SourceVuid: units[0].Vuid,
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return([]*client.MigratingDiskMeta{{Disk: disk1}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskRepairing(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigrateTask(any, any).AnyTimes().Return(nil)
		mgr.collectTask()
		todo, doing := mgr.prepareQueue.StatsTasks()
		require.True(t, mgr.hasRepairingDisk())
		require.Equal(t, disk1.DiskID, mgr.repairingDiskID)
		require.Equal(t, 11, todo+doing)
	}
}

func TestDiskRepairerPopTaskAndPrepare(t *testing.T) {
	{
		mgr := newDiskRepairer(t)
		err := mgr.popTaskAndPrepare()
		require.True(t, errors.Is(err, base.ErrNoTaskInQueue))
	}
	{
		mgr := newDiskRepairer(t)
		mgr.hasRevised = true
		mgr.repairingDiskID = proto.DiskID(0)

		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusBroken,
			DiskID:       proto.DiskID(1),
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListMigratingDisks(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListBrokenDisks(any, any).Return([]*client.DiskInfoSimple{disk1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskRepairing(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigrateTask(any, any).AnyTimes().Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AddMigratingDisk(any, any).Return(nil)
		mgr.collectTask()
		todo, doing := mgr.prepareQueue.StatsTasks()
		require.Equal(t, disk1.DiskID, mgr.repairingDiskID)
		require.Equal(t, 12, todo+doing)

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)
		err := mgr.popTaskAndPrepare()
		require.True(t, errors.Is(err, errMock))

		// finish in advance
		volume.VunitLocations[0].Vuid = volume.VunitLocations[0].Vuid + 1
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.taskLogger.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Return(nil)
		err = mgr.popTaskAndPrepare()
		todo, doing = mgr.prepareQueue.StatsTasks()

		require.NoError(t, err)
		require.Equal(t, 11, todo+doing)

		// alloc volume unit failed
		volume.VunitLocations[0].Vuid = volume.VunitLocations[0].Vuid - 1
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).Return(nil, errMock)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		err = mgr.popTaskAndPrepare()
		require.True(t, errors.Is(err, errMock))

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).DoAndReturn(func(ctx context.Context, vuid proto.Vuid) (*client.AllocVunitInfo, error) {
			vid := vuid.Vid()
			idx := vuid.Index()
			epoch := vuid.Epoch()
			epoch++
			newVuid, _ := proto.NewVuid(vid, idx, epoch)
			return &client.AllocVunitInfo{
				VunitLocation: proto.VunitLocation{Vuid: newVuid},
			}, nil
		})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		err = mgr.popTaskAndPrepare()
		require.NoError(t, err)

		todo, doing = mgr.prepareQueue.StatsTasks()
		require.Equal(t, 10, todo+doing)
		todo, doing = mgr.workQueue.StatsTasks()
		require.Equal(t, 1, todo+doing)
	}
}

func TestDiskRepairerPopTaskAndFinish(t *testing.T) {
	{
		mgr := newDiskRepairer(t)
		err := mgr.popTaskAndFinish()
		require.True(t, errors.Is(err, base.ErrNoTaskInQueue))
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateFinished, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		require.Panics(t, func() {
			mgr.popTaskAndFinish()
		})
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errMock)
		err := mgr.popTaskAndFinish()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrOldVuidNotMatch)
		require.Panics(t, func() {
			mgr.popTaskAndFinish()
		})
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrNewVuidNotMatch)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).Return(nil, errMock)
		err := mgr.popTaskAndFinish()
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Times(2).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrNewVuidNotMatch)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).DoAndReturn(func(ctx context.Context, vuid proto.Vuid) (*client.AllocVunitInfo, error) {
			vid := vuid.Vid()
			idx := vuid.Index()
			epoch := vuid.Epoch()
			epoch++
			newVuid, _ := proto.NewVuid(vid, idx, epoch)
			return &client.AllocVunitInfo{
				VunitLocation: proto.VunitLocation{Vuid: newVuid},
			}, nil
		})
		err := mgr.popTaskAndFinish()
		require.NoError(t, err)
		todo, doing := mgr.finishQueue.StatsTasks()
		require.Equal(t, 0, todo+doing)
		todo, doing = mgr.workQueue.StatsTasks()
		require.Equal(t, 1, todo+doing)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Times(2).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(errcode.ErrStatChunkFailed)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().AllocVolumeUnit(any, any).DoAndReturn(func(ctx context.Context, vuid proto.Vuid) (*client.AllocVunitInfo, error) {
			vid := vuid.Vid()
			idx := vuid.Index()
			epoch := vuid.Epoch()
			epoch++
			newVuid, _ := proto.NewVuid(vid, idx, epoch)
			return &client.AllocVunitInfo{
				VunitLocation: proto.VunitLocation{Vuid: newVuid},
			}, nil
		})
		err := mgr.popTaskAndFinish()
		require.NoError(t, err)
		todo, doing := mgr.finishQueue.StatsTasks()
		require.Equal(t, 0, todo+doing)
		todo, doing = mgr.workQueue.StatsTasks()
		require.Equal(t, 1, todo+doing)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStateWorkCompleted, newMockVolInfoMap())
		mgr.finishQueue.PushTask(t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateVolume(any, any, any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.taskLogger.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Return(nil)
		err := mgr.popTaskAndFinish()
		require.NoError(t, err)
		todo, doing := mgr.finishQueue.StatsTasks()
		require.Equal(t, 0, todo+doing)
	}
}

func TestDiskRepairerCheckRepairedAndClear(t *testing.T) {
	{
		mgr := newDiskRepairer(t)
		mgr.checkRepairedAndClear()
	}
	{
		mgr := newDiskRepairer(t)
		mgr.repairingDiskID = proto.DiskID(1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.hasRepairingDisk())
	}
	{
		mgr := newDiskRepairer(t)
		mgr.repairingDiskID = proto.DiskID(1)
		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		t1 := &proto.MigrateTask{
			TaskID:     client.GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), volume.Vid),
			TaskType:   proto.TaskTypeDiskRepair,
			SourceVuid: units[0].Vuid,
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, nil)
		require.Panics(t, func() {
			mgr.checkRepairedAndClear()
		})

		t1.State = proto.MigrateStateFinished
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskRepaired(any, any).Return(errMock)
		mgr.checkRepairedAndClear()
		require.True(t, mgr.hasRepairingDisk())

		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().SetDiskRepaired(any, any).Return(nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigratingDisk(any, any, any).Return(nil)
		mgr.checkRepairedAndClear()
		require.False(t, mgr.hasRepairingDisk())
	}
}

func TestDiskRepairerAcquireTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newDiskRepairer(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		_, err := mgr.AcquireTask(ctx, idc)
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		mgr := newDiskRepairer(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		_, err := mgr.AcquireTask(ctx, idc)
		require.True(t, errors.Is(err, proto.ErrTaskEmpty))
	}
	{
		mgr := newDiskRepairer(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		_, err := mgr.AcquireTask(ctx, idc)
		require.NoError(t, err)
	}
}

func TestDiskRepairerCancelTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newDiskRepairer(t)
		err := mgr.CancelTask(ctx, &api.CancelTaskArgs{})
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)

		err := mgr.CancelTask(ctx, &api.CancelTaskArgs{})
		require.Error(t, err)
	}
}

func TestDiskRepairerReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(errMock)
		err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
		require.NoError(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().UpdateMigrateTask(any, any).Return(nil)
		err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
		require.NoError(t, err)
	}
}

func TestDiskRepairerCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		err := mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		err := mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
		require.NoError(t, err)
		todo, doing := mgr.finishQueue.StatsTasks()
		require.Equal(t, 1, todo+doing)
		todo, doing = mgr.workQueue.StatsTasks()
		require.Equal(t, 0, todo+doing)
	}
}

func TestDiskRepairerRenewalTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	{
		mgr := newDiskRepairer(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		err := mgr.RenewalTask(ctx, idc, "")
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		mgr := newDiskRepairer(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		err := mgr.RenewalTask(ctx, idc, "")
		require.Error(t, err)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.workQueue.AddPreparedTask(idc, t1.TaskID, t1)
		err := mgr.RenewalTask(ctx, idc, t1.TaskID)
		require.NoError(t, err)
	}
}

func TestDiskRepairerStats(t *testing.T) {
	mgr := newDiskRepairer(t)
	mgr.Stats()
}

func TestDiskRepairerStatQueueTaskCnt(t *testing.T) {
	mgr := newDiskRepairer(t)
	inited, prepared, completed := mgr.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)
}

func TestDiskRepairerQueryTask(t *testing.T) {
	ctx := context.Background()
	taskID := "task"
	{
		mgr := newDiskRepairer(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any).Return(nil, errMock)
		_, err := mgr.QueryTask(ctx, taskID)
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newDiskRepairer(t)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any).Return(t1, nil)
		_, err := mgr.QueryTask(ctx, taskID)
		require.NoError(t, err)
	}
}

func TestDiskRepairerReportWorkerTaskStats(t *testing.T) {
	mgr := newDiskRepairer(t)
	mgr.ReportWorkerTaskStats(&api.TaskReportArgs{
		TaskId:               "task",
		IncreaseDataSizeByte: 1,
		IncreaseShardCnt:     1,
	})
}

func TestDiskRepairerProgress(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newDiskRepairer(t)
		diskID, _, _ := mgr.Progress(ctx)
		require.Equal(t, base.EmptyDiskID, diskID)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.repairingDiskID = proto.DiskID(1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigratingDisk(any, any, any).Return(nil, errMock)
		diskID, _, _ := mgr.Progress(ctx)
		require.Equal(t, proto.DiskID(1), diskID)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.repairingDiskID = proto.DiskID(1)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigratingDisk(any, any, any).Return(&client.MigratingDiskMeta{Disk: &client.DiskInfoSimple{DiskID: mgr.repairingDiskID}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return(nil, errMock)
		diskID, _, _ := mgr.Progress(ctx)
		require.Equal(t, proto.DiskID(1), diskID)
	}
	{
		mgr := newDiskRepairer(t)
		mgr.repairingDiskID = proto.DiskID(1)
		t1 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 1, proto.MigrateStatePrepared, newMockVolInfoMap())
		t2 := mockGenMigrateTask(proto.TaskTypeDiskRepair, "z0", 1, 2, proto.MigrateStateInited, newMockVolInfoMap())
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigratingDisk(any, any, any).Return(&client.MigratingDiskMeta{Disk: &client.DiskInfoSimple{DiskID: mgr.repairingDiskID, UsedChunkCnt: 4}}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListAllMigrateTasksByDiskID(any, any, any).Return([]*proto.MigrateTask{t1, t2}, nil)
		diskID, total, repaired := mgr.Progress(ctx)
		require.Equal(t, proto.DiskID(1), diskID)
		require.Equal(t, 4, total)
		require.Equal(t, 2, repaired)
	}
}
