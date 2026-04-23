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

package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func TestValidMigrateTask(t *testing.T) {
	taskID := GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1)
	require.True(t, ValidMigrateTask(proto.TaskTypeDiskRepair, taskID))
	require.False(t, ValidMigrateTask(proto.TaskTypeBalance, taskID))
}

func TestDiskInfoSimpleIsRepaired(t *testing.T) {
	disk := &DiskInfoSimple{Status: proto.DiskStatusRepaired}
	require.True(t, disk.IsRepaired())
	disk.Status = proto.DiskStatusNormal
	require.False(t, disk.IsRepaired())
}

func TestShardNodeDiskInfoMethods(t *testing.T) {
	disk := &ShardNodeDiskInfo{}

	disk.Status = proto.DiskStatusNormal
	require.True(t, disk.IsHealth())
	require.False(t, disk.IsBroken())
	require.False(t, disk.IsDropped())
	require.False(t, disk.IsRepaired())
	require.True(t, disk.CanDropped())

	disk.Status = proto.DiskStatusBroken
	require.False(t, disk.IsHealth())
	require.True(t, disk.IsBroken())
	require.False(t, disk.CanDropped())

	disk.Status = proto.DiskStatusDropped
	require.True(t, disk.IsDropped())
	require.True(t, disk.CanDropped())

	disk.Status = proto.DiskStatusRepaired
	require.True(t, disk.IsRepaired())
	require.True(t, disk.CanDropped())

	disk.Status = proto.DiskStatusRepairing
	require.False(t, disk.CanDropped())
}

var (
	defaultVolumeListMarker = proto.Vid(0)
	defaultDiskListMarker   = proto.DiskID(0)
)

func MockGenVolInfo(vid proto.Vid, mode codemode.CodeMode, status proto.VolumeStatus) *cmapi.VolumeInfo {
	cmInfo := mode.Tactic()
	vunitCnt := cmInfo.M + cmInfo.N + cmInfo.L
	host := "127.0.0.0:xxx"
	locations := make([]cmapi.Unit, vunitCnt)
	var idx uint8
	for i := 0; i < vunitCnt; i++ {
		locations[i].Vuid, _ = proto.NewVuid(vid, idx, 1)
		locations[i].Host = host
		locations[i].DiskID = proto.DiskID(locations[i].Vuid)
		idx++
	}

	return &cmapi.VolumeInfo{
		Units: locations,
		VolumeInfoBase: cmapi.VolumeInfoBase{
			Vid:      vid,
			CodeMode: mode,
			Status:   status,
		},
	}
}

func TestClustermgrClient(t *testing.T) {
	cli := NewClusterMgrClient(&cmapi.Config{}).(*clustermgrClient)
	mockCli := NewMockClusterManager(gomock.NewController(t))
	cli.client = mockCli

	ctx := context.Background()
	any := gomock.Any()
	errMock := errors.New("fake error")
	{
		// get config
		cli.client.(*MockClusterManager).EXPECT().GetConfig(any, any).Return("", errMock)
		_, err := cli.GetConfig(ctx, "config")
		require.True(t, errors.Is(err, errMock))

		cli.client.(*MockClusterManager).EXPECT().GetConfig(any, any).Return(taskswitch.SwitchOpen, nil)
		enable, err := cli.GetConfig(ctx, "config")
		require.NoError(t, err)
		require.Equal(t, "true", enable)
	}
	{
		// get volume info
		cli.client.(*MockClusterManager).EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)
		_, err := cli.GetVolumeInfo(ctx, 1)
		require.True(t, errors.Is(err, errMock))

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		volume2 := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusActive)
		cli.client.(*MockClusterManager).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		cli.client.(*MockClusterManager).EXPECT().GetVolumeInfo(any, any).Return(volume2, nil)
		vol, err := cli.GetVolumeInfo(ctx, 1)
		require.NoError(t, err)
		vol2, err := cli.GetVolumeInfo(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, vol.Vid, volume.Vid)
		require.Equal(t, vol.CodeMode, volume.CodeMode)
		require.Equal(t, vol.Status, volume.Status)
		require.Equal(t, len(vol.VunitLocations), len(volume.Units))
		require.True(t, vol.IsIdle())
		require.True(t, vol2.IsActive())
		require.True(t, vol.EqualWith(vol))
		require.False(t, vol.EqualWith(vol2))
	}
	{
		// lock volume
		cli.client.(*MockClusterManager).EXPECT().LockVolume(any, any).Return(nil)
		err := cli.LockVolume(ctx, 1, 0)
		require.NoError(t, err)
	}
	{
		// unlock volume
		cli.client.(*MockClusterManager).EXPECT().UnlockVolume(any, any).Return(nil)
		err := cli.UnlockVolume(ctx, 1, 0)
		require.NoError(t, err)

		cli.client.(*MockClusterManager).EXPECT().UnlockVolume(any, any).Return(errcode.ErrUnlockNotAllow)
		err = cli.UnlockVolume(ctx, 1, 0)
		require.ErrorIs(t, err, errcode.ErrUnlockNotAllow)

		cli.client.(*MockClusterManager).EXPECT().UnlockVolume(any, any).Return(errMock)
		err = cli.UnlockVolume(ctx, 1, 0)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// update volume
		cli.client.(*MockClusterManager).EXPECT().UpdateVolume(any, any).Return(nil)
		err := cli.UpdateVolume(ctx, proto.Vuid(2), proto.Vuid(1), proto.DiskID(1))
		require.NoError(t, err)
	}
	{
		// update volume
		cli.client.(*MockClusterManager).EXPECT().AllocVolumeUnit(any, any).Return(nil, errMock)
		_, err := cli.AllocVolumeUnit(ctx, proto.Vuid(2), nil, false)
		require.True(t, errors.Is(err, errMock))

		unit := &cmapi.AllocVolumeUnit{Vuid: proto.Vuid(3), DiskID: proto.DiskID(2)}
		cli.client.(*MockClusterManager).EXPECT().AllocVolumeUnit(any, any).Return(unit, nil)
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(nil, errMock)
		_, err = cli.AllocVolumeUnit(ctx, proto.Vuid(2), nil, false)
		require.True(t, errors.Is(err, errMock))

		cli.client.(*MockClusterManager).EXPECT().AllocVolumeUnit(any, any).Return(unit, nil)
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(&cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx"}}, nil)
		allocUnit, err := cli.AllocVolumeUnit(ctx, proto.Vuid(2), nil, false)
		require.NoError(t, err)
		require.Equal(t, unit.Vuid, allocUnit.Location().Vuid)
	}
	{
		// release volume unit
		cli.client.(*MockClusterManager).EXPECT().ReleaseVolumeUnit(any, any).Return(nil)
		err := cli.ReleaseVolumeUnit(ctx, proto.Vuid(2), proto.DiskID(1))
		require.NoError(t, err)
	}
	{
		// list disk volume units
		cli.client.(*MockClusterManager).EXPECT().ListVolumeUnit(any, any).Return(nil, errMock)
		_, err := cli.ListDiskVolumeUnits(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errMock))

		unit := &cmapi.VolumeUnitInfo{Vuid: proto.Vuid(3), DiskID: proto.DiskID(2)}
		cli.client.(*MockClusterManager).EXPECT().ListVolumeUnit(any, any).Return([]*cmapi.VolumeUnitInfo{unit}, nil)
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(nil, errMock)
		_, err = cli.ListDiskVolumeUnits(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errMock))

		cli.client.(*MockClusterManager).EXPECT().ListVolumeUnit(any, any).Return([]*cmapi.VolumeUnitInfo{unit}, nil)
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(&cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx"}}, nil)
		units, err := cli.ListDiskVolumeUnits(ctx, proto.DiskID(1))
		require.NoError(t, err)
		require.Equal(t, 1, len(units))
	}
	{
		// list volume
		cli.client.(*MockClusterManager).EXPECT().ListVolume(any, any).Return(cmapi.ListVolumes{}, errMock)
		_, _, err := cli.ListVolume(ctx, defaultVolumeListMarker, 10)
		require.True(t, errors.Is(err, errMock))

		cli.client.(*MockClusterManager).EXPECT().ListVolume(any, any).Return(cmapi.ListVolumes{}, nil)
		rets, _, err := cli.ListVolume(ctx, defaultVolumeListMarker, 10)
		require.NoError(t, err)
		require.Equal(t, 0, len(rets))

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		cli.client.(*MockClusterManager).EXPECT().ListVolume(any, any).Return(cmapi.ListVolumes{Volumes: []*cmapi.VolumeInfo{volume}, Marker: defaultVolumeListMarker}, nil)
		rets, marker, err := cli.ListVolume(ctx, defaultVolumeListMarker, 10)
		require.NoError(t, err)
		require.Equal(t, 1, len(rets))
		require.Equal(t, marker, defaultVolumeListMarker)
	}
	{
		// list cluster disk
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{}, errMock)
		_, err := cli.ListClusterDisks(ctx)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// list broken disk
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{}, errMock)
		_, err := cli.ListBrokenDisks(ctx)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// list repair disk
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{}, errMock)
		_, err := cli.ListRepairingDisks(ctx)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// list all disk
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{}, errMock)
		_, err := cli.listAllDisks(ctx, proto.DiskStatusNormal)
		require.True(t, errors.Is(err, errMock))

		disk1 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}}
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{Disks: []*cmapi.BlobNodeDiskInfo{disk1}, Marker: defaultDiskListMarker}, nil)
		disks, err := cli.listAllDisks(ctx, proto.DiskStatusNormal)
		require.NoError(t, err)
		require.Equal(t, 1, len(disks))
	}
	{
		// list disks
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{}, errMock)
		_, err := cli.listDisks(ctx, proto.DiskStatusNormal, 1)
		require.True(t, errors.Is(err, errMock))

		disk1 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}}
		disk2 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}}
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{Disks: []*cmapi.BlobNodeDiskInfo{disk1}, Marker: proto.DiskID(2)}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{Disks: []*cmapi.BlobNodeDiskInfo{disk2}, Marker: defaultDiskListMarker}, nil)
		disks, err := cli.listDisks(ctx, proto.DiskStatusNormal, 2)
		require.NoError(t, err)
		require.Equal(t, 2, len(disks))
	}
	{
		// list drop disk
		cli.client.(*MockClusterManager).EXPECT().ListDroppingDisk(any).Return(nil, errMock)
		_, err := cli.ListDropDisks(ctx)
		require.True(t, errors.Is(err, errMock))

		disk1 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}}
		cli.client.(*MockClusterManager).EXPECT().ListDroppingDisk(any).Return([]*cmapi.BlobNodeDiskInfo{disk1}, nil)
		disks, err := cli.ListDropDisks(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(disks))
	}
	{
		// set disk repair
		cli.client.(*MockClusterManager).EXPECT().SetDisk(any, any, any).Return(nil)
		err := cli.SetDiskRepairing(ctx, proto.DiskID(1))
		require.NoError(t, err)
	}
	{
		// set disk repaired
		cli.client.(*MockClusterManager).EXPECT().SetDisk(any, any, any).Return(nil)
		err := cli.SetDiskRepaired(ctx, proto.DiskID(1))
		require.NoError(t, err)
	}
	{
		// set disk repaired
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(nil, errMock)
		err := cli.SetDiskDropped(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errMock))

		disk1 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusDropped}}
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(disk1, nil)
		err = cli.SetDiskDropped(ctx, proto.DiskID(1))
		require.NoError(t, err)

		disk2 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusRepairing}}
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(disk2, nil)
		err = cli.SetDiskDropped(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errcode.ErrCanNotDropped))

		disk3 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}}
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(disk3, nil)
		cli.client.(*MockClusterManager).EXPECT().DroppedDisk(any, any).Return(nil)
		err = cli.SetDiskDropped(ctx, proto.DiskID(1))
		require.NoError(t, err)
	}
	{
		// get disk info
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(nil, errMock)
		_, err := cli.GetDiskInfo(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errMock))

		disk1 := &cmapi.BlobNodeDiskInfo{DiskInfo: cmapi.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusDropped}}
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(disk1, nil)
		disk, err := cli.GetDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		require.Equal(t, disk.Status, disk1.Status)
		require.False(t, disk.IsBroken())
	}
	{
		// register service
		cli.client.(*MockClusterManager).EXPECT().RegisterService(any, any, any, any, any).Return(nil)
		err := cli.Register(ctx, RegisterInfo{})
		require.NoError(t, err)
	}
	{
		// get service
		cli.client.(*MockClusterManager).EXPECT().GetService(any, any).Return(cmapi.ServiceInfo{}, nil)
		_, err := cli.GetService(ctx, "mock", proto.ClusterID(1))
		require.NoError(t, err)
	}
	{
		// add migrate task
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		task, _ := (&proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1)}).ToTask()
		err := cli.AddMigrateTask(ctx, task)
		require.NoError(t, err)
	}
	{
		// update migrate task
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		task, _ := (&proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1)}).ToTask()
		err := cli.UpdateMigrateTask(ctx, task)
		require.NoError(t, err)
	}
	{
		// get migrate task
		task1, _ := (&proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1)}).ToTask()
		taskBytes, _ := task1.Marshal()
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: taskBytes}, nil)
		err := cli.AddMigrateTask(ctx, task1)
		require.NoError(t, err)
		task2, err := cli.GetMigrateTask(ctx, task1.TaskType, task1.TaskID)
		require.NoError(t, err)
		require.Equal(t, task1.TaskID, task2.TaskID)

		// clustermgr return err
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{}, errMock)
		_, err = cli.GetMigrateTask(ctx, task1.TaskType, task1.TaskID)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// delete migrate task
		task1 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1)}
		cli.client.(*MockClusterManager).EXPECT().DeleteKV(any, any).Return(nil)
		err := cli.DeleteMigrateTask(ctx, task1.TaskID)
		require.NoError(t, err)
	}
	{ // kv over defaultListTaskNum
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Marker: "has"}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).DoAndReturn(
			func(_ context.Context, args *cmapi.ListKvOpts) (ret cmapi.ListKvRet, err error) {
				if args.Marker != "has" {
					return cmapi.ListKvRet{}, errMock
				}
				return cmapi.ListKvRet{Marker: ""}, nil
			})
		_, err := cli.ListAllMigrateTasks(ctx, proto.TaskTypeDiskRepair)
		require.NoError(t, err)
	}
	{
		// list all migrate tasks by disk_id
		diskID := proto.DiskID(100)
		task1 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeBalance, diskID, 1), TaskType: proto.TaskTypeBalance}
		task1Bytes, _ := json.Marshal(task1)
		task2 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeBalance, diskID, 1), TaskType: proto.TaskTypeBalance}
		task2Bytes, _ := json.Marshal(task2)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{{Key: task1.TaskID, Value: task1Bytes}}, Marker: task1.TaskID}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{{Key: task2.TaskID, Value: task2Bytes}}, Marker: task2.TaskID}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{}, Marker: defaultListTaskMarker}, nil)
		tasks, err := cli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeBalance, diskID)
		require.NoError(t, err)
		require.Equal(t, 2, len(tasks))
		require.Equal(t, task1.TaskID, tasks[0].TaskID)
		require.Equal(t, task2.TaskID, tasks[1].TaskID)

		// unmarshal failed
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{{Key: task1.TaskID, Value: append(task1Bytes, []byte("mock")...)}}, Marker: task1.TaskID}, nil)
		_, err = cli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeBalance, diskID)
		require.Error(t, err)

		// clustermgr return err
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{}, errMock)
		_, err = cli.ListAllMigrateTasksByDiskID(ctx, proto.TaskTypeBalance, diskID)
		require.True(t, errors.Is(err, errMock))

		// list all migrate task
		task3 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeBalance, proto.DiskID(200), 1), TaskType: proto.TaskTypeBalance}
		task3Bytes, _ := json.Marshal(task3)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{
			{Key: task1.TaskID, Value: task1Bytes},
			{Key: task2.TaskID, Value: task2Bytes},
			{Key: task3.TaskID, Value: task3Bytes},
		}, Marker: task3.TaskID}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{}, Marker: defaultListTaskMarker}, nil)
		tasks, err = cli.ListAllMigrateTasks(ctx, proto.TaskTypeBalance)
		require.NoError(t, err)
		require.Equal(t, 3, len(tasks))
		require.Equal(t, task1.TaskID, tasks[0].TaskID)
		require.Equal(t, task2.TaskID, tasks[1].TaskID)
		require.Equal(t, task3.TaskID, tasks[2].TaskID)
	}
	{
		// add migrating disk meta
		diskMeta1 := &MigratingDiskMeta{Disk: &DiskInfoSimple{DiskID: proto.DiskID(1)}, TaskType: proto.TaskTypeDiskDrop}
		metaBytes, _ := json.Marshal(diskMeta1)
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		err := cli.AddMigratingDisk(ctx, diskMeta1)
		require.NoError(t, err)

		// get migrating disk meta
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: metaBytes}, nil)
		diskMeta2, err := cli.GetMigratingDisk(ctx, diskMeta1.TaskType, diskMeta1.Disk.DiskID)
		require.NoError(t, err)
		require.Equal(t, diskMeta1.ID(), diskMeta2.ID())

		// delete migrating task
		cli.client.(*MockClusterManager).EXPECT().DeleteKV(any, any).Return(nil)
		err = cli.DeleteMigratingDisk(ctx, diskMeta1.TaskType, diskMeta1.Disk.DiskID)
		require.NoError(t, err)
	}
	{
		// list migrating disk
		diskMeta1 := &MigratingDiskMeta{Disk: &DiskInfoSimple{DiskID: proto.DiskID(1)}, TaskType: proto.TaskTypeDiskDrop}
		diskMeta1Bytes, _ := json.Marshal(diskMeta1)
		diskMeta2 := &MigratingDiskMeta{Disk: &DiskInfoSimple{DiskID: proto.DiskID(1)}, TaskType: proto.TaskTypeDiskDrop}
		diskMeta2Bytes, _ := json.Marshal(diskMeta2)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{
			{Key: diskMeta1.ID(), Value: diskMeta1Bytes}, {Key: diskMeta2.ID(), Value: diskMeta2Bytes},
		}, Marker: diskMeta2.ID()}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{}, Marker: defaultListTaskMarker}, nil)
		tasks, err := cli.ListMigratingDisks(ctx, proto.TaskTypeDiskDrop)
		require.NoError(t, err)
		require.Equal(t, 2, len(tasks))
		require.Equal(t, diskMeta1.ID(), tasks[0].ID())
		require.Equal(t, diskMeta2.ID(), tasks[1].ID())

		// unmarshal failed
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{Kvs: []*cmapi.KeyValue{
			{Key: diskMeta1.ID(), Value: diskMeta1Bytes}, {Key: diskMeta2.ID(), Value: append(diskMeta2Bytes, []byte("mock")...)},
		}, Marker: diskMeta2.ID()}, nil)
		_, err = cli.ListMigratingDisks(ctx, proto.TaskTypeDiskDrop)
		require.Error(t, err)

		// clustermgr list failed
		cli.client.(*MockClusterManager).EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{}, errMock)
		_, err = cli.ListMigratingDisks(ctx, proto.TaskTypeDiskDrop)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// set volume inspect checkpoint
		startVid := proto.Vid(100)
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		err := cli.SetVolumeInspectCheckPoint(ctx, startVid)
		require.NoError(t, err)

		// get volume inspect checkpoint
		checkpoint := &proto.VolumeInspectCheckPoint{
			StartVid: startVid,
			Ctime:    "",
		}
		checkpointBytes, _ := json.Marshal(checkpoint)
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: checkpointBytes}, nil)
		checkpoint2, err := cli.GetVolumeInspectCheckPoint(ctx)
		require.NoError(t, err)
		require.Equal(t, checkpoint.StartVid, checkpoint2.StartVid)
	}
	{
		// set consume offset
		topic := "test"
		partition := int32(100)
		offset := int64(124548412)
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		err := cli.SetConsumeOffset(proto.TaskTypeShardRepair, topic, partition, offset)
		require.NoError(t, err)

		// get consume offset
		consumeOffset := &ConsumeOffset{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		}
		consumeOffsetBytes, _ := json.Marshal(consumeOffset)
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: consumeOffsetBytes}, nil)
		offset2, err := cli.GetConsumeOffset(proto.TaskTypeShardRepair, topic, partition)
		require.NoError(t, err)
		require.Equal(t, offset, offset2)
	}
	{
		stats := &proto.VolumeDegradeStats{
			BatchSize: 10000,
			BlobStats: []proto.VolumeDegradeBatch{
				{
					BatchIndex: 1,
					CodeModeStats: []proto.VolumeDegradeCodeMode{
						{
							Mode: codemode.EC3P3,
							DegradeStats: []proto.VolumeDegradeLevel{
								{
									Level: 1,
									Count: 123,
								},
							},
						},
					},
				},
			},
			VolStats: []proto.VolumeDegradeBatch{
				{
					BatchIndex: 1,
					CodeModeStats: []proto.VolumeDegradeCodeMode{
						{
							Mode: codemode.EC3P3,
							DegradeStats: []proto.VolumeDegradeLevel{
								{
									Level: 1,
									Count: 123,
								},
							},
						},
					},
				},
			},
		}

		// set degrade stats failed
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(errMock)
		err := cli.SetVolumeDegradeStats(ctx, stats)
		require.NotNil(t, err)

		// set degrade stats success
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		err = cli.SetVolumeDegradeStats(ctx, stats)
		require.NoError(t, err)

		// get degrade stats failed
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{}, errMock)
		_, err = cli.GetVolumeDegradeStats(ctx)
		require.NotNil(t, err)

		// get degrade stats success
		statsBytes, _ := json.Marshal(stats)
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: statsBytes}, nil)
		statsRet, err := cli.GetVolumeDegradeStats(ctx)
		require.NoError(t, err)
		require.Equal(t, statsRet.BatchSize, stats.BatchSize)
		require.Equal(t, statsRet.BlobStats[0].BatchIndex, stats.BlobStats[0].BatchIndex)
		require.Equal(t, statsRet.VolStats[0].BatchIndex, stats.VolStats[0].BatchIndex)

		// delete degrade stats failed
		cli.client.(*MockClusterManager).EXPECT().DeleteKV(any, any).Return(errMock)
		err = cli.DeleteVolumeDegradeStats(ctx)
		require.NotNil(t, err)

		// delete degrade stats success
		cli.client.(*MockClusterManager).EXPECT().DeleteKV(any, any).Return(nil)
		err = cli.DeleteVolumeDegradeStats(ctx)
		require.NoError(t, err)
	}
}

func TestClustermgrClientExtra(t *testing.T) {
	cli := NewClusterMgrClient(&cmapi.Config{}).(*clustermgrClient)
	mockCli := NewMockClusterManager(gomock.NewController(t))
	cli.client = mockCli

	ctx := context.Background()
	any := gomock.Any()
	errMock := errors.New("fake error")

	{
		// set config
		mockCli.EXPECT().SetConfig(any, any, any).Return(nil)
		err := cli.SetConfig(ctx, "key", "value")
		require.NoError(t, err)

		mockCli.EXPECT().SetConfig(any, any, any).Return(errMock)
		err = cli.SetConfig(ctx, "key", "value")
		require.True(t, errors.Is(err, errMock))
	}
	{
		// get service - matching clusterID
		mockCli.EXPECT().GetService(any, any).Return(cmapi.ServiceInfo{
			Nodes: []cmapi.ServiceNode{
				{ClusterID: 1, Host: "127.0.0.1:8080"},
				{ClusterID: 2, Host: "127.0.0.2:8080"},
			},
		}, nil)
		hosts, err := cli.GetService(ctx, "scheduler", proto.ClusterID(1))
		require.NoError(t, err)
		require.Equal(t, 1, len(hosts))
		require.Equal(t, "127.0.0.1:8080", hosts[0])

		mockCli.EXPECT().GetService(any, any).Return(cmapi.ServiceInfo{}, errMock)
		_, err = cli.GetService(ctx, "scheduler", proto.ClusterID(1))
		require.True(t, errors.Is(err, errMock))
	}
	{
		// list migrate tasks (public wrapper)
		task1 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1), TaskType: proto.TaskTypeDiskRepair}
		task1Bytes, _ := json.Marshal(task1)
		opts := &cmapi.ListKvOpts{Prefix: GenMigrateTaskPrefix(proto.TaskTypeDiskRepair), Count: 10}
		mockCli.EXPECT().ListKV(any, any).Return(cmapi.ListKvRet{
			Kvs: []*cmapi.KeyValue{{Key: task1.TaskID, Value: task1Bytes}}, Marker: "",
		}, nil)
		tasks, marker, err := cli.ListMigrateTasks(ctx, proto.TaskTypeDiskRepair, opts)
		require.NoError(t, err)
		require.Equal(t, 1, len(tasks))
		require.Equal(t, "", marker)
	}
	{
		// get migrate task - task type mismatch
		task1, _ := (&proto.MigrateTask{
			TaskID:   GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), 1),
			TaskType: proto.TaskTypeDiskRepair,
		}).ToTask()
		taskBytes, _ := task1.Marshal()
		mockCli.EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: taskBytes}, nil)
		_, err := cli.GetMigrateTask(ctx, proto.TaskTypeBalance, task1.TaskID)
		require.ErrorIs(t, err, errcode.ErrIllegalTaskType)
	}
	{
		// get migrating disk - task type mismatch
		diskMeta := &MigratingDiskMeta{Disk: &DiskInfoSimple{DiskID: proto.DiskID(1)}, TaskType: proto.TaskTypeDiskDrop}
		diskMetaBytes, _ := json.Marshal(diskMeta)
		mockCli.EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: diskMetaBytes}, nil)
		_, err := cli.GetMigratingDisk(ctx, proto.TaskTypeBalance, proto.DiskID(1))
		require.ErrorIs(t, err, errcode.ErrIllegalTaskType)

		// get migrating disk - disk ID mismatch
		diskMeta2 := &MigratingDiskMeta{Disk: &DiskInfoSimple{DiskID: proto.DiskID(99)}, TaskType: proto.TaskTypeDiskDrop}
		diskMeta2Bytes, _ := json.Marshal(diskMeta2)
		mockCli.EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: diskMeta2Bytes}, nil)
		_, err = cli.GetMigratingDisk(ctx, proto.TaskTypeDiskDrop, proto.DiskID(1))
		require.ErrorIs(t, err, errcode.ErrIllegalTaskType)
	}
}

func TestClustermgrClientShardMethods(t *testing.T) {
	cli := NewClusterMgrClient(&cmapi.Config{}).(*clustermgrClient)
	mockCli := NewMockClusterManager(gomock.NewController(t))
	cli.client = mockCli

	ctx := context.Background()
	any := gomock.Any()
	errMock := errors.New("fake error")

	{
		// get shard info - error
		mockCli.EXPECT().GetShardInfo(any, any).Return(nil, errMock)
		_, err := cli.GetShardInfo(ctx, proto.ShardID(1))
		require.True(t, errors.Is(err, errMock))

		// get shard info - success
		shardInfo := &cmapi.Shard{
			ShardID:      proto.ShardID(10),
			AppliedIndex: 100,
			LeaderDiskID: proto.DiskID(2),
			Units: []cmapi.ShardUnit{
				{Suid: proto.Suid(1), DiskID: proto.DiskID(1), Host: "127.0.0.1:xxx"},
				{Suid: proto.Suid(2), DiskID: proto.DiskID(2), Host: "127.0.0.2:xxx"},
			},
		}
		mockCli.EXPECT().GetShardInfo(any, any).Return(shardInfo, nil)
		ret, err := cli.GetShardInfo(ctx, proto.ShardID(10))
		require.NoError(t, err)
		require.Equal(t, proto.ShardID(10), ret.ShardID)
		require.Equal(t, 2, len(ret.ShardUnitInfos))
	}
	{
		// update shard - error
		mockCli.EXPECT().UpdateShard(any, any).Return(errMock)
		err := cli.UpdateShard(ctx, &UpdateShardArgs{NewSuid: proto.Suid(1), OldSuid: proto.Suid(2)})
		require.True(t, errors.Is(err, errMock))

		// update shard - success
		mockCli.EXPECT().UpdateShard(any, any).Return(nil)
		err = cli.UpdateShard(ctx, &UpdateShardArgs{NewSuid: proto.Suid(3), OldSuid: proto.Suid(2)})
		require.NoError(t, err)
	}
	{
		// alloc shard unit - error
		mockCli.EXPECT().AllocShardUnit(any, any).Return(nil, errMock)
		_, err := cli.AllocShardUnit(ctx, proto.Suid(1), nil)
		require.True(t, errors.Is(err, errMock))

		// alloc shard unit - success
		allocRet := &cmapi.AllocShardUnitRet{Suid: proto.Suid(5), DiskID: proto.DiskID(3), Host: "127.0.0.3:xxx"}
		mockCli.EXPECT().AllocShardUnit(any, any).Return(allocRet, nil)
		info, err := cli.AllocShardUnit(ctx, proto.Suid(1), nil)
		require.NoError(t, err)
		require.Equal(t, proto.Suid(5), info.Suid)
		require.True(t, info.Learner)
	}
	{
		// list disk shard units - error
		mockCli.EXPECT().ListShardUnit(any, any).Return(nil, errMock)
		_, err := cli.ListDiskShardUnits(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errMock))

		// list disk shard units - success
		units := []cmapi.ShardUnitInfo{
			{Suid: proto.Suid(1), DiskID: proto.DiskID(1), Host: "host1", AppliedIndex: 10},
		}
		mockCli.EXPECT().ListShardUnit(any, any).Return(units, nil)
		ret, err := cli.ListDiskShardUnits(ctx, proto.DiskID(1))
		require.NoError(t, err)
		require.Equal(t, 1, len(ret))
		require.Equal(t, proto.Suid(1), ret[0].Suid)
	}
	{
		// list shard - stub, always returns nil
		shards, _, err := cli.ListShard(ctx, proto.ShardID(0), 10)
		require.NoError(t, err)
		require.Nil(t, shards)
	}
	{
		// list shard disk (normal status)
		disk1 := &cmapi.ShardNodeDiskInfo{
			DiskInfo:                   cmapi.DiskInfo{Status: proto.DiskStatusNormal},
			ShardNodeDiskHeartbeatInfo: cmapi.ShardNodeDiskHeartbeatInfo{DiskID: proto.DiskID(1)},
		}
		mockCli.EXPECT().ListShardNodeDisk(any, any).Return(cmapi.ListShardNodeDiskRet{
			Disks: []*cmapi.ShardNodeDiskInfo{disk1}, Marker: defaultListDiskMarker,
		}, nil)
		disks, err := cli.ListShardDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(disks))

		// list broken shard disk - error
		mockCli.EXPECT().ListShardNodeDisk(any, any).Return(cmapi.ListShardNodeDiskRet{}, errMock)
		_, err = cli.ListBrokenShardDisk(ctx)
		require.True(t, errors.Is(err, errMock))

		// list repairing shard disk - success
		mockCli.EXPECT().ListShardNodeDisk(any, any).Return(cmapi.ListShardNodeDiskRet{
			Disks: []*cmapi.ShardNodeDiskInfo{disk1}, Marker: defaultListDiskMarker,
		}, nil)
		disks, err = cli.ListRepairingShardDisk(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(disks))
	}
	{
		// set shard disk repairing
		mockCli.EXPECT().SetShardNodeDisk(any, any, any).Return(nil)
		err := cli.SetShardDiskRepairing(ctx, proto.DiskID(1))
		require.NoError(t, err)

		// set shard disk repaired
		mockCli.EXPECT().SetShardNodeDisk(any, any, any).Return(nil)
		err = cli.SetShardDiskRepaired(ctx, proto.DiskID(1))
		require.NoError(t, err)
	}
	{
		// get shard disk info - error
		mockCli.EXPECT().ShardNodeDiskInfo(any, any).Return(nil, errMock)
		_, err := cli.GetShardDiskInfo(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errMock))

		// get shard disk info - success, also covers ShardNodeDiskInfo.set
		info := &cmapi.ShardNodeDiskInfo{
			DiskInfo: cmapi.DiskInfo{Status: proto.DiskStatusNormal, Host: "127.0.0.1:xxx"},
			ShardNodeDiskHeartbeatInfo: cmapi.ShardNodeDiskHeartbeatInfo{
				DiskID: proto.DiskID(1), FreeShardCnt: 10, UsedShardCnt: 5,
			},
		}
		mockCli.EXPECT().ShardNodeDiskInfo(any, any).Return(info, nil)
		disk, err := cli.GetShardDiskInfo(ctx, proto.DiskID(1))
		require.NoError(t, err)
		require.Equal(t, proto.DiskID(1), disk.DiskID)
		require.True(t, disk.IsHealth())
	}
}
