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

//go:generate mockgen -destination=./clustermgr_mock_test.go -package=client -mock_names IClusterManager=MockClusterManager github.com/cubefs/cubefs/blobstore/scheduler/client IClusterManager

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	defaultVolumeListMarker = proto.Vid(0)
	defaultDiskListMarker   = proto.DiskID(0)
)

func init() {
	log.SetOutputLevel(log.Lfatal)
}

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
		_, err := cli.GetVolumeInfo(ctx, proto.Vid(1))
		require.True(t, errors.Is(err, errMock))

		volume := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusIdle)
		volume2 := MockGenVolInfo(10, codemode.EC6P6, proto.VolumeStatusActive)
		cli.client.(*MockClusterManager).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		cli.client.(*MockClusterManager).EXPECT().GetVolumeInfo(any, any).Return(volume2, nil)
		vol, err := cli.GetVolumeInfo(ctx, proto.Vid(1))
		require.NoError(t, err)
		vol2, err := cli.GetVolumeInfo(ctx, proto.Vid(2))
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
		err := cli.LockVolume(ctx, proto.Vid(1))
		require.NoError(t, err)
	}
	{
		// unlock volume
		cli.client.(*MockClusterManager).EXPECT().UnlockVolume(any, any).Return(nil)
		err := cli.UnlockVolume(ctx, proto.Vid(1))
		require.NoError(t, err)

		cli.client.(*MockClusterManager).EXPECT().UnlockVolume(any, any).Return(errcode.ErrUnlockNotAllow)
		err = cli.UnlockVolume(ctx, proto.Vid(1))
		require.NoError(t, err)

		cli.client.(*MockClusterManager).EXPECT().UnlockVolume(any, any).Return(errMock)
		err = cli.UnlockVolume(ctx, proto.Vid(1))
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
		_, err := cli.AllocVolumeUnit(ctx, proto.Vuid(2))
		require.True(t, errors.Is(err, errMock))

		unit := &cmapi.AllocVolumeUnit{Vuid: proto.Vuid(3), DiskID: proto.DiskID(2)}
		cli.client.(*MockClusterManager).EXPECT().AllocVolumeUnit(any, any).Return(unit, nil)
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(nil, errMock)
		_, err = cli.AllocVolumeUnit(ctx, proto.Vuid(2))
		require.True(t, errors.Is(err, errMock))

		cli.client.(*MockClusterManager).EXPECT().AllocVolumeUnit(any, any).Return(unit, nil)
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(&blobnode.DiskInfo{Host: "127.0.0.1:xxx"}, nil)
		allocUnit, err := cli.AllocVolumeUnit(ctx, proto.Vuid(2))
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
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(&blobnode.DiskInfo{Host: "127.0.0.1:xxx"}, nil)
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
		_, err := cli.ListBrokenDisks(ctx, 1)
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

		disk1 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{Disks: []*blobnode.DiskInfo{disk1}, Marker: defaultDiskListMarker}, nil)
		disks, err := cli.listAllDisks(ctx, proto.DiskStatusNormal)
		require.NoError(t, err)
		require.Equal(t, 1, len(disks))
	}
	{
		// list disks
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{}, errMock)
		_, err := cli.listDisks(ctx, proto.DiskStatusNormal, 1)
		require.True(t, errors.Is(err, errMock))

		disk1 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}
		disk2 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{Disks: []*blobnode.DiskInfo{disk1}, Marker: proto.DiskID(2)}, nil)
		cli.client.(*MockClusterManager).EXPECT().ListDisk(any, any).Return(cmapi.ListDiskRet{Disks: []*blobnode.DiskInfo{disk2}, Marker: defaultDiskListMarker}, nil)
		disks, err := cli.listDisks(ctx, proto.DiskStatusNormal, 2)
		require.NoError(t, err)
		require.Equal(t, 2, len(disks))
	}
	{
		// list drop disk
		cli.client.(*MockClusterManager).EXPECT().ListDroppingDisk(any).Return(nil, errMock)
		_, err := cli.ListDropDisks(ctx)
		require.True(t, errors.Is(err, errMock))

		disk1 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}
		cli.client.(*MockClusterManager).EXPECT().ListDroppingDisk(any).Return([]*blobnode.DiskInfo{disk1}, nil)
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

		disk1 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusDropped}
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(disk1, nil)
		err = cli.SetDiskDropped(ctx, proto.DiskID(1))
		require.NoError(t, err)

		disk2 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusRepairing}
		cli.client.(*MockClusterManager).EXPECT().DiskInfo(any, any).Return(disk2, nil)
		err = cli.SetDiskDropped(ctx, proto.DiskID(1))
		require.True(t, errors.Is(err, errcode.ErrCanNotDropped))

		disk3 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusNormal}
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

		disk1 := &blobnode.DiskInfo{Host: "127.0.0.1:xxx", Status: proto.DiskStatusDropped}
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
		err := cli.AddMigrateTask(ctx, &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), proto.Vid(1))})
		require.NoError(t, err)
	}
	{
		// update migrate task
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		err := cli.UpdateMigrateTask(ctx, &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), proto.Vid(1))})
		require.NoError(t, err)
	}
	{
		// get migrate task
		task1 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), proto.Vid(1))}
		taskBytes, _ := json.Marshal(task1)
		cli.client.(*MockClusterManager).EXPECT().SetKV(any, any, any).Return(nil)
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: taskBytes}, nil)
		err := cli.AddMigrateTask(ctx, task1)
		require.NoError(t, err)
		task2, err := cli.GetMigrateTask(ctx, task1.TaskID)
		require.NoError(t, err)
		require.Equal(t, task1.TaskID, task2.TaskID)

		// unmarshal failed
		taskBytes = append(taskBytes, []byte("mock")...)
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{Value: taskBytes}, nil)
		_, err = cli.GetMigrateTask(ctx, task1.TaskID)
		require.Error(t, err)

		// clustermgr return err
		cli.client.(*MockClusterManager).EXPECT().GetKV(any, any).Return(cmapi.GetKvRet{}, errMock)
		_, err = cli.GetMigrateTask(ctx, task1.TaskID)
		require.True(t, errors.Is(err, errMock))
	}
	{
		// delete migrate task
		task1 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeDiskRepair, proto.DiskID(1), proto.Vid(1))}
		cli.client.(*MockClusterManager).EXPECT().DeleteKV(any, any).Return(nil)
		err := cli.DeleteMigrateTask(ctx, task1.TaskID)
		require.NoError(t, err)
	}
	{
		// list all migrate tasks by disk_id
		diskID := proto.DiskID(100)
		task1 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeBalance, diskID, proto.Vid(1)), TaskType: proto.TaskTypeBalance}
		task1Bytes, _ := json.Marshal(task1)
		task2 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeBalance, diskID, proto.Vid(2)), TaskType: proto.TaskTypeBalance}
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
		task3 := &proto.MigrateTask{TaskID: GenMigrateTaskID(proto.TaskTypeBalance, proto.DiskID(200), proto.Vid(2)), TaskType: proto.TaskTypeBalance}
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
}
