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

	"github.com/rs/xid"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

func newBalancer(t *testing.T) *BalanceMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	topologyMgr := NewMockClusterTopology(ctr)
	taskLogger := mocks.NewMockRecordLogEncoder(ctr)
	migrater := NewMockMigrater(ctr)
	conf := &BalanceMgrConfig{}
	c := closer.New()

	topologyMgr.EXPECT().Close().AnyTimes().Return()
	migrater.EXPECT().StatQueueTaskCnt().AnyTimes().Return(0, 0, 0)
	migrater.EXPECT().Close().AnyTimes().DoAndReturn(c.Close)
	migrater.EXPECT().Done().AnyTimes().Return(c.Done())
	migrater.EXPECT().WaitEnable().AnyTimes().Return()
	migrater.EXPECT().Enabled().AnyTimes().Return(true)

	mgr := NewBalanceMgr(clusterMgr, volumeUpdater, taskSwitch, topologyMgr, taskLogger, conf)
	mgr.IMigrator = migrater
	return mgr
}

func TestBalanceLoad(t *testing.T) {
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(nil)
	err := mgr.Load()
	require.NoError(t, err)
}

func TestBalanceRun(t *testing.T) {
	mgr := newBalancer(t)
	defer mgr.Close()

	mgr.IMigrator.(*MockMigrater).EXPECT().Run().Return()
	mgr.IMigrator.(*MockMigrater).EXPECT().GetMigratingDiskNum().AnyTimes().Return(1)
	mgr.cfg.CollectTaskIntervalS = 1
	mgr.cfg.CheckTaskIntervalS = 1
	require.True(t, mgr.Enabled())
	mgr.Run()

	time.Sleep(1 * time.Second)
}

func TestBalanceCollectionTask(t *testing.T) {
	{
		mgr := newBalancer(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().GetMigratingDiskNum().AnyTimes().Return(1)

		err := mgr.collectionTask()
		require.True(t, errors.Is(err, ErrTooManyBalancingTasks))
		mgr.Close()
	}
	{
		mgr := newBalancer(t)
		mgr.cfg.DiskConcurrency = 2
		mgr.IMigrator.(*MockMigrater).EXPECT().GetMigratingDiskNum().AnyTimes().Return(1)

		disk1 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z0",
			Rack:         "rack1",
			Host:         "127.0.0.1:8000",
			Status:       proto.DiskStatusNormal,
			DiskID:       1,
			FreeChunkCnt: 10,
			MaxChunkCnt:  700,
		}
		disk2 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z1",
			Rack:         "rack1",
			Host:         "127.0.0.2:8000",
			Status:       proto.DiskStatusNormal,
			DiskID:       2,
			FreeChunkCnt: 100,
			MaxChunkCnt:  700,
		}
		disk3 := &client.DiskInfoSimple{
			ClusterID:    1,
			Idc:          "z1",
			Rack:         "rack1",
			Host:         "127.0.0.3:8000",
			Status:       proto.DiskStatusBroken,
			DiskID:       3,
			FreeChunkCnt: 20,
			MaxChunkCnt:  700,
		}
		clusterTopMgr := &ClusterTopologyMgr{
			taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{}),
		}
		clusterTopMgr.buildClusterTopology([]*client.DiskInfoSimple{disk1, disk2, disk3}, 1)
		mgr.IMigrator.(*MockMigrater).EXPECT().IsMigratingDisk(any).AnyTimes().DoAndReturn(func(diskID proto.DiskID) bool {
			return diskID == 1
		})
		mgr.clusterTopology = clusterTopMgr

		err := mgr.collectionTask()
		require.True(t, errors.Is(err, ErrNoBalanceVunit))

		// select one task
		mgr.cfg.MinDiskFreeChunkCnt = 101
		volume := MockGenVolInfo(10000, codemode.EC6P6, proto.VolumeStatusIdle)
		var units []*client.VunitInfoSimple
		for _, unit := range volume.VunitLocations {
			ele := client.VunitInfoSimple{
				Vuid:   unit.Vuid,
				DiskID: unit.DiskID,
			}
			units = append(units, &ele)
		}
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Return()
		err = mgr.collectionTask()
		require.NoError(t, err)

		// select one task and gen task failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(nil, errMock)
		err = mgr.collectionTask()
		require.True(t, errors.Is(err, ErrNoBalanceVunit))

		// select one task and gen task failed
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, any).Return(units, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).AnyTimes().Return(nil, errMock)
		err = mgr.collectionTask()
		require.True(t, errors.Is(err, ErrNoBalanceVunit))
	}
}

func TestBalanceAcquireTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().AcquireTask(any, any).Return(proto.MigrateTask{TaskType: proto.TaskTypeBalance}, nil)
	_, err := mgr.AcquireTask(ctx, idc)
	require.NoError(t, err)
}

func TestBalanceCancelTask(t *testing.T) {
	ctx := context.Background()
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CancelTask(any, any).Return(nil)
	err := mgr.CancelTask(ctx, &api.OperateTaskArgs{})
	require.NoError(t, err)
}

func TestBalanceReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeBalance, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
	require.NoError(t, err)
}

func TestBalanceCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeBalance, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.CompleteTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskID: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(errMock)
	err = mgr.CompleteTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskID: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.True(t, errors.Is(err, errMock))
}

func TestBalanceRenewalTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().RenewalTask(any, any, any).Return(nil)
	err := mgr.RenewalTask(ctx, idc, "")
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().RenewalTask(any, any, any).Return(errMock)
	err = mgr.RenewalTask(ctx, idc, "")
	require.True(t, errors.Is(err, errMock))
}

func TestBalanceStatQueueTaskCnt(t *testing.T) {
	mgr := newBalancer(t)
	inited, prepared, completed := mgr.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)
}

func TestBalanceCheckAndClearJunkTasks(t *testing.T) {
	{
		mgr := newBalancer(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().DeletedTasks().Return([]DeletedTask{})
		mgr.checkAndClearJunkTasks()
	}
	{
		mgr := newBalancer(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().DeletedTasks().Return([]DeletedTask{
			{DiskID: proto.DiskID(1), TaskID: xid.New().String(), DeletedTime: time.Now()},
		})
		mgr.checkAndClearJunkTasks()
	}
	{
		mgr := newBalancer(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().DeletedTasks().Return([]DeletedTask{
			{DiskID: proto.DiskID(1), TaskID: xid.New().String(), DeletedTime: time.Now().Add(-junkMigrationTaskProtectionWindow)},
		})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(nil, errMock)
		mgr.checkAndClearJunkTasks()
	}
	{
		mgr := newBalancer(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().DeletedTasks().Return([]DeletedTask{
			{DiskID: proto.DiskID(1), TaskID: xid.New().String(), DeletedTime: time.Now().Add(-junkMigrationTaskProtectionWindow)},
		})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(nil, errcode.ErrNotFound)
		mgr.IMigrator.(*MockMigrater).EXPECT().ClearDeletedTaskByID(any, any).Return()
		mgr.checkAndClearJunkTasks()
	}
	{
		mgr := newBalancer(t)
		mgr.IMigrator.(*MockMigrater).EXPECT().DeletedTasks().Return([]DeletedTask{
			{DiskID: proto.DiskID(1), TaskID: xid.New().String(), DeletedTime: time.Now().Add(-junkMigrationTaskProtectionWindow)},
		})
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(&proto.MigrateTask{}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ClearDeletedTaskByID(any, any).Return()
		mgr.checkAndClearJunkTasks()
	}
}
