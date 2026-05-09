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
	volumeUpdater := NewMockTaskAPI(ctr)
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
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Return(nil)
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
	mgr.IMigrator.(*MockMigrater).EXPECT().AcquireTask(any, any).Return(&proto.Task{TaskType: proto.TaskTypeBalance}, nil)
	_, err := mgr.AcquireTask(ctx, idc)
	require.NoError(t, err)
}

func TestBalanceCancelTask(t *testing.T) {
	ctx := context.Background()
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CancelTask(any, any).Return(nil)
	err := mgr.CancelTask(ctx, &api.TaskArgs{})
	require.NoError(t, err)
}

func TestBalanceReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().ReclaimTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeBalance, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	taskArgs := generateTaskArgs(t1, "")
	err := mgr.ReclaimTask(ctx, taskArgs)
	require.NoError(t, err)
}

func TestBalanceCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newBalancer(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(proto.TaskTypeBalance, idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	taskArgs := generateTaskArgs(t1, "")
	err := mgr.CompleteTask(ctx, taskArgs)
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(errMock)
	err = mgr.CompleteTask(ctx, taskArgs)
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

func TestSelectDisks(t *testing.T) {
	// diskTrigger has the most free chunks, triggering the IDC-level balance condition.
	diskTrigger := &client.DiskInfoSimple{
		ClusterID: 1, Idc: "z0", DiskID: 1, Status: proto.DiskStatusNormal,
		FreeChunkCnt: 200, MaxChunkCnt: 700,
	}
	// diskSlot: low free chunks → selected by slot condition
	diskSlot := &client.DiskInfoSimple{
		ClusterID: 1, Idc: "z0", DiskID: 2, Status: proto.DiskStatusNormal,
		FreeChunkCnt: 5, MaxChunkCnt: 700,
		Used: 500, Size: 1000, // 50%, below watermark
	}
	// diskWatermark: free chunks sufficient but usage above threshold → selected by watermark
	diskWatermark := &client.DiskInfoSimple{
		ClusterID: 1, Idc: "z0", DiskID: 3, Status: proto.DiskStatusNormal,
		FreeChunkCnt: 50, MaxChunkCnt: 700,
		Used: 950, Size: 1000, // 95%, above watermark
	}
	// diskHealthy: free chunks sufficient and usage below threshold → not selected
	diskHealthy := &client.DiskInfoSimple{
		ClusterID: 1, Idc: "z0", DiskID: 4, Status: proto.DiskStatusNormal,
		FreeChunkCnt: 50, MaxChunkCnt: 700,
		Used: 500, Size: 1000, // 50%, below watermark
	}
	// diskBroken: usage above threshold but broken → not selected
	diskBroken := &client.DiskInfoSimple{
		ClusterID: 1, Idc: "z0", DiskID: 5, Status: proto.DiskStatusBroken,
		FreeChunkCnt: 5, MaxChunkCnt: 700,
		Used: 950, Size: 1000,
	}

	buildTopology := func(disks []*client.DiskInfoSimple) *ClusterTopologyMgr {
		m := &ClusterTopologyMgr{taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{})}
		m.buildClusterTopology(disks, 1)
		return m
	}

	allDisks := []*client.DiskInfoSimple{diskTrigger, diskSlot, diskWatermark, diskHealthy, diskBroken}

	t.Run("watermark disabled: only slot condition applies", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.cfg.MaxDiskFreeChunkCnt = 100
		mgr.cfg.MinDiskFreeChunkCnt = 20
		mgr.cfg.DiskUsageThreshold = 0
		mgr.clusterTopology = buildTopology(allDisks)
		mgr.IMigrator.(*MockMigrater).EXPECT().IsMigratingDisk(any).AnyTimes().Return(false)

		selected := mgr.selectDisks(mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
		ids := diskIDs(selected)
		require.Contains(t, ids, diskSlot.DiskID)
		require.NotContains(t, ids, diskWatermark.DiskID)
		require.NotContains(t, ids, diskHealthy.DiskID)
		require.NotContains(t, ids, diskBroken.DiskID)
	})

	t.Run("watermark enabled: slot and watermark both qualify", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.cfg.MaxDiskFreeChunkCnt = 100
		mgr.cfg.MinDiskFreeChunkCnt = 20
		mgr.cfg.DiskUsageThreshold = 0.9
		mgr.clusterTopology = buildTopology(allDisks)
		mgr.IMigrator.(*MockMigrater).EXPECT().IsMigratingDisk(any).AnyTimes().Return(false)

		selected := mgr.selectDisks(mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
		ids := diskIDs(selected)
		require.Contains(t, ids, diskSlot.DiskID)
		require.Contains(t, ids, diskWatermark.DiskID)
		require.NotContains(t, ids, diskHealthy.DiskID)
		require.NotContains(t, ids, diskBroken.DiskID)
	})
}

func diskIDs(disks []*client.DiskInfoSimple) []proto.DiskID {
	ids := make([]proto.DiskID, 0, len(disks))
	for _, d := range disks {
		ids = append(ids, d.DiskID)
	}
	return ids
}

// TestSelectBalanceVunitByWatermark verifies that:
//   - low usage disk: the function skips larger vunits and picks the smallest used idle one
//   - high usage disk: the function skips smaller vunits and picks the largest used idle one
//
// Each case has 3 vunits with distinct Used values. The vunit that should be selected
// is placed second in the expected sort order, with the first one belonging to a non-idle
// volume. This forces the function to actually iterate in the correct order to pass.
func TestSelectBalanceVunitByWatermark(t *testing.T) {
	// 3 vunits across 3 distinct volumes, Used: 100, 500, 900
	makeVunits := func() []*client.VunitInfoSimple {
		return []*client.VunitInfoSimple{
			{Vuid: mustNewVuid(1001, 0), Used: 100},
			{Vuid: mustNewVuid(1002, 0), Used: 500},
			{Vuid: mustNewVuid(1003, 0), Used: 900},
		}
	}

	activeVol := func(vid proto.Vid) *client.VolumeInfoSimple {
		return MockGenVolInfo(vid, codemode.EC6P6, proto.VolumeStatusActive)
	}
	idleVol := func(vid proto.Vid) *client.VolumeInfoSimple {
		return MockGenVolInfo(vid, codemode.EC6P6, proto.VolumeStatusIdle)
	}

	t.Run("low usage: skip active, select smallest idle vunit", func(t *testing.T) {
		// sorted ascending: [100, 500, 900]
		// vid=1001(Used=100) is active → skip
		// vid=1002(Used=500) is idle → selected
		mgr := newBalancer(t)
		mgr.cfg.DiskUsageThreshold = 0.9
		disk := &client.DiskInfoSimple{
			DiskID: 10, FreeChunkCnt: 5,
			Used: 500, Size: 1000, // 50%, below threshold
		}
		units := makeVunits()
		wantVuid := units[1].Vuid // Used=500, capture before in-place sort

		gomock.InOrder(
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, disk.DiskID).Return(units, nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1001)).Return(activeVol(1001), nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1002)).Return(idleVol(1002), nil),
		)

		vuid, err := mgr.selectBalanceVunit(context.Background(), disk)
		require.NoError(t, err)
		require.Equal(t, wantVuid, vuid)
		mgr.Close()
	})

	t.Run("high usage: skip active, select largest idle vunit", func(t *testing.T) {
		// sorted descending: [900, 500, 100]
		// vid=1003(Used=900) is active → skip
		// vid=1002(Used=500) is idle → selected
		mgr := newBalancer(t)
		mgr.cfg.DiskUsageThreshold = 0.9
		disk := &client.DiskInfoSimple{
			DiskID: 10, FreeChunkCnt: 5,
			Used: 950, Size: 1000, // 95%, above threshold
		}
		units := makeVunits()
		wantVuid := units[2].Vuid // Used=500, capture before in-place sort

		gomock.InOrder(
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, disk.DiskID).Return(units, nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1003)).Return(idleVol(1003), nil),
		)

		vuid, err := mgr.selectBalanceVunit(context.Background(), disk)
		require.NoError(t, err)
		require.Equal(t, wantVuid, vuid)
		mgr.Close()
	})
}

func mustNewVuid(vid proto.Vid, idx uint8) proto.Vuid {
	vuid, _ := proto.NewVuid(vid, idx, 1)
	return vuid
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
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetMigrateTask(any, any, any).Return(&proto.Task{}, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().DeleteMigrateTask(any, any).Return(nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().ClearDeletedTaskByID(any, any).Return()
		mgr.checkAndClearJunkTasks()
	}
}
