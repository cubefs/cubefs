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
	"github.com/cubefs/cubefs/blobstore/common/trace"
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
		mgr.IMigrator.(*MockMigrater).EXPECT().IsTaskExist(any, any).Return(false)
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
		selected := mgr.selectDisks(context.Background(), mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
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
		selected := mgr.selectDisks(context.Background(), mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
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

// TestSelectBalanceVunitByWatermark verifies selectBalanceVunit iteration order:
//   - low usage: ascending Used order (standard balance), skip active and pick next idle
//   - high usage (no compact-migrate): descending LogicSize order, skip active and pick next idle
//   - high usage (compact-migrate enabled): high-hole-rate large chunk is prioritised
func TestSelectBalanceVunitByWatermark(t *testing.T) {
	activeVol := func(vid proto.Vid) *client.VolumeInfoSimple {
		return MockGenVolInfo(vid, codemode.EC6P6, proto.VolumeStatusActive)
	}
	idleVol := func(vid proto.Vid) *client.VolumeInfoSimple {
		return MockGenVolInfo(vid, codemode.EC6P6, proto.VolumeStatusIdle)
	}

	t.Run("low usage: skip active, select smallest-used idle vunit", func(t *testing.T) {
		// sorted ascending by Used: [100, 500, 900]
		// vid=1001(Used=100) is active → skip; vid=1002(Used=500) is idle → selected
		mgr := newBalancer(t)
		mgr.cfg.DiskUsageThreshold = 0.9
		disk := &client.DiskInfoSimple{DiskID: 10, FreeChunkCnt: 5, Used: 500, Size: 1000}
		units := []*client.VunitInfoSimple{
			{Vuid: mustNewVuid(1001, 0), Used: 100},
			{Vuid: mustNewVuid(1002, 0), Used: 500},
			{Vuid: mustNewVuid(1003, 0), Used: 900},
		}
		wantVuid := units[1].Vuid

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

	t.Run("high usage (no compact-migrate): skip active, select largest-LogicSize idle vunit", func(t *testing.T) {
		// sorted descending by LogicSize: [900G, 500G, 100G]
		// vid=1003(LogicSize=900G) is active → skip; vid=1002(LogicSize=500G) is idle → selected
		mgr := newBalancer(t)
		mgr.cfg.DiskUsageThreshold = 0.9
		disk := &client.DiskInfoSimple{DiskID: 10, FreeChunkCnt: 5, Used: 950, Size: 1000}
		const gib = uint64(1 << 30)
		units := []*client.VunitInfoSimple{
			{Vuid: mustNewVuid(1001, 0), LogicSize: 100 * gib, Used: 10 * gib},
			{Vuid: mustNewVuid(1002, 0), LogicSize: 500 * gib, Used: 50 * gib},
			{Vuid: mustNewVuid(1003, 0), LogicSize: 900 * gib, Used: 90 * gib},
		}
		wantVuid := units[1].Vuid

		gomock.InOrder(
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, disk.DiskID).Return(units, nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1003)).Return(activeVol(1003), nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1002)).Return(idleVol(1002), nil),
		)

		vuid, err := mgr.selectBalanceVunit(context.Background(), disk)
		require.NoError(t, err)
		require.Equal(t, wantVuid, vuid)
		mgr.Close()
	})

	t.Run("high usage (compact-migrate): idle high-hole chunk migrates directly", func(t *testing.T) {
		// sort order: [1002(hole=80%), 1003(LogicSize=30G,hole=10%), 1001(LogicSize=20G,hole=10%)]
		// vid=1002 is idle → migrate directly
		mgr := newBalancer(t)
		mgr.cfg.DiskUsageThreshold = 0.9
		mgr.cfg.CompactMigrateHoleRate = 0.6
		mgr.cfg.CompactMigrateMinLogicSize = 16 * uint64(1<<30)
		disk := &client.DiskInfoSimple{DiskID: 10, FreeChunkCnt: 5, Used: 950, Size: 1000}
		const gib = uint64(1 << 30)
		units := []*client.VunitInfoSimple{
			{Vuid: mustNewVuid(1001, 0), LogicSize: 20 * gib, Used: 18 * gib}, // hole=10%
			{Vuid: mustNewVuid(1002, 0), LogicSize: 20 * gib, Used: 4 * gib},  // hole=80%
			{Vuid: mustNewVuid(1003, 0), LogicSize: 30 * gib, Used: 27 * gib}, // hole=10%, largest
		}
		wantVuid := units[1].Vuid // 1002, captured before in-place sort

		gomock.InOrder(
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, disk.DiskID).Return(units, nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1002)).Return(idleVol(1002), nil),
		)

		vuid, err := mgr.selectBalanceVunit(context.Background(), disk)
		require.NoError(t, err)
		require.Equal(t, wantVuid, vuid)
		mgr.Close()
	})

	t.Run("high usage (compact-migrate): active high-hole chunk tracked in priorityVuids, next idle selected", func(t *testing.T) {
		// sort order: [1002(hole=80%), 1003(LogicSize=30G), 1001(LogicSize=20G)]
		// vid=1002 is active + meets compact threshold → added to priorityVuids
		// vid=1003 is idle → selected for migration
		mgr := newBalancer(t)
		mgr.cfg.DiskUsageThreshold = 0.9
		mgr.cfg.CompactMigrateHoleRate = 0.6
		mgr.cfg.CompactMigrateMinLogicSize = 16 * uint64(1<<30)
		disk := &client.DiskInfoSimple{DiskID: 10, FreeChunkCnt: 5, Used: 950, Size: 1000}
		const gib = uint64(1 << 30)
		vuid1002 := mustNewVuid(1002, 0)
		units := []*client.VunitInfoSimple{
			{Vuid: mustNewVuid(1001, 0), LogicSize: 20 * gib, Used: 18 * gib}, // hole=10%
			{Vuid: vuid1002, LogicSize: 20 * gib, Used: 4 * gib},              // hole=80%
			{Vuid: mustNewVuid(1003, 0), LogicSize: 30 * gib, Used: 27 * gib}, // hole=10%, largest
		}
		wantVuid := units[2].Vuid // 1003, captured before in-place sort

		gomock.InOrder(
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().ListDiskVolumeUnits(any, disk.DiskID).Return(units, nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1002)).Return(activeVol(1002), nil),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, proto.Vid(1003)).Return(idleVol(1003), nil),
		)

		vuid, err := mgr.selectBalanceVunit(context.Background(), disk)
		require.NoError(t, err)
		require.Equal(t, wantVuid, vuid)
		_, hasPriority := mgr.priorityVuids[vuid1002]
		require.True(t, hasPriority, "active high-hole vunit should be tracked in priorityVuids")
		mgr.Close()
	})
}

// TestVunitLessHighUsage covers all ordering branches of vunitLessHighUsage.
func TestVunitLessHighUsage(t *testing.T) {
	const gib = uint64(1 << 30)
	const minSize = 16 * gib

	newMgr := func(holeRate float64) *BalanceMgr {
		return &BalanceMgr{cfg: &BalanceMgrConfig{
			CompactMigrateHoleRate:     holeRate,
			CompactMigrateMinLogicSize: minSize,
		}}
	}
	mk := func(logicSize, used uint64) *client.VunitInfoSimple {
		return &client.VunitInfoSimple{LogicSize: logicSize, Used: used}
	}

	mgr := newMgr(0.6)

	t.Run("large high-hole beats large low-hole", func(t *testing.T) {
		// vi: 20G, used=4G  → hole=80% (≥0.6)
		// vj: 20G, used=14G → hole=30% (<0.6)
		vi := mk(20*gib, 4*gib)
		vj := mk(20*gib, 14*gib)
		require.True(t, mgr.vunitLessHighUsage(vi, vj))
		require.False(t, mgr.vunitLessHighUsage(vj, vi))
	})

	t.Run("both large above threshold: higher hole rate first", func(t *testing.T) {
		// vi: 20G, used=2G → hole=90%
		// vj: 20G, used=4G → hole=80%
		vi := mk(20*gib, 2*gib)
		vj := mk(20*gib, 4*gib)
		require.True(t, mgr.vunitLessHighUsage(vi, vj))
		require.False(t, mgr.vunitLessHighUsage(vj, vi))
	})

	t.Run("both large below threshold: larger LogicSize first", func(t *testing.T) {
		// vi: 30G, used=25G → hole≈17%
		// vj: 20G, used=16G → hole=20% (higher hole but smaller size)
		vi := mk(30*gib, 25*gib)
		vj := mk(20*gib, 16*gib)
		require.True(t, mgr.vunitLessHighUsage(vi, vj))
		require.False(t, mgr.vunitLessHighUsage(vj, vi))
	})

	t.Run("large always beats small regardless of hole rate", func(t *testing.T) {
		// large chunk with zero hole rate vs small chunk with 100% hole rate
		large := mk(20*gib, 20*gib) // hole=0%
		small := mk(10*gib, 0)      // hole=100%
		require.True(t, mgr.vunitLessHighUsage(large, small))
		require.False(t, mgr.vunitLessHighUsage(small, large))
	})

	t.Run("both small: larger LogicSize first", func(t *testing.T) {
		vi := mk(10*gib, 0)
		vj := mk(5*gib, 0)
		require.True(t, mgr.vunitLessHighUsage(vi, vj))
		require.False(t, mgr.vunitLessHighUsage(vj, vi))
	})

	t.Run("feature disabled (HoleRate=0): sort by LogicSize only", func(t *testing.T) {
		mgrOff := newMgr(0)
		vi := mk(20*gib, 0)
		vj := mk(10*gib, 0)
		require.True(t, mgrOff.vunitLessHighUsage(vi, vj))
		require.False(t, mgrOff.vunitLessHighUsage(vj, vi))
	})

	t.Run("equal inputs are not less", func(t *testing.T) {
		v := mk(20*gib, 4*gib)
		require.False(t, mgr.vunitLessHighUsage(v, v))
	})

	t.Run("LogicSize=0 with MinLogicSize=0: no panic (float64 NaN, not integer div-by-zero)", func(t *testing.T) {
		// When MinLogicSize=0, any vunit (including LogicSize=0) qualifies as "large",
		// triggering the holeRate path: 1 - Used/LogicSize = NaN for 0/0.
		// Go float64 division never panics; NaN comparisons simply return false.
		mgrZeroMin := &BalanceMgr{cfg: &BalanceMgrConfig{
			CompactMigrateHoleRate:     0.6,
			CompactMigrateMinLogicSize: 0,
		}}
		vi := mk(0, 0)
		vj := mk(0, 0)
		require.NotPanics(t, func() { mgrZeroMin.vunitLessHighUsage(vi, vj) })
		// sort stability: both are NaN, neither is "less"
		require.False(t, mgrZeroMin.vunitLessHighUsage(vi, vj))
	})
}

// TestMeetsCompactMigrateThreshold_ZeroLogicSize documents that LogicSize=0 never panics.
func TestMeetsCompactMigrateThreshold_ZeroLogicSize(t *testing.T) {
	const gib = uint64(1 << 30)
	mk := func(logicSize, used uint64) *client.VunitInfoSimple {
		return &client.VunitInfoSimple{LogicSize: logicSize, Used: used}
	}

	{
		// MinLogicSize=0: LogicSize=0 is >= 0, enters holeRate branch; 1 - 0/0 = NaN.
		// NaN >= threshold is false → returns false (no panic).
		mgr := &BalanceMgr{cfg: &BalanceMgrConfig{
			CompactMigrateHoleRate:     0.5,
			CompactMigrateMinLogicSize: 0,
		}}
		v := mk(0, 0)
		require.NotPanics(t, func() { mgr.meetsCompactMigrateThreshold(v) })
		require.False(t, mgr.meetsCompactMigrateThreshold(v))
	}
	{
		// MinLogicSize > 0: LogicSize=0 < MinLogicSize → early return false (no holeRate path at all).
		mgr := &BalanceMgr{cfg: &BalanceMgrConfig{
			CompactMigrateHoleRate:     0.5,
			CompactMigrateMinLogicSize: 16 * gib,
		}}
		require.False(t, mgr.meetsCompactMigrateThreshold(mk(0, 0)))
	}
}

func mustNewVuid(vid proto.Vid, idx uint8) proto.Vuid {
	vuid, _ := proto.NewVuid(vid, idx, 1)
	return vuid
}

func TestGenerateTask(t *testing.T) {
	disk := &client.DiskInfoSimple{DiskID: 10, Idc: "z0", Status: proto.DiskStatusNormal}
	vuid := mustNewVuid(1001, 0)
	_, ctx := trace.StartSpanFromContext(context.Background(), "test")

	t.Run("task already exists: cleaned from priorityVuids", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.priorityVuids[vuid] = disk
		mgr.IMigrator.(*MockMigrater).EXPECT().IsTaskExist(disk.DiskID, vuid).Return(true)

		err := mgr.generateTask(ctx, vuid, disk)
		require.NoError(t, err)
		_, exists := mgr.priorityVuids[vuid]
		require.False(t, exists)
	})

	t.Run("add task success: cleaned from priorityVuids", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.priorityVuids[vuid] = disk
		gomock.InOrder(
			mgr.IMigrator.(*MockMigrater).EXPECT().IsTaskExist(disk.DiskID, vuid).Return(false),
			mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Return(nil),
		)

		err := mgr.generateTask(ctx, vuid, disk)
		require.NoError(t, err)
		_, exists := mgr.priorityVuids[vuid]
		require.False(t, exists)
	})

	t.Run("add task failed: stays in priorityVuids", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.priorityVuids[vuid] = disk
		gomock.InOrder(
			mgr.IMigrator.(*MockMigrater).EXPECT().IsTaskExist(disk.DiskID, vuid).Return(false),
			mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Return(errMock),
		)

		err := mgr.generateTask(ctx, vuid, disk)
		require.Error(t, err)
		_, exists := mgr.priorityVuids[vuid]
		require.True(t, exists)
	})
}

func TestCollectionTaskPriorityPath(t *testing.T) {
	disk := &client.DiskInfoSimple{DiskID: 10, Idc: "z0", Status: proto.DiskStatusNormal}
	vuid := mustNewVuid(2001, 0)

	t.Run("priority vuid idle: task created, cleaned from priorityVuids", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.cfg.DiskConcurrency = 2
		mgr.priorityVuids[vuid] = disk
		mgr.IMigrator.(*MockMigrater).EXPECT().GetMigratingDiskNum().Return(1)

		idleVol := MockGenVolInfo(vuid.Vid(), codemode.EC6P6, proto.VolumeStatusIdle)
		gomock.InOrder(
			mgr.IMigrator.(*MockMigrater).EXPECT().IsMigratingDisk(disk.DiskID).Return(false),
			mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, vuid.Vid()).Return(idleVol, nil),
			mgr.IMigrator.(*MockMigrater).EXPECT().IsTaskExist(disk.DiskID, vuid).Return(false),
			mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Return(nil),
		)

		err := mgr.collectionTask()
		require.NoError(t, err)
		_, exists := mgr.priorityVuids[vuid]
		require.False(t, exists)
	})

	t.Run("priority vuid still active: skipped, stays in priorityVuids", func(t *testing.T) {
		mgr := newBalancer(t)
		mgr.cfg.DiskConcurrency = 2
		mgr.priorityVuids[vuid] = disk
		mgr.IMigrator.(*MockMigrater).EXPECT().GetMigratingDiskNum().Return(1)
		mgr.IMigrator.(*MockMigrater).EXPECT().IsMigratingDisk(any).AnyTimes().Return(false)

		activeVol := MockGenVolInfo(vuid.Vid(), codemode.EC6P6, proto.VolumeStatusActive)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, vuid.Vid()).Return(activeVol, nil)

		clusterTopMgr := &ClusterTopologyMgr{
			taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{}),
		}
		clusterTopMgr.buildClusterTopology([]*client.DiskInfoSimple{}, 1)
		mgr.clusterTopology = clusterTopMgr

		err := mgr.collectionTask()
		require.True(t, errors.Is(err, ErrNoBalanceVunit))
		_, exists := mgr.priorityVuids[vuid]
		require.True(t, exists)
	})
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
