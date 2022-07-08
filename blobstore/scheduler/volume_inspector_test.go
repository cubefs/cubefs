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

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func genMockFailShards(vid proto.Vid, bids []proto.BlobID) []*proto.MissedShard {
	vuid, _ := proto.NewVuid(vid, 1, 1)
	var FailShards []*proto.MissedShard
	for _, bid := range bids {
		FailShards = append(FailShards, &proto.MissedShard{Vuid: vuid, Bid: bid})
	}
	return FailShards
}

func TestTaskTimeout(t *testing.T) {
	task := inspectTaskInfo{}
	require.NoError(t, task.tryAcquire())
	require.Equal(t, false, task.timeout(5))
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, true, task.timeout(5))
	require.Equal(t, false, task.timeout(10000))
}

func TestBadShardDeduplicator(t *testing.T) {
	d := newBadShardDeduplicator(3)
	require.Equal(t, false, d.reduplicate(1, 1, []uint8{1, 2}))

	d.add(1, 1, []uint8{1, 2})
	require.Equal(t, true, d.reduplicate(1, 1, []uint8{1, 2}))

	require.Equal(t, false, d.reduplicate(2, 1, []uint8{1, 2}))
	d.add(2, 1, []uint8{1, 2})

	require.Equal(t, false, d.reduplicate(2, 1, []uint8{1, 2, 3}))
	d.add(2, 1, []uint8{1, 2, 3})

	d.add(2, 2, []uint8{1, 2, 3})
	require.Equal(t, false, d.reduplicate(2, 1, []uint8{1, 2, 3}))
}

func newInspector(t *testing.T) *VolumeInspectMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	taskSwitch := mocks.NewMockSwitcher(ctr)
	inspectTable := NewMockInspectCheckPointTable(ctr)
	shardRepairSender := NewMockMqProxyAPI(ctr)
	conf := &VolumeInspectMgrCfg{InspectIntervalS: defaultInspectIntervalS, TimeoutMs: 1}
	return NewVolumeInspectMgr(inspectTable, clusterMgr, shardRepairSender, taskSwitch, conf)
}

func TestInspectorRun(t *testing.T) {
	mgr := newInspector(t)

	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().WaitEnable().AnyTimes().Return()
	mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().AnyTimes().Return(true)
	mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(nil, errMock)
	mgr.tbl.(*MockInspectCheckPointTable).EXPECT().SaveCheckPoint(any, any).AnyTimes().Return(errMock)

	require.True(t, mgr.Enabled())
	go mgr.Run()

	time.Sleep(defaultInspectIntervalS * time.Second)
	mgr.Close()
}

func TestInspectorPrepare(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newInspector(t)
		mgr.cfg.InspectBatch = 2
		mgr.cfg.ListVolStep = 2

		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return(nil, proto.Vid(0), nil)
		mgr.prepare(ctx)
	}
	{
		mgr := newInspector(t)
		mgr.cfg.InspectBatch = 2
		mgr.cfg.ListVolStep = 2

		volume1 := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		volume2 := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusActive)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume1}, proto.Vid(0), nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume2}, proto.Vid(0), nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return(nil, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))
	}
	{
		mgr := newInspector(t)
		mgr.firstPrepare = false
		mgr.cfg.InspectBatch = 2
		mgr.cfg.ListVolStep = 2

		volume1 := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		volume2 := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusActive)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume1}, proto.Vid(0), nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume2}, proto.Vid(0), nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return(nil, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))
	}
}

func TestInspectorWaitCompleted(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newInspector(t)

		mgr.cfg.InspectBatch = 1
		mgr.cfg.ListVolStep = 1

		volume := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume}, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))

		for _, task := range mgr.tasks {
			task.ret = &proto.InspectRet{}
		}
		mgr.waitCompleted(ctx)
	}
}

func TestInspectorFinish(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newInspector(t)

		mgr.cfg.InspectBatch = 1
		mgr.cfg.ListVolStep = 1

		volume := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume}, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))

		for _, task := range mgr.tasks {
			task.ret = &proto.InspectRet{MissedShards: genMockFailShards(100012, []proto.BlobID{3, 4})}
		}
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().SaveCheckPoint(any, any).Return(nil)
		mgr.finish(ctx)
		require.Equal(t, 0, len(mgr.tasks))
	}
	{
		mgr := newInspector(t)

		mgr.cfg.InspectBatch = 1
		mgr.cfg.ListVolStep = 1

		volume := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume}, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))

		for _, task := range mgr.tasks {
			task.ret = &proto.InspectRet{MissedShards: genMockFailShards(100012, []proto.BlobID{3, 4})}
		}
		volume = MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusActive)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().SaveCheckPoint(any, any).Return(nil)
		mgr.finish(ctx)
		require.Equal(t, 0, len(mgr.tasks))
	}
	{
		mgr := newInspector(t)

		mgr.cfg.InspectBatch = 1
		mgr.cfg.ListVolStep = 1

		volume := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume}, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))

		for _, task := range mgr.tasks {
			task.ret = &proto.InspectRet{MissedShards: genMockFailShards(100012, []proto.BlobID{3, 4})}
		}
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.repairShardSender.(*MockMqProxyAPI).EXPECT().SendShardRepairMsg(any, any, any, any).Return(errMock)
		mgr.repairShardSender.(*MockMqProxyAPI).EXPECT().SendShardRepairMsg(any, any, any, any).AnyTimes().Return(nil)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().SaveCheckPoint(any, any).Return(nil)

		mgr.finish(ctx)
		require.Equal(t, 0, len(mgr.tasks))
	}
}

func TestInspectorAcquire(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newInspector(t)
		mgr.enableAcquire(false)
		_, err := mgr.AcquireInspect(ctx)
		require.True(t, errors.Is(err, errForbiddenAcquire))
	}
	{
		mgr := newInspector(t)
		mgr.enableAcquire(true)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(false)
		_, err := mgr.AcquireInspect(ctx)
		require.True(t, errors.Is(err, proto.ErrTaskPaused))
	}
	{
		mgr := newInspector(t)
		mgr.enableAcquire(true)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)
		_, err := mgr.AcquireInspect(ctx)
		require.True(t, errors.Is(err, proto.ErrTaskEmpty))
	}
	{
		mgr := newInspector(t)
		mgr.enableAcquire(true)
		mgr.taskSwitch.(*mocks.MockSwitcher).EXPECT().Enabled().Return(true)

		mgr.cfg.InspectBatch = 1
		mgr.cfg.ListVolStep = 1

		volume := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume}, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))

		var taskID string
		for k := range mgr.tasks {
			taskID = k
		}

		task, err := mgr.AcquireInspect(ctx)
		require.NoError(t, err)
		require.Equal(t, mgr.tasks[taskID].t.TaskId, task.TaskId)
	}
}

func TestInspectorComplete(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newInspector(t)
		mgr.enableAcquire(false)

		mgr.CompleteInspect(ctx, &proto.InspectRet{})
	}
	{
		mgr := newInspector(t)
		mgr.enableAcquire(true)

		mgr.CompleteInspect(ctx, &proto.InspectRet{})
	}
	{
		mgr := newInspector(t)
		mgr.enableAcquire(true)

		mgr.cfg.InspectBatch = 1
		mgr.cfg.ListVolStep = 1

		volume := MockGenVolInfo(100012, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.tbl.(*MockInspectCheckPointTable).EXPECT().GetCheckPoint(any).AnyTimes().Return(&proto.InspectCheckPoint{}, nil)
		mgr.volsGetter.(*MockClusterMgrAPI).EXPECT().ListVolume(any, any, any).Return([]*client.VolumeInfoSimple{volume}, proto.Vid(0), nil)

		mgr.prepare(ctx)
		require.Equal(t, 1, len(mgr.tasks))

		var taskID string
		for k := range mgr.tasks {
			taskID = k
		}

		mgr.CompleteInspect(ctx, &proto.InspectRet{TaskID: taskID})
	}
}

func TestInspectorGetTaskStats(t *testing.T) {
	mgr := newInspector(t)
	mgr.GetTaskStats()
}
