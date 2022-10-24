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
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

var (
	schedulerServer *httptest.Server
	once            sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		schedulerServer = httptest.NewServer(NewHandler(s))
	})
	return schedulerServer.URL
}

func newMockService(t *testing.T) *Service {
	ctr := gomock.NewController(t)

	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	blobDeleteMgr := NewMockTaskRunner(ctr)
	shardRepairMgr := NewMockTaskRunner(ctr)
	diskDropMgr := NewMockMigrater(ctr)
	diskRepairMgr := NewMockMigrater(ctr)
	manualMgr := NewMockMigrater(ctr)
	balanceMgr := NewMockMigrater(ctr)
	inspectorMgr := NewMockVolumeInspector(ctr)
	clusterTopology := NewMockClusterTopology(ctr)

	// return disk repair task
	diskRepairMgr.EXPECT().AcquireTask(any, any).Return(proto.MigrateTask{TaskType: proto.TaskTypeDiskRepair}, nil)

	// reclaim repair task
	diskRepairMgr.EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	clusterMgrCli.EXPECT().AllocVolumeUnit(any, any).Return(&client.AllocVunitInfo{}, nil)
	// reclaim balance task
	balanceMgr.EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	clusterMgrCli.EXPECT().AllocVolumeUnit(any, any).Return(&client.AllocVunitInfo{}, nil)
	// reclaim disk drop task
	diskDropMgr.EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	clusterMgrCli.EXPECT().AllocVolumeUnit(any, any).Return(&client.AllocVunitInfo{}, nil)
	// reclaim manual migrate task
	manualMgr.EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	clusterMgrCli.EXPECT().AllocVolumeUnit(any, any).Return(&client.AllocVunitInfo{}, nil)

	// cancel repair task
	diskRepairMgr.EXPECT().CancelTask(any, any).Return(nil)
	// cancel balance task
	balanceMgr.EXPECT().CancelTask(any, any).Return(nil)
	// cancel  disk drop task
	diskDropMgr.EXPECT().CancelTask(any, any).Return(nil)
	// cancel manual migrate task
	manualMgr.EXPECT().CancelTask(any, any).Return(nil)

	// complete repair task
	diskRepairMgr.EXPECT().CompleteTask(any, any).Return(nil)
	// complete balance task
	balanceMgr.EXPECT().CompleteTask(any, any).Return(nil)
	// complete  disk drop task
	diskDropMgr.EXPECT().CompleteTask(any, any).Return(nil)
	// complete manual migrate task
	manualMgr.EXPECT().CompleteTask(any, any).Return(nil)

	// renewal repair task
	diskRepairMgr.EXPECT().RenewalTask(any, any, any).Times(3).Return(nil)
	// renewal balance task
	balanceMgr.EXPECT().RenewalTask(any, any, any).Times(3).Return(nil)
	// renewal  disk drop task
	diskDropMgr.EXPECT().RenewalTask(any, any, any).Times(3).Return(nil)
	// renewal manual migrate task
	manualMgr.EXPECT().RenewalTask(any, any, any).Times(3).Return(nil)

	// report repair task
	diskRepairMgr.EXPECT().ReportWorkerTaskStats(any).Return()
	// report balance task
	balanceMgr.EXPECT().ReportWorkerTaskStats(any).Return()
	// report  disk drop task
	diskDropMgr.EXPECT().ReportWorkerTaskStats(any).Return()
	// report manual migrate task
	manualMgr.EXPECT().ReportWorkerTaskStats(any).Return()

	// add manual migrate task
	manualMgr.EXPECT().AddManualTask(any, any, any).Return(nil)

	// acquire inspect task
	inspectorMgr.EXPECT().AcquireInspect(any).Return(&proto.VolumeInspectTask{}, nil)

	// complete inspect task
	inspectorMgr.EXPECT().CompleteInspect(any, any).Return()

	// volume update
	clusterTopology.EXPECT().UpdateVolume(any).Return(&client.VolumeInfoSimple{}, nil)
	clusterTopology.EXPECT().UpdateVolume(any).Return(nil, errMock)

	// stats
	blobDeleteMgr.EXPECT().GetErrorStats().Return([]string{}, uint64(0))
	blobDeleteMgr.EXPECT().GetTaskStats().Return([counter.SLOT]int{}, [counter.SLOT]int{})
	blobDeleteMgr.EXPECT().Enabled().Return(true)
	shardRepairMgr.EXPECT().GetErrorStats().Return([]string{}, uint64(0))
	shardRepairMgr.EXPECT().GetTaskStats().Return([counter.SLOT]int{}, [counter.SLOT]int{})
	shardRepairMgr.EXPECT().Enabled().Return(true)
	diskRepairMgr.EXPECT().Stats().Return(api.MigrateTasksStat{})
	diskRepairMgr.EXPECT().Progress(any).Return([]proto.DiskID{proto.DiskID(1)}, 0, 0)
	diskRepairMgr.EXPECT().Enabled().Return(true)
	diskDropMgr.EXPECT().Stats().Return(api.MigrateTasksStat{})
	diskDropMgr.EXPECT().Progress(any).Return([]proto.DiskID{proto.DiskID(1)}, 0, 0)
	diskDropMgr.EXPECT().Enabled().Return(true)
	balanceMgr.EXPECT().Stats().Return(api.MigrateTasksStat{})
	balanceMgr.EXPECT().Enabled().Return(true)
	manualMgr.EXPECT().Stats().Return(api.MigrateTasksStat{})
	inspectorMgr.EXPECT().GetTaskStats().Return([counter.SLOT]int{}, [counter.SLOT]int{})
	inspectorMgr.EXPECT().Enabled().Return(true)

	// task detail
	balanceMgr.EXPECT().QueryTask(any, any).Return(nil, nil)
	diskDropMgr.EXPECT().QueryTask(any, any).Return(nil, nil)
	diskRepairMgr.EXPECT().QueryTask(any, any).Return(nil, nil)
	manualMgr.EXPECT().QueryTask(any, any).Return(nil, nil)
	balanceMgr.EXPECT().QueryTask(any, any).Return(nil, errMock)
	diskDropMgr.EXPECT().QueryTask(any, any).Return(nil, errMock)
	diskRepairMgr.EXPECT().QueryTask(any, any).Return(nil, errMock)
	manualMgr.EXPECT().QueryTask(any, any).Return(nil, errMock)

	// disk stats
	diskRepairMgr.EXPECT().DiskProgress(any, any).Return(nil, errMock)
	diskDropMgr.EXPECT().DiskProgress(any, any).Return(nil, errMock)
	diskRepairMgr.EXPECT().DiskProgress(any, any).Return(&api.DiskMigratingStats{TotalTasksCnt: int(testDisk1.UsedChunkCnt), MigratedTasksCnt: 1}, nil)
	diskDropMgr.EXPECT().DiskProgress(any, any).Return(&api.DiskMigratingStats{TotalTasksCnt: int(testDisk1.UsedChunkCnt), MigratedTasksCnt: 1}, nil)

	service := &Service{
		ClusterID:     1,
		leader:        true,
		leaderHost:    localHost + ":9800",
		balanceMgr:    balanceMgr,
		diskDropMgr:   diskDropMgr,
		manualMigMgr:  manualMgr,
		diskRepairMgr: diskRepairMgr,
		inspectMgr:    inspectorMgr,

		shardRepairMgr:  shardRepairMgr,
		blobDeleteMgr:   blobDeleteMgr,
		clusterTopology: clusterTopology,

		clusterMgrCli: clusterMgrCli,
	}
	return service
}

func TestServiceAPI(t *testing.T) {
	runMockService(newMockService(t))
	ctr := gomock.NewController(t)
	clusterMgrCli := mocks.NewMockClientAPI(ctr)
	clusterMgrCli.EXPECT().GetService(any, any).AnyTimes().Return(
		cmapi.ServiceInfo{Nodes: []cmapi.ServiceNode{{ClusterID: 1, Host: schedulerServer.URL}}}, nil)

	ctx := context.Background()
	cli := api.New(&api.Config{}, clusterMgrCli, proto.ClusterID(1))
	idc := "z0"
	diskID := proto.DiskID(1)
	volumeID := proto.Vid(1)
	taskTypes := []proto.TaskType{
		proto.TaskTypeBalance, proto.TaskTypeDiskDrop,
		proto.TaskTypeDiskRepair, proto.TaskTypeManualMigrate,
	}
	// acquire task
	task, err := cli.AcquireTask(ctx, &api.AcquireArgs{IDC: idc})
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeDiskRepair, task.TaskType)

	for _, taskType := range taskTypes {
		require.NoError(t, cli.ReclaimTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskType: taskType, TaskID: client.GenMigrateTaskID(taskType, diskID, volumeID)}))
		require.NoError(t, cli.CancelTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskType: taskType, TaskID: client.GenMigrateTaskID(taskType, diskID, volumeID)}))
		require.NoError(t, cli.CompleteTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskType: taskType, TaskID: client.GenMigrateTaskID(taskType, diskID, volumeID)}))
		require.NoError(t, cli.ReportTask(ctx, &api.TaskReportArgs{TaskType: taskType, TaskID: client.GenMigrateTaskID(taskType, diskID, volumeID)}))
	}

	require.Error(t, cli.ReclaimTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskType: "task"}))
	require.Error(t, cli.CancelTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskType: "task"}))
	require.Error(t, cli.CompleteTask(ctx, &api.OperateTaskArgs{IDC: idc, TaskType: "task"}))

	// renewal task
	_, err = cli.RenewalTask(ctx, &api.TaskRenewalArgs{
		IDC: "z0",
		IDs: map[proto.TaskType][]string{
			proto.TaskTypeBalance: {
				client.GenMigrateTaskPrefix(proto.TaskTypeBalance) + "1",
				client.GenMigrateTaskPrefix(proto.TaskTypeBalance) + "2",
				client.GenMigrateTaskPrefix(proto.TaskTypeBalance) + "3",
			},
			proto.TaskTypeDiskRepair: {
				client.GenMigrateTaskPrefix(proto.TaskTypeDiskRepair) + "1",
				client.GenMigrateTaskPrefix(proto.TaskTypeDiskRepair) + "2",
				client.GenMigrateTaskPrefix(proto.TaskTypeDiskRepair) + "3",
			},
			proto.TaskTypeDiskDrop: {
				client.GenMigrateTaskPrefix(proto.TaskTypeDiskDrop) + "1",
				client.GenMigrateTaskPrefix(proto.TaskTypeDiskDrop) + "2",
				client.GenMigrateTaskPrefix(proto.TaskTypeDiskDrop) + "3",
			},
			proto.TaskTypeManualMigrate: {
				client.GenMigrateTaskPrefix(proto.TaskTypeManualMigrate) + "1",
				client.GenMigrateTaskPrefix(proto.TaskTypeManualMigrate) + "2",
				client.GenMigrateTaskPrefix(proto.TaskTypeManualMigrate) + "3",
			},
		},
	})
	require.NoError(t, err)

	// add manual migrate task
	err = cli.AddManualMigrateTask(ctx, &api.AddManualMigrateArgs{})
	require.Equal(t, 400, rpc.DetectStatusCode(err))
	err = cli.AddManualMigrateTask(ctx, &api.AddManualMigrateArgs{Vuid: proto.Vuid(24726512599042)})
	require.NoError(t, err)

	// acquire inspect task
	_, err = cli.AcquireInspectTask(ctx)
	require.NoError(t, err)
	// complete inspect task
	require.NoError(t, cli.CompleteInspectTask(ctx, &proto.VolumeInspectRet{}))

	// volume update
	require.NoError(t, cli.UpdateVolume(ctx, schedulerServer.URL, proto.Vid(1)))
	require.Error(t, cli.UpdateVolume(ctx, schedulerServer.URL, proto.Vid(1)))

	// stats
	_, err = cli.Stats(ctx, schedulerServer.URL)
	require.NoError(t, err)

	// task detail
	{
		_, err = cli.DetailMigrateTask(ctx, nil)
		require.Error(t, err)
		_, err = cli.DetailMigrateTask(ctx, &api.MigrateTaskDetailArgs{})
		require.Error(t, err)
		_, err = cli.DetailMigrateTask(ctx, &api.MigrateTaskDetailArgs{Type: "xxxxx", ID: "task_id"})
		require.Error(t, err)
		_, err = cli.DetailMigrateTask(ctx, &api.MigrateTaskDetailArgs{Type: proto.TaskTypeBalance, ID: ""})
		require.Error(t, err)
		_, err = cli.DetailMigrateTask(ctx, &api.MigrateTaskDetailArgs{Type: proto.TaskTypeBalance, ID: "disk_repair"})
		require.Error(t, err)
	}
	for _, taskType := range taskTypes {
		_, err = cli.DetailMigrateTask(ctx, &api.MigrateTaskDetailArgs{Type: taskType, ID: client.GenMigrateTaskID(taskType, diskID, volumeID)})
		require.NoError(t, err)
		_, err = cli.DetailMigrateTask(ctx, &api.MigrateTaskDetailArgs{Type: taskType, ID: client.GenMigrateTaskID(taskType, diskID, volumeID)})
		require.Error(t, err)
	}
	// disk migrating stats
	diskMigrateTypes := []proto.TaskType{proto.TaskTypeDiskRepair, proto.TaskTypeDiskDrop}
	for _, taskType := range diskMigrateTypes {
		_, err = cli.DiskMigratingStats(ctx, &api.DiskMigratingStatsArgs{DiskID: testDisk1.DiskID, TaskType: taskType})
		require.Error(t, err)
		stats, err := cli.DiskMigratingStats(ctx, &api.DiskMigratingStatsArgs{DiskID: testDisk1.DiskID, TaskType: taskType})
		require.NoError(t, err)
		require.Equal(t, int(testDisk1.UsedChunkCnt), stats.TotalTasksCnt)
		require.Equal(t, 1, stats.MigratedTasksCnt)
	}
}
