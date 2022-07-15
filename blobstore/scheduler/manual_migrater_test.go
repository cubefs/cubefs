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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

func newManualMigrater(t *testing.T) *ManualMigrateMgr {
	ctr := gomock.NewController(t)
	clusterMgr := NewMockClusterMgrAPI(ctr)
	volumeUpdater := NewMockVolumeUpdater(ctr)
	migrateTable := NewMockMigrateTaskTable(ctr)
	migrater := NewMockMigrater(ctr)
	mgr := NewManualMigrateMgr(clusterMgr, volumeUpdater, migrateTable, proto.ClusterID(1))
	mgr.IMigrator = migrater
	return mgr
}

func TestManualMigrateLoad(t *testing.T) {
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().Load().Return(nil)
	err := mgr.Load()
	require.NoError(t, err)
}

func TestManualMigrateRun(t *testing.T) {
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().Run().Return()
	mgr.Run()
}

func TestManualMigrateAddTask(t *testing.T) {
	ctx := context.Background()
	{
		mgr := newManualMigrater(t)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)
		err := mgr.AddManualTask(ctx, proto.Vuid(1), false)
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newManualMigrater(t)
		volume := MockGenVolInfo(10001, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(nil, errMock)
		err := mgr.AddManualTask(ctx, proto.Vuid(1), false)
		require.True(t, errors.Is(err, errMock))
	}
	{
		mgr := newManualMigrater(t)
		volume := MockGenVolInfo(10001, codemode.EC6P6, proto.VolumeStatusIdle)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetVolumeInfo(any, any).Return(volume, nil)
		mgr.clusterMgrCli.(*MockClusterMgrAPI).EXPECT().GetDiskInfo(any, any).Return(&client.DiskInfoSimple{}, nil)
		mgr.IMigrator.(*MockMigrater).EXPECT().AddTask(any, any).Return()
		err := mgr.AddManualTask(ctx, proto.Vuid(1), false)
		require.NoError(t, err)
	}
}

func TestManualMigrateAcquireTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().AcquireTask(any, any).Return(proto.MigrateTask{TaskType: proto.TaskTypeManualMigrate}, nil)
	_, err := mgr.AcquireTask(ctx, idc)
	require.NoError(t, err)
}

func TestManualMigrateCancelTask(t *testing.T) {
	ctx := context.Background()
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CancelTask(any, any).Return(nil)
	err := mgr.CancelTask(ctx, &api.CancelTaskArgs{})
	require.NoError(t, err)
}

func TestManualMigrateReclaimTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().ReclaimTask(any, any, any, any, any, any).Return(nil)
	t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.ReclaimTask(ctx, idc, t1.TaskID, t1.Sources, t1.Destination, &client.AllocVunitInfo{})
	require.NoError(t, err)
}

func TestManualMigrateCompleteTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(nil)
	t1 := mockGenMigrateTask(idc, 4, 100, proto.MigrateStatePrepared, MockMigrateVolInfoMap)
	err := mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().CompleteTask(any, any).Return(errMock)
	err = mgr.CompleteTask(ctx, &api.CompleteTaskArgs{IDC: idc, TaskId: t1.TaskID, Src: t1.Sources, Dest: t1.Destination})
	require.True(t, errors.Is(err, errMock))
}

func TestManualMigrateRenewalTask(t *testing.T) {
	ctx := context.Background()
	idc := "z0"
	mgr := newManualMigrater(t)
	mgr.IMigrator.(*MockMigrater).EXPECT().RenewalTask(any, any, any).Return(nil)
	err := mgr.RenewalTask(ctx, idc, "")
	require.NoError(t, err)

	mgr.IMigrator.(*MockMigrater).EXPECT().RenewalTask(any, any, any).Return(errMock)
	err = mgr.RenewalTask(ctx, idc, "")
	require.True(t, errors.Is(err, errMock))
}
