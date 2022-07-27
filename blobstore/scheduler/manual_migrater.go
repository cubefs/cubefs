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

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/scheduler/db"
)

// ManualMigrateMgr manual migrate manager
type ManualMigrateMgr struct {
	IMigrator

	clusterMgrCli client.ClusterMgrAPI
}

// NewManualMigrateMgr returns manual migrate manager
func NewManualMigrateMgr(clusterMgrCli client.ClusterMgrAPI, volumeUpdater client.IVolumeUpdater,
	taskTbl db.IMigrateTaskTable, conf *MigrateConfig) *ManualMigrateMgr {
	mgr := &ManualMigrateMgr{
		clusterMgrCli: clusterMgrCli,
	}
	mgr.IMigrator = NewMigrateMgr(clusterMgrCli, volumeUpdater, taskswitch.NewEnabledTaskSwitch(),
		taskTbl, conf, proto.TaskTypeManualMigrate)
	mgr.IMigrator.SetLockFailHandleFunc(mgr.IMigrator.FinishTaskInAdvanceWhenLockFail)
	return mgr
}

// AddManualTask add manual migrate task
func (mgr *ManualMigrateMgr) AddManualTask(ctx context.Context, vuid proto.Vuid, forbiddenDirectDownload bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	volume, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, vuid.Vid())
	if err != nil {
		span.Errorf("get volume failed: vid[%d], err[%+v]", vuid.Vid(), err)
		return err
	}
	diskID := volume.VunitLocations[vuid.Index()].DiskID
	disk, err := mgr.clusterMgrCli.GetDiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info failed:  disk_id[%d], err[%+v]", err)
		return err
	}

	task := &proto.MigrateTask{
		TaskID:                  base.GenTaskID("manual_migrate", vuid.Vid()),
		TaskType:                proto.TaskTypeManualMigrate,
		State:                   proto.MigrateStateInited,
		SourceIDC:               disk.Idc,
		SourceDiskID:            disk.DiskID,
		SourceVuid:              vuid,
		ForbiddenDirectDownload: forbiddenDirectDownload,
	}
	mgr.IMigrator.AddTask(ctx, task)

	span.Debugf("add manual migrate task success: task_info[%+v]", task)
	return nil
}
