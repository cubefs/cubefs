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

package blobnode

import (
	"context"
	"time"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// duties：task lease with scheduler
// lease success return success
// lease fail stop task and return fail

// TaskRenewalCli define the interface of scheduler used for task renewal
type TaskRenewalCli interface {
	RenewalTask(ctx context.Context, tasks *api.TaskRenewalArgs) (ret *api.TaskRenewalRet, err error)
}

// TaskRenter used to renter task
type TaskRenter struct {
	idc string
	cli TaskRenewalCli
	tm  *TaskRunnerMgr
}

// NewTaskRenter returns task renter
func NewTaskRenter(idc string, cli TaskRenewalCli, tm *TaskRunnerMgr) *TaskRenter {
	return &TaskRenter{
		idc: idc,
		cli: cli,
		tm:  tm,
	}
}

// RenewalTaskLoop renewal task
func (tr *TaskRenter) RenewalTaskLoop() {
	for {
		tr.renewalTask()
		time.Sleep(proto.TaskRenewalPeriodS * time.Second)
	}
}

func (tr *TaskRenter) renewalTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "renewalTask")

	alive := api.TaskRenewalArgs{
		IDC:           tr.idc,
		Repair:        genRenewalArgs(tr.tm.GetRepairAliveTask()),
		Balance:       genRenewalArgs(tr.tm.GetBalanceAliveTask()),
		DiskDrop:      genRenewalArgs(tr.tm.GetDiskDropAliveTask()),
		ManualMigrate: genRenewalArgs(tr.tm.GetManualMigrateAliveTask()),
	}

	ret, err := tr.cli.RenewalTask(ctx, &alive)
	if err != nil {
		span.Errorf("renewal task failed and stop all runner: err[%+v]", err)
		tr.tm.StopAllAliveRunner()
	} else {
		tr.stopRenewalFailTask(ctx, ret)
	}
}

func (tr *TaskRenter) stopRenewalFailTask(ctx context.Context, ret *api.TaskRenewalRet) {
	span := trace.SpanFromContextSafe(ctx)
	for taskID, errMsg := range ret.Repair {
		if len(errMsg) != 0 {
			span.Infof("renewal fail should stop: taskID[%s], type[%s]", taskID, proto.RepairTaskType)
			err := tr.tm.StopTaskRunner(taskID, proto.RepairTaskType)
			if err != nil {
				span.Errorf("stop task runner failed: taskID[%s], taskType[%s], err[%+v]", taskID, proto.RepairTaskType, err)
			}
		}
	}

	for taskID, errMsg := range ret.Balance {
		if len(errMsg) != 0 {
			span.Infof("renewal fail should stop: taskID[%s], type[%s]", taskID, proto.BalanceTaskType)
			err := tr.tm.StopTaskRunner(taskID, proto.BalanceTaskType)
			if err != nil {
				span.Errorf("stop task runner failed: taskID[%s], taskType[%s], err[%+v]", taskID, proto.BalanceTaskType, err)
			}
		}
	}

	for taskID, errMsg := range ret.DiskDrop {
		if len(errMsg) != 0 {
			span.Infof("renewal fail should stop: taskID[%s], type[%s]", taskID, proto.DiskDropTaskType)
			err := tr.tm.StopTaskRunner(taskID, proto.DiskDropTaskType)
			if err != nil {
				span.Errorf("stop task runner failed: taskID[%s], taskType[%s], err[%+v]", taskID, proto.DiskDropTaskType, err)
			}
		}
	}

	for taskID, errMsg := range ret.ManualMigrate {
		if len(errMsg) != 0 {
			span.Infof("renewal fail should stop: taskID[%s], type[%s]", taskID, proto.ManualMigrateType)
			err := tr.tm.StopTaskRunner(taskID, proto.ManualMigrateType)
			if err != nil {
				span.Errorf("stop task runner failed: taskID[%s], taskType[%s], err[%+v]", taskID, proto.ManualMigrateType, err)
			}
		}
	}
}

func genRenewalArgs(runners []*TaskRunner) map[string]struct{} {
	m := make(map[string]struct{})
	for _, r := range runners {
		m[r.taskID] = struct{}{}
	}
	return m
}
