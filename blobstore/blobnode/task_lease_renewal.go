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
	aliveTasks := tr.tm.GetAliveTasks()
	if len(aliveTasks) == 0 {
		return
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "renewalTask")
	ret, err := tr.cli.RenewalTask(ctx, &api.TaskRenewalArgs{IDC: tr.idc, IDs: aliveTasks})
	if err != nil {
		span.Errorf("renewal task failed and stop all runner: err[%+v]", err)
		tr.tm.StopAllAliveRunner()
	} else {
		tr.stopRenewalFailTask(ctx, ret)
	}
}

func (tr *TaskRenter) stopRenewalFailTask(ctx context.Context, ret *api.TaskRenewalRet) {
	span := trace.SpanFromContextSafe(ctx)
	for typ, errors := range ret.Errors {
		for taskID, errMsg := range errors {
			span.Warnf("renewal fail to stop runner: type[%s], taskID[%s], error[%s]", typ, taskID, errMsg)
			if err := tr.tm.StopTaskRunner(taskID, typ); err != nil {
				span.Errorf("stop task runner failed: type[%s], taskID[%s], err[%+v]", typ, taskID, err)
			}
		}
	}
}
