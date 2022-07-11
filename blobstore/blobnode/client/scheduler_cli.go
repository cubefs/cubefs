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

import (
	"context"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// IScheduler define the interface of scheduler used for worker
type IScheduler interface {
	AcquireTask(ctx context.Context, args *api.AcquireArgs) (ret *api.WorkerTask, err error)
	AcquireInspectTask(ctx context.Context) (ret *api.WorkerInspectTask, err error)
	ReclaimTask(ctx context.Context, args *api.ReclaimTaskArgs) (err error)
	CancelTask(ctx context.Context, args *api.CancelTaskArgs) (err error)
	CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) (err error)
	CompleteInspect(ctx context.Context, args *api.CompleteInspectArgs) (err error)
	// report alive tasks
	RenewalTask(ctx context.Context, args *api.TaskRenewalArgs) (ret *api.TaskRenewalRet, err error)
	ReportTask(ctx context.Context, args *api.TaskReportArgs) (err error)
}

// SchedulerClient scheduler client
type SchedulerClient struct {
	cli api.IScheduler
}

// NewSchedulerClient returns scheduler client
func NewSchedulerClient(conf *api.Config, service cmapi.APIService, clusterID proto.ClusterID) IScheduler {
	return &SchedulerClient{
		cli: api.New(conf, service, clusterID),
	}
}

// AcquireTask acquire task, such as balance/diskdrop/repair...
func (c *SchedulerClient) AcquireTask(ctx context.Context, args *api.AcquireArgs) (task *api.WorkerTask, err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "AcquireTask", pSpan.TraceID())
	return c.cli.AcquireTask(ctx, args)
}

// AcquireInspectTask acquire inspect task
func (c *SchedulerClient) AcquireInspectTask(ctx context.Context) (task *api.WorkerInspectTask, err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "AcquireInspectTask", pSpan.TraceID())
	return c.cli.AcquireInspectTask(ctx)
}

// ReclaimTask reclaim task
func (c *SchedulerClient) ReclaimTask(ctx context.Context, args *api.ReclaimTaskArgs) (err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "ReclaimTask", pSpan.TraceID())
	return c.cli.ReclaimTask(ctx, args)
}

// CancelTask cancel task
func (c *SchedulerClient) CancelTask(ctx context.Context, args *api.CancelTaskArgs) error {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "CancelTask", pSpan.TraceID())
	return c.cli.CancelTask(ctx, args)
}

// CompleteTask complete task
func (c *SchedulerClient) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) error {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "CompleteTask", pSpan.TraceID())
	return c.cli.CompleteTask(ctx, args)
}

// CompleteInspect complete inspect task
func (c *SchedulerClient) CompleteInspect(ctx context.Context, args *api.CompleteInspectArgs) error {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "CompleteInspect", pSpan.TraceID())
	return c.cli.CompleteInspect(ctx, args)
}

// RenewalTask renewal task
func (c *SchedulerClient) RenewalTask(ctx context.Context, args *api.TaskRenewalArgs) (ret *api.TaskRenewalRet, err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "RenewalTask", pSpan.TraceID())
	for retryCnt := 1; retryCnt <= 3; retryCnt++ {
		ret, err = c.cli.RenewalTask(ctx, args)
		if err == nil {
			return
		}
		time.Sleep(1 * time.Second)
	}
	return
}

// ReportTask report task
func (c *SchedulerClient) ReportTask(ctx context.Context, args *api.TaskReportArgs) (err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "ReportTask", pSpan.TraceID())
	return c.cli.ReportTask(ctx, args)
}
