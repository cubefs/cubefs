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

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

//duties:manage inspect volume task executing

// IBidGetter define the interface of blobnode used by inspect
type IBidGetter interface {
	ListShards(ctx context.Context, location proto.VunitLocation) (shards []*client.ShardInfo, err error)
}

// IResultReporter define the interface of scheduler used by inspect
type IResultReporter interface {
	CompleteInspect(ctx context.Context, args *api.CompleteInspectArgs) (err error)
}

// InspectTaskMgr inspect task manager
type InspectTaskMgr struct {
	taskLimit limit.Limiter
	bidGetter IBidGetter
	reporter  IResultReporter
}

// NewInspectTaskMgr returns inspect task manager
func NewInspectTaskMgr(inspectConcurrency int, bidGetter IBidGetter, report IResultReporter) *InspectTaskMgr {
	return &InspectTaskMgr{
		taskLimit: count.New(inspectConcurrency),
		bidGetter: bidGetter,
		reporter:  report,
	}
}

// AddTask adds inspect task
func (mgr *InspectTaskMgr) AddTask(ctx context.Context, task *proto.VolumeInspectTask) error {
	span := trace.SpanFromContextSafe(ctx)

	err := mgr.taskLimit.Acquire()
	if err != nil {
		return err
	}

	go func() {
		defer mgr.taskLimit.Release()
		ret := mgr.doInspect(ctx, task)
		mgr.reportInspectResult(ctx, ret)
		span.Debugf("finish inspect: taskID[%s]", task.TaskId)
	}()
	return nil
}

// RunningTaskSize returns running inspect task size
func (mgr *InspectTaskMgr) RunningTaskSize() int {
	return mgr.taskLimit.Running()
}

func (mgr *InspectTaskMgr) doInspect(ctx context.Context, task *proto.VolumeInspectTask) *proto.VolumeInspectRet {
	span := trace.SpanFromContextSafe(ctx)

	mode := task.Mode
	replicas := task.Replicas
	ret := proto.VolumeInspectRet{TaskID: task.TaskId}

	if len(replicas) != workutils.AllReplCnt(mode) {
		span.Errorf("replicas length is invalid: taskID[%s], mode[%d], expect len[%d], actual len[%d]",
			task.TaskId,
			mode,
			workutils.AllReplCnt(mode),
			len(replicas))
		ret.InspectErrStr = "unexpect:code mode not match"
		return &ret
	}

	replicasBids := GetReplicasBids(ctx, mgr.bidGetter, replicas)
	for vuid, replBids := range replicasBids {
		if replBids.RetErr != nil {
			span.Errorf("get replicas bids failed: vuid[%d], err[%+v]", vuid, replBids.RetErr)
			ret.InspectErrStr = replBids.RetErr.Error()
			return &ret
		}
	}

	allBids := MergeBids(replicasBids)

	var allBlobMissed []*proto.MissedShard
	for _, bid := range allBids {
		markDel := false
		existStatus := workutils.NewBidExistStatus(mode)
		var oneBlobMissed []*proto.MissedShard
		// check stripe replicas
		for vuid, replBids := range replicasBids {
			info, ok := replBids.Bids[bid.Bid]
			if !ok {
				oneBlobMissed = append(oneBlobMissed, &proto.MissedShard{Vuid: vuid, Bid: bid.Bid})
				continue
			}
			if info.MarkDeleted() {
				markDel = true
				break
			}
			existStatus.Exist(vuid.Index())
		} // end

		if markDel {
			continue
		}

		if existStatus.ExistCnt() == 0 || existStatus.ExistCnt() == workutils.AllReplCnt(mode) {
			continue
		}

		if existStatus.CanRecover() {
			allBlobMissed = append(allBlobMissed, oneBlobMissed...)
			continue
		} else {
			span.Warnf("blob may be lost: vid[%d], bid[%d]", replicas[0].Vuid.Vid(), bid.Bid)
		}
	}
	ret.MissedShards = allBlobMissed
	return &ret
}

func (mgr *InspectTaskMgr) reportInspectResult(ctx context.Context, inspectRet *proto.VolumeInspectRet) {
	span := trace.SpanFromContextSafe(ctx)

	args := api.CompleteInspectArgs{VolumeInspectRet: inspectRet}
	err := mgr.reporter.CompleteInspect(ctx, &args)
	if err != nil {
		span.Errorf("report inspect result failed: taskID[%s], err[%+v]", inspectRet.TaskID, err)
	}
}
