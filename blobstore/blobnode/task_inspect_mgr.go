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

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

// InspectTaskMgr inspect task manager
type InspectTaskMgr struct {
	taskLimit limit.Limiter
	bidGetter client.IBlobNode
	reporter  scheduler.IInspector
}

// NewInspectTaskMgr returns inspect task manager
func NewInspectTaskMgr(concurrency int, bidGetter client.IBlobNode, reporter scheduler.IInspector) *InspectTaskMgr {
	return &InspectTaskMgr{
		taskLimit: count.New(concurrency),
		bidGetter: bidGetter,
		reporter:  reporter,
	}
}

// AddTask adds inspect task
func (mgr *InspectTaskMgr) AddTask(ctx context.Context, task *proto.VolumeInspectTask) error {
	span := trace.SpanFromContextSafe(ctx)
	if err := mgr.taskLimit.Acquire(); err != nil {
		return err
	}

	go func() {
		defer mgr.taskLimit.Release()
		ret := mgr.doInspect(ctx, task)
		if err := mgr.reporter.CompleteInspectTask(ctx, ret); err != nil {
			span.Errorf("report inspect result failed: result[%+v], err[%+v]", ret, err)
		}
		span.Debugf("finish inspect: taskID[%s]", task.TaskID)
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
	ret := &proto.VolumeInspectRet{TaskID: task.TaskID}

	if len(replicas) != mode.GetShardNum() {
		ret.InspectErrStr = "unexpect:code mode not match"
		return ret
	}

	replicasBids := GetReplicasBids(ctx, mgr.bidGetter, replicas)
	for vuid, replBids := range replicasBids {
		if replBids.RetErr != nil {
			span.Errorf("get replicas bids failed: vuid[%d], err[%+v]", vuid, replBids.RetErr)
			ret.InspectErrStr = replBids.RetErr.Error()
			return ret
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

		if existStatus.ExistCnt() == 0 || existStatus.ExistCnt() == mode.GetShardNum() {
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
	return ret
}
