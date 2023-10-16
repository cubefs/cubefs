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
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// ITaskRunner define the interface of task running status.
type ITaskRunner interface {
	Enabled() bool
	Run()
	Close()
	GetTaskStats() (success, failed [counter.SLOT]int)
	GetErrorStats() (errStats []string, totalErrCnt uint64)
}

type deleteStatus int

// blob delete status
const (
	DeleteStatusDone = deleteStatus(iota)
	DeleteStatusFailed
	DeleteStatusUnexpect
	DeleteStatusUndo
)

// ErrVunitLengthNotEqual vunit length not equal
var ErrVunitLengthNotEqual = errors.New("vunit length not equal")

type delShardRet struct {
	err  error
	vuid proto.Vuid
}

type delBlobRet struct {
	status deleteStatus
	delMsg *proto.DeleteMsg
	ctx    context.Context
	err    error
}

// DelDoc is a delete doc information for logging in dellog
type DelDoc struct {
	ClusterID     proto.ClusterID `json:"cid"`
	Bid           proto.BlobID    `json:"bid"`
	Vid           proto.Vid       `json:"vid"`
	Retry         int             `json:"retry"`
	Time          int64           `json:"t"`
	ReqID         string          `json:"rid"`
	ActualDelTime int64           `json:"del_at"` // unix time in S
}

func toDelDoc(msg proto.DeleteMsg) DelDoc {
	return DelDoc{
		ClusterID:     msg.ClusterID,
		Bid:           msg.Bid,
		Vid:           msg.Vid,
		Retry:         msg.Retry,
		Time:          msg.Time,
		ReqID:         msg.ReqId,
		ActualDelTime: time.Now().Unix(),
	}
}

type HourRange struct {
	From int `json:"from"`
	To   int `json:"to"`
}

func (t HourRange) Valid() bool {
	if t.From > t.To {
		return false
	}
	if t.From < 0 || t.From > 24 || t.To < 0 || t.To > 24 {
		return false
	}
	return true
}

// BlobDeleteConfig is blob delete config
type BlobDeleteConfig struct {
	ClusterID proto.ClusterID

	DeleteLog recordlog.Config `json:"delete_log"`
}

// BlobDeleteMgr is blob delete manager
type BlobDeleteMgr struct {
	closer.Closer
	taskSwitch      *taskswitch.TaskSwitch
	taskPool        *taskpool.TaskPool
	clusterTopology IClusterTopology
	blobnodeCli     client.BlobnodeAPI

	delSuccessCounter      prometheus.Counter
	delSuccessCounterByMin *counter.Counter
	delFailCounter         prometheus.Counter
	delFailCounterByMin    *counter.Counter
	errStatsDistribution   *base.ErrorStats

	// delete log
	delLogger recordlog.Encoder
	cfg       *BlobDeleteConfig
}

// NewBlobDeleteMgr returns blob delete manager
func NewBlobDeleteMgr(
	cfg *BlobDeleteConfig,
	clusterTopology IClusterTopology,
	switchMgr *taskswitch.SwitchMgr,
	blobnodeCli client.BlobnodeAPI,
) (*BlobDeleteMgr, error) {

	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeBlobDelete.String())
	if err != nil {
		return nil, err
	}

	delLogger, err := recordlog.NewEncoder(&cfg.DeleteLog)
	if err != nil {
		return nil, err
	}

	mgr := &BlobDeleteMgr{
		taskSwitch:      taskSwitch,
		clusterTopology: clusterTopology,
		blobnodeCli:     blobnodeCli,

		delSuccessCounter:      base.NewCounter(cfg.ClusterID, "delete", base.KindSuccess),
		delFailCounter:         base.NewCounter(cfg.ClusterID, "delete", base.KindFailed),
		errStatsDistribution:   base.NewErrorStats(),
		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},

		delLogger: delLogger,
		cfg:       cfg,
		Closer:    closer.New(),
	}

	return mgr, nil
}

func (mgr *BlobDeleteMgr) Close() {
	mgr.Closer.Close()
}

func (mgr *BlobDeleteMgr) Run() {}

// Enabled returns return if delete task switch is enable, otherwise returns false
func (mgr *BlobDeleteMgr) Enabled() bool {
	return mgr.taskSwitch.Enabled()
}

// GetTaskStats returns task stats
func (mgr *BlobDeleteMgr) GetTaskStats() (success [counter.SLOT]int, failed [counter.SLOT]int) {
	return mgr.delSuccessCounterByMin.Show(), mgr.delFailCounterByMin.Show()
}

// GetErrorStats returns error stats
func (mgr *BlobDeleteMgr) GetErrorStats() (errStats []string, totalErrCnt uint64) {
	statsResult, totalErrCnt := mgr.errStatsDistribution.Stats()
	return base.FormatPrint(statsResult), totalErrCnt
}

func (mgr *BlobDeleteMgr) Delete(ctx context.Context, msg *proto.DeleteMsg) error {
	span := trace.SpanFromContextSafe(ctx)
	item := &delBlobRet{}
	item.delMsg = msg
	item.ctx = ctx

	defer mgr.recordAllResult(item)

	if mgr.hasBrokenDisk(item.delMsg.Vid) {
		span.Debugf("the volume has broken disk and delete later: vid[%d], bid[%d]", item.delMsg.Vid, item.delMsg.Bid)
		// try to update volume
		mgr.clusterTopology.UpdateVolume(item.delMsg.Vid)
		item.status = DeleteStatusFailed
		item.err = errcode.ErrDiskBroken
		return item.err
	}

	span.Debugf("start delete msg[%+v]", item.delMsg)
	if err := mgr.deleteWithCheckVolConsistency(item.ctx, item.delMsg); err != nil {
		item.status = DeleteStatusFailed
		item.err = err
		return err
	}

	delDoc := toDelDoc(*item.delMsg)
	if err := mgr.delLogger.Encode(delDoc); err != nil {
		span.Warnf("write delete log failed: vid[%d], bid[%d], err[%+v]", delDoc.Vid, delDoc.Bid, err)
	}

	item.status = DeleteStatusDone
	return item.err
}

func (mgr *BlobDeleteMgr) recordAllResult(ret *delBlobRet) {

	ctx := ret.ctx
	delMsg := ret.delMsg
	span := trace.SpanFromContextSafe(ctx)

	switch ret.status {
	case DeleteStatusDone:
		span.Debugf("delete success: vid[%d], bid[%d]", delMsg.Vid, delMsg.Bid)
		mgr.delSuccessCounterByMin.Add()
		mgr.delSuccessCounter.Inc()

	case DeleteStatusFailed:
		span.Warnf("delete failed and send msg to fail queue: vid[%d], bid[%d] retry[%d], err[%+v]",
			delMsg.Vid, delMsg.Bid, delMsg.Retry, ret.err)
		mgr.delFailCounter.Inc()
		mgr.delFailCounterByMin.Add()
		mgr.errStatsDistribution.AddFail(ret.err)
		// it can delete in next inspect
	case DeleteStatusUnexpect:
		span.Warnf("unexpected result will ignore: msg[%+v], err[%+v]", delMsg, ret.err)
	case DeleteStatusUndo:
		span.Warnf("delete message unconsume: msg[%+v]", delMsg)
	}
}

func (mgr *BlobDeleteMgr) deleteWithCheckVolConsistency(ctx context.Context, msg *proto.DeleteMsg) error {
	return DoubleCheckedRun(ctx, mgr.clusterTopology, msg.Vid, func(info *client.VolumeInfoSimple) (*client.VolumeInfoSimple, error) {
		return mgr.deleteBlob(ctx, info, msg)
	})
}

func (mgr *BlobDeleteMgr) deleteBlob(ctx context.Context, volInfo *client.VolumeInfoSimple, msg *proto.DeleteMsg) (newVol *client.VolumeInfoSimple, err error) {

	newVol, err = mgr.markDelBlob(ctx, volInfo, msg.Bid)
	if err != nil {
		return
	}

	newVol, err = mgr.delBlob(ctx, newVol, msg.Bid)
	return
}

func (mgr *BlobDeleteMgr) markDelBlob(ctx context.Context, volInfo *client.VolumeInfoSimple,
	bid proto.BlobID) (*client.VolumeInfoSimple, error) {
	return mgr.deleteShards(ctx, volInfo, bid, true)
}

func (mgr *BlobDeleteMgr) delBlob(ctx context.Context, volInfo *client.VolumeInfoSimple,
	bid proto.BlobID) (*client.VolumeInfoSimple, error) {
	return mgr.deleteShards(ctx, volInfo, bid, false)
}

func (mgr *BlobDeleteMgr) deleteShards(
	ctx context.Context, volInfo *client.VolumeInfoSimple,
	bid proto.BlobID, markDelete bool) (new *client.VolumeInfoSimple, err error) {
	span := trace.SpanFromContextSafe(ctx)

	var updateAndRetryShards []proto.Vuid
	locations := volInfo.VunitLocations
	vid := volInfo.Vid
	retCh := make(chan delShardRet, len(locations))

	span.Debugf("delete blob: vid[%d], bid[%d], markDelete[%+v]", vid, bid, markDelete)
	for _, location := range locations {
		span.Debugf("delete shards: location[%+v]", location)
		go func(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, markDelete bool) {
			err := mgr.deleteShard(ctx, location, bid, markDelete)
			retCh <- delShardRet{err: err, vuid: location.Vuid}
		}(ctx, location, bid, markDelete)
	}

	for i := 0; i < len(locations); i++ {
		ret := <-retCh
		if ret.err != nil {
			errCode := rpc.DetectStatusCode(ret.err)
			if shouldUpdateVolumeErr(errCode) {
				span.Errorf("delete shard failed will retry: bid[%d], vuid[%d], markDelete[%+v], code[%d], err[%+v]",
					bid, ret.vuid, markDelete, errCode, ret.err)
				updateAndRetryShards = append(updateAndRetryShards, ret.vuid)
				err = ret.err
				continue
			}

			span.Errorf("delete shard failed: bid[%d], vuid[%d], markDelete[%+v], code[%d], err[%+v]",
				bid, ret.vuid, markDelete, errCode, ret.err)
			return volInfo, ret.err
		}
	}

	if len(updateAndRetryShards) == 0 {
		span.Debugf("delete blob success: vid[%d], bid[%d], markDelete[%+v] ", vid, bid, markDelete)
		return volInfo, nil
	}

	span.Infof("bid delete will update and retry: len updateAndRetryShards[%d]", len(updateAndRetryShards))
	// update clusterTopology volume
	newVolInfo, updateVolErr := mgr.clusterTopology.UpdateVolume(vid)
	if updateVolErr != nil || newVolInfo.EqualWith(volInfo) {
		// if update volInfo failed or volInfo not updated, don't need retry
		span.Warnf("new volInfo is same or clusterTopology.UpdateVolume failed: vid[%d], err[%+v]", volInfo.Vid, updateVolErr)
		return volInfo, err
	}

	if len(newVolInfo.VunitLocations) != len(locations) {
		span.Warnf("vid locations len not equal: vid[%d], old len[%d], new len[%d]", len(locations), len(newVolInfo.VunitLocations))
		return volInfo, ErrVunitLengthNotEqual
	}

	for _, oldVuid := range updateAndRetryShards {
		idx := oldVuid.Index()
		newLocation := newVolInfo.VunitLocations[idx]
		span.Debugf("start retry delete shard: bid[%d]", bid)
		err := mgr.deleteShard(ctx, newLocation, bid, markDelete)
		if err != nil {
			span.Errorf("retry delete shard: bid[%d], new location[%+v], markDelete[%+v], err[%+v]",
				bid, newLocation, markDelete, err)
			return newVolInfo, err
		}
	}

	span.Debugf("delete blob success: vid[%d], bid[%d], markDelete[%+v]", vid, bid, markDelete)
	return newVolInfo, nil
}

func (mgr *BlobDeleteMgr) deleteShard(ctx context.Context, location proto.VunitLocation,
	bid proto.BlobID, markDelete bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	var stage proto.DeleteStage
	if markDelete {
		stage = proto.DeleteStageMarkDelete
		err = mgr.blobnodeCli.MarkDelete(ctx, location, bid)
	} else {
		stage = proto.DeleteStageDelete
		err = mgr.blobnodeCli.Delete(ctx, location, bid)
	}

	defer func() {
		if err == nil {
			span.Debugf("delete shard set stage: location[%+v], stage[%d]", location, stage)
			return
		}
	}()

	if err != nil {
		errCode := rpc.DetectStatusCode(err)
		if assumeDeleteSuccess(errCode) {
			span.Debugf("delete bid failed but assume success: bid[%d], location[%+v], err[%+v] ",
				bid, location, err)
			return nil
		}
	}

	return
}

func (mgr *BlobDeleteMgr) hasBrokenDisk(vid proto.Vid) bool {
	volume, err := mgr.clusterTopology.GetVolume(vid)
	if err != nil {
		return false
	}
	for _, unit := range volume.VunitLocations {
		if mgr.clusterTopology.IsBrokenDisk(unit.DiskID) {
			return true
		}
	}
	return false
}

// for error code judgment
func shouldUpdateVolumeErr(errCode int) bool {
	return errCode == errcode.CodeDiskBroken ||
		errCode == errcode.CodeVuidNotFound ||
		errCode == errcode.CodeDiskNotFound
}

func assumeDeleteSuccess(errCode int) bool {
	return errCode == errcode.CodeBidNotFound ||
		errCode == errcode.CodeShardMarkDeleted
}

func (mgr *BlobDeleteMgr) sleep(duration time.Duration) bool {
	t := time.NewTimer(duration)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-mgr.Done():
		return false
	}
}
