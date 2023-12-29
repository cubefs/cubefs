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
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

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
	"github.com/cubefs/cubefs/blobstore/util/selector"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

type shardRepairStatus int

// shard repair status
const (
	ShardRepairStatusDone = shardRepairStatus(iota)
	ShardRepairStatusFailed
	ShardRepairStatusUnexpect
	ShardRepairStatusOrphan
	ShardRepairStatusUndo
)

// shard repair name
const (
	ShardRepair = "shard_repair"
)

// ErrBlobnodeServiceUnavailable worker service unavailable
var ErrBlobnodeServiceUnavailable = errors.New("blobnode service unavailable")

// ShardRepairConfig shard repair config
type ShardRepairConfig struct {
	ClusterID proto.ClusterID
	IDC       string
	Kafka     ShardRepairKafkaConfig

	// when the message retry times is greater than this, it will punish for a period of time before consumption
	MessagePunishThreshold int `json:"message_punish_threshold"`
	MessagePunishTimeM     int `json:"message_punish_time_m"`

	TaskPoolSize   int              `json:"task_pool_size"`
	OrphanShardLog recordlog.Config `json:"orphan_shard_log"`
	EnablePartial  bool             `json:"enable_partial"`
}

// OrphanShard orphan shard identification.
type OrphanShard struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Vid       proto.Vid       `json:"vid"`
	Bid       proto.BlobID    `json:"bid"`
}

// ShardRepairMgr shard repair manager
type ShardRepairMgr struct {
	closer.Closer
	taskPool        taskpool.TaskPool
	taskSwitch      *taskswitch.TaskSwitch
	clusterTopology IClusterTopology

	blobnodeCli      client.BlobnodeAPI
	blobnodeSelector selector.Selector

	repairSuccessCounter    prometheus.Counter
	repairSuccessCounterMin *counter.Counter
	repairFailedCounter     prometheus.Counter
	repairFailedCounterMin  *counter.Counter
	errStatsDistribution    *base.ErrorStats

	group             singleflight.Group
	orphanShardLogger recordlog.Encoder

	cfg *ShardRepairConfig
}

// NewShardRepairMgr returns shard repair manager
func NewShardRepairMgr(
	cfg *ShardRepairConfig,
	clusterTopology IClusterTopology,
	switchMgr *taskswitch.SwitchMgr,
	blobnodeCli client.BlobnodeAPI,
	clusterMgrCli client.ClusterMgrAPI,
) (*ShardRepairMgr, error) {
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeShardRepair.String())
	if err != nil {
		return nil, err
	}

	workerSelector := selector.MakeSelector(60*1000, func() (hosts []string, err error) {
		return clusterMgrCli.GetService(context.Background(), proto.ServiceNameBlobNode, cfg.ClusterID)
	})

	orphanShardsLog, err := recordlog.NewEncoder(&cfg.OrphanShardLog)
	if err != nil {
		return nil, err
	}

	return &ShardRepairMgr{
		blobnodeCli:      blobnodeCli,
		taskPool:         taskpool.New(cfg.TaskPoolSize, cfg.TaskPoolSize),
		taskSwitch:       taskSwitch,
		clusterTopology:  clusterTopology,
		blobnodeSelector: workerSelector,

		orphanShardLogger: orphanShardsLog,

		repairSuccessCounter:    base.NewCounter(cfg.ClusterID, ShardRepair, base.KindSuccess),
		repairFailedCounter:     base.NewCounter(cfg.ClusterID, ShardRepair, base.KindFailed),
		errStatsDistribution:    base.NewErrorStats(),
		repairSuccessCounterMin: &counter.Counter{},
		repairFailedCounterMin:  &counter.Counter{},

		cfg:    cfg,
		Closer: closer.New(),
	}, nil
}

// Enabled returns true if shard repair task is enabled, otherwise returns false
func (mgr *ShardRepairMgr) Enabled() bool {
	return mgr.taskSwitch.Enabled()
}

func (mgr *ShardRepairMgr) Run() {
	// for code check
	_ = struct{}{}
}

func (mgr *ShardRepairMgr) Close() {
	mgr.Closer.Close()
}

type shardRepairRet struct {
	status    shardRepairStatus
	repairMsg *proto.ShardRepairMsg
	err       error
}

// GetTaskStats returns task stats
func (mgr *ShardRepairMgr) GetTaskStats() (success [counter.SLOT]int, failed [counter.SLOT]int) {
	return mgr.repairSuccessCounterMin.Show(), mgr.repairFailedCounterMin.Show()
}

// GetErrorStats returns service error stats
func (mgr *ShardRepairMgr) GetErrorStats() (errStats []string, totalErrCnt uint64) {
	statsResult, totalErrCnt := mgr.errStatsDistribution.Stats()
	return base.FormatPrint(statsResult), totalErrCnt
}

func (mgr *ShardRepairMgr) Repair(ctx context.Context, repairMsg *proto.ShardRepairMsg) error {
	var ret shardRepairRet
	ret.repairMsg = repairMsg
	defer mgr.recordOneResult(ctx, ret)

	jobKey := fmt.Sprintf("%d:%d:%s", repairMsg.Vid, repairMsg.Bid, repairMsg.BadIdx)
	_, err, _ := mgr.group.Do(jobKey, func() (ret interface{}, e error) {
		e = mgr.repairWithCheckVolConsistency(ctx, repairMsg)
		return
	})

	if isOrphanShard(err) {
		ret = shardRepairRet{status: ShardRepairStatusOrphan, err: err}
		return err
	}

	if err != nil {
		ret = shardRepairRet{status: ShardRepairStatusFailed, err: err}
		return err
	}
	ret = shardRepairRet{status: ShardRepairStatusDone}
	return nil
}

func (mgr *ShardRepairMgr) recordOneResult(ctx context.Context, r shardRepairRet) {
	span := trace.SpanFromContextSafe(ctx)
	switch r.status {
	case ShardRepairStatusDone:
		span.Debugf("repair success: vid[%d], bid[%d]", r.repairMsg.Vid, r.repairMsg.Bid)
		mgr.repairSuccessCounter.Inc()
		mgr.repairSuccessCounterMin.Add()

	case ShardRepairStatusFailed:
		span.Warnf("repair failed and send msg to fail queue: vid[%d], bid[%d], err[%+v]",
			r.repairMsg.Vid, r.repairMsg.Bid, r.err)
		mgr.repairFailedCounter.Inc()
		mgr.repairFailedCounterMin.Add()
		mgr.errStatsDistribution.AddFail(r.err)

	case ShardRepairStatusUnexpect, ShardRepairStatusOrphan:
		mgr.repairFailedCounter.Inc()
		mgr.repairFailedCounterMin.Add()
		mgr.errStatsDistribution.AddFail(r.err)
		span.Warnf("unexpected result: msg[%+v], err[%+v]", r.repairMsg, r.err)
	case ShardRepairStatusUndo:
		span.Warnf("repair message unconsume: msg[%+v]", r.repairMsg)
	default:
		// do nothing
	}
}

func (mgr *ShardRepairMgr) repairWithCheckVolConsistency(ctx context.Context, repairMsg *proto.ShardRepairMsg) error {
	return DoubleCheckedRun(ctx, mgr.clusterTopology, repairMsg.Vid, func(info *client.VolumeInfoSimple) (*client.VolumeInfoSimple, error) {
		return mgr.tryRepair(ctx, info, repairMsg)
	})
}

func (mgr *ShardRepairMgr) tryRepair(ctx context.Context, volInfo *client.VolumeInfoSimple, repairMsg *proto.ShardRepairMsg) (*client.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)

	newVol, err := mgr.repairShard(ctx, volInfo, repairMsg)
	if err == nil {
		return newVol, nil
	}

	if err == ErrBlobnodeServiceUnavailable {
		return volInfo, err
	}

	newVol, err1 := mgr.clusterTopology.UpdateVolume(volInfo.Vid)
	if err1 != nil || newVol.EqualWith(volInfo) {
		// if update volInfo failed or volInfo not updated, don't need retry
		span.Warnf("new volInfo is same or clusterTopology.UpdateVolume failed: vid[%d], vol cache update err[%+v], repair err[%+v]",
			volInfo.Vid, err1, err)
		return volInfo, err
	}

	return mgr.repairShard(ctx, newVol, repairMsg)
}

func (mgr *ShardRepairMgr) repairShard(ctx context.Context, volInfo *client.VolumeInfoSimple, repairMsg *proto.ShardRepairMsg) (*client.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("repair shard: msg[%+v], vol info[%+v]", repairMsg, volInfo)

	hosts := mgr.blobnodeSelector.GetRandomN(1)
	if len(hosts) == 0 {
		return volInfo, ErrBlobnodeServiceUnavailable
	}
	workerHost := hosts[0]

	task := proto.ShardRepairTask{
		Bid:           repairMsg.Bid,
		CodeMode:      volInfo.CodeMode,
		Sources:       volInfo.VunitLocations,
		BadIdxes:      repairMsg.BadIdx,
		Reason:        repairMsg.Reason,
		EnablePartial: mgr.cfg.EnablePartial,
	}

	if err := mgr.blobnodeCli.RepairShard(ctx, workerHost, task); err != nil {
		if isOrphanShard(err) {
			mgr.saveOrphanShard(ctx, repairMsg)
		}
		return volInfo, err
	}

	return volInfo, nil
}

func (mgr *ShardRepairMgr) saveOrphanShard(ctx context.Context, repairMsg *proto.ShardRepairMsg) {
	span := trace.SpanFromContextSafe(ctx)

	shard := OrphanShard{
		ClusterID: repairMsg.ClusterID,
		Vid:       repairMsg.Vid,
		Bid:       repairMsg.Bid,
	}
	span.Infof("save orphan shard: [%+v]", shard)

	base.InsistOn(ctx, "save orphan shard", func() error {
		return mgr.orphanShardLogger.Encode(shard)
	})
}

func isOrphanShard(err error) bool {
	return rpc.DetectStatusCode(err) == errcode.CodeOrphanShard
}
