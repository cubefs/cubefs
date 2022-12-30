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
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// ITaskRunner define the interface of task running status.
type ITaskRunner interface {
	Enabled() bool
	RunTask()
	GetTaskStats() (success, failed [counter.SLOT]int)
	GetErrorStats() (errStats []string, totalErrCnt uint64)
}

type deleteStatus int

// blob delete status
const (
	DelDone = deleteStatus(iota)
	DelDelay
	DelFailed
	DelUnexpect
)

// ErrVunitLengthNotEqual vunit length not equal
var ErrVunitLengthNotEqual = errors.New("vunit length not equal")

type deleteStageMgr struct {
	l         sync.Mutex
	delStages map[proto.BlobID]*proto.BlobDeleteStage
}

func (dsm *deleteStageMgr) clear() {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	dsm.delStages = make(map[proto.BlobID]*proto.BlobDeleteStage)
}

func (dsm *deleteStageMgr) setBlobDelStage(bid proto.BlobID, stage proto.BlobDeleteStage) {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	stageCopy := stage.Copy()
	dsm.delStages[bid] = &stageCopy
}

func (dsm *deleteStageMgr) getBlobDelStage(bid proto.BlobID) proto.BlobDeleteStage {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	if stage, exist := dsm.delStages[bid]; exist {
		return stage.Copy()
	}
	return proto.BlobDeleteStage{}
}

func (dsm *deleteStageMgr) setShardDelStage(bid proto.BlobID, vuid proto.Vuid, stage proto.DeleteStage) {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	if dsm.delStages == nil {
		dsm.delStages = make(map[proto.BlobID]*proto.BlobDeleteStage)
	}

	if _, exist := dsm.delStages[bid]; !exist {
		dsm.delStages[bid] = &proto.BlobDeleteStage{}
	}
	dbs := dsm.delStages[bid]
	dbs.SetStage(vuid.Index(), stage)
}

func (dsm *deleteStageMgr) hasMarkDel(bid proto.BlobID, vuid proto.Vuid) bool {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	return dsm.stageEqual(bid, vuid, proto.MarkDelStage)
}

func (dsm *deleteStageMgr) stageEqual(bid proto.BlobID, vuid proto.Vuid, target proto.DeleteStage) bool {
	if ds, exist := dsm.delStages[bid]; exist {
		s, ok := ds.Stage(vuid)
		if ok && s == target {
			return true
		}
	}
	return false
}

type delShardRet struct {
	err  error
	vuid proto.Vuid
}

type delBlobRet struct {
	status deleteStatus
	err    error
	delMsg *proto.DeleteMsg
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

	TaskPoolSize int `json:"task_pool_size"`

	FailMsgConsumeIntervalMs int64 `json:"fail_msg_consume_interval_ms"`

	NormalHandleBatchCnt int `json:"normal_handle_batch_cnt"`
	FailHandleBatchCnt   int `json:"fail_handle_batch_cnt"`

	SafeDelayTimeH  int64            `json:"safe_delay_time_h"`
	DeleteHourRange HourRange        `json:"delete_hour_range"`
	DeleteLog       recordlog.Config `json:"delete_log"`

	Kafka BlobDeleteKafkaConfig `json:"-"`
}

func (cfg *BlobDeleteConfig) normalConsumerConfig() *base.KafkaConfig {
	return &base.KafkaConfig{
		Topic:      cfg.Kafka.Normal.Topic,
		Partitions: cfg.Kafka.Normal.Partitions,
		BrokerList: cfg.Kafka.BrokerList,
	}
}

func (cfg *BlobDeleteConfig) failedConsumerConfig() *base.KafkaConfig {
	return &base.KafkaConfig{
		Topic:      cfg.Kafka.Failed.Topic,
		Partitions: cfg.Kafka.Failed.Partitions,
		BrokerList: cfg.Kafka.BrokerList,
	}
}

func (cfg *BlobDeleteConfig) failedProducerConfig() *kafka.ProducerCfg {
	return &kafka.ProducerCfg{
		BrokerList: cfg.Kafka.BrokerList,
		Topic:      cfg.Kafka.Failed.Topic,
		TimeoutMs:  cfg.Kafka.FailMsgSenderTimeoutMs,
	}
}

// BlobDeleteMgr is blob delete manager
type BlobDeleteMgr struct {
	taskSwitch *taskswitch.TaskSwitch

	normalConsumer *deleteTopicConsumer
	failConsumer   *deleteTopicConsumer

	delSuccessCounter      prometheus.Counter
	delSuccessCounterByMin *counter.Counter
	delFailCounter         prometheus.Counter
	delFailCounterByMin    *counter.Counter
	errStatsDistribution   *base.ErrorStats
}

// NewBlobDeleteMgr returns blob delete manager
func NewBlobDeleteMgr(
	cfg *BlobDeleteConfig,
	clusterTopology IClusterTopology,
	blobnodeCli client.BlobnodeAPI,
	switchMgr *taskswitch.SwitchMgr,
	clusterMgrCli client.ClusterMgrAPI,
) (*BlobDeleteMgr, error) {
	normalTopicConsumers, err := base.NewKafkaPartitionConsumers(proto.TaskTypeBlobDelete, cfg.normalConsumerConfig(), clusterMgrCli)
	if err != nil {
		return nil, err
	}

	failTopicConsumers, err := base.NewKafkaPartitionConsumers(proto.TaskTypeBlobDelete, cfg.failedConsumerConfig(), clusterMgrCli)
	if err != nil {
		return nil, err
	}

	failMsgSender, err := base.NewMsgSender(cfg.failedProducerConfig())
	if err != nil {
		return nil, err
	}

	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeBlobDelete.String())
	if err != nil {
		return nil, err
	}

	delLogger, err := recordlog.NewEncoder(&cfg.DeleteLog)
	if err != nil {
		return nil, err
	}

	tp := taskpool.New(cfg.TaskPoolSize, cfg.TaskPoolSize)

	mgr := &BlobDeleteMgr{
		taskSwitch:             taskSwitch,
		delSuccessCounter:      base.NewCounter(cfg.ClusterID, "delete", base.KindSuccess),
		delFailCounter:         base.NewCounter(cfg.ClusterID, "delete", base.KindFailed),
		errStatsDistribution:   base.NewErrorStats(),
		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},
	}

	normalTopicConsumer := &deleteTopicConsumer{
		taskSwitch: taskSwitch,

		taskPool:          &tp,
		topicConsumers:    normalTopicConsumers,
		consumeBatchCnt:   cfg.NormalHandleBatchCnt,
		consumeIntervalMs: time.Duration(0),
		safeDelayTime:     time.Duration(cfg.SafeDelayTimeH) * time.Hour,
		deleteHourRange:   cfg.DeleteHourRange,
		clusterTopology:   clusterTopology,
		blobnodeCli:       blobnodeCli,
		failMsgSender:     failMsgSender,

		delSuccessCounter:      mgr.delSuccessCounter,
		delSuccessCounterByMin: mgr.delSuccessCounterByMin,
		delFailCounter:         mgr.delFailCounter,
		delFailCounterByMin:    mgr.delFailCounterByMin,
		errStatsDistribution:   mgr.errStatsDistribution,

		delLogger: delLogger,
	}

	failTopicConsumer := &deleteTopicConsumer{
		taskSwitch: taskSwitch,

		taskPool:          &tp,
		topicConsumers:    failTopicConsumers,
		consumeBatchCnt:   cfg.FailHandleBatchCnt,
		consumeIntervalMs: time.Duration(cfg.FailMsgConsumeIntervalMs) * time.Millisecond,
		safeDelayTime:     time.Duration(cfg.SafeDelayTimeH) * time.Hour,
		deleteHourRange:   cfg.DeleteHourRange,
		clusterTopology:   clusterTopology,
		blobnodeCli:       blobnodeCli,
		failMsgSender:     failMsgSender,

		delSuccessCounter:      mgr.delSuccessCounter,
		delSuccessCounterByMin: mgr.delSuccessCounterByMin,
		delFailCounter:         mgr.delFailCounter,
		delFailCounterByMin:    mgr.delFailCounterByMin,
		errStatsDistribution:   mgr.errStatsDistribution,

		delLogger: delLogger,
	}

	mgr.normalConsumer = normalTopicConsumer
	mgr.failConsumer = failTopicConsumer

	return mgr, nil
}

// RunTask consumers delete messages
func (mgr *BlobDeleteMgr) RunTask() {
	mgr.normalConsumer.run()
	mgr.failConsumer.run()
}

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

type deleteTopicConsumer struct {
	taskSwitch      *taskswitch.TaskSwitch
	clusterTopology IClusterTopology

	taskPool          *taskpool.TaskPool
	topicConsumers    []base.IConsumer
	consumeBatchCnt   int
	consumeIntervalMs time.Duration
	safeDelayTime     time.Duration
	deleteHourRange   HourRange

	blobnodeCli client.BlobnodeAPI

	failMsgSender base.IProducer
	dsm           deleteStageMgr

	// stats
	delSuccessCounter      prometheus.Counter
	delSuccessCounterByMin *counter.Counter
	delFailCounter         prometheus.Counter
	delFailCounterByMin    *counter.Counter
	errStatsDistribution   *base.ErrorStats

	// delete log
	delLogger recordlog.Encoder
}

func (d *deleteTopicConsumer) run() {
	for _, consumer := range d.topicConsumers {
		go func(consumer base.IConsumer) {
			for {
				d.taskSwitch.WaitEnable()
				if waitTime, ok := d.allowDeleting(time.Now()); !ok {
					time.Sleep(waitTime)
					continue
				}
				d.consumeAndDelete(consumer, d.consumeBatchCnt)
				if d.consumeIntervalMs != time.Duration(0) {
					time.Sleep(d.consumeIntervalMs)
				}
			}
		}(consumer)
	}
}

func (d *deleteTopicConsumer) allowDeleting(now time.Time) (waitTime time.Duration, ok bool) {
	// from <= now < to
	nowHour := now.Hour()
	if nowHour >= d.deleteHourRange.From && nowHour < d.deleteHourRange.To {
		return waitTime, true
	}

	// now < from
	fromTime := time.Date(now.Year(), now.Month(), now.Day(), d.deleteHourRange.From, 0, 0, 0, now.Location())
	if now.Before(fromTime) {
		waitTime = fromTime.Sub(now)
		return waitTime, false
	}
	// now >= to
	endTime := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	waitTime = endTime.Sub(now) + time.Duration(d.deleteHourRange.From)*time.Hour
	return
}

func (d *deleteTopicConsumer) consumeAndDelete(consumer base.IConsumer, batchCnt int) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "consumeAndDelete")
	defer span.Finish()

	if batchCnt <= 0 {
		batchCnt = 1
	}

	msgs := consumer.ConsumeMessages(ctx, batchCnt)
	if len(msgs) == 0 {
		return
	}
	d.handleMsgBatch(ctx, msgs)

	base.InsistOn(ctx, "deleter consumer.CommitOffset", func() error {
		return consumer.CommitOffset(ctx)
	})
}

func (d *deleteTopicConsumer) handleMsgBatch(ctx context.Context, mqMsgs []*sarama.ConsumerMessage) {
	span := trace.SpanFromContextSafe(ctx)
	ctx = trace.ContextWithSpan(ctx, span)

	span.Infof("handle delete msg: len[%d]", len(mqMsgs))

	var msgs []*proto.DeleteMsg
	if len(mqMsgs) != 0 {
		ms := unmarshalMsgs(mqMsgs)
		msgs = DeduplicateMsgs(ctx, ms)
		span.Infof("deduplicate messages: len[%d]", len(msgs))
	}

	if len(msgs) != 0 {
		// clear delete stage before handle batch msgs
		span.Debugf("dsm clear before delete")
		d.dsm.clear()
		for _, m := range msgs {
			span.Debugf("set blob delete stage: %+v", m.BlobDelStages)
			d.dsm.setBlobDelStage(m.Bid, m.BlobDelStages)
		}
	}

	for len(msgs) != 0 {
		finishCh := make(chan delBlobRet, len(msgs))
		for _, m := range msgs {
			func(delMsg *proto.DeleteMsg) {
				d.taskPool.Run(func() {
					d.handleOneMsg(ctx, delMsg, finishCh)
				})
			}(m)
		}

		var delayMsgs []*proto.DeleteMsg
		var maxDelayMsgTimeStamp int64 = 0
		for i := 0; i < len(msgs); i++ {
			ret := <-finishCh
			switch ret.status {
			case DelDone:
				span.Debugf("delete success: vid[%d], bid[%d], reqid[%s]", ret.delMsg.Vid, ret.delMsg.Bid, ret.delMsg.ReqId)
				d.delSuccessCounterByMin.Add()
				d.delSuccessCounter.Inc()

			case DelFailed:
				span.Warnf("delete failed and send msg to fail queue: vid[%d], bid[%d], reqid[%s], retry[%d], err[%+v]",
					ret.delMsg.Vid, ret.delMsg.Bid, ret.delMsg.ReqId, ret.delMsg.Retry, ret.err)
				d.delFailCounter.Inc()
				d.delFailCounterByMin.Add()
				d.errStatsDistribution.AddFail(ret.err)

				base.InsistOn(ctx, "deleter send2FailQueue", func() error {
					return d.send2FailQueue(ctx, *ret.delMsg)
				})

			case DelDelay:
				if ret.delMsg.Time > maxDelayMsgTimeStamp {
					maxDelayMsgTimeStamp = ret.delMsg.Time
				}
				delayMsgs = append(delayMsgs, ret.delMsg)

			case DelUnexpect:
				span.Warnf("unexpected result will ignore: msg[%+v], err[%+v]", ret.delMsg, ret.err)
			}
		}

		if len(delayMsgs) == 0 {
			return
		}

		sleepDuration := d.delayDuration(maxDelayMsgTimeStamp)
		span.Warnf("blob is protected: util[%+v], sleep[%+v]", time.Unix(maxDelayMsgTimeStamp, 0).Add(d.safeDelayTime), sleepDuration)
		time.Sleep(sleepDuration)
		msgs = delayMsgs
	}
}

func (d *deleteTopicConsumer) delayDuration(delTimeStamp int64) time.Duration {
	start := time.Unix(delTimeStamp, 0)
	now := time.Now()
	return start.Add(d.safeDelayTime).Sub(now)
}

func (d *deleteTopicConsumer) handleOneMsg(ctx context.Context, delMsg *proto.DeleteMsg, finishCh chan<- delBlobRet) {
	if !delMsg.IsValid() {
		finishCh <- delBlobRet{
			status: DelUnexpect,
			err:    proto.ErrInvalidMsg,
		}
		return
	}

	now := time.Now().UTC()
	if now.Sub(time.Unix(delMsg.Time, 0)) < d.safeDelayTime {
		finishCh <- delBlobRet{
			status: DelDelay,
			delMsg: delMsg,
		}
		return
	}
	pSpan := trace.SpanFromContextSafe(ctx)
	if d.hasBrokenDisk(delMsg.Vid) {
		pSpan.Debugf("the volume has broken disk and delete later: vid[%d], bid[%d]", delMsg.Vid, delMsg.Bid)
		finishCh <- delBlobRet{
			status: DelFailed,
			err:    errcode.ErrDiskBroken,
			delMsg: delMsg,
		}
		return
	}

	pSpan.Infof("start delete msg: [%+v]", delMsg)

	span, tmpCtx := trace.StartSpanFromContextWithTraceID(context.Background(), "handleDeleteMsg", delMsg.ReqId)
	err := d.deleteWithCheckVolConsistency(tmpCtx, delMsg.Vid, delMsg.Bid)
	if err != nil {
		finishCh <- delBlobRet{
			status: DelFailed,
			err:    err,
			delMsg: delMsg,
		}
		return
	}

	delDoc := toDelDoc(*delMsg)
	err = d.delLogger.Encode(delDoc)
	if err != nil {
		span.Warnf("write delete log failed: vid[%d], bid[%d], err[%+v]", delDoc.Vid, delDoc.Bid, err)
	}

	finishCh <- delBlobRet{
		status: DelDone,
		delMsg: delMsg,
	}
}

func (d *deleteTopicConsumer) hasBrokenDisk(vid proto.Vid) bool {
	volume, err := d.clusterTopology.GetVolume(vid)
	if err != nil {
		return false
	}
	for _, unit := range volume.VunitLocations {
		if d.clusterTopology.IsBrokenDisk(unit.DiskID) {
			return true
		}
	}
	return false
}

func (d *deleteTopicConsumer) deleteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, bid proto.BlobID) error {
	return DoubleCheckedRun(ctx, d.clusterTopology, vid, func(info *client.VolumeInfoSimple) (*client.VolumeInfoSimple, error) {
		return d.deleteBlob(ctx, info, bid)
	})
}

func (d *deleteTopicConsumer) deleteBlob(ctx context.Context, volInfo *client.VolumeInfoSimple, bid proto.BlobID) (newVol *client.VolumeInfoSimple, err error) {
	newVol, err = d.markDelBlob(ctx, volInfo, bid)
	if err != nil {
		return
	}

	newVol, err = d.delBlob(ctx, newVol, bid)
	return
}

func (d *deleteTopicConsumer) markDelBlob(ctx context.Context, volInfo *client.VolumeInfoSimple, bid proto.BlobID) (*client.VolumeInfoSimple, error) {
	return d.deleteShards(ctx, volInfo, bid, true)
}

func (d *deleteTopicConsumer) delBlob(ctx context.Context, volInfo *client.VolumeInfoSimple, bid proto.BlobID) (*client.VolumeInfoSimple, error) {
	return d.deleteShards(ctx, volInfo, bid, false)
}

func (d *deleteTopicConsumer) deleteShards(
	ctx context.Context,
	volInfo *client.VolumeInfoSimple,
	bid proto.BlobID,
	markDelete bool) (new *client.VolumeInfoSimple, err error) {
	span := trace.SpanFromContextSafe(ctx)

	var updateAndRetryShards []proto.Vuid
	locations := volInfo.VunitLocations
	vid := volInfo.Vid
	retCh := make(chan delShardRet, len(locations))

	span.Debugf("delete blob: vid[%d], bid[%d], markDelete[%+v]", vid, bid, markDelete)
	for _, location := range locations {
		span.Debugf("delete shards: location[%+v]", location)
		go func(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, markDelete bool) {
			err := d.deleteShard(ctx, location, bid, markDelete)
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
	newVolInfo, updateVolErr := d.clusterTopology.UpdateVolume(vid)
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
		err := d.deleteShard(ctx, newLocation, bid, markDelete)
		if err != nil {
			span.Errorf("retry delete shard: bid[%d], new location[%+v], markDelete[%+v], err[%+v]",
				bid, newLocation, markDelete, err)
			return newVolInfo, err
		}
	}

	span.Debugf("delete blob success: vid[%d], bid[%d], markDelete[%+v]", vid, bid, markDelete)
	return newVolInfo, nil
}

func (d *deleteTopicConsumer) deleteShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, markDelete bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// in order to prevent missing delete task,
	// regardless of whether it is deleted or not, it will be deleted,
	// just skip mark delete when has mark deleted
	if d.hasMarkDeleted(location.Vuid, bid, markDelete) {
		span.Infof("bid has mark deleted and skip: bid[%d], location[%+v]", bid, location)
		return nil
	}

	var stage proto.DeleteStage
	if markDelete {
		stage = proto.MarkDelStage
		err = d.blobnodeCli.MarkDelete(ctx, location, bid)
	} else {
		stage = proto.DelStage
		err = d.blobnodeCli.Delete(ctx, location, bid)
	}

	defer func() {
		if err == nil {
			span.Debugf("delete shard set stage: location[%+v], stage[%d]", location, stage)
			d.dsm.setShardDelStage(bid, location.Vuid, stage)
			return
		}

		if shouldBackToInitStage(err) {
			d.dsm.setShardDelStage(bid, location.Vuid, proto.InitStage)
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

func (d *deleteTopicConsumer) hasMarkDeleted(vuid proto.Vuid, bid proto.BlobID, markDelete bool) bool {
	return markDelete && d.dsm.hasMarkDel(bid, vuid)
}

func shouldBackToInitStage(err error) bool {
	//Simple handling, for all deletion errors, delete tasks are all redone from InitStage
	//todo:analyze the error codes carefully to determine which ones need to be back to init stage
	return err != nil
}

func (d *deleteTopicConsumer) send2FailQueue(ctx context.Context, msg proto.DeleteMsg) error {
	span := trace.SpanFromContextSafe(ctx)

	// set delete stage
	delStage := d.dsm.getBlobDelStage(msg.Bid)
	span.Debugf("send to fail queue: bid[%d], try[%d], delete stages[%+v]", msg.Bid, msg.Retry, delStage)
	msg.SetDeleteStage(delStage)
	span.Debugf("delete stage: [%+v]", msg.BlobDelStages)

	msg.Retry++
	b, err := json.Marshal(msg)
	if err != nil {
		// just panic if marsh fail
		span.Panicf("\"send to fail queue json.Marshal failed: msg[%+v], err[%+v]", msg, err)
	}

	err = d.failMsgSender.SendMessage(b)
	if err != nil {
		span.Errorf("failMsgSender.SendMessage failed: err[%+v]", b)
		return err
	}
	return nil
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

func unmarshalMsgs(msgs []*sarama.ConsumerMessage) (delMsgs []*proto.DeleteMsg) {
	for _, msg := range msgs {
		var delMsg proto.DeleteMsg
		err := json.Unmarshal(msg.Value, &delMsg)
		if err != nil {
			continue
		}
		delMsgs = append(delMsgs, &delMsg)
	}
	return delMsgs
}

// DeduplicateMsgs deduplicate delete messages
func DeduplicateMsgs(ctx context.Context, delMsgs []*proto.DeleteMsg) (msgs []*proto.DeleteMsg) {
	span := trace.SpanFromContextSafe(ctx)
	bids := make(map[proto.BlobID]struct{})
	for _, m := range delMsgs {
		if _, exist := bids[m.Bid]; !exist {
			msgs = append(msgs, m)
			bids[m.Bid] = struct{}{}
			continue
		}
		span.Infof("msg dropped due to same task: msg[%+v]", m)
	}
	return
}
