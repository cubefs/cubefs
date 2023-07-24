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

type deleteStageMgr struct {
	l         sync.Mutex
	delStages *proto.BlobDeleteStage
}

func newDeleteStageMgr() *deleteStageMgr {
	return &deleteStageMgr{
		delStages: &proto.BlobDeleteStage{
			Stages: make(map[uint8]proto.DeleteStage),
		},
	}
}

func (dsm *deleteStageMgr) setBlobDelStage(stage proto.BlobDeleteStage) {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	stageCopy := stage.Copy()
	dsm.delStages = &stageCopy
}

func (dsm *deleteStageMgr) getBlobDelStage() proto.BlobDeleteStage {
	dsm.l.Lock()
	defer dsm.l.Unlock()

	return dsm.delStages.Copy()
}

func (dsm *deleteStageMgr) setShardDelStage(vuid proto.Vuid, stage proto.DeleteStage) {
	dsm.l.Lock()
	defer dsm.l.Unlock()

	dsm.delStages.SetStage(vuid.Index(), stage)
}

func (dsm *deleteStageMgr) hasMarkDel(vuid proto.Vuid) bool {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	return dsm.stageEqual(vuid, proto.DeleteStageMarkDelete)
}

func (dsm *deleteStageMgr) hasDelete(vuid proto.Vuid) bool {
	dsm.l.Lock()
	defer dsm.l.Unlock()
	return dsm.stageEqual(vuid, proto.DeleteStageDelete)
}

func (dsm *deleteStageMgr) stageEqual(vuid proto.Vuid, target proto.DeleteStage) bool {
	s, ok := dsm.delStages.Stage(vuid)
	if ok && s == target {
		return true
	}
	return false
}

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
	Kafka     BlobDeleteKafkaConfig

	// when the message retry times is greater than this, it will punish for a period of time before consumption
	MessagePunishThreshold int `json:"message_punish_threshold"`
	MessagePunishTimeM     int `json:"message_punish_time_m"`
	MessageSlowDownTimeS   int `json:"message_slow_down_time_s"`

	TaskPoolSize    int              `json:"task_pool_size"`
	MaxBatchSize    int              `json:"max_batch_size"`
	BatchIntervalS  int              `json:"batch_interval_s"`
	SafeDelayTimeH  int64            `json:"safe_delay_time_h"`
	DeleteHourRange HourRange        `json:"delete_hour_range"`
	DeleteLog       recordlog.Config `json:"delete_log"`
}

func (cfg *BlobDeleteConfig) topics() []string {
	return []string{cfg.Kafka.TopicNormal, cfg.Kafka.TopicFailed}
}

func (cfg *BlobDeleteConfig) failedProducerConfig() *kafka.ProducerCfg {
	return &kafka.ProducerCfg{
		BrokerList: cfg.Kafka.BrokerList,
		Topic:      cfg.Kafka.TopicFailed,
		TimeoutMs:  cfg.Kafka.FailMsgSenderTimeoutMs,
	}
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

	kafkaConsumerClient base.KafkaConsumer
	consumers           []base.GroupConsumer
	safeDelayTime       time.Duration
	punishTime          time.Duration
	slowDownTime        time.Duration
	deleteHourRange     HourRange
	failMsgSender       base.IProducer

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
	kafkaClient base.KafkaConsumer,
) (*BlobDeleteMgr, error) {
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
		taskPool:               &tp,
		clusterTopology:        clusterTopology,
		blobnodeCli:            blobnodeCli,
		delSuccessCounter:      base.NewCounter(cfg.ClusterID, "delete", base.KindSuccess),
		delFailCounter:         base.NewCounter(cfg.ClusterID, "delete", base.KindFailed),
		errStatsDistribution:   base.NewErrorStats(),
		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},

		kafkaConsumerClient: kafkaClient,
		safeDelayTime:       time.Duration(cfg.SafeDelayTimeH) * time.Hour,
		punishTime:          time.Duration(cfg.MessagePunishTimeM) * time.Minute,
		slowDownTime:        time.Duration(cfg.MessageSlowDownTimeS) * time.Second,
		deleteHourRange:     cfg.DeleteHourRange,
		failMsgSender:       failMsgSender,
		delLogger:           delLogger,
		cfg:                 cfg,
		Closer:              closer.New(),
	}

	return mgr, nil
}

func (mgr *BlobDeleteMgr) Run() {
	go mgr.runTask()
}

func (mgr *BlobDeleteMgr) Close() {
	mgr.Closer.Close()
	mgr.stopConsumer()
}

func (mgr *BlobDeleteMgr) runTask() {
	t := time.NewTicker(time.Second)
	span := trace.SpanFromContextSafe(context.Background())
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if !mgr.taskSwitch.Enabled() {
				mgr.stopConsumer()
				continue
			}
			if _, ok := mgr.allowDeleting(time.Now()); !ok {
				mgr.stopConsumer()
				continue
			}
			if err := mgr.startConsumer(); err != nil {
				span.Errorf("run consumer failed: err[%+v]", err)
				mgr.stopConsumer()
			}
		case <-mgr.Done():
			return
		}
	}
}

func (mgr *BlobDeleteMgr) startConsumer() error {
	if mgr.consumerRunning() {
		return nil
	}
	for _, topic := range mgr.cfg.topics() {
		consumer, err := mgr.kafkaConsumerClient.StartKafkaConsumer(base.KafkaConsumerCfg{
			TaskType:     proto.TaskTypeBlobDelete,
			Topic:        topic,
			MaxBatchSize: mgr.cfg.MaxBatchSize,
			MaxWaitTimeS: mgr.cfg.BatchIntervalS,
		}, mgr.Consume)
		if err != nil {
			return err
		}
		mgr.consumers = append(mgr.consumers, consumer)
	}
	return nil
}

func (mgr *BlobDeleteMgr) stopConsumer() {
	if !mgr.consumerRunning() {
		return
	}
	for _, consumer := range mgr.consumers {
		consumer.Stop()
	}
	mgr.consumers = nil
}

func (mgr *BlobDeleteMgr) consumerRunning() bool {
	return mgr.consumers != nil
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

// Consume consume kafka message: if message is not consume will return false, otherwise return true
func (mgr *BlobDeleteMgr) Consume(msgs []*sarama.ConsumerMessage, consumerPause base.ConsumerPause) (consumed bool) {
	delItems, tracePrefix := mgr.preProcessMsg(msgs)
	defer mgr.maybeSlowDownConsume(delItems, consumerPause)
	defer mgr.recordAllResult(delItems)

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "BlobDeleteConsume", tracePrefix)
	span.Infof("start delete msgs len[%d], topic[%s], partition[%d], offset[%d]", len(msgs), msgs[0].Topic, msgs[0].Partition, msgs[0].Offset)
	wg := sync.WaitGroup{}
	wg.Add(len(delItems))
	for i := range delItems {
		idx := i
		mgr.taskPool.Run(func() {
			mgr.handleOneMsg(ctx, &delItems[idx], consumerPause)
			wg.Done()
		})
	}

	wg.Wait()
	for _, v := range delItems {
		if v.status == DeleteStatusUndo {
			return false
		}
	}
	return true
}

func (mgr *BlobDeleteMgr) preProcessMsg(msgs []*sarama.ConsumerMessage) (ret []delBlobRet, batchTraceId string) {
	ret = make([]delBlobRet, len(msgs))
	firstValidMsg, batchTraceId := true, ""

	for idx, msg := range msgs {
		err := json.Unmarshal(msg.Value, &ret[idx].delMsg)
		if err != nil {
			ret[idx].err = err
			ret[idx].status = DeleteStatusUnexpect
			continue
		}
		if !ret[idx].delMsg.IsValid() {
			ret[idx].err = proto.ErrInvalidMsg
			ret[idx].status = DeleteStatusUnexpect
			continue
		}
		if firstValidMsg {
			firstValidMsg = false
			// A batch of delete messages uses the same trace id prefix to make it easier to trace the batch of messages
			batchTraceId = ret[idx].delMsg.ReqId
		}
	}
	return ret, batchTraceId
}

func (mgr *BlobDeleteMgr) handleOneMsg(ctx context.Context, item *delBlobRet, consumerPause base.ConsumerPause) {
	if item.err != nil {
		item.ctx = ctx
		return
	}

	span := trace.SpanFromContextSafe(ctx)
	_, ctx1 := trace.StartSpanFromContextWithTraceID(ctx, span.OperationName(), span.TraceID()+"_"+item.delMsg.ReqId)
	item.ctx = ctx1
	mgr.consume(item, consumerPause)
}

func (mgr *BlobDeleteMgr) recordAllResult(rets []delBlobRet) {
	for _, ret := range rets {
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

			base.InsistOn(ctx, "deleter send2FailQueue", func() error {
				return mgr.send2FailQueue(ctx, delMsg)
			})
		case DeleteStatusUnexpect:
			span.Warnf("unexpected result will ignore: msg[%+v], err[%+v]", delMsg, ret.err)
		case DeleteStatusUndo:
			span.Warnf("delete message unconsume: msg[%+v]", delMsg)
		}
	}
}

func (mgr *BlobDeleteMgr) maybeSlowDownConsume(rets []delBlobRet, consumerPause base.ConsumerPause) {
	for _, ret := range rets {
		if ret.err == nil {
			continue
		}

		errCode := rpc.DetectStatusCode(ret.err)
		if needSubBatchSize(errCode) { // need limit, slow down consume message, only do once
			select {
			case <-consumerPause.Done():
			case <-time.After(mgr.slowDownTime):
			}
			break
		}
	}
}

func (mgr *BlobDeleteMgr) consume(item *delBlobRet, consumerPause base.ConsumerPause) {
	// quick exit if consumer is pause
	select {
	case <-consumerPause.Done():
		item.status = DeleteStatusUndo
		return
	default:
	}
	span := trace.SpanFromContextSafe(item.ctx)

	// if message retry times is greater than MessagePunishThreshold while sleep MessagePunishTimeM minutes
	if item.delMsg.Retry >= mgr.cfg.MessagePunishThreshold {
		span.Warnf("punish message for a while: until[%+v], sleep[%+v], retry[%d]",
			time.Now().Add(mgr.punishTime), mgr.punishTime, item.delMsg.Retry)
		if ok := sleep(mgr.punishTime, consumerPause); !ok {
			item.status = DeleteStatusUndo
			return
		}
	}
	now := time.Now().UTC()
	if now.Sub(time.Unix(item.delMsg.Time, 0)) < mgr.safeDelayTime {
		sleepDuration := mgr.delayDuration(item.delMsg.Time)
		span.Warnf("blob is protected: until[%+v], sleep[%+v]", time.Unix(item.delMsg.Time, 0).Add(mgr.safeDelayTime), sleepDuration)
		ok := sleep(sleepDuration, consumerPause)
		if !ok {
			item.status = DeleteStatusUndo
			return
		}
	}
	if mgr.hasBrokenDisk(item.delMsg.Vid) {
		span.Debugf("the volume has broken disk and delete later: vid[%d], bid[%d]", item.delMsg.Vid, item.delMsg.Bid)
		// try to update volume
		mgr.clusterTopology.UpdateVolume(item.delMsg.Vid)
		item.status = DeleteStatusFailed
		item.err = errcode.ErrDiskBroken
		return
	}

	span.Debugf("start delete msg[%+v]", item.delMsg)
	if err := mgr.deleteWithCheckVolConsistency(item.ctx, item.delMsg); err != nil {
		item.status = DeleteStatusFailed
		item.err = err
		return
	}

	delDoc := toDelDoc(*item.delMsg)
	if err := mgr.delLogger.Encode(delDoc); err != nil {
		span.Warnf("write delete log failed: vid[%d], bid[%d], err[%+v]", delDoc.Vid, delDoc.Bid, err)
	}

	item.status = DeleteStatusDone
}

func (mgr *BlobDeleteMgr) deleteWithCheckVolConsistency(ctx context.Context, msg *proto.DeleteMsg) error {
	return DoubleCheckedRun(ctx, mgr.clusterTopology, msg.Vid, func(info *client.VolumeInfoSimple) (*client.VolumeInfoSimple, error) {
		return mgr.deleteBlob(ctx, info, msg)
	})
}

func (mgr *BlobDeleteMgr) deleteBlob(ctx context.Context, volInfo *client.VolumeInfoSimple, msg *proto.DeleteMsg) (newVol *client.VolumeInfoSimple, err error) {
	deleteStageMgr := newDeleteStageMgr()
	deleteStageMgr.setBlobDelStage(msg.BlobDelStages)

	defer func() {
		msg.SetDeleteStage(deleteStageMgr.getBlobDelStage())
	}()

	newVol, err = mgr.markDelBlob(ctx, volInfo, msg.Bid, deleteStageMgr)
	if err != nil {
		return
	}

	newVol, err = mgr.delBlob(ctx, newVol, msg.Bid, deleteStageMgr)
	return
}

func (mgr *BlobDeleteMgr) markDelBlob(ctx context.Context, volInfo *client.VolumeInfoSimple,
	bid proto.BlobID, stageMgr *deleteStageMgr) (*client.VolumeInfoSimple, error) {
	return mgr.deleteShards(ctx, volInfo, bid, stageMgr, true)
}

func (mgr *BlobDeleteMgr) delBlob(ctx context.Context, volInfo *client.VolumeInfoSimple,
	bid proto.BlobID, stageMgr *deleteStageMgr) (*client.VolumeInfoSimple, error) {
	return mgr.deleteShards(ctx, volInfo, bid, stageMgr, false)
}

func (mgr *BlobDeleteMgr) deleteShards(
	ctx context.Context, volInfo *client.VolumeInfoSimple,
	bid proto.BlobID, stageMgr *deleteStageMgr,
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
			err := mgr.deleteShard(ctx, location, bid, stageMgr, markDelete)
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
		err := mgr.deleteShard(ctx, newLocation, bid, stageMgr, markDelete)
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
	bid proto.BlobID, stageMgr *deleteStageMgr, markDelete bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// has mark delete or delete before and just return
	if (stageMgr.hasDelete(location.Vuid)) || (markDelete && stageMgr.hasMarkDel(location.Vuid)) {
		span.Warnf("already delete and return: bid[%d], location[%+v], markDelete[%+v]",
			bid, location, markDelete)
		return nil
	}

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
			stageMgr.setShardDelStage(location.Vuid, stage)
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

func (mgr *BlobDeleteMgr) send2FailQueue(ctx context.Context, msg *proto.DeleteMsg) error {
	span := trace.SpanFromContextSafe(ctx)

	// set delete stage
	span.Debugf("send to fail queue: bid[%d], try[%d], delete stages[%+v]", msg.Bid, msg.Retry, msg.BlobDelStages)

	msg.Retry++
	b, err := json.Marshal(msg)
	if err != nil {
		// just panic if marsh fail
		span.Panicf("\"send to fail queue json.Marshal failed: msg[%+v], err[%+v]", msg, err)
	}

	err = mgr.failMsgSender.SendMessage(b)
	if err != nil {
		span.Errorf("failMsgSender.SendMessage failed: err[%+v]", b)
		return err
	}
	return nil
}

func (mgr *BlobDeleteMgr) allowDeleting(now time.Time) (waitTime time.Duration, ok bool) {
	// from <= now < to
	nowHour := now.Hour()
	if nowHour >= mgr.deleteHourRange.From && nowHour < mgr.deleteHourRange.To {
		return waitTime, true
	}

	// now < from
	fromTime := time.Date(now.Year(), now.Month(), now.Day(), mgr.deleteHourRange.From, 0, 0, 0, now.Location())
	if now.Before(fromTime) {
		waitTime = fromTime.Sub(now)
		return waitTime, false
	}
	// now >= to
	endTime := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	waitTime = endTime.Sub(now) + time.Duration(mgr.deleteHourRange.From)*time.Hour
	return
}

func (mgr *BlobDeleteMgr) delayDuration(delTimeStamp int64) time.Duration {
	start := time.Unix(delTimeStamp, 0)
	now := time.Now()
	return start.Add(mgr.safeDelayTime).Sub(now)
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

func needSubBatchSize(errCode int) bool {
	return errCode == errcode.CodeOverload
}

func sleep(duration time.Duration, consumerPause base.ConsumerPause) bool {
	t := time.NewTimer(duration)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-consumerPause.Done():
		return false
	}
}
