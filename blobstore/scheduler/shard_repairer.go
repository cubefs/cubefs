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
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

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
}

func (cfg *ShardRepairConfig) topics() []string {
	return append(cfg.Kafka.TopicNormals, cfg.Kafka.TopicFailed)
}

func (cfg *ShardRepairConfig) failedProducerConfig() *kafka.ProducerCfg {
	return &kafka.ProducerCfg{
		BrokerList: cfg.Kafka.BrokerList,
		Topic:      cfg.Kafka.TopicFailed,
		TimeoutMs:  cfg.Kafka.FailMsgSenderTimeoutMs,
	}
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

	kafkaConsumerClient base.KafkaConsumer
	consumers           []base.GroupConsumer
	failMsgSender       base.IProducer
	punishTime          time.Duration

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
	kafkaClient base.KafkaConsumer,
) (*ShardRepairMgr, error) {
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeShardRepair.String())
	if err != nil {
		return nil, err
	}

	workerSelector := selector.MakeSelector(60*1000, func() (hosts []string, err error) {
		return clusterMgrCli.GetService(context.Background(), proto.ServiceNameBlobNode, cfg.ClusterID)
	})
	failMsgSender, err := base.NewMsgSender(cfg.failedProducerConfig())
	if err != nil {
		return nil, err
	}

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

		kafkaConsumerClient: kafkaClient,
		failMsgSender:       failMsgSender,
		punishTime:          time.Duration(cfg.MessagePunishTimeM) * time.Minute,

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
	go mgr.runTask()
}

func (mgr *ShardRepairMgr) Close() {
	mgr.Closer.Close()
	mgr.stopConsumer()
}

func (mgr *ShardRepairMgr) runTask() {
	t := time.NewTicker(time.Second)
	span := trace.SpanFromContextSafe(context.Background())
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if !mgr.Enabled() {
				mgr.stopConsumer()
				continue
			}
			if err := mgr.startConsumer(); err != nil {
				span.Errorf("start consumer failed: err[%+v]", err)
				mgr.stopConsumer()
			}
		case <-mgr.Done():
			return
		}
	}
}

func (mgr *ShardRepairMgr) startConsumer() error {
	if mgr.consumerRunning() {
		return nil
	}
	for _, topic := range mgr.cfg.topics() {
		consumer, err := mgr.kafkaConsumerClient.StartKafkaConsumer(base.KafkaConsumerCfg{
			TaskType:     proto.TaskTypeShardRepair,
			Topic:        topic,
			MaxBatchSize: 1, // dont need batch, hard-coded
			MaxWaitTimeS: 1,
		}, mgr.Consume)
		if err != nil {
			return err
		}
		mgr.consumers = append(mgr.consumers, consumer)
	}
	return nil
}

func (mgr *ShardRepairMgr) stopConsumer() {
	if !mgr.consumerRunning() {
		return
	}
	for _, consumer := range mgr.consumers {
		consumer.Stop()
	}
	mgr.consumers = nil
}

func (mgr *ShardRepairMgr) consumerRunning() bool {
	return mgr.consumers != nil
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

// Consume consume kafka messages: if message is not consume will return false, otherwise return true
func (mgr *ShardRepairMgr) Consume(msgs []*sarama.ConsumerMessage, consumerPause base.ConsumerPause) bool {
	_, ctx := trace.StartSpanFromContext(context.Background(), "ShardRepairConsume")

	for _, msg := range msgs {
		rslt := mgr.handleOneMsg(ctx, msg, consumerPause)
		mgr.recordOneResult(ctx, rslt)

		if rslt.status == ShardRepairStatusUndo {
			return false
		}
	}
	return true
}

func (mgr *ShardRepairMgr) handleOneMsg(ctx context.Context, msg *sarama.ConsumerMessage, consumerPause base.ConsumerPause) (ret shardRepairRet) {
	var repairMsg *proto.ShardRepairMsg
	ret.status = ShardRepairStatusUnexpect
	defer func() {
		ret.repairMsg = repairMsg
	}()

	err := json.Unmarshal(msg.Value, &repairMsg)
	if err != nil {
		ret.err = err
		return
	}
	if !repairMsg.IsValid() {
		ret.err = proto.ErrInvalidMsg
		return
	}

	_, ctx = trace.StartSpanFromContextWithTraceID(ctx, "ShardRepairConsume", repairMsg.ReqId)
	return mgr.consume(ctx, repairMsg, consumerPause)
}

func (mgr *ShardRepairMgr) recordOneResult(ctx context.Context, r shardRepairRet) {
	span := trace.SpanFromContextSafe(ctx)
	switch r.status {
	case ShardRepairStatusDone:
		span.Debugf("repair success: vid[%d], bid[%d]", r.repairMsg.Vid, r.repairMsg.Bid)
		mgr.repairSuccessCounter.Inc()
		mgr.repairSuccessCounterMin.Add()

	case ShardRepairStatusFailed:
		span.Warnf("repair failed and send msg to fail queue: vid[%d], bid[%d], retry[%d], err[%+v]",
			r.repairMsg.Vid, r.repairMsg.Bid, r.repairMsg.Retry, r.err)
		mgr.repairFailedCounter.Inc()
		mgr.repairFailedCounterMin.Add()
		mgr.errStatsDistribution.AddFail(r.err)

		base.InsistOn(ctx, "repairer send2FailQueue", func() error {
			return mgr.send2FailQueue(ctx, r.repairMsg)
		})
	case ShardRepairStatusUnexpect, ShardRepairStatusOrphan:
		mgr.repairFailedCounter.Inc()
		mgr.repairFailedCounterMin.Add()
		mgr.errStatsDistribution.AddFail(r.err)
		span.Warnf("unexpected result: msg[%+v], err[%+v]", r.repairMsg, r.err)
	case ShardRepairStatusUndo:
		span.Warnf("repair message unconsume: msg[%+v]", r.repairMsg)
	}
}

func (mgr *ShardRepairMgr) consume(ctx context.Context, repairMsg *proto.ShardRepairMsg, consumerPause base.ConsumerPause) shardRepairRet {
	// quick exit if consumer is pause
	select {
	case <-consumerPause.Done():
		return shardRepairRet{status: ShardRepairStatusUndo}
	default:
	}
	span := trace.SpanFromContextSafe(ctx)
	// if message retry times is greater than MessagePunishThreshold while sleep MessagePunishTimeM minutes
	if repairMsg.Retry >= mgr.cfg.MessagePunishThreshold {
		span.Warnf("punish message for a while: until[%+v], sleep[%+v], retry[%d]",
			time.Now().Add(mgr.punishTime), mgr.punishTime, repairMsg.Retry)
		if ok := sleep(mgr.punishTime, consumerPause); !ok {
			return shardRepairRet{status: ShardRepairStatusUndo}
		}
	}
	jobKey := fmt.Sprintf("%d:%d:%s", repairMsg.Vid, repairMsg.Bid, repairMsg.BadIdx)
	_, err, _ := mgr.group.Do(jobKey, func() (ret interface{}, e error) {
		e = mgr.repairWithCheckVolConsistency(ctx, repairMsg)
		return
	})

	if isOrphanShard(err) {
		return shardRepairRet{status: ShardRepairStatusOrphan, err: err}
	}

	if err != nil {
		return shardRepairRet{status: ShardRepairStatusFailed, err: err}
	}

	return shardRepairRet{status: ShardRepairStatusDone}
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
		Bid:      repairMsg.Bid,
		CodeMode: volInfo.CodeMode,
		Sources:  volInfo.VunitLocations,
		BadIdxes: repairMsg.BadIdx,
		Reason:   repairMsg.Reason,
	}

	err := mgr.blobnodeCli.RepairShard(ctx, workerHost, task)
	if err == nil {
		return volInfo, nil
	}

	if isOrphanShard(err) {
		mgr.saveOrphanShard(ctx, repairMsg)
	}

	return volInfo, err
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

func (mgr *ShardRepairMgr) send2FailQueue(ctx context.Context, msg *proto.ShardRepairMsg) error {
	span := trace.SpanFromContextSafe(ctx)

	msg.Retry++
	b, err := json.Marshal(msg)
	if err != nil {
		// just panic if marsh fail
		span.Panicf("send to fail queue msg json.Marshal failed: msg[%+v], err[%+v]", msg, err)
	}

	err = mgr.failMsgSender.SendMessage(b)
	if err != nil {
		return fmt.Errorf("send message: err[%w]", err)
	}

	return nil
}
