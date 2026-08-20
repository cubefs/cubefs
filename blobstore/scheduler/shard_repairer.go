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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
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

	// EnableAZSplitShardRepair enables splitting shard repair tasks by AZ.
	// When enabled (and bad shards span >1 AZ with all IDCs known and
	// local-repairable), each AZ's bad indices are dispatched concurrently
	// to a worker in that same AZ, eliminating cross-AZ traffic for
	// local-stripe repair.
	// Default false: all bad shards go to a single worker (original behavior).
	EnableAZSplitShardRepair bool `json:"enable_az_split_shard_repair"`
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
	blobnodeSelector *idcSelector
	clusterMgrCli    client.ClusterMgrAPI
	taskCli          client.TaskAPI

	repairSuccessCounter     prometheus.Counter
	repairSuccessCounterMin  *counter.Counter
	repairFailedCounter      prometheus.Counter
	repairFailedCounterMin   *counter.Counter
	errStatsDistribution     *base.ErrorStats
	chunkMissMigrateReporter *base.AbnormalReporter

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
	taskAPI client.TaskAPI,
) (*ShardRepairMgr, error) {
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeShardRepair.String())
	if err != nil {
		return nil, err
	}

	failMsgSender, err := base.NewMsgSender(cfg.failedProducerConfig())
	if err != nil {
		return nil, err
	}

	orphanShardsLog, err := recordlog.NewEncoder(&cfg.OrphanShardLog)
	if err != nil {
		return nil, err
	}

	mgr := &ShardRepairMgr{
		blobnodeCli:      blobnodeCli,
		clusterMgrCli:    clusterMgrCli,
		taskCli:          taskAPI,
		taskPool:         taskpool.New(cfg.TaskPoolSize, cfg.TaskPoolSize),
		taskSwitch:       taskSwitch,
		clusterTopology:  clusterTopology,
		blobnodeSelector: newIDCSelector(clusterMgrCli, cfg.ClusterID),

		kafkaConsumerClient: kafkaClient,
		failMsgSender:       failMsgSender,
		punishTime:          time.Duration(cfg.MessagePunishTimeM) * time.Minute,

		orphanShardLogger: orphanShardsLog,

		repairSuccessCounter:     base.NewCounter(cfg.ClusterID, ShardRepair, base.KindSuccess),
		repairFailedCounter:      base.NewCounter(cfg.ClusterID, ShardRepair, base.KindFailed),
		errStatsDistribution:     base.NewErrorStats(),
		repairSuccessCounterMin:  &counter.Counter{},
		repairFailedCounterMin:   &counter.Counter{},
		chunkMissMigrateReporter: base.NewAbnormalReporter(cfg.ClusterID, ShardRepair, base.ChunkMissMigrateAbnormal),

		cfg:    cfg,
		Closer: closer.New(),
	}

	return mgr, nil
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
	default:
		// do nothing
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

	if isErrDiskNotFound(err) {
		mgr.processDiskNotFoundErr(ctx, volInfo, repairMsg)
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

	// update host info and cache IDC per index
	idcByVunitIdx := make([]string, len(volInfo.VunitLocations))
	for idx := range volInfo.VunitLocations {
		location := &volInfo.VunitLocations[idx]
		disk, ok := mgr.clusterTopology.GetDisk(location.DiskID)
		if ok {
			location.Host = disk.Host
			idcByVunitIdx[idx] = disk.Idc
		}
	}

	// decide whether to split by AZ: only split when the switch is on,
	// bad shards span multiple AZs, all have known IDC, and local
	// repair is possible.
	// localRepairable is checked first because it is near-free for L=0
	// codemodes (the common single-AZ case), avoiding the map allocation
	// in groupBadIdxsByAZ when splitting is impossible anyway.
	if mgr.cfg.EnableAZSplitShardRepair &&
		localRepairable(repairMsg.BadIdx, volInfo.CodeMode) {
		azBadIdxs := mgr.groupBadIdxsByAZ(repairMsg, idcByVunitIdx)
		if len(azBadIdxs) > 1 && azBadIdxs[""] == nil {
			span.Infof("split repair by AZ: azBadIdxs[%+v]", azBadIdxs)
			return mgr.repairShardByAZ(ctx, volInfo, repairMsg, azBadIdxs)
		}
	}

	return mgr.repairShardSingle(ctx, volInfo, repairMsg, idcByVunitIdx)
}

// repairShardSingle dispatches a single repair task to one worker.
// This is the original (pre-split) behavior.
func (mgr *ShardRepairMgr) repairShardSingle(
	ctx context.Context, volInfo *client.VolumeInfoSimple,
	repairMsg *proto.ShardRepairMsg, idcByVunitIdx []string,
) (*client.VolumeInfoSimple, error) {
	workerHost := mgr.blobnodeSelector.get(ctx, mgr.resolveRepairIDC(ctx, repairMsg, idcByVunitIdx))
	if workerHost == "" {
		return volInfo, ErrBlobnodeServiceUnavailable
	}

	task := proto.ShardRepairTask{
		Bid:      repairMsg.Bid,
		CodeMode: volInfo.CodeMode,
		Sources:  volInfo.VunitLocations,
		BadIdxs:  repairMsg.BadIdx,
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

// azRepairStatus holds the result of a single AZ's repair subtask,
// sent over a buffered channel for aggregation by the caller.
// Modeled after shardPutStatus in stream_put.go.
type azRepairStatus struct {
	idc      string
	err      error
	orphan   bool
	noWorker bool
}

// repairShardByAZ dispatches per-AZ repair subtasks concurrently.
// Each AZ's bad indices are sent to a worker in that same AZ,
// ensuring local stripe reads/writes stay within the AZ.
//
// Idempotency: on partial failure, retry is safe because blobnode's
// hasRepaired + getRepairShards skip already-fixed shards.
func (mgr *ShardRepairMgr) repairShardByAZ(
	ctx context.Context, volInfo *client.VolumeInfoSimple,
	repairMsg *proto.ShardRepairMsg, azBadIdxs map[string][]uint8,
) (*client.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)

	// buffered channel: each goroutine sends exactly one status, no blocking.
	statusCh := make(chan azRepairStatus, len(azBadIdxs))

	for idc, badIdxs := range azBadIdxs {
		idc := idc
		badIdxs := badIdxs

		go func() {
			status := azRepairStatus{idc: idc}
			// defer send guarantees the status is always delivered,
			// even if the logic below panics, so the main goroutine
			// collecting from statusCh never blocks forever.
			defer func() { statusCh <- status }()

			workerHost := mgr.blobnodeSelector.get(ctx, idc)
			if workerHost == "" {
				span.Warnf("no blobnode in idc[%s], AZ-split subtask failed", idc)
				status.err = ErrBlobnodeServiceUnavailable
				status.noWorker = true
				return
			}

			task := proto.ShardRepairTask{
				Bid:      repairMsg.Bid,
				CodeMode: volInfo.CodeMode,
				Sources:  volInfo.VunitLocations, // full list: blobnode needs it for global fallback
				BadIdxs:  badIdxs,                // only this AZ's bad indices
				Reason:   repairMsg.Reason,
			}

			err := mgr.blobnodeCli.RepairShard(ctx, workerHost, task)
			if err != nil {
				status.orphan = isOrphanShard(err)
				span.Warnf("AZ-split repair failed: idc[%s], badIdxs[%+v], err[%+v]",
					idc, badIdxs, err)
			}
			status.err = err
		}()
	}

	// collect results from all AZs, then aggregate in the main goroutine.
	results := make([]azRepairStatus, 0, len(azBadIdxs))
	for range azBadIdxs {
		results = append(results, <-statusCh)
	}

	var (
		firstErr       error
		successCnt     int
		failedAZs      []string
		orphanDetected bool
	)
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			label := r.idc
			if r.noWorker {
				label += "(no-worker)"
			}
			failedAZs = append(failedAZs, label)
			if r.orphan {
				orphanDetected = true
			}
			continue
		}
		successCnt++
	}

	// log orphan shard once, regardless of how many AZs detected it
	if orphanDetected {
		mgr.saveOrphanShard(ctx, repairMsg)
	}

	if firstErr != nil {
		span.Warnf("AZ-split repair partial failure: success[%d/%d], failedAZs[%v], err[%+v]",
			successCnt, len(azBadIdxs), failedAZs, firstErr)
		return volInfo, firstErr
	}

	span.Infof("AZ-split repair success: all %d AZs repaired", successCnt)
	return volInfo, nil
}

// groupBadIdxsByAZ groups bad indices by their disk's IDC.
// Bad indices with unknown IDC (empty string) are grouped under "".
func (mgr *ShardRepairMgr) groupBadIdxsByAZ(
	repairMsg *proto.ShardRepairMsg, idcByVunitIdx []string,
) map[string][]uint8 {
	azBadIdxs := make(map[string][]uint8)
	for _, badIdx := range repairMsg.BadIdx {
		idx := int(badIdx)
		var idc string
		if idx < len(idcByVunitIdx) {
			idc = idcByVunitIdx[idx]
		}
		azBadIdxs[idc] = append(azBadIdxs[idc], badIdx)
	}
	return azBadIdxs
}

// localRepairable checks whether bad shards can be repaired via local stripe.
// This is a scheduler-side copy of blobnode's localRepairable to decide
// whether to split the repair task by AZ. The explicit `t.L == 0` early
// return is an optimization; the blobnode version achieves the same result
// implicitly because LocalStripe returns nil when L == 0. The two versions
// are behaviorally equivalent for all inputs reachable via the canSplit guard.
func localRepairable(badIdxs []uint8, mode codemode.CodeMode) bool {
	t := mode.T()
	if t.L == 0 {
		return false
	}
	localMap := make(map[int]int)
	for _, idx := range badIdxs {
		stripeIdxs, _, _ := t.LocalStripe(int(idx))
		if len(stripeIdxs) == 0 {
			return false
		}
		localMap[stripeIdxs[0]]++
	}
	for _, v := range localMap {
		if v > t.L/t.AZCount {
			return false
		}
	}
	return true
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

func isErrDiskNotFound(err error) bool {
	return rpc.DetectStatusCode(err) == errcode.CodeDiskNotFound
}

func (mgr *ShardRepairMgr) processDiskNotFoundErr(ctx context.Context, volInfo *client.VolumeInfoSimple, repairMsg *proto.ShardRepairMsg) {
	span := trace.SpanFromContextSafe(ctx)
	for _, idx := range repairMsg.BadIdx {
		vunitInfo := volInfo.VunitLocations[idx]

		if mgr.chunkMissMigrateReporter.IsVuidReported(vunitInfo.Vuid) {
			span.Warnf("chunk is miss migrate and already reported, vunitInfo: %+v", vunitInfo)
			continue
		}

		// check disk status
		disk, err := mgr.clusterMgrCli.GetDiskInfo(ctx, vunitInfo.DiskID)
		if err != nil {
			span.Errorf("get diskinfo failed, vunitInfo: %+v, err: %s", volInfo, err.Error())
			continue
		}
		// maybe disk is broken but not repaired, and restarted, retry repair next time
		if disk.Status <= proto.DiskStatusRepairing {
			continue
		}
		// disk is repaired or dropped, means volInfo is too old, get new volInfo from cm
		vol, err := mgr.clusterMgrCli.GetVolumeInfo(ctx, vunitInfo.Vuid.Vid())
		if err != nil {
			span.Errorf("get volumeinfo failed, vunitInfo: %+v, err: %s", volInfo, err.Error())
			continue
		}
		if !vol.EqualWith(volInfo) {
			continue
		}

		taskExist, err := mgr.taskCli.CheckTaskExist(ctx, proto.TaskTypeManualMigrate, vunitInfo.DiskID, vunitInfo.Vuid)
		if err != nil {
			span.Errorf("check task exist failed, vunitInfo: %+v, err: %s", volInfo, err.Error())
			continue
		}
		if taskExist {
			mgr.chunkMissMigrateReporter.SetVuidReported(vunitInfo.Vuid)
			continue
		}
		mgr.chunkMissMigrateReporter.ReportAbnormal(vunitInfo.DiskID, vunitInfo.Vuid)
		mgr.chunkMissMigrateReporter.SetVuidReported(vunitInfo.Vuid)
	}
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

// resolveRepairIDC resolves the target IDC by iterating BadIdx until it finds
// a bad index whose disk IDC is known (non-empty). Returns "" if none match.
//
// Iterating all BadIdx is strictly better than using only BadIdx[0]:
//   - If BadIdx[0] maps to a disk whose topology info is stale/missing but another
//     BadIdx has a valid disk, the repair can still target the correct AZ.
//   - If the bad shards span multiple AZs and each AZ's bad count <= L/AZCount,
//     local repair is still possible. The returned AZ determines which AZ's
//     local stripe avoids cross-AZ traffic; other AZs' stripes will still
//     incur cross-AZ reads/writes (see section 15 of shard-repair.md).
//   - Only when a single AZ's bad count > L/AZCount does localRepairable()
//     return false, causing blobnode to fall back to global repair — in that
//     case the returned AZ truly does not matter.
//
// In all cases, correctness is preserved; the worst outcome is unnecessary
// cross-AZ traffic, never data loss.
func (mgr *ShardRepairMgr) resolveRepairIDC(ctx context.Context, repairMsg *proto.ShardRepairMsg, idcByVunitIdx []string) string {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("resolveRepairIDC: repairMsg[%+v], idcByVunitIdx[%+v]", repairMsg, idcByVunitIdx)
	for _, badIdx := range repairMsg.BadIdx {
		idx := int(badIdx)
		if idx < len(idcByVunitIdx) && idcByVunitIdx[idx] != "" {
			return idcByVunitIdx[idx]
		}
	}
	return ""
}

// idcSelector provides AZ-aware worker selection for shard repair.
// Each per-IDC selector and the all-worker selector are backed by
// selector.MakeSelector, which handles interval-based refresh internally.
type idcSelector struct {
	clusterMgrCli client.ClusterMgrAPI
	clusterID     proto.ClusterID

	mu        sync.RWMutex
	selectors map[string]selector.Selector // key: idc ("" for all workers), created lazily
}

func newIDCSelector(clusterMgrCli client.ClusterMgrAPI, clusterID proto.ClusterID) *idcSelector {
	return &idcSelector{
		clusterMgrCli: clusterMgrCli,
		clusterID:     clusterID,
		selectors:     make(map[string]selector.Selector),
	}
}

// getSelector lazily creates and caches a per-IDC (or all) worker selector.
// The underlying MakeSelector handles its own interval-based background refresh.
func (s *idcSelector) getSelector(ctx context.Context, idc string) selector.Selector {
	// fast path: read-lock check, multiple readers proceed concurrently
	s.mu.RLock()
	sel := s.selectors[idc]
	s.mu.RUnlock()
	if sel != nil {
		return sel
	}

	// slow path: write-lock, double-check to prevent duplicate MakeSelector
	s.mu.Lock()
	defer s.mu.Unlock()
	if sel = s.selectors[idc]; sel != nil {
		return sel
	}

	sel = selector.MakeSelector(60*1000, func() ([]string, error) {
		bgCtx := context.Background()
		nodes, err := s.clusterMgrCli.GetService(bgCtx, proto.ServiceNameWorker, s.clusterID)
		if err != nil {
			span := trace.SpanFromContextSafe(bgCtx)
			span.Warnf("refresh worker services failed: err[%+v]", err)
			return nil, err
		}
		if idc == "" {
			hosts := make([]string, len(nodes))
			for i, n := range nodes {
				hosts[i] = n.Host
			}
			return hosts, nil
		}
		var hosts []string
		for _, n := range nodes {
			if n.Idc == idc {
				hosts = append(hosts, n.Host)
			}
		}
		return hosts, nil
	})
	s.selectors[idc] = sel
	return sel
}

// Get returns a worker host in the given IDC. If the IDC has no workers,
// it falls back to a random worker from any IDC. Returns empty if no
// workers are available at all.
func (s *idcSelector) get(ctx context.Context, idc string) string {
	span := trace.SpanFromContextSafe(ctx)
	if idc != "" {
		sel := s.getSelector(ctx, idc)
		hosts := sel.GetRandomN(1)
		if len(hosts) > 0 {
			span.Debugf("selected blobnode host[%s] for idc[%s]", hosts[0], idc)
			return hosts[0]
		}
		span.Debugf("no blobnode in idc[%s], fallback to global", idc)
	}

	sel := s.getSelector(ctx, "")
	hosts := sel.GetRandomN(1)
	if len(hosts) > 0 {
		span.Debugf("selected blobnode host[%s] from global", hosts[0])
		return hosts[0]
	}
	span.Debugf("no blobnode available at all")
	return ""
}
