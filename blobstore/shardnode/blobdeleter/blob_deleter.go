// Copyright 2025 The CubeFS Authors.
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

package blobdeleter

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// execute status
type executeStatus int

const (
	executeStatusSuccess = executeStatus(iota)
	executeStatusFailed
)

type executeRet struct {
	status executeStatus
	msgExt snproto.MessageExt
	ctx    context.Context
	err    error
}

type ShardGetter interface {
	GetShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error)
	GetAllShards() []storage.ShardHandler
}

// MessageExecutor BlobDeleteMgr and ShardRepairMgr implement this interface
type MessageExecutor interface {
	ItemToMessageExt(item interface{}) (snproto.MessageExt, error)
	ExecuteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, ret interface{}) error
}

type MessageCfg struct {
	ClusterID proto.ClusterID `json:"-"`

	RetryTimes           int              `json:"retry_times"`             // retry times for failed messages
	ProduceTaskPoolSize  int              `json:"produce_task_pool_size"`  // number of tasks to produce messages(shared by shards)
	FailedMsgChannelSize int              `json:"failed_msg_channel_size"` // size of failed message channel(shared by all tiers)
	RateLimit            float64          `json:"rate_limit"`              // rate limit for delete messages(shared by all tiers)
	RateLimitBurst       int              `json:"rate_limit_burst"`        // burst rate limit for delete messages(shared by all tiers)
	MaxExecuteSliceNum   uint64           `json:"max_execute_slice_num"`   // max number of slices to execute in one batch
	TierConfig           TierConfig       `json:"tier_config"`             // tier configuration for message processing
	MessageLog           recordlog.Config `json:"message_log"`
}

type MessageMgrConfig struct {
	messageType   snproto.MessageType
	executor      MessageExecutor
	TaskSwitchMgr *taskswitch.SwitchMgr
	ShardGetter   ShardGetter
	Transport     base.BlobTransport
	VolCache      base.IVolumeCache
	MessageCfg
}

type messageMgr struct {
	cfg               *MessageMgrConfig
	taskSwitch        *taskswitch.TaskSwitch
	tsGen             *base.TsGenerator
	produceTaskPool   taskpool.TaskPool
	topShardReaderMap sync.Map // map[proto.Suid]*topShardListReader

	msgChannelsByTier      map[snproto.MessageTier]chan snproto.MessageExt
	consumeTaskPoolsByTier map[snproto.MessageTier]taskpool.TaskPool
	failedMsgChan          chan snproto.MessageExt // shared by all consumeTasks
	limiter                *rate.Limiter

	executeSuccessCounterByMin *counter.Counter
	executeFailCounterByMin    *counter.Counter
	errStatsDistribution       *base.ErrorStats

	executeLogger recordlog.Encoder
	closer.Closer
}

func newMessageMgr(cfg *MessageMgrConfig) (*messageMgr, error) {
	cfg.TierConfig.build(cfg.messageType)

	msgChannelsByTier := make(map[snproto.MessageTier]chan snproto.MessageExt)
	consumeTaskPoolsByTier := make(map[snproto.MessageTier]taskpool.TaskPool)

	for t := range cfg.TierConfig.tierArgs {
		_cfg, ok := cfg.TierConfig.tierArgs[t]
		if !ok {
			log.Panicf("tier[%d] concurrency config not found", t)
		}
		ch := make(chan snproto.MessageExt, _cfg.channelSize)
		msgChannelsByTier[t] = ch
		consumeTaskPoolsByTier[t] = taskpool.New(_cfg.taskPoolSize, 1)
	}

	taskSwitchName := snproto.ShardNodeBlobDeleteTask
	if cfg.messageType == snproto.MessageTypeRepair {
		taskSwitchName = snproto.ShardNodeSliceRepairTask
	}
	taskSwitch, err := cfg.TaskSwitchMgr.AddSwitch(taskSwitchName)
	if err != nil {
		return nil, err
	}

	logger, err := recordlog.NewEncoder(&cfg.MessageLog)
	if err != nil {
		return nil, err
	}

	m := &messageMgr{
		cfg:             cfg,
		taskSwitch:      taskSwitch,
		tsGen:           base.NewTsGenerator(base.Ts(0)),
		produceTaskPool: taskpool.New(cfg.ProduceTaskPoolSize, 1),

		msgChannelsByTier:      msgChannelsByTier,
		consumeTaskPoolsByTier: consumeTaskPoolsByTier,
		failedMsgChan:          make(chan snproto.MessageExt, cfg.FailedMsgChannelSize),
		limiter:                rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateLimitBurst),

		executeSuccessCounterByMin: &counter.Counter{},
		executeFailCounterByMin:    &counter.Counter{},
		errStatsDistribution:       base.NewErrorStats(),

		executeLogger: logger,
		Closer:        closer.New(),
	}
	return m, nil
}

func (m *messageMgr) run() {
	go m.produceLoop()
	go m.startConsume()
	go m.retryFailedLoop()
}

func (m *messageMgr) produceLoop() {
	var (
		hasExecutableMsg        uint32
		wg                      sync.WaitGroup
		roundTerm               = 1
		longWaitDuration        = 10 * time.Minute
		retryTicker             = time.NewTicker(1 * time.Second)
		checkCleanTicker        = time.NewTicker(30 * time.Minute)
		shards                  = m.cfg.ShardGetter.GetAllShards()
		produceSpan, produceCtx = trace.StartSpanFromContext(context.Background(), "message-produce")
	)
	resetTicker2Now := func() {
		retryTicker.Reset(1)
	}
	resetTicker2LongWait := func() {
		retryTicker.Reset(longWaitDuration)
	}
	for {
		select {
		case <-m.Done():
			produceSpan.Info("produceLoop stopped")
			return
		case <-checkCleanTicker.C:
			m.cleanupDeletedShards(produceCtx, shards)
			// update shards
			shards = m.cfg.ShardGetter.GetAllShards()
		case <-retryTicker.C:
			roundTerm++
		}
		m.taskSwitch.WaitEnable()
		span, ctx := trace.StartSpanFromContextWithTraceID(produceCtx, "", produceSpan.TraceID()+"-"+strconv.Itoa(roundTerm))
		if len(shards) == 0 {
			span.Warnf("no shards available, waiting")
			resetTicker2LongWait()
			continue
		}
		for i := 0; i < len(shards); i++ {
			wg.Add(1)
			shard, diskID, suid := shards[i], shards[i].GetDiskID(), shards[i].GetSuid()

			var topReader *tierShardListReader
			v, ok := m.topShardReaderMap.Load(suid)
			if !ok {
				topReader = newTierShardListReader(shard, &m.cfg.TierConfig)
				m.topShardReaderMap.Store(suid, topReader)
			} else {
				topReader = v.(*tierShardListReader)
			}

			m.produceTaskPool.Run(func() {
				defer wg.Done()
				_hasExecutableMsg, err := m.listShardMsg(ctx, diskID, topReader)
				if err != nil {
					span.Errorf("list shard message failed, suid[%d] diskID[%d], err: %s", suid, diskID, err)
					return
				}
				if !_hasExecutableMsg {
					span.Debugf("no executable message for shard[%d] diskID[%d]", suid, diskID)
					return
				}
				atomic.StoreUint32(&hasExecutableMsg, 1)
			})
		}
		wg.Wait()
		// do retry immediately when there has any executable msg
		if atomic.CompareAndSwapUint32(&hasExecutableMsg, 1, 0) {
			resetTicker2Now()
			continue
		}
		resetTicker2LongWait()
	}
}

func (m *messageMgr) cleanupDeletedShards(ctx context.Context, shards []storage.ShardHandler) {
	span := trace.SpanFromContextSafe(ctx)
	for _, shard := range shards {
		diskID, suid := shard.GetDiskID(), shard.GetSuid()

		_, err := m.getShard(diskID, suid)
		if isShardNotExistError(err) {
			m.topShardReaderMap.Delete(suid)
			span.Debugf("cleaned up deleted shard[%d] diskID[%d]", suid, diskID)
		} else if err != nil {
			span.Errorf("failed to get shard[%d] diskID[%d], err: %s", suid, diskID, err.Error())
		}
	}
}

func (m *messageMgr) listShardMsg(ctx context.Context, diskID proto.DiskID, topReader *tierShardListReader) (hasExecutableMsg bool, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !m.taskSwitch.Enabled() {
		span.Debugf("task switch disabled, waiting")
		return
	}

	suid := topReader.GetSuid()
	_, err = m.getShard(diskID, suid)
	if err != nil {
		return
	}

	if !topReader.IsLeader() {
		span.Debugf("shard[%d] is not leader, skip", suid)
		topReader.init()
		return
	}

	tierMap, err := topReader.listMessageByTier(ctx, &m.cfg.TierConfig)
	if err != nil {
		span.Errorf("shard[%d] list message from storage failed, err: %s", suid, err.Error())
		return
	}

	totalMsgs := 0
	for _, msgs := range tierMap {
		totalMsgs += len(msgs)
	}
	if totalMsgs == 0 {
		return
	}

	hasExecutableMsg = true
	for tier, msgs := range tierMap {
		if len(msgs) == 0 {
			continue
		}

		t := tier
		msgsCopy := msgs
		go func() {
			ch := m.getChan(t)
			for i := range msgsCopy {
				me, err := m.cfg.executor.ItemToMessageExt(msgsCopy[i])
				if err != nil {
					span.Errorf("convert item to messageExt failed, err: %s", err.Error())
					continue
				}
				select {
				case <-m.Done():
					return
				case ch <- me:
				}
			}
		}()
	}
	return
}

func (m *messageMgr) startConsume() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "message-consume")
	for tier := range m.msgChannelsByTier {
		t := tier
		go m.runConsumeTask(ctx, t)
	}
}

func (m *messageMgr) runConsumeTask(ctx context.Context, tier snproto.MessageTier) {
	span := trace.SpanFromContextSafe(ctx)

	batch := make([]snproto.MessageExt, 0, 16)
	n := uint64(0)
	ch := m.getChan(tier)

	for {
		select {
		case <-m.Done():
			return
		case me := <-ch:
			batch = append(batch, me)
			n += me.GetBidNum()
			for n < m.cfg.MaxExecuteSliceNum {
				select {
				case next := <-ch:
					batch = append(batch, next)
					n += next.GetBidNum()
					continue
				default:
					goto EXECUTE
				}
			}
		EXECUTE:
			batchCopy := make([]snproto.MessageExt, len(batch))
			copy(batchCopy, batch)
			m.consumeTaskPoolsByTier[tier].Run(func() {
				if err := m.execute(ctx, batchCopy); err != nil {
					span.Errorf("delete batch failed, err: %s", errors.Detail(err))
				}
			})
			batch = batch[:0]
			n = 0
		}
	}
}

func (m *messageMgr) retryFailedLoop() {
	for {
		select {
		case <-m.Done():
			return
		case msgExt := <-m.failedMsgChan:
			t := msgExt.GetTier(m.cfg.RetryTimes)
			ch := m.getChan(t)
			ch <- msgExt
		}
	}
}

func (m *messageMgr) getChan(tier snproto.MessageTier) chan snproto.MessageExt {
	return m.msgChannelsByTier[tier]
}

func (m *messageMgr) sentToFailedChan(ctx context.Context, msg snproto.MessageExt) {
	span := trace.SpanFromContextSafe(ctx)
	span.Warnf("send msgExt: %s to failed chan", msg)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	select {
	case m.failedMsgChan <- msg:
		return
	case <-ticker.C:
		span.Warnf("send msgExt: %s to failed channel timeout", msg)
		return
	}
}

func (m *messageMgr) execute(ctx context.Context, msgList []snproto.MessageExt) error {
	span := trace.SpanFromContextSafe(ctx)

	if len(msgList) == 0 {
		return nil
	}

	rets := make([]*executeRet, 0, len(msgList))
	for _, me := range msgList {
		r := &executeRet{}
		r.msgExt = me
		_, r.ctx = trace.StartSpanFromContextWithTraceID(ctx, span.OperationName(), span.TraceID()+"_"+me.GetReqId())
		rets = append(rets, r)

		if m.cfg.executor == nil {
			span.Errorf("executor is nil, cannot execute message: %s", me)
			r.err = errors.New("executor is nil")
			r.status = executeStatusFailed
			continue
		}
		if err := m.cfg.executor.ExecuteWithCheckVolConsistency(r.ctx, me.GetVid(), r); err != nil {
			r.err = err
			r.status = executeStatusFailed
		}
	}

	suidDelItems := make(map[proto.Suid][]storage.BatchItemElem)
	for _, r := range rets {
		switch r.status {
		case executeStatusSuccess:
			m.executeSuccessCounterByMin.Add()
		case executeStatusFailed:
			m.executeFailCounterByMin.Add()
		default:
		}
		if r.err == nil {
			suid := r.msgExt.GetSuid()
			_, ok := suidDelItems[suid]
			if !ok {
				suidDelItems[suid] = make([]storage.BatchItemElem, 0)
			}
			suidDelItems[suid] = append(suidDelItems[suid], storage.NewBatchItemElemDelete(string(r.msgExt.GetMsgKey())))
			continue
		}

		// process failed msg
		m.errStatsDistribution.AddFail(r.err)
		r.msgExt.AddRetry()
		if r.msgExt.GetRetry()%m.cfg.RetryTimes != 0 {
			m.sentToFailedChan(ctx, r.msgExt)
			continue
		}

		// retry times % m.cfg.RetryTimes == 0, punish msgExt and update in store
		if err := m.punish(ctx, r.msgExt); err != nil {
			return err
		}
	}

	// clear finish delete messages in storage
	for suid, items := range suidDelItems {
		m.clearShardMessages(ctx, suid, items)
	}
	return nil
}

func (m *messageMgr) punish(ctx context.Context, msgExt snproto.MessageExt) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Warnf("punish message: %s", msgExt)

	suid := msgExt.GetSuid()
	v, ok := m.topShardReaderMap.Load(suid)
	if !ok {
		span.Warnf("shard[%d] has been deleted", suid)
		return nil
	}
	shard := v.(*tierShardListReader)

	mk := newMsgKey()
	defer mk.release()

	oldMsgKey := msgExt.GetMsgKey()
	mk.setKey(oldMsgKey)
	err := mk.decode(shard.ShardingSubRangeCount())
	if err != nil {
		return errors.Info(err, "decode shardKeys failed")
	}

	ts := m.tsGen.GenerateTs()
	mk.setMsgType(msgExt.GetMsgType())
	mk.setTier(msgExt.GetTier(m.cfg.RetryTimes))
	mk.setTs(ts)
	mk.setVid(msgExt.GetVid())
	mk.setBid(msgExt.GetBid())
	punishMsgKey := mk.encode()

	msgExt.SetTime(ts.TimeUnix())
	itm, err := messageExtToItem(punishMsgKey, msgExt)
	if err != nil {
		return errors.Info(err, "delMsgToItem failed")
	}

	oldDeleteItem := storage.NewBatchItemElemDelete(string(oldMsgKey))
	newInsertItem := storage.NewBatchItemElemInsert(itm)

	h := storage.OpHeader{
		ShardKeys: mk.shardKeys,
	}
	for i := 0; i < 3; i++ {
		h.RouteVersion = shard.GetRouteVersion()
		// no need to check if shard is leader, if not leader propose as follower
		err = shard.BatchWriteItem(ctx, h, []storage.BatchItemElem{oldDeleteItem, newInsertItem}, storage.WithProposeAsFollower())
		if err != nil {
			if rpc2.DetectStatusCode(err) != apierr.CodeShardRouteVersionNeedUpdate {
				return errors.Info(err, "clear finish failed")
			}
			continue
		}
		return nil
	}
	return err
}

func (m *messageMgr) clearShardMessages(ctx context.Context, suid proto.Suid, items []storage.BatchItemElem) {
	span := trace.SpanFromContextSafe(ctx)
	if len(items) == 0 {
		return
	}

	v, ok := m.topShardReaderMap.Load(suid)
	if !ok {
		span.Errorf("shard[%d] has been deleted", suid)
		return
	}
	shard := v.(*tierShardListReader)

	mk := newMsgKey()
	defer mk.release()

	mk.setKey([]byte(items[0].ID))
	err := mk.decode(shard.ShardingSubRangeCount())
	if err != nil {
		span.Errorf("shard[%d] decode shardKeys failed, key: %+v, err: %s", suid, []byte(items[0].ID), err.Error())
		return
	}

	h := storage.OpHeader{
		ShardKeys: mk.shardKeys,
	}
	for i := 0; i < 3; i++ {
		h.RouteVersion = shard.GetRouteVersion()
		// no need to check if shard is leader, if not leader propose as follower
		err = shard.BatchWriteItem(ctx, h, items, storage.WithProposeAsFollower())
		if err != nil {
			if rpc2.DetectStatusCode(err) != apierr.CodeShardRouteVersionNeedUpdate {
				span.Errorf("shard[%d] clear finish failed, header: %+v, err: %s", suid, h, err.Error())
				return
			}
			continue
		}
		span.Debugf("shard[%d] clear finish success, count: %d", suid, len(items))
		return
	}

	if err != nil {
		span.Errorf("shard[%d] clear finish failed, err: %s", suid, err.Error())
	}
}

func (m *messageMgr) enabled() bool {
	return m.taskSwitch.Enabled()
}

func (m *messageMgr) getTaskStats() (success, failed [counter.SLOT]int) {
	return m.executeSuccessCounterByMin.Show(), m.executeFailCounterByMin.Show()
}

func (m *messageMgr) getErrorStats() (errStats []string, totalErrCnt uint64) {
	statsResult, totalErrCnt := m.errStatsDistribution.Stats()
	return base.FormatPrint(statsResult), totalErrCnt
}

func (m *messageMgr) getShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error) {
	sh, err := m.cfg.ShardGetter.GetShard(diskID, suid)
	if err != nil {
		return nil, err
	}
	return sh, err
}

func isShardNotExistError(err error) bool {
	apiErr := errors.Cause(err)
	return rpc2.DetectStatusCode(apiErr) == apierr.CodeShardDoesNotExist
}

// for error code judgment
func shouldUpdateVolumeErr(errCode int) bool {
	return errCode == apierr.CodeDiskBroken ||
		errCode == apierr.CodeVuidNotFound ||
		errCode == apierr.CodeDiskNotFound
}

func errorDialTimeout(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "dial")
}

func errorConnectionRefused(err error) bool {
	return strings.Contains(err.Error(), "connection refused")
}
