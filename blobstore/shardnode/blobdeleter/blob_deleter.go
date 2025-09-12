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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
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
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	utilerr "github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

// blob delete status
const (
	deleteStatusSuccess = deleteStatus(iota)
	deleteStatusFailed
)

type ShardGetter interface {
	GetShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error)
	GetAllShards() []storage.ShardHandler
}

type BlobDelCfg struct {
	ClusterID proto.ClusterID

	MsgChannelNum        int     `json:"msg_channel_num"`
	MsgChannelSize       int     `json:"msg_channel_size"`
	FailedMsgChannelSize int     `json:"failed_msg_channel_size"`
	ProduceTaskPoolSize  int     `json:"produce_task_pool_size"`
	RateLimit            float64 `json:"rate_limit"`
	RateLimitBurst       int     `json:"rate_limit_burst"`
	MaxListMessageNum    int     `json:"max_list_message_num"`
	MaxExecuteBidNum     uint64  `json:"max_execute_bid_num"`

	SafeDeleteTimeout util.Duration `json:"safe_delete_timeout"`
	PunishTimeout     util.Duration `json:"punish_timeout"`

	DeleteLog recordlog.Config `json:"delete_log"`
}

type BlobDelMgrConfig struct {
	TaskSwitchMgr *taskswitch.SwitchMgr
	ShardGetter   ShardGetter
	Transport     base.BlobTransport
	VolCache      base.IVolumeCache
	BlobDelCfg
}

type BlobDeleteMgr struct {
	cfg                *BlobDelMgrConfig
	taskSwitch         *taskswitch.TaskSwitch
	tsGen              *base.TsGenerator
	produceTaskPool    taskpool.TaskPool
	consumeTaskPool    taskpool.TaskPool
	shardListReaderMap sync.Map

	msgChannels   []chan *delMsgExt // shared by shard, shard's channel index = shardID % len(msgChannels)
	failedMsgChan chan *delMsgExt   // shared by all consumeTasks
	bidLimiter    *rate.Limiter

	delSuccessCounterByMin *counter.Counter
	delFailCounterByMin    *counter.Counter
	errStatsDistribution   *base.ErrorStats

	delLogger recordlog.Encoder
	closer.Closer
}

func NewBlobDeleteMgr(cfg *BlobDelMgrConfig) (*BlobDeleteMgr, error) {
	msgChannels := make([]chan *delMsgExt, cfg.MsgChannelNum)
	for i := 0; i < cfg.MsgChannelNum; i++ {
		msgChannels[i] = make(chan *delMsgExt, cfg.MsgChannelSize)
	}

	taskSwitch, err := cfg.TaskSwitchMgr.AddSwitch(snproto.ShardNodeBlobDeleteTask)
	if err != nil {
		return nil, err
	}

	delLogger, err := recordlog.NewEncoder(&cfg.DeleteLog)
	if err != nil {
		return nil, err
	}

	m := &BlobDeleteMgr{
		cfg:             cfg,
		taskSwitch:      taskSwitch,
		tsGen:           base.NewTsGenerator(base.Ts(0)),
		produceTaskPool: taskpool.New(cfg.ProduceTaskPoolSize, 1),
		consumeTaskPool: taskpool.New(cfg.MsgChannelNum, 1),

		msgChannels:   msgChannels,
		failedMsgChan: make(chan *delMsgExt, cfg.FailedMsgChannelSize),
		bidLimiter:    rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateLimitBurst),

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},
		errStatsDistribution:   base.NewErrorStats(),

		delLogger: delLogger,
		Closer:    closer.New(),
	}
	go m.produceLoop()
	go m.startConsume()
	go m.retryFailedLoop()
	return m, nil
}

func (m *BlobDeleteMgr) produceLoop() {
	var (
		hasExecutableMsg        uint32
		wg                      sync.WaitGroup
		roundTerm               = 1
		longWaitDuration        = 10 * time.Minute
		retryTicker             = time.NewTicker(1 * time.Second)
		checkCleanTicker        = time.NewTicker(30 * time.Minute)
		shards                  = m.cfg.ShardGetter.GetAllShards()
		produceSpan, produceCtx = trace.StartSpanFromContext(context.Background(), "blob-delete-produce")
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

			var listReader *shardListReader
			v, ok := m.shardListReaderMap.Load(suid)
			if !ok {
				listReader = newShardListReader(shard)
				m.shardListReaderMap.Store(suid, listReader)
			} else {
				listReader = v.(*shardListReader)
			}

			m.produceTaskPool.Run(func() {
				defer wg.Done()
				_hasExecutableMsg, err := m.listShardMsg(ctx, diskID, listReader)
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

func (m *BlobDeleteMgr) cleanupDeletedShards(ctx context.Context, shards []storage.ShardHandler) {
	span := trace.SpanFromContextSafe(ctx)
	for _, shard := range shards {
		diskID, suid := shard.GetDiskID(), shard.GetSuid()

		_, err := m.getShard(diskID, suid)
		if isShardNotExistError(err) {
			m.shardListReaderMap.Delete(suid)
			span.Debugf("cleaned up deleted shard[%d] diskID[%d]", suid, diskID)
		} else if err != nil {
			span.Errorf("failed to get shard[%d] diskID[%d], err: %s", suid, diskID, err.Error())
		}
	}
}

func (m *BlobDeleteMgr) listShardMsg(ctx context.Context, diskID proto.DiskID, listReader *shardListReader) (hasExecutableMsg bool, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !m.taskSwitch.Enabled() {
		span.Debugf("task switch disabled, waiting")
		return
	}

	suid := listReader.GetSuid()
	// check if shard deleted
	_, err = m.getShard(diskID, suid)
	if err != nil {
		return
	}

	// check if shard is leader
	if !listReader.IsLeader() {
		span.Errorf("shard[%d] is not leader, skip", suid)
		listReader.init()
		return
	}

	// list message from shard storage
	protectDuration := m.cfg.SafeDeleteTimeout.Duration
	msgList, err := listReader.listMessage(ctx, protectDuration, m.cfg.MaxListMessageNum)
	if err != nil {
		span.Errorf("shard[%d] list message from storage failed, err: %s", suid, err.Error())
		return
	}

	if len(msgList) == 0 {
		return
	}

	hasExecutableMsg = true
	chanIdx := uint32(suid.ShardID()) % uint32(len(m.msgChannels))
	ch := m.msgChannels[chanIdx]

	for i := range msgList {
		select {
		case <-m.Done():
			return
		case ch <- msgList[i]:
		}
	}
	return
}

func (m *BlobDeleteMgr) startConsume() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "blob-delete-consume")
	for i := 0; i < len(m.msgChannels); i++ {
		idx := i
		m.consumeTaskPool.Run(func() {
			m.runConsumeTask(ctx, idx)
		})
	}
}

func (m *BlobDeleteMgr) runConsumeTask(ctx context.Context, index int) {
	span := trace.SpanFromContextSafe(ctx)

	batch := make([]*delMsgExt, 0, 16)
	bidNum := uint64(0)
	for {
		select {
		case <-m.Done():
			return
		case me := <-m.msgChannels[index]:
			batch = append(batch, me)
			bidNum += uint64(me.msg.Slice.Count)
			for bidNum < m.cfg.MaxExecuteBidNum {
				select {
				case next := <-m.msgChannels[index]:
					batch = append(batch, next)
					bidNum += uint64(next.msg.Slice.Count)
					continue
				default:
					goto EXECUTE
				}
			}
		EXECUTE:
			if err := m.executeDel(ctx, batch); err != nil {
				span.Errorf("delete batch failed, err: %s", errors.Detail(err))
			}
			batch = batch[:0]
			bidNum = 0
		}
	}
}

func (m *BlobDeleteMgr) retryFailedLoop() {
	for {
		select {
		case <-m.Done():
			return
		case msgExt := <-m.failedMsgChan:
			ch := m.getChan(msgExt.suid.ShardID())
			ch <- msgExt
		}
	}
}

func (m *BlobDeleteMgr) getChan(shardID proto.ShardID) chan *delMsgExt {
	return m.msgChannels[uint32(shardID)%uint32(len(m.msgChannels))]
}

func (m *BlobDeleteMgr) sentToFailedChan(ctx context.Context, msg *delMsgExt) {
	span := trace.SpanFromContextSafe(ctx)
	span.Warnf("send msgExt to failed chan: %+v", msg)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	select {
	case m.failedMsgChan <- msg:
		return
	case <-ticker.C:
		span.Warnf("send msg: %+v to failed channel timeout", msg.msg)
		return
	}
}

func (m *BlobDeleteMgr) executeDel(ctx context.Context, msgList []*delMsgExt) error {
	span := trace.SpanFromContextSafe(ctx)

	if len(msgList) == 0 {
		return nil
	}

	rets := make([]*delBlobRet, 0, len(msgList))
	for _, me := range msgList {
		r := &delBlobRet{}
		r.msgExt = me
		_, r.ctx = trace.StartSpanFromContextWithTraceID(ctx, span.OperationName(), span.TraceID()+"_"+me.msg.ReqId)
		rets = append(rets, r)

		if err := m.deleteWithCheckVolConsistency(r.ctx, me.msg.Slice.Vid, r); err != nil {
			r.err = err
			r.status = deleteStatusFailed
		}
	}

	suidDelItems := make(map[proto.Suid][]storage.BatchItemElem)
	for _, r := range rets {
		switch r.status {
		case deleteStatusSuccess:
			m.delSuccessCounterByMin.Add()
		case deleteStatusFailed:
			m.delFailCounterByMin.Add()
		default:
		}
		if r.err == nil {
			_, ok := suidDelItems[r.msgExt.suid]
			if !ok {
				suidDelItems[r.msgExt.suid] = make([]storage.BatchItemElem, 0)
			}
			suidDelItems[r.msgExt.suid] = append(suidDelItems[r.msgExt.suid], storage.NewBatchItemElemDelete(string(r.msgExt.msgKey)))
			continue
		}

		// process failed msg
		m.errStatsDistribution.AddFail(r.err)
		r.msgExt.msg.Retry++
		if r.msgExt.msg.Retry < 3 {
			m.sentToFailedChan(ctx, r.msgExt)
			continue
		}
		// retry times > 3, punish msgExt and update in store
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

func (m *BlobDeleteMgr) deleteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, ret *delBlobRet) error {
	return m.cfg.VolCache.DoubleCheckedRun(ctx, vid, func(info *snproto.VolumeInfoSimple) (newVol *snproto.VolumeInfoSimple, _err error) {
		span := trace.SpanFromContextSafe(ret.ctx)
		msg := ret.msgExt.msg
		count := msg.Slice.Count
		bids := make([]proto.BlobID, 0, count)
		startBid := msg.Slice.MinSliceID
		i := uint32(0)
		for i < count {
			bids = append(bids, startBid+proto.BlobID(i))
			i++
		}

		// set execute rets
		defer func() {
			ret.err = _err
			if _err != nil {
				span1 := trace.SpanFromContextSafe(ret.ctx)
				span1.Errorf("deleteWithCheckVolConsistency failed, err: %s, msgExt: %+v", errors.Detail(_err), ret.msgExt)
				ret.status = deleteStatusFailed
				return
			}
			ret.status = deleteStatusSuccess
		}()

		newVol = info
		for j := range bids {
			if ret.msgExt.hasDelete(bids[j]) {
				span.Debugf("vid[%d] bid[%d] already deleted", newVol.Vid, bids[j])
				continue
			}

			if _err = m.bidLimiter.Wait(ctx); _err != nil {
				_err = errors.Info(_err, "wait bid limiter failed")
				return
			}

			if !ret.msgExt.hasMarkDel(bids[j]) {
				newVol, _err = m.deleteSlice(ctx, info, ret.msgExt, bids[j], true)
				if _err != nil {
					_err = errors.Info(_err, "mark delete failed")
					return
				}
			}
			newVol, _err = m.deleteSlice(ctx, newVol, ret.msgExt, bids[j], false)
			if _err != nil {
				_err = errors.Info(_err, "delete failed")
				return
			}
			span.Debugf("delete success: vid[%d], bid[%d]", newVol.Vid, bids[j])
			// update volume info
			if !newVol.EqualWith(info) {
				span.Debugf("volume updated, newVol: %+v", newVol)
				info = newVol
			}
			// record delete log
			doc := proto.DelDoc{
				ClusterID:     m.cfg.ClusterID,
				Vid:           vid,
				Bid:           bids[j],
				Retry:         int(ret.msgExt.msg.Retry),
				Time:          ret.msgExt.msg.Time,
				ActualDelTime: time.Now().Unix(),
				ReqID:         ret.msgExt.msg.ReqId,
			}
			if docErr := m.delLogger.Encode(doc); docErr != nil {
				span.Warnf("write delete log failed: vid[%d], bid[%d], err[%s]", doc.Vid, doc.Bid, docErr.Error())
			}
		}
		return
	})
}

type delRet struct {
	vuid proto.Vuid
	err  error
}

func (m *BlobDeleteMgr) deleteSlice(ctx context.Context, volInfo *snproto.VolumeInfoSimple, ext *delMsgExt, bid proto.BlobID, markerDel bool) (*snproto.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)
	var err error

	retChan := make(chan delRet, len(volInfo.VunitLocations))
	for i := range volInfo.VunitLocations {
		index := i
		go func() {
			_err := m.deleteShard(ctx, volInfo.VunitLocations[index], bid, ext, markerDel)
			retChan <- delRet{vuid: volInfo.VunitLocations[index].Vuid, err: _err}
		}()
	}

	// collect results
	needRetryVuids := make([]proto.Vuid, 0)
	for i := 0; i < len(volInfo.VunitLocations); i++ {
		r := <-retChan
		if r.err == nil {
			continue
		}
		if shouldUpdateVolumeErr(rpc2.DetectStatusCode(r.err)) {
			needRetryVuids = append(needRetryVuids, r.vuid)
			continue
		}
		err = r.err
	}

	if len(needRetryVuids) < 1 || err != nil {
		return volInfo, err
	}

	// get new volume info
	newVolume, err := m.cfg.VolCache.GetVolume(volInfo.Vid)
	if err != nil {
		err = errors.Info(err, "get new volume info failed")
		return volInfo, err
	}

	// equal, no need to retry
	if newVolume.EqualWith(volInfo) {
		return newVolume, err
	}

	span.Debugf("volume updated, retry vuids: %+v", needRetryVuids)
	for _, oldVuid := range needRetryVuids {
		if err = m.deleteShard(ctx, newVolume.VunitLocations[oldVuid.Index()], bid, ext, markerDel); err != nil {
			return newVolume, err
		}
	}
	return newVolume, nil
}

func (m *BlobDeleteMgr) deleteShard(ctx context.Context, info proto.VunitLocation, bid proto.BlobID, ext *delMsgExt, markerDel bool) error {
	span := trace.SpanFromContextSafe(ctx)
	var err error
	var stage deleteStage

	defer func() {
		if shouldBackToInitStage(rpc2.DetectStatusCode(err)) {
			stage = InitStage
			ext.setShardDelStage(bid, info.Vuid, stage)
		}
		if err != nil && markerDel {
			ext.setShardDelStage(bid, info.Vuid, InitStage)
			return
		}
		if err != nil {
			return
		}
		ext.setShardDelStage(bid, info.Vuid, stage)
	}()

	if markerDel && ext.hasShardMarkDel(bid, info.Vuid) ||
		!markerDel && ext.hasShardDelete(bid, info.Vuid) {
		span.Debugf("vuid[%d] bid[%d] already deleted, markerDel: %v", info.Vuid, bid, markerDel)
		return nil
	}

	if markerDel {
		stage = DeleteStageMarkDelete
		err = m.cfg.Transport.MarkDeleteSlice(ctx, info, bid)
	} else {
		stage = DeleteStageDelete
		err = m.cfg.Transport.DeleteSlice(ctx, info, bid)
	}

	if err != nil {
		errCode := rpc2.DetectStatusCode(err)
		if assumeDeleteSuccess(errCode) {
			span.Debugf("delete bid failed but assume success: bid[%d], location[%+v], err[%+v] ",
				bid, info, err)
			err = nil
			return err
		}
	}
	return err
}

func (m *BlobDeleteMgr) punish(ctx context.Context, msgExt *delMsgExt) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Warnf("punish delete msg: %+v", msgExt)
	msg := msgExt.msg

	v, ok := m.shardListReaderMap.Load(msgExt.suid)
	if !ok {
		span.Warnf("shard[%d] has been deleted", msgExt.suid)
		return nil
	}
	shard := v.(*shardListReader)
	_, _, _, shardKeys, err := decodeDelMsgKey(msgExt.msgKey, shard.ShardingSubRangeCount())
	if err != nil {
		return errors.Info(err, "decode shardKeys failed")
	}

	punishDr := m.cfg.PunishTimeout.Duration
	ts := m.tsGen.CurrentTs().Add(punishDr)
	punishMsgKey := encodeDelMsgKey(ts, msg.Slice.Vid, msg.Slice.MinSliceID, shardKeys)

	msg.Retry = 0
	msg.Time = ts.TimeUnix()
	itm, err := delMsgToItem(punishMsgKey, msg)
	if err != nil {
		return errors.Info(err, "delMsgToItem failed")
	}

	oldDeleteItem := storage.NewBatchItemElemDelete(string(msgExt.msgKey))
	newInsertItem := storage.NewBatchItemElemInsert(itm)

	h := storage.OpHeader{
		ShardKeys: shardKeys,
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

func (m *BlobDeleteMgr) clearShardMessages(ctx context.Context, suid proto.Suid, items []storage.BatchItemElem) {
	span := trace.SpanFromContextSafe(ctx)
	if len(items) == 0 {
		return
	}

	v, ok := m.shardListReaderMap.Load(suid)
	if !ok {
		span.Errorf("shard[%d] has been deleted", suid)
		return
	}
	shard := v.(*shardListReader)
	_, _, _, shardKeys, err := decodeDelMsgKey([]byte(items[0].ID), shard.ShardingSubRangeCount())
	if err != nil {
		span.Errorf("shard[%d] decode shardKeys failed, key: %+v, err: %s", suid, []byte(items[0].ID), err.Error())
		return
	}

	h := storage.OpHeader{
		ShardKeys: shardKeys,
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

func (m *BlobDeleteMgr) enabled() bool {
	return m.taskSwitch.Enabled()
}

func (m *BlobDeleteMgr) getTaskStats() (success, failed [counter.SLOT]int) {
	return m.delSuccessCounterByMin.Show(), m.delFailCounterByMin.Show()
}

func (m *BlobDeleteMgr) getErrorStats() (errStats []string, totalErrCnt uint64) {
	statsResult, totalErrCnt := m.errStatsDistribution.Stats()
	return base.FormatPrint(statsResult), totalErrCnt
}

func (m *BlobDeleteMgr) getShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error) {
	sh, err := m.cfg.ShardGetter.GetShard(diskID, suid)
	if err != nil {
		return nil, err
	}
	return sh, err
}

func (m *BlobDeleteMgr) insertDeleteMsg(ctx context.Context, req *snapi.DeleteBlobRawArgs) error {
	shard, err := m.getShard(req.Header.DiskID, req.Header.Suid)
	if err != nil {
		return err
	}

	shardKeys := req.GetShardKeys(shard.ShardingSubRangeCount())
	itm, err := m.sliceToDeleteMsgItemRaw(ctx, req.Slice, shardKeys)
	if err != nil {
		return err
	}

	oph := storage.OpHeader{
		RouteVersion: req.Header.RouteVersion,
		ShardKeys:    shardKeys,
	}
	return shard.InsertItem(ctx, oph, []byte(itm.ID), itm)
}

func (m *BlobDeleteMgr) slicesToDeleteMsgItems(ctx context.Context, slices []proto.Slice, shardKeys []string) ([]snapi.Item, error) {
	span := trace.SpanFromContextSafe(ctx)
	items := make([]snapi.Item, len(slices))
	for i := range slices {
		ts := m.tsGen.GenerateTs()
		key := encodeDelMsgKey(ts, slices[i].Vid, slices[i].MinSliceID, shardKeys)
		msg := snproto.DeleteMsg{
			Slice:       slices[i],
			Time:        ts.TimeUnix(),
			ReqId:       span.TraceID(),
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		}

		itm, err := delMsgToItem(key, msg)
		if err != nil {
			return nil, err
		}
		items[i] = itm
	}
	return items, nil
}

func (m *BlobDeleteMgr) sliceToDeleteMsgItemRaw(ctx context.Context, slices proto.Slice, shardKeys []string) (snapi.Item, error) {
	span := trace.SpanFromContextSafe(ctx)
	ts := m.tsGen.GenerateTs()
	key := encodeDelMsgKey(ts, slices.Vid, slices.MinSliceID, shardKeys)

	msg := snproto.DeleteMsg{
		Slice:       slices,
		Time:        ts.TimeUnix(),
		ReqId:       span.TraceID(),
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}

	itm, err := delMsgToItem(key, msg)
	if err != nil {
		return snapi.Item{}, err
	}
	return itm, nil
}

type deleteStatus int

type delBlobRet struct {
	status deleteStatus
	msgExt *delMsgExt
	ctx    context.Context
	err    error
}

func isShardNotExistError(err error) bool {
	apiErr := utilerr.Cause(err)
	return rpc2.DetectStatusCode(apiErr) == apierr.CodeShardDoesNotExist
}

// for error code judgment
func shouldUpdateVolumeErr(errCode int) bool {
	return errCode == apierr.CodeDiskBroken ||
		errCode == apierr.CodeVuidNotFound ||
		errCode == apierr.CodeDiskNotFound
}

func shouldBackToInitStage(errCode int) bool {
	// 653: errcode.CodeShardNotMarkDelete
	return errCode == apierr.CodeShardNotMarkDelete
}

func assumeDeleteSuccess(errCode int) bool {
	return errCode == apierr.CodeBidNotFound ||
		errCode == apierr.CodeShardMarkDeleted
}
