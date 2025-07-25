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
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/closer"
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
	MsgChannelNum        int     `json:"msg_channel_num"`
	MsgChannelSize       int     `json:"msg_channel_size"`
	FailedMsgChannelSize int     `json:"failed_msg_channel_size"`
	TaskPoolSize         int     `json:"task_pool_size"`
	RateLimit            float64 `json:"rate_limit"`
	RateLimitBurst       int     `json:"rate_limit_burst"`
	MaxListMessageNum    int     `json:"max_list_message_num"`
	MaxExecuteBidNum     uint64  `json:"max_execute_bid_num"`
	SafeDeleteTimeoutH   int     `json:"safe_delete_timeout_h"`
	PunishTimeoutH       int     `json:"punish_timeout_h"`
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
	msgKeyGen          *base.MsgKeyGenerator
	produceTaskPool    taskpool.TaskPool
	consumeTaskPool    taskpool.TaskPool
	shardListReaderMap sync.Map

	msgChannels   []chan *delMsgExt // shared by shard, shard's channel index = shardID % len(msgChannels)
	failedMsgChan chan *delMsgExt   // shared by all consumeTasks
	bidLimiter    *rate.Limiter

	delSuccessCounterByMin *counter.Counter
	delFailCounterByMin    *counter.Counter

	closer.Closer
}

func NewBlobDeleteMgr(cfg *BlobDelMgrConfig) (*BlobDeleteMgr, error) {
	msgChannels := make([]chan *delMsgExt, cfg.MsgChannelNum)
	for i := 0; i < cfg.MsgChannelNum; i++ {
		msgChannels[i] = make(chan *delMsgExt, cfg.MsgChannelSize)
	}

	taskPoolSize := cfg.TaskPoolSize
	// taskPool Size should equal or bigger than len(msgChannels)
	if taskPoolSize*taskPoolSize < cfg.MsgChannelSize {
		taskPoolSize = cfg.MsgChannelSize
	}

	taskSwitch, err := cfg.TaskSwitchMgr.AddSwitch(snproto.ShardNodeBlobDeleteTask)
	if err != nil {
		return nil, err
	}

	m := &BlobDeleteMgr{
		cfg:             cfg,
		taskSwitch:      taskSwitch,
		msgKeyGen:       base.NewMsgKeyGenerator(base.Ts(0)),
		produceTaskPool: taskpool.New(taskPoolSize, 1),
		consumeTaskPool: taskpool.New(taskPoolSize, 1),

		msgChannels:   msgChannels,
		failedMsgChan: make(chan *delMsgExt, cfg.FailedMsgChannelSize),
		bidLimiter:    rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateLimitBurst),

		delSuccessCounterByMin: &counter.Counter{},
		delFailCounterByMin:    &counter.Counter{},

		Closer: closer.New(),
	}
	go m.produceLoop()
	go m.consumeLoop()
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
		span, ctx := trace.StartSpanFromContext(produceCtx, produceSpan.TraceID()+"-"+strconv.Itoa(roundTerm))
		if len(shards) == 0 {
			span.Warnf("no shards available, waiting")
			resetTicker2LongWait()
			continue
		}
		for i := 0; i < len(shards); i++ {
			wg.Add(1)
			diskID, suid := shards[i].GetDiskID(), shards[i].GetSuid()
			m.produceTaskPool.Run(func() {
				defer wg.Done()
				_hasExecutableMsg, err := m.listShardMsg(ctx, diskID, suid)
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

func (m *BlobDeleteMgr) listShardMsg(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (hasExecutableMsg bool, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !m.taskSwitch.Enabled() {
		span.Debugf("task switch disabled, waiting")
		return
	}

	var (
		listReader *shardListReader
		sh         storage.ShardHandler
	)

	// check if shard deleted
	sh, err = m.getShard(diskID, suid)
	if err != nil {
		return
	}

	v, ok := m.shardListReaderMap.Load(suid)
	if !ok {
		listReader = newShardListReader(sh)
		m.shardListReaderMap.Store(suid, listReader)
	} else {
		listReader = v.(*shardListReader)
	}

	// check if shard is leader
	if !sh.IsLeader() {
		span.Errorf("shard[%d] is not leader, skip", suid)
		listReader.init()
		return
	}

	// list message from shard storage
	protectDuration := time.Duration(m.cfg.SafeDeleteTimeoutH) * time.Hour
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

func (m *BlobDeleteMgr) consumeLoop() {
	i := 0
	for {
		select {
		case <-m.Done():
			return
		default:
		}
		idx := i % len(m.msgChannels)
		m.consumeTaskPool.Run(func() {
			m.runConsumeTask(idx)
		})
		i++
	}
}

func (m *BlobDeleteMgr) runConsumeTask(index int) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "blob-delete:consume")

	bidNum := uint64(0)
	batch := make([]*delMsgExt, 0)
	defer func() {
		if err := m.executeDel(ctx, batch); err != nil {
			span.Errorf("delete batch failed, err: %s", err.Error())
		}
	}()

	for {
		select {
		case me := <-m.msgChannels[index]:
			if uint64(me.msg.Slice.Count)+bidNum < m.cfg.MaxExecuteBidNum {
				batch = append(batch, me)
				bidNum += uint64(me.msg.Slice.Count)
				continue
			}
			batch = append(batch, me)
			return
		default:
			return
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
	suidShardKeys := make(map[proto.Suid][][]byte)
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
			suidDelItems[r.msgExt.suid] = append(suidDelItems[r.msgExt.suid], storage.NewBatchItemElemDelete(r.msgExt.msgKey))
			suidShardKeys[r.msgExt.suid] = r.msgExt.shardKeys
			continue
		}
		r.msgExt.msg.Retry++
		if r.msgExt.msg.Retry < 3 {
			m.sentToFailedChan(ctx, r.msgExt)
			continue
		}
		// retry times > 3, punish msgExt and update in store
		span1 := trace.SpanFromContextSafe(r.ctx)
		span1.Warnf("punish msgExt: %+v", r.msgExt)
		if err := m.punish(ctx, r.msgExt); err != nil {
			return err
		}
	}

	// clear finish delete messages in storage
	for suid, items := range suidDelItems {
		m.clearShardMessages(ctx, suid, suidShardKeys[suid], items)
	}
	return nil
}

func (m *BlobDeleteMgr) deleteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, ret *delBlobRet) error {
	return m.cfg.VolCache.DoubleCheckedRun(ctx, vid, func(info *snproto.VolumeInfoSimple) (newVol *snproto.VolumeInfoSimple, _err error) {
		msg := ret.msgExt.msg
		count := msg.Slice.Count
		bids := make([]proto.BlobID, 0, count)
		startBid := msg.Slice.MinSliceID
		i := uint32(0)
		for i < count {
			bids = append(bids, startBid+proto.BlobID(i))
			i++
		}
		stageMgr := newDeleteStageMgr()
		// set blob del stage from msgExt
		stageMgr.setMsgDelStage(msg.MsgDelStage)

		// set execute rets
		defer func() {
			ret.msgExt.msg.MsgDelStage = stageMgr.getMsgDelStage()
			ret.err = _err
			if _err != nil {
				span1 := trace.SpanFromContextSafe(ret.ctx)
				span1.Errorf("batch delete failed, err: %s, msgExt: %+v", _err.Error(), ret.msgExt)
				ret.status = deleteStatusFailed
				return
			}
			ret.status = deleteStatusSuccess
		}()

		for j := range bids {
			if stageMgr.hasDelete(bids[j]) {
				continue
			}

			if _err = m.bidLimiter.Wait(ctx); _err != nil {
				return
			}

			if !stageMgr.hasMarkDel(bids[j]) {
				newVol, _err = m.deleteSlice(ctx, info, stageMgr, bids[j], true)
				if _err != nil {
					return
				}
			}

			newVol, _err = m.deleteSlice(ctx, newVol, stageMgr, bids[j], false)
			if _err != nil {
				return
			}
		}
		return
	})
}

type delRet struct {
	vuid proto.Vuid
	err  error
}

func (m *BlobDeleteMgr) deleteSlice(ctx context.Context, volInfo *snproto.VolumeInfoSimple, stageMgr *deleteStageMgr, bid proto.BlobID, markerDel bool) (*snproto.VolumeInfoSimple, error) {
	var err error
	retChan := make(chan delRet, len(volInfo.VunitLocations))

	for i := range volInfo.VunitLocations {
		index := i
		go func() {
			err = m.deleteShard(ctx, volInfo.VunitLocations[index], bid, stageMgr, markerDel)
			retChan <- delRet{vuid: volInfo.VunitLocations[index].Vuid, err: err}
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
		// other error, return directly
		err = r.err
		return volInfo, err
	}

	if len(needRetryVuids) < 1 {
		return volInfo, err
	}

	// get new volume info
	newVolume, err := m.cfg.VolCache.GetVolume(volInfo.Vid)
	if err != nil {
		return volInfo, err
	}

	// equal, no need to retry
	if newVolume.EqualWith(volInfo) {
		return newVolume, err
	}

	for _, oldVuid := range needRetryVuids {
		if err = m.deleteShard(ctx, newVolume.VunitLocations[oldVuid.Index()], bid, stageMgr, markerDel); err != nil {
			return newVolume, err
		}
	}
	return newVolume, nil
}

func (m *BlobDeleteMgr) deleteShard(ctx context.Context, info proto.VunitLocation, bid proto.BlobID, stageMgr *deleteStageMgr, markerDel bool) error {
	span := trace.SpanFromContextSafe(ctx)
	var err error
	var stage deleteStage

	defer func() {
		if shouldBackToInitStage(rpc2.DetectStatusCode(err)) {
			stage = InitStage
			stageMgr.setShardDelStage(bid, info.Vuid, stage)
		}
		if err != nil {
			return
		}
		stageMgr.setShardDelStage(bid, info.Vuid, stage)
	}()

	if markerDel && stageMgr.hasShardMarkDel(bid, info.Vuid) ||
		!markerDel && stageMgr.hasShardDelete(bid, info.Vuid) {
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
	msg := msgExt.msg

	v, ok := m.shardListReaderMap.Load(msgExt.suid)
	if !ok {
		span.Errorf("shard[%d] has been deleted", msgExt.suid)
		return nil
	}

	punishDr := time.Duration(m.cfg.PunishTimeoutH) * time.Hour
	ts := m.msgKeyGen.CurrentTs().Add(punishDr)
	punishMsgKey := m.msgKeyGen.EncodeDelMsgKeyWithTs(ts, msg.Slice.Vid, msg.Slice.MinSliceID, msgExt.shardKeys)

	msg.Retry = 0
	msg.Time = ts.TimeUnix()
	itm, err := delMsgToItem(punishMsgKey, msg)
	if err != nil {
		return err
	}

	oldDeleteItem := storage.NewBatchItemElemDelete(msgExt.msgKey)
	newInsertItem := storage.NewBatchItemElemInsert(itm)

	shard := v.(*shardListReader)
	h := storage.OpHeader{
		ShardKeys: msgExt.shardKeys,
	}

	for i := 0; i < 3; i++ {
		h.RouteVersion = shard.GetRouteVersion()
		// no need to check if shard is leader, if not leader propose as follower
		err = shard.BatchWriteItem(ctx, h, []storage.BatchItemElem{oldDeleteItem, newInsertItem}, storage.WithProposeAsFollower())
		if err != nil {
			if rpc2.DetectStatusCode(err) != apierr.CodeShardRouteVersionNeedUpdate {
				span.Errorf("shard[%d] clear finish failed, err: %s", msgExt.suid, err.Error())
				return err
			}
			continue
		}
		return nil
	}
	return err
}

func (m *BlobDeleteMgr) clearShardMessages(ctx context.Context, suid proto.Suid, shardKeys [][]byte, items []storage.BatchItemElem) {
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
	h := storage.OpHeader{
		ShardKeys: shardKeys,
	}

	var err error
	for i := 0; i < 3; i++ {
		h.RouteVersion = shard.GetRouteVersion()
		// no need to check if shard is leader, if not leader propose as follower
		err = shard.BatchWriteItem(ctx, h, items, storage.WithProposeAsFollower())
		if err != nil {
			if rpc2.DetectStatusCode(err) != apierr.CodeShardRouteVersionNeedUpdate {
				span.Errorf("shard[%d] clear finish failed, err: %s", suid, err.Error())
				return
			}
			continue
		}
		return
	}

	if err != nil {
		span.Errorf("shard[%d] clear finish failed, err: %s", suid, err.Error())
	}
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

	itm, shardKeys, err := m.sliceToDeleteMsgItemRaw(ctx, req.Slice, shard.ShardingSubRangeCount())
	if err != nil {
		return err
	}

	oph := storage.OpHeader{
		RouteVersion: req.Header.RouteVersion,
		ShardKeys:    shardKeys,
	}
	return shard.InsertItem(ctx, oph, itm.ID, itm)
}

func (m *BlobDeleteMgr) slicesToDeleteMsgItems(ctx context.Context, slices []proto.Slice, shardKeys [][]byte) ([]snapi.Item, error) {
	span := trace.SpanFromContextSafe(ctx)
	items := make([]snapi.Item, len(slices))
	for i := range slices {
		ts, key := m.msgKeyGen.EncodeDelMsgKey(slices[i].Vid, slices[i].MinSliceID, shardKeys)
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

func (m *BlobDeleteMgr) sliceToDeleteMsgItemRaw(ctx context.Context, slices proto.Slice, tagNum int) (snapi.Item, [][]byte, error) {
	span := trace.SpanFromContextSafe(ctx)
	ts, key, shardKeys := m.msgKeyGen.EncodeRawDelMsgKey(slices.Vid, slices.MinSliceID, tagNum)

	msg := snproto.DeleteMsg{
		Slice:       slices,
		Time:        ts.TimeUnix(),
		ReqId:       span.TraceID(),
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}

	itm, err := delMsgToItem(key, msg)
	if err != nil {
		return snapi.Item{}, nil, err
	}
	return itm, shardKeys, nil
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
