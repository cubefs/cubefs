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

package message

import (
	"context"
	"fmt"
	"time"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type (
	BlobDelMgrConfig MessageMgrConfig
	BlobDeleteMgr    struct {
		*messageMgr
	}
)

func NewBlobDeleteMgr(cfg *BlobDelMgrConfig) (*BlobDeleteMgr, error) {
	msgCfg := (*MessageMgrConfig)(cfg)
	if msgCfg.messageType == 0 {
		msgCfg.messageType = snproto.MessageTypeDelete
	}

	deleteMgr := &BlobDeleteMgr{}
	msgCfg.executor = deleteMgr
	msgCfg.reporter = base.NewDeleteBlobTaskReporter(msgCfg.ClusterID)
	msgMgr, err := newMessageMgr(msgCfg)
	if err != nil {
		return nil, err
	}

	deleteMgr.messageMgr = msgMgr
	deleteMgr.Start()

	return deleteMgr, nil
}

func (m *BlobDeleteMgr) Start() {
	m.messageMgr.run()
}

func (m *BlobDeleteMgr) SlicesToDeleteMsgItems(ctx context.Context, slices []proto.Slice, shardKeys []string) ([]snapi.Item, error) {
	return m.slicesToDeleteMsgItems(ctx, slices, shardKeys)
}

func (m *BlobDeleteMgr) Delete(ctx context.Context, req *snapi.DeleteBlobRawArgs) error {
	return m.insertDeleteMsg(ctx, req)
}

func (m *BlobDeleteMgr) Stats() *snapi.ShardnodeTaskStatsRet {
	deleteSuccessCounter, deleteFailedCounter := m.getTaskStats()
	delErrStats, delTotalErrCnt := m.getErrorStats()
	return &snapi.ShardnodeTaskStatsRet{
		Enable:        m.enabled(),
		SuccessPerMin: fmt.Sprint(deleteSuccessCounter),
		FailedPerMin:  fmt.Sprint(deleteFailedCounter),
		TotalErrCnt:   delTotalErrCnt,
		ErrStats:      delErrStats,
	}
}

func (m *BlobDeleteMgr) ItemToMessageExt(item interface{}) (snproto.MessageExt, error) {
	msgItem, ok := item.(messageItem)
	if !ok {
		return nil, errors.New("invalid item")
	}

	msg, err := itemToDeleteMsg(msgItem.item)
	if err != nil {
		return nil, err
	}

	return &delMsgExt{
		msg:    msg,
		suid:   msgItem.suid,
		msgKey: []byte(msgItem.item.ID),
	}, nil
}

func (m *BlobDeleteMgr) ExecuteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, ret interface{}) error {
	return m.cfg.VolCache.DoubleCheckedRun(ctx, vid, func(info *snproto.VolumeInfoSimple) (newVol *snproto.VolumeInfoSimple, _err error) {
		executeRet, ok := ret.(*executeRet)
		if !ok {
			return nil, errors.New("invalid execute ret")
		}

		me, ok := executeRet.msgExt.(*delMsgExt)
		if !ok {
			return nil, errors.New("not a delete message")
		}

		count := me.msg.Slice.Count
		ids := make([]proto.BlobID, 0, count)
		startId := me.msg.Slice.MinSliceID
		i := uint32(0)
		for i < count {
			ids = append(ids, startId+proto.BlobID(i))
			i++
		}

		ctx := executeRet.ctx
		span := trace.SpanFromContextSafe(ctx)

		// set execute rets
		defer func() {
			executeRet.err = _err
			if _err != nil {
				span1 := trace.SpanFromContextSafe(ctx)
				span1.Errorf("deleteWithCheckVolConsistency failed, err: %s, msgExt: %s", errors.Detail(_err), executeRet.msgExt)
				executeRet.status = executeStatusFailed
				return
			}
			executeRet.status = executeStatusSuccess
		}()

		newVol = info
		for j := range ids {
			if me.hasDelete(ids[j]) {
				span.Debugf("vid[%d] sliceId[%d] already deleted", newVol.Vid, ids[j])
				continue
			}

			if _err = m.limiter.Wait(ctx); _err != nil {
				_err = errors.Info(_err, "wait slice limiter failed")
				return
			}

			if !me.hasMarkDel(ids[j]) {
				newVol, _err = m.deleteSlice(ctx, info, me, ids[j], true)
				if _err != nil {
					_err = errors.Info(_err, "mark delete failed")
					return
				}
			}

			newVol, _err = m.deleteSlice(ctx, newVol, me, ids[j], false)
			if _err != nil {
				_err = errors.Info(_err, "delete failed")
				return
			}
			span.Debugf("delete success: vid[%d], sliceId[%d]", newVol.Vid, ids[j])

			// update volume info
			if !newVol.EqualWith(info) {
				span.Debugf("volume updated, newVol: %+v", newVol)
				info = newVol
			}

			// record delete log
			doc := proto.DelDoc{
				ClusterID:     m.cfg.ClusterID,
				Vid:           vid,
				Bid:           ids[j],
				Retry:         int(executeRet.msgExt.GetRetry()),
				Time:          executeRet.msgExt.GetTime(),
				ActualDelTime: time.Now().Unix(),
				ReqID:         executeRet.msgExt.GetReqId(),
			}
			if docErr := m.executeLogger.Encode(doc); docErr != nil {
				span.Warnf("write delete log failed: vid[%d], sliceId[%d], err[%s]", doc.Vid, doc.Bid, docErr.Error())
			}
		}
		return
	})
}

type delRet struct {
	vuid proto.Vuid
	err  error
}

var errVunitLengthNotEqual = errors.New("vunit length not equal")

func (m *BlobDeleteMgr) deleteSlice(ctx context.Context, volInfo *snproto.VolumeInfoSimple, ext *delMsgExt, sliceId proto.BlobID, markerDel bool) (*snproto.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)
	var err error

	retChan := make(chan delRet, len(volInfo.VunitLocations))
	for i := range volInfo.VunitLocations {
		index := i
		go func() {
			_err := m.deleteSliceUnit(ctx, volInfo.VunitLocations[index], sliceId, ext, markerDel)
			retChan <- delRet{vuid: volInfo.VunitLocations[index].Vuid, err: _err}
		}()
	}

	// collect results
	needRetryVuids := make([]proto.Vuid, 0)
	for i := 0; i < len(volInfo.VunitLocations); i++ {
		ret := <-retChan
		if ret.err == nil {
			continue
		}
		err = ret.err
		errCode := rpc2.DetectStatusCode(err)
		if shouldUpdateVolumeErr(errCode) || errorDialTimeout(err) || errorConnectionRefused(err) {
			span.Errorf("delete shard failed will retry: sliceId[%d], vuid[%d], markDelete[%+v], code[%d], err[%+v]",
				sliceId, ret.vuid, markerDel, errCode, err)
			needRetryVuids = append(needRetryVuids, ret.vuid)
			continue
		}
		span.Warnf("delete shard failed: sliceId[%d], vuid[%d], markDelete[%+v], code[%d], err[%+v]",
			sliceId, ret.vuid, markerDel, errCode, err)
		return volInfo, err
	}

	if len(needRetryVuids) == 0 {
		return volInfo, nil
	}

	span.Infof("slice delete will update and retry: len updateAndRetryShards[%d]", len(needRetryVuids))
	// get new volume info
	newVolume, updateVolErr := m.cfg.VolCache.UpdateVolume(volInfo.Vid)
	if updateVolErr != nil || newVolume.EqualWith(volInfo) {
		span.Warnf("new volInfo is same or clusterTopology.UpdateVolume failed: vid[%d], err[%+v]", volInfo.Vid, updateVolErr)
		return volInfo, err
	}

	if len(newVolume.VunitLocations) != len(volInfo.VunitLocations) {
		span.Warnf("vid locations len not equal: vid[%d], old len[%d], new len[%d]", len(volInfo.VunitLocations), len(newVolume.VunitLocations))
		return volInfo, errVunitLengthNotEqual
	}

	for _, oldVuid := range needRetryVuids {
		span.Debugf("start retry delete shard: sliceId[%d], vuid[%d]", sliceId, oldVuid)
		if err = m.deleteSliceUnit(ctx, newVolume.VunitLocations[oldVuid.Index()], sliceId, ext, markerDel); err != nil {
			return newVolume, err
		}
	}
	return newVolume, nil
}

func (m *BlobDeleteMgr) deleteSliceUnit(ctx context.Context, info proto.VunitLocation, sliceId proto.BlobID, ext *delMsgExt, markerDel bool) error {
	span := trace.SpanFromContextSafe(ctx)
	var err error
	var stage deleteStage

	defer func() {
		if shouldBackToInitStage(rpc2.DetectStatusCode(err)) {
			stage = InitStage
			ext.setSliceUnitDelStage(sliceId, info.Vuid, stage)
			return
		}
		if err != nil && markerDel {
			ext.setSliceUnitDelStage(sliceId, info.Vuid, InitStage)
			return
		}
		if err != nil {
			return
		}
		// already deleted
		if stage == InitStage {
			return
		}
		ext.setSliceUnitDelStage(sliceId, info.Vuid, stage)
	}()

	if markerDel && ext.hasSliceUnitMarkDel(sliceId, info.Vuid) || ext.hasSliceUnitDelete(sliceId, info.Vuid) {
		span.Debugf("vuid[%d] sliceId[%d] already deleted, markerDel: %v", info.Vuid, sliceId, markerDel)
		return nil
	}

	if markerDel {
		stage = DeleteStageMarkDelete
		err = m.cfg.BlobTransport.MarkDeleteSliceUnit(ctx, info, sliceId)
	} else {
		stage = DeleteStageDelete
		err = m.cfg.BlobTransport.DeleteSliceUnit(ctx, info, sliceId)
	}

	if err != nil {
		errCode := rpc2.DetectStatusCode(err)
		if assumeDeleteSuccess(errCode) {
			span.Debugf("delete slice failed but assume success: sliceId[%d], location[%+v], err[%+v] ",
				sliceId, info, err)
			err = nil
			return err
		}
	}
	return err
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
	msgKey := newMsgKey()
	defer msgKey.release()

	msgKey.setMsgType(snproto.MessageTypeDelete)
	msgKey.setTier(snproto.TierSingleIdx)
	msgKey.setShardKeys(shardKeys)

	for i := range slices {
		ts := m.tsGen.GenerateTs()
		msgKey.setTs(ts)
		msgKey.setVid(slices[i].Vid)
		msgKey.setBid(slices[i].MinSliceID)
		key := msgKey.encode()

		msg := snproto.DeleteMsg{
			Slice:       slices[i],
			Time:        ts.TimeUnix(),
			ReqId:       span.TraceID(),
			MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
		}

		itm, err := deleteMsgToItem(key, msg)
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

	msgKey := newMsgKey()
	defer msgKey.release()

	msgKey.setMsgType(snproto.MessageTypeDelete)
	msgKey.setTier(snproto.TierSingleIdx)
	msgKey.setTs(ts)
	msgKey.setVid(slices.Vid)
	msgKey.setBid(slices.MinSliceID)
	msgKey.setShardKeys(shardKeys)
	key := msgKey.encode()

	msg := snproto.DeleteMsg{
		Slice:       slices,
		Time:        ts.TimeUnix(),
		ReqId:       span.TraceID(),
		MsgDelStage: make(map[uint64]snproto.BlobDeleteStage),
	}

	itm, err := deleteMsgToItem(key, msg)
	if err != nil {
		return snapi.Item{}, err
	}
	return itm, nil
}

func itemToDeleteMsg(itm snapi.Item) (msg snproto.DeleteMsg, err error) {
	var msgRaw []byte
	for i := range itm.Fields {
		if itm.Fields[i].ID == snproto.DeleteBlobMsgFieldID {
			msgRaw = itm.Fields[i].Value
			break
		}
	}
	if len(msgRaw) < 1 {
		return msg, errors.New("empty delete message data in item")
	}

	msg = snproto.DeleteMsg{}
	if err = msg.Unmarshal(msgRaw); err != nil {
		return
	}
	return
}

func deleteMsgToItem(key []byte, msg snproto.DeleteMsg) (itm snapi.Item, err error) {
	raw, err := msg.Marshal()
	if err != nil {
		return
	}
	itm = snapi.Item{
		ID: string(key),
		Fields: []snapi.Field{
			{
				ID:    snproto.DeleteBlobMsgFieldID,
				Value: raw,
			},
		},
	}
	return
}

func shouldBackToInitStage(errCode int) bool {
	// 653: errcode.CodeShardNotMarkDelete
	return errCode == apierr.CodeShardNotMarkDelete
}

func assumeDeleteSuccess(errCode int) bool {
	return errCode == apierr.CodeBidNotFound ||
		errCode == apierr.CodeShardMarkDeleted
}
