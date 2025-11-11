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

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/blobstore/util/selector"
)

// ErrBlobnodeServiceUnavailable worker service unavailable
var ErrBlobnodeServiceUnavailable = errors.New("blobnode service unavailable")

// OrphanSlice orphan slice identification.
type OrphanSlice struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Vid       proto.Vid       `json:"vid"`
	Bid       proto.BlobID    `json:"bid"`
}

type SliceRepairMgrConfig struct {
	*MessageMgrConfig
	Transport        base.Transport
	BlobNodeSelector selector.Selector
	SCClient         scheduler.ITaskInfoNotifier
}

// SliceRepairMgr handles repair messages
type SliceRepairMgr struct {
	*messageMgr
	transport                base.Transport
	scClient                 scheduler.ITaskInfoNotifier
	blobNodeSelector         selector.Selector
	chunkMissMigrateReporter *base.AbnormalReporter
}

func NewSliceRepairMgr(cfg *SliceRepairMgrConfig) (*SliceRepairMgr, error) {
	if cfg.messageType == 0 {
		cfg.messageType = snproto.MessageTypeRepair
	}

	repairMgr := &SliceRepairMgr{}
	cfg.executor = repairMgr

	msgMgr, err := newMessageMgr(cfg.MessageMgrConfig)
	if err != nil {
		return nil, err
	}

	repairMgr.messageMgr = msgMgr
	repairMgr.Start()

	return repairMgr, nil
}

func (m *SliceRepairMgr) Start() {
	m.messageMgr.run()
}

func (r *SliceRepairMgr) Repair(ctx context.Context, req *snapi.RepairSliceArgs) error {
	return r.insertRepairMsg(ctx, req)
}

func (r *SliceRepairMgr) Stats() *snapi.ShardnodeTaskStatsRet {
	repairSuccessCounter, repairFailedCounter := r.getTaskStats()
	repairErrStats, repairTotalErrCnt := r.getErrorStats()
	return &snapi.ShardnodeTaskStatsRet{
		Enable:        r.enabled(),
		SuccessPerMin: fmt.Sprint(repairSuccessCounter),
		FailedPerMin:  fmt.Sprint(repairFailedCounter),
		TotalErrCnt:   repairTotalErrCnt,
		ErrStats:      repairErrStats,
	}
}

func (m *SliceRepairMgr) ItemToMessageExt(item interface{}) (snproto.MessageExt, error) {
	msgItem, ok := item.(messageItem)
	if !ok {
		return nil, errors.New("invalid item")
	}
	msg, err := itemToRepairMsg(msgItem.item)
	if err != nil {
		return nil, err
	}
	return &repairMsgExt{
		msg:    msg,
		suid:   msgItem.suid,
		msgKey: []byte(msgItem.item.ID),
	}, nil
}

// ExecuteWithCheckVolConsistency implements MessageExecutor interface for SliceRepairMgr
func (m *SliceRepairMgr) ExecuteWithCheckVolConsistency(ctx context.Context, vid proto.Vid, ret interface{}) error {
	return m.cfg.VolCache.DoubleCheckedRun(ctx, vid, func(info *snproto.VolumeInfoSimple) (newVol *snproto.VolumeInfoSimple, _err error) {
		executeRet, ok := ret.(*executeRet)
		if !ok {
			return nil, errors.New("invalid execute ret")
		}

		me, ok := executeRet.msgExt.(*repairMsgExt)
		if !ok {
			return nil, errors.New("not a repair message")
		}
		return m.tryRepair(executeRet.ctx, info, &me.msg)
	})
}

func (m *SliceRepairMgr) tryRepair(ctx context.Context, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) (*snproto.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)

	newVol, err := m.repairSlice(ctx, volInfo, repairMsg)
	if err == nil {
		return newVol, nil
	}

	if err == ErrBlobnodeServiceUnavailable {
		return volInfo, err
	}

	if isErrDiskNotFound(err) {
		m.processDiskNotFoundErr(ctx, volInfo, repairMsg)
	}

	newVol, err1 := m.cfg.VolCache.UpdateVolume(volInfo.Vid)
	if err1 != nil || newVol.EqualWith(volInfo) {
		// if update volInfo failed or volInfo not updated, don't need retry
		span.Warnf("new volInfo is same or clusterTopology.UpdateVolume failed: vid[%d], vol cache update err[%+v], repair err[%+v]",
			volInfo.Vid, err1, err)
		return volInfo, err
	}

	return m.repairSlice(ctx, newVol, repairMsg)
}

func (m *SliceRepairMgr) repairSlice(ctx context.Context, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) (*snproto.VolumeInfoSimple, error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("repair slice: msg[%+v], vol info[%+v]", repairMsg, volInfo)

	hosts := m.blobNodeSelector.GetRandomN(1)
	if len(hosts) == 0 {
		return volInfo, ErrBlobnodeServiceUnavailable
	}
	workerHost := hosts[0]

	err := m.cfg.BlobTransport.RepairSlice(ctx, workerHost, volInfo, repairMsg)
	if err == nil {
		return volInfo, nil
	}

	if isOrphanSlice(err) {
		m.saveOrphanSlice(ctx, repairMsg)
	}

	return volInfo, err
}

func (m *SliceRepairMgr) saveOrphanSlice(ctx context.Context, repairMsg *snproto.SliceRepairMsg) {
	span := trace.SpanFromContextSafe(ctx)

	slice := OrphanSlice{
		ClusterID: m.cfg.ClusterID,
		Vid:       repairMsg.Vid,
		Bid:       repairMsg.Bid,
	}
	span.Infof("save orphan slice: [%+v]", slice)

	insistOn(ctx, "save orphan slice", func() error {
		return m.executeLogger.Encode(slice)
	})
}

func (m *SliceRepairMgr) processDiskNotFoundErr(ctx context.Context, volInfo *snproto.VolumeInfoSimple, repairMsg *snproto.SliceRepairMsg) {
	span := trace.SpanFromContextSafe(ctx)
	for _, idx := range repairMsg.BadIdx {
		vunitInfo := volInfo.VunitLocations[idx]

		if m.chunkMissMigrateReporter.IsVuidReported(vunitInfo.Vuid) {
			span.Warnf("chunk is miss migrate and already reported, vunitInfo: %+v", vunitInfo)
			continue
		}

		// check disk status
		disk, err := m.transport.GetBlobnodeDiskInfo(ctx, vunitInfo.DiskID)
		if err != nil {
			span.Errorf("get diskinfo failed, vunitInfo: %+v, err: %s", volInfo, err.Error())
			continue
		}
		// maybe disk is broken but not repaired, and restarted, retry repair next time
		if disk.Status <= proto.DiskStatusRepairing {
			continue
		}
		// disk is repaired or dropped, means volInfo is too old, get new volInfo from cm
		vol, err := m.transport.GetVolumeInfo(ctx, vunitInfo.Vuid.Vid())
		if err != nil {
			span.Errorf("get volumeinfo failed, vunitInfo: %+v, err: %s", volInfo, err.Error())
			continue
		}
		if !vol.EqualWith(volInfo) {
			continue
		}

		ret, err := m.scClient.CheckTaskExist(ctx, &scheduler.CheckTaskExistArgs{
			TaskType: proto.TaskTypeManualMigrate,
			DiskID:   vunitInfo.DiskID,
			Vuid:     vunitInfo.Vuid,
		})
		if err != nil {
			span.Errorf("check task exist failed, vunitInfo: %+v, err: %s", volInfo, err.Error())
			continue
		}
		if ret.Exist {
			m.chunkMissMigrateReporter.SetVuidReported(vunitInfo.Vuid)
			continue
		}
		m.chunkMissMigrateReporter.ReportAbnormal(vunitInfo.DiskID, vunitInfo.Vuid)
		m.chunkMissMigrateReporter.SetVuidReported(vunitInfo.Vuid)
	}
}

func (m *SliceRepairMgr) insertRepairMsg(ctx context.Context, req *snapi.RepairSliceArgs) error {
	shard, err := m.getShard(req.Header.DiskID, req.Header.Suid)
	if err != nil {
		return err
	}

	span := trace.SpanFromContextSafe(ctx)
	ts := m.tsGen.GenerateTs()
	shardKeys := req.GetShardKeys(shard.ShardingSubRangeCount())
	tier := snproto.TierSingleIdx
	if len(req.BadIdx) > 1 {
		tier = snproto.TierMultiIdx
	}

	mk := newMsgKey()
	defer mk.release()

	mk.msgType = snproto.MessageTypeRepair
	mk.tier = tier
	mk.ts = ts
	mk.vid = req.Vid
	mk.bid = req.Bid
	mk.shardKeys = shardKeys
	key := mk.encode()

	msg := snproto.SliceRepairMsg{
		Bid:    req.Bid,
		Vid:    req.Vid,
		BadIdx: req.BadIdx,
		Reason: req.Reason,
		ReqId:  span.TraceID(),
	}

	raw, err := msg.Marshal()
	if err != nil {
		return err
	}
	itm := snapi.Item{
		ID: string(key),
		Fields: []snapi.Field{
			{
				ID:    snproto.SliceRepairMsgFieldID,
				Value: raw,
			},
		},
	}

	oph := storage.OpHeader{
		RouteVersion: req.Header.RouteVersion,
		ShardKeys:    shardKeys,
	}
	return shard.InsertItem(ctx, oph, []byte(itm.ID), itm)
}

func isOrphanSlice(err error) bool {
	return rpc2.DetectStatusCode(err) == apierr.CodeOrphanShard
}

func isErrDiskNotFound(err error) bool {
	return rpc2.DetectStatusCode(err) == apierr.CodeDiskNotFound
}

func insistOn(ctx context.Context, errMsg string, on func() error) {
	span := trace.SpanFromContextSafe(ctx)
	attempt := 0
	retry.Insist(time.Second, on, func(err error) {
		attempt++
		span.Errorf("insist attempt-%d: %s %s", attempt, errMsg, err.Error())
	})
}

func itemToRepairMsg(itm snapi.Item) (msg snproto.SliceRepairMsg, err error) {
	var msgRaw []byte
	for i := range itm.Fields {
		if itm.Fields[i].ID == snproto.SliceRepairMsgFieldID {
			msgRaw = itm.Fields[i].Value
			break
		}
	}
	if len(msgRaw) < 1 {
		return msg, errors.New("empty repair message data in item")
	}

	msg = snproto.SliceRepairMsg{}
	if err = msg.Unmarshal(msgRaw); err != nil {
		return
	}
	return
}
