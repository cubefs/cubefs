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

package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	shardnodeapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	shardnodeproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	shardStatusNormal        = shardStatus(1)
	shardStatusStopReadWrite = shardStatus(2)

	needReadIndex   = 0
	noNeedReadIndex = 1
)

var errShardStopWriting = errors.New("shard stop writing")

type ValGetter interface {
	Read(p []byte) (n int, err error)
	Value() []byte
	Size() int
	Close()
}

type (
	batchItemOp   uint8
	BatchItemElem struct {
		opType batchItemOp
		shardnode.Item
	}
)

const (
	batchItemOpInsert = iota
	batchItemOpUpdate
	batchItemOpDelete
)

func NewBatchItemElemInsert(itm shardnode.Item) BatchItemElem {
	return BatchItemElem{
		opType: batchItemOpInsert,
		Item:   itm,
	}
}

func NewBatchItemElemDelete(id []byte) BatchItemElem {
	return BatchItemElem{
		opType: batchItemOpDelete,
		Item: shardnode.Item{
			ID: id,
		},
	}
}

type (
	ShardBlobHandler interface {
		CreateBlob(ctx context.Context, h OpHeader, name []byte, b proto.Blob) (proto.Blob, error)
		UpdateBlob(ctx context.Context, h OpHeader, name []byte, b proto.Blob) error
		DeleteBlob(ctx context.Context, h OpHeader, name []byte, items []shardnode.Item) error
		GetBlob(ctx context.Context, h OpHeader, name []byte) (proto.Blob, error)
		ListBlob(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64) (blobs []proto.Blob, nextMarker []byte, err error)
	}
	ShardItemHandler interface {
		// item
		InsertItem(ctx context.Context, h OpHeader, id []byte, i shardnode.Item) error
		UpdateItem(ctx context.Context, h OpHeader, id []byte, i shardnode.Item) error
		DeleteItem(ctx context.Context, h OpHeader, id []byte) error
		GetItem(ctx context.Context, h OpHeader, id []byte) (shardnode.Item, error)
		ListItem(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64) (items []shardnode.Item, nextMarker []byte, err error)
		BatchWriteItem(ctx context.Context, h OpHeader, elems []BatchItemElem, opts ...ShardOptionFunc) error
	}
	ShardHandler interface {
		ShardItemHandler
		ShardBlobHandler
		GetRouteVersion() proto.RouteVersion
		TransferLeader(ctx context.Context, diskID proto.DiskID) error
		Checkpoint(ctx context.Context) error
		Stats(ctx context.Context, readIndex bool) (shardnode.ShardStats, error)
		GetSuid() proto.Suid
		GetDiskID() proto.DiskID
		GetUnits() []clustermgr.ShardUnit
		CheckAndClearShard(ctx context.Context) error
		ShardingSubRangeCount() int
		IsLeader() bool
	}
	OpHeader struct {
		RouteVersion proto.RouteVersion
		ShardKeys    [][]byte
	}

	ShardBaseConfig struct {
		RaftSnapTransmitConfig RaftSnapshotTransmitConfig `json:"raft_snap_transmit_config"`
		TruncateWalLogInterval uint64                     `json:"truncate_wal_log_interval"`
		Transport              base.ShardTransport
	}

	shardConfig struct {
		*ShardBaseConfig
		suid         proto.Suid
		diskID       proto.DiskID
		shardInfo    shardInfo
		store        *store.Store
		raftManager  raft.Manager
		addrResolver raft.AddressResolver
		disk         *Disk
	}

	shardStatus uint8
)

func newShard(ctx context.Context, cfg shardConfig) (s *shard, err error) {
	span := trace.SpanFromContext(ctx)
	span.Infof("new shard with config: %+v", cfg)

	s = &shard{
		suid:   cfg.suid,
		diskID: cfg.diskID,

		store: cfg.store,
		shardKeys: &shardKeysGenerator{
			suid: cfg.suid,
		},
		disk: cfg.disk,
		cfg:  cfg.ShardBaseConfig,
	}
	s.shardInfoMu.shardInfo = cfg.shardInfo

	// initial members
	members := make([]raft.Member, 0, len(cfg.shardInfo.Units))
	for _, node := range cfg.shardInfo.Units {
		nodeHost, err := cfg.addrResolver.Resolve(ctx, uint64(node.DiskID))
		if err != nil {
			return nil, err
		}
		memberCtx := shardnodeproto.ShardMemberCtx{
			Suid: node.GetSuid(),
		}
		raw, _err := memberCtx.Marshal()
		if _err != nil {
			return nil, _err
		}
		members = append(members, raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost.String(),
			Type:    raft.MemberChangeType_AddMember,
			Learner: node.Learner,
			Context: raw,
		})
	}
	span.Debugf("shard members: %+v", members)

	s.raftGroup, err = cfg.raftManager.CreateRaftGroup(context.Background(), &raft.GroupConfig{
		// Note: set raft group id with shard id as all shard node share the same shard id
		ID:      uint64(cfg.shardInfo.ShardID),
		Applied: cfg.shardInfo.AppliedIndex,
		Members: members,
		SM:      (*shardSM)(s),
	})
	if err != nil {
		return
	}
	s.shardState.readIndexFunc = func(ctx context.Context) error {
		return s.raftGroup.ReadIndex(ctx)
	}

	if len(members) == 1 {
		err = s.raftGroup.Campaign(ctx)
	}
	span.Infof("add Shard success, shardID[%d],suid[%d],diskID[%d]", cfg.shardInfo.ShardID, cfg.suid, cfg.diskID)

	return
}

type shard struct {
	suid   proto.Suid
	diskID proto.DiskID

	shardState  shardState
	shardInfoMu struct {
		sync.RWMutex
		shardInfo

		leader             proto.DiskID
		lastStableIndex    uint64
		lastTruncatedIndex uint64
	}
	// add disk ref for finalizer gc
	disk      *Disk
	shardKeys *shardKeysGenerator
	store     *store.Store
	raftGroup raft.Group
	cfg       *ShardBaseConfig
}

func (s *shard) InsertItem(ctx context.Context, h OpHeader, id []byte, i shardnode.Item) error {
	span := trace.SpanFromContextSafe(ctx)
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	internalItem := protoItemToInternalItem(i)
	kv, err := initKV(s.shardKeys.encodeItemKey(id), &io.LimitedReader{R: rpc2.Codec2Reader(&internalItem), N: int64(internalItem.Size())})
	defer kv.Release()
	if err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   raftOpInsertItem,
		Data: kv.Marshal(),
	}
	resp, err := s.raftGroup.Propose(ctx, &proposalData)
	if err != nil {
		return err
	}
	appendTrackLogAfterPropose(span, resp.Data)
	return nil
}

func (s *shard) UpdateItem(ctx context.Context, h OpHeader, id []byte, i shardnode.Item) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	internalItem := protoItemToInternalItem(i)
	kv, err := initKV(s.shardKeys.encodeItemKey(id), &io.LimitedReader{R: rpc2.Codec2Reader(&internalItem), N: int64(internalItem.Size())})
	defer kv.Release()
	if err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   raftOpUpdateItem,
		Data: kv.Marshal(),
	}
	_, err = s.raftGroup.Propose(ctx, &proposalData)

	return err
}

func (s *shard) GetItem(ctx context.Context, h OpHeader, id []byte) (protoItem shardnode.Item, err error) {
	vg, err := s.get(ctx, h, s.shardKeys.encodeItemKey(id))
	if err != nil {
		return
	}

	itm := &item{}
	if err = itm.Unmarshal(vg.Value()); err != nil {
		vg.Close()
		return
	}
	vg.Close()

	protoItem.ID = itm.ID
	// transform into external item
	protoItem.Fields = internalFieldsToProtoFields(itm.Fields)
	return
}

func (s *shard) GetItems(ctx context.Context, h OpHeader, keys [][]byte) (ret []shardnode.Item, err error) {
	if err := s.checkShardOptHeader(h); err != nil {
		return nil, err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return nil, convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	store := s.store.KVStore()
	vgs, err := store.MultiGet(ctx, dataCF, keys, nil)
	if err != nil {
		return
	}

	ret = make([]shardnode.Item, len(vgs))
	for i := range ret {
		item := &item{}
		err = item.Unmarshal(vgs[i].Value())
		vgs[i].Close()
		if err != nil {
			for j := i + 1; j < len(vgs); j++ {
				vgs[j].Close()
			}
			return
		}
		ret[i] = shardnode.Item{
			ID:     item.ID,
			Fields: internalFieldsToProtoFields(item.Fields),
		}
	}

	return
}

func (s *shard) ListItem(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64) (items []shardnode.Item, nextMarker []byte, err error) {
	rangeFunc := func(value []byte) error {
		itm := &item{}
		err := itm.Unmarshal(value)
		items = append(items, shardnode.Item{
			ID:     itm.ID,
			Fields: internalFieldsToProtoFields(itm.Fields),
		})
		return err
	}
	if len(marker) > 0 {
		marker = s.shardKeys.encodeItemKey(marker)
	}
	nextMarker, err = s.list(ctx, h, s.shardKeys.encodeItemKey(prefix), marker, count, rangeFunc)
	return items, s.shardKeys.decodeItemKey(nextMarker), err
}

func (s *shard) DeleteItem(ctx context.Context, h OpHeader, id []byte) error {
	return s.delete(ctx, h, s.shardKeys.encodeItemKey(id), raftOpDeleteItem)
}

func (s *shard) BatchWriteItem(ctx context.Context, h OpHeader, elems []BatchItemElem, opts ...ShardOptionFunc) error {
	span := trace.SpanFromContextSafe(ctx)

	opt := &shardOption{}
	opt.applyOptions(opts)

	if !opt.proposeAsFollower && !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	wb := s.store.KVStore().NewWriteBatch()
	defer wb.Close()

	for i := range elems {
		switch elems[i].opType {
		case batchItemOpInsert, batchItemOpUpdate:
			raw, err := elems[i].Marshal()
			if err != nil {
				return err
			}
			wb.Put(dataCF, s.shardKeys.encodeItemKey(elems[i].ID), raw)
		case batchItemOpDelete:
			wb.Delete(dataCF, s.shardKeys.encodeItemKey(elems[i].ID))
		default:
			span.Panicf("unknown batch write item op")
		}
	}

	proposalData := raft.ProposalData{
		Op:   raftOpWriteBatchRaw,
		Data: wb.Data(),
	}
	resp, err := s.raftGroup.Propose(ctx, &proposalData)
	if err != nil {
		return err
	}
	appendTrackLogAfterPropose(span, resp.Data)
	return nil
}

func (s *shard) CreateBlob(ctx context.Context, h OpHeader, name []byte, b proto.Blob) (proto.Blob, error) {
	span := trace.SpanFromContextSafe(ctx)

	if !s.isLeader() {
		return proto.Blob{}, apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return proto.Blob{}, err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return proto.Blob{}, convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	kv, err := initKV(s.shardKeys.encodeBlobKey(name), &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	defer kv.Release()
	if err != nil {
		return proto.Blob{}, err
	}

	proposalData := raft.ProposalData{
		Op:   raftOpInsertBlob,
		Data: kv.Marshal(),
	}
	resp, err := s.raftGroup.Propose(ctx, &proposalData)
	if err != nil {
		return proto.Blob{}, err
	}
	appendTrackLogAfterPropose(span, resp.Data)
	return fetchBlobFromProposeRet(resp.Data), nil
}

func (s *shard) UpdateBlob(ctx context.Context, h OpHeader, name []byte, b proto.Blob) error {
	span := trace.SpanFromContextSafe(ctx)

	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	kv, err := initKV(s.shardKeys.encodeBlobKey(name), &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	defer kv.Release()
	if err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   raftOpUpdateBlob,
		Data: kv.Marshal(),
	}
	resp, err := s.raftGroup.Propose(ctx, &proposalData)
	if err != nil {
		return err
	}
	appendTrackLogAfterPropose(span, resp.Data)
	return nil
}

func (s *shard) GetBlob(ctx context.Context, h OpHeader, name []byte) (proto.Blob, error) {
	vg, err := s.get(ctx, h, s.shardKeys.encodeBlobKey(name))
	if err != nil {
		return proto.Blob{}, err
	}
	blob := proto.Blob{}
	if err = blob.Unmarshal(vg.Value()); err != nil {
		err = errors.Info(err, fmt.Sprintf("unmarshal blob failed, raw: %v", vg.Value()))
		vg.Close()
		return proto.Blob{}, err
	}
	vg.Close()
	return blob, nil
}

func (s *shard) ListBlob(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64) (blobs []proto.Blob, nextMarker []byte, err error) {
	rangeFunc := func(data []byte) error {
		b := proto.Blob{}
		if err = b.Unmarshal(data); err != nil {
			return err
		}
		blobs = append(blobs, b)
		return nil
	}

	if len(marker) > 0 {
		marker = s.shardKeys.encodeBlobKey(marker)
	}

	nextMarker, err = s.list(ctx, h, s.shardKeys.encodeBlobKey(prefix), marker, count, rangeFunc)
	return blobs, s.shardKeys.decodeBlobKey(nextMarker), err
}

func (s *shard) DeleteBlob(ctx context.Context, h OpHeader, name []byte, items []shardnode.Item) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	wb := s.store.KVStore().NewWriteBatch()
	defer wb.Close()

	// new delete messages
	for i := range items {
		itm := protoItemToInternalItem(items[i])
		raw, err := itm.Marshal()
		if err != nil {
			return err
		}
		wb.Put(dataCF, s.shardKeys.encodeItemKey(itm.ID), raw)
	}

	// delete meta
	wb.Delete(dataCF, s.shardKeys.encodeBlobKey(name))

	_, err := s.raftGroup.Propose(ctx, &raft.ProposalData{Op: raftOpWriteBatchRaw, Data: wb.Data()})
	return err
}

func (s *shard) GetRouteVersion() proto.RouteVersion {
	s.shardInfoMu.RLock()
	routeVersion := s.shardInfoMu.RouteVersion
	s.shardInfoMu.RUnlock()
	return routeVersion
}

func (s *shard) UpdateShardRouteVersion(version proto.RouteVersion) {
	s.shardInfoMu.Lock()
	if s.shardInfoMu.RouteVersion < version {
		s.shardInfoMu.RouteVersion = version
	}
	s.shardInfoMu.Unlock()
}

func (s *shard) Stats(ctx context.Context, readIndex bool) (shardnode.ShardStats, error) {
	prepCheck := s.shardState.prepRWCheck
	if !readIndex {
		prepCheck = s.shardState.prepRWCheckNoReadIndex
	}
	if err := prepCheck(ctx); err != nil {
		return shardnode.ShardStats{}, convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	s.shardInfoMu.RLock()
	units := make([]clustermgr.ShardUnit, len(s.shardInfoMu.Units))
	copy(units, s.shardInfoMu.Units)
	routeVersion := s.shardInfoMu.RouteVersion
	appliedIndex := s.shardInfoMu.AppliedIndex
	rg := s.shardInfoMu.Range
	s.shardInfoMu.RUnlock()

	leaderUnit, err := s.getLeader(true)
	if err != nil {
		err := errors.Info(err, "get shard leader failed")
		return shardnode.ShardStats{}, err
	}

	leaderHost, err := s.cfg.Transport.ResolveNodeAddr(ctx, leaderUnit.GetDiskID())
	if err != nil {
		err := errors.Info(err, "resolve shard leader host failed")
		return shardnode.ShardStats{}, err
	}

	raftStat, err := s.raftGroup.Stat()
	if err != nil {
		err := errors.Info(err, "get raft group stat failed")
		return shardnode.ShardStats{}, err
	}

	return shardnode.ShardStats{
		Suid:         s.suid,
		AppliedIndex: appliedIndex,
		LeaderDiskID: leaderUnit.GetDiskID(),
		LeaderSuid:   leaderUnit.GetSuid(),
		LeaderHost:   leaderHost,
		Learner:      leaderUnit.GetLearner(),
		RouteVersion: routeVersion,
		Range:        rg,
		Units:        units,
		RaftStat:     *raftStat,
	}, nil
}

// Checkpoint do checkpoint job with raft group
// we should do any memory flush job or dump worker here
func (s *shard) Checkpoint(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	// save applied index and shard's info
	if err := s.SaveShardInfo(ctx, true, false); err != nil {
		return errors.Info(err, "save shard info failed")
	}

	// get persisted info
	info, err := s.getShardInfoFromPersistentTier(ctx)
	if err != nil {
		return errors.Info(err, "get shard info from persist layer failed")
	}
	appliedIndex := info.AppliedIndex

	// truncate raft log finally
	if appliedIndex > s.shardInfoMu.lastTruncatedIndex+s.cfg.TruncateWalLogInterval*2 {
		if err := s.raftGroup.Truncate(ctx, appliedIndex-s.cfg.TruncateWalLogInterval); err != nil {
			return errors.Info(err, "truncate raft wal log failed")
		}
		s.shardInfoMu.lastTruncatedIndex = appliedIndex - s.cfg.TruncateWalLogInterval
		span.Debugf("apply index: %d, truncate index: %d", appliedIndex, s.shardInfoMu.lastTruncatedIndex)
	}

	// save last stable index
	s.shardInfoMu.lastStableIndex = appliedIndex
	span.Debugf("shard[%d] suid[%d] do checkpoint success, apply index: %d", s.suid.ShardID(), s.suid, appliedIndex)

	return nil
}

func (s *shard) UpdateShard(ctx context.Context, op proto.ShardUpdateType, node clustermgr.ShardUnit, nodeHost string) error {
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	switch op {
	case proto.ShardUpdateTypeAddMember:
		if s.isShardUnitExist(node.GetSuid()) {
			return nil
		}
		if !s.validateUnitAdd(node) {
			return apierr.ErrIllegalUpdateUnit
		}
		fallthrough
	case proto.ShardUpdateTypeUpdateMember:
		if op == proto.ShardUpdateTypeUpdateMember && !s.validateUnitUpdate(node) {
			return apierr.ErrIllegalUpdateUnit
		}
		memberCtx := shardnodeproto.ShardMemberCtx{
			Suid: node.GetSuid(),
		}
		raw, err := memberCtx.Marshal()
		if err != nil {
			return err
		}
		return s.raftGroup.MemberChange(ctx, &raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost,
			Type:    raft.MemberChangeType_AddMember,
			Learner: node.Learner,
			Context: raw,
		})
	case proto.ShardUpdateTypeRemoveMember:
		if !s.isShardUnitExist(node.GetSuid()) {
			return nil
		}
		s.shardInfoMu.RLock()
		units := s.shardInfoMu.Units
		if len(units) < 3 {
			s.shardInfoMu.RUnlock()
			return apierr.ErrNoEnoughRaftMember
		}
		s.shardInfoMu.RUnlock()
		return s.raftGroup.MemberChange(ctx, &raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost,
			Type:    raft.MemberChangeType_RemoveMember,
			Learner: node.Learner,
		})
	default:
		return errors.Newf("unsuppoted shard update type: %d", op)
	}
}

func (s *shard) TransferLeader(ctx context.Context, diskID proto.DiskID) error {
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()
	return s.raftGroup.LeaderTransfer(ctx, uint64(diskID))
}

func (s *shard) SaveShardInfo(ctx context.Context, withLock bool, flush bool) error {
	span := trace.SpanFromContextSafe(ctx)
	if withLock {
		s.shardInfoMu.Lock()
		defer s.shardInfoMu.Unlock()
	}
	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeShardInfoKey()
	value, err := s.shardInfoMu.shardInfo.Marshal()
	if err != nil {
		return err
	}
	if !flush {
		return kvStore.SetRaw(ctx, dataCF, key, value)
	}
	if err := kvStore.SetRaw(ctx, dataCF, key, value); err != nil {
		return err
	}
	// todo: flush data, write and lock column family with atomic flush support.
	// todo: need cgo support with FlushCFs

	span.Infof("save shard: %+v, flush: %+v", s.shardInfoMu.shardInfo, flush)
	return kvStore.FlushCF(ctx, dataCF)
}

func (s *shard) DeleteShard(ctx context.Context, nodeHost string, clearData bool) error {
	span := trace.SpanFromContextSafe(ctx)

	raftRemoveFunc := func(u clustermgr.ShardUnit) error {
		span.Warnf("remove shard[%d] suid[%d] by unit: %+v", s.suid.ShardID(), s.suid, u)
		if u.Suid == proto.InvalidSuid {
			return errors.New("can not find shard unit to update")
		}
		host, _err := s.cfg.Transport.ResolveNodeAddr(ctx, u.GetDiskID())
		if _err != nil {
			return _err
		}
		if _err = s.cfg.Transport.UpdateShard(ctx, host, shardnodeapi.UpdateShardArgs{
			DiskID:          u.GetDiskID(),
			Suid:            u.GetSuid(),
			ShardUpdateType: proto.ShardUpdateTypeRemoveMember,
			Unit: clustermgr.ShardUnit{
				Suid:   s.suid,
				DiskID: s.diskID,
			},
		}); _err != nil {
			return _err
		}
		return nil
	}

	units := make([]clustermgr.ShardUnit, 0)
	for _, u := range s.shardInfoMu.Units {
		if s.disk.isWritable() {
			units = append(units, u)
			continue
		}
		if u.Suid.Index() != s.suid.Index() {
			units = append(units, u)
		}
	}
	var err error
	for _, u := range units {
		if err = raftRemoveFunc(u); err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	// wait current shard apply member change done
	for {
		if !s.isShardUnitExist(s.suid) {
			time.Sleep(1 * time.Second)
			break
		}
	}

	span.Warnf("disk[%d] shard[%d] suid[%d] remove from members done", s.diskID, s.suid.ShardID(), s.suid)

	// 2. stop all writing in this shard
	if err = s.Stop(); err != nil {
		return errors.Info(err, "stop shard failed")
	}
	if err = s.WaitStop(); err != nil {
		return errors.Info(err, "wait stop shard failed")
	}
	span.Warnf("disk[%d] shard[%d] suid[%d] stopped", s.diskID, s.suid.ShardID(), s.suid)

	if !clearData {
		return nil
	}
	// 3. clear all shard's data
	kvStore := s.store.KVStore()
	batch := kvStore.NewWriteBatch()

	batch.DeleteRange(dataCF, s.shardKeys.encodeShardDataPrefix(), s.shardKeys.encodeShardDataMaxPrefix())
	batch.Delete(dataCF, s.shardKeys.encodeShardInfoKey())
	if err = kvStore.Write(ctx, batch); err != nil {
		return errors.Info(err, "kvstore write batch failed")
	}
	if err = s.raftGroup.Clear(); err != nil {
		return errors.Info(err, "clear raft data failed")
	}

	span.Warnf("disk[%d] shard[%d] suid[%d] all data cleared", s.diskID, s.suid.ShardID(), s.suid)
	return kvStore.FlushCF(ctx, dataCF)
}

func (s *shard) CheckAndClearShard(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	stats, err := s.Stats(ctx, true)
	if err != nil {
		return err
	}
	if stats.LeaderDiskID != s.diskID {
		return nil
	}

	raftStats := make(map[uint64]bool)
	for _, p := range stats.RaftStat.Peers {
		raftStats[p.NodeID] = p.RecentActive
	}

	for _, u := range stats.Units {
		if u.Suid == s.GetSuid() {
			continue
		}

		var active, ok bool
		if active, ok = raftStats[uint64(u.DiskID)]; !ok {
			return errors.Newf("disk[%d] not found in raft peers", u.DiskID)
		}
		if active {
			continue
		}

		host, err := s.cfg.Transport.ResolveNodeAddr(ctx, u.DiskID)
		if err != nil {
			return errors.Info(err, "resolve node address failed", u.DiskID)
		}
		_, err = s.cfg.Transport.ShardStats(ctx, host, shardnodeapi.GetShardArgs{
			DiskID: u.DiskID,
			Suid:   u.Suid,
		})
		if err == nil {
			continue
		}
		if rpc.DetectStatusCode(err) != apierr.CodeShardNodeDiskNotFound {
			return errors.Info(err, "get shard stats failed", u.DiskID, u.Suid)
		}
		err = s.UpdateShard(ctx, proto.ShardUpdateTypeRemoveMember, clustermgr.ShardUnit{
			Suid:   u.Suid,
			DiskID: u.DiskID,
		}, host)
		if err != nil {
			return errors.Info(err, "update shard failed", s.suid, u.DiskID)
		}
		span.Infof("remove shard[%d] suid[%d] from unit[%+v] done", s.suid.ShardID(), s.suid, u)
		return err
	}
	return nil
}

func (s *shard) Start() {
	// Do nothing because need to be improved later
}

func (s *shard) Stop() error {
	return s.shardState.stopWriting()
}

func (s *shard) WaitStop() error {
	// wait all operation done on this shard before close shard, ensure memory safe
	s.shardState.waitPendingRequestDone()
	return nil
}

func (s *shard) Close() {
	s.raftGroup.Close()
}

func (s *shard) GetAppliedIndex() uint64 {
	return (*shardSM)(s).getAppliedIndex()
}

func (s *shard) GetSuid() proto.Suid {
	return s.suid
}

func (s *shard) ShardingSubRangeCount() int {
	return len(s.shardInfoMu.shardInfo.Range.Subs)
}

func (s *shard) IsLeader() bool {
	return s.isLeader()
}

func (s *shard) GetDiskID() proto.DiskID {
	return s.diskID
}

func (s *shard) GetUnits() []clustermgr.ShardUnit {
	s.shardInfoMu.RLock()
	units := s.shardInfoMu.Units
	s.shardInfoMu.RUnlock()
	return units
}

func (s *shard) get(ctx context.Context, h OpHeader, key []byte) (ValGetter, error) {
	if err := s.checkShardOptHeader(h); err != nil {
		return nil, err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return nil, convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	kvStore := s.store.KVStore()
	ret, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil {
		if errors.Is(err, kvstore.ErrNotFound) {
			return nil, apierr.ErrKeyNotFound
		}
		return nil, err
	}
	return ret, nil
}

func (s *shard) delete(ctx context.Context, h OpHeader, key []byte, op uint32) error {
	span := trace.SpanFromContextSafe(ctx)

	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	proposalData := raft.ProposalData{
		Op:   op,
		Data: key,
	}

	resp, err := s.raftGroup.Propose(ctx, &proposalData)
	if err != nil {
		return err
	}
	appendTrackLogAfterPropose(span, resp.Data)
	return nil
}

func (s *shard) list(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64, rangeFunc func([]byte) error) (nextMarker []byte, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if h.RouteVersion < s.GetRouteVersion() {
		return nil, apierr.ErrShardRouteVersionNeedUpdate
	}
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		return nil, convertStoppingWriteErr(err)
	}
	defer s.shardState.prepRWCheckDone()

	kvStore := s.store.KVStore()
	cursor := kvStore.List(ctx, dataCF, prefix, marker, nil)
	defer cursor.Close()

	count += 1
	for count > 0 {
		kg, vg, err := cursor.ReadNext()
		if err != nil {
			return nil, err
		}
		if vg == nil {
			nextMarker = nil
			break
		}

		if count == 1 {
			nextMarker = make([]byte, len(kg.Key()))
			copy(nextMarker, kg.Key())
			kg.Close()
			vg.Close()
			break
		}

		if rangeFunc == nil {
			span.Panicf("range func is nil")
		}

		if err = rangeFunc(vg.Value()); err != nil {
			err = errors.Info(err, fmt.Sprintf("range func failed, key: %v, value:%v", kg.Key(), vg.Value()))
			kg.Close()
			vg.Close()
			return nil, err
		}

		kg.Close()
		vg.Close()
		count--
	}
	return nextMarker, nil
}

func (s *shard) isShardUnitExist(suid proto.Suid) bool {
	s.shardInfoMu.RLock()
	defer s.shardInfoMu.RUnlock()

	units := s.shardInfoMu.Units
	for _, u := range units {
		if u.Suid == suid {
			return true
		}
	}
	return false
}

func (s *shard) validateUnitAdd(node clustermgr.ShardUnit) bool {
	if s.suid.ShardID() != node.Suid.ShardID() {
		return false
	}
	s.shardInfoMu.RLock()
	defer s.shardInfoMu.RUnlock()

	units := s.shardInfoMu.Units
	for _, u := range units {
		// prevent add shard unit with exist diskID
		if u.DiskID == node.DiskID {
			return false
		}
	}
	return true
}

func (s *shard) validateUnitUpdate(node clustermgr.ShardUnit) bool {
	if s.suid.ShardID() != node.Suid.ShardID() {
		return false
	}

	s.shardInfoMu.RLock()
	defer s.shardInfoMu.RUnlock()

	units := s.shardInfoMu.Units
	exist := false
	for _, u := range units {
		// prevent update shard unit with same suid but diff DiskID
		if u.Suid == node.Suid && u.DiskID != node.DiskID {
			return false
		}
		if u.Suid == node.Suid && u.DiskID == node.DiskID {
			exist = true
		}
	}
	// prevent update shard unit not exist
	return exist
}

func (s *shard) checkShardOptHeader(h OpHeader) error {
	if h.RouteVersion < s.GetRouteVersion() {
		return apierr.ErrShardRouteVersionNeedUpdate
	}
	ci := sharding.NewCompareItem(s.shardInfoMu.Range.Type, h.ShardKeys)
	if !s.shardInfoMu.Range.Belong(ci) {
		return apierr.ErrShardRangeMismatch
	}
	return nil
}

func (s *shard) isLeader() bool {
	s.shardInfoMu.RLock()
	isLeader := s.shardInfoMu.leader == s.diskID
	s.shardInfoMu.RUnlock()

	return isLeader
}

func (s *shard) getLeader(withLock bool) (clustermgr.ShardUnit, error) {
	if withLock {
		s.shardInfoMu.RLock()
		defer s.shardInfoMu.RUnlock()
	}

	units := s.shardInfoMu.Units
	leaderDiskID := s.shardInfoMu.leader
	if leaderDiskID == proto.InvalidDiskID {
		return clustermgr.ShardUnit{}, apierr.ErrShardNoLeader
	}
	leaderSuid := proto.Suid(0)
	learner := false
	for i := range units {
		if units[i].DiskID == leaderDiskID {
			leaderSuid = units[i].GetSuid()
		}
		if units[i].DiskID == s.diskID {
			learner = units[i].Learner
		}
	}
	return clustermgr.ShardUnit{
		DiskID:  leaderDiskID,
		Suid:    leaderSuid,
		Learner: learner,
	}, nil
}

func (s *shard) getShardInfoFromPersistentTier(ctx context.Context) (info clustermgr.Shard, err error) {
	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeShardInfoKey()

	ro := kvStore.NewReadOption()
	ro.SetReadTier(kvstore.ReadTierPersisted)
	defer ro.Close()

	value, err := kvStore.GetRaw(ctx, dataCF, key, kvstore.WithReadOption(ro))
	if err != nil {
		return
	}
	if err = info.Unmarshal(value); err != nil {
		return
	}
	return
}

func convertStoppingWriteErr(err error) error {
	if errors.Is(err, errShardStopWriting) {
		return apierr.ErrShardRouteVersionNeedUpdate
	}
	return err
}

// nolint
type shardState struct {
	status        shardStatus
	pendingReqs   int
	pendingReqsWg sync.WaitGroup

	splitting     bool
	lastSplitTime time.Time
	splitDone     chan struct{}

	restartLeaderReadIndex uint32
	readIndexFunc          func(ctx context.Context) error

	lock sync.RWMutex
}

func (s *shardState) stopWriting() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// shard is splitting, can't be stop writing
	if s.splitting {
		return errors.New("shard is splitting")
	}

	if s.status != shardStatusStopReadWrite {
		s.pendingReqsWg.Add(s.pendingReqs)
		s.status = shardStatusStopReadWrite
	}

	return nil
}

// nolint
func (s *shardState) splitStartWriting() {
	s.lock.Lock()
	s.status = shardStatusNormal
	s.lock.Unlock()

	close(s.splitDone)
}

// nolint
func (s *shardState) splitStopWriting() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.status != shardStatusStopReadWrite {
		s.pendingReqsWg.Add(s.pendingReqs)
		s.status = shardStatusStopReadWrite
	}

	s.splitDone = make(chan struct{})
}

func (s *shardState) prepRWCheck(ctx context.Context) error {
	if atomic.LoadUint32(&s.restartLeaderReadIndex) == needReadIndex && s.readIndexFunc != nil {
		if err := s.readIndexFunc(ctx); err != nil {
			return err
		}
		atomic.StoreUint32(&s.restartLeaderReadIndex, noNeedReadIndex)
	}
	return s.tryRW()
}

func (s *shardState) prepRWCheckNoReadIndex(ctx context.Context) error {
	return s.tryRW()
}

func (s *shardState) tryRW() error {
	s.lock.Lock()

	// allow writing check in the list lock arena
	if !s.allowRW() {
		s.lock.Unlock()

		if !s.splitting {
			// return route need update when shard stop write progress
			return errShardStopWriting
		}

		// wait for start write when split shard progress
		s.waitSplitDone()
		s.lock.Lock()
	}

	s.pendingReqs++
	s.lock.Unlock()

	return nil
}

func (s *shardState) prepRWCheckDone() {
	s.lock.Lock()
	s.pendingReqs--
	// decrease pending request
	if !s.allowRW() {
		s.pendingReqsWg.Done()
	}
	s.lock.Unlock()
}

func (s *shardState) waitSplitDone() {
	s.lock.RLock()
	done := s.splitDone
	s.lock.RUnlock()
	<-done
}

// nolint
func (s *shardState) startSplitting() bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// already in splitting, return false and do not start new split
	if s.splitting {
		return false
	}
	// check if shard stop writing
	if !s.allowRW() {
		return false
	}

	s.splitting = true
	return true
}

// nolint
func (s *shardState) stopSplitting() {
	s.lock.Lock()
	s.lastSplitTime = time.Now()
	s.splitting = false
	s.lock.Unlock()
}

func (s *shardState) waitPendingRequestDone() {
	s.pendingReqsWg.Wait()
}

func (s *shardState) allowRW() bool {
	return s.status != shardStatusStopReadWrite
}

type shardKeysGenerator struct {
	suid proto.Suid
}

// encode item key with prefix: d[shardID]-a-[key]
func (s *shardKeysGenerator) encodeItemKey(key []byte) []byte {
	shardItemPrefixSize := shardItemPrefixSize()
	newKey := make([]byte, shardItemPrefixSize+len(key))
	encodeShardItemPrefix(s.suid.ShardID(), newKey)
	copy(newKey[shardItemPrefixSize:], key)
	return newKey
}

func (s *shardKeysGenerator) decodeItemKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	shardItemPrefixSize := shardItemPrefixSize()
	return key[shardItemPrefixSize:]
}

// encode blob key with prefix: d[shardID]-b-[key]
func (s *shardKeysGenerator) encodeBlobKey(key []byte) []byte {
	shardBlobPrefixSize := shardBlobPrefixSize()
	newKey := make([]byte, shardBlobPrefixSize+len(key))
	encodeShardBlobPrefix(s.suid.ShardID(), newKey)
	copy(newKey[shardBlobPrefixSize:], key)
	return newKey
}

func (s *shardKeysGenerator) decodeBlobKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	shardBlobPrefixSize := shardBlobPrefixSize()
	return key[shardBlobPrefixSize:]
}

// encode shard info key with prefix: s[shardID]
func (s *shardKeysGenerator) encodeShardInfoKey() []byte {
	key := make([]byte, shardInfoPrefixSize())
	encodeShardInfoPrefix(s.suid, key)
	return key
}

// encode shard data prefix with prefix: d[shardID]
// it can be used for listing all shard's data or delete shard's data
func (s *shardKeysGenerator) encodeShardDataPrefix() []byte {
	key := make([]byte, shardDataPrefixSize())
	encodeShardDataPrefix(s.suid.ShardID(), key)
	return key
}

// encode shard data max prefix with prefix: d[shardID]-max
// it can be used for delete shard's data
func (s *shardKeysGenerator) encodeShardDataMaxPrefix() []byte {
	key := make([]byte, shardMaxPrefixSize())
	encodeShardDataMaxPrefix(s.suid.ShardID(), key)
	return key
}

func protoItemToInternalItem(i shardnode.Item) (ret item) {
	ret = item{
		ID:     i.ID,
		Fields: protoFieldsToInternalFields(i.Fields),
	}
	return
}

func protoFieldsToInternalFields(external []shardnode.Field) []shardnodeproto.Field {
	// todo: use memory pool
	ret := make([]shardnodeproto.Field, len(external))
	for i := range external {
		ret[i] = shardnodeproto.Field{
			ID:    external[i].ID,
			Value: external[i].Value,
		}
	}
	return ret
}

func internalFieldsToProtoFields(internal []shardnodeproto.Field) []shardnode.Field {
	// todo: use memory pool
	ret := make([]shardnode.Field, len(internal))
	for i := range internal {
		ret[i] = shardnode.Field{
			ID:    internal[i].ID,
			Value: internal[i].Value,
		}
	}
	return ret
}

func appendTrackLogAfterPropose(span trace.Span, data interface{}) {
	if data == nil {
		return
	}
	ret, ok := data.(applyRet)
	if !ok {
		panic("illegal response.Data type")
	}
	span.AppendRPCTrackLog(ret.traceLog)
}

func fetchBlobFromProposeRet(data interface{}) (b proto.Blob) {
	if data == nil {
		return
	}
	ret, ok := data.(applyRet)
	if !ok {
		panic("illegal response.Data type")
	}
	return ret.blob
}

type ShardKeysGenerator struct {
	shardKeysGenerator
}

func NewShardKeysGenerator(suid proto.Suid) ShardKeysGenerator {
	return ShardKeysGenerator{
		shardKeysGenerator{suid: suid},
	}
}

func (ss *ShardKeysGenerator) EncodeShardInfoKey() []byte {
	return ss.encodeShardInfoKey()
}

func (ss *ShardKeysGenerator) EncodeShardDataPrefix() []byte {
	return ss.encodeShardDataPrefix()
}

func (ss *ShardKeysGenerator) EncodeShardDataMaxPrefix() []byte {
	return ss.encodeShardDataMaxPrefix()
}
