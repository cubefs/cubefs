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
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	rdb "github.com/tecbot/gorocksdb"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	shardnodeproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	raftOpInsertItem uint32 = iota + 1
	raftOpUpdateItem
	raftOpDeleteItem
	raftOpInsertBlob
	raftOpUpdateBlob
	raftOpDeleteBlob
	raftOpWriteBatchRaw

	setRaw        = "set"
	getRaw        = "get"
	delRaw        = "del"
	writeBatchRaw = "batch"
)

type shardSM shard

type metaOp uint8

const (
	metaOpInsert metaOp = iota + 1
	metaOpUpdate
	metaOpDelete
)

type metaDataType uint8

const (
	metaDataTypeItem metaDataType = iota + 1
	metaDataTypeBlob
)

type metaOpUnit struct {
	op           metaOp
	dataType     metaDataType
	spaceID      uint64
	keySize      int
	oldValueSize int
	newValueSize int
	hashValues   []uint64
}

func (s *shardSM) Apply(ctx context.Context, pd []raft.ProposalData, index uint64) (rets []interface{}, err error) {
	rets = make([]interface{}, len(pd))
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("shard [%d] apply index: %d", s.suid, index)
	defer func() {
		if err != nil {
			span.Errorf("shard [%d] apply index failed, err:%s", s.suid, errors.Detail(err))
		}
	}()

	metaOpUnits := make([]*metaOpUnit, 0)
	for i := range pd {
		_span, c := trace.StartSpanFromContextWithTraceID(context.Background(), "", span.TraceID())
		switch pd[i].Op {
		case raftOpInsertItem:
			mou, err := s.applyInsertItem(c, pd[i].Data)
			if err != nil {
				return nil, err
			}
			metaOpUnits = append(metaOpUnits, mou)
			rets[i] = applyRet{traceLog: _span.TrackLog()}
		case raftOpUpdateItem:
			mou, err := s.applyUpdateItem(c, pd[i].Data)
			if err != nil {
				return nil, err
			}
			metaOpUnits = append(metaOpUnits, mou)
			rets[i] = applyRet{traceLog: _span.TrackLog()}
		case raftOpInsertBlob:
			var blob proto.Blob
			blob, mou, err := s.applyInsertBlob(c, pd[i].Data)
			if err != nil {
				return nil, err
			}
			metaOpUnits = append(metaOpUnits, mou)
			rets[i] = applyRet{
				traceLog: _span.TrackLog(),
				blob:     blob,
			}
			metaOpUnits = append(metaOpUnits, mou)
		case raftOpUpdateBlob:
			mou, err := s.applyUpdateBlob(c, pd[i].Data)
			if err != nil {
				return nil, err
			}
			metaOpUnits = append(metaOpUnits, mou)
		case raftOpDeleteBlob, raftOpDeleteItem:
			mou, err := s.applyDeleteRaw(c, pd[i].Data)
			if err != nil {
				return nil, err
			}
			metaOpUnits = append(metaOpUnits, mou)
			rets[i] = applyRet{traceLog: _span.TrackLog()}
		case raftOpWriteBatchRaw:
			mous, err := s.applyWriteBatchRaw(c, pd[i].Data)
			if err != nil {
				return nil, err
			}
			metaOpUnits = append(metaOpUnits, mous...)
			rets[i] = applyRet{traceLog: _span.TrackLog()}
		default:
			panic(fmt.Sprintf("unsupported operation type: %d", pd[i].Op))
		}
	}

	// update meta first to make metaStats.appliedIndex >= shardInfo.appliedIndex,
	// when do checkpoint, get shardInfo first and save shardInfo and metaStats by batch,
	// so that when shard restart and replay raft log, it can update metaStats without missing any raft log.
	(*shard)(s).updateMetaStatsByMetaOpUnit(ctx, metaOpUnits, index)
	s.setAppliedIndex(index)
	return
}

func (s *shardSM) LeaderChange(peerID uint64) error {
	log.Info(fmt.Sprintf("shard[%d] receive Leader change, diskID: %d, suid: %d, peerID: %d",
		s.suid.ShardID(), s.diskID, s.suid, peerID))
	// todo: report Leader change to master
	s.shardInfoMu.Lock()
	s.shardInfoMu.leader = proto.DiskID(peerID)
	s.shardInfoMu.Unlock()

	if peerID > 0 && peerID != uint64(s.disk.DiskID()) {
		atomic.StoreUint32(&s.shardState.restartLeaderReadIndex, noNeedReadIndex)
	}

	return nil
}

func (s *shardSM) ApplyMemberChange(cc *raft.Member, index uint64) error {
	span, c := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("suid: [%d] apply member change, member:%+v", s.suid, cc)

	if err := s.shardState.prepRWCheck(ctx); err != nil {
		span.Warnf("shard is stop writing by delete")
		return nil
	}
	defer s.shardState.prepRWCheckDone()

	s.shardInfoMu.Lock()
	defer s.shardInfoMu.Unlock()

	switch cc.Type {
	case raft.MemberChangeType_AddMember:
		found := false
		for i := range s.shardInfoMu.Units {
			if s.shardInfoMu.Units[i].DiskID == proto.DiskID(cc.NodeID) {
				s.shardInfoMu.Units[i].Learner = cc.Learner
				found = true
				break
			}
		}
		if !found {
			memberCtx := shardnodeproto.ShardMemberCtx{}
			err := memberCtx.Unmarshal(cc.GetContext())
			if err != nil {
				return err
			}

			s.shardInfoMu.Units = append(s.shardInfoMu.Units, clustermgr.ShardUnit{
				Suid:    memberCtx.GetSuid(),
				DiskID:  proto.DiskID(cc.NodeID),
				Learner: cc.Learner,
			})
			span.Debugf("shard add member:%+v, ctx:%+v", cc, memberCtx)
		}
	case raft.MemberChangeType_RemoveMember:
		for i, node := range s.shardInfoMu.Units {
			if node.DiskID == proto.DiskID(cc.NodeID) {
				s.shardInfoMu.Units = append(s.shardInfoMu.Units[:i], s.shardInfoMu.Units[i+1:]...)
				span.Debugf("shard remove member:%+v", cc)
				break
			}
		}
		if proto.DiskID(cc.NodeID) == s.diskID {
			s.disk.raftManager.RemoveRaftGroup(ctx, uint64(s.suid.ShardID()), false)
		}
	default:

	}

	if err := (*shard)(s).SaveShardInfo(c, false, true); err != nil {
		if errors.Is(err, errShardStopWriting) {
			span.Warnf("shard is stop writing by delete")
			return nil
		}
		return errors.Info(err, "save shard into failed")
	}
	return nil
}

func (s *shardSM) Snapshot() (raft.Snapshot, error) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "snapshot")
	if err := s.shardState.prepRWCheck(ctx); err != nil {
		if errors.Is(err, errShardStopWriting) {
			span.Warnf("shard is stop writing by delete")
			return nil, nil
		}
		span.Errorf("preRWCheck failed when make snapshot, err: %s", err.Error())
		return nil, err
	}

	kvStore := s.store.KVStore()
	// save metaStats to snapshot
	metaStats := s.metaStats.get()
	appliedIndex := metaStats.AppliedIndex
	metaStatsValue, err := metaStats.Marshal()
	if err != nil {
		return nil, err
	}
	metaStatsKey := s.shardKeys.encodeShardMetaStatsKey()
	kvStore.SetRaw(ctx, dataCF, metaStatsKey, metaStatsValue, nil)

	kvSnap := kvStore.NewSnapshot()
	readOpt := kvStore.NewReadOption()
	readOpt.SetSnapShot(kvSnap)

	// create cf list reader for shard data
	lrs := make([]kvstore.ListReader, 0)
	for _, cf := range []kvstore.CF{dataCF} {
		prefix := s.shardKeys.encodeShardDataPrefix()
		lrs = append(lrs, kvStore.List(ctx, cf, prefix, nil, readOpt))
	}
	// TODO:
	// lrs = append(lrs, kvStore.List(ctx, dataCF, s.shardKeys.encodeShardInfoKey(), nil, readOpt))

	return &raftSnapshot{
		appliedIndex:               appliedIndex,
		RaftSnapshotTransmitConfig: &s.cfg.RaftSnapTransmitConfig,
		st:                         kvSnap,
		ro:                         readOpt,
		lrs:                        lrs,
		kvStore:                    kvStore,
		done: func() {
			s.shardState.prepRWCheckDone()
		},
	}, nil
}

func (s *shardSM) ApplySnapshot(ctx context.Context, header raft.RaftSnapshotHeader, snap raft.Snapshot) error {
	span := trace.SpanFromContextSafe(ctx)
	defer snap.Close()
	span.Debugf("shard[%d] suid[%d] start apply snapshot, index: %d", s.suid.ShardID(), s.suid, snap.Index())

	if err := s.shardState.prepRWCheck(ctx); err != nil {
		if errors.Is(err, errShardStopWriting) {
			span.Warnf("shard is stop writing by delete")
			return nil
		}
		span.Errorf("preRWCheck failed when make snapshot, err: %s", err.Error())
		return err
	}
	defer s.shardState.prepRWCheckDone()

	kvStore := s.store.KVStore()

	// clear all data with shard prefix
	batch := kvStore.NewWriteBatch()
	batch.DeleteRange(dataCF, s.shardKeys.encodeShardDataPrefix(), s.shardKeys.encodeShardDataMaxPrefix())
	// flush

	if err := kvStore.Write(ctx, batch, nil); err != nil {
		return err
	}

	for {
		batch, err := snap.ReadBatch()
		if err != nil && err != io.EOF {
			return err
		}

		if batch != nil {
			if _err := kvStore.Write(ctx, batch.(raftBatch).batch, nil); _err != nil {
				span.Debugf("shard[%d] suid[%d] applying snapshot, apply index:%d", s.suid.ShardID(), s.suid, snap.Index())
				batch.Close()
				return _err
			}
			batch.Close()
		}
		if err == io.EOF {
			break
		}
	}

	// save applied index and shard's info
	s.setAppliedIndex(snap.Index())
	// save shard unit by members
	members := header.Members
	units := make([]clustermgr.ShardUnit, 0, len(members))
	for i := range members {
		mctx := shardnodeproto.ShardMemberCtx{}
		if err := mctx.Unmarshal(members[i].Context); err != nil {
			return errors.Info(err, "unmarshal member context failed")
		}
		units = append(units, clustermgr.ShardUnit{
			Suid:    mctx.Suid,
			DiskID:  proto.DiskID(members[i].GetNodeID()),
			Learner: members[i].Learner,
		})
	}
	s.shardInfoMu.Units = units
	if err := (*shard)(s).SaveShardInfo(ctx, true, true); err != nil {
		if errors.Is(err, errShardStopWriting) {
			span.Warnf("shard is stop writing by delete")
			return nil
		}
		return errors.Info(err, "save shard into failed")
	}

	// load metaStats from snapshot and set to shardInfoMu.metaStats
	metaStatsKey := s.shardKeys.encodeShardMetaStatsKey()
	raw, err := kvStore.GetRaw(ctx, dataCF, metaStatsKey, nil)
	if err != nil {
		return errors.Info(err, "get meta stats from snapshot failed")
	}
	metaStats := shardnodeproto.ShardMetaStats{}
	if err := metaStats.Unmarshal(raw); err != nil {
		return errors.Info(err, "unmarshal meta stats from snapshot failed")
	}
	s.metaStats.set(metaStats)

	span.Debugf("shard [%d] apply snapshot success, apply index:%d", s.suid, snap.Index())
	return nil
}

func (s *shardSM) applyUpdateItem(ctx context.Context, data []byte) (*metaOpUnit, error) {
	span := trace.SpanFromContext(ctx)

	kvh := newKV(data)
	key := kvh.Key()

	pi := &item{}
	if err := pi.Unmarshal(kvh.Value()); err != nil {
		return nil, errors.Info(err, "unmarshal item failed")
	}

	kvStore := s.store.KVStore()
	oldValueSize := 0
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if vg != nil {
		oldValueSize = vg.Size()
		defer vg.Close()
	}

	if err != nil {
		// replay raft wal log may meet with item deleted and replay update item operation
		if errors.Is(err, kvstore.ErrNotFound) {
			span.Warnf("item[%v] has been deleted", pi)
			return nil, nil
		}
		return nil, errors.Info(err, "get raw kv failed")
	}

	item := &item{}
	if err = item.Unmarshal(vg.Value()); err != nil {
		return nil, err
	}

	fieldMap := make(map[proto.FieldID]int)
	for i := range item.Fields {
		fieldMap[item.Fields[i].ID] = i
	}
	for _, updateField := range pi.Fields {
		// update existed field or insert new field
		if idx, ok := fieldMap[updateField.ID]; ok {
			item.Fields[idx].Value = updateField.Value
			continue
		}
		item.Fields = append(item.Fields, shardnodeproto.Field{ID: updateField.ID, Value: updateField.Value})
	}

	data, err = item.Marshal()
	if err != nil {
		return nil, err
	}
	if err := kvStore.SetRaw(ctx, dataCF, key, data, nil); err != nil {
		return nil, errors.Info(err, "kv store set failed")
	}

	metaDataType, spaceID := (*shard)(s).getMetaDataTypeAndSpaceIDByKey(key)
	return &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataType,
		spaceID:      spaceID,
		keySize:      len(key),
		oldValueSize: oldValueSize,
		newValueSize: len(data),
	}, nil
}

func (s *shardSM) applyInsertItem(ctx context.Context, data []byte) (*metaOpUnit, error) {
	span := trace.SpanFromContextSafe(ctx)

	kvh := newKV(data)
	key := kvh.Key()

	kvStore := s.store.KVStore()
	start := time.Now()
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	withErr := err
	if errors.Is(withErr, kvstore.ErrNotFound) {
		withErr = nil
	}
	span.AppendTrackLog(getRaw, start, withErr, trace.OptSpanDurationUs())
	if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
		return nil, errors.Info(err, "get raw kv failed")
	}
	// already insert, just return
	if err == nil {
		vg.Close()
		return nil, nil
	}

	start = time.Now()
	err = kvStore.SetRaw(ctx, dataCF, key, kvh.Value(), nil)
	span.AppendTrackLog(setRaw, start, err, trace.OptSpanDurationUs())
	if err != nil {
		return nil, errors.Info(err, "kv store set failed")
	}

	// setup metaOpUnit
	sd := (*shard)(s)
	metaDataType, spaceID := sd.getMetaDataTypeAndSpaceIDByKey(key)
	hashValues, err := sd.getHashValues(key)
	if err != nil {
		return nil, err
	}
	return &metaOpUnit{
		op:           metaOpInsert,
		dataType:     metaDataType,
		spaceID:      spaceID,
		keySize:      len(key),
		oldValueSize: 0,
		newValueSize: len(kvh.Value()),
		hashValues:   hashValues,
	}, err
}

func (s *shardSM) applyInsertBlob(ctx context.Context, data []byte) (proto.Blob, *metaOpUnit, error) {
	span := trace.SpanFromContextSafe(ctx)

	kvh := newKV(data)
	key := kvh.Key()

	kvStore := s.store.KVStore()
	start := time.Now()
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if vg != nil {
		defer vg.Close()
	}

	withErr := err
	if errors.Is(withErr, kvstore.ErrNotFound) {
		withErr = nil
	}
	span.AppendTrackLog(getRaw, start, withErr, trace.OptSpanDurationUs())
	if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
		return proto.Blob{}, nil, errors.Info(err, "get raw kv failed")
	}

	b := proto.Blob{}

	// already insert, return old blob
	if err == nil {
		if err = b.Unmarshal(vg.Value()); err != nil {
			return proto.Blob{}, nil, err
		}
		return b, nil, nil
	}

	start = time.Now()
	err = kvStore.SetRaw(ctx, dataCF, key, kvh.Value(), nil)
	span.AppendTrackLog(setRaw, start, err, trace.OptSpanDurationUs())
	if err != nil {
		return proto.Blob{}, nil, errors.Info(err, "kv store set failed")
	}

	if err = b.Unmarshal(kvh.Value()); err != nil {
		return proto.Blob{}, nil, err
	}

	// setup metaOpUnit
	sd := (*shard)(s)
	metaDataType, spaceID := (*shard)(s).getMetaDataTypeAndSpaceIDByKey(key)
	hashValues, err := sd.getHashValues(key)
	if err != nil {
		return proto.Blob{}, nil, err
	}
	return b, &metaOpUnit{
		op:           metaOpInsert,
		dataType:     metaDataType,
		spaceID:      spaceID,
		keySize:      len(key),
		oldValueSize: 0,
		newValueSize: len(kvh.Value()),
		hashValues:   hashValues,
	}, nil
}

func (s *shardSM) applyUpdateBlob(ctx context.Context, data []byte) (*metaOpUnit, error) {
	span := trace.SpanFromContextSafe(ctx)

	kvh := newKV(data)
	key := kvh.Key()

	kvStore := s.store.KVStore()
	start := time.Now()
	oldValueSize := 0
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if vg != nil {
		oldValueSize = vg.Size()
		defer vg.Close()
	}

	span.AppendTrackLog(getRaw, start, err, trace.OptSpanDurationUs())
	if err != nil {
		if errors.Is(err, kvstore.ErrNotFound) {
			span.Warnf("shard [%d] get blob key [%s] has been deleted", s.suid, string(key))
			return nil, nil
		}
		return nil, errors.Info(err, "get kv failed")
	}

	// already insert, just check if same value
	if bytes.Equal(kvh.Value(), vg.Value()) {
		return nil, nil
	}

	start = time.Now()
	err = kvStore.SetRaw(ctx, dataCF, key, kvh.Value(), nil)
	span.AppendTrackLog(setRaw, start, err, trace.OptSpanDurationUs())
	if err != nil {
		return nil, errors.Info(err, "kv store set failed")
	}

	metaDataType, spaceID := (*shard)(s).getMetaDataTypeAndSpaceIDByKey(key)
	return &metaOpUnit{
		op:           metaOpUpdate,
		dataType:     metaDataType,
		spaceID:      spaceID,
		keySize:      len(key),
		oldValueSize: oldValueSize,
		newValueSize: len(kvh.Value()),
	}, nil
}

func (s *shardSM) applyDeleteRaw(ctx context.Context, data []byte) (*metaOpUnit, error) {
	span := trace.SpanFromContextSafe(ctx)

	kvStore := s.store.KVStore()
	// independent check, avoiding decrease ino used repeatedly at raft log replay progress
	start := time.Now()
	oldValueSize := 0
	vg, err := kvStore.Get(ctx, dataCF, data, nil)
	if vg != nil {
		oldValueSize = vg.Size()
		defer vg.Close()
	}
	withErr := err
	if errors.Is(withErr, kvstore.ErrNotFound) {
		withErr = nil
	}

	span.AppendTrackLog(getRaw, start, withErr, trace.OptSpanDurationUs())
	if err != nil {
		if !errors.Is(err, kvstore.ErrNotFound) {
			return nil, err
		}
		return nil, nil
	}

	start = time.Now()
	err = kvStore.Delete(ctx, dataCF, data, nil)
	span.AppendTrackLog(delRaw, start, err, trace.OptSpanDurationUs())
	if err != nil {
		return nil, errors.Info(err, "kv store delete failed")
	}

	// setup metaOpUnit
	sd := (*shard)(s)
	metaDataType, spaceID := sd.getMetaDataTypeAndSpaceIDByKey(data)
	hashValues, err := sd.getHashValues(data)
	if err != nil {
		return nil, err
	}
	return &metaOpUnit{
		op:           metaOpDelete,
		dataType:     metaDataType,
		spaceID:      spaceID,
		keySize:      len(data),
		oldValueSize: oldValueSize,
		newValueSize: 0,
		hashValues:   hashValues,
	}, nil
}

func (s *shardSM) applyWriteBatchRaw(ctx context.Context, data []byte) ([]*metaOpUnit, error) {
	span := trace.SpanFromContextSafe(ctx)

	start := time.Now()
	kvStore := s.store.KVStore()

	batch := kvStore.NewWriteBatch()
	defer batch.Close()

	batch.From(data)
	count := batch.Count()
	metaOpUnits := make([]*metaOpUnit, 0, count)
	br := batch.Iterator()
	for br.Next() {
		key := br.Key()
		mou := &metaOpUnit{}
		metaDataType, spaceID := (*shard)(s).getMetaDataTypeAndSpaceIDByKey(key)
		mou.dataType = metaDataType
		mou.spaceID = spaceID
		mou.keySize = len(key)
		mou.newValueSize = len(br.Value())
		// read from store to get old value size
		hashValues, err := (*shard)(s).getHashValues(key)
		if err != nil {
			return nil, err
		}
		mou.hashValues = hashValues
		vg, err := kvStore.Get(ctx, dataCF, key, nil)
		if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
			return nil, err
		}
		if vg != nil {
			defer vg.Close()
			mou.oldValueSize = vg.Size()
		}
		switch br.Type() {
		case kvstore.WriteBatchType(rdb.WriteBatchCFValueRecord):
			mou.op = metaOpInsert
			if vg != nil {
				mou.op = metaOpUpdate
			}
		case kvstore.WriteBatchType(rdb.WriteBatchCFDeletionRecord):
			mou.op = metaOpDelete
		default:
			span.Errorf("unexpected write batch type: %d", br.Type())
		}
		metaOpUnits = append(metaOpUnits, mou)
	}
	err := kvStore.Write(ctx, batch, nil)
	span.AppendTrackLog(writeBatchRaw, start, err, trace.OptSpanDurationUs())
	return metaOpUnits, err
}

func (s *shardSM) setAppliedIndex(index uint64) {
	atomic.StoreUint64(&s.shardInfoMu.AppliedIndex, index)
}

func (s *shardSM) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.shardInfoMu.AppliedIndex)
}

type applyRet struct {
	traceLog []string
	blob     proto.Blob
}
