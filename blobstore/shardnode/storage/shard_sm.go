package storage

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	storageproto "github.com/cubefs/cubefs/blobstore/shardnode/storage/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	RaftOpInsertItem uint32 = iota + 1
	RaftOpUpdateItem
	RaftOpDeleteItem
	RaftOpLinkItem
	RaftOpUnlinkItem
	RaftOpAllocInoRange
)

type shardSM shard

func (s *shardSM) Apply(cxt context.Context, pd []raft.ProposalData, index uint64) (rets []interface{}, err error) {
	rets = make([]interface{}, len(pd))

	for i := range pd {
		_, c := trace.StartSpanFromContextWithTraceID(context.Background(), "", string(pd[i].Context))
		switch pd[i].Op {
		case RaftOpInsertItem:
			if err = s.applyInsertItem(c, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpUpdateItem:
			if err = s.applyUpdateItem(c, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpDeleteItem:
			if err = s.applyDeleteItem(c, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		default:
			panic(fmt.Sprintf("unsupported operation type: %d", pd[i].Op))
		}
	}

	s.setAppliedIndex(index)
	return
}

func (s *shardSM) LeaderChange(peerID uint64) error {
	log.Info("shard receive Leader change", peerID)
	// todo: report Leader change to master
	s.shardMu.Lock()
	s.shardMu.leader = proto.DiskID(peerID)
	s.shardMu.Unlock()
	// todo: read index before start to serve request

	return nil
}

func (s *shardSM) ApplyMemberChange(cc *raft.Member, index uint64) error {
	_, c := trace.StartSpanFromContext(context.Background(), "")

	s.shardMu.Lock()
	defer s.shardMu.Unlock()

	switch cc.Type {
	case raft.MemberChangeType_AddMember:
		found := false
		for _, node := range s.shardMu.Units {
			if node.DiskID == proto.DiskID(cc.NodeID) {
				found = true
				break
			}
		}
		if !found {
			s.shardMu.Units = append(s.shardMu.Units, clustermgr.ShardUnitInfo{
				DiskID:  proto.DiskID(cc.NodeID),
				Learner: cc.Learner,
			})
		}
	case raft.MemberChangeType_RemoveMember:
		for i, node := range s.shardMu.Units {
			if node.DiskID == proto.DiskID(cc.NodeID) {
				s.shardMu.Units = append(s.shardMu.Units[:i], s.shardMu.Units[i+1:]...)
				break
			}
		}
	}

	return (*shard)(s).SaveShardInfo(c, false, true)
}

func (s *shardSM) Snapshot() raft.Snapshot {
	kvStore := s.store.KVStore()
	appliedIndex := s.getAppliedIndex()
	kvSnap := kvStore.NewSnapshot()
	readOpt := kvStore.NewReadOption()
	readOpt.SetSnapShot(kvSnap)

	// create cf list reader for shard data
	lrs := make([]kvstore.ListReader, 0)
	for _, cf := range []kvstore.CF{dataCF} {
		prefix := s.shardKeys.encodeShardDataPrefix()
		lrs = append(lrs, kvStore.List(context.Background(), cf, prefix, nil, readOpt))
	}
	// todo: auto id is increase in the shard info, so we need to sync shard info
	// by create cfs list reader for shard info. we may delete this by set auto id with seperated kv
	lrs = append(lrs, kvStore.List(context.Background(), dataCF, s.shardKeys.encodeShardInfoKey(), nil, readOpt))

	return &raftSnapshot{
		appliedIndex:               appliedIndex,
		RaftSnapshotTransmitConfig: &s.cfg.RaftSnapTransmitConfig,
		st:                         kvSnap,
		ro:                         readOpt,
		lrs:                        lrs,
		kvStore:                    kvStore,
	}
}

func (s *shardSM) ApplySnapshot(snap raft.Snapshot) error {
	defer snap.Close()
	kvStore := s.store.KVStore()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// clear all data with shard prefix
	batch := kvStore.NewWriteBatch()
	batch.DeleteRange(dataCF, s.shardKeys.encodeShardDataPrefix(), s.shardKeys.encodeShardDataMaxPrefix())
	if err := kvStore.Write(ctx, batch, nil); err != nil {
		return err
	}

	for {
		batch, err := snap.ReadBatch()
		if err != nil && err != io.EOF {
			return err
		}

		if batch != nil {
			if err = kvStore.Write(ctx, batch.(raftBatch).batch, nil); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
		}
		if err == io.EOF {
			break
		}
	}

	// save applied index and shard's info
	s.setAppliedIndex(snap.Index())
	if err := (*shard)(s).SaveShardInfo(ctx, true, true); err != nil {
		return errors.Info(err, "save shard into failed")
	}

	return nil
}

func (s *shardSM) applyInsertItem(ctx context.Context, data []byte) error {
	pi := &item{}
	if err := pi.Unmarshal(data); err != nil {
		return errors.Info(err, "unmarshal propose item failed")
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeItemKey(pi.ID)

	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
		return errors.Info(err, "get item failed")
	}
	// already insert, just return
	if err == nil {
		vg.Close()
		return nil
	}
	vg.Close()

	if err := kvStore.SetRaw(ctx, dataCF, key, data, nil); err != nil {
		return errors.Info(err, "kv store set failed")
	}
	return nil
}

func (s *shardSM) applyUpdateItem(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)
	pi := &item{}
	if err := pi.Unmarshal(data); err != nil {
		return err
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeItemKey(pi.ID)
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil {
		// replay raft wal log may meet with item deleted and replay update item operation
		if errors.Is(err, kvstore.ErrNotFound) {
			span.Warnf("item[%v] has been deleted", pi)
			return nil
		}
		return err
	}
	item := &item{}
	if err = item.Unmarshal(vg.Value()); err != nil {
		vg.Close()
		return err
	}
	vg.Close()

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
		item.Fields = append(item.Fields, storageproto.Field{ID: updateField.ID, Value: updateField.Value})
	}

	data, err = item.Marshal()
	if err != nil {
		return err
	}
	if err := kvStore.SetRaw(ctx, dataCF, key, data, nil); err != nil {
		return errors.Info(err, "kv store set failed")
	}

	return nil
}

func (s *shardSM) applyDeleteItem(ctx context.Context, data []byte) error {
	id := data
	kvStore := s.store.KVStore()

	// independent check, avoiding decrease ino used repeatedly at raft log replay progress
	key := s.shardKeys.encodeItemKey(id)
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil {
		if !errors.Is(err, kvstore.ErrNotFound) {
			return err
		}
		return nil
	}
	item := &item{}
	if err = item.Unmarshal(vg.Value()); err != nil {
		vg.Close()
		return err
	}
	vg.Close()

	if err := kvStore.Delete(ctx, dataCF, key, nil); err != nil {
		return errors.Info(err, "kv store delete failed")
	}
	return nil
}

func (s *shardSM) setAppliedIndex(index uint64) {
	atomic.StoreUint64(&s.shardMu.AppliedIndex, index)
}

func (s *shardSM) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.shardMu.AppliedIndex)
}
