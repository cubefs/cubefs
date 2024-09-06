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
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
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
)

type ValGetter interface {
	Read(p []byte) (n int, err error)
	Value() []byte
	Size() int
	Close()
}

type (
	ShardKVHandler interface {
		// KV
		Insert(ctx context.Context, h OpHeader, kv *KV) error
		Update(ctx context.Context, h OpHeader, kv *KV) error
		Get(ctx context.Context, h OpHeader, key []byte) (ValGetter, error)
		Delete(ctx context.Context, h OpHeader, key []byte) error
		List(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64, rangeFunc func([]byte) error) (nextMarker []byte, err error)
	}
	ShardItemHandler interface {
		// item
		UpdateItem(ctx context.Context, h OpHeader, i shardnode.Item) error
		GetItem(ctx context.Context, h OpHeader, id []byte) (shardnode.Item, error)
		ListItem(ctx context.Context, h OpHeader, prefix, id []byte, count uint64) (items []shardnode.Item, nextMarker []byte, err error)
	}
	ShardHandler interface {
		ShardKVHandler
		ShardItemHandler
		GetRouteVersion() proto.RouteVersion
		TransferLeader(ctx context.Context, diskID proto.DiskID) error
		Checkpoint(ctx context.Context) error
		Stats(ctx context.Context) (shardnode.ShardStats, error)
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

	learner := false
	for _, node := range cfg.shardInfo.Units {
		if node.DiskID == cfg.diskID {
			learner = node.Learner
			break
		}
	}
	// initial members
	members := make([]raft.Member, 0, len(cfg.shardInfo.Units))
	for _, node := range cfg.shardInfo.Units {
		nodeHost, err := cfg.addrResolver.Resolve(ctx, uint64(node.DiskID))
		if err != nil {
			return nil, err
		}
		members = append(members, raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost.String(),
			Type:    raft.MemberChangeType_AddMember,
			Learner: learner,
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

	if len(members) == 1 {
		err = s.raftGroup.Campaign(ctx)
	}

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

func (s *shard) UpdateItem(ctx context.Context, h OpHeader, i shardnode.Item) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

	internalItem := protoItemToInternalItem(i)
	data, err := internalItem.Marshal()
	if err != nil {
		return err
	}
	proposalData := raft.ProposalData{
		Op:   RaftOpUpdateItem,
		Data: data,
	}
	_, err = s.raftGroup.Propose(ctx, &proposalData)

	return err
}

func (s *shard) GetItem(ctx context.Context, h OpHeader, id []byte) (protoItem shardnode.Item, err error) {
	vg, err := s.Get(ctx, h, id)
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
	if err := s.shardState.prepRWCheck(); err != nil {
		return nil, err
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
	nextMarker, err = s.List(ctx, h, prefix, marker, count, rangeFunc)
	if err != nil {
		return
	}
	return items, nextMarker, nil
}

func (s *shard) Insert(ctx context.Context, h OpHeader, kv *KV) error {
	defer kv.Release()
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

	proposalData := raft.ProposalData{
		Op:   RaftOpInsertRaw,
		Data: kv.Marshal(),
	}
	if _, err := s.raftGroup.Propose(ctx, &proposalData); err != nil {
		return err
	}

	return nil
}

func (s *shard) Update(ctx context.Context, h OpHeader, kv *KV) error {
	defer kv.Release()
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

	proposalData := raft.ProposalData{
		Op:   RaftOpUpdateRaw,
		Data: kv.Marshal(),
	}
	if _, err := s.raftGroup.Propose(ctx, &proposalData); err != nil {
		return err
	}

	return nil
}

func (s *shard) Get(ctx context.Context, h OpHeader, key []byte) (ValGetter, error) {
	if err := s.checkShardOptHeader(h); err != nil {
		return nil, err
	}
	if err := s.shardState.prepRWCheck(); err != nil {
		return nil, err
	}
	defer s.shardState.prepRWCheckDone()

	kvStore := s.store.KVStore()
	ret, err := kvStore.Get(ctx, dataCF, s.shardKeys.encodeItemKey(key), nil)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *shard) Delete(ctx context.Context, h OpHeader, key []byte) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

	proposalData := raft.ProposalData{
		Op:   RaftOpDeleteRaw,
		Data: key,
	}
	if _, err := s.raftGroup.Propose(ctx, &proposalData); err != nil {
		return err
	}

	return nil
}

func (s *shard) List(ctx context.Context, h OpHeader, prefix, marker []byte, count uint64, rangeFunc func([]byte) error) (nextMarker []byte, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if err := s.checkShardOptHeader(h); err != nil {
		return nil, err
	}
	if err := s.shardState.prepRWCheck(); err != nil {
		return nil, err
	}
	defer s.shardState.prepRWCheckDone()

	kvStore := s.store.KVStore()
	cursor := kvStore.List(ctx, dataCF, s.shardKeys.encodeItemKey(prefix), s.shardKeys.encodeItemKey(marker), nil)

	count += 1
	for count > 0 {
		kg, vg, err := cursor.ReadNext()
		if err != nil {
			return nil, err
		}
		if vg == nil {
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
			kg.Close()
			vg.Close()
			return nil, err
		}

		kg.Close()
		vg.Close()
		count--
	}
	return s.shardKeys.decodeItemKey(nextMarker), nil
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

func (s *shard) Stats(ctx context.Context) (shardnode.ShardStats, error) {
	if err := s.shardState.prepRWCheck(); err != nil {
		return shardnode.ShardStats{}, err
	}
	defer s.shardState.prepRWCheckDone()

	s.shardInfoMu.RLock()
	units := s.shardInfoMu.Units
	routeVersion := s.shardInfoMu.RouteVersion
	leaderDiskID := s.shardInfoMu.leader
	leaderSuid := proto.Suid(0)
	appliedIndex := s.shardInfoMu.AppliedIndex
	rg := s.shardInfoMu.Range
	learner := false
	for i := range units {
		if units[i].DiskID == leaderDiskID {
			leaderSuid = units[i].GetSuid()
		}
		if units[i].DiskID == s.diskID {
			learner = units[i].Learner
		}
	}
	s.shardInfoMu.RUnlock()

	raftStat, err := s.raftGroup.Stat()
	if err != nil {
		return shardnode.ShardStats{}, err
	}

	leaderHost, err := s.cfg.Transport.ResolveAddr(ctx, leaderDiskID)
	if err != nil {
		return shardnode.ShardStats{}, err
	}

	return shardnode.ShardStats{
		Suid:         s.suid,
		AppliedIndex: appliedIndex,
		LeaderDiskID: leaderDiskID,
		LeaderSuid:   leaderSuid,
		LeaderHost:   leaderHost,
		Learner:      learner,
		RouteVersion: routeVersion,
		Range:        rg,
		Units:        units,
		RaftStat:     *raftStat,
	}, nil
}

// Checkpoint do checkpoint job with raft group
// we should do any memory flush job or dump worker here
func (s *shard) Checkpoint(ctx context.Context) error {
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

	appliedIndex := (*shardSM)(s).getAppliedIndex()

	// save applied index and shard's info
	if err := s.SaveShardInfo(ctx, true, true); err != nil {
		return errors.Info(err, "save shard into failed")
	}

	// truncate raft log finally
	if appliedIndex > s.shardInfoMu.lastTruncatedIndex+s.cfg.TruncateWalLogInterval*2 {
		if err := s.raftGroup.Truncate(ctx, appliedIndex-s.cfg.TruncateWalLogInterval); err != nil {
			return errors.Info(err, "truncate raft wal log failed")
		}
		s.shardInfoMu.lastTruncatedIndex = appliedIndex - s.cfg.TruncateWalLogInterval
	}

	// save last stable index
	s.shardInfoMu.lastStableIndex = appliedIndex

	return nil
}

func (s *shard) UpdateShard(ctx context.Context, op proto.ShardUpdateType, node clustermgr.ShardUnit, nodeHost string) error {
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

	switch op {
	case proto.ShardUpdateTypeAddMember, proto.ShardUpdateTypeUpdateMember:
		return s.raftGroup.MemberChange(ctx, &raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost,
			Type:    raft.MemberChangeType_AddMember,
			Learner: node.Learner,
		})
	case proto.ShardUpdateTypeRemoveMember:
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
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()
	return s.raftGroup.LeaderTransfer(ctx, uint64(diskID))
}

func (s *shard) SaveShardInfo(ctx context.Context, withLock bool, flush bool) error {
	if err := s.shardState.prepRWCheck(); err != nil {
		return err
	}
	defer s.shardState.prepRWCheckDone()

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
		wo := kvStore.NewWriteOption()
		defer wo.Close()
		return kvStore.SetRaw(ctx, dataCF, key, value, kvstore.WithWriteOption(wo))
	}

	if err := kvStore.SetRaw(ctx, dataCF, key, value); err != nil {
		return err
	}
	// todo: flush data, write and lock column family with atomic flush support.
	// todo: need cgo support with FlushCFs
	return kvStore.FlushCF(ctx, dataCF)
}

func (s *shard) DeleteShard(ctx context.Context, nodeHost string) error {
	s.shardInfoMu.Lock()
	defer s.shardInfoMu.Unlock()

	// 1. check raft status and remove member itself
	stat, err := s.raftGroup.Stat()
	if err != nil {
		return errors.Info(err, "raft stat failed")
	}
	if len(stat.Peers) > 1 {
		for _, pr := range stat.Peers {
			if uint64(s.diskID) == pr.NodeID {
				if err := s.raftGroup.MemberChange(ctx, &raft.Member{
					NodeID: uint64(s.diskID),
					Host:   nodeHost,
					Type:   raft.MemberChangeType_RemoveMember,
				}); err != nil {
					return errors.Info(err, "remove raft member failed")
				}
			}
		}
	}

	// 2. stop all writing in this shard
	if err := s.Stop(); err != nil {
		return errors.Info(err, "stop shard failed")
	}

	// 3. clear all shard's data
	kvStore := s.store.KVStore()
	batch := kvStore.NewWriteBatch()

	batch.DeleteRange(dataCF, s.shardKeys.encodeShardDataPrefix(), s.shardKeys.encodeShardDataMaxPrefix())
	batch.Delete(dataCF, s.shardKeys.encodeShardInfoKey())
	if err := kvStore.Write(ctx, batch); err != nil {
		return errors.Info(err, "kvstore write batch failed")
	}

	return nil
}

func (s *shard) Start() {
}

func (s *shard) Stop() error {
	if err := s.shardState.stopWriting(); err != nil {
		return err
	}

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

func (s *shard) GetStableIndex() uint64 {
	return s.shardInfoMu.lastStableIndex
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

// nolint
type shardState struct {
	status        shardStatus
	pendingReqs   int
	pendingReqsWg sync.WaitGroup

	splitting     bool
	lastSplitTime time.Time
	splitDone     chan struct{}

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

func (s *shardState) prepRWCheck() error {
	s.lock.Lock()

	// allow writing check in the list lock arena
	if !s.allowRW() {
		s.lock.Unlock()

		if !s.splitting {
			// return route need update when unbind shard stop write progress
			return apierr.ErrShardRouteVersionNeedUpdate
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
	// check if shard stop writing first
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

// encode item key with prefix: d[shardID]-i-[key]
func (s *shardKeysGenerator) encodeItemKey(key []byte) []byte {
	shardItemPrefixSize := shardItemPrefixSize()
	newKey := make([]byte, shardItemPrefixSize+len(key))
	encodeShardItemPrefix(s.suid, newKey)
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
	encodeShardDataPrefix(s.suid, key)
	return key
}

// encode shard data max prefix with prefix: d[shardID]-max
// it can be used for delete shard's data
func (s *shardKeysGenerator) encodeShardDataMaxPrefix() []byte {
	key := make([]byte, shardMaxPrefixSize())
	encodeShardDataMaxPrefix(s.suid, key)
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
