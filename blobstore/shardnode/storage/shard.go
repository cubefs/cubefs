package storage

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	storageproto "github.com/cubefs/cubefs/blobstore/shardnode/storage/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const keyLocksNum = 1024

const (
	shardStatusNormal        = shardStatus(1)
	shardStatusStopReadWrite = shardStatus(2)
)

type (
	ShardHandler interface {
		InsertItem(ctx context.Context, h OpHeader, i shardnode.Item) error
		UpdateItem(ctx context.Context, h OpHeader, i shardnode.Item) error
		DeleteItem(ctx context.Context, h OpHeader, id []byte) error
		GetItem(ctx context.Context, h OpHeader, id []byte) (shardnode.Item, error)
		ListItem(ctx context.Context, h OpHeader, prefix, id []byte, count uint64) (items []shardnode.Item, nextMarker []byte, err error)
		GetEpoch() uint64
		Checkpoint(ctx context.Context) error
		Stats() ShardStats
	}

	OpHeader struct {
		RouteVersion uint64
		ShardKeys    [][]byte
	}

	ShardBaseConfig struct {
		RaftSnapTransmitConfig RaftSnapshotTransmitConfig `json:"raft_snap_transmit_config"`
		TruncateWalLogInterval uint64                     `json:"truncate_wal_log_interval"`
	}

	ShardStats struct {
		Suid         proto.Suid
		AppliedIndex uint64
		LeaderIdx    uint32
		Epoch        uint64
		Range        sharding.Range
		Units        []shardUnitInfo
	}

	shardConfig struct {
		*ShardBaseConfig
		suid         proto.Suid
		diskID       proto.DiskID
		shardInfo    shardInfo
		store        *store.Store
		raftManager  raft.Manager
		addrResolver raft.AddressResolver
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
		cfg: cfg.ShardBaseConfig,
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

	sf        singleflight.Group
	shardKeys *shardKeysGenerator
	store     *store.Store
	raftGroup raft.Group
	cfg       *ShardBaseConfig
}

func (s *shard) InsertItem(ctx context.Context, h OpHeader, i shardnode.Item) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}

	internalItem := protoItemToInternalItem(i)
	data, err := internalItem.Marshal()
	if err != nil {
		return err
	}
	proposalData := raft.ProposalData{
		Op:   RaftOpInsertItem,
		Data: data,
	}
	if _, err := s.raftGroup.Propose(ctx, &proposalData); err != nil {
		return err
	}

	return nil
}

func (s *shard) UpdateItem(ctx context.Context, h OpHeader, i shardnode.Item) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}

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

func (s *shard) DeleteItem(ctx context.Context, h OpHeader, id []byte) error {
	if !s.isLeader() {
		return apierr.ErrShardNodeNotLeader
	}
	if err := s.checkShardOptHeader(h); err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   RaftOpDeleteItem,
		Data: id,
	}
	_, err := s.raftGroup.Propose(ctx, &proposalData)
	return err
}

func (s *shard) GetItem(ctx context.Context, h OpHeader, id []byte) (protoItem shardnode.Item, err error) {
	if err := s.checkShardOptHeader(h); err != nil {
		return shardnode.Item{}, err
	}

	kvStore := s.store.KVStore()
	data, err := kvStore.GetRaw(ctx, dataCF, s.shardKeys.encodeItemKey(id), nil)
	if err != nil {
		return
	}
	itm := &item{}
	if err = itm.Unmarshal(data); err != nil {
		return
	}

	protoItem.ID = itm.ID
	// transform into external item
	protoItem.Fields = internalFieldsToProtoFields(itm.Fields)
	return
}

func (s *shard) GetItems(ctx context.Context, keys [][]byte) (ret []shardnode.Item, err error) {
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
	if err := s.checkShardOptHeader(h); err != nil {
		return nil, nil, err
	}
	kvStore := s.store.KVStore()
	cursor := kvStore.List(ctx, dataCF, s.shardKeys.encodeItemKey(prefix), s.shardKeys.encodeItemKey(marker), nil)

	count += 1
	for count > 0 {
		kg, vg, err := cursor.ReadNext()
		if err != nil {
			return nil, nil, err
		}
		if vg == nil {
			break
		}

		itm := &item{}
		if err = itm.Unmarshal(vg.Value()); err != nil {
			return nil, nil, err
		}
		if count == 1 {
			nextMarker = make([]byte, len(kg.Key()))
			copy(nextMarker, kg.Key())
			kg.Close()
			vg.Close()
			break
		}

		kg.Close()
		vg.Close()

		items = append(items, shardnode.Item{
			ID:     itm.ID,
			Fields: internalFieldsToProtoFields(itm.Fields),
		})
		count--
	}
	return items, s.shardKeys.decodeItemKey(nextMarker), nil
}

func (s *shard) GetEpoch() uint64 {
	s.shardInfoMu.RLock()
	epoch := s.shardInfoMu.Epoch
	s.shardInfoMu.RUnlock()
	return epoch
}

func (s *shard) Stats() ShardStats {
	s.shardInfoMu.RLock()
	units := s.shardInfoMu.Units
	epoch := s.shardInfoMu.Epoch
	leaderIdx := uint32(0)
	appliedIndex := s.shardInfoMu.AppliedIndex
	rg := s.shardInfoMu.Range
	for i := range units {
		if units[i].DiskID == s.shardInfoMu.leader {
			leaderIdx = uint32(i)
		}
	}
	s.shardInfoMu.RUnlock()

	return ShardStats{
		Suid:         s.suid,
		AppliedIndex: appliedIndex,
		LeaderIdx:    leaderIdx,
		Epoch:        epoch,
		Range:        rg,
		Units:        units,
	}
}

// Checkpoint do checkpoint job with raft group
// we should do any memory flush job or dump worker here
func (s *shard) Checkpoint(ctx context.Context) error {
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

func (s *shard) UpdateShard(ctx context.Context, op proto.ShardUpdateType, node clustermgr.ShardUnit, nodeHost string) {
	switch op {
	case proto.ShardUpdateTypeAddMember, proto.ShardUpdateTypeUpdateMember:
		s.raftGroup.MemberChange(ctx, &raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost,
			Type:    raft.MemberChangeType_AddMember,
			Learner: node.Learner,
		})
	case proto.ShardUpdateTypeRemoveMember:
		s.raftGroup.MemberChange(ctx, &raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    nodeHost,
			Type:    raft.MemberChangeType_RemoveMember,
			Learner: node.Learner,
		})
	}
}

func (s *shard) SaveShardInfo(ctx context.Context, withLock bool, flush bool) error {
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
	// 1. check raft status and remove member itself
	stat, err := s.raftGroup.Stat()
	if err != nil {
		return errors.Info(err, "raft stat failed")
	}
	if len(stat.Nodes) > 1 {
		for _, diskID := range stat.Nodes {
			if uint64(s.diskID) == diskID {
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
	// todo: check shard route version ?
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

func (s *shardState) splitStartWriting() {
	s.lock.Lock()
	s.status = shardStatusNormal
	s.lock.Unlock()

	close(s.splitDone)
}

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

type shardStopper struct {
	stopWriteDone chan struct{}
	pendingReqWg  sync.WaitGroup
}

func protoItemToInternalItem(i shardnode.Item) (ret item) {
	ret = item{
		ID:     i.ID,
		Fields: protoFieldsToInternalFields(i.Fields),
	}
	return
}

func protoFieldsToInternalFields(external []shardnode.Field) []storageproto.Field {
	// todo: use memory pool
	ret := make([]storageproto.Field, len(external))
	for i := range external {
		ret[i] = storageproto.Field{
			ID:    external[i].ID,
			Value: external[i].Value,
		}
	}
	return ret
}

func internalFieldsToProtoFields(internal []storageproto.Field) []shardnode.Field {
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
