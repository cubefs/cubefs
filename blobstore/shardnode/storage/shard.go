package storage

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"

	"golang.org/x/sync/singleflight"

	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	storageproto "github.com/cubefs/cubefs/blobstore/shardnode/storage/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const keyLocksNum = 1024

type (
	ShardHandler interface {
		InsertItem(ctx context.Context, h OpHeader, i shardnode.Item) error
		UpdateItem(ctx context.Context, h OpHeader, i shardnode.Item) error
		DeleteItem(ctx context.Context, h OpHeader, id []byte) error
		GetItem(ctx context.Context, h OpHeader, id []byte) (shardnode.Item, error)
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

	shardConfig struct {
		*ShardBaseConfig
		diskID       proto.DiskID
		shardInfo    shardInfo
		store        *store.Store
		raftManager  raft.Manager
		addrResolver raft.AddressResolver
	}

	ShardStats struct {
		Leader proto.DiskID
		Epoch  uint64
		Units  []shardUnitInfo
	}
)

func newShard(ctx context.Context, cfg shardConfig) (s *shard, err error) {
	span := trace.SpanFromContext(ctx)
	span.Infof("new shard with config: %+v", cfg)

	s = &shard{
		shardID: cfg.shardInfo.ShardID,
		diskID:  cfg.diskID,

		// startIno: calculateStartIno(cfg.shardInfo.ShardID),

		store: cfg.store,
		shardKeys: &shardKeysGenerator{
			shardID: cfg.shardInfo.ShardID,
		},
		cfg: cfg.ShardBaseConfig,
	}
	s.shardMu.shardInfo = cfg.shardInfo
	/*for _, field := range cfg.fieldMetas {
		if field.Type == proto.FieldMeta_Embedding {
			// todo: supports multiple embedded fields
			s.embeddingField = field.Name
			break
		}
	}
	vectorIndexUserConfig, err := config.ParseAndValidateConfig(&proto.VectorIndexConfig{
		VectorIndexType: cfg.vectorIndexConfig.GetVectorIndexType(),
		UserConfig:      cfg.vectorIndexConfig.GetUserConfig(),
	})
	if err != nil {
		return
	}
	s.vectorIndex, err = vector.NewVectorIndex(vectorIndexUserConfig, s, s.sid, s.shardID)
	if err != nil {
		return
	}*/
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
	shardID            proto.ShardID
	diskID             proto.DiskID
	lastStableIndex    uint64
	lastTruncatedIndex uint64

	shardMu struct {
		sync.RWMutex
		shardInfo
		leader proto.DiskID
	}
	sf singleflight.Group

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

	internalItem := s.protoItemToInternalItem(i)
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

	internalItem := s.protoItemToInternalItem(i)
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

func (s *shard) GetEpoch() uint64 {
	s.shardMu.RLock()
	epoch := s.shardMu.Epoch
	s.shardMu.RUnlock()
	return epoch
}

func (s *shard) Stats() ShardStats {
	s.shardMu.RLock()
	replicates := make([]shardUnitInfo, len(s.shardMu.Units))
	copy(replicates, s.shardMu.Units)
	epoch := s.shardMu.Epoch
	leader := s.shardMu.leader
	s.shardMu.RUnlock()

	return ShardStats{
		Leader: leader,
		Epoch:  epoch,
		Units:  replicates,
	}
}

func (s *shard) Start() {
}

func (s *shard) Stop() {
	// TODO: stop all operation on this shard
}

func (s *shard) Close() {
	// TODO: wait all operation done on this shard and then close shard, ensure memory safe

	s.raftGroup.Close()
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
	if appliedIndex > s.lastTruncatedIndex+s.cfg.TruncateWalLogInterval*2 {
		if err := s.raftGroup.Truncate(ctx, appliedIndex-s.cfg.TruncateWalLogInterval); err != nil {
			return errors.Info(err, "truncate raft wal log failed")
		}
		s.lastTruncatedIndex = appliedIndex - s.cfg.TruncateWalLogInterval
	}

	// save last stable index
	s.lastStableIndex = appliedIndex

	return nil
}

func (s *shard) SaveShardInfo(ctx context.Context, withLock bool, flush bool) error {
	if withLock {
		s.shardMu.Lock()
		defer s.shardMu.Unlock()
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeShardInfoKey()
	value, err := s.shardMu.shardInfo.Marshal()
	if err != nil {
		return err
	}

	if !flush {
		wo := kvStore.NewWriteOption()
		defer wo.Close()
		return kvStore.SetRaw(ctx, dataCF, key, value, wo)
	}

	if err := kvStore.SetRaw(ctx, dataCF, key, value, nil); err != nil {
		return err
	}
	// todo: flush data, write and lock column family with atomic flush support.
	// todo: need cgo support with FlushCFs
	return kvStore.FlushCF(ctx, dataCF)
}

func (s *shard) GetAppliedIndex() uint64 {
	return (*shardSM)(s).getAppliedIndex()
}

func (s *shard) GetStableIndex() uint64 {
	return s.lastStableIndex
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

func (s *shard) protoItemToInternalItem(i shardnode.Item) (ret item) {
	ret = item{
		ID:     i.ID,
		Fields: protoFieldsToInternalFields(i.Fields),
	}
	return
}

func (s *shard) checkShardOptHeader(h OpHeader) error {
	// todo: check shard route version ?
	ci := sharding.NewCompareItem(s.shardMu.Range.Type, h.ShardKeys)
	if s.shardMu.Range.Belong(ci) {
		return apierr.ErrShardRangeMismatch
	}
	return nil
}

func (s *shard) isLeader() bool {
	s.shardMu.RLock()
	isLeader := s.shardMu.leader == s.diskID
	s.shardMu.RUnlock()

	return isLeader
}

type shardKeysGenerator struct {
	shardID proto.ShardID
}

// encode item key with prefix: d[shardID]-i-[key]
func (s *shardKeysGenerator) encodeItemKey(key []byte) []byte {
	shardItemPrefixSize := shardItemPrefixSize()
	newKey := make([]byte, shardItemPrefixSize+len(key))
	encodeShardItemPrefix(s.shardID, newKey)
	copy(newKey[shardItemPrefixSize:], key)
	return key
}

// encode shard info key with prefix: s[shardID]
func (s *shardKeysGenerator) encodeShardInfoKey() []byte {
	key := make([]byte, shardInfoPrefixSize())
	encodeShardInfoPrefix(s.shardID, key)
	return key
}

// encode shard data prefix with prefix: d[shardID]
// it can be used for listing all shard's data or delete shard's data
func (s *shardKeysGenerator) encodeShardDataPrefix() []byte {
	key := make([]byte, shardDataPrefixSize())
	encodeShardDataPrefix(s.shardID, key)
	return key
}

// encode shard data max prefix with prefix: d[shardID]-max
// it can be used for delete shard's data
func (s *shardKeysGenerator) encodeShardDataMaxPrefix() []byte {
	key := make([]byte, shardMaxPrefixSize())
	encodeShardDataMaxPrefix(s.shardID, key)
	return key
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
