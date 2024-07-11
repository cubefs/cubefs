package catalog

import (
	"context"
	"encoding/binary"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
)

type (
	spaceConfig struct {
		sid          proto.SpaceID
		spaceName    string
		spaceVersion uint64
		fieldMetas   []clustermgr.FieldMeta
		shardGetter  ShardGetter
	}
)

func newSpace(cfg *spaceConfig) (*Space, error) {
	fieldMetaMap := make(map[proto.FieldID]clustermgr.FieldMeta, len(cfg.fieldMetas))
	for _, field := range cfg.fieldMetas {
		fieldMetaMap[field.ID] = field
	}

	s := &Space{
		sid:         cfg.sid,
		name:        cfg.spaceName,
		fieldMetas:  fieldMetaMap,
		shardGetter: cfg.shardGetter,
	}

	return s, nil
}

type Space struct {
	// immutable
	sid  proto.SpaceID
	name string

	// mutable
	spaceVersion uint64
	fieldMetas   map[proto.FieldID]clustermgr.FieldMeta

	shardGetter ShardGetter
}

func (s *Space) Load() error {
	return nil
}

func (s *Space) InsertItem(ctx context.Context, h shardnode.ShardOpHeader, i shardnode.Item) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.ShardID)
	if err != nil {
		return err
	}
	if !s.validateFields(i.Fields) {
		return apierr.ErrUnknownField
	}

	i.ID = s.generateItemKey(i.ID)

	return shard.InsertItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, i)
}

func (s *Space) UpdateItem(ctx context.Context, h shardnode.ShardOpHeader, i shardnode.Item) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.ShardID)
	if err != nil {
		return err
	}
	if !s.validateFields(i.Fields) {
		return apierr.ErrUnknownField
	}

	i.ID = s.generateItemKey(i.ID)

	return shard.UpdateItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, i)
}

func (s *Space) DeleteItem(ctx context.Context, h shardnode.ShardOpHeader, id []byte) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.ShardID)
	if err != nil {
		return err
	}

	return shard.DeleteItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, s.generateItemKey(id))
}

func (s *Space) GetItem(ctx context.Context, h shardnode.ShardOpHeader, id []byte) (shardnode.Item, error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.ShardID)
	if err != nil {
		return shardnode.Item{}, err
	}

	return shard.GetItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, s.generateItemKey(id))
}

func (s *Space) validateFields(fields []shardnode.Field) bool {
	for i := range fields {
		if _, ok := s.fieldMetas[fields[i].ID]; !ok {
			return false
		}
	}
	return true
}

// generateItemKey item key with space id and space version
// the generated key format like this: [sid]-[id]-[spaceVer]
func (s *Space) generateItemKey(id []byte) []byte {
	// todo: reuse with memory pool
	dest := make([]byte, s.generateItemKeyLen(id))
	binary.BigEndian.PutUint64(dest, uint64(s.sid))

	// todo: align id with 8 bytes padding
	idLen := len(id)
	copy(dest[8:], id)
	// big endian encode and reverse
	// latest space version item will store in front of oldest. eg:
	// sid-id-3
	// sid-id-2
	// sid-id-1
	binary.BigEndian.PutUint64(dest[8+idLen:], s.spaceVersion)
	dest[idLen] = ^dest[idLen]
	dest[idLen+1] = ^dest[idLen+1]
	dest[idLen+2] = ^dest[idLen+2]
	dest[idLen+3] = ^dest[idLen+3]
	dest[idLen+4] = ^dest[idLen+4]
	dest[idLen+5] = ^dest[idLen+5]
	dest[idLen+6] = ^dest[idLen+6]
	dest[idLen+7] = ^dest[idLen+7]
	return dest
}

func (s *Space) generateItemKeyLen(id []byte) int {
	return 8 + len(id) + 8
}
