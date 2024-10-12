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

package catalog

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/security"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog/allocator"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
)

type (
	spaceConfig struct {
		clusterID    proto.ClusterID
		sid          proto.SpaceID
		spaceName    string
		spaceVersion uint64
		fieldMetas   []clustermgr.FieldMeta
		shardGetter  ShardGetter
		allocator    allocator.Allocator
	}
)

func newSpace(cfg *spaceConfig) (*Space, error) {
	fieldMetaMap := make(map[proto.FieldID]clustermgr.FieldMeta, len(cfg.fieldMetas))
	for _, field := range cfg.fieldMetas {
		fieldMetaMap[field.ID] = field
	}

	s := &Space{
		clusterID:    cfg.clusterID,
		sid:          cfg.sid,
		spaceVersion: cfg.spaceVersion,
		name:         cfg.spaceName,
		fieldMetas:   fieldMetaMap,
		shardGetter:  cfg.shardGetter,
		allocator:    cfg.allocator,
	}

	return s, nil
}

// Space definition
// nolint
type Space struct {
	// immutable
	clusterID proto.ClusterID
	sid       proto.SpaceID
	name      string

	// mutable
	spaceVersion uint64
	fieldMetas   map[proto.FieldID]clustermgr.FieldMeta

	shardGetter ShardGetter
	allocator   allocator.Allocator
}

func (s *Space) Load() error {
	return nil
}

func (s *Space) InsertItem(ctx context.Context, h shardnode.ShardOpHeader, i shardnode.Item) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}
	if !s.validateFields(i.Fields) {
		return apierr.ErrUnknownField
	}

	i.ID = s.generateSpaceKey(i.ID)
	kv, err := storage.InitKV(i.ID, &io.LimitedReader{R: rpc2.Codec2Reader(&i), N: int64(i.Size())})
	if err != nil {
		return err
	}

	return shard.Insert(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, kv)
}

func (s *Space) UpdateItem(ctx context.Context, h shardnode.ShardOpHeader, i shardnode.Item) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}
	if !s.validateFields(i.Fields) {
		return apierr.ErrUnknownField
	}

	i.ID = s.generateSpaceKey(i.ID)

	return shard.UpdateItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, i)
}

func (s *Space) DeleteItem(ctx context.Context, h shardnode.ShardOpHeader, id []byte) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}

	return shard.Delete(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, s.generateSpaceKey(id))
}

func (s *Space) GetItem(ctx context.Context, h shardnode.ShardOpHeader, id []byte) (shardnode.Item, error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return shardnode.Item{}, err
	}

	return shard.GetItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, s.generateSpaceKey(id))
}

func (s *Space) ListItem(ctx context.Context, h shardnode.ShardOpHeader, prefix, marker []byte, count uint64) ([]shardnode.Item, []byte, error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return nil, nil, err
	}

	items, nextMarker, err := shard.ListItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, s.generateSpaceKey(prefix), s.generateSpaceKey(marker), count)
	if err != nil {
		return nil, nil, err
	}
	return items, s.decodeSpaceKey(nextMarker), nil
}

func (s *Space) CreateBlob(ctx context.Context, req *shardnode.CreateBlobArgs) (resp shardnode.CreateBlobRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	_, err = s.GetBlob(ctx, &shardnode.GetBlobArgs{
		Header: req.Header,
		Name:   req.Name,
	})
	if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
		return
	}
	if err == nil {
		err = apierr.ErrBlobAlreadyExists
		return
	}

	// init blob base info
	b := proto.Blob{
		Name: req.Name,
		Location: proto.Location{
			ClusterID: s.clusterID,
			CodeMode:  req.CodeMode,
			SliceSize: req.SliceSize,
			Crc:       0,
		},
		Sealed: false,
	}

	var slices []proto.Slice
	if req.Size_ == 0 {
		goto INSERT
	}

	// alloc slices
	slices, err = s.allocator.AllocSlices(ctx, req.CodeMode, req.Size_, req.SliceSize)
	if err != nil {
		span.Errorf("alloc slice failed, err:%s", err.Error())
		return
	}
	// set slices
	b.Location.Slices = slices
	if err = security.LocationCrcFill(&b.Location); err != nil {
		return
	}

INSERT:
	key := s.generateSpaceKey(req.Name)
	kv, _err := storage.InitKV(key, &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	if _err != nil {
		err = _err
		return
	}

	if _err = sd.Insert(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, kv); _err != nil {
		err = _err
		return
	}
	resp.Blob = b
	return
}

func (s *Space) GetBlob(ctx context.Context, req *shardnode.GetBlobArgs) (resp shardnode.GetBlobRet, err error) {
	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	key := s.generateSpaceKey(req.Name)
	vg, err := sd.Get(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, key)
	if err != nil {
		return
	}

	b := proto.Blob{}
	if err = b.Unmarshal(vg.Value()); err != nil {
		vg.Close()
		return
	}
	vg.Close()

	resp.Blob = b
	return
}

func (s *Space) DeleteBlob(ctx context.Context, req *shardnode.DeleteBlobArgs) error {
	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}
	key := s.generateSpaceKey(req.Name)
	return sd.Delete(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, key)
}

func (s *Space) SealBlob(ctx context.Context, req *shardnode.SealBlobArgs) error {
	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}

	key := s.generateSpaceKey(req.Name)
	vg, err := sd.Get(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, key)
	if err != nil {
		return err
	}

	b := proto.Blob{}
	if err = b.Unmarshal(vg.Value()); err != nil {
		vg.Close()
		return err
	}
	vg.Close()

	if _, ok := checkSlices(b.Location.Slices, req.Slices, b.Location.SliceSize); !ok {
		return apierr.ErrIllegalSlices
	}

	b.Sealed = true
	b.Location.Size_ = req.GetSize_()
	b.Location.Slices = req.GetSlices()
	if err = security.LocationCrcFill(&b.Location); err != nil {
		return err
	}

	kv, err := storage.InitKV(key, &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	if err != nil {
		return err
	}

	return sd.Update(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, kv)
}

func (s *Space) ListBlob(ctx context.Context, h shardnode.ShardOpHeader, prefix, marker []byte, count uint64) (blobs []proto.Blob, nextMarker []byte, err error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}
	rangeFunc := func(data []byte) error {
		b := proto.Blob{}
		if err = b.Unmarshal(data); err != nil {
			return err
		}
		blobs = append(blobs, b)
		return nil
	}
	nextMarker, err = shard.List(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, s.generateSpaceKey(prefix), s.generateSpaceKey(marker), count, rangeFunc)
	if err != nil {
		return nil, nil, err
	}
	return blobs, s.decodeSpaceKey(nextMarker), nil
}

func (s *Space) AllocSlice(ctx context.Context, req *shardnode.AllocSliceArgs) (resp shardnode.AllocSliceRet, err error) {
	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	key := s.generateSpaceKey(req.Name)
	vg, err := sd.Get(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, key)
	if err != nil {
		return
	}
	b := proto.Blob{}
	if err = b.Unmarshal(vg.Value()); err != nil {
		vg.Close()
		return
	}
	vg.Close()

	sliceSize := b.Location.GetSliceSize()
	failedSlice := req.GetFailedSlice()
	localSlices := b.Location.GetSlices()

	var (
		idxes []uint32
		ok    bool
	)
	// check if request slices in local slices
	if idxes, ok = checkSlices(localSlices, []proto.Slice{failedSlice}, sliceSize); !ok {
		err = apierr.ErrIllegalSlices
		return
	}

	// alloc slices
	slices, err := s.allocator.AllocSlices(ctx, req.CodeMode, req.Size_, sliceSize)
	if err != nil {
		return
	}

	// reset blob slice
	idx := idxes[0]
	if failedSlice.Count == 0 && failedSlice.ValidSize == 0 {
		localSlices = append(localSlices[:idx], append(slices, localSlices[idx+1:]...)...)
	} else {
		// request slice part write failed
		localSlices[idx] = failedSlice
		localSlices = append(localSlices[:idx+1], append(slices, localSlices[idx+1:]...)...)
	}

	b.Location.Slices = localSlices
	if err = security.LocationCrcFill(&b.Location); err != nil {
		return
	}

	kv, err := storage.InitKV(key, &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	if err != nil {
		return
	}

	err = sd.Update(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    h.ShardKeys,
	}, kv)
	if err != nil {
		return
	}
	resp.Slices = append(resp.Slices, slices...)
	return
}

func (s *Space) validateFields(fields []shardnode.Field) bool {
	for i := range fields {
		if _, ok := s.fieldMetas[fields[i].ID]; !ok {
			return false
		}
	}
	return true
}

// generateSpaceKey item key with space id and space version
// the generated key format like this: [sid]-[id]-[spaceVer]
func (s *Space) generateSpaceKey(id []byte) []byte {
	// todo: reuse with memory pool
	dest := make([]byte, s.generateSpaceKeyLen(id))
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

func (s *Space) decodeSpaceKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	return key[8 : len(key)-8]
}

func (s *Space) generateSpaceKeyLen(id []byte) int {
	return 8 + len(id) + 8
}

func checkSlices(loc, req []proto.Slice, sliceSize uint32) ([]uint32, bool) {
	idxes := make([]uint32, 0)
	locMap := make(map[proto.BlobID]proto.Slice, len(loc))
	locIndexMap := make(map[proto.BlobID]uint32, len(loc))
	for i := range loc {
		locMap[loc[i].MinSliceID] = loc[i]
		locIndexMap[loc[i].MinSliceID] = uint32(i)
	}
	for i := range req {
		id := req[i].MinSliceID
		if _, ok := locMap[id]; !ok {
			return idxes, false
		}
		if req[i].Count > locMap[id].Count || req[i].ValidSize > uint64(locMap[id].Count*sliceSize) {
			return idxes, false
		}
		idxes = append(idxes, locIndexMap[id])
	}
	return idxes, true
}
