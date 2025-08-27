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
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/security"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog/allocator"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
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

const (
	opAlloc  = "a"
	opGet    = "g"
	opInsert = "i"
	opUpdate = "u"
	opDelete = "d"

	blobTraceTag = "BlobName"
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

	return shard.InsertItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(i.ID, shard.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(i.ID)), i)
}

func (s *Space) UpdateItem(ctx context.Context, h shardnode.ShardOpHeader, i shardnode.Item) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}
	if !s.validateFields(i.Fields) {
		return apierr.ErrUnknownField
	}

	return shard.UpdateItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(i.ID, shard.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(i.ID)), i)
}

func (s *Space) DeleteItem(ctx context.Context, h shardnode.ShardOpHeader, id string) error {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}

	return shard.DeleteItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(id, shard.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(id)))
}

func (s *Space) GetItem(ctx context.Context, h shardnode.ShardOpHeader, id string) (shardnode.Item, error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return shardnode.Item{}, err
	}

	return shard.GetItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(id, shard.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(id)))
}

func (s *Space) ListItem(ctx context.Context, h shardnode.ShardOpHeader, prefix, marker string, count uint64) ([]shardnode.Item, string, error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return nil, "", err
	}

	var _marker []byte
	if len(marker) > 0 {
		_marker = s.generateSpaceKey([]byte(marker))
	}
	items, nextMarker, err := shard.ListItem(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
	}, s.generateSpacePrefix([]byte(prefix)), _marker, count)
	if err != nil {
		return nil, "", err
	}
	if len(nextMarker) > 0 {
		nextMarker = s.decodeSpaceKey(nextMarker)
	}
	return items, string(nextMarker), nil
}

func (s *Space) CreateBlob(ctx context.Context, req *shardnode.CreateBlobArgs) (resp shardnode.CreateBlobRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.SetTag(blobTraceTag, string(req.Name))

	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	_, getErr := s.getBlob(ctx, sd, req.Header, req.Name)
	if getErr != nil && !errors.Is(getErr, apierr.ErrKeyNotFound) {
		err = getErr
		return
	}
	if getErr == nil {
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

	var (
		start  time.Time
		slices []proto.Slice
	)
	if req.Size_ == 0 {
		goto INSERT
	}

	start = time.Now()
	// alloc slices
	slices, err = s.allocator.AllocSlices(ctx, req.CodeMode, req.Size_, req.SliceSize, nil)
	span.AppendTrackLog(opAlloc, start, err, trace.OptSpanDurationUs())
	if err != nil {
		err = errors.Info(err, "alloc slices failed")
		return
	}
	// set slices
	b.Location.Slices = slices
	if err = security.LocationCrcFill(&b.Location); err != nil {
		return
	}

INSERT:
	start = time.Now()
	cb, _err := sd.CreateBlob(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(req.Name, sd.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(req.Name)), b)
	span.AppendTrackLog(opInsert, start, _err, trace.OptSpanDurationUs())
	if _err != nil {
		err = errors.Info(_err, "insert kv failed")
		return
	}
	if len(cb.Name) < 1 {
		err = errors.New("get empty blob after create")
		return
	}
	resp.Blob = cb
	return
}

func (s *Space) GetBlob(ctx context.Context, req *shardnode.GetBlobArgs) (resp shardnode.GetBlobRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.SetTag(blobTraceTag, string(req.Name))

	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	blob, err := s.getBlob(ctx, sd, req.Header, req.Name)
	if err != nil {
		return
	}
	resp.Blob = blob
	return
}

func (s *Space) DeleteBlob(ctx context.Context, req *shardnode.DeleteBlobArgs, items []shardnode.Item) error {
	span := trace.SpanFromContextSafe(ctx)
	span.SetTag(blobTraceTag, string(req.Name))

	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}

	start := time.Now()
	err = sd.DeleteBlob(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(req.Name, sd.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(req.Name)), items)
	span.AppendTrackLog(opDelete, start, err, trace.OptSpanDurationUs())
	return err
}

func (s *Space) SealBlob(ctx context.Context, req *shardnode.SealBlobArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.SetTag(blobTraceTag, string(req.Name))

	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return err
	}

	b, err := s.getBlob(ctx, sd, req.Header, req.Name)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			b, _err := s.getBlob(ctx, sd, req.Header, req.Name)
			if _err != nil {
				span.Errorf("get blob failed, blob name: %s, err: %s", string(req.Name), _err.Error())
				return
			}
			span.Errorf("seal failed, blob: %+v, err: %s", b, err.Error())
		}
	}()

	if len(b.Location.Slices) != len(req.Slices) {
		span.Errorf("slices length not equal")
		return apierr.ErrIllegalSlices
	}

	b.Sealed = true
	b.Location.Size_ = req.GetSize_()

	sliceSize := b.Location.SliceSize
	remainSize := req.GetSize_()
	for i := range req.Slices {
		if !compareSlice(req.Slices[i], b.Location.Slices[i]) {
			err = apierr.ErrIllegalSlices
			return
		}

		/*last slice may not be full written,
		0 < remainSize <= req.Slices[i].Count*sliceSize
		and remainSize must equal to req.Slices[i].ValidSize*/
		if i == len(req.Slices)-1 {
			if remainSize > uint64(req.Slices[i].Count*sliceSize) ||
				remainSize <= 0 ||
				req.Slices[i].ValidSize != remainSize {
				err = apierr.ErrIllegalLocationSize
				return
			}
			b.Location.Slices[i].ValidSize = remainSize
			b.Location.Slices[i].Count = req.Slices[i].Count
			break
		}

		/*local validSize recorded: blob was re-allocated,
		and current slice is not the last slice,
		req.Slice[i].ValidSize must equal to b.Location.Slices[i].ValidSize*/
		if b.Location.Slices[i].ValidSize != 0 {
			validSize := b.Location.Slices[i].ValidSize
			if validSize >= remainSize || req.Slices[i].ValidSize != validSize ||
				req.Slices[i].Count != b.Location.Slices[i].Count {
				err = apierr.ErrIllegalLocationSize
				return
			}
			remainSize -= validSize
			continue
		}

		/*local validSize not recorded: blob was not re-allocated, or
		current slice is remaining slices after re-allocated, and not the last slice*/
		validSize := uint64(req.Slices[i].Count * sliceSize)
		if validSize >= remainSize {
			err = apierr.ErrIllegalLocationSize
			return
		}
		b.Location.Slices[i].ValidSize = validSize
		remainSize -= validSize
	}

	if err = security.LocationCrcFill(&b.Location); err != nil {
		return err
	}

	start := time.Now()
	err = sd.UpdateBlob(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(req.Name, sd.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(req.Name)), b)
	span.AppendTrackLog(opUpdate, start, err, trace.OptSpanDurationUs())
	if err != nil {
		err = errors.Info(err, "update kv failed")
		return err
	}
	return nil
}

func (s *Space) ListBlob(ctx context.Context, h shardnode.ShardOpHeader, prefix, marker string, count uint64) (blobs []proto.Blob, nextMarker string, err error) {
	shard, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	var _marker []byte
	if len(marker) > 0 {
		_marker = s.generateSpaceKey([]byte(marker))
	}

	blobs, bytesNextMarker, err := shard.ListBlob(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
	}, s.generateSpacePrefix([]byte(prefix)), _marker, count)
	if err != nil {
		err = errors.Info(err, "shard list blob failed")
		return nil, "", err
	}
	if len(bytesNextMarker) > 0 {
		bytesNextMarker = s.decodeSpaceKey(bytesNextMarker)
	}
	return blobs, string(bytesNextMarker), nil
}

func (s *Space) AllocSlice(ctx context.Context, req *shardnode.AllocSliceArgs) (resp shardnode.AllocSliceRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.SetTag(blobTraceTag, string(req.Name))

	h := req.Header
	sd, err := s.shardGetter.GetShard(h.DiskID, h.Suid)
	if err != nil {
		return
	}

	getBlobRet, err := s.GetBlob(ctx, &shardnode.GetBlobArgs{
		Header: req.Header,
		Name:   req.Name,
	})
	if err != nil {
		return
	}
	b := getBlobRet.Blob

	if b.Sealed {
		err = apierr.ErrBlobAlreadySealed
		return
	}

	sliceSize := b.Location.GetSliceSize()
	failedSlice := req.GetFailedSlice()
	localSlices := b.Location.GetSlices()

	var (
		failedVid        []proto.Vid
		failedIdx        = -1
		fillValidSizeIdx int
	)
	// check if request slices in local slices
	if !isEmptySlice(req.FailedSlice) {
		failedVid = []proto.Vid{req.FailedSlice.Vid}
		for i := range localSlices {
			if !compareSlice(localSlices[i], failedSlice) {
				continue
			}
			failedIdx = i
			break
		}
		if failedIdx < 0 {
			return resp, apierr.ErrIllegalSlices
		}
	}

	// alloc slices
	start := time.Now()
	slices, err := s.allocator.AllocSlices(ctx, req.CodeMode, req.Size_, sliceSize, failedVid)
	span.AppendTrackLog(opAlloc, start, err, trace.OptSpanDurationUs())
	if err != nil {
		err = errors.Info(err, "alloc slices failed")
		return
	}

	// reset blob slice
	// find failed slice in localSlices
	if !(failedIdx < 0) {
		if failedSlice.Count == 0 && failedSlice.ValidSize == 0 {
			localSlices = append(localSlices[:failedIdx], append(slices, localSlices[failedIdx+1:]...)...)
		} else {
			// request slice part write failed
			localSlices[failedIdx] = failedSlice
			localSlices = append(localSlices[:failedIdx+1], append(slices, localSlices[failedIdx+1:]...)...)
		}
		fillValidSizeIdx = failedIdx
	} else {
		fillValidSizeIdx = len(localSlices)
		localSlices = append(localSlices, slices...)
	}

	for i := 0; i < fillValidSizeIdx; i++ {
		// validSize has filled when last time allocSlice
		if localSlices[i].ValidSize > 0 {
			continue
		}
		// todo: when support append write, can not calculate this way
		localSlices[i].ValidSize = uint64(sliceSize * localSlices[i].Count)
	}

	b.Location.Slices = localSlices
	if err = security.LocationCrcFill(&b.Location); err != nil {
		return
	}

	start = time.Now()
	err = sd.UpdateBlob(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(req.Name, sd.ShardingSubRangeCount()),
	}, s.generateSpaceKey([]byte(req.Name)), b)
	span.AppendTrackLog(opUpdate, start, err, trace.OptSpanDurationUs())
	if err != nil {
		err = errors.Info(err, "update kv failed")
		return
	}
	resp.Slices = append(resp.Slices, slices...)
	return
}

func (s *Space) GetShardingSubRangeCount(diskID proto.DiskID, suid proto.Suid) (int, error) {
	sd, err := s.shardGetter.GetShard(diskID, suid)
	if err != nil {
		return -1, err
	}
	return sd.ShardingSubRangeCount(), nil
}

func (s *Space) getBlob(ctx context.Context, sd storage.ShardHandler, h shardnode.ShardOpHeader, name string) (b proto.Blob, err error) {
	span := trace.SpanFromContextSafe(ctx)
	key := s.generateSpaceKey([]byte(name))

	start := time.Now()
	b, err = sd.GetBlob(ctx, storage.OpHeader{
		RouteVersion: h.RouteVersion,
		ShardKeys:    shardnode.DecodeShardKeys(name, sd.ShardingSubRangeCount()),
	}, key)

	withErr := err
	if errors.Is(err, apierr.ErrKeyNotFound) {
		withErr = nil
	}
	span.AppendTrackLog(opGet, start, withErr, trace.OptSpanDurationUs())
	if err != nil {
		return
	}
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
	spaceKeyLen, paddingLen := s.generateSpaceKeyLen(id)
	dest := make([]byte, spaceKeyLen)
	// put prefix first
	copy(dest, snproto.SpaceDataPrefix)

	index := 1
	binary.BigEndian.PutUint64(dest[index:], uint64(s.sid))
	index += 8

	// align id with 8 bytes padding
	idLen := len(id)
	copy(dest[index:], id)
	index += idLen
	for i := 0; i < paddingLen; i++ {
		dest[index+i] = 0 // Padding with 0s
	}
	index += paddingLen

	// big endian encode and reverse
	// latest space version item will store in front of oldest. eg:
	// sid-id-3
	// sid-id-2
	// sid-id-1
	binary.BigEndian.PutUint64(dest[index:], s.spaceVersion)
	for i := 0; i < 8; i++ {
		dest[index+i] = ^dest[index+i]
	}
	index += 8
	// Record paddingLen in the last 8 bytes
	binary.BigEndian.PutUint64(dest[index:], uint64(paddingLen))
	return dest
}

func (s *Space) generateSpacePrefix(prefix []byte) []byte {
	dest := make([]byte, s.generateSpacePrefixLen(prefix))
	copy(dest, snproto.SpaceDataPrefix)

	binary.BigEndian.PutUint64(dest[1:], uint64(s.sid))
	copy(dest[9:], prefix)
	return dest
}

func (s *Space) decodeSpaceKey(key []byte) []byte {
	if len(key) < 25 {
		panic(fmt.Sprintf("decode illegal space key: %+v", key))
	}
	// extract paddingLen from the last 8 bytes
	paddingLen := int(binary.BigEndian.Uint64(key[len(key)-8:]))
	// calculate the total length of id and padding
	totalIdLen := len(key) - 16 - 8 - 1
	if totalIdLen < 0 {
		return nil
	}
	// extract id and padding
	idWithPadding := key[9 : 9+totalIdLen]

	// remove padding to get the original id
	return idWithPadding[:totalIdLen-paddingLen]
}

func (s *Space) generateSpaceKeyLen(id []byte) (int, int) {
	idLen := len(id)
	paddingLen := (8 - (idLen % 8)) % 8
	// prefix[1] + spaceId[8] + len(id) + len(padding) + spaceVersion[8] + paddingLen[8]
	return 1 + 8 + len(id) + paddingLen + 8 + 8, paddingLen
}

func (s *Space) generateSpacePrefixLen(prefix []byte) int {
	return 1 + 8 + len(prefix)
}

func isEmptySlice(s proto.Slice) bool {
	return s.MinSliceID == proto.InValidBlobID && s.Vid == proto.InvalidVid && s.ValidSize == 0 && s.Count == 0
}

func compareSlice(src, dst proto.Slice) bool {
	return src.MinSliceID == dst.MinSliceID && src.Vid == dst.Vid
}
