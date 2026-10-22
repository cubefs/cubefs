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

package disk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// super block (kv storage)
// disk register format:
//
//          disk/${meta_name} -> ${v}
//
// vuid - chunk maptable:
//
//          vuids/${vuid} -> {chunkid}
//
// chunks meta register format:
//
//          chunks/${chunk_name} -> {chunk_meta}
//
// shards meta register format:
//
//          shards/${chunk_name}/${ShardKey} -> {ShardMeta}
//
// data inspect state (DataInspectMgr per-disk inspect store), same kv handler:
//
//          inspect/disk              -> {InspectDiskState JSON}
//          inspect/vuids/${vuid}     -> {InspectChunkState JSON}
//
// Disk-level state records cycle boundaries (CycleStartAt, CycleID).
// Chunk-level state records per-vuid scan progress (Cursor, CycleMaxBid,
// CycleID, Counted, CycleCnt, CycleScanned, BadBids, etc.).
// Keys use dedicated prefixes that do not
// collide with the
// existing key spaces. Orphan chunk entries whose vuid no longer exists on disk are
// garbage-collected at load time via gcOrphanInspectState.
//

//
// example layout:
//
// meta/superblock ──┬─ disk/
//                   │   └─ diskinfo ............. DiskMeta
//                   ├─ vuids/
//                   │   └─ ${Vuid} .............. ChunkID
//                   ├─ chunks/
//                   │   └─ ${ChunkID} ........... VuidMeta
//                   ├─ shards/
//                   │   └─ ${ChunkID}/
//                   │       └─ ${Bid} ........... ShardMeta
//                   ├─ inspect/disk ............. InspectDiskState
//                   └─ inspect/vuids/
//                       └─ ${Vuid} .............. InspectChunkState
//
//

const (
	slashSeparator = "/"
)

const (
	_diskSpacePrefix        = "disk"
	_chunkSpacePrefix       = "chunks"
	_vuidSpacePrefix        = "vuids"
	_inspectSpacePrefix     = "inspect"
	_inspectVuidSpacePrefix = "inspect/vuids"

	_diskmetaKey = "diskinfo"
)

var (
	ErrVuidSpaceKeyPrefix     = errors.New("disk: vuid space prefix key error")
	errInspectChunkStateRange = errors.New("disk: inspect chunk state range")
)

// "inspect/disk"
func GenInspectDiskStateKey() []byte {
	return []byte(fmt.Sprintf("%s/disk", _inspectSpacePrefix))
}

// "inspect/vuids/${vuid}"
func GenInspectChunkStateKey(vuid proto.Vuid) []byte {
	return []byte(fmt.Sprintf("%s/%d", _inspectVuidSpacePrefix, uint64(vuid)))
}

const (
	sbOpen   int32 = iota // open, db available
	sbLocked              // inspect is using db
	sbClosed              // Close finished, and db must not be used
)

type SuperBlock struct {
	db     db.MetaHandler
	closed int32 // 0 = open, 1 = locked (inspect using db), 2 = closed
}

// tryLock marks the superblock in-use for inspect db IO. False means closed or already locked.
func (s *SuperBlock) tryLock() bool {
	return atomic.CompareAndSwapInt32(&s.closed, sbOpen, sbLocked)
}

func (s *SuperBlock) unlock() {
	atomic.CompareAndSwapInt32(&s.closed, sbLocked, sbOpen)
}

func GenChunkKey(id clustermgr.ChunkID) string {
	return fmt.Sprintf("%s/%s", _chunkSpacePrefix, id)
}

func GenDiskKey(id string) string {
	return fmt.Sprintf("%s/%s", _diskSpacePrefix, id)
}

func GenVuidSpaceKey(vuid proto.Vuid) string {
	return fmt.Sprintf("%s%s%d", _vuidSpacePrefix, slashSeparator, vuid)
}

func parseVuidSpacePrefix(key string) (vuid proto.Vuid, err error) {
	strs := strings.Split(key, slashSeparator)
	prefix, vuidstr := strs[0], strs[1]
	if prefix != _vuidSpacePrefix {
		return vuid, ErrVuidSpaceKeyPrefix
	}

	id, err := strconv.ParseUint(vuidstr, 10, 64)
	if err != nil {
		return vuid, err
	}

	return proto.Vuid(id), nil
}

func (s *SuperBlock) writeData(ctx context.Context, Key []byte, Value []byte) error {
	if Key == nil || Value == nil {
		return bloberr.ErrInvalidParam
	}

	kv := rdb.KV{
		Key:   Key,
		Value: Value,
	}

	err := s.db.Put(ctx, kv)
	if err != nil {
		return err
	}

	return nil
}

func (s *SuperBlock) readData(ctx context.Context, Key []byte) (data []byte, err error) {
	if Key == nil {
		return nil, bloberr.ErrInvalidParam
	}

	data, err = s.db.Get(ctx, Key)
	if err != nil {
		return
	}

	return data, nil
}

func (s *SuperBlock) UpsertChunk(ctx context.Context, id clustermgr.ChunkID, vm core.VuidMeta) (err error) {
	if !bnapi.IsValidChunkID(id) {
		return bloberr.ErrInvalidChunkId
	}

	data, err := json.Marshal(vm)
	if err != nil {
		return err
	}
	key := []byte(GenChunkKey(id))

	return s.writeData(ctx, key, data)
}

func (s *SuperBlock) BindVuidChunk(ctx context.Context, vuid proto.Vuid, id clustermgr.ChunkID) (err error) {
	if !bnapi.IsValidChunkID(id) {
		return bloberr.ErrInvalidChunkId
	}

	key := []byte(GenVuidSpaceKey(vuid))
	value := []byte(id.String())

	return s.writeData(ctx, key, value)
}

func (s *SuperBlock) UnbindVuidChunk(ctx context.Context, vuid proto.Vuid, id clustermgr.ChunkID) (err error) {
	key := []byte(GenVuidSpaceKey(vuid))

	err = s.db.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

func (s *SuperBlock) ReadVuidBind(ctx context.Context, vuid proto.Vuid) (id clustermgr.ChunkID, err error) {
	key := []byte(GenVuidSpaceKey(vuid))
	data, err := s.readData(ctx, key)
	if err != nil {
		return clustermgr.InvalidChunkID, err
	}

	id, err = clustermgr.DecodeChunk(string(data))
	if err != nil {
		return clustermgr.InvalidChunkID, err
	}
	return id, nil
}

func (s *SuperBlock) ReadChunk(ctx context.Context, id clustermgr.ChunkID) (vm core.VuidMeta, err error) {
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidChunkID(id) {
		span.Errorf("Invalid chunkid:%v", id)
		return vm, bloberr.ErrInvalidChunkId
	}

	key := []byte(GenChunkKey(id))
	data, err := s.readData(ctx, key)
	if err != nil {
		span.Errorf("Failed readData: %s, err:%v", string(key), err)
		return
	}

	err = json.Unmarshal(data, &vm)
	if err != nil {
		span.Errorf("Failed unmarshal, err:%v", err)
		return
	}

	return vm, err
}

func (s *SuperBlock) UpsertDisk(ctx context.Context, diskid proto.DiskID, dm core.DiskMeta) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("update disk, diskid:%v, dm:%v", diskid, dm)

	if !bnapi.IsValidDiskID(diskid) {
		span.Errorf("Invalid diskID:%d", diskid)
		return bloberr.ErrInvalidParam
	}

	diskmeta, err := json.Marshal(dm)
	if err != nil {
		span.Errorf("Failed marshal diskID:%d, err:%v", diskid, err)
		return err
	}

	key := []byte(GenDiskKey(_diskmetaKey))
	value := diskmeta

	return s.writeData(ctx, key, value)
}

func (s *SuperBlock) LoadDiskInfo(ctx context.Context) (dm core.DiskMeta, err error) {
	span := trace.SpanFromContextSafe(ctx)

	key := []byte(GenDiskKey(_diskmetaKey))
	data, err := s.readData(ctx, key)
	if err != nil {
		span.Errorf("Failed readData: %s, err:%v", string(key), err)
		return
	}

	err = json.Unmarshal(data, &dm)
	if err != nil {
		span.Errorf("Failed unmarshal, err:%v", err)
		return
	}

	return dm, err
}

func (s *SuperBlock) ListChunks(ctx context.Context) (chunks map[clustermgr.ChunkID]core.VuidMeta, err error) {
	iter := s.db.NewIterator(ctx)
	defer iter.Close()

	prefix := []byte(_chunkSpacePrefix)

	chunks = make(map[clustermgr.ChunkID]core.VuidMeta)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		v := iter.Value()
		value := v.Data()

		vm := core.VuidMeta{}
		err = json.Unmarshal(value, &vm)
		if err != nil {
			v.Free()
			return
		}
		v.Free()

		chunks[vm.ChunkID] = vm
	}

	if err = iter.Err(); err != nil {
		return nil, err
	}

	return chunks, nil
}

func (s *SuperBlock) ListVuids(ctx context.Context) (vuids map[proto.Vuid]clustermgr.ChunkID, err error) {
	span := trace.SpanFromContextSafe(ctx)

	iter := s.db.NewIterator(ctx)
	defer iter.Close()

	prefix := []byte(_vuidSpacePrefix)

	vuids = make(map[proto.Vuid]clustermgr.ChunkID)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		k, v := iter.Key(), iter.Value()
		key, value := k.Data(), v.Data()

		vuid, err := parseVuidSpacePrefix(string(key))
		if err != nil {
			span.Errorf("Failed parse key, key:%s, err:%v", key, err)
			k.Free()
			v.Free()
			return nil, err
		}

		chunkid, err := clustermgr.DecodeChunk(string(value))
		if err != nil {
			k.Free()
			v.Free()
			return nil, err
		}
		k.Free()
		v.Free()

		vuids[vuid] = chunkid
	}

	if err = iter.Err(); err != nil {
		span.Errorf("occur err:%v during iteration", err)
		return nil, err
	}

	return vuids, nil
}

func (s *SuperBlock) CleanChunkSpace(ctx context.Context, id clustermgr.ChunkID) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	minKeyID := &core.ShardKey{
		Chunk: id,
		Bid:   proto.InValidBlobID,
	}
	maxKeyID := &core.ShardKey{
		Chunk: id,
		Bid:   proto.MaxBlobID,
	}

	minKey := storage.GenShardKey(minKeyID)
	maxKey := storage.GenShardKey(maxKeyID)

	err = s.db.DeleteRange(ctx, minKey, maxKey)
	if err != nil {
		span.Errorf("Failed delete range, minKey:%v, maxKey:%v, err:%v", minKey, maxKey, err)
		return
	}

	span.Infof("clean chunk <%s> finished. minKey:%v, maxKey:%v", id, minKey, maxKey)

	return nil
}

func (s *SuperBlock) DeleteChunk(ctx context.Context, id clustermgr.ChunkID) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidChunkID(id) {
		return bloberr.ErrInvalidChunkId
	}

	key := []byte(GenChunkKey(id))

	err = s.db.Delete(ctx, key)
	if err != nil {
		span.Errorf("Failed delete key:%s", key)
		return err
	}

	span.Infof("delete id:%s success", id)

	return nil
}

func (s *SuperBlock) SetHandlerIOError(handleIOError func(err error)) {
	s.db.SetHandleIOError(handleIOError)
}

func (s *SuperBlock) Close(ctx context.Context) error {
	for !atomic.CompareAndSwapInt32(&s.closed, sbOpen, sbClosed) {
		if atomic.LoadInt32(&s.closed) == sbClosed {
			return nil
		}
		runtime.Gosched()
	}
	s.db.Close(ctx)
	s.db = nil
	return nil
}

// DataInspectMgr persisted state uses the inspect key layout defined above and the
// shared inspect state types from core/proto.go.

func (s *SuperBlock) ReadInspectDiskState(ctx context.Context) (st core.InspectDiskState, err error) {
	span := trace.SpanFromContextSafe(ctx)
	data, err := s.readData(ctx, GenInspectDiskStateKey())
	if err != nil {
		return st, err
	}
	if err = json.Unmarshal(data, &st); err != nil {
		span.Warnf("read inspect disk state failed: %+v", err)
		return st, err
	}
	return st, nil
}

func (s *SuperBlock) UpsertInspectDiskState(ctx context.Context, st core.InspectDiskState) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !bnapi.IsValidDiskID(st.DiskID) {
		span.Errorf("Invalid diskID:%d", st.DiskID)
		return bloberr.ErrInvalidParam
	}

	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if err = s.writeData(ctx, GenInspectDiskStateKey(), data); err != nil {
		return err
	}
	return nil
}

func (s *SuperBlock) ReadInspectChunkState(ctx context.Context, vuid proto.Vuid) (st core.InspectChunkState, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !vuid.IsValid() {
		span.Errorf("Invalid vuid:%d", vuid)
		return st, bloberr.ErrInvalidParam
	}

	data, err := s.readData(ctx, GenInspectChunkStateKey(vuid))
	if err != nil {
		return st, err
	}
	if err = json.Unmarshal(data, &st); err != nil {
		span.Warnf("read inspect chunk state failed: %+v", err)
		return st, err
	}
	return st, nil
}

func (s *SuperBlock) UpsertInspectChunkState(ctx context.Context, st core.InspectChunkState) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !st.Vuid.IsValid() {
		span.Errorf("Invalid vuid:%d", st.Vuid)
		return bloberr.ErrInvalidParam
	}

	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return s.writeData(ctx, GenInspectChunkStateKey(st.Vuid), data)
}

func (s *SuperBlock) DeleteInspectChunkState(ctx context.Context, vuid proto.Vuid) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !vuid.IsValid() {
		span.Errorf("Invalid vuid:%d", vuid)
		return bloberr.ErrInvalidParam
	}

	return s.db.Delete(ctx, GenInspectChunkStateKey(vuid))
}

func (s *SuperBlock) RangeInspectChunkState(ctx context.Context, fn func(st *core.InspectChunkState) bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	iter := s.db.NewIterator(ctx)
	defer iter.Close()

	prefix := []byte(_inspectVuidSpacePrefix + slashSeparator)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		v := iter.Value()
		data := v.Data()
		st := &core.InspectChunkState{}
		if err := json.Unmarshal(data, st); err != nil {
			v.Free()
			return err
		}
		v.Free()

		if !fn(st) {
			return errInspectChunkStateRange
		}
	}

	if err = iter.Err(); err != nil {
		span.Errorf("occur err:%v during iteration", err)
		return err
	}

	return nil
}

func NewSuperBlock(dirpath string, conf *core.Config) (s *SuperBlock, err error) {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", dirpath)

	span.Debugf("New super block, conf:%v", conf)

	if dirpath == "" || conf == nil {
		return nil, bloberr.ErrInvalidParam
	}

	metadb, err := db.NewMetaHandler(dirpath, conf.MetaConfig)
	if err != nil {
		span.Errorf("new meta failed. path:<%s> err:%v", dirpath, err)
		return nil, err
	}

	s = &SuperBlock{
		db: metadb,
	}

	span.Debugf("New super block success")

	return s, err
}
