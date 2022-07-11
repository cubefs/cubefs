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
	"encoding/binary"
	"errors"
	"sync"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// Slash separator.
const (
	_chunkShardSpacePrefix = "shards"
)

var _shardVer = []byte{0x1}

var (
	ErrShardBufferSize = errors.New("shard buffer size not match")
	ErrShardKeyPrefix  = errors.New("shard key error prefix")
)

var (
	shardSpacePrefixLen = len(_chunkShardSpacePrefix)
	shardChunkCommonLen = shardSpacePrefixLen + bnapi.ChunkIdLength
	shardKeyLen         = shardChunkCommonLen + 8
)

type metafile struct {
	lock          sync.RWMutex
	db            db.MetaHandler // meta kv db
	id            bnapi.ChunkId  // chunk id
	shardkeyPool  sync.Pool      // shard key pool
	supportInline bool           //
	closed        bool
}

func (cm *metafile) genShardKey(id core.ShardKey) []byte {
	buf := cm.shardkeyPool.Get().([]byte)
	writeShardKey(buf, &id)
	return buf
}

func writeShardKey(buf []byte, id *core.ShardKey) {
	if len(buf) != shardKeyLen {
		panic(ErrShardBufferSize)
	}
	copy(buf[0:shardSpacePrefixLen], []byte(_chunkShardSpacePrefix))
	copy(buf[shardSpacePrefixLen:shardChunkCommonLen], id.Chunk[:])

	binary.BigEndian.PutUint64(buf[shardChunkCommonLen:shardKeyLen], uint64(id.Bid))
}

func GenShardKey(id *core.ShardKey) []byte {
	buf := make([]byte, shardKeyLen)
	writeShardKey(buf, id)
	return buf
}

func GenChunkCommonKey(id bnapi.ChunkId) []byte {
	buf := make([]byte, shardChunkCommonLen)

	copy(buf[0:shardSpacePrefixLen], []byte(_chunkShardSpacePrefix))
	copy(buf[shardSpacePrefixLen:shardChunkCommonLen], id[:])

	return buf
}

func parseShardKey(data []byte) (key core.ShardKey, err error) {
	// pattern => shards${chunk_name}${ShardKey}

	if len(data) != shardKeyLen {
		return key, ErrShardKeyPrefix
	}

	spacePrefix := data[0:shardSpacePrefixLen]
	if !bytes.Equal(spacePrefix, []byte(_chunkShardSpacePrefix)) {
		return key, ErrShardKeyPrefix
	}

	copy(key.Chunk[:], data[shardSpacePrefixLen:shardChunkCommonLen])
	key.Bid = proto.BlobID(binary.BigEndian.Uint64(data[shardChunkCommonLen:shardKeyLen]))

	return key, nil
}

func (cm *metafile) writeData(ctx context.Context, Key []byte, Value []byte) error {
	span := trace.SpanFromContextSafe(ctx)

	kv := rdb.KV{
		Key:   Key,
		Value: Value,
	}

	start := time.Now()

	err := cm.db.Put(ctx, kv)
	span.AppendTrackLog("md.w", start, err)
	if err != nil {
		return err
	}
	return nil
}

func (cm *metafile) readData(ctx context.Context, Key []byte) (data []byte, err error) {
	data, err = cm.db.Get(ctx, Key)
	if err != nil {
		return
	}

	return data, nil
}

func (cm *metafile) deleteKey(ctx context.Context, Key []byte) (err error) {
	err = cm.db.Delete(ctx, Key)
	if err != nil {
		return
	}

	return nil
}

func (cm *metafile) batchDeleteKey(ctx context.Context, keyPrefix []byte) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	iter := cm.db.NewIterator(ctx, func(op *rdb.Op) {
		op.Ro = rdb.NewReadOptions()
		// set fill cache false
		op.Ro.SetFillCache(false)
	})
	defer iter.Close()

	for iter.Seek(keyPrefix); iter.ValidForPrefix(keyPrefix); iter.Next() {
		bidKey := iter.Key().Data()
		err = cm.db.Delete(ctx, bidKey)
		if err != nil {
			span.Errorf("Failed delete prefix<%s> key:%v", keyPrefix, bidKey)
			return err
		}
	}

	span.Infof("cleanchunk <%s> finished.", keyPrefix)

	return
}

func (cm *metafile) ID() bnapi.ChunkId {
	return cm.id
}

func (cm *metafile) InnerDB() db.MetaHandler {
	return cm.db
}

func (cm *metafile) Write(ctx context.Context, bid proto.BlobID, value core.ShardMeta) (err error) {
	id := core.ShardKey{
		Chunk: cm.id,
		Bid:   bid,
	}

	key := cm.genShardKey(id)
	defer cm.shardkeyPool.Put(key) // nolint: staticcheck

	// convert to bytes
	valBytes, _ := value.Marshal()

	return cm.writeData(ctx, key, valBytes)
}

func (cm *metafile) Read(ctx context.Context, bid proto.BlobID) (value core.ShardMeta, err error) {
	id := core.ShardKey{
		Chunk: cm.id,
		Bid:   bid,
	}

	key := cm.genShardKey(id)
	defer cm.shardkeyPool.Put(key) // nolint: staticcheck

	data, err := cm.readData(ctx, key)
	if err != nil {
		return
	}

	err = value.Unmarshal(data)
	if err != nil {
		return
	}

	return value, nil
}

func (cm *metafile) Delete(ctx context.Context, bid proto.BlobID) (err error) {
	id := core.ShardKey{
		Chunk: cm.id,
		Bid:   bid,
	}

	shardKey := cm.genShardKey(id)
	defer cm.shardkeyPool.Put(shardKey) // nolint: staticcheck

	err = cm.deleteKey(ctx, shardKey)
	if err != nil {
		return
	}

	return nil
}

func (cm *metafile) Close() {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if cm.db == nil {
		cm.closed = true
		return
	}

	// It will be automatically recycled when gc
	cm.db = nil
	cm.closed = true
}

func (cm *metafile) SupportInline() bool {
	return cm.supportInline
}

func (cm *metafile) Destroy(ctx context.Context) (err error) {
	keyPrefix := GenChunkCommonKey(cm.id)
	err = cm.batchDeleteKey(ctx, []byte(keyPrefix))
	return
}

// scan a chunk meta file
// from - to ( startBid, ... ]
func (cm *metafile) Scan(ctx context.Context, startBid proto.BlobID, limit int,
	fn func(bid proto.BlobID, sm *core.ShardMeta) error) (err error,
) {
	span := trace.SpanFromContextSafe(ctx)
	iter := cm.db.NewIterator(ctx, func(op *rdb.Op) {
		op.Ro = rdb.NewReadOptions()
		// set fill cache false
		op.Ro.SetFillCache(false)
	})
	defer iter.Close()

	if limit == 0 {
		return bloberr.ErrInvalidParam
	}

	prefix := GenChunkCommonKey(cm.id)

	var startKey []byte
	if startBid == proto.InValidBlobID {
		startKey = prefix
	} else {
		startKey = cm.genShardKey(core.ShardKey{
			Chunk: cm.id,
			Bid:   startBid,
		})
		defer cm.shardkeyPool.Put(startKey) // nolint: staticcheck
	}

	iter.Seek(startKey)
	if startBid > proto.InValidBlobID && iter.ValidForPrefix(prefix) {
		iter.Next()
	}

	for ; iter.ValidForPrefix(prefix) && limit > 0; iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}
		k, v := iter.Key(), iter.Value()
		key, value := k.Data(), v.Data()

		sk, err := parseShardKey(key)
		if err != nil {
			span.Errorf("parse key failed. key:%s, err:%v", string(key), err)
			k.Free()
			v.Free()
			return err
		}

		sm := core.ShardMeta{}
		err = sm.Unmarshal(value)
		if err != nil {
			span.Errorf("parse value failed. err:%v", err)
			k.Free()
			v.Free()
			return err
		}
		k.Free()
		v.Free()

		if err = fn(sk.Bid, &sm); err != nil {
			span.Errorf("fn(%d, %v) occur error. err:%v", sk.Bid, sm, err)
			return err
		}
		limit--
	}

	if limit > 0 {
		return core.ErrChunkScanEOF
	}

	return nil
}

func NewChunkMeta(ctx context.Context, config *core.Config, meta core.VuidMeta, db db.MetaHandler) (cm *metafile, err error) {
	span := trace.SpanFromContextSafe(ctx)

	// check args
	if db == nil || config == nil {
		span.Errorf("db handle <%v> or config <%v> is nil", meta, config)
		return nil, bloberr.ErrInvalidParam
	}

	if !bnapi.IsValidDiskID(meta.DiskID) {
		span.Errorf("diskId is invalid. meta:%v", meta)
		return nil, bloberr.ErrInvalidParam
	}

	cm = &metafile{
		id:            meta.ChunkId,
		db:            db,
		supportInline: config.SupportInline,
		shardkeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, shardKeyLen)
			},
		},
		closed: false,
	}

	return cm, err
}
