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
	"io"
	"sync/atomic"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

/*
 * Handle encapsulation of underlying data operations
 */

type storage struct {
	pendingCnt int64
	meta       core.MetaHandler
	data       core.DataHandler
}

func NewStorage(meta core.MetaHandler, data core.DataHandler) core.Storage {
	return &storage{meta: meta, data: data}
}

func (stg *storage) PendingError() error {
	return nil
}

func (stg *storage) IncrPendingCnt() {
	atomic.AddInt64(&stg.pendingCnt, 1)
}

func (stg *storage) DecrPendingCnt() {
	atomic.AddInt64(&stg.pendingCnt, -1)
}

func (stg *storage) PendingRequest() int64 {
	return atomic.LoadInt64(&stg.pendingCnt)
}

func (stg *storage) ID() bnapi.ChunkId {
	return stg.meta.ID()
}

func (stg *storage) MetaHandler() core.MetaHandler {
	return stg.meta
}

func (stg *storage) DataHandler() core.DataHandler {
	return stg.data
}

func (stg *storage) RawStorage() core.Storage {
	return nil
}

func (stg *storage) Write(ctx context.Context, b *core.Shard) (err error) {
	data, meta := stg.data, stg.meta

	// write data file, will modify *shard
	err = data.Write(ctx, b)
	if err != nil {
		return err
	}

	// write meta
	return meta.Write(ctx, b.Bid, core.ShardMeta{
		Version: _shardVer[0],
		Size:    b.Size,
		Crc:     b.Crc,
		Offset:  b.Offset,
		Flag:    b.Flag,
	})
}

func (stg *storage) ReadShardMeta(ctx context.Context, bid proto.BlobID) (sm *core.ShardMeta, err error) {
	meta := stg.meta
	shard, err := meta.Read(ctx, bid)
	if err != nil {
		return nil, err
	}
	return &shard, nil
}

func (stg *storage) NewRangeReader(ctx context.Context, b *core.Shard, from, to int64) (rc io.Reader, err error) {
	rc, err = stg.data.Read(ctx, b, uint32(from), uint32(to))
	if err != nil {
		return nil, err
	}

	return rc, nil
}

func (stg *storage) MarkDelete(ctx context.Context, bid proto.BlobID) (err error) {
	meta := stg.meta

	shard, err := meta.Read(ctx, bid)
	if err != nil {
		return err
	}

	if shard.Flag == bnapi.ShardStatusMarkDelete {
		return bloberr.ErrShardMarkDeleted
	}

	shard.Flag = bnapi.ShardStatusMarkDelete

	err = meta.Write(ctx, bid, shard)
	if err != nil {
		return err
	}

	return nil
}

func (stg *storage) Delete(ctx context.Context, bid proto.BlobID) (n int64, err error) {
	span := trace.SpanFromContextSafe(ctx)

	meta, data := stg.meta, stg.data

	shardMeta, err := meta.Read(ctx, bid)
	if err != nil {
		span.Errorf("Failed: shard:%v read err:%v", bid, err)
		return n, err
	}

	if shardMeta.Flag != bnapi.ShardStatusMarkDelete {
		span.Errorf("Failed: shard:%v already delete, err:%v", bid, err)
		return n, bloberr.ErrShardNotMarkDelete
	}

	// delete meta
	err = meta.Delete(ctx, bid)
	if err != nil {
		span.Errorf("Failed: shard:%v meta delete:%v", bid, err)
		return n, err
	}

	// data inline , skip
	if shardMeta.Inline {
		return int64(shardMeta.Size), nil
	}

	shard := &core.Shard{
		Vuid:   meta.ID().VolumeUnitId(),
		Bid:    bid,
		Size:   shardMeta.Size,
		Flag:   shardMeta.Flag,
		Offset: shardMeta.Offset,
		Crc:    shardMeta.Crc,
	}

	// discard hole
	err = data.Delete(ctx, shard)
	if err != nil {
		span.Errorf("Failed: shard:%v discard hole err:%v", bid, err)
		return n, err
	}

	return int64(shardMeta.Size), nil
}

func (stg *storage) ScanMeta(ctx context.Context, startBid proto.BlobID, limit int,
	fn func(bid proto.BlobID, sm *core.ShardMeta) error) (err error) {
	return stg.meta.Scan(ctx, startBid, limit, fn)
}

func (stg *storage) SyncData(ctx context.Context) (err error) {
	return stg.data.Flush()
}

func (stg *storage) Sync(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	meta, data := stg.meta, stg.data

	if err = meta.InnerDB().Flush(ctx); err != nil {
		span.Errorf("Failed meta flush, err:%v", err)
	}

	if err = data.Flush(); err != nil {
		span.Errorf("Failed data flush, err:%v", err)
	}

	return err
}

func (stg *storage) Stat(ctx context.Context) (stat *core.StorageStat, err error) {
	return stg.data.Stat()
}

func (stg *storage) Close(ctx context.Context) {
	meta, data := stg.meta, stg.data
	stg.meta, stg.data = nil, nil

	meta.Close()
	data.Close()
}

func (stg *storage) Destroy(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	meta, data := stg.meta, stg.data

	// clean bid meta
	err := meta.Destroy(ctx)
	if err != nil {
		span.Errorf("destroy meta failed: %s", meta.ID())
	}

	// clean data
	err = data.Destroy(ctx)
	if err != nil {
		span.Errorf("destroy data failed: %s", meta.ID())
	}

	meta, data = nil, nil
}
