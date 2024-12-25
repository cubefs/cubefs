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
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/store"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// Handle encapsulation of underlying data operations

// Storage interface of chunk storage.
type Storage interface {
	ID() clustermgr.ChunkID
	RawStorage() Storage
	ChunkHandler() store.ChunkHandler

	Write(ctx context.Context, b *core.Shard) (n int, err error)
	RangeReader(ctx context.Context, b *core.Shard) (rc io.ReadCloser, err error)
	MarkDelete(ctx context.Context, bid proto.BlobID) (err error)
	Delete(ctx context.Context, bid proto.BlobID) (n int64, err error)

	IncrPendingCnt()
	DecrPendingCnt()
	PendingRequest() int64
	PendingError() error

	SyncData(ctx context.Context) (err error)
	Sync(ctx context.Context) (err error)
	Close(ctx context.Context)
}

type storage struct {
	pendingCnt int64
	handler    store.ChunkHandler
}

func NewStorage(handler store.ChunkHandler) Storage {
	return &storage{handler: handler}
}

func (stg *storage) ID() clustermgr.ChunkID           { return stg.handler.MetaHandler().ID() }
func (stg *storage) RawStorage() Storage              { return nil }
func (stg *storage) ChunkHandler() store.ChunkHandler { return stg.handler }

func (stg *storage) IncrPendingCnt()       { atomic.AddInt64(&stg.pendingCnt, 1) }
func (stg *storage) DecrPendingCnt()       { atomic.AddInt64(&stg.pendingCnt, -1) }
func (stg *storage) PendingRequest() int64 { return atomic.LoadInt64(&stg.pendingCnt) }
func (stg *storage) PendingError() error   { return nil }

func (stg *storage) Write(ctx context.Context, b *core.Shard) (int, error) {
	return stg.handler.Write(ctx, b)
}

func (stg *storage) RangeReader(ctx context.Context, b *core.Shard) (rc io.ReadCloser, err error) {
	return stg.handler.Read(ctx, b)
}

func (stg *storage) MarkDelete(ctx context.Context, bid proto.BlobID) (err error) {
	meta := stg.handler.MetaHandler()
	shardMeta, err := meta.Get(ctx, bid)
	if err != nil {
		return err
	}
	if shardMeta.Flag == bnapi.ShardStatusMarkDelete {
		return bloberr.ErrShardMarkDeleted
	}
	shardMeta.Flag = bnapi.ShardStatusMarkDelete
	return meta.Update(ctx, bid, shardMeta)
}

func (stg *storage) Delete(ctx context.Context, bid proto.BlobID) (n int64, err error) {
	span := getSpan(ctx)
	meta := stg.handler.MetaHandler()

	shardMeta, err := meta.Get(ctx, bid)
	if err != nil {
		span.Errorf("Failed: shard:%v read err:%v", bid, err)
		return n, err
	}
	if shardMeta.Flag != bnapi.ShardStatusMarkDelete {
		span.Errorf("Failed: shard:%v already delete, err:%v", bid, err)
		return n, bloberr.ErrShardNotMarkDelete
	}

	shard := &core.Shard{
		Bid:    bid,
		Vuid:   stg.handler.MetaHandler().ID().VolumeUnitId(),
		Size:   shardMeta.Size,
		Offset: shardMeta.Offset,
		Crc:    shardMeta.Crc,
		Flag:   shardMeta.Flag,
		Inline: shardMeta.Inline,
	}
	if err = stg.handler.Delete(ctx, shard); err != nil {
		span.Errorf("Failed: shard:%v delete err:%v", bid, err)
		return n, err
	}
	return int64(shardMeta.Size), nil
}

func (stg *storage) SyncData(ctx context.Context) (err error) {
	return stg.handler.Flush(ctx)
}

func (stg *storage) Sync(ctx context.Context) (err error) {
	span := getSpan(ctx)
	if err = stg.handler.MetaHandler().Flush(ctx); err != nil {
		span.Errorf("sync meta failed: %v", err)
		return
	}
	if err = stg.handler.Flush(ctx); err != nil {
		span.Errorf("sync data failed: %v", err)
	}
	return err
}

func (stg *storage) Close(ctx context.Context) {
	stg.handler.MetaHandler().Close(ctx)
	stg.handler.Close(ctx)
}

func getSpan(ctx context.Context) trace.Span {
	return trace.SpanFromContextSafe(ctx)
}
