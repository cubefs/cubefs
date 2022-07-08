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
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type mockBrokenMeta struct {
	id bnapi.ChunkId
}

type mockBrokenData struct{}

func (mm *mockBrokenMeta) ID() bnapi.ChunkId {
	return mm.id
}

func (mm *mockBrokenMeta) InnerDB() db.MetaHandler {
	return nil
}

func (mm *mockBrokenMeta) Write(ctx context.Context, bid proto.BlobID, value core.ShardMeta) (err error) {
	return bloberr.ErrUnexpected
}

func (mm *mockBrokenMeta) Read(ctx context.Context, bid proto.BlobID) (value core.ShardMeta, err error) {
	err = bloberr.ErrUnexpected

	return
}

func (mm *mockBrokenMeta) Delete(ctx context.Context, bid proto.BlobID) (err error) {
	err = bloberr.ErrUnexpected
	return
}

func (mm *mockBrokenMeta) Scan(ctx context.Context, startBid proto.BlobID, limit int,
	fn func(bid proto.BlobID, sm *core.ShardMeta) error) (err error) {
	err = bloberr.ErrUnexpected
	return
}

func (mm *mockBrokenMeta) SupportInline() bool {
	return false
}

func (mm *mockBrokenMeta) Destroy(ctx context.Context) (err error) {
	err = bloberr.ErrUnexpected
	return
}

func (mm *mockBrokenMeta) Close() {
}

func (mm *mockBrokenData) Write(ctx context.Context, shard *core.Shard) error {
	return bloberr.ErrUnexpected
}

func (mm *mockBrokenData) Read(ctx context.Context, shard *core.Shard, from, to uint32) (r io.Reader, err error) {
	return r, bloberr.ErrUnexpected
}

func (mm *mockBrokenData) Stat() (stat *core.StorageStat, err error) {
	err = bloberr.ErrUnexpected

	return
}

func (mm *mockBrokenData) Flush() (err error) {
	err = bloberr.ErrUnexpected

	return
}

func (mm *mockBrokenData) Delete(ctx context.Context, shard *core.Shard) (err error) {
	err = bloberr.ErrUnexpected

	return
}

func (mm *mockBrokenData) Destroy(ctx context.Context) (err error) {
	err = bloberr.ErrUnexpected

	return
}

func (mm *mockBrokenData) Close() {
}

func TestReplStorage_Operations(t *testing.T) {
	stg := NewStorage(&mockmeta{
		id:   bnapi.ChunkId{0x1},
		bids: map[proto.BlobID]core.ShardMeta{},
	}, &mockdata{})
	require.NotNil(t, stg)

	stg1 := NewStorage(&mockmeta{
		id:   bnapi.ChunkId{0x1},
		bids: map[proto.BlobID]core.ShardMeta{},
	}, &mockdata{})
	require.NotNil(t, stg)

	stg2 := NewStorage(&mockBrokenMeta{}, &mockBrokenData{})

	repstg := NewReplicateStg(stg, stg1, nil)
	require.NotNil(t, repstg)

	// ===============================
	// ----- write -------
	ctx := context.TODO()
	// normal
	b1 := &core.Shard{
		Bid: 1,
	}
	err := repstg.Write(ctx, b1)
	require.NoError(t, err)

	// data error
	b2 := &core.Shard{
		Bid: 2,
	}
	err = repstg.Write(ctx, b2)
	require.Error(t, err)

	// meta error
	b3 := &core.Shard{
		Bid: 3,
	}
	err = repstg.Write(ctx, b3)
	require.Error(t, err)

	// ===================================
	var notifyCnt int
	notify := func(err error) {
		notifyCnt += 1
	}
	repstg1 := NewReplicateStg(stg, stg2, notify)
	require.NotNil(t, repstg1)
	err = repstg1.Write(ctx, b1)
	require.Error(t, err)
	require.Equal(t, 1, notifyCnt)
	err = repstg1.Write(ctx, b1)
	require.Error(t, err)
	require.Equal(t, 2, notifyCnt)

	err = repstg1.SyncData(ctx)
	require.Error(t, err)

	repstg2 := NewReplicateStg(stg2, stg1, notify)
	require.NotNil(t, repstg2)
	err = repstg2.Write(ctx, b1)
	require.Error(t, err)
	require.Equal(t, 4, notifyCnt)
}
