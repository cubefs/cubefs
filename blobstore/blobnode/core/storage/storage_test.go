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
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type mockmeta struct {
	id            bnapi.ChunkId
	bids          map[proto.BlobID]core.ShardMeta
	supportInline bool
}

type mockdata struct {
	offset int64
}

var (
	_contents = [...]string{
		"",      // invalid bid
		"bid-1", // bid :1
		"bid-2", // bid :2
		"bid-3", // bid :3
		"bid-4", // bid :4
	}
	_testBidTestMetas = []struct {
		content string
		retErr  error
		meta    core.ShardMeta
	}{
		{_contents[0], bloberr.ErrInvalidParam, core.ShardMeta{}},
		{_contents[1], nil, core.ShardMeta{}},
		{_contents[2], nil, core.ShardMeta{}},
		{_contents[3], bloberr.ErrUnexpected, core.ShardMeta{}},
		{_contents[4], nil, core.ShardMeta{Flag: bnapi.ShardStatusMarkDelete}},
	}

	_testBidTestDatas = []struct {
		retErr error
	}{
		{nil},
		{nil},
		{bloberr.ErrUnexpected},
		{nil},
		{nil},
	}
)

func (mm *mockmeta) ID() bnapi.ChunkId {
	return mm.id
}

func (mm *mockmeta) InnerDB() db.MetaHandler {
	return nil
}

func (mm *mockmeta) Write(ctx context.Context, bid proto.BlobID, value core.ShardMeta) (err error) {
	index := int64(bid)
	err = _testBidTestMetas[index].retErr

	return
}

func (mm *mockmeta) Read(ctx context.Context, bid proto.BlobID) (value core.ShardMeta, err error) {
	index := int64(bid)

	value = _testBidTestMetas[index].meta
	err = _testBidTestMetas[index].retErr

	return
}

func (mm *mockmeta) Delete(ctx context.Context, bid proto.BlobID) (err error) {
	return
}

func (mm *mockmeta) Scan(ctx context.Context, startBid proto.BlobID, limit int,
	fn func(bid proto.BlobID, sm *core.ShardMeta) error) (err error) {
	return
}

func (cm *mockmeta) SupportInline() bool {
	return cm.supportInline
}

func (mm *mockmeta) Destroy(ctx context.Context) (err error) {
	return
}

func (mm *mockmeta) Close() {
}

func (mm *mockdata) Write(ctx context.Context, shard *core.Shard) error {
	index := int(shard.Bid)

	mm.offset += int64(shard.Size)

	return _testBidTestDatas[index].retErr
}

func (mm *mockdata) Read(ctx context.Context, shard *core.Shard, from, to uint32) (r io.Reader, err error) {
	r = shard.Body
	return
}

func (mm *mockdata) Stat() (stat *core.StorageStat, err error) {
	return
}

func (mm *mockdata) Flush() (err error) {
	return
}

func (mm *mockdata) Delete(ctx context.Context, shard *core.Shard) (err error) {
	return
}

func (mm *mockdata) Destroy(ctx context.Context) (err error) {
	return
}

func (mm *mockdata) Close() {
}

func TestNewStorage(t *testing.T) {
	stg := NewStorage(nil, nil)
	require.NotNil(t, stg)
}

func TestStorage_Operations(t *testing.T) {
	stg := NewStorage(&mockmeta{
		id:   bnapi.ChunkId{0x1},
		bids: map[proto.BlobID]core.ShardMeta{},
	}, &mockdata{})
	require.NotNil(t, stg)

	require.Equal(t, 0, int(stg.PendingRequest()))
	stg.IncrPendingCnt()
	require.Equal(t, 1, int(stg.PendingRequest()))
	stg.DecrPendingCnt()
	require.Equal(t, 0, int(stg.PendingRequest()))

	require.Equal(t, bnapi.ChunkId{0x1}, stg.ID())

	require.NotNil(t, stg.MetaHandler())
	require.NotNil(t, stg.DataHandler())

	// ----- write -------
	ctx := context.TODO()
	// normal
	b1 := &core.Shard{
		Bid: 1,
	}
	err := stg.Write(ctx, b1)
	require.NoError(t, err)

	// data error
	b2 := &core.Shard{
		Bid: 2,
	}
	err = stg.Write(ctx, b2)
	require.Error(t, err)

	// meta error
	b3 := &core.Shard{
		Bid: 3,
	}
	err = stg.Write(ctx, b3)
	require.Error(t, err)

	// ------------- read ---------------
	mockBody := []byte("test")
	memBuffer := &bytes.Buffer{}
	b := &core.Shard{
		Bid:    1,
		Body:   bytes.NewReader(mockBody),
		From:   0,
		To:     int64(len(mockBody)),
		Writer: memBuffer,
	}

	// ----------- read meta ------------
	sm, err := stg.ReadShardMeta(ctx, b1.Bid)
	require.NotNil(t, sm)
	require.NoError(t, err)

	// ----------- reader ----------------
	b.Body = bytes.NewReader(mockBody)
	rc, err := stg.NewRangeReader(ctx, b, 0, int64(len(mockBody)))
	require.NoError(t, err)
	buffer, err := ioutil.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, mockBody, buffer)

	// ---------- mark delete -----------------------
	err = stg.MarkDelete(ctx, b1.Bid)
	require.NoError(t, err)

	// --------------- delete -------------------
	b4 := &core.Shard{
		Bid: 4,
	}
	_, err = stg.Delete(ctx, b4.Bid)
	require.NoError(t, err)
}

func TestStorage_ShardInline(t *testing.T) {
	stg := NewStorage(&mockmeta{
		id:            bnapi.ChunkId{0x1},
		bids:          map[proto.BlobID]core.ShardMeta{},
		supportInline: true,
	}, &mockdata{})
	require.NotNil(t, stg)

	stg = NewTinyFileStg(stg, 8)
	require.NotNil(t, stg)

	require.Equal(t, bnapi.ChunkId{0x1}, stg.ID())

	require.NotNil(t, stg.MetaHandler())
	require.NotNil(t, stg.DataHandler())

	// ----- write -------
	ctx := context.TODO()
	// normal
	b1 := &core.Shard{
		Bid:  1,
		Body: bytes.NewReader([]byte("test")),
	}
	err := stg.Write(ctx, b1)
	require.NoError(t, err)

	// data error
	b2 := &core.Shard{
		Bid: 2,
	}
	err = stg.Write(ctx, b2)
	// data inline, no error
	require.NoError(t, err)

	// meta error
	b3 := &core.Shard{
		Bid: 3,
	}
	err = stg.Write(ctx, b3)
	require.Error(t, err)
}
