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
	"fmt"
	"io"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	cproto "github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/stretchr/testify/require"
)

func TestServerShardSM_Item(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	oldProtoItem := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{ID: 0, Value: []byte("string")},
			{ID: 1, Value: []byte{1}},
		},
	}
	sk := mockShard.shard.shardKeys
	oldkv, err := initKV(sk.encodeItemKey(oldProtoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(oldProtoItem), N: int64(oldProtoItem.Size())})
	require.NoError(t, err)
	oldProtoItemBytes := oldkv.Marshal()

	newProtoItem := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{ID: 0, Value: []byte("string")},
			{ID: 1, Value: []byte{2}},
		},
	}
	newkv, err := initKV(sk.encodeItemKey(oldProtoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(newProtoItem), N: int64(newProtoItem.Size())})
	require.NoError(t, err)
	newProtoItemBytes := newkv.Marshal()

	// Insert
	err = mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, oldProtoItem.ID, oldProtoItem)
	err = mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, oldProtoItem.ID, oldProtoItem)
	// Update
	notFoundItem := &proto.Item{ID: []byte{10}}
	notFoundKV, err := initKV(sk.encodeItemKey(oldProtoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(notFoundItem), N: int64(notFoundItem.Size())})
	require.NoError(t, err)
	notFoundItemBytes := notFoundKV.Marshal()
	err = mockShard.shardSM.applyUpdateItem(ctx, notFoundItemBytes)
	require.Nil(t, err)

	err = mockShard.shardSM.applyUpdateItem(ctx, newProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, newProtoItem.ID, newProtoItem)
	// Delete
	err = mockShard.shardSM.applyDeleteRaw(ctx, sk.encodeItemKey(newProtoItem.ID))
	require.Nil(t, err)
	_, err = mockShard.shard.GetItem(ctx, OpHeader{
		ShardKeys: [][]byte{newProtoItem.ID},
	}, newProtoItem.ID)
	require.ErrorIs(t, err, errors.ErrKeyNotFound)
	err = mockShard.shardSM.applyDeleteRaw(ctx, sk.encodeItemKey(newProtoItem.ID))
	require.Nil(t, err)
	_, err = mockShard.shard.GetItem(ctx, OpHeader{
		ShardKeys: [][]byte{newProtoItem.ID},
	}, newProtoItem.ID)
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	// List
	n := 4
	items := make([]*proto.Item, n)
	for i := 0; i < n; i++ {
		s := fmt.Sprint(i)
		protoItem := &proto.Item{
			ID: []byte(s),
			Fields: []proto.Field{
				{ID: 0, Value: []byte("string")},
				{ID: 1, Value: []byte(s)},
			},
		}
		kv, err := initKV(sk.encodeItemKey(protoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(protoItem), N: int64(protoItem.Size())})
		require.NoError(t, err)
		err = mockShard.shardSM.applyInsertItem(ctx, kv.Marshal())
		require.Nil(t, err)
		items[i] = protoItem
	}
	rets, marker, err := mockShard.shard.ListItem(ctx, OpHeader{
		ShardKeys: [][]byte{items[0].ID},
	}, nil, items[0].ID, uint64(n-1))
	require.Nil(t, err)
	require.Equal(t, items[n-1].ID, marker)

	for i := 0; i < n-1; i++ {
		require.Equal(t, items[i].ID, rets[i].ID)
	}

	_, marker, err = mockShard.shard.ListItem(ctx, OpHeader{
		ShardKeys: [][]byte{items[0].ID},
	}, nil, items[0].ID, uint64(n))
	require.Nil(t, err)
	require.Nil(t, marker)
}

func TestServerShardSM_Apply(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	i1 := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{ID: 1, Value: []byte("string")},
			{ID: 2, Value: []byte{1}},
		},
	}
	i2 := &proto.Item{
		ID: []byte{2},
		Fields: []proto.Field{
			{ID: 1, Value: []byte("string")},
			{ID: 2, Value: []byte{1}},
		},
	}
	i3 := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{ID: 1, Value: []byte("string1")},
			{ID: 2, Value: []byte{2}},
		},
	}
	i4 := &proto.Item{
		ID: []byte{100},
		Fields: []proto.Field{
			{ID: 1, Value: []byte("string1")},
			{ID: 2, Value: []byte{2}},
		},
	}

	ib1, _ := initKV(i1.ID, &io.LimitedReader{R: rpc2.Codec2Reader(i1), N: int64(i1.Size())})
	ib2, _ := initKV(i2.ID, &io.LimitedReader{R: rpc2.Codec2Reader(i2), N: int64(i2.Size())})
	ib3, _ := initKV(i3.ID, &io.LimitedReader{R: rpc2.Codec2Reader(i3), N: int64(i3.Size())})
	ib4, _ := initKV(i4.ID, &io.LimitedReader{R: rpc2.Codec2Reader(i4), N: int64(i4.Size())})

	db := i1.ID

	pds := []raft.ProposalData{
		{Op: raftOpInsertItem, Data: ib1.Marshal()},
		{Op: raftOpInsertItem, Data: ib2.Marshal()},
		{Op: raftOpUpdateItem, Data: ib3.Marshal()},
		{Op: raftOpDeleteItem, Data: db},
	}
	_, err := mockShard.shardSM.Apply(ctx, pds, 1)
	require.Nil(t, err)

	// key not found, return nil
	pds = []raft.ProposalData{
		{Op: raftOpUpdateBlob, Data: ib4.Marshal()},
	}
	_, err = mockShard.shardSM.Apply(ctx, pds, 1)
	require.Nil(t, err)

	// key already insert
	pds = []raft.ProposalData{
		{Op: raftOpUpdateBlob, Data: ib1.Marshal()},
	}
	_, err = mockShard.shardSM.Apply(ctx, pds, 1)
	require.Nil(t, err)

	require.Panics(t, func() {
		_, _ = mockShard.shardSM.Apply(ctx, []raft.ProposalData{{
			Op: 999,
		}}, 1)
	})
}

func TestServer_BlobList(t *testing.T) {
	ctx := context.Background()
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	err := mockShard.shard.SaveShardInfo(ctx, false, false)
	require.Nil(t, err)

	sk := mockShard.shard.shardKeys
	blobs := make([]cproto.Blob, 0)
	n := 4
	for i := 0; i < n; i++ {
		b := cproto.Blob{
			Name: []byte(fmt.Sprintf("blob%d", i)),
		}
		kv, _ := initKV(sk.encodeBlobKey(b.Name), &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
		mockShard.shardSM.applyInsertBlob(ctx, kv.Marshal())
		blobs = append(blobs, b)
	}

	// without prefix and marker
	retBlobs, mkr, err := mockShard.shard.ListBlob(ctx, OpHeader{}, nil, nil, uint64(n+1))
	require.Nil(t, err)
	require.Equal(t, n, len(retBlobs))
	for i := 0; i < n; i++ {
		require.Equal(t, blobs[i].Name, retBlobs[i].Name)
	}
	require.Nil(t, mkr)

	_, mkr, err = mockShard.shard.ListBlob(ctx, OpHeader{}, nil, nil, uint64(n-1))
	require.Nil(t, err)
	require.Equal(t, blobs[n-1].Name, mkr)

	// with prefix
	_, mkr, err = mockShard.shard.ListBlob(ctx, OpHeader{}, []byte("blob"), nil, uint64(n))
	require.Nil(t, err)
	require.Equal(t, n, len(retBlobs))
	for i := 0; i < n; i++ {
		require.Equal(t, blobs[i].Name, retBlobs[i].Name)
	}
	require.Nil(t, mkr)

	// with marker
	retBlobs, mkr, err = mockShard.shard.ListBlob(ctx, OpHeader{}, nil, []byte("blob3"), uint64(1))
	require.Nil(t, err)
	require.Nil(t, mkr)
	require.Equal(t, 1, len(retBlobs))
	require.Equal(t, blobs[n-1].Name, retBlobs[0].Name)
}

func TestServer_CreateBlob(t *testing.T) {
	ctx := context.Background()
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	b1 := cproto.Blob{Name: []byte("blob1")}
	kv, _ := initKV(b1.Name, &io.LimitedReader{R: rpc2.Codec2Reader(&b1), N: int64(b1.Size())})
	b11, err := mockShard.shardSM.applyInsertBlob(ctx, kv.Marshal())
	require.Nil(t, err)
	require.Equal(t, b1, b11)

	b1.Location.Size_ = 1024
	kv, _ = initKV(b1.Name, &io.LimitedReader{R: rpc2.Codec2Reader(&b1), N: int64(b1.Size())})
	b11, err = mockShard.shardSM.applyInsertBlob(ctx, kv.Marshal())
	require.Nil(t, err)
	require.NotEqual(t, b1, b11)
}

func TestServer_Snapshot(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	sk := mockShard.shard.shardKeys
	b1 := cproto.Blob{Name: []byte("blob1")}
	kv, _ := initKV(sk.encodeBlobKey(b1.Name), &io.LimitedReader{R: rpc2.Codec2Reader(&b1), N: int64(b1.Size())})
	_, err := mockShard.shardSM.applyInsertBlob(ctx, kv.Marshal())
	require.Nil(t, err)

	ss, err := mockShard.shardSM.Snapshot()
	require.Nil(t, err)

	members := make([]raft.Member, 3)
	for i := range members {
		mctx := proto.ShardMemberCtx{
			Suid: cproto.EncodeSuid(1, uint8(i), 1),
		}
		rawCtx, _ := mctx.Marshal()

		members[i] = raft.Member{
			NodeID:  1,
			Host:    "127.0.0.1",
			Type:    raft.MemberChangeType_AddMember,
			Learner: false,
			Context: rawCtx,
		}
	}

	err = mockShard.shardSM.ApplySnapshot(context.TODO(), raft.RaftSnapshotHeader{Members: members}, ss)
	require.Nil(t, err)

	// if shard is stop writing when processing snapshot, do not return err

	mockShard.shard.shardState.stopWriting()
	_, err = mockShard.shardSM.Snapshot()
	require.Nil(t, err)

	mockShard.shard.shardState.lock.Lock()
	mockShard.shard.shardState.status = shardStatusNormal
	mockShard.shard.shardState.lock.Unlock()
	ss, err = mockShard.shardSM.Snapshot()
	require.Nil(t, err)

	mockShard.shard.shardState.stopWriting()
	err = mockShard.shardSM.ApplySnapshot(context.TODO(), raft.RaftSnapshotHeader{Members: members}, ss)
	require.Nil(t, err)
}

func TestShardInfo(t *testing.T) {
	ctx := context.Background()
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	err := mockShard.shard.SaveShardInfo(ctx, false, true)
	require.Nil(t, err)

	_, err = mockShard.shard.getShardInfoFromPersistentTier(ctx)
	require.Nil(t, err)
}

func checkItemEqual(t *testing.T, shard *mockShard, id []byte, item *proto.Item) {
	ret, err := shard.shard.GetItem(ctx, OpHeader{
		ShardKeys: [][]byte{id},
	}, id)
	if err != nil {
		require.ErrorIs(t, err, kvstore.ErrNotFound)
		return
	}

	itm := proto.Item{
		ID:     ret.ID,
		Fields: protoFieldsToInternalFields(ret.Fields),
	}
	require.Equal(t, *item, itm)
}
