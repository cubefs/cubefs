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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/proto"
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
	oldProtoItemBytes, err := oldProtoItem.Marshal()
	require.NoError(t, err)

	newProtoItem := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{ID: 0, Value: []byte("string")},
			{ID: 1, Value: []byte{2}},
		},
	}
	newProtoItemBytes, err := newProtoItem.Marshal()
	require.NoError(t, err)

	// Insert
	err = mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, oldProtoItem.ID, oldProtoItem)
	err = mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, oldProtoItem.ID, oldProtoItem)
	// Update
	require.Error(t, mockShard.shardSM.applyUpdateItem(ctx, []byte("a")))
	notFoundItem := proto.Item{ID: []byte{10}}
	notFoundItemBytes, _ := notFoundItem.Marshal()
	err = mockShard.shardSM.applyUpdateItem(ctx, notFoundItemBytes)
	require.Nil(t, err)

	err = mockShard.shardSM.applyUpdateItem(ctx, newProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, newProtoItem.ID, newProtoItem)
	// Delete
	err = mockShard.shardSM.applyDeleteItem(ctx, newProtoItem.ID)
	require.Nil(t, err)
	_, err = mockShard.shard.GetItem(ctx, OpHeader{
		ShardKeys: [][]byte{newProtoItem.ID},
	}, newProtoItem.ID)
	require.ErrorIs(t, err, kvstore.ErrNotFound)
	err = mockShard.shardSM.applyDeleteItem(ctx, newProtoItem.ID)
	require.Nil(t, err)
	_, err = mockShard.shard.GetItem(ctx, OpHeader{
		ShardKeys: [][]byte{newProtoItem.ID},
	}, newProtoItem.ID)
	require.ErrorIs(t, err, kvstore.ErrNotFound)

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
		protoItemBytes, err := protoItem.Marshal()
		require.NoError(t, err)
		err = mockShard.shardSM.applyInsertItem(ctx, protoItemBytes)
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

	ib1, _ := i1.Marshal()
	ib2, _ := i2.Marshal()
	ib3, _ := i3.Marshal()

	db := i1.ID

	pds := []raft.ProposalData{
		{Op: RaftOpInsertItem, Data: ib1},
		{Op: RaftOpInsertItem, Data: ib2},
		{Op: RaftOpUpdateItem, Data: ib3},
		{Op: RaftOpDeleteItem, Data: db},
	}
	ret, err := mockShard.shardSM.Apply(ctx, pds, 1)
	require.Nil(t, err)
	require.Nil(t, ret[0])
	require.Nil(t, ret[1])
	require.Nil(t, ret[2])
	require.Nil(t, ret[3])

	require.Panics(t, func() {
		_, _ = mockShard.shardSM.Apply(ctx, []raft.ProposalData{{
			Op: 999,
		}}, 1)
	})
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
