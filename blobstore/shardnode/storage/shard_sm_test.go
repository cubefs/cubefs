package storage

import (
	"encoding/binary"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestServerShardSM_Item(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	oldEmbedding := proto.Embedding{Elements: []float32{0.11, 0.22}, Source: "source"}
	oldEmbeddingBytes, _ := oldEmbedding.Marshal()
	oldProtoItem := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
			{Name: "f2", Value: oldEmbeddingBytes},
		},
	}
	oldProposeItem, err := mockShard.shard.generateProposeItem(ctx, *oldProtoItem)
	require.NoError(t, err)
	oldProtoItemBytes := oldProposeItem.raw

	newEmbedding := proto.Embedding{
		Elements: []float32{0.111, 0.222},
		Source:   "source1",
	}
	newEmbeddingBytes, _ := newEmbedding.Marshal()
	newProtoItem := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string1")},
			{Name: "f2", Value: newEmbeddingBytes},
		},
	}
	newProposeItem, err := mockShard.shard.generateProposeItem(ctx, *newProtoItem)
	require.NoError(t, err)
	newProtoItemBytes := newProposeItem.raw
	_delete := func() {
		_ = mockShard.shardSM.applyDeleteItem(ctx, []byte{'1'})
	}
	// Insert
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Delete(A).Return(nil).AnyTimes())
	applyInsertItemPanic := func() { mockShard.shardSM.applyInsertItem(ctx, []byte("a")) }
	require.Panics(t, applyInsertItemPanic)
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().ValidateBeforeInsert(A).Return(errors.New("vectorIndex validate error")).Times(1))
	require.Error(t, mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes))
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().ValidateBeforeInsert(A).Return(nil).AnyTimes())
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Add(A, A).Return(errors.New("vectorIndex add error")).Times(1))
	require.Error(t, mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes))
	_delete()
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Add(A, A).Return(nil))
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
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Add(A, A).Return(errors.New("vectorIndex add error")).Times(1))
	require.Error(t, mockShard.shardSM.applyUpdateItem(ctx, newProtoItemBytes))
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Add(A, A).Return(nil))
	err = mockShard.shardSM.applyUpdateItem(ctx, newProtoItemBytes)
	require.Nil(t, err)
	checkItemEqual(t, mockShard, newProtoItem.ID, newProtoItem)
	// Delete
	err = mockShard.shardSM.applyDeleteItem(ctx, newProtoItem.ID)
	require.Nil(t, err)
	_, err = mockShard.shard.GetItem(ctx, ShardOpHeader{
		ShardKeys: [][]byte{newProtoItem.ID},
	}, newProtoItem.ID)
	require.ErrorIs(t, err, kvstore.ErrNotFound)
	err = mockShard.shardSM.applyDeleteItem(ctx, newProtoItem.ID)
	require.Nil(t, err)
	_, err = mockShard.shard.GetItem(ctx, ShardOpHeader{
		ShardKeys: [][]byte{newProtoItem.ID},
	}, newProtoItem.ID)
	require.ErrorIs(t, err, kvstore.ErrNotFound)
}

func TestServerShardSM_Link(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	fields := []proto.Field{
		{Name: "f1", Value: []byte("string")},
	}
	l1 := &proto.Link{Parent: []byte{1}, Name: []byte("link1"), Fields: fields}
	l1Byte, _ := l1.Marshal()
	l2 := &proto.Link{Parent: []byte{5}, Name: []byte("link2"), Fields: fields}
	l2Byte, _ := l2.Marshal()
	item1 := &proto.Item{
		ID:     []byte{1},
		Fields: fields,
	}
	item2 := &proto.Item{
		ID:     []byte{2},
		Fields: fields,
	}
	pi1, err := mockShard.shard.generateProposeItem(ctx, *item1)
	require.NoError(t, err)
	b1 := pi1.raw
	pi2, err := mockShard.shard.generateProposeItem(ctx, *item2)
	require.NoError(t, err)
	b2 := pi2.raw
	_ = mockShard.shardSM.applyInsertItem(ctx, b1)
	_ = mockShard.shardSM.applyInsertItem(ctx, b2)
	// link
	err = mockShard.shardSM.applyLink(ctx, l1Byte)
	require.Nil(t, err)
	checkLinkEqual(t, mockShard, proto.GetLink{Parent: l1.Parent, Name: l1.Name}, l1)
	require.Error(t, mockShard.shardSM.applyLink(ctx, []byte("a")))
	// unlink
	err = mockShard.shardSM.applyUnlink(ctx, l1Byte)
	require.Nil(t, err)
	_, err = mockShard.shard.GetLink(ctx, ShardOpHeader{ShardKeys: [][]byte{l1.Parent}},
		proto.GetLink{Parent: l1.Parent, Name: l1.Name})
	require.ErrorIs(t, err, kvstore.ErrNotFound)
	err = mockShard.shardSM.applyUnlink(ctx, l2Byte)
	require.Nil(t, err)
	require.Error(t, mockShard.shardSM.applyUnlink(ctx, []byte("a")))
}

func TestServerShardSM_Apply(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	e1 := proto.Embedding{Elements: []float32{0.11, 0.22}, Source: "source"}
	eb1, _ := e1.Marshal()
	e2 := proto.Embedding{Elements: []float32{0.111, 0.222}, Source: "source"}
	eb2, _ := e2.Marshal()

	i1 := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
			{Name: "f2", Value: eb1},
		},
	}
	i2 := &proto.Item{
		ID: []byte{2},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
			{Name: "f2", Value: eb1},
		},
	}
	i3 := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string1")},
			{Name: "f2", Value: eb2},
		},
	}
	pi1, _ := mockShard.shard.generateProposeItem(ctx, *i1)
	ib1 := pi1.raw
	pi2, _ := mockShard.shard.generateProposeItem(ctx, *i2)
	ib2 := pi2.raw
	pi3, _ := mockShard.shard.generateProposeItem(ctx, *i3)
	ib3 := pi3.raw

	l1 := &proto.Link{
		Parent: i1.ID,
		Name:   []byte("link1"),
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
		},
	}
	lb1, _ := l1.Marshal()
	ul := &proto.Unlink{Parent: i1.ID, Name: []byte("link1")}
	ulb, _ := ul.Marshal()

	db := i1.ID
	r1 := make([]byte, 16)
	binary.BigEndian.PutUint64(r1, 1)
	binary.BigEndian.PutUint64(r1[8:], 100)

	pds := []raft.ProposalData{
		{Op: RaftOpInsertItem, Data: ib1},
		{Op: RaftOpInsertItem, Data: ib2},
		{Op: RaftOpUpdateItem, Data: ib3},
		{Op: RaftOpLinkItem, Data: lb1},
		{Op: RaftOpUnlinkItem, Data: ulb},
		{Op: RaftOpDeleteItem, Data: db},
		{Op: RaftOpAllocInoRange, Data: r1},
	}
	mockShard.mockVectorIndex.EXPECT().ValidateBeforeInsert(A).Return(nil).AnyTimes()
	mockShard.mockVectorIndex.EXPECT().Add(A, A).Return(nil).AnyTimes()
	mockShard.mockVectorIndex.EXPECT().Delete(A).Return(nil).AnyTimes()
	ret, err := mockShard.shardSM.Apply(ctx, pds, 1)
	require.Nil(t, err)
	require.Nil(t, ret[0])
	require.Nil(t, ret[1])
	require.Nil(t, ret[2])
	require.Nil(t, ret[3])
	require.Nil(t, ret[4])
	require.Nil(t, ret[5])
	require.Equal(t, autoIDRange{}, ret[6])

	ret, err = mockShard.shardSM.Apply(ctx, []raft.ProposalData{{
		Op:   RaftOpAllocInoRange,
		Data: r1,
	}}, 2)
	require.Equal(t, autoIDRange{
		start: 1,
		end:   101,
	}, ret[0])
	require.Nil(t, err)
	require.Panics(t, func() {
		_, _ = mockShard.shardSM.Apply(ctx, []raft.ProposalData{{
			Op: 999,
		}}, 1)
	})
}

func checkItemEqual(t *testing.T, shard *mockShard, id []byte, item *proto.Item) {
	ret, err := shard.shard.GetItem(ctx, ShardOpHeader{
		ShardKeys: [][]byte{id},
	}, id)
	if err != nil {
		require.ErrorIs(t, err, kvstore.ErrNotFound)
		return
	}
	require.Equal(t, *item, ret)
}

func checkLinkEqual(t *testing.T, shard *mockShard, getLink proto.GetLink, link *proto.Link) {
	ret, err := shard.shard.GetLink(ctx, ShardOpHeader{
		ShardKeys: [][]byte{link.Parent},
	}, getLink)
	if err != nil {
		require.ErrorIs(t, err, kvstore.ErrNotFound)
		return
	}
	require.Equal(t, *link, ret)
}
