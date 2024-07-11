package catalog

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/apierr"
	"github.com/cubefs/inodedb/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -source=shard.go -destination=./mock_shard_test.go -package=catalog -mock_names spaceShardHandler=MockSpaceShardHandler

type mockSpace struct {
	space         *Space
	shardErrSpace *Space
	mockHandler   *MockSpaceShardHandler
}

func newMockSpace(tb testing.TB) (*mockSpace, func()) {
	fixedFields := make(map[string]proto.FieldMeta)
	fixedFields["f1"] = proto.FieldMeta{
		Name:    "f1",
		Type:    proto.FieldMeta_String,
		Indexed: proto.FieldMeta_Null,
	}
	fixedFields["f2"] = proto.FieldMeta{
		Name:    "f2",
		Type:    proto.FieldMeta_String,
		Indexed: proto.FieldMeta_Null,
	}
	handler := NewMockSpaceShardHandler(C(tb))
	space := &Space{
		sid:        1,
		name:       "space1",
		fieldMetas: fixedFields,
		catalogHandler: func(diskID proto.DiskID, shardID proto.ShardID) (shardConfigGetter, error) {
			return handler, nil
		},
	}
	shardErrSpace := &Space{
		sid:        1,
		name:       "space1",
		fieldMetas: fixedFields,
		catalogHandler: func(diskID proto.DiskID, shardID proto.ShardID) (shardConfigGetter, error) {
			return nil, apierr.ErrShardNotExist
		},
	}
	return &mockSpace{space: space, shardErrSpace: shardErrSpace, mockHandler: handler}, func() {
	}
}

func TestSpace_Item(t *testing.T) {
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	fields := []proto.Field{
		{Name: "f1", Value: []byte("f1")},
		{Name: "f2", Value: []byte("f2")},
	}
	// insert
	gomock.InOrder(mockSpace.mockHandler.EXPECT().InsertItem(A, A, A).Return(nil))
	err := mockSpace.space.InsertItem(ctx, proto.ShardOpHeader{}, proto.Item{Fields: fields})
	require.Nil(t, err)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().InsertItem(A, A, A).Return(uint64(0), errors.New("insert error")))
	err = mockSpace.space.InsertItem(ctx, proto.ShardOpHeader{}, proto.Item{Fields: fields})
	require.Equal(t, errors.New("insert error"), err)
	err = mockSpace.space.InsertItem(ctx, proto.ShardOpHeader{}, proto.Item{Fields: []proto.Field{
		{Name: "f3", Value: []byte("string")},
	}})
	require.Equal(t, apierr.ErrUnknownField, err)
	err = mockSpace.shardErrSpace.InsertItem(ctx, proto.ShardOpHeader{}, proto.Item{})
	require.Equal(t, apierr.ErrShardNotExist, err)
	// get
	gomock.InOrder(mockSpace.mockHandler.EXPECT().GetItem(A, A, A).Return(proto.Item{Fields: fields}, nil))
	ret, err := mockSpace.space.GetItem(ctx, proto.ShardOpHeader{}, []byte{1})
	require.Nil(t, err)
	require.Equal(t, proto.Item{Fields: fields}, ret)
	_, err = mockSpace.shardErrSpace.GetItem(ctx, proto.ShardOpHeader{}, []byte{99})
	require.Equal(t, apierr.ErrShardNotExist, err)
	// update
	gomock.InOrder(mockSpace.mockHandler.EXPECT().UpdateItem(A, A, A).Return(nil))
	err = mockSpace.space.UpdateItem(ctx, proto.ShardOpHeader{}, proto.Item{Fields: fields})
	require.Nil(t, err)
	err = mockSpace.space.UpdateItem(ctx, proto.ShardOpHeader{}, proto.Item{Fields: []proto.Field{
		{Name: "f3", Value: []byte("string")},
	}})
	require.Equal(t, apierr.ErrUnknownField, err)
	err = mockSpace.shardErrSpace.UpdateItem(ctx, proto.ShardOpHeader{}, proto.Item{})
	require.Equal(t, apierr.ErrShardNotExist, err)
	// delete
	gomock.InOrder(mockSpace.mockHandler.EXPECT().DeleteItem(A, A, A).Return(nil))
	err = mockSpace.space.DeleteItem(ctx, proto.ShardOpHeader{}, []byte{1})
	require.Nil(t, err)
	err = mockSpace.shardErrSpace.DeleteItem(ctx, proto.ShardOpHeader{}, []byte{1})
	require.Equal(t, apierr.ErrShardNotExist, err)
}

func TestSpace_Link(t *testing.T) {
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	fields := []proto.Field{
		{Name: "f1", Value: []byte("string")},
	}
	l := proto.Link{Parent: []byte{1}, Name: []byte("link1"), Fields: fields}

	// link
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Link(A, A, A).Return(nil))
	err := mockSpace.space.Link(ctx, proto.ShardOpHeader{}, l)
	require.Nil(t, err)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Link(A, A, A).Return(errors.New("link error")))
	err = mockSpace.space.Link(ctx, proto.ShardOpHeader{}, l)
	require.Equal(t, errors.New("link error"), err)
	err = mockSpace.shardErrSpace.Link(ctx, proto.ShardOpHeader{}, l)
	require.ErrorIs(t, apierr.ErrShardNotExist, err)
	// get link
	gomock.InOrder(mockSpace.mockHandler.EXPECT().GetLink(A, A, A).Return(l, nil))
	ret, err := mockSpace.space.GetLink(ctx, proto.ShardOpHeader{}, proto.GetLink{
		Parent: l.Parent,
		Name:   l.Name,
	})
	require.Nil(t, err)
	require.Equal(t, l, ret)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().GetLink(A, A, A).Return(proto.Link{}, errors.New("get link error")))
	_, err = mockSpace.space.GetLink(ctx, proto.ShardOpHeader{}, proto.GetLink{
		Parent: l.Parent,
		Name:   l.Name,
	})
	require.Equal(t, errors.New("get link error"), err)
	_, err = mockSpace.shardErrSpace.GetLink(ctx, proto.ShardOpHeader{}, proto.GetLink{
		Parent: l.Parent,
		Name:   l.Name,
	})
	require.ErrorIs(t, apierr.ErrShardNotExist, err)
	// list
	gomock.InOrder(mockSpace.mockHandler.EXPECT().ListLink(A, A, A, A, A).Return([]proto.Link{l}, nil))
	links, err := mockSpace.space.List(ctx, proto.ShardOpHeader{}, l.Parent, nil, 10)
	require.Nil(t, err)
	require.Equal(t, []proto.Link{l}, links)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().ListLink(A, A, A, A, A).Return([]proto.Link{l}, errors.New("list error")))
	_, err = mockSpace.space.List(ctx, proto.ShardOpHeader{}, l.Parent, nil, 10)
	require.Equal(t, errors.New("list error"), err)
	_, err = mockSpace.shardErrSpace.List(ctx, proto.ShardOpHeader{}, l.Parent, nil, 10)
	require.ErrorIs(t, apierr.ErrShardNotExist, err)
	// unlink
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Unlink(A, A, A).Return(nil))
	err = mockSpace.space.Unlink(ctx, proto.ShardOpHeader{}, proto.Unlink{
		Parent: l.Parent,
		Name:   l.Name,
	})
	require.Nil(t, err)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Unlink(A, A, A).Return(errors.New("unlink error")))
	err = mockSpace.space.Unlink(ctx, proto.ShardOpHeader{}, proto.Unlink{
		Parent: l.Parent,
		Name:   l.Name,
	})
	require.Equal(t, errors.New("unlink error"), err)
	err = mockSpace.shardErrSpace.Unlink(ctx, proto.ShardOpHeader{}, proto.Unlink{
		Parent: l.Parent,
		Name:   l.Name,
	})
	require.ErrorIs(t, apierr.ErrShardNotExist, err)
}
