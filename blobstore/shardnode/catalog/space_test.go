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

package catalog

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

//go:generate mockgen -source=./catalog.go -destination=./mock_catalog.go -package=catalog -mock_names ShardGetter=MockShardGetter
//go:generate mockgen -source=../storage/shard.go -destination=../storage/mock_shard.go -package=storage -mock_names ShardHandler=MockSpaceShardHandler

type mockSpace struct {
	space         *Space
	shardErrSpace *Space
	mockHandler   *storage.MockSpaceShardHandler
}

func newMockSpace(tb testing.TB) (*mockSpace, func()) {
	fixedFields := make(map[proto.FieldID]clustermgr.FieldMeta)
	fixedFields[1] = clustermgr.FieldMeta{
		Name:        "f1",
		FieldType:   proto.FieldTypeString,
		IndexOption: proto.IndexOptionNull,
	}
	fixedFields[2] = clustermgr.FieldMeta{
		Name:        "f2",
		FieldType:   proto.FieldTypeString,
		IndexOption: proto.IndexOptionNull,
	}
	handler := storage.NewMockSpaceShardHandler(C(tb))

	sg := NewMockShardGetter(C(tb))
	sg.EXPECT().GetShard(A, A).Return(handler, nil).AnyTimes()

	sg2 := NewMockShardGetter(C(tb))
	sg2.EXPECT().GetShard(A, A).Return(nil, apierr.ErrShardDoesNotExist).AnyTimes()

	space := &Space{
		sid:         1,
		name:        "space1",
		fieldMetas:  fixedFields,
		shardGetter: sg,
	}
	shardErrSpace := &Space{
		sid:         1,
		name:        "space1",
		fieldMetas:  fixedFields,
		shardGetter: sg2,
	}
	return &mockSpace{space: space, shardErrSpace: shardErrSpace, mockHandler: handler}, func() {
	}
}

func TestSpace_Item(t *testing.T) {
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	fields := []shardnode.Field{
		{ID: 1, Value: []byte("f1")},
		{ID: 2, Value: []byte("f2")},
	}
	oph := shardnode.ShardOpHeader{}
	// insert
	gomock.InOrder(mockSpace.mockHandler.EXPECT().InsertItem(A, A, A).Return(nil))
	err := mockSpace.space.InsertItem(ctx, oph, shardnode.Item{Fields: fields})
	require.Nil(t, err)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().InsertItem(A, A, A).Return(errors.New("insert error")))
	err = mockSpace.space.InsertItem(ctx, oph, shardnode.Item{Fields: fields})
	require.Equal(t, errors.New("insert error"), err)
	err = mockSpace.space.InsertItem(ctx, oph, shardnode.Item{Fields: []shardnode.Field{
		{ID: 3, Value: []byte("string")},
	}})
	require.Equal(t, apierr.ErrUnknownField, err)
	err = mockSpace.shardErrSpace.InsertItem(ctx, oph, shardnode.Item{})
	require.Equal(t, apierr.ErrShardDoesNotExist, err)
	// get
	gomock.InOrder(mockSpace.mockHandler.EXPECT().GetItem(A, A, A).Return(shardnode.Item{Fields: fields}, nil))
	ret, err := mockSpace.space.GetItem(ctx, oph, []byte{1})
	require.Nil(t, err)
	require.Equal(t, shardnode.Item{Fields: fields}, ret)
	_, err = mockSpace.shardErrSpace.GetItem(ctx, oph, []byte{99})
	require.Equal(t, apierr.ErrShardDoesNotExist, err)
	// update
	gomock.InOrder(mockSpace.mockHandler.EXPECT().UpdateItem(A, A, A).Return(nil))
	err = mockSpace.space.UpdateItem(ctx, oph, shardnode.Item{Fields: fields})
	require.Nil(t, err)
	err = mockSpace.space.UpdateItem(ctx, oph, shardnode.Item{Fields: []shardnode.Field{
		{ID: 3, Value: []byte("string")},
	}})
	require.Equal(t, apierr.ErrUnknownField, err)
	err = mockSpace.shardErrSpace.UpdateItem(ctx, oph, shardnode.Item{})
	require.Equal(t, apierr.ErrShardDoesNotExist, err)
	// delete
	gomock.InOrder(mockSpace.mockHandler.EXPECT().DeleteItem(A, A, A).Return(nil))
	err = mockSpace.space.DeleteItem(ctx, oph, []byte{1})
	require.Nil(t, err)
	err = mockSpace.shardErrSpace.DeleteItem(ctx, oph, []byte{1})
	require.Equal(t, apierr.ErrShardDoesNotExist, err)
}

func TestKey(t *testing.T) {
	key := []byte("aaa")
	space := Space{sid: 1000, spaceVersion: 1}

	spaceKey := space.generateItemKey(key)
	t.Log(spaceKey)

	_key := space.decodeItemKey(spaceKey)
	bytes.Equal(key, _key)
}
