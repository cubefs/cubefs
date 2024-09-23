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
	"context"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog/allocator"
	"github.com/cubefs/cubefs/blobstore/shardnode/mock"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type mockSpace struct {
	space         *Space
	shardErrSpace *Space
	mockHandler   *mock.MockSpaceShardHandler
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
	handler := mock.NewMockSpaceShardHandler(C(tb))

	sg := mock.NewMockShardGetter(C(tb))
	sg.EXPECT().GetShard(A, A).Return(handler, nil).AnyTimes()

	sg2 := mock.NewMockShardGetter(C(tb))
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
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Insert(A, A, A).Return(nil))
	// insert
	err := mockSpace.space.InsertItem(ctx, oph, shardnode.Item{Fields: fields})
	require.Nil(t, err)
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Insert(A, A, A).Return(errors.New("insert error")))
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
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Delete(A, A, A).Return(nil))
	err = mockSpace.space.DeleteItem(ctx, oph, []byte{1})
	require.Nil(t, err)
	err = mockSpace.shardErrSpace.DeleteItem(ctx, oph, []byte{1})
	require.Equal(t, apierr.ErrShardDoesNotExist, err)
}

type mockValGetter struct {
	value []byte
	io.Reader
}

func newMockValGetter(value []byte) storage.ValGetter {
	return &mockValGetter{value: value, Reader: bytes.NewReader(value)}
}

func (vg *mockValGetter) Value() []byte { return vg.value }

func (vg *mockValGetter) Size() int { return len(vg.value) }

func (vg *mockValGetter) Close() {}

func TestSpace_Blob(t *testing.T) {
	ctx := context.Background()
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()

	tp := allocator.NewMockAllocTransport(t)
	alc, err := allocator.NewAllocator(ctx, allocator.BlobConfig{}, allocator.VolConfig{}, tp)
	require.Nil(t, err)
	time.Sleep(100 * time.Millisecond)
	mockSpace.space.allocator = alc

	// insert
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Insert(A, A, A).Return(nil))

	name := []byte("blob")
	oph := shardnode.ShardOpHeader{}
	args := &shardnode.CreateBlobArgs{
		Header:    oph,
		Name:      name,
		CodeMode:  codemode.EC6P6,
		Size_:     1024 * 10,
		SliceSize: 64,
	}
	ret, err := mockSpace.space.CreateBlob(ctx, args)
	require.Nil(t, err)
	require.NotNil(t, ret.Blob.Location)

	blobBytes, err := ret.Blob.Marshal()
	require.Nil(t, err)

	// get
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(newMockValGetter(blobBytes), nil))

	ret1, err := mockSpace.space.GetBlob(ctx, &shardnode.GetBlobArgs{Header: oph, Name: name})
	require.Nil(t, err)
	require.Equal(t, ret.Blob, ret1.Blob)

	// seal
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(newMockValGetter(blobBytes), nil))
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Update(A, A, A).Return(nil))

	err = mockSpace.space.SealBlob(ctx, &shardnode.SealBlobArgs{Header: oph, Name: name})
	require.Nil(t, err)

	// delete
	gomock.InOrder(mockSpace.mockHandler.EXPECT().Delete(A, A, A).Return(nil))
	err = mockSpace.space.DeleteBlob(ctx, &shardnode.DeleteBlobArgs{Header: oph, Name: name})
	require.Nil(t, err)
}

func TestKey(t *testing.T) {
	key := []byte("aaa")
	space := Space{sid: 1000, spaceVersion: 1}

	spaceKey := space.generateSpaceKey(key)
	t.Log(spaceKey)

	_key := space.decodeSpaceKey(spaceKey)
	bytes.Equal(key, _key)
}

func TestBlob(t *testing.T) {
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	b := proto.Blob{
		Name: []byte("blob"),
		Location: proto.Location{
			ClusterID: 10000,
			CodeMode:  11,
			Size_:     10 * 1024,
			SliceSize: 128,
			Crc:       0,
			Slices: []proto.Slice{
				{
					MinSliceID: 10,
					Vid:        100,
					Count:      20,
				},
				{
					MinSliceID: 30,
					Vid:        100,
					Count:      10,
				},
			},
		},
	}
	kv, err := storage.InitKV(mockSpace.space.generateSpaceKey(b.GetName()), &io.LimitedReader{
		R: rpc2.Codec2Reader(&b),
		N: int64(b.Size()),
	})
	require.Nil(t, err)

	data := kv.Marshal()
	kv2 := storage.NewKV(data)
	require.Equal(t, mockSpace.space.generateSpaceKey(b.GetName()), kv2.Key())

	b2 := proto.Blob{}
	err = b2.Unmarshal(kv.Value())
	require.Nil(t, err)
	require.Equal(t, b, b2)
}
