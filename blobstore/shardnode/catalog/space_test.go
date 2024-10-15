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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/security"
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

func TestSpace_CreateBlob(t *testing.T) {
	ctx := context.Background()
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	space := mockSpace.space

	alc := mock.NewMockAllocator(C(t))
	space.allocator = alc

	slices := []proto.Slice{
		{
			MinSliceID: proto.BlobID(1),
			Vid:        proto.Vid(100),
			Count:      160,
			ValidSize:  uint64(10240),
		},
	}

	name := []byte("blob")
	oph := shardnode.ShardOpHeader{}
	args := &shardnode.CreateBlobArgs{
		Header:    oph,
		Name:      name,
		CodeMode:  codemode.EC6P6,
		Size_:     1024 * 10,
		SliceSize: 64,
	}

	gomock.InOrder(alc.EXPECT().AllocSlices(A, A, A, A).Return(slices, nil))

	mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(nil, kvstore.ErrNotFound)
	mockSpace.mockHandler.EXPECT().Insert(A, A, A).Return(nil)

	ret, err := mockSpace.space.CreateBlob(ctx, args)
	require.Nil(t, err)
	b := ret.Blob
	require.NotNil(t, b.Location.Slices)
	require.True(t, security.LocationCrcVerify(&b.Location))

	// repeat create
	raw, err := b.Marshal()
	require.Nil(t, err)
	mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(newMockValGetter(raw), nil)

	ret, err = mockSpace.space.CreateBlob(ctx, args)
	require.Equal(t, apierr.ErrBlobAlreadyExists, err)

	// create with alloc size 0
	args.Size_ = 0

	mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(nil, kvstore.ErrNotFound)
	mockSpace.mockHandler.EXPECT().Insert(A, A, A).Return(nil)
	ret, err = mockSpace.space.CreateBlob(ctx, args)
	require.Nil(t, err)
	b = ret.Blob
	require.Nil(t, b.Location.Slices)
}

func TestSpace_AllocSlice(t *testing.T) {
	ctx := context.Background()
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	space := mockSpace.space

	alc := mock.NewMockAllocator(C(t))
	space.allocator = alc

	locSlices := []proto.Slice{
		{MinSliceID: 1, Count: 10, ValidSize: 100},
		{MinSliceID: 2, Count: 20, ValidSize: 200},
		{MinSliceID: 3, Count: 30, ValidSize: 300},
	}
	name := []byte("blob")
	mode := codemode.EC6P6
	b := proto.Blob{
		Name: name,
		Location: proto.Location{
			CodeMode:  mode,
			Slices:    locSlices,
			SliceSize: 10,
		},
	}
	raw, err := b.Marshal()
	require.Nil(t, err)
	mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(newMockValGetter(raw), nil).Times(3)

	newSlices := []proto.Slice{
		{MinSliceID: 4, Count: 10, ValidSize: 100},
	}
	alc.EXPECT().AllocSlices(A, A, A, A).Return(newSlices, nil).Times(2)
	mockSpace.mockHandler.EXPECT().Update(A, A, A).Return(nil).Times(2)
	args := &shardnode.AllocSliceArgs{
		Header:      shardnode.ShardOpHeader{},
		Name:        name,
		Size_:       64,
		FailedSlice: proto.Slice{MinSliceID: 2, Count: 10, ValidSize: 100},
	}
	ret, err := space.AllocSlice(ctx, args)
	require.Nil(t, err)
	require.Equal(t, newSlices, ret.Slices)

	args.FailedSlice.Count = 0
	args.FailedSlice.ValidSize = 0
	ret, err = space.AllocSlice(ctx, args)
	require.Nil(t, err)
	require.Equal(t, newSlices, ret.Slices)

	// alloc failed
	alc.EXPECT().AllocSlices(A, A, A, A).Return(nil, apierr.ErrNoAvaliableVolume)
	ret, err = space.AllocSlice(ctx, args)
	require.Equal(t, apierr.ErrNoAvaliableVolume, errors.Cause(err))
}

func TestSpace_SealBlob(t *testing.T) {
	ctx := context.Background()
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	space := mockSpace.space

	alc := mock.NewMockAllocator(C(t))
	space.allocator = alc

	locSlices := []proto.Slice{
		{MinSliceID: 1, Count: 10, ValidSize: 100},
		{MinSliceID: 2, Count: 20, ValidSize: 200},
		{MinSliceID: 3, Count: 30, ValidSize: 300},
	}
	name := []byte("blob")
	mode := codemode.EC6P6
	b := proto.Blob{
		Name: name,
		Location: proto.Location{
			CodeMode:  mode,
			Slices:    locSlices,
			SliceSize: 10,
		},
	}
	raw, err := b.Marshal()
	require.Nil(t, err)
	mockSpace.mockHandler.EXPECT().Get(A, A, A).Return(newMockValGetter(raw), nil).Times(2)
	mockSpace.mockHandler.EXPECT().Update(A, A, A).Return(nil).Times(1)

	args := &shardnode.SealBlobArgs{
		Header: shardnode.ShardOpHeader{},
		Name:   name,
		Size_:  600,
		Slices: locSlices,
	}
	err = space.SealBlob(ctx, args)
	require.Nil(t, err)

	args.Slices[1].ValidSize = 300
	err = space.SealBlob(ctx, args)
	require.Equal(t, apierr.ErrIllegalSlices, err)
}

func TestSpace_DeleteBlob(t *testing.T) {
	ctx := context.Background()
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	space := mockSpace.space

	mockSpace.mockHandler.EXPECT().Delete(A, A, A).Return(nil)

	err := space.DeleteBlob(ctx, &shardnode.DeleteBlobArgs{
		Header: shardnode.ShardOpHeader{},
		Name:   []byte("blob"),
	})
	require.Nil(t, err)
}

func TestSpace_ListBlob(t *testing.T) {
	ctx := context.Background()
	mockSpace, cleanSpace := newMockSpace(t)
	defer cleanSpace()
	space := mockSpace.space

	nextMarker := []byte("next")
	mockSpace.mockHandler.EXPECT().List(A, A, A, A, A, A).Return(space.generateSpaceKey(nextMarker), nil)
	_, m, err := space.ListBlob(ctx, shardnode.ShardOpHeader{}, nil, nil, 10)
	require.Nil(t, err)
	require.Equal(t, nextMarker, m)
}

func Test_SpaceKey(t *testing.T) {
	space := Space{sid: 1000, spaceVersion: 1}

	key := []byte("blob1")

	spaceKey := space.generateSpaceKey(key)
	_key := space.decodeSpaceKey(spaceKey)
	require.True(t, bytes.Equal(key, _key))

	_prefix := space.generateSpacePrefix(nil)
	require.True(t, bytes.Contains(spaceKey, _prefix))

	_prefix = space.generateSpacePrefix([]byte("b"))
	require.True(t, bytes.Contains(spaceKey, _prefix))
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

func TestCheckSlices(t *testing.T) {
	type args struct {
		loc       []proto.Slice
		req       []proto.Slice
		sliceSize uint32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "valid slices",
			args: args{
				loc: []proto.Slice{
					{MinSliceID: 1, Count: 10, ValidSize: 100},
					{MinSliceID: 2, Count: 20, ValidSize: 200},
				},
				req: []proto.Slice{
					{MinSliceID: 1, Count: 5, ValidSize: 50},
					{MinSliceID: 2, Count: 10, ValidSize: 100},
				},
				sliceSize: 10,
			},
			want: true,
		},
		{
			name: "invalid slices - missing MinSliceID",
			args: args{
				loc: []proto.Slice{
					{MinSliceID: 1, Count: 10, ValidSize: 100},
					{MinSliceID: 2, Count: 20, ValidSize: 200},
				},
				req: []proto.Slice{
					{MinSliceID: 3, Count: 5, ValidSize: 50},
				},
				sliceSize: 10,
			},
			want: false,
		},
		{
			name: "invalid slices - count exceeds",
			args: args{
				loc: []proto.Slice{
					{MinSliceID: 1, Count: 10, ValidSize: 100},
					{MinSliceID: 2, Count: 20, ValidSize: 200},
				},
				req: []proto.Slice{
					{MinSliceID: 1, Count: 15, ValidSize: 150},
				},
				sliceSize: 10,
			},
			want: false,
		},
		{
			name: "invalid slices - valid size exceeds",
			args: args{
				loc: []proto.Slice{
					{MinSliceID: 1, Count: 10, ValidSize: 100},
					{MinSliceID: 2, Count: 20, ValidSize: 200},
				},
				req: []proto.Slice{
					{MinSliceID: 1, Count: 20, ValidSize: 200},
				},
				sliceSize: 10,
			},
			want: false,
		},
		{
			name: "empty loc",
			args: args{
				loc:       []proto.Slice{},
				req:       []proto.Slice{{MinSliceID: 1, Count: 5, ValidSize: 50}},
				sliceSize: 10,
			},
			want: false,
		},
		{
			name: "empty req",
			args: args{
				loc:       []proto.Slice{{MinSliceID: 1, Count: 10, ValidSize: 100}},
				req:       []proto.Slice{},
				sliceSize: 10,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := checkSlices(tt.args.loc, tt.args.req, tt.args.sliceSize)
			require.Equal(t, tt.want, ok)
		})
	}
}
