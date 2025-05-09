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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog/allocator"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	_, ctx = trace.StartSpanFromContext(context.Background(), "Testing")
)

func mockTransport(tb testing.TB) *base.MockTransport {
	tp := allocator.NewMockAllocTransport(tb).(*base.MockTransport)
	return tp
}

func TestServerCatalog_New(t *testing.T) {
	cfg := &Config{}
	tp := mockTransport(t)
	cfg.Transport = tp

	tp.EXPECT().GetAllSpaces(A).Return(nil, errors.New("")).Times(1)
	require.Panics(t, func() { NewCatalog(ctx, cfg) })

	tp.EXPECT().GetAllSpaces(A).Return(
		[]clustermgr.Space{
			{SpaceID: 1, Name: "space1"},
		}, nil,
	)
	require.NotPanics(t, func() { NewCatalog(ctx, cfg) })
}

func TestServerCatalog_Space(t *testing.T) {
	cfg := &Config{}
	tp := mockTransport(t)
	cfg.Transport = tp

	sid1 := proto.SpaceID(1)
	sid2 := proto.SpaceID(2)
	sid3 := proto.SpaceID(3)
	tp.EXPECT().GetAllSpaces(A).Return([]clustermgr.Space{
		{
			SpaceID: sid1,
			Name:    "space1",
		},
		{
			SpaceID: sid2,
			Name:    "space2",
		},
	}, nil)

	mockErr := errors.New("space not found")
	tp.EXPECT().GetSpace(A, A).DoAndReturn(func(ctx context.Context, sid proto.SpaceID) (*clustermgr.Space, error) {
		if sid == sid3 {
			return &clustermgr.Space{
				SpaceID: sid3,
				Name:    "space3",
			}, nil
		}
		return nil, mockErr
	}).Times(2)

	// test new catalog
	c := NewCatalog(ctx, cfg)

	// test get space
	space, err := c.GetSpace(ctx, sid3)
	require.Nil(t, err)
	require.Equal(t, sid3, space.sid)

	space, err = c.GetSpace(ctx, proto.SpaceID(4))
	require.Nil(t, space)
	require.Equal(t, mockErr, errors.Cause(err))
}

func TestServerCatalog_InitRoute(t *testing.T) {}
