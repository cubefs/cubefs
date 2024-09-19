// Copyright 2024 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/stretchr/testify/require"
)

func TestCatalogMgr_ListShardInfo(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "listShardInfo")
	args := &clustermgr.ListShardArgs{
		Count: 503,
	}
	infos, err := mockCatalogMgr.ListShardInfo(ctx, args)
	require.NoError(t, err)
	require.Equal(t, len(infos), 10)

	args.Count = 3
	infos, err = mockCatalogMgr.ListShardInfo(ctx, args)
	require.NoError(t, err)
	require.Equal(t, len(infos), 3)

	args.Marker = 9
	infos, err = mockCatalogMgr.ListShardInfo(ctx, args)
	require.NoError(t, err)
	require.Equal(t, len(infos), 1)
	require.Equal(t, infos[0].ShardID, proto.ShardID(10))

	args.Marker = 29
	infos, err = mockCatalogMgr.ListShardInfo(ctx, args)
	require.NoError(t, err)
	require.Nil(t, infos)
}

func TestCatalogMgr_GetShardInfo(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "getShardInfo")
	ret, err := mockCatalogMgr.GetShardInfo(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, ret.ShardID, proto.ShardID(1))
}
