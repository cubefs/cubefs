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

func TestCatalogMgr_Space(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()

	_, ctx := trace.StartSpanFromContext(context.Background(), "space")
	ret, err := mockCatalogMgr.ListSpaceInfo(ctx, &clustermgr.ListSpaceArgs{
		Count: 2001,
	})
	require.NoError(t, err)
	require.Equal(t, 10, len(ret))

	ret, err = mockCatalogMgr.ListSpaceInfo(ctx, &clustermgr.ListSpaceArgs{
		Count:  100,
		Marker: proto.SpaceID(1),
	})
	require.NoError(t, err)
	require.Equal(t, 9, len(ret))

	ret, err = mockCatalogMgr.ListSpaceInfo(ctx, &clustermgr.ListSpaceArgs{
		Count:  100,
		Marker: proto.SpaceID(100),
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret))

	space, err := mockCatalogMgr.GetSpaceInfoByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, proto.SpaceID(1), space.SpaceID)
	require.Equal(t, "spaceName1", space.Name)

	space, err = mockCatalogMgr.GetSpaceInfoByName(ctx, "spaceName1")
	require.NoError(t, err)
	require.Equal(t, proto.SpaceID(1), space.SpaceID)
	require.Equal(t, "spaceName1", space.Name)

	fieldMeta1 := clustermgr.FieldMeta{
		Name:        "filedName1",
		FieldType:   proto.FieldTypeBool,
		IndexOption: proto.IndexOptionIndexed,
	}
	args := &clustermgr.CreateSpaceArgs{
		Name:       "spaceName100",
		FieldMetas: []clustermgr.FieldMeta{fieldMeta1},
	}
	err = mockCatalogMgr.CreateSpace(ctx, args)
	require.NoError(t, err)

	args = &clustermgr.CreateSpaceArgs{
		Name: "spaceName1",
	}
	err = mockCatalogMgr.CreateSpace(ctx, args)
	require.Error(t, err)

	args = &clustermgr.CreateSpaceArgs{}
	err = mockCatalogMgr.CreateSpace(ctx, args)
	require.Error(t, err)

	fieldMeta2 := clustermgr.FieldMeta{
		FieldType:   proto.FieldTypeBool,
		IndexOption: proto.IndexOptionIndexed,
	}
	args = &clustermgr.CreateSpaceArgs{
		Name:       "spaceName101",
		FieldMetas: []clustermgr.FieldMeta{fieldMeta2},
	}
	err = mockCatalogMgr.CreateSpace(ctx, args)
	require.Error(t, err)

	fieldMeta2.Name = "filedName1"
	args = &clustermgr.CreateSpaceArgs{
		Name:       "spaceName101",
		FieldMetas: []clustermgr.FieldMeta{fieldMeta1, fieldMeta2},
	}
	err = mockCatalogMgr.CreateSpace(ctx, args)
	require.Error(t, err)
}
