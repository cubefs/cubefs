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

package clustermgr

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/stretchr/testify/require"
)

func TestService_Route(t *testing.T) {
	testService, clean := initServiceWithShardData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// get route with specified routeVersion
	ret, err := cmClient.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{RouteVersion: 1})
	require.NoError(t, err)
	require.Equal(t, 9, len(ret.Items))
	require.Equal(t, proto.RouteVersion(10), ret.RouteVersion)

	// get route without specified routeVersion
	ret, err = cmClient.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{})
	require.NoError(t, err)
	require.Equal(t, 10, len(ret.Items))
	require.Equal(t, proto.RouteVersion(10), ret.RouteVersion)

	// get route with routeVersion which is bigger than CM's
	ret, err = cmClient.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{RouteVersion: 100})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret.Items))
	require.Equal(t, proto.RouteVersion(0), ret.RouteVersion)
}
