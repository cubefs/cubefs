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
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteMgr(t *testing.T) {
	ctx := context.Background()
	ringBufferSize := uint32(3)
	catalogDBPath := os.TempDir() + "/" + uuid.NewString() + strconv.FormatInt(rand.Int63n(math.MaxInt64), 10)
	catalogDB, err := catalogdb.Open(catalogDBPath)
	if err != nil {
		log.Error("open db error")
		return
	}
	defer os.RemoveAll(catalogDBPath)

	storage, err := catalogdb.OpenCatalogTable(catalogDB)
	if err != nil {
		log.Error("open catalog table error")
		return
	}
	// routeMgr
	routeMgr, err := newRouteMgr(ctx, ringBufferSize, storage)
	assert.NoError(t, err)

	// add 3 items
	items := make([]*routeItem, 0)
	for i := 1; i <= 3; i++ {
		item := &routeItem{
			RouteVersion: proto.RouteVersion(routeMgr.genRouteVersion(ctx, 1)),
			Type:         proto.CatalogChangeItemAddShard,
			ItemDetail:   &routeItemShardAdd{ShardID: proto.ShardID(i)},
		}
		items = append(items, item)
	}
	routeMgr.insertRouteItems(ctx, items)
	assert.Equal(t, uint64(3), routeMgr.getRouteVersion())

	// add 1 item
	item4 := &routeItem{
		RouteVersion: proto.RouteVersion(routeMgr.genRouteVersion(ctx, 1)),
		Type:         proto.CatalogChangeItemAddShard,
		ItemDetail:   &routeItemShardAdd{ShardID: 4},
	}
	routeMgr.insertRouteItems(ctx, []*routeItem{item4})
	assert.Equal(t, uint64(4), routeMgr.getRouteVersion())

	// storage items
	items = append(items, item4)
	var routeRecords []*catalogdb.RouteInfoRecord
	for _, item := range items {
		routeRecords = append(routeRecords, routeItemToRouteRecord(item))
	}
	err = storage.PutShardsAndUnitsAndRouteItems(nil, nil, routeRecords)
	assert.NoError(t, err)

	// new a second routemgr
	routeMgr2, err := newRouteMgr(ctx, ringBufferSize, storage)
	assert.NoError(t, err)
	go routeMgr2.loop()
	// 4 item will auto truncate to 3 item
	items2, isLatest := routeMgr2.getRouteItems(ctx, 2)
	assert.Equal(t, false, isLatest)
	assert.Equal(t, 2, len(items2))

	// get the latest version
	items3, isLatest := routeMgr2.getRouteItems(ctx, 4)
	assert.Equal(t, true, isLatest)
	assert.Equal(t, 0, len(items3))

	// add the 5th item, and then the items is [2,3,4,5]
	item5 := &routeItem{
		RouteVersion: proto.RouteVersion(routeMgr2.genRouteVersion(ctx, 1)),
		Type:         proto.CatalogChangeItemUpdateShard,
		ItemDetail:   &routeItemShardUpdate{SuidPrefix: proto.EncodeSuidPrefix(5, 1)},
	}
	routeMgr2.insertRouteItems(ctx, []*routeItem{item5})
	assert.Equal(t, uint64(5), routeMgr2.getRouteVersion())

	// remove the item
	err = routeMgr2.removeOldRouteItems(ctx)
	assert.NoError(t, err)

	// now the items is [3,4,5], so get 2 is null
	items4, isLatest := routeMgr2.getRouteItems(ctx, 2)
	assert.Equal(t, false, isLatest)
	assert.Equal(t, 0, len(items4))

	items5, isLatest := routeMgr2.getRouteItems(ctx, 3)
	assert.Equal(t, false, isLatest)
	assert.Equal(t, 2, len(items5))

	routeMgr2.Close()
}

func TestRouteItemRing(t *testing.T) {
	ring := newRouteItemRing(3)
	items, isLatest := ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	for i := 1; i <= 3; i++ {
		item := &routeItem{
			RouteVersion: proto.RouteVersion(i),
		}
		ring.put(item)
	}
	assert.Equal(t, proto.RouteVersion(1), ring.getMinVer())
	assert.Equal(t, proto.RouteVersion(3), ring.getMaxVer())

	items, isLatest = ring.getFrom(1)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, false, isLatest)

	items, isLatest = ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	item4 := &routeItem{
		RouteVersion: proto.RouteVersion(4),
	}
	ring.put(item4)
	assert.Equal(t, ring.getMinVer(), proto.RouteVersion(2))
	assert.Equal(t, ring.getMaxVer(), proto.RouteVersion(4))
}

func TestCatalogMgr_Route(t *testing.T) {
	mockCatalogMgr, clean := initMockCatalogMgr(t, testConfig)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "route")

	// get all catalogs
	ret, err := mockCatalogMgr.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{})
	require.NoError(t, err)
	require.Equal(t, 10, len(ret.Items))
	require.Equal(t, proto.RouteVersion(11), ret.RouteVersion)

	// get catalog with normal routerVersion
	ret, err = mockCatalogMgr.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{
		RouteVersion: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 10, len(ret.Items))
	require.Equal(t, proto.RouteVersion(11), ret.RouteVersion)

	// get catalog with abnormal routerVersion
	ret, err = mockCatalogMgr.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{
		RouteVersion: 100,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret.Items))
	require.Equal(t, proto.RouteVersion(0), ret.RouteVersion)
}
