package volumemgr

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestVolumeMgr_Route(t *testing.T) {
	mockVolumeMgr, clean := initMockVolumeMgr(t)
	defer clean()
	_, ctx := trace.StartSpanFromContext(context.Background(), "route")

	// get all routes
	ret, err := mockVolumeMgr.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{})
	require.NoError(t, err)
	require.Equal(t, 30, len(ret.Items))
	require.Equal(t, proto.RouteVersion(1), ret.RouteVersion)

	// get routes with normal routerVersion
	ret, err = mockVolumeMgr.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{
		RouteVersion: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret.Items))
	require.Equal(t, proto.RouteVersion(0), ret.RouteVersion)

	// get routes with abnormal routerVersion
	ret, err = mockVolumeMgr.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{
		RouteVersion: 100,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret.Items))
	require.Equal(t, proto.RouteVersion(0), ret.RouteVersion)
}

func TestVolumeRouteMgr(t *testing.T) {
	ctx := context.Background()
	ringBufferSize := uint32(3)
	dbPath := os.TempDir() + "/" + uuid.NewString() + strconv.FormatInt(rand.Int63n(math.MaxInt64), 10)
	volumeDB, err := volumedb.Open(dbPath)
	if err != nil {
		log.Error("open db error")
		return
	}
	defer os.RemoveAll(dbPath)

	storage, err := volumedb.OpenVolumeTable(volumeDB)
	if err != nil {
		log.Error("open volume table error")
		return
	}
	base.RemoveOldRouteInternal = 1 * time.Second
	// routeMgr
	routeMgr := base.NewRouteMgr(ringBufferSize, true, routeRecordToRouteItem, storage)
	err = routeMgr.LoadRoute(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), routeMgr.GetRouteVersion())

	// add 1 item, [2]
	item1 := &base.RouteItem{
		RouteVersion: proto.RouteVersion(routeMgr.GenRouteVersion(ctx, 1)),
		Type:         proto.RouteItemTypeAddVolume,
		ItemDetail:   &routeItemVolumeAdd{Vid: 2},
	}
	routeMgr.InsertRouteItems(ctx, []*base.RouteItem{item1})
	require.Equal(t, uint64(2), routeMgr.GetRouteVersion())

	items, isLatest := routeMgr.GetRouteItems(ctx, 1)
	require.Equal(t, false, isLatest)
	require.Equal(t, 1, len(items))

	// add 3 items, [3,4,5]
	items = make([]*base.RouteItem, 0)
	for i := 3; i <= 5; i++ {
		item := &base.RouteItem{
			RouteVersion: proto.RouteVersion(routeMgr.GenRouteVersion(ctx, 1)),
			Type:         proto.RouteItemTypeAddVolume,
			ItemDetail:   &routeItemVolumeAdd{Vid: proto.Vid(i)},
		}
		items = append(items, item)
	}
	routeMgr.InsertRouteItems(ctx, items)
	require.Equal(t, uint64(5), routeMgr.GetRouteVersion())

	// storage items
	items = append(items, item1)
	var routeRecords []*base.RouteInfoRecord
	for _, item := range items {
		routeRecords = append(routeRecords, routeItemToRouteRecord(item))
	}
	err = storage.PutVolumesAndUnitsAndRoutes(nil, nil, routeRecords)
	require.NoError(t, err)

	// new a second routemgr
	routeMgr2 := base.NewRouteMgr(ringBufferSize, true, routeRecordToRouteItem, storage)
	err = routeMgr2.LoadRoute(ctx)
	require.NoError(t, err)
	go routeMgr2.Loop()
	// 4 item will auto truncate to 3 item, [3,4,5]
	items2, isLatest := routeMgr2.GetRouteItems(ctx, 3)
	require.Equal(t, false, isLatest)
	require.Equal(t, 2, len(items2))

	// get the latest version
	items3, isLatest := routeMgr2.GetRouteItems(ctx, 5)
	require.Equal(t, true, isLatest)
	require.Equal(t, 0, len(items3))

	// add the 5th item, and then the items is [3,4,5,6]
	item5 := &base.RouteItem{
		RouteVersion: proto.RouteVersion(routeMgr2.GenRouteVersion(ctx, 1)),
		Type:         proto.RouteItemTypeUpdateVolume,
		ItemDetail:   &routeItemVolumeUpdate{VuidPrefix: proto.EncodeVuidPrefix(6, 1)},
	}
	routeMgr2.InsertRouteItems(ctx, []*base.RouteItem{item5})
	require.Equal(t, uint64(6), routeMgr2.GetRouteVersion())

	// wait util remove the old items done, [4,5,6]
	time.Sleep(3 * time.Second)
	route, err := storage.GetFirstRoute()
	require.NoError(t, err)
	require.Equal(t, proto.RouteVersion(4), route.RouteVersion)

	// now the items is [4,5,6], so get 3 is null
	items4, isLatest := routeMgr2.GetRouteItems(ctx, 3)
	require.Equal(t, false, isLatest)
	require.Equal(t, 0, len(items4))

	items5, isLatest := routeMgr2.GetRouteItems(ctx, 4)
	require.Equal(t, false, isLatest)
	require.Equal(t, 2, len(items5))

	routeMgr2.Close()
}
