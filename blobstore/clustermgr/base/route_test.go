package base

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type routeStorageMock struct {
	firstRoute      *RouteInfoRecord
	routes          []*RouteInfoRecord
	listErr         error
	firstErr        error
	deleteErr       error
	deleteCalled    bool
	deleteCalledNum int
	deleteBefore    proto.RouteVersion
}

func (m *routeStorageMock) GetFirstRoute() (*RouteInfoRecord, error) {
	if m.firstErr != nil {
		return nil, m.firstErr
	}
	return m.firstRoute, nil
}

func (m *routeStorageMock) ListRoute() ([]*RouteInfoRecord, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.routes, nil
}

func (m *routeStorageMock) DeleteOldRoutes(before proto.RouteVersion) error {
	m.deleteCalled = true
	m.deleteCalledNum++
	m.deleteBefore = before
	return m.deleteErr
}

func recordToItem(info *RouteInfoRecord) *RouteItem {
	return &RouteItem{
		RouteVersion: info.RouteVersion,
		Type:         info.Type,
		ItemDetail:   info.ItemDetail,
	}
}

func TestRouteItemRing(t *testing.T) {
	ring := newRouteItemRing(3)
	items, isLatest := ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	for i := 1; i <= 3; i++ {
		item := &RouteItem{
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

	// ver older than min
	items, isLatest = ring.getFrom(0)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, false, isLatest)

	item4 := &RouteItem{
		RouteVersion: proto.RouteVersion(4),
	}
	ring.put(item4)
	assert.Equal(t, ring.getMinVer(), proto.RouteVersion(2))
	assert.Equal(t, ring.getMaxVer(), proto.RouteVersion(4))
}

func TestRouteItemRing_PutInconsistent_Panic(t *testing.T) {
	ring := newRouteItemRing(3)
	ring.put(&RouteItem{RouteVersion: 1})
	assert.Panics(t, func() {
		ring.put(&RouteItem{RouteVersion: 3})
	})
}

func TestRemoveOldRouteItems_NoDeleteWhenStableLessThanTruncate(t *testing.T) {
	storage := &routeStorageMock{
		firstRoute: &RouteInfoRecord{RouteVersion: proto.RouteVersion(1)},
	}
	routeMgr := NewRouteMgr(10, false, nil, storage)
	routeMgr.stableRouteVersion = proto.RouteVersion(5)

	err := routeMgr.removeOldRouteItems(context.Background())
	assert.NoError(t, err)
	assert.False(t, storage.deleteCalled)
	assert.Equal(t, 0, storage.deleteCalledNum)
}

func TestRemoveOldRouteItems_GetFirstError(t *testing.T) {
	storage := &routeStorageMock{firstErr: errors.New("db error")}
	routeMgr := NewRouteMgr(10, false, nil, storage)

	err := routeMgr.removeOldRouteItems(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get first route item failed")
}

func TestRemoveOldRouteItems_NoFirstRoute(t *testing.T) {
	storage := &routeStorageMock{}
	routeMgr := NewRouteMgr(10, false, nil, storage)

	err := routeMgr.removeOldRouteItems(context.Background())
	assert.NoError(t, err)
	assert.False(t, storage.deleteCalled)
}

func TestRemoveOldRouteItems_DeleteSuccess(t *testing.T) {
	storage := &routeStorageMock{
		firstRoute: &RouteInfoRecord{RouteVersion: proto.RouteVersion(1)},
	}
	routeMgr := NewRouteMgr(10, false, nil, storage)
	routeMgr.stableRouteVersion = proto.RouteVersion(20)

	err := routeMgr.removeOldRouteItems(context.Background())
	assert.NoError(t, err)
	assert.True(t, storage.deleteCalled)
	assert.Equal(t, proto.RouteVersion(11), storage.deleteBefore)
}

func TestRemoveOldRouteItems_DeleteError(t *testing.T) {
	storage := &routeStorageMock{
		firstRoute: &RouteInfoRecord{RouteVersion: proto.RouteVersion(1)},
		deleteErr:  errors.New("delete failed"),
	}
	routeMgr := NewRouteMgr(10, false, nil, storage)
	routeMgr.stableRouteVersion = proto.RouteVersion(20)

	err := routeMgr.removeOldRouteItems(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "delete oldest route items failed")
}

func TestLoadRoute_ReloadAfterInitNullRoute(t *testing.T) {
	storage := &routeStorageMock{}
	routeMgr := NewRouteMgr(10, true, recordToItem, storage)

	err := routeMgr.LoadRoute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), routeMgr.GetRouteVersion())

	storage.routes = []*RouteInfoRecord{
		{RouteVersion: proto.RouteVersion(66393)},
		{RouteVersion: proto.RouteVersion(66394)},
	}
	err = routeMgr.LoadRoute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(66394), routeMgr.GetRouteVersion())

	items, isLatest := routeMgr.GetRouteItems(context.Background(), 66393)
	assert.False(t, isLatest)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, proto.RouteVersion(66394), items[0].RouteVersion)
}

func TestLoadRoute_ListRouteError(t *testing.T) {
	storage := &routeStorageMock{listErr: errors.New("list failed")}
	routeMgr := NewRouteMgr(10, true, recordToItem, storage)

	err := routeMgr.LoadRoute(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list failed")
}

func TestLoadRoute_TruncateByInterval(t *testing.T) {
	storage := &routeStorageMock{
		routes: []*RouteInfoRecord{
			{RouteVersion: 1},
			{RouteVersion: 2},
			{RouteVersion: 3},
			{RouteVersion: 4},
			{RouteVersion: 5},
		},
	}
	routeMgr := NewRouteMgr(3, false, recordToItem, storage)

	err := routeMgr.LoadRoute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), routeMgr.GetRouteVersion())

	// only last 3 kept in ring: [3,4,5], ver=2 is older than min
	items, isLatest := routeMgr.GetRouteItems(context.Background(), 2)
	assert.False(t, isLatest)
	assert.Equal(t, 0, len(items))

	items, isLatest = routeMgr.GetRouteItems(context.Background(), 3)
	assert.False(t, isLatest)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, proto.RouteVersion(4), items[0].RouteVersion)
}

func TestLoadRoute_WithoutInitNullRoute(t *testing.T) {
	storage := &routeStorageMock{}
	routeMgr := NewRouteMgr(10, false, recordToItem, storage)

	err := routeMgr.LoadRoute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), routeMgr.GetRouteVersion())
}

func TestRouteMgr_GenInsertCloseAndLoop(t *testing.T) {
	oldInterval := RemoveOldRouteInternal
	RemoveOldRouteInternal = 20 * time.Millisecond
	defer func() { RemoveOldRouteInternal = oldInterval }()

	storage := &routeStorageMock{
		firstRoute: &RouteInfoRecord{RouteVersion: proto.RouteVersion(1)},
	}
	routeMgr := NewRouteMgr(3, true, recordToItem, storage)
	err := routeMgr.LoadRoute(context.Background())
	assert.NoError(t, err)

	ver := routeMgr.GenRouteVersion(context.Background(), 1)
	assert.Equal(t, uint64(2), ver)

	routeMgr.InsertRouteItems(context.Background(), []*RouteItem{
		{RouteVersion: proto.RouteVersion(2)},
		{RouteVersion: proto.RouteVersion(3)},
		{RouteVersion: proto.RouteVersion(4)},
		{RouteVersion: proto.RouteVersion(5)},
	})
	assert.Equal(t, uint64(5), routeMgr.GetRouteVersion())

	// trigger Loop delete path: first=1, truncate=3, stable=5 → 1+3 < 5
	go routeMgr.Loop()
	time.Sleep(80 * time.Millisecond)
	routeMgr.Close()
	time.Sleep(30 * time.Millisecond)

	assert.True(t, storage.deleteCalled)
	assert.Equal(t, proto.RouteVersion(3), storage.deleteBefore)
}
