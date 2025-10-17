package base

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var RemoveOldRouteInternal = 1 * time.Minute

type RouteStorage interface {
	GetFirstRoute() (*RouteInfoRecord, error)
	ListRoute() ([]*RouteInfoRecord, error)
	DeleteOldRoutes(before proto.RouteVersion) error
}

type RouteMgr struct {
	truncateIntervalNum  uint32
	initNullRoute        bool
	unstableRouteVersion proto.RouteVersion
	stableRouteVersion   proto.RouteVersion
	increments           *routeItemRing
	done                 chan struct{}
	lock                 sync.RWMutex
	recordToItem         func(info *RouteInfoRecord) *RouteItem
	storage              RouteStorage
}

func NewRouteMgr(truncateIntervalNum uint32, initNullRoute bool, recordToItem func(info *RouteInfoRecord) *RouteItem, storage RouteStorage) *RouteMgr {
	r := &RouteMgr{
		truncateIntervalNum: truncateIntervalNum,
		initNullRoute:       initNullRoute,
		increments:          newRouteItemRing(truncateIntervalNum),
		done:                make(chan struct{}),
		recordToItem:        recordToItem,
		storage:             storage,
	}
	return r
}

func (r *RouteMgr) Close() {
	close(r.done)
}

func (r *RouteMgr) LoadRoute(ctx context.Context) error {
	// load route into memory
	records, err := r.storage.ListRoute()
	if err != nil {
		return errors.Info(err, "storage ListRoute").Detail(err)
	}
	if len(records) > int(r.truncateIntervalNum) {
		records = records[len(records)-int(r.truncateIntervalNum):]
	}
	maxRouteVersion := r.stableRouteVersion
	for _, record := range records {
		item := r.recordToItem(record)
		r.increments.put(item)
		if item.RouteVersion > maxRouteVersion {
			maxRouteVersion = item.RouteVersion
		}
	}
	r.stableRouteVersion = maxRouteVersion
	r.unstableRouteVersion = maxRouteVersion

	// init null route
	if r.initNullRoute && r.stableRouteVersion == 0 {
		maxRouteVersion = proto.RouteVersion(1)
		r.increments.put(&RouteItem{RouteVersion: maxRouteVersion})
		r.stableRouteVersion = maxRouteVersion
		r.unstableRouteVersion = maxRouteVersion
	}

	return nil
}

func (r *RouteMgr) GetRouteVersion() uint64 {
	return atomic.LoadUint64((*uint64)(&r.stableRouteVersion))
}

func (r *RouteMgr) GenRouteVersion(ctx context.Context, step uint64) uint64 {
	return atomic.AddUint64((*uint64)(&r.unstableRouteVersion), step)
}

func (r *RouteMgr) InsertRouteItems(ctx context.Context, items []*RouteItem) {
	r.lock.Lock()
	defer r.lock.Unlock()

	maxStableRouteVersion := proto.RouteVersion(0)
	for _, item := range items {
		r.increments.put(item)
		if item.RouteVersion > maxStableRouteVersion {
			maxStableRouteVersion = item.RouteVersion
		}
	}
	atomic.StoreUint64((*uint64)(&r.stableRouteVersion), uint64(maxStableRouteVersion))
}

func (r *RouteMgr) GetRouteItems(ctx context.Context, ver proto.RouteVersion) (ret []*RouteItem, isLatest bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.increments.getFrom(ver)
}

func (r *RouteMgr) Loop() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ticker := time.NewTicker(RemoveOldRouteInternal)

	for {
		select {
		case <-ticker.C:
			// check route items num, remove old route item if exceed the max increment items limit
			r.removeOldRouteItems(ctx)
		case <-r.done:
			return
		}
	}
}

func (r *RouteMgr) removeOldRouteItems(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	item, err := r.storage.GetFirstRoute()
	if err != nil {
		span.Errorf("get first route item failed: %s", err.Error())
		return fmt.Errorf("get first route item failed: %s", err.Error())
	}
	if item == nil {
		span.Info("routeTbl has no first route")
		return nil
	}

	stableRouteVersion := atomic.LoadUint64((*uint64)(&r.stableRouteVersion))
	if uint64(item.RouteVersion) < stableRouteVersion-uint64(r.truncateIntervalNum) {
		if err := r.storage.DeleteOldRoutes(proto.RouteVersion(stableRouteVersion-uint64(r.truncateIntervalNum)) + 1); err != nil {
			span.Errorf("delete oldest route items failed: %s", err.Error())
			return fmt.Errorf("delete oldest route items failed: %s", err.Error())
		}
		span.Infof("delete oldest route items[%d] success", item.RouteVersion)
	}
	return nil
}

// persistent record
type RouteInfoRecord struct {
	RouteVersion proto.RouteVersion `json:"route_version"`
	Type         interface{}        `json:"type"`
	ItemDetail   interface{}        `json:"item"`
}

// memory item
type RouteItem struct {
	RouteVersion proto.RouteVersion
	Type         interface{}
	ItemDetail   interface{}
}

type routeItemRing struct {
	data     []*RouteItem
	head     uint32
	tail     uint32
	nextTail uint32
	cap      uint32
	usedCap  uint32
}

func newRouteItemRing(cap uint32) *routeItemRing {
	ring := &routeItemRing{
		data: make([]*RouteItem, cap),
		cap:  cap,
	}
	return ring
}

func (r *routeItemRing) put(item *RouteItem) {
	r.data[r.nextTail] = item
	r.tail = r.nextTail
	if r.cap == r.usedCap {
		r.head++
		r.head = r.head % r.cap
		r.nextTail++
		r.nextTail = r.nextTail % r.cap
	} else {
		r.nextTail++
		r.nextTail = r.nextTail % r.cap
		r.usedCap++
	}
	if (r.data[r.head].RouteVersion + proto.RouteVersion(r.usedCap)) != (r.data[r.tail].RouteVersion + 1) {
		errMsg := fmt.Sprintf("route cache ring is not consistently, head %v ver: %v, usedCap: %v, tail %v ver: %v",
			r.head, r.data[r.head].RouteVersion, r.usedCap, r.tail, r.data[r.tail].RouteVersion)
		panic(errMsg)
	}
}

func (r *routeItemRing) getFrom(ver proto.RouteVersion) (ret []*RouteItem, isLatest bool) {
	if r.head == r.tail {
		return nil, true
	}
	if r.getMinVer() > ver {
		return nil, false
	}

	if r.getMaxVer() <= ver {
		return nil, true
	}

	headVer := r.data[r.head].RouteVersion
	i := (r.head + uint32(ver-headVer+1)) % r.cap
	for j := 0; j < int(r.usedCap); j++ {
		ret = append(ret, r.data[i])
		i = (i + 1) % r.cap
		if i == r.nextTail {
			break
		}
	}
	return ret, false
}

func (r *routeItemRing) getMinVer() proto.RouteVersion {
	return r.data[r.head].RouteVersion
}

func (r *routeItemRing) getMaxVer() proto.RouteVersion {
	return r.data[r.tail].RouteVersion
}
