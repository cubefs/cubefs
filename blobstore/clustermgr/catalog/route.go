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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/gogo/protobuf/types"
)

func (c *CatalogMgr) GetCatalogChanges(ctx context.Context, args *clustermgr.GetCatalogChangesArgs) (ret *clustermgr.GetCatalogChangesRet, err error) {
	span := trace.SpanFromContextSafe(ctx)

	var (
		items    []*routeItem
		isLatest bool
	)
	if args.RouteVersion > 0 {
		items, isLatest = c.routeMgr.getRouteItems(ctx, args.RouteVersion)
	}
	ret = new(clustermgr.GetCatalogChangesRet)
	if items == nil && !isLatest {
		// get all catalog
		ret.RouteVersion = proto.RouteVersion(c.routeMgr.getRouteVersion())
		shards := c.allShards.list()
		for _, shard := range shards {
			items = append(items, &routeItem{
				Type:       proto.CatalogChangeItemAddShard,
				ItemDetail: &routeItemShardAdd{ShardID: shard.shardID},
			})
		}
	}

	for i := range items {
		ret.Items = append(ret.Items, clustermgr.CatalogChangeItem{
			RouteVersion: items[i].RouteVersion,
			Type:         items[i].Type,
		})
		switch items[i].Type {
		case proto.CatalogChangeItemAddShard:
			shardID := items[i].ItemDetail.(*routeItemShardAdd).ShardID
			shard := c.allShards.getShard(shardID)
			addShardItem := &clustermgr.CatalogChangeShardAdd{
				ShardID: shardID,
			}
			shard.withRLocked(func() error {
				if items[i].RouteVersion == proto.InvalidRouteVersion {
					items[i].RouteVersion = shard.info.RouteVersion
				}
				for _, unit := range shard.info.Units {
					unitInfo := shardUnitToShardUnitInfo(unit, items[i].RouteVersion, shard.info.Range, shard.info.LeaderDiskID)
					addShardItem.Units = append(addShardItem.Units, unitInfo)
				}
				return nil
			})
			addShardItem.RouteVersion = items[i].RouteVersion
			ret.Items[i].Item, err = types.MarshalAny(addShardItem)
			span.Debugf("addShardItem: %+v", addShardItem)
		case proto.CatalogChangeItemUpdateShard:
			suidPrefix := items[i].ItemDetail.(*routeItemShardUpdate).SuidPrefix
			shard := c.allShards.getShard(suidPrefix.ShardID())
			updateShardItem := &clustermgr.CatalogChangeShardUpdate{
				ShardID:      suidPrefix.ShardID(),
				RouteVersion: items[i].RouteVersion,
			}
			shard.withRLocked(func() error {
				unit := shard.info.Units[suidPrefix.Index()]
				updateShardItem.Unit = shardUnitToShardUnitInfo(unit, items[i].RouteVersion, shard.info.Range, shard.info.LeaderDiskID)
				return nil
			})
			ret.Items[i].Item, err = types.MarshalAny(updateShardItem)
			span.Debugf("updateShardItem: %+v", updateShardItem)
		default:
		}

		if err != nil {
			return nil, err
		}
	}

	if ret.RouteVersion == 0 && len(ret.Items) > 0 {
		ret.RouteVersion = ret.Items[len(ret.Items)-1].RouteVersion
	}
	return ret, nil
}

type routeMgr struct {
	truncateIntervalNum  uint32
	unstableRouteVersion proto.RouteVersion
	stableRouteVersion   proto.RouteVersion
	increments           *routeItemRing
	done                 chan struct{}
	lock                 sync.RWMutex

	storage *catalogdb.CatalogTable
}

func newRouteMgr(ctx context.Context, truncateIntervalNum uint32, storage *catalogdb.CatalogTable) (*routeMgr, error) {
	routeMgr := &routeMgr{
		truncateIntervalNum: truncateIntervalNum,
		increments:          newRouteItemRing(truncateIntervalNum),
		done:                make(chan struct{}),
		storage:             storage,
	}

	// load route into memory
	records, err := storage.ListRoute()
	if err != nil {
		return nil, err
	}

	if len(records) > int(truncateIntervalNum) {
		records = records[len(records)-int(truncateIntervalNum):]
	}
	maxRouteVersion := proto.RouteVersion(0)
	for _, record := range records {
		item := routeRecordToRouteItem(record)
		routeMgr.increments.put(item)
		if item.RouteVersion > maxRouteVersion {
			maxRouteVersion = item.RouteVersion
		}
	}
	routeMgr.stableRouteVersion = maxRouteVersion
	routeMgr.unstableRouteVersion = maxRouteVersion

	go routeMgr.loop()

	return routeMgr, nil
}

func (r *routeMgr) Close() {
	close(r.done)
}

func (r *routeMgr) getRouteVersion() uint64 {
	return atomic.LoadUint64((*uint64)(&r.stableRouteVersion))
}

func (r *routeMgr) genRouteVersion(ctx context.Context, step uint64) uint64 {
	return atomic.AddUint64((*uint64)(&r.unstableRouteVersion), step)
}

func (r *routeMgr) insertRouteItems(ctx context.Context, items []*routeItem) {
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

func (r *routeMgr) getRouteItems(ctx context.Context, ver proto.RouteVersion) (ret []*routeItem, isLatest bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.increments.getFrom(ver)
}

func (r *routeMgr) loop() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ticker := time.NewTicker(1 * time.Minute)

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

func (r *routeMgr) removeOldRouteItems(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	item, err := r.storage.GetFirstRouteItem()
	if err != nil {
		span.Errorf("get first route item failed: %s", err.Error())
		return fmt.Errorf("get first route item failed: %s", err.Error())
	}
	if item == nil {
		span.Info("routeTbl has no first route")
		return nil
	}

	if uint64(item.RouteVersion) < atomic.LoadUint64((*uint64)(&r.stableRouteVersion))-uint64(r.truncateIntervalNum) {
		if err := r.storage.DeleteOldRoutes(item.RouteVersion); err != nil {
			span.Errorf("delete oldest route items failed: %s", err.Error())
			return fmt.Errorf("delete oldest route items failed: %s", err.Error())
		}
		span.Infof("delete oldest route items[%d] success", item.RouteVersion)
	}
	return nil
}

type routeItemRing struct {
	data     []*routeItem
	head     uint32
	tail     uint32
	nextTail uint32
	cap      uint32
	usedCap  uint32
}

func newRouteItemRing(cap uint32) *routeItemRing {
	ring := &routeItemRing{
		data: make([]*routeItem, cap),
		cap:  cap,
	}
	return ring
}

func (r *routeItemRing) put(item *routeItem) {
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

func (r *routeItemRing) getFrom(ver proto.RouteVersion) (ret []*routeItem, isLatest bool) {
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
