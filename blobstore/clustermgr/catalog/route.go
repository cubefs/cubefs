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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/gogo/protobuf/types"
)

func (c *CatalogMgr) GetCatalogChanges(ctx context.Context, args *clustermgr.GetCatalogChangesArgs) (ret *clustermgr.GetCatalogChangesRet, err error) {
	span := trace.SpanFromContextSafe(ctx)

	var (
		items    []*base.RouteItem
		isLatest bool
	)
	if args.RouteVersion > 0 {
		items, isLatest = c.routeMgr.GetRouteItems(ctx, args.RouteVersion)
	}
	ret = new(clustermgr.GetCatalogChangesRet)
	if items == nil && !isLatest {
		// get all catalog
		ret.RouteVersion = proto.RouteVersion(c.routeMgr.GetRouteVersion())
		shards := c.allShards.list()
		items = make([]*base.RouteItem, 0, len(shards))
		for _, shard := range shards {
			items = append(items, &base.RouteItem{
				Type:       proto.CatalogChangeItemAddShard,
				ItemDetail: &routeItemShardAdd{ShardID: shard.shardID},
			})
		}
	}

	for i := range items {
		ret.Items = append(ret.Items, clustermgr.CatalogChangeItem{
			RouteVersion: items[i].RouteVersion,
			Type:         items[i].Type.(proto.CatalogChangeItemType),
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
					ret.Items[i].RouteVersion = shard.info.RouteVersion
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
