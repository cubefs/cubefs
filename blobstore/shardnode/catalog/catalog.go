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
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog/allocator"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

type (
	Config struct {
		ClusterID   proto.ClusterID
		Transport   base.Transport
		ShardGetter ShardGetter
		AllocCfg
	}

	AllocCfg struct {
		BidAllocNums         uint64
		RetainIntervalS      int64
		DefaultAllocVolsNum  int
		InitVolumeNum        int
		TotalThresholdRatio  float64
		RetainVolumeBatchNum int
		RetainBatchIntervalS int64
	}

	ShardGetter interface {
		GetShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error)
	}
)

type Catalog struct {
	spaces    sync.Map
	transport base.Transport

	allocator allocator.Allocator

	cfg *Config
	closer.Closer
}

func NewCatalog(ctx context.Context, cfg *Config) *Catalog {
	span := trace.SpanFromContext(ctx)

	blobCfg := allocator.BlobConfig{BidAllocNums: cfg.BidAllocNums}
	volCfg := allocator.VolConfig{
		RetainIntervalS:      cfg.RetainIntervalS,
		DefaultAllocVolsNum:  cfg.DefaultAllocVolsNum,
		InitVolumeNum:        cfg.InitVolumeNum,
		TotalThresholdRatio:  cfg.TotalThresholdRatio,
		RetainVolumeBatchNum: cfg.RetainVolumeBatchNum,
		RetainBatchIntervalS: cfg.RetainBatchIntervalS,
		VolumeReserveSize:    cfg.InitVolumeNum,
	}

	alc, err := allocator.NewAllocator(ctx, blobCfg, volCfg, cfg.Transport)
	if err != nil {
		span.Fatalf("new catalog allocator error: %v", err)
	}

	catalog := &Catalog{
		cfg:       cfg,
		transport: cfg.Transport,
		Closer:    closer.New(),
		allocator: alc,
	}
	spaces, err := cfg.Transport.GetAllSpaces(ctx)
	if err != nil {
		span.Panicf("get all spaces failed: %s", err)
	}
	for i := range spaces {
		catalog.spaces.Store(spaces[i].SpaceID, &spaces[i])
	}

	return catalog
}

func (c *Catalog) GetSpace(ctx context.Context, sid proto.SpaceID) (*Space, error) {
	return c.getSpace(ctx, sid)
}

func (c *Catalog) getSpace(ctx context.Context, sid proto.SpaceID) (*Space, error) {
	v, ok := c.spaces.Load(sid)
	if !ok {
		if err := c.updateSpace(ctx, sid); err != nil {
			return nil, err
		}
		v, _ = c.spaces.Load(sid)
	}

	return v.(*Space), nil
}

func (c *Catalog) updateSpace(ctx context.Context, sid proto.SpaceID) error {
	spaceMeta, err := c.transport.GetSpace(ctx, sid)
	if err != nil {
		return err
	}

	space, err := newSpace(&spaceConfig{
		clusterID:   c.cfg.ClusterID,
		sid:         spaceMeta.SpaceID,
		spaceName:   spaceMeta.Name,
		fieldMetas:  spaceMeta.FieldMetas,
		shardGetter: c.cfg.ShardGetter,
		allocator:   c.allocator,
	})
	if err != nil {
		return err
	}

	c.spaces.LoadOrStore(spaceMeta.SpaceID, space)
	return nil
}

/*
func (c *Catalog) initRoute(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	routeVersion, changes, err := c.transport.GetRouteUpdate(ctx, c.routeVersion.Load())
	if err != nil {
		return errors.Info(err, "get route update failed")
	}

	for _, routeItem := range changes {
		switch routeItem.Type {
		case proto.CatalogChangeItem_AddSpace:
			spaceItem := new(proto.CatalogChangeSpaceAdd)
			if err := types.UnmarshalAny(routeItem.item, spaceItem); err != nil {
				return errors.Info(err, "unmarshal add Space item failed")
			}

			space, err := newSpace(&spaceConfig{
				sid:               spaceItem.SpaceID,
				spaceName:         spaceItem.Name,
				spaceType:         spaceItem.Type,
				spaceVersion:      0,
				fieldMetas:        spaceItem.FixedFields,
				vectorIndexConfig: spaceItem.VectorIndexConfig,
				getShard:          c,
			})
			if err != nil {
				return err
			}

			span.Debugf("load space[%+v] from catalog change", space)
			if _, loaded := c.spaces.LoadOrStore(spaceItem.SpaceID, space); loaded {
				continue
			}

		case proto.CatalogChangeItem_DeleteSpace:
			spaceItem := new(proto.CatalogChangeSpaceDelete)
			if err := types.UnmarshalAny(routeItem.item, spaceItem); err != nil {
				return errors.Info(err, "unmarshal delete Space item failed")
			}

			c.spaces.Delete(spaceItem.SpaceID)
		default:
		}
		c.updateRouteVersion(routeItem.RouteVersion)
	}

	c.updateRouteVersion(routeVersion)
	return nil
}*/
