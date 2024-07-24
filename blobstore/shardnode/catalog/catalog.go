package catalog

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

const defaultTaskPoolSize = 64

type (
	Config struct {
		Transport   *base.Transport
		ShardGetter ShardGetter
	}

	ShardGetter interface {
		GetShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error)
	}
)

type Catalog struct {
	routeVersion atomic.Uint64
	spaces       sync.Map
	transport    *base.Transport
	taskPool     taskpool.TaskPool

	cfg *Config
	closer.Closer
}

func NewCatalog(ctx context.Context, cfg *Config) *Catalog {
	span := trace.SpanFromContext(ctx)

	catalog := &Catalog{
		cfg:       cfg,
		transport: cfg.Transport,
		taskPool:  taskpool.New(defaultTaskPoolSize, defaultTaskPoolSize),
		Closer:    closer.New(),
	}
	spaces, err := cfg.Transport.GetAllSpaces(ctx)
	if err != nil {
		span.Fatalf("get all spaces failed: %s", err)
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
		v, ok = c.spaces.Load(sid)
	}

	return v.(*Space), nil
}

func (c *Catalog) updateSpace(ctx context.Context, sid proto.SpaceID) error {
	spaceMeta, err := c.transport.GetSpace(ctx, sid)
	if err != nil {
		return err
	}

	space, err := newSpace(&spaceConfig{
		sid:         spaceMeta.SpaceID,
		spaceName:   spaceMeta.Name,
		fieldMetas:  spaceMeta.FieldMetas,
		shardGetter: c.cfg.ShardGetter,
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

func (c *Catalog) updateRouteVersion(new uint64) {
	for old := c.routeVersion.Load(); old < new; {
		if c.routeVersion.CompareAndSwap(old, new) {
			return
		}
	}
}
