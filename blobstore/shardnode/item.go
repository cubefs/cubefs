package shardnode

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
)

func (s *service) InsertItem(ctx context.Context, req *shardnode.InsertItemRequest) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.InsertItem(ctx, req.GetHeader(), req.GetItem())
}

func (s *service) UpdateItem(ctx context.Context, req *shardnode.UpdateItemRequest) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.UpdateItem(ctx, req.GetHeader(), req.GetItem())
}

func (s *service) DeleteItem(ctx context.Context, req *shardnode.DeleteItemRequest) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.DeleteItem(ctx, req.GetHeader(), req.GetID())
}

func (s *service) GetItem(ctx context.Context, req *shardnode.GetItemRequest) (item shardnode.Item, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	item, err = space.GetItem(ctx, req.GetHeader(), req.GetID())
	return
}

func (s *service) ListItem(ctx context.Context, req *shardnode.ListItemRequest) (resp shardnode.ListItemResponse, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	items, nextMarker, err := space.ListItem(ctx, req.GetHeader(), req.GetPrefix(), req.GetMarker(), req.GetCount())
	if err != nil {
		return
	}
	resp.Items = items
	resp.NextMarker = nextMarker
	return
}
