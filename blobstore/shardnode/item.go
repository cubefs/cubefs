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

// nolint
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
