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

// nolint
package shardnode

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
)

func (s *service) CreateBlob(ctx context.Context, req *shardnode.CreateBlobArgs) (resp shardnode.CreateBlobRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	return space.CreateBlob(ctx, req)
}

func (s *service) DeleteBlob(ctx context.Context, req *shardnode.DeleteBlobArgs) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return nil
	}
	return space.DeleteBlob(ctx, req)
}

func (s *service) SealBlob(ctx context.Context, req *shardnode.SealBlobArgs) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return nil
	}
	return space.SealBlob(ctx, req)
}

func (s *service) GetBlob(ctx context.Context, req *shardnode.GetBlobArgs) (blob shardnode.GetBlobRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return shardnode.GetBlobRet{}, err
	}
	return space.GetBlob(ctx, req)
}

func (s *service) ListBlob(ctx context.Context, req *shardnode.ListBlobArgs) (resp shardnode.ListBlobRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return shardnode.ListBlobRet{}, err
	}
	blobs, nextMarker, err := space.ListBlob(ctx, req.GetHeader(), req.GetPrefix(), req.GetMarker(), req.GetCount())
	if err != nil {
		return
	}
	resp.Blobs = blobs
	resp.NextMarker = nextMarker
	return
}
