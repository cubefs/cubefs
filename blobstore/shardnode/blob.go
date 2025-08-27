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
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
)

func (s *service) createBlob(ctx context.Context, req *shardnode.CreateBlobArgs) (resp shardnode.CreateBlobRet, err error) {
	if len(req.Name) < 1 {
		err = apierr.ErrBlobNameEmpty
		return
	}
	if len(req.Name) > storage.MaxKeySize {
		err = apierr.ErrKeySizeTooLarge
		return
	}
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	return space.CreateBlob(ctx, req)
}

func (s *service) deleteBlob(ctx context.Context, req *shardnode.DeleteBlobArgs) (blob shardnode.GetBlobRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}

	blob, err = space.GetBlob(ctx, &shardnode.GetBlobArgs{
		Header: req.Header,
		Name:   req.Name,
	})

	tagNum, err := space.GetShardingSubRangeCount(req.Header.DiskID, req.Header.Suid)
	if err != nil {
		return
	}
	shardKeys := shardnode.DecodeShardKeys(req.Name, tagNum)

	items, err := s.blobDelMgr.SlicesToDeleteMsgItems(ctx, blob.Blob.Location.Slices, shardKeys)
	if err != nil {
		return
	}
	return blob, space.DeleteBlob(ctx, req, items)
}

func (s *service) sealBlob(ctx context.Context, req *shardnode.SealBlobArgs) error {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return err
	}
	return space.SealBlob(ctx, req)
}

func (s *service) getBlob(ctx context.Context, req *shardnode.GetBlobArgs) (blob shardnode.GetBlobRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return shardnode.GetBlobRet{}, err
	}
	return space.GetBlob(ctx, req)
}

func (s *service) listBlob(ctx context.Context, req *shardnode.ListBlobArgs) (resp shardnode.ListBlobRet, err error) {
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

func (s *service) allocSlice(ctx context.Context, req *shardnode.AllocSliceArgs) (resp shardnode.AllocSliceRet, err error) {
	sid := req.Header.SpaceID
	space, err := s.catalog.GetSpace(ctx, sid)
	if err != nil {
		return
	}
	return space.AllocSlice(ctx, req)
}

func (s *service) deleteBlobRaw(ctx context.Context, req *shardnode.DeleteBlobRawArgs) error {
	return s.blobDelMgr.Delete(ctx, req)
}
