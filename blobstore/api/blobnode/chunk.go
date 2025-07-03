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

package blobnode

import (
	"context"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func IsValidDiskID(id proto.DiskID) bool {
	return id != proto.InvalidDiskID
}

func IsValidChunkID(id clustermgr.ChunkID) bool {
	return id != clustermgr.InvalidChunkID
}

func IsValidChunkStatus(status clustermgr.ChunkStatus) bool {
	return status < clustermgr.ChunkNumStatus
}

type CreateChunkArgs struct {
	DiskID    proto.DiskID `json:"diskid"`
	Vuid      proto.Vuid   `json:"vuid"`
	ChunkSize int64        `json:"chunksize,omitempty"`
}

func (c *client) CreateChunk(ctx context.Context, host string, args *CreateChunkArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/create/diskid/%v/vuid/%v?chunksize=%v",
		host, args.DiskID, args.Vuid, args.ChunkSize)

	err = c.PostWith(ctx, urlStr, nil, rpc.NoneBody)
	return
}

type StatChunkArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
}

func (c *client) StatChunk(ctx context.Context, host string, args *StatChunkArgs) (ci *clustermgr.ChunkInfo, err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/stat/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)
	ci = new(clustermgr.ChunkInfo)
	err = c.GetWith(ctx, urlStr, ci)
	return
}

type ChangeChunkStatusArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Force  bool         `json:"force,omitempty"`
}

type ChunkInspectArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
}

func (c *client) ReleaseChunk(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/release/diskid/%v/vuid/%v?force=%v", host, args.DiskID, args.Vuid, args.Force)
	err = c.PostWith(ctx, urlStr, nil, rpc.NoneBody)
	return
}

func (c *client) SetChunkReadonly(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/readonly/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)

	err = c.PostWith(ctx, urlStr, nil, rpc.NoneBody)
	return
}

func (c *client) SetChunkReadwrite(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/readwrite/diskid/%v/vuid/%v", host, args.DiskID, args.Vuid)
	err = c.PostWith(ctx, urlStr, nil, rpc.NoneBody)
	return
}

type ListChunkArgs struct {
	DiskID proto.DiskID `json:"diskid"`
}

type ListChunkRet struct {
	ChunkInfos []*clustermgr.ChunkInfo `json:"chunk_infos"`
}

func (c *client) ListChunks(ctx context.Context, host string, args *ListChunkArgs) (ret []*clustermgr.ChunkInfo, err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/chunk/list/diskid/%v", host, args.DiskID)

	listRet := &ListChunkRet{}
	err = c.GetWith(ctx, urlStr, listRet)
	if err != nil {
		return nil, err
	}

	return listRet.ChunkInfos, nil
}

type CompactChunkArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
}

type DiskProbeArgs struct {
	Path string `json:"path"`
}

type BadShard struct {
	DiskID proto.DiskID
	Vuid   proto.Vuid
	Bid    proto.BlobID
}
