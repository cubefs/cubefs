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
	"io"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type Config struct {
	rpc.Config
}

type client struct {
	rpc.Client
}

func New(cfg *Config) StorageAPI {
	return &client{rpc.NewClient(&cfg.Config)}
}

func (c *client) String(ctx context.Context, host string) string {
	return ""
}

func (c *client) IsOnline(ctx context.Context, host string) (b bool) {
	return true
}

func (c *client) Close(ctx context.Context, host string) (err error) {
	return nil
}

func (c *client) Stat(ctx context.Context, host string) (dis []*clustermgr.BlobNodeDiskInfo, err error) {
	urlStr := fmt.Sprintf("%v/stat", host)
	dis = make([]*clustermgr.BlobNodeDiskInfo, 0)
	err = c.GetWith(ctx, urlStr, &dis)
	return
}

type ConfigReloadArgs struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type InspectRateArgs struct {
	Rate int `json:"rate"`
}

type DiskStatArgs struct {
	DiskID proto.DiskID `json:"diskid"`
}

func (c *client) DiskInfo(ctx context.Context, host string, args *DiskStatArgs) (di *clustermgr.BlobNodeDiskInfo, err error) {
	if !IsValidDiskID(args.DiskID) {
		return nil, errors.ErrInvalidDiskId
	}

	urlStr := fmt.Sprintf("%v/disk/stat/diskid/%v", host, args.DiskID)
	di = new(clustermgr.BlobNodeDiskInfo)
	err = c.GetWith(ctx, urlStr, di)
	return
}

type StorageAPI interface {
	String(ctx context.Context, host string) string
	IsOnline(ctx context.Context, host string) bool
	Close(ctx context.Context, host string) error
	Stat(ctx context.Context, host string) (infos []*clustermgr.BlobNodeDiskInfo, err error)
	DiskInfo(ctx context.Context, host string, args *DiskStatArgs) (di *clustermgr.BlobNodeDiskInfo, err error)

	// chunks
	CreateChunk(ctx context.Context, host string, args *CreateChunkArgs) (err error)
	StatChunk(ctx context.Context, host string, args *StatChunkArgs) (ci *clustermgr.ChunkInfo, err error)
	ReleaseChunk(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error)
	SetChunkReadonly(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error)
	SetChunkReadwrite(ctx context.Context, host string, args *ChangeChunkStatusArgs) (err error)
	ListChunks(ctx context.Context, host string, args *ListChunkArgs) (cis []*clustermgr.ChunkInfo, err error)

	// shard
	GetShard(ctx context.Context, host string, args *GetShardArgs) (body io.ReadCloser, shardCrc uint32, err error)
	RangeGetShard(ctx context.Context, host string, args *RangeGetShardArgs) (body io.ReadCloser, shardCrc uint32, err error)
	PutShard(ctx context.Context, host string, args *PutShardArgs) (crc uint32, err error)
	StatShard(ctx context.Context, host string, args *StatShardArgs) (si *ShardInfo, err error)
	MarkDeleteShard(ctx context.Context, host string, args *DeleteShardArgs) (err error)
	DeleteShard(ctx context.Context, host string, args *DeleteShardArgs) (err error)
	ListShards(ctx context.Context, host string, args *ListShardsArgs) (sis []*ShardInfo, next proto.BlobID, err error)

	WorkerAPI
}

// WorkerAPI woker api on blobnode.
type WorkerAPI interface {
	RepairShard(ctx context.Context, host string, args *proto.ShardRepairTask) (err error)
	WorkerStats(ctx context.Context, host string) (ret WorkerStats, err error)
}
