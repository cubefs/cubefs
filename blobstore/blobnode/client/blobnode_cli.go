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

package client

import (
	"context"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"io"
	"strings"

	api "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var defaultFirstStartBid = proto.BlobID(0)

// IBlobNode define the interface of blobnode used for worker
type IBlobNode interface {
	StatChunk(ctx context.Context, location proto.VunitLocation) (ci *ChunkInfo, err error)
	StatShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) (si *ShardInfo, err error)
	ListShards(ctx context.Context, location proto.VunitLocation) (shards []*ShardInfo, err error)
	GetShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, ioType api.IOType) (body io.ReadCloser, crc32 uint32, err error)
	PutShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, size int64, body io.Reader, ioType api.IOType) (err error)
}

type ShardStatusRet struct {
}

type ShardUnitInfo struct {
	Suid    proto.Suid   `json:"suid"`
	DiskID  proto.DiskID `json:"disk_id"`
	Learner bool         `json:"learner"`
}

type UpdateShardArgs struct {
	ShardID proto.ShardID    `json:"shard_id"`
	DiskID  proto.DiskID     `json:"disk_id"`
	Units   []*ShardUnitInfo `json:"units"`
}

type SubRange struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

type Range struct {
	RangeType sharding.RangeType `json:"range_type"`
	SubRanges []SubRange         `json:"sub_ranges"`
}

type AddShardArgs struct {
	DiskID  proto.DiskID  `json:"disk_id"`
	ShardID proto.ShardID `json:"shard_id"`
	Range   Range
	Units   []*ShardUnitInfo
	Epoch   uint64
}

// IShardNode ShardNode client
type IShardNode interface {
	UpdateShard(ctx context.Context, args *UpdateShardArgs) error
	GetShardStatus(ctx context.Context, id proto.ShardID) (error, *ShardStatusRet)
	AdShard(ctx context.Context, args *AddShardArgs) error
}

type ShardNodeClient struct {
}

func (c *ShardNodeClient) UpdateShard(ctx context.Context, args *UpdateShardArgs) error {
	return nil
}

func (c *ShardNodeClient) GetShardStatus(ctx context.Context, id proto.ShardID) (error, *ShardStatusRet) {
	return nil, nil
}
func (c *ShardNodeClient) AdShard(ctx context.Context, args *AddShardArgs) error {
	return nil
}

// BlobNodeClient blobnode client
type BlobNodeClient struct {
	cli api.StorageAPI
}

const (
	// ShardStatusNotExist shard not exist code
	ShardStatusNotExist = 100
)

// ChunkInfo chunk info
type ChunkInfo struct {
	clustermgr.ChunkInfo
}

// Locked return true if chunk is locked
func (c *ChunkInfo) Locked() bool {
	return c.ChunkInfo.Status == clustermgr.ChunkStatusReadOnly
}

// ShardInfo shard info
type ShardInfo struct {
	api.ShardInfo
}

// Normal return true if shard is normal
func (si *ShardInfo) Normal() bool {
	return si.Flag == api.ShardStatusNormal
}

// MarkDeleted return true if shard is mark delete
func (si *ShardInfo) MarkDeleted() bool {
	return si.Flag == api.ShardStatusMarkDelete
}

// NotExist returns true if shard is not exist
func (si *ShardInfo) NotExist() bool {
	return si.Flag == ShardStatusNotExist
}

// NewBlobNodeClient returns blobnode client
func NewBlobNodeClient(conf *api.Config) IBlobNode {
	return &BlobNodeClient{
		cli: api.New(conf),
	}
}

// StatChunk returns chunk stat
func (c *BlobNodeClient) StatChunk(ctx context.Context, location proto.VunitLocation) (ci *ChunkInfo, err error) {
	ctx = trace.NewContextFromContext(ctx)
	span := trace.SpanFromContext(ctx).WithOperation("StatChunk")
	info, err := c.cli.StatChunk(ctx, location.Host, &api.StatChunkArgs{DiskID: location.DiskID, Vuid: location.Vuid})
	if err != nil {
		span.Debugf("StatChunk failed: location[%+v], code[%d], err[%v]", location, rpc.DetectStatusCode(err), err)
		return
	}
	span.Debugf("StatChunk success: chunk info[%+v], location[%+v]", info, location)
	return &ChunkInfo{ChunkInfo: *info}, nil
}

// GetShard returns shard data
func (c *BlobNodeClient) GetShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, ioType api.IOType) (body io.ReadCloser, crc32 uint32, err error) {
	ctx = trace.NewContextFromContext(ctx)
	return c.cli.GetShard(ctx, location.Host, &api.GetShardArgs{DiskID: location.DiskID, Vuid: location.Vuid, Bid: bid, Type: ioType})
}

// StatShard return shard stat
func (c *BlobNodeClient) StatShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) (si *ShardInfo, err error) {
	ctx = trace.NewContextFromContext(ctx)
	span := trace.SpanFromContext(ctx).WithOperation("StatShard")
	info, err := c.cli.StatShard(ctx, location.Host, &api.StatShardArgs{DiskID: location.DiskID, Vuid: location.Vuid, Bid: bid})
	if err != nil {
		if errCode := rpc.DetectStatusCode(err); errCode == errcode.CodeBidNotFound {
			span.Debugf("StatShard not found and set flag ShardStatusNotExist: location[%+v], bid[%d]", location, bid)
			var info2 ShardInfo
			info2.Vuid = location.Vuid
			info2.Bid = bid
			info2.Flag = ShardStatusNotExist
			return &info2, nil
		}
		return nil, err
	}
	span.Debugf("StatShard success: location[%+v], bid[%d], shard info[%+v]", location, bid, info)
	return &ShardInfo{*info}, nil
}

// ListShards return shards info
func (c *BlobNodeClient) ListShards(ctx context.Context, location proto.VunitLocation) (sis []*ShardInfo, err error) {
	ctx = trace.NewContextFromContext(ctx)
	span := trace.SpanFromContext(ctx).WithOperation("ListShards")
	startBid := defaultFirstStartBid
	for {
		infos, next, err := c.cli.ListShards(ctx, location.Host, &api.ListShardsArgs{DiskID: location.DiskID, Vuid: location.Vuid, StartBid: startBid})
		if err != nil {
			span.Errorf("ListShards failed: location[%+v], StartBid[%d], code[%d], err[%+v]", location, startBid, rpc.DetectStatusCode(err), err)
			return nil, err
		}
		for _, info := range infos {
			sis = append(sis, &ShardInfo{*info})
		}
		startBid = next
		if startBid == defaultFirstStartBid {
			break
		}
	}
	span.Debugf("ListShards success: location[%+v], shards len[%d]", location, len(sis))
	return sis, nil
}

// PutShard put data to shard
func (c *BlobNodeClient) PutShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, size int64, body io.Reader, ioType api.IOType) (err error) {
	ctx = trace.NewContextFromContext(ctx)
	span := trace.SpanFromContext(ctx).WithOperation("PutShard")
	_, err = c.cli.PutShard(ctx, location.Host, &api.PutShardArgs{DiskID: location.DiskID, Vuid: location.Vuid, Bid: bid, Body: body, Size: size, Type: ioType})
	if err != nil {
		span.Errorf("PutShard failed: location[%+v], bid[%d], code[%d], err[%+v]", location, bid, rpc.DetectStatusCode(err), err)
		errMsg := err.Error()
		if strings.Contains(errMsg, "Timeout") || strings.Contains(errMsg, "timeout") {
			err = errcode.ErrPutShardTimeout
		}
	}
	return
}
