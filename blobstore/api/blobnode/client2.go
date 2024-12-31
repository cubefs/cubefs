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

package blobnode

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const BlockSizeV2 = 32 << 10

// TODO: using protobuf
type (
	PutShardArgsV2 = rpc2.AnyCodec[PutShardArgs]
	PutShardRetV2  = rpc2.AnyCodec[PutShardRet]
	GetShardArgsV2 = rpc2.AnyCodec[GetShardArgs]
)

type Storage2API interface {
	GetShard(ctx context.Context, host string, args *GetShardArgs) (body io.ReadCloser, shardCrc uint32, err error)
	RangeGetShard(ctx context.Context, host string, args *RangeGetShardArgs) (body io.ReadCloser, shardCrc uint32, err error)
	PutShard(ctx context.Context, host string, args *PutShardArgs) (crc uint32, err error)
}

type client2 struct {
	rpc2.Client
}

func New2(config rpc2.Client) Storage2API {
	defaulter.Empty(&config.ConnectorConfig.Network, "tcp")
	defaulter.IntegerLessOrEqual(&config.ConnectorConfig.DialTimeout.Duration, 200*time.Millisecond)
	return &client2{config}
}

func (c *client2) PutShard(ctx context.Context, host string, args *PutShardArgs) (crc uint32, err error) {
	rc, ok := args.Body.(io.ReadCloser)
	if !ok {
		rc = io.NopCloser(args.Body)
	}
	rc = crc32block.NewSizedCoder(rc, args.Size, args.Length, BlockSizeV2, crc32block.ModeEncode, false)
	cl, _ := crc32block.PartialEncodeSizeWith(args.Size, args.Length, BlockSizeV2)

	req, err := rpc2.NewRequest(ctx, host, "/v2/shard/put", &PutShardArgsV2{Value: *args}, rc)
	if err != nil {
		return
	}
	req.OptionBodyAlignedUpload()
	req.ContentLength = cl

	ret := new(PutShardRetV2)
	resp, err := c.Do(req, ret)
	if err != nil {
		return
	}
	resp.Body.Close()
	crc = ret.Value.Crc
	return
}

func (c *client2) GetShard(ctx context.Context, host string, args *GetShardArgs) (body io.ReadCloser, shardCrc uint32, err error) {
	return c.RangeGetShard(ctx, host, &RangeGetShardArgs{GetShardArgs: *args, Offset: -1, Size: -1})
}

func (c *client2) RangeGetShard(ctx context.Context, host string, args *RangeGetShardArgs) (body io.ReadCloser, shardCrc uint32, err error) {
	req, err := rpc2.NewRequest(ctx, host, "/v2/shard/get", nil, rpc2.Codec2Reader(&GetShardArgsV2{Value: args.GetShardArgs}))
	if err != nil {
		return
	}

	if args.Offset >= 0 && args.Size >= 0 {
		from, to := args.Offset, args.Size+args.Offset
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", from, to-1))
	}

	resp, err := c.Do(req, nil)
	if err != nil {
		return
	}

	if resp.Header.Get("CRC") != "" {
		var crc uint64
		crc, err = strconv.ParseUint(resp.Header.Get("CRC"), 10, 32)
		if err != nil {
			resp.Body.Close()
			return
		}
		shardCrc = uint32(crc)
	}
	return resp.Body, shardCrc, nil
}
