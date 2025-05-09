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
	"math"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	MaxShardSize = math.MaxUint32
)

type ShardInfo struct {
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
	Size   int64        `json:"size"`
	Crc    uint32       `json:"crc"`
	Flag   ShardStatus  `json:"flag"` // 1:normal,2:markDelete
	Inline bool         `json:"inline"`
}

type ShardStatus uint8

const (
	ShardStatusDefault    ShardStatus = 0x0 // 0 ; 0000
	ShardStatusNormal     ShardStatus = 0x1 // 1 ; 0001
	ShardStatusMarkDelete ShardStatus = 0x2 // 2 ; 0010
)

const (
	ShardDataInline = 0x80 // 1000 0000
)

type PutShardArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
	Size   int64        `json:"size"`
	Type   IOType       `json:"iotype,omitempty"`
	Body   io.Reader    `json:"-"`
}

type PutShardRet struct {
	Crc uint32 `json:"crc"`
}

func (c *client) PutShard(ctx context.Context, host string, args *PutShardArgs) (crc uint32, err error) {
	if args.Size > MaxShardSize {
		err = bloberr.ErrShardSizeTooLarge
		return
	}
	if !args.Type.IsValid() {
		err = bloberr.ErrInvalidParam
		return
	}

	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	ret := &PutShardRet{
		Crc: proto.InvalidCrc32,
	}
	urlStr := fmt.Sprintf("%v/shard/put/diskid/%v/vuid/%v/bid/%v/size/%v?iotype=%d",
		host, args.DiskID, args.Vuid, args.Bid, args.Size, args.Type)
	req, err := http.NewRequest(http.MethodPost, urlStr, args.Body)
	if err != nil {
		err = convertEIO(err)
		return
	}
	req.ContentLength = args.Size
	err = c.DoWith(ctx, req, ret, rpc.WithCrcEncode())
	if err == nil {
		crc = ret.Crc
	}

	return
}

type GetShardArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
	Type   IOType       `json:"iotype,omitempty"`
}

func (c *client) GetShard(ctx context.Context, host string, args *GetShardArgs) (
	body io.ReadCloser, shardCrc uint32, err error,
) {
	if !args.Type.IsValid() {
		err = bloberr.ErrInvalidParam
		return
	}

	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v?iotype=%d",
		host, args.DiskID, args.Vuid, args.Bid, args.Type)

	resp, err := c.Get(ctx, urlStr)
	if err != nil {
		err = convertEIO(err)
		return nil, 0, err
	}

	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		err = rpc.ParseResponseErr(resp)
		return
	}

	if resp.Header.Get("CRC") != "" {
		crc, err := strconv.ParseUint(resp.Header.Get("CRC"), 10, 32)
		if err != nil {
			return nil, proto.InvalidCrc32, err
		}
		shardCrc = uint32(crc)
	}

	return resp.Body, shardCrc, nil
}

type RangeGetShardArgs struct {
	GetShardArgs
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

func (c *client) RangeGetShard(ctx context.Context, host string, args *RangeGetShardArgs) (
	body io.ReadCloser, shardCrc uint32, err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	if !args.Type.IsValid() {
		err = bloberr.ErrInvalidParam
		return
	}

	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/shard/get/diskid/%v/vuid/%v/bid/%v?iotype=%d",
		host, args.DiskID, args.Vuid, args.Bid, args.Type)

	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		err = convertEIO(err)
		span.Errorf("Failed new req. urlStr:%s, err:%v", urlStr, err)
		return
	}

	// set http range header
	from, to := args.Offset, args.Size+args.Offset
	rangeStr := fmt.Sprintf("bytes=%v-%v", from, to-1)
	req.Header.Set("Range", rangeStr)

	resp, err := c.Do(ctx, req)
	if err != nil {
		span.Errorf("Failed get body, err:%v", err)
		return
	}

	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		err = rpc.ParseResponseErr(resp)
		return
	}

	if resp.Header.Get("CRC") != "" {
		crc, err := strconv.ParseUint(resp.Header.Get("CRC"), 10, 32)
		if err != nil {
			return nil, proto.InvalidCrc32, err
		}
		shardCrc = uint32(crc)
	}

	return resp.Body, shardCrc, nil
}

type DeleteShardArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
}

func (c *client) MarkDeleteShard(ctx context.Context, host string, args *DeleteShardArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/shard/markdelete/diskid/%v/vuid/%v/bid/%v", host, args.DiskID, args.Vuid, args.Bid)
	err = c.PostWith(ctx, urlStr, nil, rpc.NoneBody)
	return
}

func (c *client) DeleteShard(ctx context.Context, host string, args *DeleteShardArgs) (err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/shard/delete/diskid/%v/vuid/%v/bid/%v", host, args.DiskID, args.Vuid, args.Bid)
	err = c.PostWith(ctx, urlStr, nil, rpc.NoneBody)
	return
}

type StatShardArgs struct {
	DiskID proto.DiskID `json:"diskid"`
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
}

func (c *client) StatShard(ctx context.Context, host string, args *StatShardArgs) (si *ShardInfo, err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/shard/stat/diskid/%v/vuid/%v/bid/%v",
		host, args.DiskID, args.Vuid, args.Bid)
	si = &ShardInfo{}
	err = c.GetWith(ctx, urlStr, si)
	return
}

type ListShardsArgs struct {
	DiskID   proto.DiskID `json:"diskid"`
	Vuid     proto.Vuid   `json:"vuid" `
	StartBid proto.BlobID `json:"startbid"`
	Status   ShardStatus  `json:"status" `
	Count    int          `json:"count" `
}

type ListShardsRet struct {
	ShardInfos []*ShardInfo `json:"shard_infos"`
	Next       proto.BlobID `json:"next"`
}

func (c *client) ListShards(ctx context.Context, host string, args *ListShardsArgs) (sis []*ShardInfo, next proto.BlobID, err error) {
	if !IsValidDiskID(args.DiskID) {
		err = bloberr.ErrInvalidDiskId
		return
	}

	urlStr := fmt.Sprintf("%v/shard/list/diskid/%v/vuid/%v/startbid/%v/status/%v/count/%v",
		host, args.DiskID, args.Vuid, args.StartBid, args.Status, args.Count)

	listRet := ListShardsRet{}
	err = c.GetWith(ctx, urlStr, &listRet)
	if err != nil {
		return nil, proto.InValidBlobID, err
	}

	return listRet.ShardInfos, listRet.Next, nil
}

func convertEIO(err error) error {
	if base.IsEIO(err) {
		return bloberr.ErrDiskBroken
	}

	return err
}
