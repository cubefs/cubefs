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
	"fmt"
	"hash/crc32"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const _blockV2 = blobnode.BlockSizeV2

var traceOptAny = trace.OptSpanDurationAny

func (s *Service) ShardPutV2(w rpc2.ResponseWriter, req *rpc2.Request) error {
	argsAny := new(blobnode.PutShardArgsV2)
	if err := req.ParseParameter(argsAny); err != nil {
		return err
	}
	args := argsAny.Value

	ctx, span := req.Context(), req.Span().WithOperation("ShardPutV2")

	newSize, newPad := crc32block.PartialEncodeSizeWith(args.Size+args.Length, 0, _blockV2)
	oldSize, oldPad := crc32block.PartialEncodeSizeWith(args.Length, 0, _blockV2)
	sizeWithCrc := (newSize - newPad) - (oldSize - oldPad)
	if (oldSize-oldPad)%_blockV2 != 0 {
		sizeWithCrc += crc32.Size
	}
	span.Debugf("size:%d new(size:%d pad:%d) old(size:%d pad:%d) args: %+v",
		sizeWithCrc, newSize, newPad, oldSize, oldPad, args)
	if newSize > math.MaxUint32 {
		return errcode.ErrShardSizeTooLarge
	}
	if !blobnode.IsValidDiskID(args.DiskID) {
		return errcode.ErrInvalidDiskId
	}
	if args.Bid == proto.InValidBlobID {
		return errcode.ErrShardInvalidBid
	}
	if !args.Type.IsValid() {
		return errcode.ErrInvalidParam
	}

	convertIoType(&args.Type)
	ctx = blobnode.SetIoType(ctx, args.Type)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		return errcode.ErrNoSuchDisk
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		return errcode.ErrNoSuchVuid
	}
	err := cs.AllowModify()
	if err != nil {
		return err
	}
	if !cs.HasEnoughSpace(sizeWithCrc) {
		span.Errorf("cs has no enougn space. args:%v, chunk info:%v, disk:%v",
			args, cs.ChunkInfo(ctx), cs.Disk().Stats())
		return errcode.ErrChunkNoSpace
	}

	shard := core.NewShardWriter(args.Bid, args.Vuid, uint32(sizeWithCrc),
		crc32block.NewSizedCoder(req.Body, args.Size, args.Length, _blockV2, crc32block.ModeCheck, false),
	)

	start := time.Now()
	err = cs.Write(ctx, shard)
	span.AppendTrackLog("disk.put", start, err, traceOptAny())
	if err != nil {
		span.Errorf("disk put args: %+v, err: %v", args, err)
		return err
	}

	if !shard.Inline {
		start = time.Now()
		err = cs.SyncData(ctx)
		span.AppendTrackLog("sync", start, err, traceOptAny())
		if err != nil {
			span.Errorf("sync shard, args: %+v, err: %v", args, err)
			return err
		}
	}

	s.reportPutTraffic(args.Type, sizeWithCrc)

	var ret blobnode.PutShardRetV2
	ret.Value.Crc = shard.Crc
	return w.WriteOK(&ret)
}

func (s *Service) ShardGetV2(w rpc2.ResponseWriter, req *rpc2.Request) error {
	argsAny := new(blobnode.GetShardArgsV2)
	if err := req.ParseParameter(argsAny); err != nil {
		return err
	}
	args := argsAny.Value

	ctx, span := req.Context(), req.Span().WithOperation("ShardGetV2")
	span.Debugf("args: %+v", args)

	if !blobnode.IsValidDiskID(args.DiskID) {
		return errcode.ErrInvalidDiskId
	}
	if !args.Type.IsValid() {
		return errcode.ErrInvalidParam
	}

	var (
		shardSize   int64
		from, to    int64
		err         error
		written     int64
		wroteHeader bool
	)
	rangeBytesStr := req.Header.Get("Range")
	if rangeBytesStr != "" {
		// [start, end]
		from, to, err = base.ParseHttpRangeStr(rangeBytesStr)
		if err != nil {
			return err
		}
	} else {
		to = -1
	}
	convertIoType(&args.Type)
	ctx = blobnode.SetIoType(ctx, args.Type)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		return errcode.ErrNoSuchDisk
	}
	if !ds.IsWritable() {
		return errcode.ErrDiskBroken
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		return errcode.ErrNoSuchVuid
	}

	shard := core.NewShardReader(args.Bid, args.Vuid, from, to, nil)
	shard.Writer2 = w

	shard.PrepareHook = func(shard *core.Shard) {
		shardSize, from, to, _ = shard.RangedSize(from, to)
		w.Header().Set(rpc.HeaderContentType, rpc.MIMEStream)
		w.Header().Set("CRC", strconv.FormatUint(uint64(shard.Crc), 10))
		if rangeBytesStr != "" {
			w.Header().Set(rpc.HeaderContentRange, fmt.Sprintf("bytes %d-%d/%d", from, to-1, shardSize))
			w.SetContentLength(to - from)
			w.WriteHeader(http.StatusPartialContent, nil)
		} else {
			w.SetContentLength(shardSize)
			w.WriteHeader(http.StatusOK, nil)
		}

		wroteHeader = true
		w.Flush()
	}

	written, err = cs.RangeRead(ctx, shard)
	s.reportGetTraffic(args.Type, written)

	if err != nil {
		span.Errorf("read, args: %+v err: %v, written: %v", args, err, written)
		if isShardErr(err) {
			s.inspectMgr.reportBadShard(cs, args.Bid, err)
		}
		if !wroteHeader {
			err = handlerBidNotFoundErr(err)
		}
	}
	return err
}
