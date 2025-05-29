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
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	ShardListPageLimit = 65536
)

/*
 *  method:         GET
 *  url:            /shard/get/diskid/{diskid}/vuid/{vuid}/bid/{bid}?iotype={iotype}
 *  response body:  bidData
 */
func (s *Service) ShardGet(c *rpc.Context) {
	args := new(bnapi.GetShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx, w := c.Request.Context(), c.Writer
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	// parse range bytes
	var (
		from, to    int64
		err         error
		written     int64
		wroteHeader bool
	)
	rangeBytesStr := c.Request.Header.Get("Range")
	if rangeBytesStr != "" {
		// [start, end]
		from, to, err = base.ParseHttpRangeStr(rangeBytesStr)
		if err != nil {
			c.RespondError(err)
			return
		}
	}

	if !args.Type.IsValid() {
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if !ds.IsWritable() { // not normal disk, skip
		c.RespondError(bloberr.ErrDiskBroken)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	// set io type
	convertOldIOType(&args.Type)
	ctx = bnapi.SetIoType(ctx, args.Type)
	qosLmt, exist := ds.GetIoQos().GetQueueQos(ctx)
	if !exist {
		span.Errorf("fail to get qos mgr, disk:%d", cs.Disk().ID())
		c.RespondError(bloberr.ErrInternal)
		return
	}

	if err = qosLmt.AcquireBid(uint64(args.Bid)); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.ReleaseBid(uint64(args.Bid))
	if err = qosLmt.Acquire(); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.Release()

	// build shard reader
	shard := core.NewShardReader(args.Bid, args.Vuid, from, to, w)

	shard.PrepareHook = func(shard *core.Shard) {
		// set crc to header
		// build http response header
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Transfer-Encoding", "binary")

		from, to := shard.From, shard.To
		bodySize := int64(shard.Size)

		if rangeBytesStr != "" {
			bodySize = to - from
			if bodySize == int64(shard.Size) {
				w.Header().Set("CRC", strconv.FormatUint(uint64(shard.Crc), 10))
			}
			rangeResp := "bytes " + strconv.FormatInt(from, 10) + "-" + strconv.FormatInt(to-1, 10) + "/" + strconv.FormatInt(int64(shard.Size), 10)
			w.Header().Set("Content-Length", strconv.FormatInt(int64(bodySize), 10))
			w.Header().Set("Content-Range", rangeResp)
			c.RespondStatus(http.StatusPartialContent)
		} else {
			w.Header().Set("CRC", strconv.FormatUint(uint64(shard.Crc), 10))
			w.Header().Set("Content-Length", strconv.FormatInt(bodySize, 10))
			c.RespondStatus(http.StatusOK)
		}

		wroteHeader = true

		// flush header, First byte optimization
		c.Flush()
	}

	if rangeBytesStr != "" {
		// [from, to)
		written, err = cs.RangeRead(ctx, shard)
	} else {
		written, err = cs.Read(ctx, shard)
	}

	if err != nil {
		span.Errorf("Failed read. args:%v err:%v, written:%v", args, err, written)
		if isShardErr(err) {
			s.inspectMgr.reportBadShard(ctx, cs, args.Bid, err)
		}
		if !wroteHeader {
			err = handlerBidNotFoundErr(err)
			c.RespondError(err)
		}
		return
	}

	s.reportGetTraffic(args.Type, written)
}

func (s *Service) ShardsGet(c *rpc.Context) {
	args := new(bnapi.GetShardsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, w := c.Request.Context(), c.Writer
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}
	if !args.Type.IsValid() {
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}
	var (
		written     int64
		wroteHeader bool
	)

	// set io type
	convertOldIOType(&args.Type)
	ctx = bnapi.SetIoType(ctx, args.Type)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if !ds.IsWritable() { // not normal disk, skip
		c.RespondError(bloberr.ErrDiskBroken)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	// build shard reader
	shard, err := core.NewBatchShardReader(args.Bids, args.Vuid, w, ds.GetConfig().BatchBufferSize)
	if err != nil {
		c.RespondError(err)
		return
	}

	shard.PrepareHook = func(shard *core.BatchShard) {
		// build http response header
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Transfer-Encoding", "binary")

		bodySize := shard.Size

		w.Header().Set("Content-Length", strconv.FormatInt(bodySize, 10))
		c.RespondStatus(http.StatusOK)

		wroteHeader = true

		// flush header, First byte optimization
		c.Flush()
	}

	written, err = cs.BatchRead(ctx, shard)
	if err != nil {
		span.Errorf("Failed batch read. args:%v err:%v, written:%v", args, err, written)
		if !wroteHeader {
			err = handlerBidNotFoundErr(err)
			c.RespondError(err)
		}
		return
	}
	s.reportGetTraffic(args.Type, written)
}

/*
 *  method:         GET
 *  url:            /shard/list/diskid/{diskid}/vuid/{vuid}/startbid/{bid}/status/{status}/count/{count}
 *  response body:  Marshal([]*bnapi.ShardInfo)
 */
func (s *Service) ShardList(c *rpc.Context) {
	args := new(bnapi.ListShardsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("args: %v", args)

	if args.Count <= 0 {
		args.Count = ShardListPageLimit
	}
	if args.Count > ShardListPageLimit {
		c.RespondError(bloberr.ErrShardListExceedLimit)
		return
	}
	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskid:%v not exist", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if !ds.IsWritable() {
		c.RespondError(bloberr.ErrDiskBroken)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("vuid:%d not exist, diskID:%d", args.Vuid, args.DiskID)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	sis, next, err := cs.ListShards(ctx, args.StartBid, args.Count, args.Status)
	if err != nil {
		span.Errorf("Failed list shard. args:%+v err:%v", args, err)
		c.RespondError(err)
		return
	}
	ret := bnapi.ListShardsRet{
		ShardInfos: sis,
		Next:       next,
	}
	c.RespondJSON(ret)
}

/*
 *  method:         GET
 *  url:            /shard/stat/diskid/{diskid}/vuid/{vuidValue}/bid/{bidValue}?iotype={iotype}
 *  response body:  json.Marshal(ShardMeta)
 */
func (s *Service) ShardStat(c *rpc.Context) {
	args := new(bnapi.StatShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskid:%v not exist", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if !ds.IsWritable() {
		c.RespondError(bloberr.ErrDiskBroken)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("vuid<%d> not exist.args: %v", args.Vuid, args)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	convertOldIOType(&args.Type)
	ctx = bnapi.SetIoType(ctx, args.Type)
	qosLmt, exist := ds.GetIoQos().GetQueueQos(ctx)
	if !exist {
		span.Errorf("fail to get qos mgr, disk:%d", cs.Disk().ID())
		c.RespondError(bloberr.ErrInternal)
		return
	}
	if err := qosLmt.Acquire(); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.Release()

	sm, err := cs.ReadShardMeta(ctx, args.Bid)
	if err != nil {
		err = handlerBidNotFoundErr(err)
		span.Errorf("Failed Get stat: args:%+v, err:%v", args, err)
		c.RespondError(err)
		return
	}

	stat := bnapi.ShardInfo{
		Vuid:   args.Vuid,
		Bid:    args.Bid,
		Size:   int64(sm.Size),
		Crc:    sm.Crc,
		Flag:   sm.Flag,
		Inline: sm.Inline,
		Offset: sm.Offset,

		NopData: sm.NopData,
	}
	c.RespondJSON(stat)
}

/*
 *  method:         POST
 *  url:            /shard/markdelete/diskid/{diskid}/vuid/{vuid}/bid/{bid}
 *  request body:   json.Marshal(deleteArgs)
 */
func (s *Service) ShardMarkdelete(c *rpc.Context) {
	args := new(bnapi.DeleteShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if !ds.IsWritable() { // not normal disk, skip
		c.RespondError(bloberr.ErrDiskBroken)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	ctx = bnapi.SetIoType(ctx, bnapi.DeleteIO)
	qosLmt, exist := ds.GetIoQos().GetQueueQos(ctx)
	if !exist {
		span.Errorf("fail to get qos mgr, disk:%d", cs.Disk().ID())
		c.RespondError(bloberr.ErrInternal)
		return
	}

	if err := qosLmt.AcquireBid(uint64(args.Bid)); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.ReleaseBid(uint64(args.Bid))

	if err := qosLmt.Acquire(); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.Release()

	err := cs.MarkDelete(ctx, args.Bid)
	if err != nil {
		err = handlerBidNotFoundErr(err)
		span.Errorf("Failed to mark delete, args:%+v err:%v", args, err)
		c.RespondError(err)
		return
	}
}

/*
 *  method:         POST
 *  url:            /shard/delete/diskid/{diskid}/vuid/{vuid}/bid/{bid}
 *  request body:   json.Marshal(deleteArgs)
 */
func (s *Service) ShardDelete(c *rpc.Context) {
	args := new(bnapi.DeleteShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if !ds.IsWritable() { // not normal disk, skip
		c.RespondError(bloberr.ErrDiskBroken)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	err := cs.AllowModify()
	if err != nil {
		span.Warnf("ChunkStorage can not delete: %v", err)
		c.RespondError(err)
		return
	}

	// set io type
	ctx = bnapi.SetIoType(ctx, bnapi.DeleteIO)
	qosLmt, exist := ds.GetIoQos().GetQueueQos(ctx)
	if !exist {
		span.Errorf("fail to get qos mgr, disk:%d", cs.Disk().ID())
		c.RespondError(bloberr.ErrInternal)
		return
	}

	if err = qosLmt.AcquireBid(uint64(args.Bid)); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.ReleaseBid(uint64(args.Bid))

	if err = qosLmt.Acquire(); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.Release()

	err = cs.Delete(ctx, args.Bid)
	if err != nil {
		err = handlerBidNotFoundErr(err)
		span.Errorf("Failed to delete, args:%+v, err:%v", args, err)
		c.RespondError(err)
		return
	}
}

/*
 *  method:         POST
 *  url:            /shard/put/diskid/{diskid}/vuid/{vuid}/bid/{bid}/size/{size}?iotype={iotype}&nopdata=false
 *  request body:   bidData
 */
func (s *Service) ShardPut(c *rpc.Context) {
	args := new(bnapi.PutShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	ret := &bnapi.PutShardRet{
		Crc: proto.InvalidCrc32,
	}

	if !bnapi.IsValidDiskID(args.DiskID) {
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	if args.Size > math.MaxUint32 {
		c.RespondError(bloberr.ErrShardSizeTooLarge)
		return
	}

	if args.Bid == proto.InValidBlobID {
		c.RespondError(bloberr.ErrShardInvalidBid)
		return
	}

	if !args.Type.IsValid() {
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	// set io type
	ctx = bnapi.SetIoType(ctx, args.Type)
	qosLmt, exist := ds.GetIoQos().GetQueueQos(ctx)
	if !exist {
		span.Errorf("fail to get qos mgr, disk:%d", cs.Disk().ID())
		c.RespondError(bloberr.ErrInternal)
		return
	}
	if err := qosLmt.Acquire(); err != nil {
		c.RespondError(err)
		return
	}
	defer qosLmt.Release()

	err := cs.AllowModify()
	if err != nil {
		span.Errorf("cs status check Invalid. err: %v", err)
		c.RespondError(err)
		return
	}

	if !args.NopData && !cs.HasEnoughSpace(args.Size) {
		span.Errorf("cs has no enougn space. args:%v, chunk info:%v, disk:%v",
			args, cs.ChunkInfo(ctx), cs.Disk().Stats())
		c.RespondError(bloberr.ErrChunkNoSpace)
		return
	}

	shard := core.NewShardWriter(args.Bid, args.Vuid, uint32(args.Size), c.Request.Body)
	shard.NopData = args.NopData

	start := time.Now()
	err = cs.Write(ctx, shard)
	span.AppendTrackLog("disk.put", start, err)
	if err != nil {
		span.Errorf("Failed to put shard, args: %+v, err: %v", args, err)
		c.RespondError(err)
		return
	}
	ret.Crc = shard.Crc

	if !shard.Inline && !shard.NopData {
		start = time.Now()
		err = cs.SyncData(ctx)
		span.AppendTrackLog("sync", start, err)
		if err != nil {
			span.Errorf("Failed to sync shard, args: %+v, err: %v", args, err)
			c.RespondError(err)
			return
		}
	}

	s.reportPutTraffic(args.Type, args.Size)
	c.RespondJSON(ret)
}

func handlerBidNotFoundErr(err error) error {
	if os.IsNotExist(err) {
		return bloberr.ErrNoSuchBid
	}
	return err
}

func isShardErr(err error) bool {
	if err == crc32block.ErrMismatchedCrc || strings.Contains(err.Error(), "block checksum mismatch") {
		return true
	}
	return false
}

// todo: will remove this function after all the old version nodes are upgraded.
// for compatibility with previous versions io type
func convertOldIOType(iot *bnapi.IOType) {
	if *iot == bnapi.WriteIO {
		*iot = bnapi.ReadIO
	}
}
