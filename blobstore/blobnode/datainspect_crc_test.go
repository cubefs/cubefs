// Copyright 2026 The CubeFS Authors.
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
	"hash/crc32"
	"net/http"
	"os"
	"syscall"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func shardHeader(code int) []byte {
	var hdr bnapi.ShardsHeader
	hdr.Set(code)
	return hdr[:]
}

func TestBatchCRCWriter(t *testing.T) {
	payload := []byte("0123456789")
	badPayload := []byte("abcdefghij")

	t.Run("single shard ok", func(t *testing.T) {
		w := newBatchCRCWriter([]*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: int64(len(payload)), Crc: crc32.ChecksumIEEE(payload)},
		})
		w.Write(shardHeader(http.StatusOK))
		w.Write(payload)
		require.Empty(t, w.badBids)
		require.Equal(t, 1, w.idx)
	})

	t.Run("single shard crc mismatch", func(t *testing.T) {
		w := newBatchCRCWriter([]*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: int64(len(payload)), Crc: crc32.ChecksumIEEE(payload) ^ 1},
		})
		w.Write(shardHeader(http.StatusOK))
		w.Write(payload)
		require.Equal(t, []proto.BlobID{1}, w.badBids)
	})

	t.Run("multi shard payload split across writes", func(t *testing.T) {
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: int64(len(payload)), Crc: crc32.ChecksumIEEE(payload)},
			// second shard's payload differs from its declared crc -> bad bid
			{Bid: 2, Vuid: proto.Vuid(1001), Size: int64(len(badPayload)), Crc: crc32.ChecksumIEEE(badPayload) ^ 1},
			// third shard good: hasher must be reset between shards
			{Bid: 3, Vuid: proto.Vuid(1001), Size: int64(len(payload)), Crc: crc32.ChecksumIEEE(payload)},
		}
		w := newBatchCRCWriter(shards)

		w.Write(shardHeader(http.StatusOK))
		w.Write(payload[:4])
		w.Write(payload[4:])

		w.Write(shardHeader(http.StatusOK))
		w.Write(badPayload)

		w.Write(shardHeader(http.StatusOK))
		w.Write(payload)

		require.Equal(t, []proto.BlobID{2}, w.badBids)
		require.Equal(t, 3, w.idx)
	})

	t.Run("error header marks failure point without advancing", func(t *testing.T) {
		w := newBatchCRCWriter([]*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 10, Crc: crc32.ChecksumIEEE(payload)},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 10, Crc: crc32.ChecksumIEEE(payload)},
		})
		// BatchReader writes the error header and then stops (ErrBidNotMatch),
		// so the writer must keep idx on the failing shard for handleBidNotMatch.
		w.Write(shardHeader(http.StatusInternalServerError))
		require.Equal(t, 0, w.idx)
		require.Empty(t, w.badBids)
	})

	t.Run("writes after all shards consumed are ignored", func(t *testing.T) {
		w := newBatchCRCWriter([]*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: int64(len(payload)), Crc: crc32.ChecksumIEEE(payload)},
		})
		w.Write(shardHeader(http.StatusOK))
		w.Write(payload)
		require.Len(t, w.badBids, 0)

		// trailing writes must not panic or corrupt the result
		w.Write(shardHeader(http.StatusOK))
		w.Write(payload)
		require.Len(t, w.badBids, 0)
	})
}

func TestSplitIntoBatches(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		require.Empty(t, splitIntoBatches(nil, 1<<20))
	})

	t.Run("filters nop inline zero-size and sorts by offset", func(t *testing.T) {
		shards := []*bnapi.ShardInfo{
			{Bid: 300, Offset: 300, Size: 100},
			{Bid: 1, NopData: true, Offset: 0, Size: 100},
			{Bid: 2, Inline: true, Offset: 1, Size: 100},
			{Bid: 3, Offset: 2, Size: 0},
			{Bid: 100, Offset: 100, Size: 100},
			{Bid: 200, Offset: 200, Size: 100},
		}
		batches := splitIntoBatches(shards, 150)
		require.Len(t, batches, 3)
		require.Equal(t, []proto.BlobID{100}, batchBids(batches[0]))
		require.Equal(t, []proto.BlobID{200}, batchBids(batches[1]))
		require.Equal(t, []proto.BlobID{300}, batchBids(batches[2]))
	})

	t.Run("single shard larger than max size", func(t *testing.T) {
		shards := []*bnapi.ShardInfo{{Bid: 1, Offset: 0, Size: 1000}}
		batches := splitIntoBatches(shards, 100)
		require.Len(t, batches, 1)
		require.Len(t, batches[0], 1)
	})

	t.Run("fits in one batch", func(t *testing.T) {
		shards := []*bnapi.ShardInfo{
			{Bid: 2, Offset: 100, Size: 100},
			{Bid: 1, Offset: 0, Size: 100},
		}
		batches := splitIntoBatches(shards, 1<<20)
		require.Len(t, batches, 1)
		require.Equal(t, []proto.BlobID{1, 2}, batchBids(batches[0]))
	})
}

func batchBids(batch []*bnapi.ShardInfo) []proto.BlobID {
	bids := make([]proto.BlobID, 0, len(batch))
	for _, si := range batch {
		bids = append(bids, si.Bid)
	}
	return bids
}

func TestInspectShard(t *testing.T) {
	ctx := context.Background()
	vuid := proto.Vuid(1001)
	si := &bnapi.ShardInfo{Bid: 1, Vuid: vuid, Size: 100, Crc: 1}
	mgr := &DataInspectMgr{}

	t.Run("read ok", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(vuid).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(100), nil)
		require.NoError(t, mgr.inspectShard(ctx, cs, si))
	})

	t.Run("shard deleted during read", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(vuid).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(0), os.ErrNotExist)
		require.NoError(t, mgr.inspectShard(ctx, cs, si))
	})

	t.Run("overwritten empty shard", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(vuid).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(1)).Return(&core.ShardMeta{Size: 0}, nil)
		require.NoError(t, mgr.inspectShard(ctx, cs, si))
	})

	t.Run("meta deleted during double check", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(vuid).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(1)).Return(nil, os.ErrNotExist)
		require.NoError(t, mgr.inspectShard(ctx, cs, si))
	})

	t.Run("read error with live meta keeps error", func(t *testing.T) {
		ctr := gomock.NewController(t)
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(vuid).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(1)).Return(&core.ShardMeta{Size: 100}, nil)
		require.ErrorIs(t, mgr.inspectShard(ctx, cs, si), errMock)
	})
}

func TestFallbackInspectShards(t *testing.T) {
	ctx := context.Background()
	mgr := &DataInspectMgr{}

	t.Run("healthy shards no bads", func(t *testing.T) {
		ctr := gomock.NewController(t)
		ds := NewMockDiskAPI(ctr)
		cs := NewMockChunkAPI(ctr)
		ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(100), nil).Times(2)
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		}
		bads, err := mgr.fallbackInspectShards(ctx, cs, ds, shards)
		require.NoError(t, err)
		require.Empty(t, bads)
	})

	t.Run("non-eio errors collected and scan continues", func(t *testing.T) {
		ctr := gomock.NewController(t)
		ds := NewMockDiskAPI(ctr)
		cs := NewMockChunkAPI(ctr)
		ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(0), errMock).Times(2)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 100}, nil).Times(2)
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		}
		bads, err := mgr.fallbackInspectShards(ctx, cs, ds, shards)
		require.NoError(t, err)
		require.Len(t, bads, 2)
		require.Equal(t, proto.DiskID(11), bads[0].DiskID)
		require.Equal(t, proto.BlobID(1), bads[0].Bid)
		require.Equal(t, proto.BlobID(2), bads[1].Bid)
	})

	t.Run("eio stops immediately", func(t *testing.T) {
		ctr := gomock.NewController(t)
		ds := NewMockDiskAPI(ctr)
		cs := NewMockChunkAPI(ctr)
		ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().Read(any, any).Return(int64(0), errMock).Times(1)
		cs.EXPECT().Read(any, any).Return(int64(0), &os.PathError{Op: "read", Path: "data", Err: syscall.EIO}).Times(1)
		// inspectShard always re-checks shard meta after a failed data read,
		// including for EIO, before the fallback path aborts.
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 100}, nil).Times(2)
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		}
		bads, err := mgr.fallbackInspectShards(ctx, cs, ds, shards)
		require.Error(t, err)
		require.ErrorIs(t, err, syscall.EIO)
		require.Len(t, bads, 1)
		require.Equal(t, proto.BlobID(1), bads[0].Bid)
	})
}

func TestHandleBidNotMatch(t *testing.T) {
	ctx := context.Background()
	mgr := &DataInspectMgr{}
	// burst must be > 0: the inlined rate-limit loop in inspectBatch never
	// consumes tokens when the limiter's burst is zero
	lmt := rate.NewLimiter(rate.Inf, 1<<20)

	t.Run("reuse crc results and re-inspect failing shard", func(t *testing.T) {
		ctr := gomock.NewController(t)
		ds := NewMockDiskAPI(ctr)
		cs := NewMockChunkAPI(ctr)

		ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		// bid1 was confirmed bad before the header mismatch; bid2 is the
		// failing shard; both stay corrupted on re-inspection.
		cs.EXPECT().Read(any, any).Return(int64(0), errMock).Times(2)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 100}, nil).Times(2)

		crcWriter := newBatchCRCWriter([]*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		})
		crcWriter.badBids = []proto.BlobID{1}
		crcWriter.idx = 1 // shards[1] header mismatched

		shards := []*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		}
		bads, err := mgr.handleBidNotMatch(ctx, cs, ds, shards, crcWriter, lmt)
		require.NoError(t, err)
		require.Len(t, bads, 2)
		require.Equal(t, proto.BlobID(1), bads[0].Bid)
		require.Equal(t, proto.BlobID(2), bads[1].Bid)
	})

	t.Run("first shard fails and rest continue via batch", func(t *testing.T) {
		ctr := gomock.NewController(t)
		ds := NewMockDiskAPI(ctr)
		cs := NewMockChunkAPI(ctr)

		ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		ds.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1 << 20}}).AnyTimes()
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		// failing shard recovers on per-shard re-inspection
		cs.EXPECT().Read(any, any).Return(int64(0), nil).Times(1)
		// continuation batch reads cleanly
		cs.EXPECT().BatchRead(any, any).Return(int64(0), nil).Times(1)

		crcWriter := newBatchCRCWriter([]*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		})
		crcWriter.idx = 0 // shards[0] header mismatched

		shards := []*bnapi.ShardInfo{
			{Bid: 1, Vuid: proto.Vuid(1001), Size: 100},
			{Bid: 2, Vuid: proto.Vuid(1001), Size: 100},
		}
		bads, err := mgr.handleBidNotMatch(ctx, cs, ds, shards, crcWriter, lmt)
		require.NoError(t, err)
		require.Empty(t, bads)
	})
}
