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
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// newScheduleTestMgr builds a manager with an enabled switch and a live service
// close channel, suitable for unit-testing the per-disk schedule functions.
func newScheduleTestMgr() *DataInspectMgr {
	return &DataInspectMgr{
		conf: DataInspectConf{
			BatchReadSize: core.DefaultInspectBatchReadSize,
			CycleDays:     core.DefaultInspectCycleDays,
			RateLimit:     minRateLimit,
		},
		taskSwitch: taskswitch.NewEnabledTaskSwitch(),
		svr:        &Service{closeCh: make(chan struct{})},
	}
}

func nopPage(bids ...proto.BlobID) []*bnapi.ShardInfo {
	page := make([]*bnapi.ShardInfo, 0, len(bids))
	for _, b := range bids {
		page = append(page, &bnapi.ShardInfo{Bid: b, Vuid: proto.Vuid(1001), NopData: true})
	}
	return page
}

// zeroSizeShards builds ListShards pages that skip CRC I/O (Size==0 filtered by
// splitIntoBatches), so schedule cursor bookkeeping can be unit-tested in isolation.
func zeroSizeShards(start, n int) []*bnapi.ShardInfo {
	out := make([]*bnapi.ShardInfo, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &bnapi.ShardInfo{Bid: proto.BlobID(start + i), Size: 0})
	}
	return out
}

// expectNopPage installs one ListShards page expectation. NopData shards are
// skipped by splitIntoBatches, so the scan flow never reaches BatchRead and the
// paging/cursor logic can be tested with a mock chunk.
func expectNopPage(cs *MockChunkAPI, startBid proto.BlobID, page []*bnapi.ShardInfo, next proto.BlobID) {
	cs.EXPECT().ListShards(any, startBid, any, bnapi.ShardStatusNormal).Return(page, next, nil).Times(1)
}

// expectScanDiskChunk wires the common Disk/Chunk expectations used by
// inspectScanWindow unit tests. inspectShardsPage always calls
// cs.Disk() even when the page is empty after NopData filtering.
func expectScanDiskChunk(ds *MockDiskAPI, cs *MockChunkAPI) {
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().IsClosing().Return(false).AnyTimes()
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
}

// expectNopPages installs ListShards expectations for ascending bids paged by
// pageSize, following the storage Scan semantics: each page is exclusive of its
// start bid and the last page reports next == InValidBlobID (EOF).
func expectNopPages(cs *MockChunkAPI, bids []proto.BlobID, pageSize int) {
	for start := 0; start < len(bids); start += pageSize {
		end := start + pageSize
		if end > len(bids) {
			end = len(bids)
		}
		var startBid proto.BlobID
		if start > 0 {
			startBid = bids[start-1]
		}
		next := proto.InValidBlobID
		if end < len(bids) {
			next = bids[end-1]
		}
		expectNopPage(cs, startBid, nopPage(bids[start:end]...), next)
	}
}

func bidsRange(from, to int) []proto.BlobID {
	bids := make([]proto.BlobID, 0, to-from+1)
	for i := from; i <= to; i++ {
		bids = append(bids, proto.BlobID(i))
	}
	return bids
}

func testGaugeValue(vec *prometheus.GaugeVec, labels []string) float64 {
	m := &dto.Metric{}
	_ = vec.WithLabelValues(labels...).Write(m)
	return m.GetGauge().GetValue()
}

func TestCalcScanShardsPerRound(t *testing.T) {
	mgr := &DataInspectMgr{conf: DataInspectConf{CycleDays: 90}}
	cycleDuration := time.Duration(90) * core.CycleDayDuration
	targetDuration := time.Duration(float64(cycleDuration) * cycleTargetFraction)
	startAt := func(elapsed time.Duration) int64 {
		return time.Now().Add(-elapsed).UnixNano()
	}

	t.Run("not counted returns zero", func(t *testing.T) {
		st := core.InspectChunkState{CycleCnt: -1, CycleScanned: 0}
		diskSt := core.InspectDiskState{CycleStartAt: startAt(0)}
		require.Equal(t, 0, mgr.calcScanShardsPerRound(st, diskSt))
	})

	t.Run("ahead of schedule skips round", func(t *testing.T) {
		st := core.InspectChunkState{CycleCnt: 1000, CycleScanned: 100}
		diskSt := core.InspectDiskState{CycleStartAt: startAt(0)}
		require.Equal(t, 0, mgr.calcScanShardsPerRound(st, diskSt))
	})

	t.Run("behind schedule catches up to time target", func(t *testing.T) {
		st := core.InspectChunkState{CycleCnt: 1000, CycleScanned: 100}
		diskSt := core.InspectDiskState{CycleStartAt: startAt(time.Duration(float64(targetDuration) * 0.5))}
		got := mgr.calcScanShardsPerRound(st, diskSt)
		// Derive the expected gap from the actual elapsed time so wall-clock
		// drift between startAt() and CycleElapsed() cannot flake the assertion.
		progress := float64(diskSt.CycleElapsed()) / float64(targetDuration)
		want := int(math.Ceil(float64(st.CycleCnt)*progress)) - int(st.CycleScanned)
		require.Equal(t, want, got)
		require.InDelta(t, 400, got, 2)
	})

	t.Run("window capped at max per round", func(t *testing.T) {
		st := core.InspectChunkState{CycleCnt: 1_000_000, CycleScanned: 0}
		diskSt := core.InspectDiskState{CycleStartAt: startAt(time.Duration(float64(targetDuration) * 0.9))}
		require.Equal(t, maxScanShardsPerRound, mgr.calcScanShardsPerRound(st, diskSt))
	})

	t.Run("past soft target scans all remaining", func(t *testing.T) {
		st := core.InspectChunkState{CycleCnt: 1000, CycleScanned: 100}
		// Pad past the soft target so wall-clock jitter cannot fall back into the
		// time-proportional branch.
		diskSt := core.InspectDiskState{CycleStartAt: startAt(targetDuration + time.Minute)}
		require.Equal(t, 900, mgr.calcScanShardsPerRound(st, diskSt))
	})

	t.Run("fully scanned returns zero", func(t *testing.T) {
		st := core.InspectChunkState{CycleCnt: 1000, CycleScanned: 1000}
		diskSt := core.InspectDiskState{CycleStartAt: startAt(0)}
		require.Equal(t, 0, mgr.calcScanShardsPerRound(st, diskSt))
	})
}

func TestMergeBadBids(t *testing.T) {
	ctx := context.Background()
	mgr := &DataInspectMgr{}

	badShards := func(bids ...proto.BlobID) []bnapi.BadShard {
		bads := make([]bnapi.BadShard, 0, len(bids))
		for _, b := range bids {
			bads = append(bads, bnapi.BadShard{Bid: b})
		}
		return bads
	}

	t.Run("empty inspected no change", func(t *testing.T) {
		st := &core.InspectChunkState{BadBids: badBidSet(1, 2)}
		mgr.mergeBadBids(ctx, nil, st, nil, nil)
		require.Equal(t, badBidSet(1, 2), st.BadBids)
	})

	t.Run("healthy inspected clears, new bad added, existing bad kept", func(t *testing.T) {
		st := &core.InspectChunkState{BadBids: badBidSet(1, 2)}
		mgr.mergeBadBids(ctx, nil, st, nopPage(1, 3, 4), badShards(5))
		require.Equal(t, badBidSet(2, 5), st.BadBids)
	})

	t.Run("bad bid re-detected stays", func(t *testing.T) {
		st := &core.InspectChunkState{BadBids: badBidSet(1, 2)}
		mgr.mergeBadBids(ctx, nil, st, nopPage(1, 2), badShards(1))
		require.Equal(t, badBidSet(1), st.BadBids)
	})

	t.Run("nil bad bids initialized", func(t *testing.T) {
		st := &core.InspectChunkState{}
		mgr.mergeBadBids(ctx, nil, st, nopPage(1), badShards(7))
		require.Equal(t, badBidSet(7), st.BadBids)
	})

	t.Run("bad bids beyond limit dropped", func(t *testing.T) {
		ctr := gomock.NewController(t)
		ds := NewMockDiskAPI(ctr)
		cs := NewMockChunkAPI(ctr)
		ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		cs.EXPECT().Disk().Return(ds).AnyTimes()
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()

		seeded := make(map[proto.BlobID]struct{}, maxInspectBadBids)
		for i := 1; i <= maxInspectBadBids; i++ {
			seeded[proto.BlobID(i)] = struct{}{}
		}
		st := &core.InspectChunkState{BadBids: seeded}
		mgr.mergeBadBids(ctx, cs, st, nil, badShards(proto.BlobID(maxInspectBadBids+1)))
		require.Len(t, st.BadBids, maxInspectBadBids)
		_, exist := st.BadBids[proto.BlobID(maxInspectBadBids+1)]
		require.False(t, exist)
	})
}

func TestInspectCountOnly(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	ds.EXPECT().IsClosing().Return(false).AnyTimes()
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	expectNopPages(cs, bidsRange(1, 250), listShardBatch)

	st := &core.InspectChunkState{Vuid: proto.Vuid(1001), CycleID: 1}
	require.NoError(t, mgr.inspectCountOnly(ctx, ds, cs, st))
	require.Equal(t, int64(250), st.CycleCnt)
	require.Equal(t, proto.BlobID(250), st.CycleMaxBid)
	require.False(t, st.NeedCount())
}

func TestInspectScanWindow_WindowPagingAndCompletion(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	expectScanDiskChunk(ds, cs)
	expectNopPages(cs, bidsRange(1, 250), listShardBatch)

	st := &core.InspectChunkState{Vuid: proto.Vuid(1001), CycleID: 1, CycleCnt: 250, CycleMaxBid: 250}

	// window 120: two full pages are consumed, cursor pauses at the next start bid
	scanned, err := mgr.inspectScanWindow(ctx, ds, cs, st, 120)
	require.NoError(t, err)
	require.Equal(t, 200, scanned)
	require.Equal(t, int64(200), st.CycleScanned)
	require.Equal(t, proto.BlobID(200), st.Cursor)
	require.False(t, st.CycleDone())

	// final pass reaches the cycle snapshot bound and completes the chunk
	scanned, err = mgr.inspectScanWindow(ctx, ds, cs, st, 1000)
	require.NoError(t, err)
	require.Equal(t, 50, scanned)
	require.Equal(t, int64(250), st.CycleScanned)
	require.Equal(t, proto.BlobID(250), st.Cursor)
	require.True(t, st.CycleDone())
}

func TestInspectScanWindow_StopsAtCycleSnapshotBound(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	expectScanDiskChunk(ds, cs)

	// count-only snapshot captured bids 1..250; bids 251..350 were written
	// afterwards. The scan stops on the page that passes the snapshot bound
	// (next >= CycleMaxBid), so the page starting at 300 is never requested:
	// shards written after the snapshot are deferred to the next cycle.
	expectNopPage(cs, 0, nopPage(bidsRange(1, 100)...), 100)
	expectNopPage(cs, 100, nopPage(bidsRange(101, 200)...), 200)
	expectNopPage(cs, 200, nopPage(bidsRange(201, 300)...), 300)

	st := &core.InspectChunkState{Vuid: proto.Vuid(1001), CycleID: 1, CycleCnt: 250, CycleMaxBid: 250}
	scanned, err := mgr.inspectScanWindow(ctx, ds, cs, st, 1000)
	require.NoError(t, err)
	require.Equal(t, 300, scanned)
	require.Equal(t, proto.BlobID(250), st.Cursor)
	require.Equal(t, int64(300), st.CycleScanned)
	require.True(t, st.CycleDone())
}

func TestInspectScanWindow_ToEnd(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	expectScanDiskChunk(ds, cs)
	expectNopPages(cs, bidsRange(1, 250), listShardBatch)

	st := &core.InspectChunkState{Vuid: proto.Vuid(1001), CycleID: 1, CycleCnt: 250, CycleMaxBid: 250}
	scanned, err := mgr.inspectScanWindow(ctx, ds, cs, st, 0) // 0 = scan to end
	require.NoError(t, err)
	require.Equal(t, 250, scanned)
	require.Equal(t, int64(250), st.CycleScanned)
	require.Equal(t, proto.BlobID(250), st.Cursor)
	require.True(t, st.CycleDone())
}

func TestInspectScanWindow_ForcePathEmptyChunk(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	ds.EXPECT().IsClosing().Return(false).AnyTimes()
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	expectNopPage(cs, 0, nil, proto.InValidBlobID)

	// Force-scan caller pattern: count first; empty chunk is CycleDone without a CRC pass.
	st := &core.InspectChunkState{Vuid: proto.Vuid(1001), CycleID: 1, CycleCnt: -1}
	require.NoError(t, mgr.inspectCountOnly(ctx, ds, cs, st))
	require.Equal(t, int64(0), st.CycleCnt)
	require.Equal(t, proto.BlobID(0), st.CycleMaxBid)
	require.True(t, st.CycleDone())
}

func TestReconcileBadBids(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().IsClosing().Return(false).AnyTimes()
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()

	t.Run("deleted shard cleared", func(t *testing.T) {
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(1)).Return(nil, os.ErrNotExist).Times(1)
		st := &core.InspectChunkState{Vuid: proto.Vuid(1001), BadBids: badBidSet(1)}
		require.NoError(t, mgr.reconcileBadBids(ctx, ds, cs, st))
		require.Empty(t, st.BadBids)
	})

	t.Run("recovered shard cleared", func(t *testing.T) {
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(2)).Return(&core.ShardMeta{Size: 100, Crc: 1}, nil).Times(1)
		cs.EXPECT().Read(any, any).Return(int64(100), nil).Times(1)
		st := &core.InspectChunkState{Vuid: proto.Vuid(1001), BadBids: badBidSet(2)}
		require.NoError(t, mgr.reconcileBadBids(ctx, ds, cs, st))
		require.Empty(t, st.BadBids)
	})

	t.Run("still corrupted kept", func(t *testing.T) {
		// reconcile reads meta, inspectShard re-reads it after the failed data read
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(3)).Return(&core.ShardMeta{Size: 100, Crc: 1}, nil).Times(2)
		cs.EXPECT().Read(any, any).Return(int64(0), errMock).Times(1)
		st := &core.InspectChunkState{Vuid: proto.Vuid(1001), BadBids: badBidSet(3)}
		require.NoError(t, mgr.reconcileBadBids(ctx, ds, cs, st))
		require.Equal(t, badBidSet(3), st.BadBids)
	})

	t.Run("meta error kept", func(t *testing.T) {
		cs.EXPECT().ReadShardMeta(any, proto.BlobID(4)).Return(nil, errMock).Times(1)
		st := &core.InspectChunkState{Vuid: proto.Vuid(1001), BadBids: badBidSet(4)}
		require.NoError(t, mgr.reconcileBadBids(ctx, ds, cs, st))
		require.Equal(t, badBidSet(4), st.BadBids)
	})

	t.Run("visits bids in ascending order", func(t *testing.T) {
		var got []proto.BlobID
		cs.EXPECT().ReadShardMeta(any, any).DoAndReturn(
			func(_ context.Context, bid proto.BlobID) (*core.ShardMeta, error) {
				got = append(got, bid)
				return nil, os.ErrNotExist
			},
		).Times(3)
		st := &core.InspectChunkState{Vuid: proto.Vuid(1001), BadBids: badBidSet(5, 1, 3)}
		require.NoError(t, mgr.reconcileBadBids(ctx, ds, cs, st))
		require.Equal(t, []proto.BlobID{1, 3, 5}, got)
		require.Empty(t, st.BadBids)
	})
}

func expiredDiskState(cycleID uint64) core.InspectDiskState {
	return core.InspectDiskState{
		DiskID:       proto.DiskID(11),
		CycleID:      cycleID,
		CycleStartAt: time.Now().Add(-91 * 24 * time.Hour).UnixNano(),
	}
}

func newExpiredCycleFixture(t *testing.T, vuid proto.Vuid) (*DataInspectMgr, *MockDiskAPI, *MockChunkAPI, []core.VuidMeta) {
	t.Helper()
	ctr := gomock.NewController(t)
	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)

	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().IsClosing().Return(false).AnyTimes()
	ds.EXPECT().GetChunkStorage(vuid).Return(cs, true).AnyTimes()
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 11},
	}).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()
	cs.EXPECT().Status().Return(clustermgr.ChunkStatusNormal).AnyTimes()
	cs.EXPECT().VuidMeta().Return(&core.VuidMeta{}).AnyTimes()
	cs.EXPECT().Vuid().Return(vuid).AnyTimes()
	cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()

	chunks := []core.VuidMeta{{Vuid: vuid, Status: clustermgr.ChunkStatusNormal}}
	return newScheduleTestMgr(), ds, cs, chunks
}

func TestCheckAndFinishExpiredCycle_InitFirstCycle(t *testing.T) {
	ctx := context.Background()
	mgr, ds, _, chunks := newExpiredCycleFixture(t, proto.Vuid(1001))

	diskSt := core.InspectDiskState{DiskID: 11}
	ds.EXPECT().StoreInspectDiskState(any, any).DoAndReturn(
		func(_ context.Context, got core.InspectDiskState) error {
			require.Equal(t, proto.DiskID(11), got.DiskID)
			return nil
		},
	).Times(1)

	require.NoError(t, mgr.checkAndFinishExpiredCycle(ctx, ds, chunks, &diskSt))
	require.Equal(t, uint64(1), diskSt.CycleID)
	require.Greater(t, diskSt.CycleStartAt, int64(0))
}

func TestCheckAndFinishExpiredCycle_NotExpired(t *testing.T) {
	ctx := context.Background()
	mgr, ds, _, chunks := newExpiredCycleFixture(t, proto.Vuid(1001))

	diskSt := core.InspectDiskState{
		DiskID:       proto.DiskID(11),
		CycleID:      1,
		CycleStartAt: time.Now().Add(-time.Hour).UnixNano(),
	}
	// no StoreInspectDiskState / chunk state access expected
	require.NoError(t, mgr.checkAndFinishExpiredCycle(ctx, ds, chunks, &diskSt))
	require.Equal(t, uint64(1), diskSt.CycleID)
}

func TestCheckAndFinishExpiredCycle_ForceScanAndAdvance(t *testing.T) {
	ctx := context.Background()
	mgr, ds, cs, chunks := newExpiredCycleFixture(t, proto.Vuid(1001))
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	expectNopPages(cs, bidsRange(1, 250), listShardBatch) // count-only after lazy reset
	expectNopPages(cs, bidsRange(1, 250), listShardBatch) // force CRC scan

	// stale chunk state from cycle 0 is lazily reset to the current cycle
	ds.EXPECT().LoadInspectChunkState(any, proto.Vuid(1001)).Return(
		core.InspectChunkState{Vuid: proto.Vuid(1001), CycleID: 0, CycleCnt: 250, CycleMaxBid: 250}, nil,
	).Times(1)
	ds.EXPECT().StoreInspectChunkState(any, any).DoAndReturn(
		func(_ context.Context, got core.InspectChunkState) error {
			require.Equal(t, uint64(1), got.CycleID)
			require.Equal(t, proto.BlobID(250), got.Cursor)
			require.True(t, got.CycleDone())
			return nil
		},
	).Times(1)

	diskSt := expiredDiskState(1)
	var storedDisk core.InspectDiskState
	ds.EXPECT().StoreInspectDiskState(any, any).DoAndReturn(
		func(_ context.Context, got core.InspectDiskState) error {
			storedDisk = got
			return nil
		},
	).Times(1)

	require.NoError(t, mgr.checkAndFinishExpiredCycle(ctx, ds, chunks, &diskSt))
	require.Equal(t, uint64(2), diskSt.CycleID)
	require.Equal(t, uint64(2), storedDisk.CycleID)
	require.Greater(t, storedDisk.CycleStartAt, int64(0))
}

func TestCheckAndFinishExpiredCycle_ReleasedChunkSkipped(t *testing.T) {
	ctx := context.Background()
	mgr, ds, _, _ := newExpiredCycleFixture(t, proto.Vuid(1001))
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	// released chunk never touches GetChunkStorage / chunk state
	chunks := []core.VuidMeta{{Vuid: proto.Vuid(1002), Status: clustermgr.ChunkStatusRelease}}

	diskSt := expiredDiskState(1)
	ds.EXPECT().StoreInspectDiskState(any, any).Return(nil).Times(1)
	require.NoError(t, mgr.checkAndFinishExpiredCycle(ctx, ds, chunks, &diskSt))
	require.Equal(t, uint64(2), diskSt.CycleID)
}

func TestCheckAndFinishExpiredCycle_ControlStopKeepsCycle(t *testing.T) {
	ctx := context.Background()
	mgr, ds, _, chunks := newExpiredCycleFixture(t, proto.Vuid(1001))
	ds.EXPECT().IsWritable().Return(true).AnyTimes()

	ds.EXPECT().LoadInspectChunkState(any, proto.Vuid(1001)).DoAndReturn(
		func(context.Context, proto.Vuid) (core.InspectChunkState, error) {
			// Outer-loop stop check already passed; disable the switch so the
			// force-scan loop aborts before ListShards and leaves the cycle unchanged.
			mgr.taskSwitch.Disable()
			return core.InspectChunkState{
				Vuid: proto.Vuid(1001), CycleID: 1, CycleCnt: 250, CycleMaxBid: 250,
			}, nil
		},
	).Times(1)

	diskSt := expiredDiskState(1)
	err := mgr.checkAndFinishExpiredCycle(ctx, ds, chunks, &diskSt)
	require.ErrorIs(t, err, core.ErrInspectStopped)
	require.Equal(t, uint64(1), diskSt.CycleID)
}

func TestLogRoundSummFiltersStaleAndReleasedState(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	mgr := newScheduleTestMgr()
	ds := NewMockDiskAPI(ctr)
	aliveCS := NewMockChunkAPI(ctr)
	releasedCS := NewMockChunkAPI(ctr)

	diskSt := core.InspectDiskState{DiskID: 11, CycleID: 2, CycleStartAt: time.Now().UnixNano()}
	states := []*core.InspectChunkState{
		{Vuid: 1001, CycleID: 2, CycleCnt: 100, CycleScanned: 40, BadBids: badBidSet(1)},      // current cycle, alive
		{Vuid: 1002, CycleID: 1, CycleCnt: 50, CycleScanned: 50, BadBids: badBidSet(2, 3)},    // stale cycle
		{Vuid: 1003, CycleID: 2, CycleCnt: 10, CycleScanned: 10, BadBids: badBidSet(4, 5, 6)}, // released
		{Vuid: 1004, CycleID: 2, CycleCnt: -1, BadBids: badBidSet(7)},                         // current cycle, not counted yet
	}

	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().RangeInspectChunkState(any, any).DoAndReturn(
		func(_ context.Context, fn func(*core.InspectChunkState) bool) error {
			for _, st := range states {
				fn(st)
			}
			return nil
		},
	).Times(1)
	ds.EXPECT().GetChunkStorage(any).DoAndReturn(func(vuid proto.Vuid) (core.ChunkAPI, bool) {
		if vuid == proto.Vuid(1003) {
			return releasedCS, true
		}
		return aliveCS, true
	}).AnyTimes()
	aliveCS.EXPECT().Status().Return(clustermgr.ChunkStatusNormal).AnyTimes()
	releasedCS.EXPECT().Status().Return(clustermgr.ChunkStatusRelease).AnyTimes()
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{
		DiskInfo:          clustermgr.DiskInfo{ClusterID: 9},
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 77},
	}).AnyTimes()

	span := trace.SpanFromContextSafe(ctx)
	mgr.logRoundSumm(ctx, span, ds, diskSt, 40)

	// only current-cycle alive states contribute: vuid 1001 (1 bad) + vuid 1004 (1 bad)
	require.Equal(t, float64(2), testGaugeValue(dataInspectBadShardByDiskVec, []string{"9", "77"}))
}

func TestInspectScanWindowUpdatesCursorOnInterrupt(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)
	svr := &Service{ctx: ctx, closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 10, RateLimit: 1024 * 1024, BatchReadSize: 1 << 20}, svr)
	mgr.taskSwitch = taskswitch.NewEnabledTaskSwitch()

	closing := false
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().IsClosing().DoAndReturn(func() bool { return closing }).AnyTimes()
	ds.EXPECT().IsWritable().Return(true).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()

	// After the first successful page, mark disk closing so the next loop head stops.
	page1 := zeroSizeShards(1, 2)
	cs.EXPECT().ListShards(any, proto.BlobID(0), listShardBatch, bnapi.ShardStatusNormal).
		DoAndReturn(func(context.Context, proto.BlobID, int, bnapi.ShardStatus) ([]*bnapi.ShardInfo, proto.BlobID, error) {
			closing = true
			return page1, proto.BlobID(3), nil
		})

	st := &core.InspectChunkState{
		Vuid:         1001,
		CycleID:      1,
		Cursor:       proto.InValidBlobID,
		CycleMaxBid:  10,
		CycleCnt:     10,
		CycleScanned: 0,
	}
	// window<=0 (to end) and window>0 share the same interrupt/cursor path.
	scanned, err := mgr.inspectScanWindow(ctx, ds, cs, st, 0)
	require.ErrorIs(t, err, core.ErrInspectStopped)
	require.Equal(t, 2, scanned)
	require.Equal(t, proto.BlobID(3), st.Cursor)
	require.Equal(t, int64(2), st.CycleScanned)

	closing = false
	cs.EXPECT().ListShards(any, proto.BlobID(0), listShardBatch, bnapi.ShardStatusNormal).
		DoAndReturn(func(context.Context, proto.BlobID, int, bnapi.ShardStatus) ([]*bnapi.ShardInfo, proto.BlobID, error) {
			closing = true
			return page1, proto.BlobID(3), nil
		})
	st.Cursor = proto.InValidBlobID
	st.CycleScanned = 0
	scanned, err = mgr.inspectScanWindow(ctx, ds, cs, st, 50)
	require.ErrorIs(t, err, core.ErrInspectStopped)
	require.Equal(t, 2, scanned)
	require.Equal(t, proto.BlobID(3), st.Cursor)
	require.Equal(t, int64(2), st.CycleScanned)
}

func TestInspectLimitsConcurrent(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	svr := &Service{ctx: ctx, closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{RateLimit: 1024}, svr)

	disks := make([]core.DiskAPI, 0, 8)
	for i := 1; i <= 8; i++ {
		ds := NewMockDiskAPI(ctr)
		id := proto.DiskID(i)
		ds.EXPECT().ID().Return(id).AnyTimes()
		disks = append(disks, ds)
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.setLimiters(disks)
			mgr.setAllDiskRateForce(2048)
			for _, ds := range disks {
				_ = mgr.getLimiter(ds)
			}
		}()
	}
	wg.Wait()

	for _, ds := range disks {
		require.NotNil(t, mgr.getLimiter(ds))
	}
	require.Equal(t, 2048, mgr.conf.RateLimit)
}

func TestGetLimiterLazyCreate(t *testing.T) {
	ctr := gomock.NewController(t)
	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

	t.Run("creates from configured rate and caches", func(t *testing.T) {
		mgr := &DataInspectMgr{
			conf:   DataInspectConf{RateLimit: 4 * 1024 * 1024},
			limits: make(map[proto.DiskID]*rate.Limiter),
		}
		lmt := mgr.getLimiter(ds)
		require.NotNil(t, lmt)
		require.Equal(t, 2*4*1024*1024, lmt.Burst())
		require.Same(t, lmt, mgr.getLimiter(ds))
	})

	t.Run("falls back to system default rate when unset", func(t *testing.T) {
		mgr := &DataInspectMgr{limits: make(map[proto.DiskID]*rate.Limiter)}
		lmt := mgr.getLimiter(ds)
		require.NotNil(t, lmt)
		require.Equal(t, 2*defaultInspectRate, lmt.Burst())
	})
}
