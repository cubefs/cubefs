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
	"errors"
	"math"
	"sort"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	listShardBatch = 100

	maxScanShardsPerRound = 10_000 // ceiling window per round, avoids IO spikes when catching up
	cycleTargetFraction   = 0.95   // finish scanning within 95% of the cycle, leaving slack before the hard deadline

	maxInspectBadBids = 1000
)

var errServiceClosed = errors.New("service is closed")

func isInspectControlStop(err error) bool {
	return errors.Is(err, core.ErrInspectStopped) || errors.Is(err, errServiceClosed)
}

func (mgr *DataInspectMgr) inspectShouldStop(ds core.DiskAPI) bool {
	return !mgr.getSwitch() || ds.IsClosing() || !ds.IsWritable()
}

// checkChunkInspectable applies common chunk eligibility checks.
// reason is non-empty when the chunk should be skipped
func (mgr *DataInspectMgr) checkChunkInspectable(ds core.DiskAPI, chunk core.VuidMeta) (cs core.ChunkAPI, reason string) {
	if chunk.Status == clustermgr.ChunkStatusRelease {
		return nil, "chunk released"
	}
	cs, found := ds.GetChunkStorage(chunk.Vuid)
	if !found {
		return nil, "not found"
	}
	if cs.Status() == clustermgr.ChunkStatusRelease {
		return nil, "chunk released"
	}
	if cs.VuidMeta().Compacting {
		return nil, "isCompacting"
	}
	return cs, ""
}

// checkAndFinishExpiredCycle handles disk-level cycle boundaries at the start of a round:
// it initializes the first cycle when CycleStartAt is zero, and when the current
// cycle has hit its hard deadline it force-scans unfinished chunks, then advances
// the inspect cycle generation. Chunk progress is not reset here; inspectChunk
// lazily resets stale chunk state on first touch.
//
// If force-scan is interrupted by inspect control stop (switch off, disk closing,
// not writable, or service close), the cycle is not advanced, but progress is
// kept for retry.
func (mgr *DataInspectMgr) checkAndFinishExpiredCycle(ctx context.Context,
	ds core.DiskAPI, chunks []core.VuidMeta, diskSt *core.InspectDiskState,
) error {
	span := trace.SpanFromContextSafe(ctx)

	// when the disk first run this dataInspect function
	if diskSt.CycleStartAt == 0 {
		span.Infof("inspect disk:%d init inspect disk state first", ds.ID())
		diskSt.CycleID = 1
		diskSt.CycleStartAt = time.Now().UnixNano()
		if err := ds.InspectState().StoreInspectDiskState(ctx, *diskSt); err != nil {
			span.Errorf("inspect disk:%d save disk state failed: %+v", ds.ID(), err)
			return err
		}
		span.Infof("inspect disk:%d inspect init disk state successful", ds.ID())
		return nil
	}

	if !diskSt.CycleExpired(mgr.conf.CycleDays) {
		// this cycle not expired, do inspect step-by-step
		return nil
	}

	span.Warnf("inspect disk:%d inspect cycle expired, scan all remaining chunks", ds.ID())

	// This is the hard-deadline scan-to-end procedure for current expired cycle.
	// A single chunk's error should not block the disk from advancing cycle, and
	// only specific stop conditions ( disk switch off, closing, not writable, etc.)
	// may abort the advance procedure and do retries later.
	for _, chunk := range chunks {
		// inspect switch and disk status check
		if mgr.inspectShouldStop(ds) {
			span.Warnf("inspect disk:%d switch %v, closing %v, writable %v skip",
				ds.ID(), mgr.getSwitch(), ds.IsClosing(), ds.IsWritable())
			return core.ErrInspectStopped
		}

		// chunk alive and chunk status check
		// this helps to ensure at most one chunk bind to specific vuid
		cs, reason := mgr.checkChunkInspectable(ds, chunk)
		if cs == nil {
			span.Debugf("inspect disk:%d vuid:%d skip reason: %s", ds.ID(), chunk.Vuid, reason)
			continue
		}

		st, err := ds.InspectState().LoadInspectChunkState(ctx, chunk.Vuid)
		if err != nil {
			span.Errorf("inspect disk:%d vuid:%d read chunk inspect_state failed: %+v", ds.ID(), chunk.Vuid, err)
			continue
		}

		// when meets previous cycle, reset and restart the inspect process
		if st.CycleID < diskSt.CycleID {
			span.Debugf("inspect disk:%d vuid:%d stale cycle %d -> %d, lazy reset",
				ds.ID(), chunk.Vuid, st.CycleID, diskSt.CycleID)
			st.ResetForCycle(diskSt.CycleID)
		}

		if st.CycleDone() {
			continue
		}

		span.Infof("inspect disk:%d vuid:%d %s cycle expired, scan from cursor:%d maxBid:%d",
			ds.ID(), chunk.Vuid, cs.ID(), st.Cursor, st.CycleMaxBid)

		// Force path: count first if needed, then scan to end (window <= 0).
		if st.NeedCount() {
			if err := mgr.inspectCountOnly(ctx, ds, cs, &st); err != nil {
				span.Errorf("inspect disk:%d vuid:%d %s count-only failed: %+v", ds.ID(), chunk.Vuid, cs.ID(), err)
				if isInspectControlStop(err) {
					return err
				}
				continue
			}
		}
		if !st.CycleDone() {
			if _, err := mgr.inspectScanWindow(ctx, ds, cs, &st, 0); err != nil {
				span.Errorf("inspect disk:%d vuid:%d %s force-scan failed: %+v", ds.ID(), chunk.Vuid, cs.ID(), err)
				// If detected inspect should stop, skip the following cycle advance routine, and retries later,
				// and other errs will be ignored.
				if isInspectControlStop(err) {
					return err
				}
				continue
			}
		}

		if err := ds.InspectState().StoreInspectChunkState(ctx, st); err != nil {
			span.Errorf("inspect disk:%d vuid:%d persist chunk inspect_state failed: %+v", ds.ID(), chunk.Vuid, err)
			continue
		}

		mgr.updateBadShardByChunk(cs, &st)
		span.Infof("inspect disk:%d vuid:%d %s force-scan done", ds.ID(), chunk.Vuid, cs.ID())
	}

	// advance disk cycle after all force-scan work for the old cycle is done.
	diskSt.CycleID++
	diskSt.CycleStartAt = time.Now().UnixNano()
	if err := ds.InspectState().StoreInspectDiskState(ctx, *diskSt); err != nil {
		span.Errorf("inspect disk:%d persist disk inspect_state failed: %+v", ds.ID(), err)
		return err
	}

	span.Infof("inspect disk:%d new inspect cycle %d started", ds.ID(), diskSt.CycleID)
	return nil
}

// inspectChunk is the single-chunk scheduling entry point:
// pick modeCountOnly (first touch of a cycle), modeScan (steady-state windowed scan),
// or skip entirely (already finished this cycle, released, or mid-compaction).
// Returns the number of shards actually CRC-verified in this call (for round summary logging).
func (mgr *DataInspectMgr) inspectChunk(ctx context.Context, ds core.DiskAPI, diskSt core.InspectDiskState, chunk core.VuidMeta) (scanned int) {
	span := trace.SpanFromContextSafe(ctx)

	cs, reason := mgr.checkChunkInspectable(ds, chunk)
	if cs == nil {
		span.Debugf("inspect disk:%d vuid:%d skip, reason: %s", ds.ID(), chunk.Vuid, reason)
		return
	}

	st, err := ds.InspectState().LoadInspectChunkState(ctx, chunk.Vuid)
	if err != nil {
		span.Errorf("inspect disk:%d vuid:%d read chunk inspect_state failed: %+v", ds.ID(), chunk.Vuid, err)
		return
	}

	if st.CycleID < diskSt.CycleID {
		span.Debugf("inspect disk:%d vuid:%d stale cycle %d -> %d, lazy reset",
			ds.ID(), chunk.Vuid, st.CycleID, diskSt.CycleID)
		st.ResetForCycle(diskSt.CycleID)
	}

	var inspectErr error
	switch {
	case st.CycleDone():
		return
	case st.NeedCount():
		inspectErr = mgr.inspectCountOnly(ctx, ds, cs, &st)
		if inspectErr == nil {
			span.Infof("inspect disk:%d vuid:%d %s count maxBid:%d", ds.ID(), chunk.Vuid, cs.ID(), st.CycleMaxBid)
		}
	default:
		window := mgr.calcScanShardsPerRound(st, diskSt)
		if window == 0 {
			// scan progress is ahead of time progress; skip this round
			return
		}
		scanned, inspectErr = mgr.inspectScanWindow(ctx, ds, cs, &st, window)
	}

	if inspectErr != nil {
		span.Warnf("inspect chunk vuid:%d error, skip persisting this round: %+v", chunk.Vuid, inspectErr)
		return
	}

	if err := ds.InspectState().StoreInspectChunkState(ctx, st); err != nil {
		span.Errorf("inspect disk:%d vuid:%d persist chunk inspect_state failed: %+v", ds.ID(), chunk.Vuid, err)
		return
	}
	mgr.updateBadShardByChunk(cs, &st)
	return
}

func (mgr *DataInspectMgr) updateBadShardByChunk(cs core.ChunkAPI, st *core.InspectChunkState) {
	info := cs.Disk().DiskInfo()
	dataInspectBadShardByChunkVec.WithLabelValues(dataInspectChunkLabelValues(info, cs.Vuid())...).
		Set(float64(len(st.BadBids)))
}

// inspectCountOnly walks the chunk's shard metadata only (no data I/O, no CRC) to record
// the shard-count snapshot and the max bid at the start of a cycle.
func (mgr *DataInspectMgr) inspectCountOnly(ctx context.Context, ds core.DiskAPI, cs core.ChunkAPI, st *core.InspectChunkState) error {
	var total int64
	var maxBid proto.BlobID
	cursor := proto.InValidBlobID
	for {
		if mgr.inspectShouldStop(ds) {
			return core.ErrInspectStopped
		}
		shards, next, err := cs.ListShards(ctx, cursor, listShardBatch, bnapi.ShardStatusNormal)
		if err != nil {
			return err
		}
		total += int64(len(shards))
		if len(shards) > 0 {
			maxBid = shards[len(shards)-1].Bid // ListShards returns bid-ascending order
		}
		if next == proto.InValidBlobID {
			break
		}
		cursor = next
	}
	st.CycleCnt = total
	st.CycleMaxBid = maxBid
	return nil
}

// inspectScanWindow advances a chunk's CRC scan from st.Cursor.
//
// window > 0: inspect at most that many shards this call (steady-state windowing;
// callers get the size from calcScanShardsPerRound and must not pass 0 for "skip").
// window <= 0: scan through to the cycle snapshot bound / EOF (force-scan path).
//
// Count-only is the caller's responsibility when st.NeedCount(); this function only
// walks shard pages. st.Cursor is advanced in memory after each fully successful page.
// Returns the number of shards listed in this call.
func (mgr *DataInspectMgr) inspectScanWindow(ctx context.Context,
	ds core.DiskAPI, cs core.ChunkAPI, st *core.InspectChunkState, window int,
) (int, error) {
	span := trace.SpanFromContextSafe(ctx)

	cursor, done := st.Cursor, 0
	for {
		if window > 0 && done >= window {
			break
		}
		if mgr.inspectShouldStop(ds) {
			return done, core.ErrInspectStopped
		}
		// listShardBatch is only the pagination cap for ListShards; it may be larger than
		// window, which only makes the window coarser but does not matter.
		shards, next, err := cs.ListShards(ctx, cursor, listShardBatch, bnapi.ShardStatusNormal)
		if err != nil {
			return done, err
		}

		if len(shards) > 0 {
			inspected, bads, ioErr := mgr.inspectShardsPage(ctx, cs, shards, nil)
			mgr.mergeBadBids(ctx, cs, st, inspected, bads)
			mgr.reportBatchBadShards(ctx, cs, bads)
			mgr.trySendCrcRepair(ctx, cs, bads)
			// EIO should stop the inspect immediately; prior pages already updated st.Cursor.
			if base.IsEIO(ioErr) {
				span.Errorf("inspect disk:%d vuid:%d %s io error, skip", ds.ID(), cs.Vuid(), cs.ID())
				return done, ioErr
			}
			// control stop should stop the inspect immediately and definitely
			if isInspectControlStop(ioErr) {
				return done, ioErr
			}
			n := len(shards)
			done += n
			st.CycleScanned += int64(n)
		}

		// covered up to the cycle's snapshot bound, or every existing shard consumed (EOF).
		// Keep Cursor at CycleMaxBid so completion can be derived from Counted + Cursor.
		if next >= st.CycleMaxBid || next == proto.InValidBlobID {
			st.Cursor = st.CycleMaxBid
			if err := mgr.reconcileBadBids(ctx, ds, cs, st); err != nil {
				return done, err
			}
			span.Infof("inspect disk:%d vuid:%d %s scan done", ds.ID(), cs.Vuid(), cs.ID())
			break
		}
		// advance in-memory cursor after a fully successful page
		st.Cursor = next
		cursor = next
	}
	return done, nil
}

// calcScanShardsPerRound picks this round's window so that scan progress tracks elapsed
// time across the cycle's soft target duration (CycleDays * cycleTargetFraction, i.e. 95%).
// It is per-chunk scheduling inside inspectChunk; it does not reset the cycle.
//
// When elapsed >= targetDuration, it returns all remaining shards so inspectScanWindow can try
// to finish the chunk before the hard deadline. Stragglers are force-scanned by
// checkAndFinishExpiredCycle when the cycle expires.
func (mgr *DataInspectMgr) calcScanShardsPerRound(st core.InspectChunkState, diskSt core.InspectDiskState) int {
	cycleDuration := time.Duration(mgr.conf.CycleDays) * core.CycleDayDuration
	targetDuration := time.Duration(float64(cycleDuration) * cycleTargetFraction)
	elapsed := diskSt.CycleElapsed()

	remainingShards := st.CycleCnt - st.CycleScanned
	if remainingShards <= 0 {
		return 0
	}

	// Past the soft target: let inspectScanWindow try to finish this chunk in this inspectChunk round.
	if elapsed >= targetDuration {
		return int(remainingShards)
	}

	timeProgress := float64(elapsed) / float64(targetDuration)
	targetScanned := int64(math.Ceil(float64(st.CycleCnt) * timeProgress))
	gap := targetScanned - st.CycleScanned // how far behind target; <=0 means ahead of schedule

	// scan progress is ahead of time progress; caller should skip this round
	if gap <= 0 {
		return 0
	}
	return clampScanWindow(int(gap), remainingShards)
}

func clampScanWindow(window int, remainingShards int64) int {
	if window > maxScanShardsPerRound {
		window = maxScanShardsPerRound
	}
	if window > int(remainingShards) {
		window = int(remainingShards)
	}
	return window
}

// mergeBadBids incrementally updates BadBids using only the shards actually
// covered by this round's scan: confirmed-healthy bids are removed, newly found bad bids
// are recorded as a set. Historical bad bids outside this round's scan range, or bids
// since deleted, are left untouched here and are fully reconciled once per cycle by
// reconcileBadBids.
func (mgr *DataInspectMgr) mergeBadBids(
	ctx context.Context, cs core.ChunkAPI, st *core.InspectChunkState, inspected []*bnapi.ShardInfo, bads []bnapi.BadShard,
) {
	if len(inspected) == 0 {
		return
	}

	badSet := make(map[proto.BlobID]struct{}, len(bads))
	for _, b := range bads {
		badSet[b.Bid] = struct{}{}
	}

	for _, s := range inspected {
		if _, bad := badSet[s.Bid]; bad {
			continue
		}
		// if bad shard becomes ok, will delete it from inspect_state
		delete(st.BadBids, s.Bid)
	}

	span := trace.SpanFromContextSafe(ctx)
	for _, b := range bads {
		if _, exist := st.BadBids[b.Bid]; exist {
			continue
		}
		if len(st.BadBids) >= maxInspectBadBids {
			span.Errorf("inspect disk:%d vuid:%d bad bids reach limit %d, drop bid:%d",
				cs.Disk().ID(), cs.Vuid(), maxInspectBadBids, b.Bid)
			continue
		}
		if st.BadBids == nil {
			st.BadBids = make(map[proto.BlobID]struct{})
		}
		st.BadBids[b.Bid] = struct{}{}
	}
}

// reconcileBadBids performs a full re-check of every historical bad bid once a chunk's
// scan cycle completes, so bids that were deleted or repaired outside this cycle's scan
// coverage don't linger forever in BadBids.
// Bids are visited in ascending order so meta/data IO follows typical on-disk layout.
func (mgr *DataInspectMgr) reconcileBadBids(ctx context.Context, ds core.DiskAPI, cs core.ChunkAPI, st *core.InspectChunkState) error {
	if len(st.BadBids) == 0 {
		return nil
	}
	span := trace.SpanFromContextSafe(ctx)

	bids := make([]proto.BlobID, 0, len(st.BadBids))
	for bid := range st.BadBids {
		bids = append(bids, bid)
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i] < bids[j] })

	for _, bid := range bids {
		if mgr.inspectShouldStop(ds) {
			return core.ErrInspectStopped
		}

		// BadBids only contains bids, so need to readShardMeta first to construct shardInfo
		sm, err := cs.ReadShardMeta(ctx, bid)
		if base.IsShardDeleted(err) {
			span.Infof("inspect disk:%d vuid:%d reconcile bid:%d shard deleted, clear inspect_state", ds.ID(), cs.Vuid(), bid)
			delete(st.BadBids, bid) // shard deleted, clear the record
			continue
		}
		if err != nil {
			span.Warnf("inspect disk:%d vuid:%d reconcile bid:%d read shard meta failed: %+v", ds.ID(), cs.Vuid(), bid, err)
			continue // read shard meta failed, keep the bad record
		}

		si := &bnapi.ShardInfo{Bid: bid, Vuid: cs.Vuid(), Size: int64(sm.Size), Crc: sm.Crc}
		if err := mgr.inspectShard(ctx, cs, si); err == nil {
			span.Infof("inspect disk:%d vuid:%d reconcile bid:%d data recovered, clear inspect_state", ds.ID(), cs.Vuid(), bid)
			delete(st.BadBids, bid) // data recovered, clear the record
		}
		// still corrupted: keep the record
	}

	return nil
}

func (mgr *DataInspectMgr) logRoundSumm(ctx context.Context, span trace.Span, ds core.DiskAPI, diskSt core.InspectDiskState, roundScanned int64) {
	var totalShards, totalScanned, totalBadBids int64
	if err := ds.InspectState().RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		// only count states belonging to the current inspect cycle
		if st.CycleID != diskSt.CycleID {
			return true
		}
		// skip chunks that have been released or are no longer alive on this disk
		cs, ok := ds.GetChunkStorage(st.Vuid)
		if !ok || cs.Status() == clustermgr.ChunkStatusRelease {
			return true
		}
		// skip chunks that have not finished count-only in this cycle
		if !st.NeedCount() {
			totalShards += st.CycleCnt
			totalScanned += st.CycleScanned
		}
		totalBadBids += int64(len(st.BadBids))
		return true
	}); err != nil {
		span.Warnf("inspect disk:%d round summ iterate inspect state failed: %+v", ds.ID(), err)
	}

	// summary diskLevel total badshards
	info := ds.DiskInfo()
	dataInspectBadShardByDiskVec.WithLabelValues(dataInspectDiskLabelValues(info)...).Set(float64(totalBadBids))

	var progressPct float64
	if totalShards > 0 {
		progressPct = float64(totalScanned) / float64(totalShards) * 100
	}

	var timeElapsedPct float64
	var targetProgressPct float64
	if diskSt.CycleStartAt > 0 && mgr.conf.CycleDays > 0 {
		cycleDuration := time.Duration(mgr.conf.CycleDays) * core.CycleDayDuration
		timeElapsed := diskSt.CycleElapsed()
		timeElapsedPct = float64(timeElapsed) / float64(cycleDuration) * 100
		targetProgressPct = timeElapsedPct / cycleTargetFraction
	}

	span.Infof("inspect disk:%d round summ: cycle_id=%d round=%d scanned=%d/%d progress=%.1f%% time_progress=(%.1f%%)%.1f%%",
		ds.ID(), diskSt.CycleID, roundScanned, totalScanned, totalShards, progressPct, targetProgressPct, timeElapsedPct)
}

// checkInspectBatchStop returns a control-stop error if the inspect context is
// canceled, the service is closing, or the optional parentCtx (on-demand path) is done.
func (mgr *DataInspectMgr) checkInspectBatchStop(ctx, parentCtx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-mgr.svr.closeCh:
		return errServiceClosed
	default:
	}
	if parentCtx != nil {
		select {
		case <-parentCtx.Done():
			// parent request canceled or timed out, stop the inspect
			return parentCtx.Err()
		default:
		}
	}
	return nil
}

// inspectShardsPage runs full CRC inspection over one ListShards page,
// additionally splitting by the configured BatchReadSize so a single BatchRead call
// never exceeds the configured data size. It returns the subset of shards that
// were inspected (zero-size/inline/nop shards carry no data and are skipped), for
// accurate bad-bid bookkeeping by the caller.
// Note that count-only does not apply this filtering, but the actual scan path
// does skip si.NopData || si.Inline || si.Size == 0, so the real scan can be
// faster than the count pass.
func (mgr *DataInspectMgr) inspectShardsPage(ctx context.Context, cs core.ChunkAPI, shards []*bnapi.ShardInfo, parentCtx context.Context) (
	inspected []*bnapi.ShardInfo, bads []bnapi.BadShard, ioErr error,
) {
	span := trace.SpanFromContextSafe(ctx)
	ds := cs.Disk()
	for _, batch := range splitIntoBatches(shards, mgr.conf.BatchReadSize) {
		if err := mgr.checkInspectBatchStop(ctx, parentCtx); err != nil {
			return inspected, bads, err
		}
		if mgr.inspectShouldStop(ds) {
			return inspected, bads, core.ErrInspectStopped
		}

		lmt := mgr.getLimiter(ds)
		b, err := mgr.inspectBatch(ctx, cs, ds, batch, lmt)
		bads = append(bads, b...)
		if base.IsEIO(err) {
			// this batch's coverage is incomplete/unknown on IO error; excluding it from
			// `inspected` avoids bad-bid records being cleared by mistake.
			span.Errorf("inspect disk:%d vuid:%d %s io error, skip", ds.ID(), cs.Vuid(), cs.ID())
			return inspected, bads, err
		}
		// other error will continue and skip this batch
		if err == nil {
			inspected = append(inspected, batch...)
		}
	}
	return inspected, bads, nil
}
