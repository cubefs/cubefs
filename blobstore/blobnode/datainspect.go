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
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"golang.org/x/time/rate"
)

const (
	minRateLimit = 64 * 1024 // 64 KB/s
)

type (
	InspectDiskState  = core.InspectDiskState
	InspectChunkState = core.InspectChunkState
)

type DataInspectConf struct {
	IntervalSec   int   `json:"interval_sec"`    // wait switch interval
	RateLimit     int   `json:"rate_limit"`      // max rate limit per second
	BatchReadSize int64 `json:"batch_read_size"` // max data bytes per BatchRead call; default 16MB
	CycleDays     int   `json:"cycle_days"`      // full inspect cycle length in days; default 90

	Proxy proxy.LbConfig `json:"proxy"` // used to send crc repair messages.

	Record recordlog.Config `json:"record"`
}

type DataInspectStat struct {
	DataInspectConf
	Open bool `json:"open"` // data_inspect enabled
}

// lazyRepairSender builds the proxy repair client on first use and caches it.
type lazyRepairSender struct {
	once   sync.Once
	sender proxy.LbMsgSender
	build  func() proxy.LbMsgSender
}

// get returns the cached sender, building it once on first call.
func (l *lazyRepairSender) get() proxy.LbMsgSender {
	l.once.Do(func() {
		if l.build != nil {
			l.sender = l.build()
		}
	})
	return l.sender
}

// DataInspectMgr is the service-level facade of the background data-inspect
// feature. It keeps the shared taskSwitch check, record log, repairSender and
// drives the per-disk inspect rounds (loopDataInspect); the disk owns the whole
// per-disk inspect algorithm and its in-memory inspect state store.
type DataInspectMgr struct {
	conf DataInspectConf

	limitsMu sync.Mutex
	limits   map[proto.DiskID]*rate.Limiter

	svr        *Service
	taskSwitch *taskswitch.TaskSwitch

	recorder recordlog.Encoder // local record log

	// proxy client to send crc repair msg, lazily built on first send
	repairSender lazyRepairSender

	// reconcilingStartup marks the one-shot startup bad-bid reconcile in-process.
	// When set (1), inspect rounds are skipped so the two flows cannot overwrite.
	reconcilingStartup int32
}

func NewDataInspectMgr(svr *Service, conf DataInspectConf, switchMgr *taskswitch.SwitchMgr) (*DataInspectMgr, error) {
	if conf.CycleDays < 0 {
		return nil, fmt.Errorf("cycle days value must be positive: %d", conf.CycleDays)
	}
	// zero means "unset" and falls back to the default
	if conf.CycleDays == 0 {
		conf.CycleDays = core.DefaultInspectCycleDays
	}

	taskSwitch, err := switchMgr.AddSwitch(proto.TaskSwitchDataInspect.String())
	if err != nil {
		return nil, err
	}

	// init data inspect record: if record dir exist, will create it; else will return NopEncoder
	var recorder recordlog.Encoder
	rConf := &conf.Record
	if conf.Record.Dir == "" {
		rConf = nil
	}
	if recorder, err = recordlog.NewEncoder(rConf); err != nil {
		return nil, err
	}

	if conf.BatchReadSize <= 0 {
		conf.BatchReadSize = core.DefaultInspectBatchReadSize
	}

	mgr := &DataInspectMgr{
		conf:       conf,
		limits:     make(map[proto.DiskID]*rate.Limiter),
		svr:        svr,
		taskSwitch: taskSwitch,
		recorder:   recorder,
	}
	mgr.repairSender.build = func() proxy.LbMsgSender {
		return proxy.NewMQLbClient(&mgr.conf.Proxy, svr.ClusterMgrClient, svr.Conf.HostInfo.ClusterID)
	}
	return mgr, nil
}

// RecordBadBids writes bad-bid records to the local record log.
func (mgr *DataInspectMgr) RecordBadBids(ctx context.Context,
	info clustermgr.BlobNodeDiskInfo, vuid proto.Vuid, bids []string, errStr string,
) {
	mgr.recordBadBids(ctx, info, vuid, bids, errStr)
}

// AddInspectBadMetric counts bad shard occurrences found by background inspect.
func (mgr *DataInspectMgr) AddInspectBadMetric(info clustermgr.BlobNodeDiskInfo, count int) {
	dataInspectBadVec.WithLabelValues(dataInspectDiskLabelValues(info)...).Add(float64(count))
}

// loopDataInspect is the local dataInspect scheduling goroutine.
// It snapshots live disks and starts one inspection goroutine per writable disk.
// Each round waits for all disk goroutines to finish before the next ticker starts a new round.
func (mgr *DataInspectMgr) loopDataInspect() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "Inspect")

	t := time.NewTicker(time.Duration(mgr.conf.IntervalSec) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if mgr.getSwitch() {
				mgr.runInspectRound(ctx)
			}
		case <-mgr.svr.closeCh:
			mgr.recorder.Close()
			span.Warn("loop inspect data closed.")
			return
		}
	}
}

func (mgr *DataInspectMgr) runInspectRound(ctx context.Context) {
	// skip the round when startup bad-bid reconcile has not been finished
	if mgr.isStartupReconciling() {
		return
	}

	disks := mgr.svr.copyDiskStorages(ctx)
	mgr.setLimiters(disks)

	wg := sync.WaitGroup{}
	for _, ds := range disks {
		if !ds.IsWritable() || ds.IsClosing() {
			continue
		}

		wg.Add(1)
		go func(ds core.DiskAPI) {
			defer wg.Done()
			mgr.inspectDisk(ds)
		}(ds)
	}
	wg.Wait()
}

// inspectDisk drives one disk's inspection for this round: disk-level cycle bookkeeping
// a). checkAndFinishExpiredCycle: hard cycle deadline, force-scan stragglers, reset cycle)
// b). per-chunk incremental scan (inspectChunk/inspectScanWindow: time-proportional windowing with a hard catch-up deadline).
func (mgr *DataInspectMgr) inspectDisk(ds core.DiskAPI) {
	span, ctx := trace.StartSpanFromContextWithTraceID(
		context.Background(), "", ds.ID().ToString()+"_Inspect_"+trace.RandomID().String())
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)

	span.Debugf("inspect disk:%d start", ds.ID())
	defer span.Debugf("inspect disk:%d finish", ds.ID())

	diskSt, err := ds.InspectState().LoadInspectDiskState(ctx)
	if err != nil {
		span.Errorf("inspect disk:%d read disk inspect_state failed: %+v", ds.ID(), err)
		return
	}

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("inspect disk:%d list chunks failed: %+v", ds.ID(), err)
		return
	}

	// inspect cycle deadline check
	if err := mgr.checkAndFinishExpiredCycle(ctx, ds, chunks, &diskSt); err != nil {
		span.Errorf("inspect disk:%d cycle %d failed: %+v", ds.ID(), diskSt.CycleID, err)
		return
	}
	// batch flush any disk-level cycle state written above before the chunk loop
	ds.InspectState().FlushInspectState(ctx)

	var roundScanned int64
	step := len(chunks) / 20
	for i, chunk := range chunks {
		// log progress roughly every 5%, or on the last chunk
		if (step != 0 && (i+1)%step == 0) || i == len(chunks)-1 {
			span.Infof("inspect disk:%d progress: %d/%d chunks", ds.ID(), i+1, len(chunks))
		}

		if mgr.inspectShouldStop(ds) {
			span.Warnf("inspect disk:%d stopped: switch=%v closing=%v writable=%v",
				ds.ID(), mgr.getSwitch(), ds.IsClosing(), ds.IsWritable())
			break
		}
		roundScanned += int64(mgr.inspectChunk(ctx, ds, diskSt, chunk))
		ds.InspectState().FlushInspectState(ctx)
	}

	ds.InspectState().FlushInspectState(ctx)
	mgr.logRoundSumm(ctx, span, ds, diskSt, roundScanned)
}

// batchCRCWriter verifies the CRC of each shard's decoded data written by BatchRead.
//
// BatchRead writes each shard as two separate Write calls:
//  1. Write([GetShardsHeaderSize bytes]) — the ShardsHeader; skip it.
//  2. Write([one CRC-block payload, ≤ CrcBlockUnitSize bytes]) — repeated per block.
//
// A single Write call never spans two shards, so the state machine only needs to track
// whether the next Write is a header or payload data.
type batchCRCWriter struct {
	shards       []*bnapi.ShardInfo
	idx          int
	nextIsHeader bool // true: the next Write is the 4-byte ShardsHeader for shards[idx]
	dataLeft     int64
	hasher       hash.Hash32
	badBids      []proto.BlobID
}

func newBatchCRCWriter(shards []*bnapi.ShardInfo) *batchCRCWriter {
	return &batchCRCWriter{
		shards:       shards,
		nextIsHeader: len(shards) > 0,
		hasher:       crc32.NewIEEE(),
	}
}

func (w *batchCRCWriter) Write(p []byte) (n int, _ error) {
	n = len(p)
	if w.idx >= len(w.shards) {
		return n, nil
	}
	if w.nextIsHeader {
		var hdr bnapi.ShardsHeader
		copy(hdr[:], p)
		w.nextIsHeader = false
		if hdr.Get() != http.StatusOK {
			return n, nil
		}
		w.dataLeft = w.shards[w.idx].Size
		w.hasher.Reset()
		return n, nil
	}
	w.hasher.Write(p)
	w.dataLeft -= int64(n)
	if w.dataLeft <= 0 {
		if w.hasher.Sum32() != w.shards[w.idx].Crc {
			w.badBids = append(w.badBids, w.shards[w.idx].Bid)
		}
		w.idx++
		w.nextIsHeader = w.idx < len(w.shards)
	}
	return n, nil
}

func splitIntoBatches(shards []*bnapi.ShardInfo, maxSize int64) [][]*bnapi.ShardInfo {
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Offset < shards[j].Offset
	})

	var batches [][]*bnapi.ShardInfo
	var cur []*bnapi.ShardInfo
	var curSize int64
	for _, si := range shards {
		if si.NopData || si.Inline || si.Size == 0 {
			continue
		}
		if len(cur) > 0 && curSize+si.Size > maxSize {
			batches = append(batches, cur)
			cur = nil
			curSize = 0
		}
		cur = append(cur, si)
		curSize += si.Size
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

// fallbackInspectShards inspects each shard individually, used when BatchRead fails.
func (mgr *DataInspectMgr) fallbackInspectShards(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI,
	shards []*bnapi.ShardInfo,
) ([]bnapi.BadShard, error) {
	var badShards []bnapi.BadShard
	for _, si := range shards {
		if err := mgr.inspectShard(ctx, cs, si); err != nil {
			if base.IsEIO(err) {
				return badShards, err
			}
			badShards = append(badShards, bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid, Err: err})
		}
	}
	return badShards, nil
}

// reInspectCRCMismatches re-inspects each CRC-mismatched shard individually to
// confirm the corruption before reporting it.
func (mgr *DataInspectMgr) reInspectCRCMismatches(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI, shards []*bnapi.ShardInfo, badBids []proto.BlobID) ([]bnapi.BadShard, error) {
	if len(badBids) == 0 {
		return nil, nil
	}
	span := trace.SpanFromContextSafe(ctx)
	badBidSet := make(map[proto.BlobID]struct{}, len(badBids))
	for _, bid := range badBids {
		badBidSet[bid] = struct{}{}
	}
	var badShards []bnapi.BadShard
	for _, si := range shards {
		if _, isBad := badBidSet[si.Bid]; !isBad {
			continue
		}
		span.Warnf("crc mismatch detected, re-inspecting shard. vuid:%d, bid:%d", cs.Vuid(), si.Bid)
		if err := mgr.inspectShard(ctx, cs, si); err != nil {
			if base.IsEIO(err) {
				return badShards, err
			}
			badShards = append(badShards, bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid, Err: err})
		}
	}
	return badShards, nil
}

// inspectBatch inspects one batch of shards with a single BatchRead call.
// lmt is this disk's inspect rate limiter (always non-nil from getLimiter).
func (mgr *DataInspectMgr) inspectBatch(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI,
	shards []*bnapi.ShardInfo, lmt *rate.Limiter,
) (badShards []bnapi.BadShard, ioErr error) {
	span := trace.SpanFromContextSafe(ctx)

	// build BidInfo; shards already filtered (Size > 0) by splitIntoBatches
	bids := make([]bnapi.BidInfo, len(shards))
	var totalSize int64
	for i, si := range shards {
		bids[i] = bnapi.BidInfo{Bid: si.Bid, Size: si.Size, Offset: si.Offset, Crc: si.Crc}
		totalSize += si.Size
	}

	// rate limit by total batch size
	remain := totalSize
	for remain > 0 && lmt != nil {
		tokenSz := lmt.Burst()
		if remain <= int64(tokenSz) {
			tokenSz = int(remain)
		}
		if err := lmt.WaitN(ctx, tokenSz); err != nil {
			span.Errorf("fail to limit batch inspect: %+v", err)
			return nil, err
		}
		remain -= int64(tokenSz)
	}

	crcWriter := newBatchCRCWriter(shards)
	batchShard, err := core.NewBatchShardReader(bids, cs.Vuid(), crcWriter, ds.GetConfig().BatchBufferSize)
	if err != nil {
		// offsets not monotonically increasing or invalid param; fallback per-shard
		span.Warnf("create batch reader failed, fallback per-shard. vuid:%d, err:%+v", cs.Vuid(), err)
		return mgr.fallbackInspectShards(ctx, cs, ds, shards)
	}

	if _, err = cs.BatchRead(ctx, batchShard); err != nil {
		if base.IsEIO(err) {
			return nil, err
		}
		if err == bloberr.ErrBidNotMatch {
			return mgr.handleBidNotMatch(ctx, cs, ds, shards, crcWriter, lmt)
		}
		span.Warnf("batch read failed, fallback per-shard. vuid:%d, err:%+v", cs.Vuid(), err)
		return mgr.fallbackInspectShards(ctx, cs, ds, shards)
	}

	return mgr.reInspectCRCMismatches(ctx, cs, ds, shards, crcWriter.badBids)
}

func (mgr *DataInspectMgr) handleBidNotMatch(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI,
	shards []*bnapi.ShardInfo, crcWriter *batchCRCWriter, lmt *rate.Limiter,
) (badShards []bnapi.BadShard, ioErr error) {
	span := trace.SpanFromContextSafe(ctx)
	failIdx := crcWriter.idx

	// reuse CRC results for shards already read before the failure
	var err error
	badShards, err = mgr.reInspectCRCMismatches(ctx, cs, ds, shards[:failIdx], crcWriter.badBids)
	if err != nil {
		return badShards, err
	}

	// the failing shard has a corrupted file header; confirm via per-shard flow
	if failIdx < len(shards) {
		span.Warnf("bid header mismatch, re-inspecting shard. vuid:%d, bid:%d", cs.Vuid(), shards[failIdx].Bid)
		bads, err := mgr.fallbackInspectShards(ctx, cs, ds, shards[failIdx:failIdx+1])
		badShards = append(badShards, bads...)
		if err != nil {
			return badShards, err
		}
	}

	// continue batch inspection for shards that were never read
	if failIdx+1 < len(shards) {
		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, shards[failIdx+1:], lmt)
		badShards = append(badShards, bads...)
		return badShards, ioErr
	}
	return badShards, nil
}

// inspectChunkFull runs a one-shot, full, non-persistent scan of a chunk (all shards, no
// cursor/window bookkeeping). Used only by the on-demand HTTP trigger; the
// scheduled background inspection goes through inspectDisk / inspectChunk and does
// track progress in the persisted InspectState.
func (mgr *DataInspectMgr) inspectChunkFull(pCtx context.Context, cs core.ChunkAPI) ([]bnapi.BadShard, error) {
	// This on-demand endpoint intentionally bypasses the background data-inspect
	// switch: it is an explicit operator request, not the scheduled task.
	span := trace.SpanFromContextSafe(pCtx)
	ds := cs.Disk()
	if ds.IsClosing() {
		span.Warnf("inspect chunk vuid:%d disk is closing, skip", cs.Vuid())
		return nil, core.ErrInspectStopped
	}

	var ctx context.Context
	span, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "", span.TraceID())
	span.Debugf("start to inspect chunk vuid:%d, chunk:%s.", cs.Vuid(), cs.ID())

	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	total := 0
	badShards := make([]bnapi.BadShard, 0)

	scanFn := func(batchShards []*bnapi.ShardInfo) error {
		total += len(batchShards)
		_, bads, ioErr := mgr.inspectShardsPage(ctx, cs, batchShards, pCtx)
		badShards = append(badShards, bads...)
		if ioErr == nil {
			return nil
		}
		if base.IsEIO(ioErr) || isInspectControlStop(ioErr) ||
			errors.Is(ioErr, context.Canceled) || errors.Is(ioErr, context.DeadlineExceeded) {
			return ioErr
		}
		span.Warnf("batch read io error, skip batch. vuid:%d, err:%+v", cs.Vuid(), ioErr)
		return nil
	}

	err := mgr.scanShards(ctx, cs, scanFn)
	mgr.reportBatchBadShards(ctx, cs, badShards)
	mgr.trySendCrcRepair(ctx, cs, badShards)
	span.Infof("finish to inspect chunk, vuid:%d, chunk:%s, total:%d, wrong:%d, err:%+v",
		cs.Vuid(), cs.ID(), total, len(badShards), err)
	return badShards, err
}

// trySendCrcRepair sends an in-place crc repair message to proxy for each
// confirmed CRC-corrupted shard.
func (mgr *DataInspectMgr) trySendCrcRepair(ctx context.Context, cs core.ChunkAPI, badShards []bnapi.BadShard) {
	if len(badShards) == 0 {
		return
	}
	sender := mgr.repairSender.get()
	if sender == nil {
		return
	}
	span := trace.SpanFromContextSafe(ctx)
	clusterID := cs.Disk().DiskInfo().ClusterID

	for _, bad := range badShards {
		// only data-corruption (crc) shards are repaired in place; other error
		// types are reported only and must not trigger an in-place rebuild.
		if !errors.Is(bad.Err, crc32block.ErrMismatchedCrc) {
			continue
		}
		idx := bad.Vuid.Index()
		// the inspect-crc reason tells the repairer to rebuild this idx in place
		// even though its meta still reports Normal.
		args := &proxy.ShardRepairArgs{
			ClusterID: clusterID,
			Bid:       bad.Bid,
			Vid:       bad.Vuid.Vid(),
			BadIdxes:  []uint8{idx},
			Reason:    proto.ShardRepairReasonInspectCrc,
		}
		if err := sender.SendShardRepairMsg(ctx, args); err != nil {
			span.Errorf("send crc repair msg failed, vuid:%d, bid:%d, err:%+v", bad.Vuid, bad.Bid, err)
			continue
		}
		span.Infof("send crc repair msg, vuid:%d, bid:%d, idx:%d", bad.Vuid, bad.Bid, idx)
	}
}

// inspectShard checks shard integrity and metadata double-check.
// Returns error if shard is corrupted, nil if healthy or deleted.
func (mgr *DataInspectMgr) inspectShard(ctx context.Context, cs core.ChunkAPI, si *bnapi.ShardInfo) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// Read shard data - normally succeeds; other, record error if data fail and meta not delete
	shardReader := core.NewShardReader(si.Bid, si.Vuid, 0, 0, io.Discard)
	if _, err = cs.Read(ctx, shardReader); err == nil {
		return nil
	}

	if base.IsShardDeleted(err) {
		span.Warnf("shard deleted, skip. vuid:%d, bid:%d, err:%+v", cs.Vuid(), si.Bid, err)
		return nil
	}
	if sm, metaErr := cs.ReadShardMeta(ctx, si.Bid); metaErr == nil && sm.Size == 0 {
		span.Warnf("shard overwritten empty, skip. vuid:%d, bid:%d", cs.Vuid(), si.Bid)
		return nil
	} else if base.IsShardDeleted(metaErr) {
		span.Warnf("shard meta deleted, skip. vuid:%d, bid:%d, err:%+v", cs.Vuid(), si.Bid, metaErr)
		return nil
	}

	return err
}

// scanShards pages through every normal shard in cs via ListShards, invoking fn once per
// page, until EOF or fn returns an error.
func (mgr *DataInspectMgr) scanShards(ctx context.Context, cs core.ChunkAPI, fn func([]*bnapi.ShardInfo) error) (err error) {
	startBid := proto.InValidBlobID
	for {
		shards, next, _err := cs.ListShards(ctx, startBid, listShardBatch, bnapi.ShardStatusNormal)
		if _err != nil {
			return _err
		}

		if err = fn(shards); err != nil {
			return err
		}
		startBid = next
		if next == proto.InValidBlobID {
			break
		}
	}
	return nil
}

// ---------------------------------------------------------------------------------------
// report path
// ---------------------------------------------------------------------------------------

func (mgr *DataInspectMgr) cleanDiskInspectMetric(ds core.DiskAPI, diskID proto.DiskID) {
	info := ds.DiskInfo()
	diskLabels := prometheus.Labels{
		"cluster_id": info.ClusterID.ToString(),
		"disk_id":    diskID.ToString(),
	}

	// Remove all inspect metric series for the dropped/replaced disk
	dataInspectBadShardByDiskVec.DeletePartialMatch(diskLabels)
	dataInspectBadShardByChunkVec.DeletePartialMatch(diskLabels)
	dataInspectBadVec.DeletePartialMatch(diskLabels)
	readBadShardVec.DeletePartialMatch(diskLabels)
}

// refreshDiskBadShardMetrics repopulates the disk-level and per-chunk bad-shard
// gauges of one disk from its current in-memory inspect state.
func (mgr *DataInspectMgr) refreshDiskBadShardMetrics(ctx context.Context, ds core.DiskAPI) {
	info := ds.DiskInfo()
	var totalBadBids int64
	ds.InspectState().RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		cs, ok := ds.GetChunkStorage(st.Vuid)
		if !ok || cs.Status() == clustermgr.ChunkStatusRelease {
			return true
		}
		badBids := int64(len(st.BadBids))
		totalBadBids += badBids
		dataInspectBadShardByChunkVec.WithLabelValues(dataInspectChunkLabelValues(info, st.Vuid)...).
			Set(float64(badBids))
		return true
	})
	dataInspectBadShardByDiskVec.WithLabelValues(dataInspectDiskLabelValues(info)...).Set(float64(totalBadBids))
}

// reconcileBadBidsAtStartup check persisted bad bids when service starts.
// Bad bids may be fixed and deleted in memory but failed to persist. Disks are
// reconciled in parallel and per-disk gauges are refreshed as each disk's
// reconcile finishes.
func (mgr *DataInspectMgr) reconcileBadBidsAtStartup(ctx context.Context) {
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	// block inspect rounds until this one-shot pass finishes
	mgr.setStartupReconciling(true)
	defer mgr.setStartupReconciling(false)
	span := trace.SpanFromContextSafe(ctx)

	disks := mgr.svr.copyDiskStorages(ctx)
	wg := &sync.WaitGroup{}
	for _, ds := range disks {
		if ds.IsClosing() {
			continue
		}
		wg.Add(1)
		go func(ds core.DiskAPI) {
			defer wg.Done()
			if err := mgr.reconcileDiskBadBids(ctx, ds); err != nil {
				span.Warnf("inspect disk:%d startup badBids reconcile failed: %+v", ds.ID(), err)
			}
		}(ds)
	}
	wg.Wait()
	span.Infof("startup bad-bid reconcile done, disks: %d", len(disks))
}

func (mgr *DataInspectMgr) setStartupReconciling(reconciling bool) {
	if reconciling {
		atomic.StoreInt32(&mgr.reconcilingStartup, 1)
	} else {
		atomic.StoreInt32(&mgr.reconcilingStartup, 0)
	}
}

// isStartupReconciling reports whether the startup bad-bid reconcile is running
func (mgr *DataInspectMgr) isStartupReconciling() bool {
	return atomic.LoadInt32(&mgr.reconcilingStartup) == 1
}

// reconcileDiskBadBids check badBids of all live chunks for the disk, persists
// the cleaned state, and then refresh the disk's badBids gauges. This does not
// consult the inspect switch: this is the startup one-shot pass.
func (mgr *DataInspectMgr) reconcileDiskBadBids(ctx context.Context, ds core.DiskAPI) error {
	span := trace.SpanFromContextSafe(ctx)

	// collect only the vuids that have bad bids to minimize lock hold time
	var vuids []proto.Vuid
	ds.InspectState().RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		if len(st.BadBids) > 0 {
			vuids = append(vuids, st.Vuid)
		}
		return true
	})

	var total, remain int
	for _, vuid := range vuids {
		// skip chunks which are no longer alive on this disk
		cs, ok := ds.GetChunkStorage(vuid)
		if !ok || cs.Status() == clustermgr.ChunkStatusRelease {
			continue
		}

		st, err := ds.InspectState().LoadInspectChunkState(ctx, vuid)
		if err != nil {
			span.Warnf("inspect disk:%d vuid:%d load inspect state failed: %+v", ds.ID(), vuid, err)
			continue
		}
		if len(st.BadBids) == 0 {
			continue
		}

		total += len(st.BadBids)
		cleared, err := mgr.reconcileBadBids(ctx, ds, cs, &st, false)
		if err != nil {
			// may be the disk is closing or not writable
			if errors.Is(err, core.ErrInspectStopped) {
				span.Infof("inspect disk:%d startup bad-bid reconcile stopped", ds.ID())
				break
			}
			span.Warnf("inspect disk:%d vuid:%d startup bad-bid reconcile failed: %+v", ds.ID(), vuid, err)
			return err
		}
		remain += len(st.BadBids) - cleared
	}

	// persist the cleaned state so a later restart does not reload the stale
	// records again, then rebuild the gauges from the reconciled state
	ds.InspectState().FlushInspectState(ctx)
	mgr.refreshDiskBadShardMetrics(ctx, ds)
	span.Infof("inspect disk:%d startup bad-bid reconcile done, %d/%d", ds.ID(), remain, total)
	return nil
}

// onBadBidRepaired deletes a tracked bad bid as soon as its shard has been
// successfully rewritten (the crc-repair write-back goes through ShardPut with
// the same bid), so the bad-shard gauges drop right after the repair instead of
// waiting for the next inspect round or cycle. Regular puts never match this.
// Only this chunk's gauge is refreshed here; the disk-level gauge is
// recalibrated by logRoundSumm at the end of each inspect round for performance.
func (mgr *DataInspectMgr) onBadBidRepaired(ctx context.Context, ds core.DiskAPI, cs core.ChunkAPI, bid proto.BlobID) {
	span := trace.SpanFromContextSafe(ctx)

	cleared, err := ds.InspectState().DeleteBadBid(ctx, cs.Vuid(), bid)
	if err != nil {
		span.Errorf("inspect disk:%d vuid:%d bid:%d delete bad bid after repair failed: %+v", ds.ID(), cs.Vuid(), bid, err)
		return
	}
	if !cleared {
		return
	}

	mgr.updateBadShardByChunk(ctx, cs)
	span.Infof("inspect disk:%d vuid:%d bid:%d repaired, deleted bad bid from inspect_state", ds.ID(), cs.Vuid(), bid)
}

// reportBadShard is called when  user read (Get/Put) finds a bad shard.
// It records the bad bid to the local log and increments readBadShardVec (user-read metric).
func (mgr *DataInspectMgr) reportBadShard(ctx context.Context, cs core.ChunkAPI, blobID proto.BlobID, err error) {
	span := trace.SpanFromContextSafe(ctx)
	// don't report this error
	if isInspectReportIgnoredError(err) {
		return
	}

	// report one bad shard, when the upper-level user at get/put, an error was found
	// It's possible that this disk has inspected this bid error before, or it might not.
	// Report with "add" and combine it with "record" for analysis and processing
	diskInfo := cs.Disk().DiskInfo()
	mgr.recordBadBids(ctx, diskInfo, cs.Vuid(), []string{blobID.ToString()}, err.Error())

	// track the bid in inspect state so queries/metrics see it and
	// reconcileBadBids re-checks it until repaired.
	badBidMeta := core.BadBidMeta{
		FoundAt: time.Now().UnixNano(),
		Reason:  err.Error(),
	}

	if _, aerr := cs.Disk().InspectState().AddBadBid(ctx, cs.Vuid(), blobID, badBidMeta); aerr != nil {
		span.Errorf("inspect disk:%d vuid:%d bid:%d add bad bid failed: %+v",
			diskInfo.DiskID, cs.Vuid(), blobID, aerr)
	}
	mgr.updateBadShardByChunk(ctx, cs)

	// crc-mismatch shard is handled by in-place repair, so it is not reported to the metric
	// inspect will find this and send repair msg
	if errors.Is(err, crc32block.ErrMismatchedCrc) {
		return
	}

	// add couter for user read met bad shards
	reportReadBadShard(diskInfo)
}

// reportBatchBadShards aggregates a batch of bad shards from the same chunk and
// reports them together, because repair is typically at chunk granularity.
func (mgr *DataInspectMgr) reportBatchBadShards(ctx context.Context, cs core.ChunkAPI, items []bnapi.BadShard) int {
	if len(items) == 0 {
		return 0
	}
	span := trace.SpanFromContextSafe(ctx)

	// Under each error, aggregate the bid of that error type
	// e.g. {
	//          "err 11": ["bid1", "2", "3"],
	//          "err 22": ["bid66", "77", "88"],
	//      }
	uniqueErr := map[string][]string{}
	metricBadBid := 0 // count of bad bids reported to metric, excluding crc (repaired in place)
	for _, item := range items {
		if isInspectReportIgnoredError(item.Err) {
			continue
		}

		uniqueErr[item.Err.Error()] = append(uniqueErr[item.Err.Error()], item.Bid.ToString())
		span.Errorf("inspect blob error, bad shard:%v", item)

		// crc-mismatch shards are handled by in-place repair, so they are still
		// recorded to the local log but not reported to the cumulative metric
		// (which cannot be cleaned after repair).
		if !errors.Is(item.Err, crc32block.ErrMismatchedCrc) {
			metricBadBid++
		}
	}

	if len(uniqueErr) == 0 {
		return 0
	}

	// record local log (includes crc-mismatch shards)
	totalBadBid, info := 0, cs.Disk().DiskInfo()
	for errStr, bids := range uniqueErr {
		totalBadBid += len(bids)
		mgr.RecordBadBids(ctx, info, cs.Vuid(), bids, errStr)
	}

	// all non-ignored errors (including crc-mismatch) increment the inspect bad counter.
	if totalBadBid > 0 {
		mgr.AddInspectBadMetric(info, totalBadBid)
	}

	span.Errorf("inspect blob error, total bad count:%d, non-crc count:%d", totalBadBid, metricBadBid)
	return totalBadBid
}

type badBidRecord struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	DiskID    proto.DiskID    `json:"disk_id"`
	Vuid      proto.Vuid      `json:"vuid"`
	Timestamp int64           `json:"ts"`
	Bids      string          `json:"bids"`
	Reason    string          `json:"reason"`
}

func (mgr *DataInspectMgr) recordBadBids(ctx context.Context, info clustermgr.BlobNodeDiskInfo, vuid proto.Vuid, bids []string, errStr string) {
	span := trace.SpanFromContextSafe(ctx)

	// record local log
	record := badBidRecord{
		ClusterID: info.ClusterID,
		DiskID:    info.DiskID,
		Vuid:      vuid,
		Bids:      strings.Join(bids, ","),
		Timestamp: time.Now().Unix(),
		Reason:    errStr,
	}
	if err := mgr.recorder.Encode(record); err != nil {
		span.Errorf("fail to write bad blob inspect record: [%v], err[%+v]", record, err)
	}
}

func (mgr *DataInspectMgr) getSwitch() bool {
	return mgr.taskSwitch.Enabled()
}

// setLimiters create inspect rate limiters at the start of each round.
func (mgr *DataInspectMgr) setLimiters(disks []core.DiskAPI) {
	mgr.limitsMu.Lock()
	defer mgr.limitsMu.Unlock()
	rateLimit := mgr.inspectRateLimit()
	for _, ds := range disks {
		if _, ok := mgr.limits[ds.ID()]; !ok {
			mgr.limits[ds.ID()] = rate.NewLimiter(rate.Limit(rateLimit), 2*rateLimit)
		}
	}
}

func (mgr *DataInspectMgr) getLimiter(ds core.DiskAPI) *rate.Limiter {
	mgr.limitsMu.Lock()
	defer mgr.limitsMu.Unlock()

	if lmt, ok := mgr.limits[ds.ID()]; ok {
		return lmt
	}
	rateLimit := mgr.inspectRateLimit()
	lmt := rate.NewLimiter(rate.Limit(rateLimit), 2*rateLimit)
	mgr.limits[ds.ID()] = lmt
	return mgr.limits[ds.ID()]
}

// inspectRateLimit returns the configured inspect rate, falling back to the
// system default when the config is unset or invalid.
func (mgr *DataInspectMgr) inspectRateLimit() int {
	if mgr.conf.RateLimit > 0 {
		return mgr.conf.RateLimit
	}
	return defaultInspectRate
}

// setAllDiskRateForce updates the rate of every already-created limiter; limiters
// created later pick up the new configured rate lazily.
func (mgr *DataInspectMgr) setAllDiskRateForce(newLimit int) {
	mgr.limitsMu.Lock()
	defer mgr.limitsMu.Unlock()
	mgr.conf.RateLimit = newLimit
	for _, lmt := range mgr.limits {
		lmt.SetLimit(rate.Limit(newLimit))
		lmt.SetBurst(2 * newLimit)
	}
}

func (mgr *DataInspectMgr) inspectCycleDays() int {
	mgr.limitsMu.Lock()
	defer mgr.limitsMu.Unlock()
	return mgr.conf.CycleDays
}

// SetCycleDays updates the full-inspect cycle length (in days) at runtime.
func (mgr *DataInspectMgr) SetCycleDays(days int) {
	mgr.limitsMu.Lock()
	defer mgr.limitsMu.Unlock()
	mgr.conf.CycleDays = days
}

// dataInspectConf returns a copy of the mutable inspect config under the same lock
// used by the runtime update APIs.
func (mgr *DataInspectMgr) dataInspectConf() DataInspectConf {
	mgr.limitsMu.Lock()
	defer mgr.limitsMu.Unlock()
	return mgr.conf
}

func (s *Service) SetInspectRate(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(bnapi.InspectRateArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.Rate < minRateLimit {
		span.Errorf("rate value is too small: %d, min rate %d", args.Rate, minRateLimit)
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	span.Infof("set data inspect rate args: %+v", args)

	s.inspectMgr.setAllDiskRateForce(args.Rate)
	c.Respond()
}

func (s *Service) SetInspectCycle(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(bnapi.InspectCycleArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.Days <= 0 {
		span.Errorf("cycle days value must be positive: %d", args.Days)
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	span.Infof("set data inspect cycle args: %+v", args)

	s.inspectMgr.SetCycleDays(args.Days)
	c.Respond()
}

// GetInspectStat get data inspection state: switch open, rate, interval, etc.
func (s *Service) GetInspectStat(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	stat := DataInspectStat{
		DataInspectConf: s.inspectMgr.dataInspectConf(),
		Open:            s.inspectMgr.getSwitch(),
	}
	span.Infof("data inspect args: %+v", stat)
	c.RespondJSON(&stat)
}

// CleanInspectMetric set diskID metric is zero, maybe disk is broken/repaired and replace new disk with another diskID
// 'localhost:${port}/inspect/cleanmetric?diskid=2'
func (s *Service) CleanInspectMetric(c *rpc.Context) {
	args := new(bnapi.InspectCleanMetricArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span := trace.SpanFromContextSafe(c.Request.Context())
	span.Infof("clean data inspect metric args: %+v", args)

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

	s.inspectMgr.cleanDiskInspectMetric(ds, ds.ID())
	c.Respond()
}

// ---------------------------------------------------------------------------------------
// query API
// ---------------------------------------------------------------------------------------

func (s *Service) listInspectDiskStates(ctx context.Context) (map[proto.DiskID]core.InspectDiskState, error) {
	disks := s.copyDiskStorages(ctx)
	sort.Slice(disks, func(i, j int) bool { return disks[i].ID() < disks[j].ID() })

	states := make(map[proto.DiskID]core.InspectDiskState, len(disks))
	for _, ds := range disks {
		if ds.IsClosing() {
			continue
		}

		st, err := ds.InspectState().LoadInspectDiskState(ctx)
		if err != nil {
			return nil, err
		}
		states[ds.ID()] = st
	}
	return states, nil
}

// listInspectChunkStates returns the persisted inspect state of every chunk on the disk.
func (s *Service) listInspectChunkStates(ctx context.Context, ds core.DiskAPI) (map[proto.Vuid]core.InspectChunkState, error) {
	states := make(map[proto.Vuid]core.InspectChunkState)
	if err := ds.InspectState().RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		states[st.Vuid] = *st
		return true
	}); err != nil {
		return nil, err
	}
	return states, nil
}

// GetInspectChunkState returns the persisted inspect state of one chunk (vuid),
// or of all chunks on the disk when vuid=0. The response is always a map keyed
// by vuid.
// 'localhost:${port}/inspect/stat/diskid/1/vuid/10001'
// 'localhost:${port}/inspect/stat/diskid/1/vuid/0'
func (s *Service) GetInspectChunkState(c *rpc.Context) {
	args := new(bnapi.ChunkInspectArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
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
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	if ds.IsClosing() {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	// vuid == 0 means return the inspect states of all chunks under this disk.
	if args.Vuid == 0 {
		states, err := s.listInspectChunkStates(c.Request.Context(), ds)
		if err != nil {
			c.RespondError(err)
			return
		}
		c.RespondJSON(states)
		return
	}

	if !args.Vuid.IsValid() {
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	st, err := ds.InspectState().LoadInspectChunkState(c.Request.Context(), args.Vuid)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(map[proto.Vuid]core.InspectChunkState{args.Vuid: st})
}

// GetInspectDiskState returns the persisted disk-level inspect state for one disk,
// or all disk states when diskid=0. The response is always a map keyed by disk id.
// 'localhost:${port}/inspect/stat/diskid/1'
// 'localhost:${port}/inspect/stat/diskid/0'
func (s *Service) GetInspectDiskState(c *rpc.Context) {
	args := new(bnapi.DiskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if args.DiskID == 0 {
		states, err := s.listInspectDiskStates(c.Request.Context())
		if err != nil {
			c.RespondError(err)
			return
		}
		c.RespondJSON(states)
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
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}
	if ds.IsClosing() {
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	st, err := ds.InspectState().LoadInspectDiskState(c.Request.Context())
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(map[proto.DiskID]core.InspectDiskState{args.DiskID: st})
}

func isInspectReportIgnoredError(err error) bool {
	// It may expand other errors, deleted shard, and so on
	return base.IsShardDeleted(err)
}
