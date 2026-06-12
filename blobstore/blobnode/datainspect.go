package blobnode

import (
	"context"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	listShardBatch       = 100
	minRateLimit         = 64 * 1024 // 64 KB/s
	defaultBatchReadSize = 16 << 20  // 16 MB
)

var (
	dataInspectMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "data_inspect",
			Help:      "blobnode data inspect",
		},
		[]string{"cluster_id", "disk_id"},
	)
	errServiceClosed = errors.New("service is closed")
)

type DataInspectConf struct {
	IntervalSec   int   `json:"interval_sec"`    // wait switch interval
	RateLimit     int   `json:"rate_limit"`      // max rate limit per second
	NextRoundSec  int   `json:"next_round_sec"`  // wait next round inspect interval
	BatchReadSize int64 `json:"batch_read_size"` // max data bytes per BatchRead call; default 16MB

	Record recordlog.Config `json:"record"`
}

type DataInspectStat struct {
	DataInspectConf
	Open     bool                 `json:"open"`
	Progress map[proto.DiskID]int `json:"progress"`
}

type DataInspectMgr struct {
	conf   DataInspectConf
	limits map[proto.DiskID]*rate.Limiter

	svr        *Service
	taskSwitch *taskswitch.TaskSwitch

	round    uint64            // round of data inspect
	progress sync.Map          // progress of data inspect: diskID -> progress[0, 100]
	recorder recordlog.Encoder // local record log
}

func NewDataInspectMgr(svr *Service, conf DataInspectConf, switchMgr *taskswitch.SwitchMgr) (*DataInspectMgr, error) {
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
		conf.BatchReadSize = defaultBatchReadSize
	}

	mgr := &DataInspectMgr{
		conf:       conf,
		limits:     make(map[proto.DiskID]*rate.Limiter),
		svr:        svr,
		taskSwitch: taskSwitch,
		recorder:   recorder,
	}
	return mgr, nil
}

func (mgr *DataInspectMgr) loopDataInspect() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "Inspect")
	t := time.NewTicker(time.Duration(mgr.conf.IntervalSec) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if mgr.getSwitch() {
				mgr.inspectAllDisks(ctx)
			}

		case <-mgr.svr.closeCh:
			mgr.recorder.Close()
			span.Warn("loop inspect data closed.")
			return
		}
	}
}

func (mgr *DataInspectMgr) inspectAllDisks(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	span.Warn("start to inspect all disks.")
	disks := mgr.svr.copyDiskStorages(ctx)
	mgr.setLimiters(disks)

	mgr.prepareDiskInspectionState(disks)
	mgr.recordInspectStartPoint(ctx)
	defer mgr.roundIncrease()

	var wg sync.WaitGroup
	for _, ds := range disks {
		if !ds.IsWritable() { // not normal disk, skip it.
			continue
		}
		wg.Add(1)
		go mgr.inspectDisk(ds, &wg)
	}

	wg.Wait()
	if !mgr.getSwitch() {
		span.Warn("stop to inspect disks.")
		return
	}

	// set progress to 100%, work inspect done
	mgr.progress.Range(func(key, value interface{}) bool {
		mgr.progress.Store(key.(proto.DiskID), 100)
		return true
	})
	span.Warn("finish to inspect all disks.")
	mgr.waitNextRoundInspect()
}

func (mgr *DataInspectMgr) inspectDisk(ds core.DiskAPI, wg *sync.WaitGroup) {
	defer wg.Done()
	span, ctx := trace.StartSpanFromContextWithTraceID(
		context.Background(), "", ds.ID().ToString()+"_Inspect_"+trace.RandomID().String())

	span.Infof("start to inspect disk:%d", ds.ID())
	defer span.Infof("finish to inspect disk:%d", ds.ID())
	// clean metric
	mgr.cleanDiskInspectMetric(ds, ds.ID())

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("ListChunks error:%+v", err)
		return
	}

	step := len(chunks) / 20
	for i, chunk := range chunks {
		// report progress: per 5% percent, or last chunk
		if (step != 0 && (i+1)%step == 0) || i == len(chunks)-1 {
			span.Warnf("chunk inspcet progress: %d / %d", i+1, len(chunks))
		}
		mgr.progress.Store(ds.ID(), 100*(i+1)/len(chunks))

		if chunk.Status == clustermgr.ChunkStatusRelease {
			continue
		}
		cs, found := ds.GetChunkStorage(chunk.Vuid)
		if !found {
			span.Errorf("inspect vuid:%d not found", chunk.Vuid)
			continue
		}
		if !cs.Disk().IsWritable() { // not normal disk, skip it.
			span.Warn("disk is broken, skip inspect chunk")
			return
		}

		if _, err = mgr.inspectChunk(ctx, cs); err != nil {
			span.Errorf("inspect chunk error:%+v", err)
			return
		}
		if !mgr.getSwitch() {
			return
		}
	}
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
) []bnapi.BadShard {
	var badShards []bnapi.BadShard
	for _, si := range shards {
		if err := mgr.inspectShard(ctx, cs, si); err != nil {
			badShards = append(badShards, bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid, Err: err})
		}
	}
	return badShards
}

func (mgr *DataInspectMgr) reInspectCRCMismatches(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI, shards []*bnapi.ShardInfo, badBids []proto.BlobID, lmt *rate.Limiter) []bnapi.BadShard {
	if len(badBids) == 0 {
		return nil
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
			badShards = append(badShards, bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid, Err: err})
		}
	}
	return badShards
}

func (mgr *DataInspectMgr) inspectBatch(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI, shards []*bnapi.ShardInfo, lmt *rate.Limiter) (badShards []bnapi.BadShard, ioErr error) {
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
		return mgr.fallbackInspectShards(ctx, cs, ds, shards), nil
	}

	if _, err = cs.BatchRead(ctx, batchShard); err != nil {
		if base.IsEIO(err) {
			return nil, err
		}
		if err == bloberr.ErrBidNotMatch {
			return mgr.handleBidNotMatch(ctx, cs, ds, shards, crcWriter, lmt)
		}
		span.Warnf("batch read failed, fallback per-shard. vuid:%d, err:%+v", cs.Vuid(), err)
		return mgr.fallbackInspectShards(ctx, cs, ds, shards), nil
	}

	return mgr.reInspectCRCMismatches(ctx, cs, ds, shards, crcWriter.badBids, lmt), nil
}

func (mgr *DataInspectMgr) handleBidNotMatch(ctx context.Context, cs core.ChunkAPI, ds core.DiskAPI,
	shards []*bnapi.ShardInfo, crcWriter *batchCRCWriter, lmt *rate.Limiter,
) (badShards []bnapi.BadShard, ioErr error) {
	span := trace.SpanFromContextSafe(ctx)
	failIdx := crcWriter.idx

	// reuse CRC results for shards already read before the failure
	badShards = mgr.reInspectCRCMismatches(ctx, cs, ds, shards[:failIdx], crcWriter.badBids, lmt)

	// the failing shard has a corrupted file header; confirm via per-shard flow
	if failIdx < len(shards) {
		span.Warnf("bid header mismatch, re-inspecting shard. vuid:%d, bid:%d", cs.Vuid(), shards[failIdx].Bid)
		bads := mgr.fallbackInspectShards(ctx, cs, ds, shards[failIdx:failIdx+1])
		badShards = append(badShards, bads...)
	}

	// continue batch inspection for shards that were never read
	if failIdx+1 < len(shards) {
		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, shards[failIdx+1:], lmt)
		badShards = append(badShards, bads...)
		return badShards, ioErr
	}
	return badShards, nil
}

func (mgr *DataInspectMgr) inspectChunk(pCtx context.Context, cs core.ChunkAPI) ([]bnapi.BadShard, error) {
	span := trace.SpanFromContextSafe(pCtx)
	ctx, cancel := context.WithCancel(context.Background())
	span, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", span.TraceID())
	span.Debugf("start to inspect chunk vuid:%d, chunk:%s.", cs.Vuid(), cs.ID())

	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	ds := cs.Disk()
	total := 0
	badShards := make([]bnapi.BadShard, 0)

	scanFn := func(batchShards []*bnapi.ShardInfo) (err error) {
		total += len(batchShards)
		for _, batch := range splitIntoBatches(batchShards, mgr.conf.BatchReadSize) {
			select {
			case <-pCtx.Done():
				span.Warnf("inspect chunk stop, upper context canceled. vuid:%d, chunk:%s.", cs.Vuid(), cs.ID())
				return pCtx.Err()
			case <-mgr.svr.closeCh:
				cancel()
				span.Warnf("inspect chunk stop, service is closed. vuid:%d, chunk:%s.", cs.Vuid(), cs.ID())
				return errServiceClosed
			default:
			}

			bads, ioErr := mgr.inspectBatch(ctx, cs, ds, batch, mgr.getLimiter(ds))
			badShards = append(badShards, bads...)
			if ioErr != nil {
				span.Warnf("batch read io error, skip batch. vuid:%d, err:%+v", cs.Vuid(), ioErr)
			}
		}
		return nil
	}

	err := mgr.scanShards(ctx, cs, scanFn)
	mgr.reportBatchBadShards(ctx, cs, badShards)
	span.Infof("finish to inspect chunk, vuid:%d, chunk:%s, total:%d, wrong:%d, err:%+v",
		cs.Vuid(), cs.ID(), total, len(badShards), err)
	return badShards, err
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

		if !mgr.getSwitch() {
			return nil
		}
	}
	return nil
}

func (mgr *DataInspectMgr) waitNextRoundInspect() {
	t := time.NewTimer(time.Duration(mgr.conf.NextRoundSec) * time.Second) // wait next round inspect
	defer t.Stop()

	select {
	case <-t.C:
	case <-mgr.svr.closeCh:
	}
}

func (mgr *DataInspectMgr) cleanDiskInspectMetric(ds core.DiskAPI, diskID proto.DiskID) {
	dataInspectMetric.WithLabelValues(
		ds.DiskInfo().ClusterID.ToString(),
		diskID.ToString(),
	).Set(0)
}

// It was reported only once. When the upper-level user at get/put, an error was found
func (mgr *DataInspectMgr) reportBadShard(ctx context.Context, cs core.ChunkAPI, blobID proto.BlobID, err error) {
	// don't report this error
	if isInspectReportIgnoredError(err) {
		return
	}

	// report one bad shard, when the upper-level user at get/put, an error was found
	// It's possible that this disk has inspected this bid error before, or it might not.
	// Report with "add" and combine it with "record" for analysis and processing
	diskInfo := cs.Disk().DiskInfo()
	mgr.recordBadBids(ctx, cs, []string{blobID.ToString()}, err.Error())
	dataInspectMetric.WithLabelValues(
		diskInfo.ClusterID.ToString(),
		diskInfo.DiskID.ToString(),
	).Add(1)
}

// Aggregate a batch of errors and report them all at once(the same chunk), Because the repair of data is often at the granularity of chunks
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
	for _, item := range items {
		if isInspectReportIgnoredError(item.Err) {
			continue
		}

		uniqueErr[item.Err.Error()] = append(uniqueErr[item.Err.Error()], item.Bid.ToString())
		span.Errorf("inspect blob error, bad shard:%v", item)
	}

	if len(uniqueErr) == 0 {
		return 0
	}

	// record local log
	totalBadBid, diskInfo := 0, cs.Disk().DiskInfo()
	for errStr, bids := range uniqueErr {
		totalBadBid += len(bids)
		mgr.recordBadBids(ctx, cs, bids, errStr)
	}

	// report metric
	dataInspectMetric.WithLabelValues(
		diskInfo.ClusterID.ToString(),
		diskInfo.DiskID.ToString(),
	).Add(float64(totalBadBid))

	span.Errorf("inspect blob error, total bad count:%d", totalBadBid)
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

func (mgr *DataInspectMgr) recordBadBids(ctx context.Context, cs core.ChunkAPI, bids []string, errStr string) {
	span := trace.SpanFromContextSafe(ctx)

	// record local log
	diskInfo := cs.Disk().DiskInfo()
	record := badBidRecord{
		ClusterID: diskInfo.ClusterID,
		DiskID:    diskInfo.DiskID,
		Vuid:      cs.Vuid(),
		Bids:      strings.Join(bids, ","),
		Timestamp: time.Now().Unix(),
		Reason:    errStr,
	}
	if err := mgr.recorder.Encode(record); err != nil {
		span.Errorf("fail to write bad blob inspect record: [%v], err[%+v]", record, err)
	}
}

type roundRecord struct {
	Round     uint64 `json:"round"`
	Timestamp int64  `json:"timestamp"`
}

func (mgr *DataInspectMgr) recordInspectStartPoint(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	record := roundRecord{Round: mgr.round, Timestamp: time.Now().Unix()}

	if err := mgr.recorder.Encode(record); err != nil {
		span.Errorf("fail to write inspect round record: [%v], err[%+v]", record, err)
	}
}

func (mgr *DataInspectMgr) prepareDiskInspectionState(disks []core.DiskAPI) {
	// clear previous progress, may be replaced a new disk, need remove old	progress
	mgr.progress.Range(func(k, _ interface{}) bool {
		mgr.progress.Delete(k)
		return true
	})

	// init or reset progress to 0% by current all disks, at the beginning of each new round of inspection
	for _, ds := range disks {
		mgr.progress.Store(ds.ID(), 0)
	}
}

func (mgr *DataInspectMgr) setLimiters(disks []core.DiskAPI) {
	for _, ds := range disks {
		if _, ok := mgr.limits[ds.ID()]; !ok {
			mgr.limits[ds.ID()] = rate.NewLimiter(rate.Limit(mgr.conf.RateLimit), 2*mgr.conf.RateLimit)
		}
	}
}

func (mgr *DataInspectMgr) getLimiter(ds core.DiskAPI) *rate.Limiter {
	return mgr.limits[ds.ID()]
}

func (mgr *DataInspectMgr) getSwitch() bool {
	return mgr.taskSwitch.Enabled()
}

func (mgr *DataInspectMgr) roundIncrease() {
	mgr.round++
}

func (mgr *DataInspectMgr) setAllDiskRateForce(newLimit int) {
	for _, lmt := range mgr.limits {
		lmt.SetLimit(rate.Limit(newLimit))
		lmt.SetBurst(2 * newLimit)
	}
	mgr.conf.RateLimit = newLimit
}

func (s *Service) SetInspectRate(c *rpc.Context) {
	args := new(bnapi.InspectRateArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.Rate < minRateLimit {
		c.RespondError(errors.New("rate value is too small"))
		return
	}

	span := trace.SpanFromContextSafe(c.Request.Context())
	span.Infof("set data inspect rate args: %+v", args)
	s.inspectMgr.setAllDiskRateForce(args.Rate)
	c.Respond()
}

// GetInspectStat get data inspection state: switch open, rate, progress, interval, etc.
func (s *Service) GetInspectStat(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	progress := make(map[proto.DiskID]int)
	s.inspectMgr.progress.Range(func(k, v interface{}) bool {
		progress[k.(proto.DiskID)] = v.(int)
		return true
	})

	stat := DataInspectStat{
		DataInspectConf: s.inspectMgr.conf,
		Open:            s.inspectMgr.getSwitch(),
		Progress:        progress,
	}
	span.Infof("data inspect args: %+v", stat)
	c.RespondJSON(&stat)
}

// CleanInspectMetric set diskID metric is zero, maybe disk is broken/repaired and replace new disk with another diskID
// 'localhost:${port}/inspect/cleanmetric?disk_id=2'
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

func init() {
	prometheus.MustRegister(dataInspectMetric)
}

func isInspectReportIgnoredError(err error) bool {
	// It may expand other errors, deleted shard, and so on
	return base.IsShardDeleted(err)
}
