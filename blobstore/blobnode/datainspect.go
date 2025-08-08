package blobnode

import (
	"context"
	"errors"
	"io"
	"os"
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
	listShardBatch = 100
	minRateLimit   = 64 * 1024 // 64 KB/s
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
	IntervalSec int `json:"interval_sec"`   // wait switch interval
	RateLimit   int `json:"rate_limit"`     // max rate limit per second
	NexRoundSec int `json:"next_round_sec"` // wait next round inspect interval

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

	round     uint64                     // round of data inspect
	recorder  recordlog.Encoder          // local record log
	inspected map[proto.DiskID]*sync.Map // inspected bad blobs
	progress  map[proto.DiskID]int       // progress of data inspect
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

	mgr := &DataInspectMgr{
		conf:       conf,
		limits:     make(map[proto.DiskID]*rate.Limiter),
		svr:        svr,
		taskSwitch: taskSwitch,
		recorder:   recorder,
		inspected:  make(map[proto.DiskID]*sync.Map),
		progress:   make(map[proto.DiskID]int),
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

	mgr.recordInspectStartPoint(ctx)
	defer func() { mgr.round++ }()

	var wg sync.WaitGroup
	for _, ds := range disks {
		mgr.progress[ds.ID()] = 0 // set progress to 0%, will start work inspect
		if !ds.IsWritable() {     // not normal disk, skip it.
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

	for id := range mgr.progress {
		mgr.progress[id] = 100 // set progress to 100%, work inspect done
	}
	span.Warn("finish to inspect all disks.")
	mgr.waitNextRoundInspect()
}

func (mgr *DataInspectMgr) inspectDisk(ds core.DiskAPI, wg *sync.WaitGroup) {
	defer wg.Done()
	span, ctx := trace.StartSpanFromContextWithTraceID(
		context.Background(), "", ds.ID().ToString()+"_Inspect_"+trace.RandomID().String())

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
		mgr.progress[ds.ID()] = 100 * (i + 1) / len(chunks)

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

func (mgr *DataInspectMgr) inspectChunk(pCtx context.Context, cs core.ChunkAPI) ([]bnapi.BadShard, error) {
	span := trace.SpanFromContextSafe(pCtx)
	ctx, cancel := context.WithCancel(context.Background())
	span, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", span.TraceID())
	span.Debugf("start to inspect chunk vuid:%d, chunkid:%s.", cs.Vuid(), cs.ID())

	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	ds := cs.Disk()
	total := 0
	badShards := make([]bnapi.BadShard, 0)
	inspected := mgr.getInspectedBlobs(ds.ID())

	scanFn := func(batchShards []*bnapi.ShardInfo) (err error) {
		total += len(batchShards)
		for _, si := range batchShards {
			_, exist := inspected.Load(si.Bid)
			if si.Size <= 0 || exist {
				continue
			}

			select {
			case <-pCtx.Done():
				span.Warnf("inspect chunk stop, upper level has context canceled. vuid:%d,chunkid:%s.", cs.Vuid(), cs.ID())
				return pCtx.Err()
			case <-mgr.svr.closeCh:
				cancel()
				span.Warnf("inspect chunk stop, service is closed. vuid:%d, chunkid:%s.", cs.Vuid(), cs.ID())
				return errServiceClosed
			default:
			}

			if err = mgr.inspectShard(ctx, cs, si, mgr.getLimiter(ds)); err != nil {
				// add bad bids of chunk, to this disk. prevent next round will report these bids, it repeated
				inspected.Store(si.Bid, struct{}{})
				badShards = append(badShards, bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid, Err: err})
				if base.IsEIO(err) {
					return err
				}
			}
		}
		return nil
	}

	err := mgr.scanShards(ctx, cs, scanFn)
	mgr.reportBatchBadShards(ctx, cs, badShards)
	span.Infof("finish to inspect chunk, vuid:%d, chunkid:%s, total:%d, wrong:%d, err:%+v",
		cs.Vuid(), cs.ID(), total, len(badShards), err)
	return badShards, err
}

func (mgr *DataInspectMgr) inspectShard(ctx context.Context, cs core.ChunkAPI, si *bnapi.ShardInfo, lmt *rate.Limiter) (err error) {
	discard := io.Discard
	shardReader := core.NewShardReader(si.Bid, si.Vuid, 0, 0, discard)

	// Tokens of the corresponding size are obtained based on the size of the shard.
	// If the size of shard is 1MB, you need to get 1024*1024 tokens
	remain := si.Size
	tokenSz := lmt.Burst()
	for remain > 0 {
		if remain <= int64(tokenSz) {
			tokenSz = int(remain)
		}
		lmt.WaitN(ctx, tokenSz)
		remain -= int64(tokenSz)
	}

	_, err = cs.Read(ctx, shardReader)
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
	t := time.NewTimer(time.Duration(mgr.conf.NexRoundSec) * time.Second) // wait next round inspect
	defer t.Stop()

	select {
	case <-t.C:
	case <-mgr.svr.closeCh:
	}
}

func (mgr *DataInspectMgr) getInspectedBlobs(diskID proto.DiskID) *sync.Map {
	if inspected, exists := mgr.inspected[diskID]; exists {
		return inspected
	}
	inspected := &sync.Map{}
	mgr.inspected[diskID] = inspected
	return inspected
}

func (mgr *DataInspectMgr) cleanDiskInspectMetric(ds core.DiskAPI, diskID proto.DiskID) {
	dataInspectMetric.WithLabelValues(
		ds.DiskInfo().ClusterID.ToString(),
		diskID.ToString(),
	).Set(0)
}

// It was reported only once. When the upper-level user at get/put, an error was found
func (mgr *DataInspectMgr) reportBadShard(ctx context.Context, cs core.ChunkAPI, blobID proto.BlobID, err error) {
	diskInfo := cs.Disk().DiskInfo()

	// prevent repeated reporting
	inspected := mgr.getInspectedBlobs(diskInfo.DiskID)
	if _, exist := inspected.Load(blobID); exist {
		return
	}
	inspected.Store(blobID, struct{}{})

	// don't report this error
	if isInspectReportIgnoredError(err) {
		return
	}

	// report one bad shard, when the upper-level user at get/put, an error was found
	// It's possible that this disk has inspected this bid error before, or it might not.
	// Report with "add" and combine it with "record" for analysis and processing
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

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if args.Rate < minRateLimit {
		c.RespondError(errors.New("rate value is too small"))
		return
	}

	span.Infof("set data inspect rate args: %+v", args)
	s.inspectMgr.setAllDiskRateForce(args.Rate)
	c.Respond()
}

func (s *Service) GetInspectStat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	stat := DataInspectStat{
		DataInspectConf: s.inspectMgr.conf,
		Open:            s.inspectMgr.getSwitch(),
		Progress:        s.inspectMgr.progress,
	}
	span.Infof("data inspect args: %+v", stat)
	c.RespondJSON(&stat)
}

// CleanInspectMetric set diskID metric is zero, maybe disk is broken/repaired and replace new disk with another diskID
// 'localhost:${port}/inspect/cleanmetric?cluster_id=1&disk_id=2'
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
	return os.IsNotExist(err) || rpc.DetectStatusCode(err) == bloberr.CodeBidNotFound
}
