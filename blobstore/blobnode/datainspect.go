package blobnode

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
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
	slowDownRatio  = 2
	speedUpCnt     = 2000
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
		[]string{"cluster_id", "idc", "rack", "host", "disk_id"},
	)
	errServiceClosed = errors.New("service is closed")
)

type DataInspectConf struct {
	IntervalSec int `json:"interval_sec"` // next round inspect interval
	RateLimit   int `json:"rate_limit"`   // max rate limit per second

	Record recordlog.Config `json:"record"`
}

type DataInspectStat struct {
	DataInspectConf
	Open bool `json:"open"`
}

type DataInspectMgr struct {
	conf   DataInspectConf
	limits map[proto.DiskID]*rate.Limiter

	svr        *Service
	taskSwitch *taskswitch.TaskSwitch

	recorder recordlog.Encoder
}

func NewDataInspectMgr(svr *Service, conf DataInspectConf, switchMgr *taskswitch.SwitchMgr) (*DataInspectMgr, error) {
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskSwitchDataInspect.String())
	if err != nil {
		return nil, err
	}

	// init data inspect record
	recorder, err := recordlog.NewEncoder(&conf.Record)
	if err != nil {
		return nil, err
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
	span, ctx := trace.StartSpanFromContext(mgr.svr.ctx, "")
	t := time.NewTicker(time.Second * 5) // wait switch
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if !mgr.getSwitch() {
				continue
			}
			mgr.inspectAllDisks(ctx)

		case <-mgr.svr.closeCh:
			mgr.recorder.Close()
			span.Warnf("loop inspect data closed.")
			return
		}
	}
}

func (mgr *DataInspectMgr) inspectAllDisks(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	span.Info("loop start inspect all disks.")
	disks := mgr.svr.copyDiskStorages(ctx)
	mgr.setLimiters(disks)

	var wg sync.WaitGroup
	for _, ds := range disks {
		if !ds.IsWritable() { // not writable, dont need inspect disk
			continue
		}
		wg.Add(1)
		go mgr.inspectDisk(ctx, ds, &wg)
	}

	wg.Wait()
	if !mgr.getSwitch() {
		span.Info("stop inspect disks.")
		return
	}

	span.Info("finish inspect all disks.")
	mgr.waitNextRoundInspect()
}

func (mgr *DataInspectMgr) inspectDisk(ctx context.Context, ds core.DiskAPI, wg *sync.WaitGroup) {
	defer wg.Done()
	span := trace.SpanFromContextSafe(ctx)

	// clean metric
	mgr.cleanDiskInspectMetric(ds)

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("ListChunks error:%v", err)
		return
	}

	for _, chunk := range chunks {
		if chunk.Status == clustermgr.ChunkStatusRelease {
			continue
		}
		cs, found := ds.GetChunkStorage(chunk.Vuid)
		if !found {
			span.Errorf("vuid:%v not found", chunk.Vuid)
			continue
		}
		_, err = mgr.inspectChunk(ctx, cs)
		if err != nil {
			span.Errorf("inspect error:%v", err)
			return
		}
		if !mgr.getSwitch() {
			return
		}
	}
}

func (mgr *DataInspectMgr) inspectChunk(pCtx context.Context, cs core.ChunkAPI) ([]bnapi.BadShard, error) {
	span := trace.SpanFromContextSafe(pCtx)
	span.Debugf("start inspect chunk, vuid:%v,chunkid:%s.", cs.Vuid(), cs.ID())

	ctx, cancel := context.WithCancel(context.Background())
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	startBid := proto.InValidBlobID
	ds := cs.Disk()
	badShards := make([]bnapi.BadShard, 0)
	lmt := mgr.getLimiter(ds)
	successCnt := int64(0)

	scanFn := func(batchShards []*bnapi.ShardInfo) (err error) {
		for _, si := range batchShards {
			if si.Size <= 0 {
				continue
			}

			discard := io.Discard
			shardReader := core.NewShardReader(si.Bid, si.Vuid, 0, 0, discard)
			// retry overload
			for {
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
				if err == bloberr.ErrOverload {
					successCnt = 0
					mgr.setRate(lmt, minRateLimit) // slow down
					continue
				}

				successCnt++
				if successCnt%speedUpCnt == 0 {
					mgr.setRate(lmt, lmt.Limit()*slowDownRatio) // speed up
				}
				break
			}

			if err != nil && err != bloberr.ErrOverload {
				badShard := bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid, Err: err}
				badShards = append(badShards, badShard)
				span.Errorf("inspect blob error, bad shard:%+v, bad count:%d", badShard, len(badShards))
			}
		}
		return nil
	}

	err := mgr.ScanShard(ctx, cs, startBid, scanFn)
	mgr.reportBatchBadShards(ctx, cs, badShards)
	if err != nil {
		return nil, err
	}
	span.Debugf("finished inspect chunk, vuid:%v, chunkid:%v, err:%v", cs.Vuid(), cs.ID(), err)
	return badShards, nil
}

func (mgr *DataInspectMgr) ScanShard(ctx context.Context, cs core.ChunkAPI, startBid proto.BlobID, inspectFunc func(batchShards []*bnapi.ShardInfo) (err error)) (err error) {
	for {
		shards, next, err := cs.ListShards(ctx, startBid, listShardBatch, bnapi.ShardStatusNormal)
		if err != nil {
			return err
		}

		err = inspectFunc(shards)
		if err != nil {
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
	t := time.NewTimer(time.Duration(mgr.conf.IntervalSec) * time.Second) // wait next round inspect
	defer t.Stop()

	select {
	case <-t.C:
	case <-mgr.svr.closeCh:
	}
}

func (mgr *DataInspectMgr) cleanDiskInspectMetric(ds core.DiskAPI) {
	diskInfo := ds.DiskInfo()
	dataInspectMetric.WithLabelValues(diskInfo.ClusterID.ToString(),
		diskInfo.Idc,
		diskInfo.Rack,
		diskInfo.Host,
		diskInfo.DiskID.ToString(),
	).Set(0)
}

// It was reported only once. When the upper-level user at get/put, an error was found
func (mgr *DataInspectMgr) reportBadShard(ctx context.Context, cs core.ChunkAPI, blobID proto.BlobID, err error) {
	if isInspectReportIgnoredError(err) {
		return
	}

	bid := strconv.FormatUint(uint64(blobID), 10)
	diskInfo := cs.Disk().DiskInfo()
	dataInspectMetric.WithLabelValues(diskInfo.ClusterID.ToString(),
		diskInfo.Idc,
		diskInfo.Rack,
		diskInfo.Host,
		diskInfo.DiskID.ToString(),
	).Set(1)

	mgr.recordBadBids(ctx, cs, []string{bid}, err.Error())
}

// Aggregate a batch of errors and report them all at once(the same chunk), Because the repair of data is often at the granularity of chunks
func (mgr *DataInspectMgr) reportBatchBadShards(ctx context.Context, cs core.ChunkAPI, items []bnapi.BadShard) int {
	if len(items) == 0 {
		return 0
	}

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

		uniqueErr[item.Err.Error()] = append(uniqueErr[item.Err.Error()], strconv.FormatUint(uint64(item.Bid), 10))
	}

	if len(uniqueErr) == 0 {
		return 0
	}

	// record local log
	totalBadBid := 0
	for errStr, bids := range uniqueErr {
		mgr.recordBadBids(ctx, cs, bids, errStr)
		totalBadBid += len(bids)
	}

	// report metric
	diskInfo := cs.Disk().DiskInfo()
	dataInspectMetric.WithLabelValues(diskInfo.ClusterID.ToString(),
		diskInfo.Idc,
		diskInfo.Rack,
		diskInfo.Host,
		diskInfo.DiskID.ToString(),
	).Set(float64(totalBadBid))

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
		span.Warnf("fail to write bad data inspect record: record[%v], err[%+v]", record, err)
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

func (mgr *DataInspectMgr) setRate(lmt *rate.Limiter, newLimit rate.Limit) {
	if newLimit >= minRateLimit && newLimit <= rate.Limit(mgr.conf.RateLimit) {
		lmt.SetLimit(newLimit)
		lmt.SetBurst(int(2 * newLimit))
	}
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

	conf := DataInspectStat{DataInspectConf: s.inspectMgr.conf}
	conf.Open = s.inspectMgr.getSwitch()
	span.Infof("data inspect args: %+v", conf)
	c.RespondJSON(&conf)
}

func init() {
	prometheus.MustRegister(dataInspectMetric)
}

func isInspectReportIgnoredError(err error) bool {
	return os.IsNotExist(err) || rpc.DetectStatusCode(err) == bloberr.CodeBidNotFound
}
