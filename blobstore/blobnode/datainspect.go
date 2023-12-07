package blobnode

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
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

var dataInspectMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "blobnode",
		Name:      "data_inspect",
		Help:      "blobnode data inspect",
	},
	[]string{"cluster_id", "idc", "rack", "host", "disk_id", "vuid", "bid", "error"},
)

type DataInspectConf struct {
	IntervalSec int `json:"interval_sec"` // next round inspect interval
	RateLimit   int `json:"rate_limit"`   // max rate limit per second
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
}

func NewDataInspectMgr(svr *Service, conf DataInspectConf, switchMgr *taskswitch.SwitchMgr) (*DataInspectMgr, error) {
	taskSwitch, err := switchMgr.AddSwitch(proto.TaskSwitchDataInspect.String())
	if err != nil {
		return nil, err
	}

	mgr := &DataInspectMgr{
		conf:       conf,
		limits:     make(map[proto.DiskID]*rate.Limiter),
		svr:        svr,
		taskSwitch: taskSwitch,
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
		if ds.Status() >= proto.DiskStatusBroken {
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

	chunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("ListChunks error:%v", err)
		return
	}

	for _, chunk := range chunks {
		if chunk.Status == bnapi.ChunkStatusRelease {
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

	go func() {
		<-mgr.svr.closeCh
		cancel()
	}()

	scanFn := func(batchShards []*bnapi.ShardInfo) (err error) {
		for _, si := range batchShards {
			if si.Size <= 0 {
				continue
			}

			discard := io.Discard
			shardReader := core.NewShardReader(si.Bid, si.Vuid, 0, 0, discard)
			// retry overload
			for {
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
				badShard := bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid}
				badShards = append(badShards, badShard)
				span.Errorf("inspect blob error, vuid:%v, bid:%v, err:%v, bad shards:%v", si.Vuid, si.Bid, err, badShards)
				mgr.reportBadShard(cs, si.Bid, err)
			}

			select {
			case <-ctx.Done():
				span.Warnf("inspect chunk done.chunkid:%v", cs.ID())
				return ctx.Err()
			default:
			}
		}
		return nil
	}

	err := mgr.ScanShard(ctx, cs, startBid, scanFn)
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

func (mgr *DataInspectMgr) reportBadShard(cs core.ChunkAPI, blobID proto.BlobID, err error) {
	bid := strconv.FormatUint(uint64(blobID), 10)
	diskInfo := cs.Disk().DiskInfo()
	dataInspectMetric.WithLabelValues(diskInfo.ClusterID.ToString(),
		diskInfo.Idc,
		diskInfo.Rack,
		diskInfo.Host,
		cs.Disk().ID().ToString(),
		cs.Vuid().ToString(),
		bid,
		err.Error()).Set(1)
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
