package blobnode

import (
	"context"
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
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	listShardBatch = 100
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
	Open        bool `json:"open"`
	IntervalSec int  `json:"interval_sec"`
	RateLimit   int  `json:"rate_limit"`
	//MaxCount    int     `json:"max_count"`
	//Factor      float64 `json:"factor"`
}

type DataInspectMgr struct {
	svr    *Service
	lck    sync.Mutex
	conf   DataInspectConf
	limits map[proto.DiskID]*rate.Limiter
}

func NewDataInspectMgr(svr *Service, conf DataInspectConf) *DataInspectMgr {
	return &DataInspectMgr{
		svr:  svr,
		conf: conf,
		//lmt:  rate.NewLimiter(rate.Every(time.Second), 10),
		//lmt: rate.NewLimiter(rate.Limit(listShardBatch), 2*listShardBatch),
	}
}

func (mgr *DataInspectMgr) loopDataInspect() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if !mgr.getSwitch() {
				continue
			}
			mgr.inspectAll()

		case <-mgr.svr.closeCh:
			return
		}
	}
}

func (mgr *DataInspectMgr) setLimiter(ds core.DiskAPI) {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()

	if _, ok := mgr.limits[ds.ID()]; !ok {
		mgr.limits[ds.ID()] = rate.NewLimiter(rate.Limit(mgr.conf.RateLimit), 2*mgr.conf.RateLimit)
	}
}

func (mgr *DataInspectMgr) getLimiter(ds core.DiskAPI) *rate.Limiter {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()

	return mgr.limits[ds.ID()]
}

func (mgr *DataInspectMgr) inspectAll() {
	pSpan, pCtx := trace.StartSpanFromContext(mgr.svr.ctx, "")
	timer := time.NewTimer(time.Duration(mgr.conf.IntervalSec) * time.Second)
	defer timer.Stop()

	for {
		pSpan.Info("loop start inspect disks.")
		var wg sync.WaitGroup
		disks := mgr.svr.copyDiskStorages(pCtx)
		for _, ds := range disks {
			wg.Add(1)
			mgr.setLimiter(ds)
			go mgr.inspectDisk(pSpan, ds, &wg)
		}
		wg.Wait()
		if !mgr.getSwitch() {
			return
		}

		timer.Reset(time.Duration(mgr.conf.IntervalSec) * time.Second)
		select {
		case <-mgr.svr.closeCh:
			pSpan.Warnf("loop inspect data done.")
			return
		case <-timer.C:
		}
	}
}

func (mgr *DataInspectMgr) inspectDisk(pSpan trace.Span, ds core.DiskAPI, wg *sync.WaitGroup) {
	defer wg.Done()

	span, ctx := trace.StartSpanFromContext(context.Background(), pSpan.OperationName()+"_"+ds.ID().ToString())
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

func (mgr *DataInspectMgr) inspectChunk(ctx context.Context, cs core.ChunkAPI) ([]bnapi.BadShard, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start inspect chunk, vuid:%v,chunkid:%s.", cs.Vuid(), cs.ID())
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	startBid := proto.InValidBlobID
	ds := cs.Disk()
	badShards := make([]bnapi.BadShard, 0)

	//qos := ds.GetIoQos()
	//qos.Allow() //qos.Remain(ctx)
	//remain := 10
	lmt := mgr.getLimiter(ds)

	scanFn := func(pCtx context.Context, batchShards []*bnapi.ShardInfo) (err error) {
		//lmt.WaitN(ctx, listShardBatch)

		for _, si := range batchShards {
			pSpan := trace.SpanFromContextSafe(pCtx)
			span, ctx := trace.StartSpanFromContext(context.Background(), pSpan.OperationName())
			ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
			discard := io.Discard
			shardReader := core.NewShardReader(si.Bid, si.Vuid, 0, 0, discard)

			lmt.WaitN(ctx, 1)
			_, err = cs.Read(ctx, shardReader)
			if err == bloberr.ErrOverload {
				mgr.slowDown(lmt)
				continue
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

func (mgr *DataInspectMgr) ScanShard(ctx context.Context, cs core.ChunkAPI, startBid proto.BlobID, inspectFunc func(ctx context.Context, batchShards []*bnapi.ShardInfo) (err error)) (err error) {
	for {
		shards, next, err := cs.ListShards(ctx, startBid, listShardBatch, bnapi.ShardStatusNormal)
		if err != nil {
			return err
		}

		err = inspectFunc(ctx, shards)
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

func (mgr *DataInspectMgr) setSwitch(flag bool) {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()

	mgr.conf.Open = flag
}

func (mgr *DataInspectMgr) getSwitch() bool {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()

	return mgr.conf.Open
}

func (mgr *DataInspectMgr) slowDown(lmt *rate.Limiter) {
	newLimit := lmt.Limit() / 2
	if newLimit >= 1 {
		lmt.SetLimit(newLimit)
	}
}

func (mgr *DataInspectMgr) setRate(lmt *rate.Limiter, newLimit int) {
	lmt.SetLimit(rate.Limit(newLimit))
}

func (mgr *DataInspectMgr) setAllDiskRate(newLimit int) {
	mgr.lck.Lock()
	defer mgr.lck.Unlock()

	for _, lmt := range mgr.limits {
		mgr.setRate(lmt, newLimit)
	}
}

func (s *Service) setInspectSwitch(c *rpc.Context) {
	args := new(bnapi.InspectOpenArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("data inspect switch args: %+v", args)
	s.inspectMgr.setSwitch(args.Open)
}

func (s *Service) setInspectRate(c *rpc.Context) {
	args := new(bnapi.InspectRateArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("data inspect args: %+v", args)
	s.inspectMgr.setAllDiskRate(args.Rate)
}

func init() {
	prometheus.MustRegister(dataInspectMetric)
}
