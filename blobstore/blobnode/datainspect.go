package blobnode

import (
	"context"
	"io"
	"strconv"
	"sync"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/prometheus/client_golang/prometheus"
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

func (s *Service) loopDataInspect() {
	if !conf.DiskConfig.EnableDataInspect {
		return
	}
	span, ctx := trace.StartSpanFromContext(s.ctx, "")
	timer := time.NewTimer(time.Duration(s.Conf.ChunkInspectIntervalSec) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop inspect data done.")
			return
		case <-timer.C:
			span.Info("loop start inspect disks.")
			var wg sync.WaitGroup
			disks := s.copyDiskStorages(ctx)
			for _, ds := range disks {
				span, ctx := trace.StartSpanFromContext(ctx, "")
				wg.Add(1)
				go func(ds core.DiskAPI) {
					defer wg.Done()
					chunks, err := ds.ListChunks(ctx)
					if err != nil {
						span.Errorf("ListChunks error:%v", err)
						return
					}
					for _, chunk := range chunks {
						cs, found := ds.GetChunkStorage(chunk.Vuid)
						if !found {
							span.Errorf("vuid:%v not found", chunk.Vuid)
							continue
						}
						_, err = startInspect(ctx, cs)
						if err != nil {
							span.Errorf("inspect error:%v", err)
							return
						}
					}
				}(ds)
			}
			wg.Wait()
			timer.Reset(time.Duration(s.Conf.ChunkInspectIntervalSec) * time.Second)
		}
	}
}

func ScanShard(ctx context.Context, cs core.ChunkAPI, startBid proto.BlobID, inspectFunc func(ctx context.Context, batchShards []*bnapi.ShardInfo) (err error)) (err error) {
	for {
		shards, next, err := cs.ListShards(ctx, startBid, 1024, bnapi.ShardStatusNormal)
		if err != nil {
			return err
		}
		err = inspectFunc(ctx, shards)
		startBid = next
		if err != nil {
			return err
		}
		if next == proto.InValidBlobID {
			break
		}
	}
	return nil
}

func startInspect(ctx context.Context, cs core.ChunkAPI) ([]bnapi.BadShard, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start inspect chunk, vuid:%s,chunkid:%s.", cs.Vuid(), cs.ID())
	ctx = bnapi.Setiotype(ctx, bnapi.InspectIO)
	startBid := proto.InValidBlobID
	ds := cs.Disk()
	badShards := make([]bnapi.BadShard, 0)
	scanFn := func(ctx context.Context, batchShards []*bnapi.ShardInfo) (err error) {
		for _, si := range batchShards {
			span, ctx := trace.StartSpanFromContext(ctx, "")
			discard := io.Discard
			shardReader := core.NewShardReader(si.Bid, si.Vuid, 0, 0, discard)
			_, err := cs.Read(ctx, shardReader)
			if err != nil {
				badShard := bnapi.BadShard{DiskID: ds.ID(), Vuid: si.Vuid, Bid: si.Bid}
				badShards = append(badShards, badShard)
				span.Errorf("inspect blob error, vuid:%v, bid:%v, err:%v, bad shards:%v", si.Vuid, si.Bid, err, badShards)
				reportBadShard(cs, si.Bid, err)
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
	err := ScanShard(ctx, cs, startBid, scanFn)
	if err != nil {
		return nil, err
	}
	span.Debugf("finished inspect chunk, vuid:%v, chunkid:%v, err:%v", cs.Vuid(), cs.ID(), err)
	return badShards, nil
}

func reportBadShard(cs core.ChunkAPI, blobID proto.BlobID, err error) {
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

func init() {
	prometheus.MustRegister(dataInspectMetric)
}
