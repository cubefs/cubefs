// Copyright 2022 The CubeFS Authors.
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

package chunk

import (
	"context"
	"errors"
	"sync"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var (
	ErrCompactCheck   = errors.New("chunk compact check not match")
	ErrCompactStopped = errors.New("chunk compact stopped")
)

/*
 *	compacting chunkFile
 */

type compactTask struct {
	stopCh chan struct{}
	once   sync.Once
}

func (cs *chunk) StartCompact(ctx context.Context) (newcs core.ChunkAPI, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Warnf("==== start compact chunk:(%s) ====", cs.ID())

	// new dst
	now := time.Now().UnixNano()

	vm := core.VuidMeta{
		Version:     cs.version,
		Vuid:        cs.vuid,
		DiskID:      cs.diskID,
		ChunkId:     bnapi.NewChunkId(cs.vuid),
		ParentChunk: cs.ID(),
		ChunkSize:   int64(cs.fileInfo.Total),
		Ctime:       now,
		Mtime:       now,
		Status:      bnapi.ChunkStatusDefault,
	}

	stg := cs.getStg()

	// new dstChunkStorage
	ncs, err := newChunkStorage(ctx, cs.Disk().GetDataPath(), vm, cs.readScheduler, cs.writeScheduler, func(o *core.Option) {
		o.Conf = cs.Disk().GetConfig()
		o.DB = stg.MetaHandler().InnerDB()
		o.IoQos = cs.Disk().GetIoQos()
		o.Disk = cs.Disk()
		o.CreateDataIfMiss = true
	})
	if err != nil {
		span.Errorf("Failed create dst ChunkStorage. err:%v", err)
		return nil, err
	}

	task := cs.compactTask.Load().(*compactTask)
	notify := func(err error) {
		task.once.Do(func() {
			close(task.stopCh)
		})
	}

	// create replicate stg
	backgroundStg := ncs.getStg()
	repStg := storage.NewReplicateStg(stg, backgroundStg, notify)

	cs.lock.Lock()
	{
		cs.compacting = true
		// note: set double write stg
		cs.setStg(repStg)
	}
	cs.lock.Unlock()
	span.Warnf("set chunk:%s compacting success", cs.ID())

	// wait for all requests before switching handles to complete
	timestamp := cs.consistent.Synchronize()
	span.Infof("all requests before the switch handle are completed, timestamp:%v", timestamp)

	err = cs.doCompact(ctx, ncs)
	if err != nil {
		span.Errorf("Failed doCompact Chunk. err:%v", err)
		goto ErrCompact
	}

	// init chunk file info
	err = ncs.refreshFstat(ctx)
	if err != nil {
		span.Errorf("Failed init Chunk info. err:%v", err)
		goto ErrCompact
	}

ErrCompact:
	if err != nil {
		cs.handleErrCompact(ctx, ncs)
		return nil, err
	}

	return ncs, nil
}

func (cs *chunk) handleErrCompact(ctx context.Context, ncs core.ChunkAPI) {
	span := trace.SpanFromContextSafe(ctx)

	// The front access must be removed
	// before the destruction operation can be done;
	cs.lock.Lock()
	{
		rawStg := cs.getStg().RawStorage()
		if rawStg == nil {
			panic("never happened")
		}
		// restore raw stg
		cs.setStg(rawStg)

	}
	cs.lock.Unlock()

	// wait for all requests before switching handles to complete
	timestamp := cs.consistent.Synchronize()
	span.Infof("all requests before the switch handle are completed, timestamp:%v", timestamp)

	ncs.(*chunk).Destroy(ctx)
}

func (cs *chunk) doCompact(ctx context.Context, ncs *chunk) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	ctx = bnapi.Setiotype(ctx, bnapi.CompactIO)

	startBid := proto.InValidBlobID
	replStg := cs.getStg()

	task := cs.compactTask.Load().(*compactTask)

	// read src & write dst
	copyShardFunc := func(blobID proto.BlobID, srcMeta *core.ShardMeta) (err error) {
		// update startBid
		startBid = blobID

		cs.bidlimiter.Acquire(blobID)
		defer cs.bidlimiter.Release(blobID)

		// get blob data from srcChunkStorage
		shard, err := cs.NewReader(ctx, blobID)
		if err != nil {
			span.Errorf("read shard(%v) data from chunk(%s) failed: %v", blobID, cs.ID(), err)
			return
		}

		// check overwrite
		if srcMeta.Offset != shard.Offset ||
			srcMeta.Size != shard.Size || srcMeta.Crc != shard.Crc {
			span.Warnf("bid:%v overwrite, old:%v, new:%v", blobID, srcMeta, shard)
		}

		// dstChunkStorage write data
		err = ncs.Write(ctx, shard)
		if err != nil {
			span.Errorf("write shard(%v) to chunk(%s) failed: %v", blobID, ncs.ID(), err)
			return
		}

		// note: To prevent too much dirty data in one-time flushing,
		// which leads to a sharp increase in time delay
		err = ncs.SyncData(ctx)
		if err != nil {
			span.Errorf("sync shard(%v) to chunk(%s) failed: %v", blobID, ncs.ID(), err)
			return
		}

		// monitor task termination
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-task.stopCh:
			return ErrCompactStopped
		default:
			// nothing
		}

		return
	}

	// scan src
COMPACT:
	for {
		select {
		case <-task.stopCh:
			span.Infof("chunk storage(%v) compacting have been stopped", cs.ID())
			return ErrCompactStopped
		case <-ctx.Done():
			span.Infof("chunk storage(%v) compacting err:%v", cs.ID(), ctx.Err())
			return ctx.Err()
		default:
			err = replStg.ScanMeta(ctx, startBid, cs.conf.CompactBatchSize, copyShardFunc)
			if err != nil {
				if err == core.ErrChunkScanEOF {
					span.Infof("chunk %s scan finished", cs.ID())
					err = nil
					break COMPACT
				} else {
					span.Errorf("scan chunk meta(%s) failed: %v", cs.ID(), err)
					return err
				}
			}
		}
	}

	// check pending err
	if err = replStg.PendingError(); err != nil {
		span.Errorf("stg has pending err:%v", err)
		return err
	}

	if err = ncs.Sync(ctx); err != nil {
		span.Errorf("sync chunk(%s) failed.", ncs.ID())
		return err
	}

	if cs.Disk().GetConfig().NeedCompactCheck {
		err = cs.compactCheck(ctx, ncs)
		if err != nil {
			span.Errorf("chunk compact check failed: src:%s, ncs:%s, err:%v", cs.ID(), ncs.ID(), err)
			return err
		}
	}

	return nil
}

func (cs *chunk) CommitCompact(ctx context.Context, ncs core.ChunkAPI) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	newcs := ncs.(*chunk)

	// replStg -> normal background stg
	repStg, backgroundStg := cs.getStg(), newcs.getStg()

	span.Infof("compact commit. Change <%s> to <%s>", repStg.ID(), backgroundStg.ID())

	cs.lock.Lock()
	{
		// exchange handler
		cs.setStg(backgroundStg)
		newcs.setStg(repStg)
	}
	cs.lock.Unlock()

	cs.lastCompactTime = time.Now().UnixNano()

	// finally init stats
	if err = cs.refreshFstat(ctx); err != nil {
		span.Errorf("Failed refresh fstat. <%s>", cs.ID())
		return err
	}

	// wait for all requests before switching handles to complete
	timestamp := cs.consistent.Synchronize()
	span.Infof("All requests before the switch handle are completed, timestamp:%v", timestamp)

	return nil
}

func (cs *chunk) StopCompact(ctx context.Context, ncs core.ChunkAPI) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	cs.lock.Lock()
	defer cs.lock.Unlock()

	cs.compacting = false
	cs.resetCompactTask()

	// note: In an abnormal situation, compact fails, but stg is double-written stg
	curStg := cs.getStg()
	rawStg := curStg.RawStorage()
	if rawStg != nil {
		// If a commit is made, then it will definitely not enter this branch;
		span.Warnf("cur:%s/raw:%s compact fails, stg is double-written stg", curStg.ID(), rawStg.ID())
		// restore stg
		cs.setStg(rawStg)
	}

	return nil
}

func (cs *chunk) NeedCompact(ctx context.Context) bool {
	span := trace.SpanFromContextSafe(ctx)

	stg := cs.getStg()

	stat, err := stg.Stat(context.TODO())
	if err != nil {
		span.Errorf("get chunk data space info failed: %v", err)
		return false
	}

	size, phySize := stat.FileSize, stat.PhySize

	// file size is too large
	if size >= cs.conf.CompactTriggerThreshold {
		span.Debugf("phySize:%v/fsize:%v, threshold:%v",
			phySize, size, cs.conf.CompactTriggerThreshold)
		return true
	}

	// void rate exceeds threshold
	if size > cs.conf.CompactMinSizeThreshold && (1-float64(phySize)/float64(size)) >= cs.conf.CompactEmptyRateThreshold {
		span.Debugf("phySize:%v/fsize:%v, minSize:%v threshold:%v",
			phySize, size, cs.conf.CompactMinSizeThreshold, cs.conf.CompactEmptyRateThreshold)
		return true
	}

	return false
}

func (cs *chunk) compactCheck(ctx context.Context, ncs *chunk) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	replStg, nStg := cs.getStg(), ncs.getStg()

	span.Infof("compact final check source:%s, dest:%s", replStg.ID(), nStg.ID())

	startBid := proto.BlobID(proto.InValidBlobID)
	task := cs.compactTask.Load().(*compactTask)

	checkFunc := func(blobID proto.BlobID, srcMeta *core.ShardMeta) (err error) {
		// update start bid
		startBid = blobID

		cs.bidlimiter.Acquire(blobID)
		defer cs.bidlimiter.Release(blobID)

		dstMeta, err := nStg.ReadShardMeta(ctx, blobID)
		if err != nil {
			span.Errorf("read chunk(%s)/shard(%v)/srcMeat(%v) meta failed: %v",
				nStg.ID(), blobID, srcMeta, err)
			return
		}

		if dstMeta.Size != srcMeta.Size ||
			dstMeta.Crc != srcMeta.Crc ||
			dstMeta.Flag != srcMeta.Flag {
			span.Errorf("chunk src: %s dst:%s match, shard src:%v, dst:%v",
				replStg.ID(), nStg.ID(), srcMeta, dstMeta)
			return ErrCompactCheck
		}

		// monitor task termination
		select {
		case <-task.stopCh:
			return ErrCompactStopped
		default:
			// nothing
		}

		return nil
	}

	// for each source stg
	for err != core.ErrChunkScanEOF {
		err = replStg.ScanMeta(ctx, startBid, cs.conf.CompactBatchSize, checkFunc)
		if err != nil && err != core.ErrChunkScanEOF {
			span.Errorf("scan from srcChunkStorage failed: %v", err)
			return err
		}
	}

	span.Infof("check source:%s, dest:%s success", replStg.ID(), nStg.ID())

	return nil
}
