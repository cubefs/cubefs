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

package disk

import (
	"context"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	DefaultSetCompactingCnt = 5
	setCompactInterval      = 100 * time.Millisecond
)

func (ds *DiskStorage) loopCompactFile() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "Compact"+ds.Conf.Path)

	timer := initTimer(ds.Conf.ChunkCompactIntervalSec)
	defer timer.Stop()

	span.Infof("start compact executor.")

	// consumer
	ds.loopAttach(func() {
		for {
			select {
			case <-ds.closeCh:
				span.Warnf("loopCompact done...")
				return
			case vuid := <-ds.compactCh:
				span.Debugf("recv compact message. vuid:[%d]", vuid)
				if err := ds.ExecCompactChunk(vuid); err != nil {
					span.Errorf("compact vuid: %d err:%v", vuid, err)
				}
			}
		}
	})

	span.Infof("start compact checker.")

	// producer
	for {
		select {
		case <-ds.closeCh:
			span.Warnf("loopCompact done...")
			return
		case <-timer.C:
			ds.runCompactFiles()
			resetTimer(ds.Conf.ChunkCompactIntervalSec, timer)
		}
	}
}

func (ds *DiskStorage) runCompactFiles() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("Compact"+ds.Conf.Path))

	span.Debugf("find chunks that meet the conditions ===")
	defer span.Debugf("check compact files done. ===")

	chunks := make([]core.ChunkAPI, 0)
	ds.Lock.RLock()
	for _, chunk := range ds.Chunks {
		chunks = append(chunks, chunk)
	}
	ds.Lock.RUnlock()

	for _, chunk := range chunks {
		if !chunk.NeedCompact(ctx) {
			continue
		}
		span.Infof("will compact vuid:<%d>", chunk.Vuid())
		ds.EnqueueCompact(ctx, chunk.Vuid())
		// Once in a round
		return
	}
}

func (ds *DiskStorage) EnqueueCompact(ctx context.Context, vuid proto.Vuid) {
	ds.compactCh <- vuid
}

func (ds *DiskStorage) CompactChunkInternal(ctx context.Context, vuid proto.Vuid) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	cs, found := ds.GetChunkStorage(vuid)
	if !found {
		span.Errorf("cannot happened. vuid:%v not found", vuid)
		return bloberr.ErrNoSuchVuid
	}

	// no lock here. compact chunk. generate new chunkfile
	ncs, err := cs.StartCompact(ctx)
	if err != nil {
		span.Errorf("Failed start compact, err:%v", err)
		_ = cs.StopCompact(ctx, nil)
		return err
	}

	// The following logic, for the same vuid, only allows serial execution
	if ds.ChunkLimitPerKey.Acquire(vuid) != nil {
		return bloberr.ErrOverload
	}
	defer ds.ChunkLimitPerKey.Release(vuid)

	ncsMeta := ncs.VuidMeta()
	ncsMeta.Status = cs.Status()
	ncsMeta.Mtime = time.Now().UnixNano()

	// insert new chunkmeta. no side effect.
	err = ds.SuperBlock.UpsertChunk(ctx, ncs.ID(), *ncsMeta)
	if err != nil {
		span.Errorf("Failed upsert chunk<%s>, err:%v", ncs.ID(), err)
		goto STOPCOMPACT
	}

	// MUST: any writing and deletion must be prohibited

	// vuid change to new chunkfile
	err = ds.SuperBlock.BindVuidChunk(ctx, vuid, ncs.ID())
	if err != nil {
		span.Errorf("Failed vuid[%d] bind new chunkfile[%s]", vuid, ncs.ID())
		goto STOPCOMPACT
	}

	// change memory fd.
	err = cs.CommitCompact(ctx, ncs)
	if err != nil {
		span.Errorf("Failed start compact, err:%v", err)
		goto STOPCOMPACT
	}

STOPCOMPACT:
	// compact done and enable modify
	err = cs.StopCompact(ctx, ncs)
	if err != nil {
		span.Errorf("Failed StopCompact vuid[%d] newchunkfile[%s]", vuid, ncsMeta.ChunkId)
		return err
	}

	// mark destroy ncs
	err = ds.destroyRedundant(ctx, ncs)
	if err != nil {
		span.Errorf("Failed update chunk[%s] status. err:%v", ncsMeta.ChunkId, err)
	}

	span.Infof("compact success. vuid[%d] chunkfile[%s]", vuid, cs.ID())

	return nil
}

func (ds *DiskStorage) destroyRedundant(ctx context.Context, ncs core.ChunkAPI) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// keep new chunkstorage meta
	ncsMeta := ncs.VuidMeta()

	// wait old stg all request done；
	for {
		time.Sleep(10 * time.Second)
		if !ncs.HasPendingRequest() {
			break
		}
		span.Debugf("=== wait chunk(%s) all request done ===", ncsMeta.ChunkId)
	}

	span.Infof("safe here. id:%s request all done.", ncsMeta.ChunkId)

	// isolated. safe
	ncs.Close(ctx)

	// update chunk status, mark destroy. destroy async
	ncsMeta.Status = bnapi.ChunkStatusRelease
	ncsMeta.Reason = bnapi.ReleaseForCompact
	ncsMeta.Mtime = time.Now().UnixNano()

	return ds.SuperBlock.UpsertChunk(ctx, ncsMeta.ChunkId, *ncsMeta)
}

func (ds *DiskStorage) ExecCompactChunk(vuid proto.Vuid) (err error) {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("Compact"+ds.Conf.Path))

	_, found := ds.GetChunkStorage(vuid)
	if !found {
		return bloberr.ErrNoSuchVuid
	}

	// Persistent compacting field
	err = ds.UpdateChunkCompactState(ctx, vuid, true)
	if err != nil {
		span.Errorf("update chunk(%v) compacting failed: %v", vuid, err)
		return err
	}

	err = ds.notifyCompacting(ctx, vuid, true)
	if err != nil {
		span.Errorf("set chunk(%v) compacting failed: %v", vuid, err)
		return
	}

	// do compact internal
	if err = ds.CompactChunkInternal(ctx, vuid); err != nil {
		span.Errorf("Failed compact vuid:%v, err:%v", vuid, err)
		return err
	}

	if err = ds.notifyCompacting(ctx, vuid, false); err != nil {
		span.Errorf("set chunk(%v) compacting failed: %v", vuid, err)
		return err
	}

	if err = ds.UpdateChunkCompactState(ctx, vuid, false); err != nil {
		span.Errorf("Failed update vuid:%v state, err:%v", vuid, err)
		return err
	}

	return nil
}

func (ds *DiskStorage) notifyCompacting(ctx context.Context, vuid proto.Vuid, compacting bool) (
	err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	// report compact to CM
	for i := 0; i < DefaultSetCompactingCnt; i++ {
		err = ds.Conf.NotifyCompacting(ctx, &cmapi.SetCompactChunkArgs{
			Vuid:       vuid,
			Compacting: compacting,
		})
		if err == nil {
			span.Infof("send chunk(%v) compacting(%v) to cm success",
				vuid, compacting)
			return
		}
		span.Warnf("send chunk(%v) compacting(%v) to cm failed: %v",
			vuid, compacting, err)
		time.Sleep(setCompactInterval)
	}

	return
}
