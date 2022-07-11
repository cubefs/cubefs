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

package storage

import (
	"context"
	"io"
	"sync"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type notifyFunc func(err error)

type replicateStorage struct {
	lock      sync.RWMutex
	masterStg core.Storage
	slaveStg  core.Storage
	notify    func(error)
	lastErr   error
}

func NewReplicateStg(master core.Storage, slave core.Storage, notify notifyFunc) (replStg core.Storage) {
	return &replicateStorage{masterStg: master, slaveStg: slave, notify: notify}
}

func (stg *replicateStorage) PendingError() error {
	stg.lock.RLock()
	defer stg.lock.RUnlock()
	return stg.lastErr
}

func (stg *replicateStorage) IncrPendingCnt() {
	stg.masterStg.IncrPendingCnt()
	stg.slaveStg.IncrPendingCnt()
}

func (stg *replicateStorage) DecrPendingCnt() {
	stg.masterStg.DecrPendingCnt()
	stg.slaveStg.DecrPendingCnt()
}

func (stg *replicateStorage) PendingRequest() int64 {
	return stg.masterStg.PendingRequest() + stg.slaveStg.PendingRequest()
}

func (stg *replicateStorage) ID() bnapi.ChunkId {
	return stg.masterStg.ID()
}

func (stg *replicateStorage) MetaHandler() core.MetaHandler {
	return stg.masterStg.MetaHandler()
}

func (stg *replicateStorage) DataHandler() core.DataHandler {
	return stg.masterStg.DataHandler()
}

func (stg *replicateStorage) RawStorage() core.Storage {
	return stg.masterStg
}

type waiter func(error) error

func (stg *replicateStorage) notifyErr(ctx context.Context, err error) {
	stg.lock.Lock()
	stg.lastErr = err
	stg.lock.Unlock()

	if stg.notify == nil {
		return
	}
	stg.notify(err)
}

func (stg *replicateStorage) forward(ctx context.Context, b *core.Shard) (waiter waiter, err error) {
	span := trace.SpanFromContextSafe(ctx)

	fwdCh := make(chan error, 1)
	pr, pw := io.Pipe()

	shard := core.ShardCopy(b)
	shard.Body = pr

	slave := stg.slaveStg

	go func() {
		var fwdErr error
		defer func() {
			pr.CloseWithError(fwdErr)
			fwdCh <- fwdErr
		}()

		fwdErr = slave.Write(ctx, shard)
		if fwdErr != nil {
			span.Errorf("fwd error :%v", fwdErr)
			return
		}

		span.Debugf("write bid:%v done on cpStg", shard.Bid)
	}()

	// Note：important
	b.Body = io.TeeReader(b.Body, pw)

	// if there is an error, the overall error occurs, and close the pipeline.
	waiter = func(err error) (fwdErr error) {
		// Error caused by client disconnection
		if err == bloberr.ErrReaderError {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
		// Waiting for the completion of forward copy data writing
		fwdErr = <-fwdCh

		// Error handling steps:
		// 1. Failed to return user
		// 2. Terminate compact task
		if err != nil {
			stg.notifyErr(ctx, err)
			return err
		}

		if fwdErr != nil {
			stg.notifyErr(ctx, fwdErr)
			return fwdErr
		}

		return nil
	}

	return
}

// Special customized api, other apis are transmitted to masterStg
func (stg *replicateStorage) Write(ctx context.Context, b *core.Shard) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// modify shard.Body
	wait, _ := stg.forward(ctx, b)
	defer func() {
		fwdErr := wait(err)
		span.Debugf("wait err:%v, fwdErr:%v", err, fwdErr)
		err = fwdErr
	}()

	// write original location
	err = stg.masterStg.Write(ctx, b)
	if err != nil {
		return err
	}

	return nil
}

func (stg *replicateStorage) ReadShardMeta(ctx context.Context, bid proto.BlobID) (sm *core.ShardMeta, err error) {
	return stg.masterStg.ReadShardMeta(ctx, bid)
}

func (stg *replicateStorage) NewRangeReader(ctx context.Context, b *core.Shard, from, to int64) (rc io.Reader, err error) {
	return stg.masterStg.NewRangeReader(ctx, b, from, to)
}

func (stg *replicateStorage) MarkDelete(ctx context.Context, bid proto.BlobID) (err error) {
	return stg.masterStg.MarkDelete(ctx, bid)
}

func (stg *replicateStorage) Delete(ctx context.Context, bid proto.BlobID) (n int64, err error) {
	return stg.masterStg.Delete(ctx, bid)
}

func (stg *replicateStorage) ScanMeta(ctx context.Context, startBid proto.BlobID, limit int,
	fn func(bid proto.BlobID, sm *core.ShardMeta) error) (err error) {
	return stg.masterStg.ScanMeta(ctx, startBid, limit, fn)
}

func (stg *replicateStorage) SyncData(ctx context.Context) (err error) {
	var fwdErr error
	fwdCh := make(chan error, 1)

	span := trace.SpanFromContextSafe(ctx)
	start := time.Now()

	go func() {
		span := trace.SpanFromContextSafe(ctx)
		innerErr := stg.slaveStg.SyncData(ctx)
		span.AppendTrackLog("sync.2", start, innerErr)
		fwdCh <- innerErr
	}()

	defer func() {
		fwdErr = <-fwdCh
		// both done here
		if err != nil {
			stg.notifyErr(ctx, err)
			return
		}
		if fwdErr != nil {
			err = fwdErr
			stg.notifyErr(ctx, fwdErr)
			return
		}
	}()

	err = stg.masterStg.SyncData(ctx)
	span.AppendTrackLog("sync.1", start, err)

	return err
}

func (stg *replicateStorage) Sync(ctx context.Context) (err error) {
	return stg.masterStg.Sync(ctx)
}

func (stg *replicateStorage) Stat(ctx context.Context) (stat *core.StorageStat, err error) {
	return stg.masterStg.Stat(ctx)
}

func (stg *replicateStorage) Close(ctx context.Context) {
	stg.masterStg.Close(ctx)
}

func (stg *replicateStorage) Destroy(ctx context.Context) {
	stg.masterStg.Destroy(ctx)
}
