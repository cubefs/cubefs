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

package blobnode

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"golang.org/x/sync/singleflight"
)

const (
	serverShutdownPoll     = 500 * time.Millisecond // 500 ms
	DefaultShutdownTimeout = 60 * time.Second       // 60 s
)

type Service struct {
	lock          sync.RWMutex
	Disks         map[proto.DiskID]core.DiskAPI
	WorkerService *WorkerService

	// client handler
	ClusterMgrClient *cmapi.Client
	groupRun         singleflight.Group

	Conf *Config

	// limiter
	PutQpsLimitPerDisk    limit.ResettableLimiter
	GetQpsLimitPerDisk    limit.Limiter
	GetQpsLimitPerKey     limit.Limiter
	DeleteQpsLimitPerKey  limit.Limiter
	DeleteQpsLimitPerDisk limit.ResettableLimiter
	ChunkLimitPerVuid     limit.Limiter
	DiskLimitPerKey       limit.Limiter
	InspectLimiterPerKey  limit.Limiter

	RequestCount int64

	// ctx is used for initiated requests that
	// may need to be canceled on server shutdown.
	ctx    context.Context
	cancel context.CancelFunc

	closed  bool
	closeCh chan struct{}
}

func (s *Service) requestCounter(c *rpc.Context) {
	atomic.AddInt64(&s.RequestCount, 1)
	defer atomic.AddInt64(&s.RequestCount, -1)
	c.Next()
}

func (s *Service) waitAllRequestsDone(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	shutdownTimer := time.NewTimer(DefaultShutdownTimeout)
	defer shutdownTimer.Stop()
	ticker := time.NewTicker(serverShutdownPoll)
	defer ticker.Stop()
	for {
		select {
		case <-shutdownTimer.C:
			span.Warnf("timed out. some requests are still active. doing abnormal shutdown")
			return
		case <-ticker.C:
			if atomic.LoadInt64(&s.RequestCount) <= 0 {
				return
			}
		}
	}
}

func (s *Service) Close() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "ServiceClose")

	// notify loop
	close(s.closeCh)
	span.Warnf("close closeCh done")

	s.cancel()

	// wait all requests done.
	s.waitAllRequestsDone(ctx)
	span.Warnf("all requests done")

	// sync chunks
	chunks := s.copyChunkStorages(ctx)
	for _, cs := range chunks {
		if err := cs.Sync(ctx); err != nil {
			span.Errorf("Failed sync %s. err:%v", err)
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// reset chunks
	for _, d := range s.Disks {
		d.ResetChunks(ctx)
	}

	// reset nil
	s.Disks = nil

	// at least two rounds of garbage collection
	for i := 0; i < 4; i++ {
		runtime.GC()
		time.Sleep(time.Second)
	}

	s.closed = true

	s.WorkerService.Close()

	span.Info("service close done.")
}

/*
 *  method:         GET
 *  url:            /stat
 *  response body:  json.Marshal([]DiskInfo)
 */
func (s *Service) Stat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("stat")

	s.lock.RLock()
	diskinfos := make([]*bnapi.DiskInfo, 0)
	for i := range s.Disks {
		diskInfo := s.Disks[i].DiskInfo()
		diskinfos = append(diskinfos, &(diskInfo))
	}
	s.lock.RUnlock()
	c.RespondJSON(diskinfos)
}

/*
 *  method:         GET
 *  url:            /debug/stat
 *  response body:  json.Marshal([]DiskInfo)
 */
func (s *Service) DebugStat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("debug stat")

	disks := s.copyDiskStorages(ctx)
	chunks := make([]core.ChunkAPI, 0)
	for _, ds := range disks {
		_ = ds.WalkChunksWithLock(ctx, func(cs core.ChunkAPI) (err error) {
			chunks = append(chunks, cs)
			return nil
		})
	}

	ret := make(map[string]interface{})
	ret["chunks"] = chunks
	c.RespondJSON(ret)
}

/*
 *  method:         GET
 *  url:            /disk/stat/diskid/{diskid}
 *  response body:  json.Marshal(DiskInfo)
 */
func (s *Service) DiskStat(c *rpc.Context) {
	args := new(bnapi.DiskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("diskstat args: %v", args)
	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskID %d not exist", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	info := ds.DiskInfo()
	c.RespondJSON(&info)
}

func (s *Service) copyDiskStorages(ctx context.Context) []core.DiskAPI {
	disks := make([]core.DiskAPI, 0)
	s.lock.RLock()
	for _, ds := range s.Disks {
		disks = append(disks, ds)
	}
	s.lock.RUnlock()
	return disks
}

func (s *Service) copyChunkStorages(ctx context.Context) []core.ChunkAPI {
	disks := s.copyDiskStorages(ctx)
	chunks := make([]core.ChunkAPI, 0)
	for _, ds := range disks {
		_ = ds.WalkChunksWithLock(ctx, func(cs core.ChunkAPI) (err error) {
			chunks = append(chunks, cs)
			return nil
		})
	}
	return chunks
}
