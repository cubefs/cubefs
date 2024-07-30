// Copyright 2024 The CubeFS Authors.
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

package catalog

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	// scope name
	shardIDScopeName = "shard_id"
	spaceIDScopeName = "space_id"

	// defaultValue
	defaultFlushIntervalS                      = 600
	defaultListMaxCount                        = 2000
	defaultCheckInitShardIntervalS             = 60
	defaultShardConcurrentMapNum        uint32 = 10
	defaultSpaceConcurrentMapNum        uint32 = 10
	defaultApplyConcurrency             uint32 = 20
	defaultRouteItemTruncateIntervalNum uint32 = 1 << 14
)

var (
	ErrShardNotExist           = apierrors.ErrShardNotExist
	ErrShardUnitNotExist       = apierrors.ErrShardUnitNotExist
	ErrOldSuidNotMatch         = apierrors.ErrOldSuidNotMatch
	ErrNewSuidNotMatch         = apierrors.ErrNewSuidNotMatch
	ErrOldIsLeanerNotMatch     = apierrors.ErrOldIsLeanerNotMatch
	ErrNewDiskIDNotMatch       = apierrors.ErrNewDiskIDNotMatch
	ErrRepeatUpdateShardUnit   = errors.New("repeat update shard unit")
	ErrCreateShardAlreadyExist = errors.New("create shard already exist")
	ErrInvalidShard            = errors.New("shard is invalid ")
	ErrRepeatedApplyRequest    = errors.New("repeated apply request")
	ErrAllocShardFailed        = errors.New("alloc shard failed")
)

type CatalogMgr struct {
	module        string
	allShards     *concurrentShards
	allSpaces     *concurrentSpaces
	dirty         atomic.Value
	initShardDone int32
	applyTaskPool *base.TaskDistribution

	catalogTbl   *catalogdb.CatalogTable
	transitedTbl *catalogdb.TransitedTable

	raftServer      raftserver.RaftServer
	scopeMgr        scopemgr.ScopeMgrAPI
	routeMgr        *routeMgr
	diskMgr         cluster.ShardNodeManagerAPI
	shardNodeClient cluster.ShardNodeAPI

	lastFlushTime  time.Time
	pendingEntries sync.Map
	closeLoopChan  chan struct{}
	Config
}

// only leader node can create shard
func (c *CatalogMgr) loop() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	ticker := time.NewTicker(time.Second * time.Duration(c.CheckInitShardIntervalS))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.IsShardInitDone(ctx) {
				span.Warn("init create shard is done")
				return
			}
			if !c.raftServer.IsLeader() {
				continue
			}
			// finish last create shard job firstly
			if err := c.finishLastCreateJob(ctx); err != nil {
				span.Errorf("finish last create shard job failed ==> %s", errors.Detail(err))
				continue
			}
			// finish last create shard job can lead to shard init done
			if c.IsShardInitDone(ctx) {
				span.Warn("init create shard is done")
				return
			}
			span.Info("leader node start to create shard")
			if err := c.createShard(ctx); err != nil {
				span.Errorf("create shard failed ==> %s", errors.Detail(err))
				continue
			}

		case <-c.closeLoopChan:
			return
		}
	}
}
