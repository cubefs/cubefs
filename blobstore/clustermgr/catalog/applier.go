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
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeCreateSpace = iota + 1
	OperTypeInitCreateShard
	OperTypeCreateShard
	OperTypeIncreaseShardUnitsEpoch
	OperTypeAllocShardUnit
	OperTypeUpdateShardUnit
	OperTypeUpdateShardUnitStatus
	OperTypeShardReport
	OperTypeAdminUpdateShard
	OperTypeAdminUpdateShardUnit
)

const (
	initShardDone       = 1
	synchronizedShardID = 1 // serialize the apply operation of shard to avoid inconsistent route version
	synchronizedSpaceID = 2
)

type initCreateShardCtx struct {
	Shards []createShardCtx `json:"shards"`
}

type allocShardUnitCtx struct {
	Suid           proto.Suid  `json:"suid"`
	NextEpoch      uint32      `json:"next_epoch"`
	PendingSuidKey interface{} `json:"pending_suid_key"`
}

type createShardCtx struct {
	ShardID        proto.ShardID              `json:"shard_id"`
	ShardInfo      shardInfoBase              `json:"shard_info"`
	ShardUnitInfos []clustermgr.ShardUnitInfo `json:"shard_unit_infos"`
}

func (c *createShardCtx) toShard() *shardItem {
	units := make([]*shardUnit, len(c.ShardUnitInfos))
	for i, suInfo := range c.ShardUnitInfos {
		units[i] = &shardUnit{
			suidPrefix: suInfo.Suid.SuidPrefix(),
			epoch:      suInfo.Suid.Epoch(),
			nextEpoch:  suInfo.Suid.Epoch(),
			info:       &suInfo,
		}
	}
	shard := &shardItem{
		shardID: c.ShardID,
		units:   units,
		info:    c.ShardInfo,
	}
	return shard
}

func (c *CatalogMgr) LoadData(ctx context.Context) error {
	if err := c.loadShard(ctx); err != nil {
		return errors.Info(err, "load shard failed").Detail(err)
	}
	if c.allShards.getShardNum() == c.InitShardNum {
		atomic.StoreInt32(&c.initShardDone, initShardDone)
	}
	if err := c.loadSpace(ctx); err != nil {
		return errors.Info(err, "load space failed").Detail(err)
	}
	return nil
}

func (c *CatalogMgr) GetModuleName() string {
	return c.module
}

func (c *CatalogMgr) SetModuleName(module string) {
	c.module = module
}

func (c *CatalogMgr) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(operTypes))
	errs := make([]error, len(operTypes))

	for i, t := range operTypes {
		idx := i
		taskSpan, taskCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[idx].ReqID)
		switch t {
		case OperTypeCreateSpace:
			args := &clustermgr.Space{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx])
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedSpaceID, func() {
				defer wg.Done()
				if err = c.applyCreateSpace(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply create space failed, args: ", args).Detail(err)
				}
			})

		case OperTypeInitCreateShard:
			args := &initCreateShardCtx{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx])
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyInitCreateShard(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply initial create shard failed").Detail(err)
				}
				wg.Done()
			})

		case OperTypeCreateShard:
			args := &createShardCtx{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx])
				wg.Done()
				continue
			}
			shard := args.toShard()
			if err != nil {
				errs[idx] = errors.Info(err, "transform create shard context into shard failed, create shard context: ", args).Detail(err)
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyCreateShard(taskCtx, shard); err != nil {
					errs[idx] = errors.Info(err, "apply create shard failed, shard: ", shard)
				}
				wg.Done()
			})

		case OperTypeIncreaseShardUnitsEpoch:
			args := make([]*catalogdb.ShardUnitInfoRecord, 0)
			err := json.Unmarshal(datas[idx], &args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx])
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyIncreaseShardUnitsEpoch(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply increase units epoch failed")
				}
				wg.Done()
			})

		case OperTypeAllocShardUnit:
			args := &allocShardUnitCtx{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyAllocShardUnit(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply alloc shard unit failed, args: ", args)
				}
				wg.Done()
			})

		case OperTypeUpdateShardUnit:
			args := &clustermgr.UpdateShardArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyUpdateShardUnit(taskCtx, args.NewSuid, args.NewDiskID, args.NewIsLeaner); err != nil {
					errs[idx] = errors.Info(err, "apply update shard unit failed, args: ", args)
				}
				wg.Done()
			})

		case OperTypeUpdateShardUnitStatus:
			args := make([]proto.SuidPrefix, 0, c.CodeMode.GetShardNum())
			err := json.Unmarshal(datas[idx], &args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyUpdateShardUnitStatus(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply update shard unit status failed, args: ", args)
				}
				wg.Done()
			})

		case OperTypeShardReport:
			start := time.Now()
			args := &clustermgr.ShardReportArgs{}
			err = args.Unmarshal(datas[idx])
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx])
				wg.Done()
				continue
			}
			unmarshalCost := time.Since(start) / time.Microsecond
			start = time.Now()
			// shard report only modify shardUnit leaner and leader info, it's safe when apply concurrently
			c.applyTaskPool.Run(rand.Intn(int(c.ApplyConcurrency)), func() {
				if err = c.applyShardReport(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply shard report failed, args: ", args)
				}
				wg.Done()
				taskSpan.Debugf("receive apply msg, unmarshal cost: %dus, apply shard report cost: %dus",
					unmarshalCost, time.Since(start)/time.Microsecond)
			})

		case OperTypeAdminUpdateShard:
			args := &clustermgr.Shard{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyAdminUpdateShard(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply admin shard update failed, args: ", args)
				}
				wg.Done()
			})

		case OperTypeAdminUpdateShardUnit:
			args := &clustermgr.AdminUpdateShardUnitArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, t, datas[idx])
				wg.Done()
				continue
			}
			c.applyTaskPool.Run(synchronizedShardID, func() {
				if err = c.applyAdminUpdateShardUnit(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply admin shard unit update failed, args: ", args)
				}
				wg.Done()
			})

		default:
			errs[idx] = errors.New("unsupported operation")
			wg.Done()
		}
	}

	wg.Wait()
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount += 1
			span.Errorf("operation type: %d, apply failed => ", operTypes[i], errors.Detail(errs[i]))
		}
	}
	if failedCount > 0 {
		return errors.New(fmt.Sprintf("%s batch apply failed, failed count: %d", c.module, failedCount))
	}

	return nil
}

func (c *CatalogMgr) Flush(ctx context.Context) error {
	if time.Since(c.lastFlushTime) < time.Duration(c.FlushIntervalS)*time.Second {
		return nil
	}
	c.lastFlushTime = time.Now()

	var (
		retErr error
		span   = trace.SpanFromContextSafe(ctx)
	)

	dirty := c.dirty.Load().(*concurrentShards)
	c.dirty.Store(newConcurrentShards(c.ShardConcurrentMapNum))

	dirty.rangeShard(func(shard *shardItem) (err error) {
		select {
		case <-ctx.Done():
			return errors.New("cancel ctx")
		default:
		}
		err = shard.withRLocked(func() error {
			shardRecords := []*catalogdb.ShardInfoRecord{shard.toShardRecord()}
			shardUnitRecords := shardUnitsToShardUnitRecords(shard.units)
			return c.catalogTbl.PutShardsAndUnitsAndRouteItems(shardRecords, shardUnitRecords, nil)
		})
		retErr = err
		return
	})

	if retErr != nil {
		span.Error("CatalogMgr flush put shard units failed, err: ", retErr)
		return retErr
	}
	return nil
}

func (c *CatalogMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
	// Do nothing.
}
