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
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	IncreaseEpochInterval = 3
)

// 1. alloc shardID and shard range, then init shard unit basic info
// 2. save shard and unit info into transited table(raft propose)
// 3. alloc shards for shard, return if failed (the rest jobs will be finished by finishLastCreateJob)
// 4. raft propose createShard if 4th step success
// 5. success and return
func (c *CatalogMgr) createShard(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	// alloc shardID and shard range, and save initial shard and unit info into transited tbl
	createShardCtxs := make([]createShardCtx, 0, c.InitShardNum)
	minShardID, _, err := c.scopeMgr.Alloc(ctx, shardIDScopeName, c.InitShardNum)
	if err != nil {
		return errors.Info(err, "scope alloc shardID failed")
	}
	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 2, c.InitShardNum)
	unitCount := c.CodeMode.GetShardNum()
	for i := 0; i < len(ranges); i++ {
		shardID := proto.ShardID(minShardID + uint64(i))
		unitInfos := make([]clustermgr.ShardUnitInfo, unitCount)
		shardInfoUnits := make([]clustermgr.ShardUnit, unitCount)
		for index := 0; index < unitCount; index++ {
			suid := proto.EncodeSuid(shardID, uint8(index), proto.MinEpoch)
			unitInfos[index] = clustermgr.ShardUnitInfo{
				Suid:   suid,
				DiskID: proto.InvalidDiskID,
				Range:  *ranges[i],
			}
			shardInfoUnits[index] = clustermgr.ShardUnit{
				Suid:    suid,
				DiskID:  proto.InvalidDiskID,
				Learner: false,
			}
		}
		shardInfo := shardInfoBase{
			Shard: clustermgr.Shard{
				ShardID: shardID,
				Range:   *ranges[i],
				Units:   shardInfoUnits,
			},
		}
		createShardCtxs = append(createShardCtxs, createShardCtx{
			ShardID:        shardID,
			ShardInfo:      shardInfo,
			ShardUnitInfos: unitInfos,
		})
	}
	initCreateShardArgs := &initCreateShardCtx{Shards: createShardCtxs}
	data, err := json.Marshal(initCreateShardArgs)
	if err != nil {
		return errors.Info(err, "json marshal failed")
	}
	proposeInfo := base.EncodeProposeInfo(c.GetModuleName(), OperTypeInitCreateShard, data, base.ProposeContext{ReqID: span.TraceID()})
	if err = c.raftServer.Propose(ctx, proposeInfo); err != nil {
		return errors.Info(err, "raft propose initial create shard failed")
	}

	// create shard
	for _, createShardArgs := range createShardCtxs {
		// check avoid shard already exist
		if oldShard := c.allShards.getShard(createShardArgs.ShardID); oldShard != nil {
			return errors.Info(ErrCreateShardAlreadyExist, fmt.Sprintf("create shardID:%d already exist, please check scopeMgr alloc", createShardArgs.ShardID))
		}
		span, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", span.TraceID()+"/"+createShardArgs.ShardID.ToString())
		span.Debugf("create shard, context[%+v]", createShardArgs)

		// alloc shard for all units
		err := c.allocShardForAllUnits(ctx, &createShardArgs)
		if err != nil {
			return errors.Info(err, fmt.Sprintf("alloc shard[%d] unit failed", createShardArgs.ShardID))
		}
		span.Debugf("alloc shard for unit success, shard context[%+v]", createShardArgs)

		data, err := json.Marshal(createShardArgs)
		if err != nil {
			return errors.Info(err, "json marshal failed").Detail(err)
		}
		proposeInfo := base.EncodeProposeInfo(c.GetModuleName(), OperTypeCreateShard, data, base.ProposeContext{ReqID: span.TraceID()})
		if err = c.raftServer.Propose(ctx, proposeInfo); err != nil {
			return errors.Info(err, fmt.Sprintf("raft propose create shard[%d] failed", createShardArgs.ShardID)).Detail(err)
		}
	}

	return nil
}

func (c *CatalogMgr) finishLastCreateJob(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	var shardRecs []*catalogdb.ShardInfoRecord
	rangeErr := c.transitedTbl.RangeShard(func(rec *catalogdb.ShardInfoRecord) error {
		shardRecs = append(shardRecs, rec)
		return nil
	})
	if rangeErr != nil {
		return errors.Info(rangeErr, "transitedTbl range shard failed").Detail(rangeErr)
	}

	for _, rec := range shardRecs {
		span.Debugf("finish last create shard job, shard: %+v", rec)
		unitRecs := make([]catalogdb.ShardUnitInfoRecord, 0, len(rec.SuidPrefixes))
		unitInfos := make([]clustermgr.ShardUnitInfo, 0, len(rec.SuidPrefixes))
		shardInfoUnits := make([]clustermgr.ShardUnit, 0, len(rec.SuidPrefixes))
		for _, suidPrefix := range rec.SuidPrefixes {
			unitRec, err := c.transitedTbl.GetShardUnit(suidPrefix)
			if err != nil {
				return errors.Info(err, "get transited shard unit failed").Detail(err)
			}
			// must increase epoch of shard unit firstly
			unitRec.Epoch += IncreaseEpochInterval
			unitRecs = append(unitRecs, *unitRec)
			unit := shardUnitRecordToShardUnit(rec, unitRec)
			unitInfos = append(unitInfos, *unit.info)
			shardInfoUnits = append(shardInfoUnits, clustermgr.ShardUnit{
				Suid:    unit.info.Suid,
				DiskID:  unit.info.DiskID,
				Learner: unit.info.Learner,
			})
		}
		// save shard units into transited tbl
		data, err := json.Marshal(unitRecs)
		if err != nil {
			return errors.Info(err, "marshal propose units data failed").Detail(err)
		}
		proposeInfo := base.EncodeProposeInfo(c.GetModuleName(), OperTypeIncreaseShardUnitsEpoch, data, base.ProposeContext{ReqID: span.TraceID()})
		if err = c.raftServer.Propose(ctx, proposeInfo); err != nil {
			return errors.Info(err, "raft propose increase shard units epoch failed").Detail(err)
		}

		createShardArgs := &createShardCtx{
			ShardID:        rec.ShardID,
			ShardInfo:      shardRecordToShardInfo(rec, shardInfoUnits),
			ShardUnitInfos: unitInfos,
		}
		// alloc shard for all units
		err = c.allocShardForAllUnits(ctx, createShardArgs)
		if err != nil {
			return errors.Info(err, "alloc shard for unit failed").Detail(err)
		}
		span.Debugf("alloc shard for unit success, shard context[%+v]", createShardArgs)

		data, err = json.Marshal(createShardArgs)
		if err != nil {
			return errors.Info(err, "json marshal failed").Detail(err)
		}
		proposeInfo = base.EncodeProposeInfo(c.GetModuleName(), OperTypeCreateShard, data, base.ProposeContext{ReqID: span.TraceID()})
		if err = c.raftServer.Propose(ctx, proposeInfo); err != nil {
			return errors.Info(err, "raft propose create shard failed").Detail(err)
		}
	}
	return nil
}

func (c *CatalogMgr) applyInitCreateShard(ctx context.Context, args *initCreateShardCtx) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debug("start apply initial create shard")

	shardRecs := make([]*catalogdb.ShardInfoRecord, 0, len(args.Shards))
	unitsRecs := make([]*catalogdb.ShardUnitInfoRecord, 0, len(args.Shards)*c.CodeMode.GetShardNum())

	for _, shard := range args.Shards {
		si := shard.toShard()
		shardRecs = append(shardRecs, si.toShardRecord())
		unitsRecs = append(unitsRecs, shardUnitsToShardUnitRecords(si.units)...)
	}
	if err := c.transitedTbl.PutShardsAndShardUnits(shardRecs, unitsRecs); err != nil {
		return errors.Info(err, "put shard and unit into transitedTbl failed")
	}
	return nil
}

func (c *CatalogMgr) applyIncreaseShardUnitsEpoch(ctx context.Context, units []*catalogdb.ShardUnitInfoRecord) error {
	return c.transitedTbl.PutShardUnits(units)
}

func (c *CatalogMgr) applyCreateShard(ctx context.Context, shard *shardItem) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply create shard, shardID[%d]", shard.shardID)

	if !shard.isValid() {
		return errors.Info(ErrInvalidShard, "create shard is invalid, shardID[%d], units[%qv]", shard.shardID, shard.units).Detail(ErrInvalidShard)
	}

	// already create, then return
	if c.allShards.getShard(shard.shardID) != nil {
		return nil
	}

	// insert route item
	routeVersion := c.routeMgr.genRouteVersion(ctx, 1)
	route := &routeItem{
		RouteVersion: proto.RouteVersion(routeVersion),
		Type:         proto.CatalogChangeItemAddShard,
		ItemDetail:   &routeItemShardAdd{ShardID: shard.shardID},
	}
	c.routeMgr.insertRouteItems(ctx, []*routeItem{route})
	shard.info.RouteVersion = proto.RouteVersion(routeVersion)
	for _, unit := range shard.units {
		unit.info.RouteVersion = proto.RouteVersion(routeVersion)
	}

	shardRecord := shard.toShardRecord()
	unitRecords := shardUnitsToShardUnitRecords(shard.units)
	// delete transited table firstly, put shard and units secondly.
	// it's idempotent when wal log replay
	if err := c.transitedTbl.DeleteShardAndUnits(shardRecord, unitRecords); err != nil {
		return errors.Info(err, fmt.Sprintf("delete shard [%d] from transitedTbl failed", shard.shardID)).Detail(err)
	}

	shardRecords := []*catalogdb.ShardInfoRecord{shardRecord}
	routeRecords := []*catalogdb.RouteInfoRecord{routeItemToRouteRecord(route)}
	if err := c.catalogTbl.PutShardsAndUnitsAndRouteItems(shardRecords, unitRecords, routeRecords); err != nil {
		return errors.Info(err, fmt.Sprintf("put shard[%+v], units[%+v] and route[%+v] into catalogTbl failed",
			shardRecord, unitRecords, routeRecords)).Detail(err)
	}
	c.allShards.putShard(shard)

	if c.allShards.getShardNum() == c.InitShardNum {
		err := c.kvMgr.Set(proto.ShardInitDoneKey, []byte("1"))
		if err != nil {
			return errors.Info(err, "put shard init done key to kv failed")
		}
		atomic.StoreInt32(&c.initShardDone, initShardDone)
	}

	return nil
}

// alloc shard for all shard units
func (c *CatalogMgr) allocShardForAllUnits(ctx context.Context, shardCtx *createShardCtx) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start alloc shard for all units,shard is %d", shardCtx.ShardID)

	idcCnt := c.CodeMode.Tactic().AZCount
	availableIDC := make([]string, 0)
	for i := range c.IDC {
		if c.IDC[i] == c.UnavailableIDC {
			continue
		}
		availableIDC = append(availableIDC, c.IDC[i])
	}
	if len(availableIDC) != idcCnt {
		span.Errorf("available idc count:%d not match codeMode idc count:%d", len(availableIDC), idcCnt)
		return errors.New("available idc count not match codeMode idc count")
	}

	suids := make([]proto.Suid, 0, c.CodeMode.GetShardNum())
	for _, suInfo := range shardCtx.ShardUnitInfos {
		suids = append(suids, suInfo.Suid)
	}

	// The shard routeVersion recorded by shardnode will be corrected when reporting shards
	policy := cluster.AllocShardsPolicy{
		DiskType:     proto.DiskTypeNVMeSSD,
		Suids:        suids,
		Range:        shardCtx.ShardInfo.Range,
		RouteVersion: proto.RouteVersion(c.routeMgr.getRouteVersion()),
	}

	for i := 0; i < IncreaseEpochInterval; i++ {
		disks, excludeDiskSetID, err := c.diskMgr.AllocShards(ctx, policy)
		if err != nil {
			span.Errorf("alloc shards failed, err: %v, tryTimes: %d", err, i)
			policy.ExcludeDiskSets = append(policy.ExcludeDiskSets, excludeDiskSetID)
			// when creating shard failed, shard epoch must be increased
			newSuids := make([]proto.Suid, 0, c.CodeMode.GetShardNum())
			for _, suid := range policy.Suids {
				suidPrefix := suid.SuidPrefix()
				newSuid := proto.EncodeSuid(suidPrefix.ShardID(), suidPrefix.Index(), suid.Epoch()+1)
				newSuids = append(newSuids, newSuid)
			}
			policy.Suids = newSuids
			continue
		}
		for index, suid := range policy.Suids {
			diskInfo, err := c.diskMgr.GetDiskInfo(ctx, disks[index])
			if err != nil {
				span.Errorf("allocated disk, get diskInfo [diskID:%d] error:%v", disks[index], err)
				return err
			}
			shardCtx.ShardUnitInfos[index].DiskID = disks[index]
			shardCtx.ShardUnitInfos[index].Host = diskInfo.Host
			shardCtx.ShardUnitInfos[index].Suid = suid
			shardCtx.ShardUnitInfos[index].Status = proto.ShardUnitStatusNormal

			shardCtx.ShardInfo.Units[index].DiskID = disks[index]
			shardCtx.ShardInfo.Units[index].Host = diskInfo.Host
			shardCtx.ShardInfo.Units[index].Suid = suid
		}
		return nil
	}

	return ErrAllocShardFailed
}
