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

package clustermgr

import (
	"encoding/json"
	"io"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/catalog"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	maxReportShardBodyLength = 1 << 23
)

func (s *Service) ShardUnitAlloc(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.AllocShardUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ShardUnitAlloc request, args: %v", args)

	if !s.CatalogMgr.IsShardInitDone(ctx) {
		span.Warn("shard init is not done")
		c.RespondError(apierrors.ErrShardInitNotDone)
		return
	}

	ret, err := s.CatalogMgr.AllocShardUnit(ctx, args.Suid)
	if err != nil {
		span.Error("alloc shardUnit failed, err: ", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) ShardUnitList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListShardUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ShardUnitList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("list units read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	unitInfos, err := s.CatalogMgr.ListShardUnitInfo(ctx, args)
	if err != nil {
		span.Error(errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(&clustermgr.ListShardUnitRet{ShardUnitInfos: unitInfos})
}

func (s *Service) ShardUpdate(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.UpdateShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ShardUpdate request, args: %v", args)

	err := s.CatalogMgr.PreUpdateShardUnit(ctx, args)
	if err != nil {
		if errors.Is(err, catalog.ErrRepeatUpdateShardUnit) {
			span.Info("repeat update shard unit, ignore and return success")
			return
		}
		span.Errorf("update shard error:%v", err)
		c.RespondError(err)
		return
	}
	err = s.CatalogMgr.UpdateShardUnit(ctx, args)
	if err != nil {
		span.Error(errors.Detail(err))
		c.RespondError(err)
		return
	}
}

func (s *Service) ShardGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.GetShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ShardGet request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("get read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.CatalogMgr.GetShardInfo(ctx, args.ShardID)
	if err != nil {
		span.Errorf("get shard error, shardID is: %v, error:%v", args.ShardID, err)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) ShardList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ShardList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("list read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	infos, err := s.CatalogMgr.ListShardInfo(ctx, args)
	if err != nil {
		span.Errorf("list shard error,args is: %v, error:%v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}

	if len(infos) > 0 {
		c.RespondJSON(&clustermgr.ListShardRet{Shards: infos, Marker: infos[len(infos)-1].ShardID})
		return
	}
}

func (s *Service) ShardReport(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if c.Request.ContentLength > maxReportShardBodyLength {
		span.Warnf("shard report body length exceed limit: %d, body length: %d", maxReportShardBodyLength, c.Request.ContentLength)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}

	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		span.Warnf("shard report read body err: %v", err)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	args := &clustermgr.ShardReportArgs{}
	if err = args.Unmarshal(raw); err != nil {
		span.Errorf("decode report shard arguments failed, err: %v", err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	span.Debugf("accept ShardReport request, args: %v", args)

	ret, err := s.CatalogMgr.ReportShard(ctx, args, raw)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) AdminUpdateShard(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.Shard)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if args.Range.Type != sharding.RangeType_RangeTypeHash {
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	span.Debugf("accept AdminUpdateShard request, args: %v", args)

	shard, err := s.CatalogMgr.GetShardInfo(ctx, args.ShardID)
	if err != nil {
		c.RespondError(err)
		return
	}
	if args.RouteVersion != 0 {
		shard.RouteVersion = args.RouteVersion
	}
	if !args.Range.IsEmpty() {
		shard.Range = args.Range
	}

	data, err := json.Marshal(shard)
	if err != nil {
		span.Errorf("admin update shard json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.CatalogMgr.GetModuleName(), catalog.OperTypeAdminUpdateShard, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) AdminUpdateShardUnit(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.AdminUpdateShardUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept AdminUpdateShardUnit request, args: %v", args)

	shard, err := s.CatalogMgr.GetShardInfo(ctx, args.Suid.ShardID())
	if err != nil {
		c.RespondError(err)
		return
	}

	if int(args.Suid.Index()) >= len(shard.Units) {
		span.Errorf("admin update shard unit, index: %d over suids length ", args.Suid.Index())
		c.RespondError(apierrors.ErrShardUnitNotExist)
		return
	}

	if !proto.IsValidEpoch(args.Epoch) || !proto.IsValidEpoch(args.NextEpoch) {
		span.Errorf("epoch: %d or nextEpoch: %d not valid", args.Epoch, args.NextEpoch)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	if args.DiskID > 0 {
		_, err := s.ShardNodeMgr.GetDiskInfo(ctx, args.DiskID)
		if err != nil {
			c.RespondError(err)
			return
		}
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("update shard unit json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(err)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.CatalogMgr.GetModuleName(), catalog.OperTypeAdminUpdateShardUnit, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}
