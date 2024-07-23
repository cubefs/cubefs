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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/cluster"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *Service) ShardNodeDiskIDAlloc(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Info("accept ShardNodeDiskIDAlloc request")
	diskID, err := s.ShardNodeMgr.AllocDiskID(ctx)
	if err != nil {
		span.Error("alloc disk id failed =>", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(&clustermgr.DiskIDAllocRet{DiskID: diskID})
}

func (s *Service) ShardNodeDiskAdd(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ShardNodeDiskInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ShardNodeDiskAdd request, args: %v", args)

	if args.ClusterID != s.ClusterID {
		span.Warn("invalid clusterID")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	curDiskID := s.ScopeMgr.GetCurrent(cluster.ShardNodeDiskIDScopeName)
	if proto.DiskID(curDiskID) < args.DiskID {
		span.Warn("invalid disk_id")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	curNodeID := s.ScopeMgr.GetCurrent(cluster.ShardNodeIDScopeName)
	if proto.NodeID(curNodeID) < args.NodeID {
		span.Warn("invalid node_id")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	err := s.ShardNodeMgr.AddDisk(ctx, args)
	if err != nil {
		c.RespondError(err)
		return
	}
}

func (s *Service) ShardNodeDiskInfo(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DiskInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ShardNodeDiskInfo request, args: %v", args)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("info read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.ShardNodeMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		span.Warnf("disk not found: %d", args.DiskID)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) ShardNodeDiskList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListOptionArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ShardNodeDiskList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("list read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	// idc can not be nil when rack param set
	if args.Rack != "" && args.Idc == "" {
		span.Warnf("can not list disk by rack only")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	if args.Marker != proto.InvalidDiskID {
		if _, err := s.ShardNodeMgr.GetDiskInfo(ctx, args.Marker); err != nil {
			span.Warnf("invalid marker, marker disk not exist")
			err = apierrors.ErrIllegalArguments
			c.RespondError(err)
			return
		}
	}
	if args.Count == 0 {
		args.Count = 10
	}

	disks, marker, err := s.ShardNodeMgr.ListDiskInfo(ctx, args)
	if err != nil {
		span.Errorf("list disk info failed =>", errors.Detail(err))
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
		return
	}
	ret := &clustermgr.ListShardNodeDiskRet{
		Disks:  disks,
		Marker: marker,
	}
	c.RespondJSON(ret)
}

func (s *Service) ShardNodeDiskSet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DiskSetArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ShardNodeDiskSet request, args: %v", args)

	// not allow to set disk dropped in this API
	if args.Status < proto.DiskStatusNormal || args.Status >= proto.DiskStatusDropped {
		c.RespondError(apierrors.ErrInvalidStatus)
		return
	}

	diskInfo, err := s.ShardNodeMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		c.RespondError(err)
		return
	}
	if diskInfo.Status == args.Status {
		return
	}

	err = s.ShardNodeMgr.SetStatus(ctx, args.DiskID, args.Status, false)
	if err != nil {
		span.Errorf("disk set failed =>", errors.Detail(err))
		c.RespondError(err)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("set args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.ShardNodeMgr.GetModuleName(), cluster.OperTypeSetDiskStatus, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
	if args.Status == proto.DiskStatusBroken {
		err = s.CatalogMgr.UpdateShardUnitStatus(ctx, args.DiskID)
		if err != nil {
			span.Error(errors.Detail(err))
			return
		}
	}
}

func (s *Service) ShardNodeDiskHeartbeat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ShardNodeDisksHeartbeatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	heartbeatDisks := make([]clustermgr.ShardNodeDiskHeartbeatInfo, 0)
	for i := range args.Disks {
		// filter frequentHeartBeat disk
		frequentHeartBeat, err := s.ShardNodeMgr.IsFrequentHeartBeat(args.Disks[i].DiskID, s.HeartbeatNotifyIntervalS)
		if err != nil {
			span.Errorf("filter frequent heartbeat disk %d failed, err: %v", args.Disks[i].DiskID, err)
			c.RespondError(err)
			return
		}
		if !frequentHeartBeat {
			heartbeatDisks = append(heartbeatDisks, args.Disks[i])
		} else {
			span.Warnf("disk %d heartbeat too frequent", args.Disks[i].DiskID)
		}
	}
	if len(heartbeatDisks) == 0 {
		return
	}

	args.Disks = heartbeatDisks
	data, err := json.Marshal(args)
	span.Debugf("heartbeat params: %s", string(data))
	if err != nil {
		span.Errorf("heartbeat args: %v, error: %v", args, err)
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.ShardNodeMgr.GetModuleName(), cluster.OperTypeHeartbeatDiskInfo, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) AdminShardNodeDiskUpdate(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ShardNodeDiskInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept AdminShardNodeDiskUpdate request, args: %v", args)

	_, err := s.ShardNodeMgr.GetDiskInfo(ctx, args.DiskID)
	if err != nil {
		span.Errorf("admin update disk:%d not exist", args.DiskID)
		c.RespondError(err)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("update args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.ShardNodeMgr.GetModuleName(), cluster.OperTypeAdminUpdateDisk, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}
