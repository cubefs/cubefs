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
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *Service) NodeAdd(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.BlobNodeInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept NodeAdd request, args: %v", args)

	if nodeID, ok := s.BlobNodeMgr.CheckNodeInfoDuplicated(ctx, &args.NodeInfo); ok {
		span.Warnf("node already exist, no need to create again, node info: %v", args)
		c.RespondJSON(&clustermgr.NodeIDAllocRet{NodeID: nodeID})
		return
	}
	if args.ClusterID != s.ClusterID {
		span.Warn("invalid clusterID")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	for i := range s.IDC {
		if args.Idc == s.IDC[i] {
			break
		}
		if i == len(s.IDC)-1 {
			span.Warnf("invalid idc %s, service idc: %v", args.Idc, s.IDC)
			c.RespondError(apierrors.ErrIllegalArguments)
			return
		}
	}
	if err := s.BlobNodeMgr.ValidateNodeInfo(ctx, &args.NodeInfo); err != nil {
		span.Warn("invalid nodeinfo")
		c.RespondError(err)
		return
	}

	nodeID, err := s.BlobNodeMgr.AllocNodeID(ctx)
	if err != nil {
		span.Errorf("alloc node id failed =>", errors.Detail(err))
		c.RespondError(err)
		return
	}
	args.NodeID = nodeID

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, node info: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.BlobNodeMgr.GetModuleName(), cluster.OperTypeAddNode, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
	c.RespondJSON(&clustermgr.NodeIDAllocRet{NodeID: nodeID})
}

func (s *Service) NodeDrop(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.NodeInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept NodeDrop request, args: %v", args)

	err := s.BlobNodeMgr.DropNode(ctx, args)
	if err != nil {
		c.RespondError(err)
		return
	}
}

func (s *Service) NodeInfo(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.NodeInfoArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept NodeInfo request, args: %v", args)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("node info read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.BlobNodeMgr.GetNodeInfo(ctx, args.NodeID)
	if err != nil {
		span.Warnf("node not found: %d", args.NodeID)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) TopoInfo(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Info("accept TopoInfo request")

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("topo info read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	c.RespondJSON(s.BlobNodeMgr.GetTopoInfo(ctx))
}
