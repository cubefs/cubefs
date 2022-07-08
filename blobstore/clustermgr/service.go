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

package clustermgr

import (
	"encoding/json"
	"net"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/servicemgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *Service) ServiceGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.GetServiceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ServiceGet request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	c.RespondJSON(s.ServiceMgr.GetServiceInfo(args.Name))
}

func (s *Service) ServiceRegister(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.RegisterArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ServiceRegister request, args: %v", args)

	if proto.ClusterID(args.ClusterID) != s.ClusterID {
		span.Warnf("register service clusterID:%d not match", args.ClusterID)
		c.RespondError(apierrors.ErrInvalidClusterID)
		return
	}
	isRightIdc := false
	for _, idc := range s.IDC {
		if idc == args.Idc {
			isRightIdc = true
			break
		}
	}
	if !isRightIdc {
		span.Warnf("register service idc:%d not match", args.Idc)
		c.RespondError(apierrors.ErrInvalidIDC)
		return
	}

	if !isValidHost(args.Host) {
		c.RespondError(apierrors.ErrRegisterServiceInvalidParams)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	err = s.raftNode.Propose(ctx, base.EncodeProposeInfo(s.ServiceMgr.GetModuleName(), servicemgr.OpRegister, data, base.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func isValidHost(host string) bool {
	// must contain http://
	if !strings.HasPrefix(host, "http://") {
		return false
	}
	host = string(host[7:])
	// must contain port
	if !strings.Contains(host, ":") {
		return false
	}
	host = host[:strings.Index(host, ":")]

	return net.ParseIP(host) != nil
}

func (s *Service) ServiceUnregister(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.UnregisterArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ServiceUnregister request, args: %v", args)

	if !s.ServiceMgr.IsRegistered(args.Name, args.Host) {
		span.Warnf("service %#v not exist", args)
		c.RespondError(apierrors.ErrNotFound)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	err = s.raftNode.Propose(ctx,
		base.EncodeProposeInfo(
			s.ServiceMgr.GetModuleName(),
			servicemgr.OpUnregister,
			data,
			base.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func (s *Service) ServiceHeartbeat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.HeartbeatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept ServiceHeartbeat request, args: %v", args)

	if !s.ServiceMgr.IsRegistered(args.Name, args.Host) {
		span.Warnf("service %#v not exist", args)
		c.RespondError(apierrors.ErrNotFound)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		c.RespondError(errors.Info(apierrors.ErrUnexpected).Detail(err))
		return
	}
	err = s.raftNode.Propose(ctx,
		base.EncodeProposeInfo(
			s.ServiceMgr.GetModuleName(),
			servicemgr.OpHeartbeat,
			data,
			base.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func (s *Service) ServiceList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Info("accept ServiceList request")

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	info, err := s.ServiceMgr.ListServiceInfo()
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(info)
}
