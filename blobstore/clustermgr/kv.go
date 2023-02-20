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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/kvmgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// Get get value
func (s *Service) KvGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.GetKvArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span.Debugf("accept KvGet request, args: %s", args.Key)
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	value, err := s.KvMgr.Get(args.Key)
	if err == kvstore.ErrNotFound {
		c.RespondError(apierrors.ErrNotFound)
		return
	}
	if err != nil {
		span.Errorf("get key failed,error: %v", err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	c.RespondJSON(&clustermgr.GetKvRet{Value: value})
}

func (s *Service) KvSet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.SetKvArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if args.Key == "" || args.Value == nil {
		span.Warn("set key and value not allow nil")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	if proto.IsSysConfigKey(args.Key) {
		span.Warnf("system config key:[%s] not allow to set by api", args.Key)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}

	span.Debugf("accept KvSet request, args: %+v", args)
	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("set key failed, error:%v", err)
		c.RespondError(err)
		return
	}

	err = s.raftNode.Propose(ctx, base.EncodeProposeInfo(s.KvMgr.GetModuleName(), kvmgr.OperTypeSetKv, data, base.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		span.Errorf("raft propose failed, error:%v", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func (s *Service) KvDelete(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.DeleteKvArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span.Debugf("accept KvDelete request, args: %v", args)
	if proto.IsSysConfigKey(args.Key) {
		span.Warnf("%s not allow delete", args.Key)
		c.RespondError(apierrors.ErrRejectDelSysConfig)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("marshal failed, error:%v", err)
		c.RespondError(err)
		return
	}

	err = s.raftNode.Propose(ctx, base.EncodeProposeInfo(s.KvMgr.GetModuleName(), kvmgr.OperTypeDeleteKv, data, base.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		span.Errorf("raft propose failed, error:%v", err)
		c.RespondError(apierrors.ErrRaftPropose)
	}
}

func (s *Service) KvList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListKvOpts)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept KvList request, args:%+v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("KvList read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.KvMgr.List(args)
	if err != nil {
		span.Errorf("list failed, error:%v", err)
		err = errors.Info(apierrors.ErrUnexpected).Detail(err)
		c.RespondError(err)
		return
	}

	c.RespondJSON(ret)
}
