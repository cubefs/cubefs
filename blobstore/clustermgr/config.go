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
	"os"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/configmgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

// Get config: /config/get?key=enable_delete
func (s *Service) ConfigGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ConfigArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ConfigGet request key:%v\n", args.Key)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.ConfigMgr.Get(ctx, args.Key)
	if err == os.ErrNotExist {
		span.Errorf("config get err: %v", err)
		c.RespondError(apierrors.ErrNotFound)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) ConfigSet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ConfigSetArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ConfigSet request :%v\n", args)

	if args.Key == proto.CodeModeConfigKey {
		span.Warnf("code mode key not allow to set by api")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}

	if err := s.ConfigMgr.Set(ctx, args.Key, args.Value); err != nil {
		span.Errorf("ConfigSet json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
}

func (s *Service) ConfigDelete(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ConfigArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ConfigDelete request key:%v\n", args.Key)

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("ConfigDelete json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(errors.Info(apierrors.ErrConfigArgument).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.ConfigMgr.GetModuleName(), configmgr.OperTypeDeleteConfig, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

// Get all config: /config/list
func (s *Service) ConfigList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Debug("accept ConfigList request, ctx: ", ctx)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	ret := clustermgr.AllConfig{}
	if val, err := s.ConfigMgr.List(ctx); err == nil {
		ret.Configs = val
		c.RespondJSON(ret)
	} else {
		c.RespondError(err)
	}
}
