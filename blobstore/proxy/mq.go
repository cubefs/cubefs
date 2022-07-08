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

package proxy

import (
	api "github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// SendRepairMessage send repair message to kafka
// 1. message from access
// 2. message from scheduler
func (s *Service) SendRepairMessage(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	args := new(api.ShardRepairArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept SendRepairMessage request, args: %v", args)
	if args.ClusterID != s.ClusterID {
		span.Errorf("clusterID not match: info[%+v], self clusterID[%d]", args, s.ClusterID)
		c.RespondError(errcode.ErrClusterIDNotMatch)
		return
	}

	err := s.shardRepairMgr.SendShardRepairMsg(ctx, args)
	if err != nil {
		span.Errorf("send shard repair message failed: %+v", err)
		c.RespondError(err)
		return
	}

	c.Respond()
}

// SendDeleteMessage send delete message to kafka
// 1. message from access because of put object fail
// 2. message from access because of business side delete
func (s *Service) SendDeleteMessage(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	args := new(api.DeleteArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span.Infof("accept SendDeleteMessage request, args: %v", args)
	if args.ClusterID != s.ClusterID {
		span.Errorf("clusterID not match: info[%+v], self clusterID[%d]", args, s.ClusterID)
		c.RespondError(errcode.ErrClusterIDNotMatch)
		return
	}

	err := s.blobDeleteMgr.SendDeleteMsg(ctx, args)
	if err != nil {
		span.Errorf("send delete message failed: %+v", err)
		c.RespondError(err)
		return
	}

	c.Respond()
}
