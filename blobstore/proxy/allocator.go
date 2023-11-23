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
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// Alloc vids and bids from allocator
func (s *Service) Alloc(c *rpc.Context) {
	args := new(proxy.AllocVolsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if args.BidCount == 0 || args.Fsize == 0 {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}
	span.Infof("accept Alloc request, args: %v", args)
	resp, err := s.volumeMgr.Alloc(ctx, args)
	if err != nil {
		span.Errorf("alloc volume failed, err: %v", err)
		c.RespondError(err)
		return
	}
	c.RespondJSON(resp)
	span.Infof("alloc request response: %v", resp)
}

// List the volumes in this allocator
func (s *Service) List(c *rpc.Context) {
	args := new(proxy.ListVolsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if !args.CodeMode.IsValid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	span.Infof("accept List request, args: %v", args)
	vids, volumes, err := s.volumeMgr.List(ctx, args.CodeMode)
	if err != nil {
		c.RespondError(err)
		return
	}
	resp := &proxy.VolumeList{
		Vids:    vids,
		Volumes: volumes,
	}
	c.RespondJSON(resp)
	span.Infof("list request response: %v", resp)
}

// Discard use for management to remove invalid volumes in time
func (s *Service) Discard(c *rpc.Context) {
	args := new(proxy.DiscardVolsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("accept Discard request, args: %v", args)

	err := s.volumeMgr.Discard(ctx, args)
	if err != nil {
		c.RespondError(err)
		return
	}
}
