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
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// GetCacheVolume returns volume in cacher.
func (s *Service) GetCacheVolume(c *rpc.Context) {
	args := new(proxy.CacheVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	volume, err := s.cacher.GetVolume(ctx, args)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Warnf("get volume args:%v error:%s", args, err.Error())
		c.RespondError(err)
		return
	}

	c.RespondJSON(volume)
}

// GetCacheDisk returns disk info in cacher.
func (s *Service) GetCacheDisk(c *rpc.Context) {
	args := new(proxy.CacheDiskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	disk, err := s.cacher.GetDisk(ctx, args)
	if err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Warnf("get disk args:%v error:%s", args, err.Error())
		c.RespondError(err)
		return
	}

	c.RespondJSON(disk)
}
