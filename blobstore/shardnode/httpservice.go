// Copyright 2025 The CubeFS Authors.
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

package shardnode

import (
	"encoding/json"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type HttpService struct {
	*service
}

func (s *HttpService) HttpShardStats(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(shardnode.GetShardArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span.Debugf("HttpShardStats, args: %+v", args)
	ret, err := s.getShardStats(ctx, args.DiskID, args.Suid)
	if err != nil {
		c.RespondError(err)
		return
	}

	data, err := json.Marshal(ret)
	if err != nil {
		c.RespondError(err)
		return
	}

	c.RespondWith(http.StatusOK, rpc.MIMEJSON, data)
}

func (s *HttpService) HttpDeleteBlobStats(c *rpc.Context) {
	ret := s.deleteBlobStats()
	data, err := json.Marshal(ret)
	if err != nil {
		c.RespondError(err)
		return
	}

	c.RespondWith(http.StatusOK, rpc.MIMEJSON, data)
}

func newHttpHandler(service *HttpService) *rpc.Router {
	rpc.RegisterArgsParser(&shardnode.GetShardArgs{}, "json")

	rpc.GET("/shard/stats", service.HttpShardStats, rpc.OptArgsQuery())
	rpc.GET("/blob/delete/stats", service.HttpDeleteBlobStats)

	return rpc.DefaultRouter
}

func setUpHttp() (*rpc.Router, []rpc.ProgressHandler) {
	service := newService(&conf)
	return newHttpHandler(&HttpService{service}), nil
}
