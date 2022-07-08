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
	"context"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type Config struct {
	rpc.Config
}

type client struct {
	rpc.Client
}

type Client interface {
	MsgSender
	Allocator
}

func New(cfg *Config) Client {
	return &client{rpc.NewClient(&cfg.Config)}
}

func (c *client) VolumeAlloc(ctx context.Context, host string, args *AllocVolsArgs) (ret []AllocRet, err error) {
	ret = make([]AllocRet, 0)
	err = c.PostWith(ctx, host+"/volume/alloc", &ret, args)
	return
}

func (c *client) SendShardRepairMsg(ctx context.Context, host string, args *ShardRepairArgs) error {
	return c.PostWith(ctx, host+"/repairmsg", nil, args)
}

func (c *client) SendDeleteMsg(ctx context.Context, host string, args *DeleteArgs) error {
	return c.PostWith(ctx, host+"/deletemsg", nil, args)
}
