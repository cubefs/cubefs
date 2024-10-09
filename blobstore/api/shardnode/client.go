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

package shardnode

import (
	"context"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

type Config = rpc2.Client

type Client struct {
	rpc2.Client
}

func New(cli Config) *Client {
	defaulter.Empty(&cli.ConnectorConfig.Network, "tcp")
	defaulter.IntegerLessOrEqual(&cli.ConnectorConfig.DialTimeout.Duration, 200*time.Millisecond)
	return &Client{Client: cli}
}

func (c *Client) doRequest(ctx context.Context, host, path string, args rpc2.Marshaler, ret rpc2.Unmarshaler) (err error) {
	req, err := rpc2.NewRequest(ctx, host, path, nil, rpc2.Codec2Reader(args))
	if err != nil {
		return
	}
	return c.DoWith(req, ret)
}
