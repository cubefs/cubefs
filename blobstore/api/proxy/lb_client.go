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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/selector"
)

var errNoServiceAvailable = errors.New("no service available")

type LbConfig struct {
	Config

	RetryHostsCnt      int   `json:"retry_hosts_cnt"`
	HostSyncIntervalMs int64 `json:"host_sync_interval_ms"`
}

type lbClient struct {
	Client
	selector      selector.Selector
	retryHostsCnt int
}

func NewMQLbClient(cfg *LbConfig, service clustermgr.APIService, clusterID proto.ClusterID) LbMsgSender {
	hostGetter := func() ([]string, error) {
		svrInfos, err := service.GetService(context.Background(), clustermgr.GetServiceArgs{Name: proto.ServiceNameProxy})
		if err != nil {
			return nil, err
		}

		var hosts []string
		for _, s := range svrInfos.Nodes {
			if clusterID == proto.ClusterID(s.ClusterID) {
				hosts = append(hosts, s.Host)
			}
		}
		if len(hosts) == 0 {
			return nil, errNoServiceAvailable
		}

		return hosts, nil
	}

	if cfg.HostSyncIntervalMs == 0 {
		cfg.HostSyncIntervalMs = 1000
	}
	if cfg.RetryHostsCnt == 0 {
		cfg.RetryHostsCnt = 1
	}

	return &lbClient{
		retryHostsCnt: cfg.RetryHostsCnt,
		Client:        New(&cfg.Config),
		selector:      selector.NewSelectorWithGetter(cfg.HostSyncIntervalMs, hostGetter),
	}
}

func (c *lbClient) SendDeleteMsg(ctx context.Context, args *DeleteArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	hosts := c.selector.GetRandomN(c.retryHostsCnt)
	if len(hosts) == 0 {
		return errNoServiceAvailable
	}
	for _, h := range hosts {
		err = c.Client.SendDeleteMsg(ctx, h, args)
		if err == nil || !shouldRetry(err) {
			return err
		}
		span.Errorf("send delete message failed, host: %s, args: %+v, err:%+v", h, args, err)
	}

	return err
}

func (c *lbClient) SendShardRepairMsg(ctx context.Context, args *ShardRepairArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	ctx = trace.ContextWithSpan(ctx, span)

	hosts := c.selector.GetRandomN(c.retryHostsCnt)
	if len(hosts) == 0 {
		return errNoServiceAvailable
	}
	for _, h := range hosts {
		err = c.Client.SendShardRepairMsg(ctx, h, args)
		if err == nil || !shouldRetry(err) {
			return err
		}
		span.Errorf("seed shard repair failed, host: %s, args: %+v, err:%+v", h, args, err)
	}

	return err
}

func shouldRetry(err error) bool {
	if err == nil {
		return false // success
	}
	_, ok := err.(rpc.HTTPError)
	return ok
}
