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

package client

import (
	"context"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// ProxyAPI define the interface of proxy used by scheduler
type ProxyAPI interface {
	SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error
}

// proxyClient proxy client
type proxyClient struct {
	client    api.LbMsgSender
	clusterID proto.ClusterID
}

// NewProxyClient returns proxy client
func NewProxyClient(cfg *api.LbConfig, clusterMgr *cmapi.Client, clusterID proto.ClusterID) ProxyAPI {
	return &proxyClient{client: api.NewMQLbClient(cfg, clusterMgr, clusterID), clusterID: clusterID}
}

// SendShardRepairMsg send shard repair message
func (c *proxyClient) SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error {
	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "SendShardRepairMsg", pSpan.TraceID())
	span.Debugf("send shard repair msg vid %d bid %d badIdx %+v", vid, bid, badIdx)

	err := c.client.SendShardRepairMsg(ctx, &api.ShardRepairArgs{
		ClusterID: c.clusterID,
		Bid:       bid,
		Vid:       vid,
		BadIdxes:  badIdx,
		Reason:    "inspect",
	})

	span.Debugf("send shard repair msg ret err %+v", err)
	return err
}
