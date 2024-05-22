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

package clustermgr

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type NodeInfoArgs struct {
	NodeID proto.NodeID `json:"node_id"`
}

type NodeIDAllocRet struct {
	NodeID proto.NodeID `json:"node_id"`
}

// AllocNodeID alloc nodeID from cluster manager
func (c *Client) AllocNodeID(ctx context.Context) (proto.NodeID, error) {
	ret := &NodeIDAllocRet{}
	err := c.PostWith(ctx, "/nodeid/alloc", ret, rpc.NoneBody)
	if err != nil {
		return 0, err
	}
	return ret.NodeID, nil
}

// AddNode add a new node into cluster manager
func (c *Client) AddNode(ctx context.Context, info *blobnode.NodeInfo) (proto.NodeID, error) {
	ret := &NodeIDAllocRet{}
	err := c.PostWith(ctx, "/node/add", ret, info)
	if err != nil {
		return 0, err
	}
	return ret.NodeID, nil
}

// DropNode drop a node from cluster manager
func (c *Client) DropNode(ctx context.Context, id proto.NodeID) (err error) {
	err = c.PostWith(ctx, "/node/drop", nil, &NodeInfoArgs{NodeID: id})
	return
}

// NodeInfo get node info from cluster manager
func (c *Client) NodeInfo(ctx context.Context, id proto.NodeID) (ret *blobnode.NodeInfo, err error) {
	ret = &blobnode.NodeInfo{}
	err = c.GetWith(ctx, "/node/info?node_id="+id.ToString(), ret)
	return
}
