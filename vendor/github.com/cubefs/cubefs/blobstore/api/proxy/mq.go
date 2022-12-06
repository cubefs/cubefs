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

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type MsgSender interface {
	SendDeleteMsg(ctx context.Context, host string, args *DeleteArgs) error
	SendShardRepairMsg(ctx context.Context, host string, args *ShardRepairArgs) error
}

type LbMsgSender interface {
	SendDeleteMsg(ctx context.Context, args *DeleteArgs) error
	SendShardRepairMsg(ctx context.Context, args *ShardRepairArgs) error
}

type DeleteArgs struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Blobs     []BlobDelete    `json:"blobs"`
}

type BlobDelete struct {
	Bid proto.BlobID `json:"bid"`
	Vid proto.Vid    `json:"vid"`
}

type ShardRepairArgs struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Bid       proto.BlobID    `json:"bid"`
	Vid       proto.Vid       `json:"vid"`
	BadIdxes  []uint8         `json:"bad_idxes"`
	Reason    string          `json:"reason"`
}
