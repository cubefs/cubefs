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

package catalog

import (
	"context"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (c *CatalogMgr) GetShardInfo(ctx context.Context, shardID proto.ShardID) (ret *cmapi.Shard, err error) {
	shard := c.allShards.getShard(shardID)
	if shard == nil {
		return nil, ErrShardNotExist
	}
	shardInfo := cmapi.Shard{}
	shard.withRLocked(func() error {
		shardInfo = shard.toShardInfo()
		return nil
	})
	return &shardInfo, nil
}

func (c *CatalogMgr) ListShardInfo(ctx context.Context, args *cmapi.ListShardArgs) (ret []cmapi.Shard, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if args.Count > defaultListMaxCount {
		args.Count = defaultListMaxCount
	}
	shardIDs, err := c.catalogTbl.ListShard(args.Count, args.Marker)
	if err != nil {
		span.Errorf("list shard failed:%v", err)
		return nil, errors.Info(err, "catalogMgr list shard failed").Detail(err)
	}
	if len(shardIDs) == 0 {
		return
	}

	ret = make([]cmapi.Shard, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		shard := c.allShards.getShard(shardID)
		shardInfo := cmapi.Shard{}
		shard.withRLocked(func() error {
			shardInfo = shard.toShardInfo()
			return nil
		})
		ret = append(ret, shardInfo)
	}

	return ret, nil
}
