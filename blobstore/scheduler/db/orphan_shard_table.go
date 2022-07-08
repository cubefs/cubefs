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

package db

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// IOrphanShardTable define the interface to save orphan shard record.
type IOrphanShardTable interface {
	Save(shard OrphanShard) error
}

// OrphanShard orphan shard identification.
type OrphanShard struct {
	ClusterID proto.ClusterID `bson:"cluster_id"`
	Vid       proto.Vid       `bson:"vid"`
	Bid       proto.BlobID    `bson:"bid"`
}

type orphanShardTable struct {
	coll *mongo.Collection
}

func openOrphanedShardTable(coll *mongo.Collection) IOrphanShardTable {
	return &orphanShardTable{coll: coll}
}

func (t *orphanShardTable) Save(shard OrphanShard) error {
	_, err := t.coll.InsertOne(context.Background(), shard)
	return err
}
