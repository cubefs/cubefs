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
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/cubefs/cubefs/blobstore/common/mongoutil"
)

// Config mongo config
type Config struct {
	Mongo  mongoutil.Config `json:"mongo"`
	DBName string           `json:"db_name"`

	OrphanShardTable string `json:"orphaned_shard_table"`
	KafkaOffsetTable string `json:"kafka_offset_table"`
}

// Database used for database operate
type Database struct {
	DB *mongo.Database

	KafkaOffsetTable IKafkaOffsetTable
	OrphanShardTable IOrphanShardTable
}

// OpenDatabase open database
func OpenDatabase(conf *Config) (tables *Database, err error) {
	client, err := mongoutil.GetClient(conf.Mongo)
	if err != nil {
		return nil, err
	}
	db := client.Database(conf.DBName)
	tables = &Database{DB: db}

	tables.KafkaOffsetTable = openKafkaOffsetTable(mustCreateCollection(db, conf.KafkaOffsetTable))
	tables.OrphanShardTable = openOrphanedShardTable(mustCreateCollection(db, conf.OrphanShardTable))

	return
}

func mustCreateCollection(db *mongo.Database, collName string) *mongo.Collection {
	err := db.RunCommand(context.Background(), bsonx.Doc{{Key: "create", Value: bsonx.String(collName)}}).Err()
	if err == nil || strings.Contains(err.Error(), "already exists") {
		return db.Collection(collName)
	}
	panic(fmt.Sprintf("create collection error: %v", err))
}
