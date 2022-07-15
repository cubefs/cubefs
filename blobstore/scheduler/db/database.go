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
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/cubefs/cubefs/blobstore/common/mongoutil"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	// deleteMark for mark delete
	deleteMark = "delete_mark"
)

// Config mongo config
type Config struct {
	Mongo  mongoutil.Config `json:"mongo"`
	DBName string           `json:"db_name"`

	BalanceTable           string `json:"balance_table"`
	DiskDropTable          string `json:"disk_drop_table"`
	ManualMigrateTable     string `json:"manual_migrate_table"`
	RepairTable            string `json:"repair_table"`
	InspectCheckPointTable string `json:"inspect_checkpoint_table"`
	OrphanShardTable       string `json:"orphaned_shard_table"`
	KafkaOffsetTable       string `json:"kafka_offset_table"`

	ArchiveTasksTable string `json:"archive_tasks_table"`
}

// Database used for database operate
type Database struct {
	DB *mongo.Database

	BalanceTable       IMigrateTaskTable
	DiskDropTable      IMigrateTaskTable
	ManualMigrateTable IMigrateTaskTable
	RepairTaskTable    IRepairTaskTable

	KafkaOffsetTable IKafkaOffsetTable
	OrphanShardTable IOrphanShardTable

	InspectCheckPointTable IInspectCheckPointTable

	ArchiveTable IArchiveTable
}

// OpenDatabase open database
func OpenDatabase(conf *Config) (tables *Database, err error) {
	client, err := mongoutil.GetClient(conf.Mongo)
	if err != nil {
		return nil, err
	}
	db := client.Database(conf.DBName)
	tables = &Database{DB: db}

	if tables.BalanceTable, err = openMigrateTbl(
		mustCreateCollection(db, conf.BalanceTable),
		proto.TaskTypeBalance.String()); err != nil {
		return nil, err
	}
	if tables.DiskDropTable, err = openMigrateTbl(
		mustCreateCollection(db, conf.DiskDropTable),
		proto.TaskTypeDiskDrop.String()); err != nil {
		return nil, err
	}
	if tables.ManualMigrateTable, err = openMigrateTbl(
		mustCreateCollection(db, conf.ManualMigrateTable),
		proto.TaskTypeManualMigrate.String()); err != nil {
		return nil, err
	}
	if tables.RepairTaskTable, err = OpenRepairTaskTbl(
		mustCreateCollection(db, conf.RepairTable),
		proto.TaskTypeDiskRepair.String()); err != nil {
		return nil, err
	}
	if tables.InspectCheckPointTable, err = OpenInspectCheckPointTbl(
		mustCreateCollection(db, conf.InspectCheckPointTable)); err != nil {
		return nil, err
	}
	if tables.ArchiveTable, err = openArchiveTbl(
		mustCreateCollection(db, conf.ArchiveTasksTable)); err != nil {
		return nil, err
	}
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

func deleteBson() bson.M {
	return bson.M{"$set": bson.M{deleteMark: true, "del_time": time.Now().Unix()}}
}

func inDelayTime(delTime int64, delayMin int) bool {
	now := time.Now()
	return now.Sub(time.Unix(delTime, 0)) <= time.Duration(delayMin)*time.Minute
}
