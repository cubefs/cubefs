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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IKafkaOffsetTable define interface of kafka offset table use by delete or repair message consume.
type IKafkaOffsetTable interface {
	Set(topic string, partition int32, offset int64) error
	Get(topic string, partition int32) (int64, error)
}

type kafkaOffset struct {
	Topic     string `bson:"topic"`
	Partition int32  `bson:"partition"`
	Offset    int64  `bson:"offset"`
}

type kafkaOffsetTable struct {
	coll *mongo.Collection
}

func openKafkaOffsetTable(coll *mongo.Collection) IKafkaOffsetTable {
	return &kafkaOffsetTable{coll: coll}
}

func (t *kafkaOffsetTable) Set(topic string, partition int32, off int64) error {
	info := kafkaOffset{Topic: topic, Partition: partition, Offset: off}
	selector := bson.M{"topic": topic, "partition": partition}

	update := bson.M{
		"$set": info,
	}
	opts := options.Update().SetUpsert(true)
	_, err := t.coll.UpdateOne(context.Background(), selector, update, opts)
	return err
}

func (t *kafkaOffsetTable) Get(topic string, partition int32) (int64, error) {
	infos := kafkaOffset{}
	selector := bson.M{"topic": topic, "partition": partition}
	err := t.coll.FindOne(context.Background(), &selector).Decode(&infos)
	return infos.Offset, err
}
