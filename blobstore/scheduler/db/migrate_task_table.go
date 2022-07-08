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
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// IMigrateTaskTable define the interface of db use by migrate
type IMigrateTaskTable interface {
	Insert(ctx context.Context, task *proto.MigrateTask) error
	Delete(ctx context.Context, taskID string) error
	MarkDeleteByDiskID(ctx context.Context, diskID proto.DiskID) error
	MarkDeleteByStates(ctx context.Context, states []proto.MigrateState) error
	Update(ctx context.Context, oldState proto.MigrateState, task *proto.MigrateTask) error
	Find(ctx context.Context, taskID string) (task *proto.MigrateTask, err error)
	FindByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error)
	FindAll(ctx context.Context) (tasks []*proto.MigrateTask, err error)

	IRecordSrcTbl
}

// MigrateTaskTbl migrate table
type MigrateTaskTbl struct {
	coll *mongo.Collection
	name string
}

// openMigrateTbl open migrate tables
func openMigrateTbl(coll *mongo.Collection, name string) (IMigrateTaskTable, error) {
	tbl := &MigrateTaskTbl{
		coll: coll,
		name: name,
	}
	err := tbl.createIndex()
	return tbl, err
}

func (tbl *MigrateTaskTbl) createIndex() error {
	ctx := context.Background()
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	mods := []mongo.IndexModel{
		{
			Keys:    bsonx.Doc{{Key: "state", Value: bsonx.Int32(-1)}},
			Options: options.Index().SetName("_state_").SetBackground(true),
		},
		{
			Keys:    bsonx.Doc{{Key: deleteMark, Value: bsonx.Int32(-1)}},
			Options: options.Index().SetName("_delete_mark_").SetBackground(true),
		},
		{
			Keys:    bsonx.Doc{{Key: "source_disk_id", Value: bsonx.Int32(-1)}},
			Options: options.Index().SetName("_source_disk_id_").SetBackground(true),
		},
	}

	_, err := tbl.coll.Indexes().CreateMany(ctx, mods, opts)
	return err
}

// Insert insert task to db
func (tbl *MigrateTaskTbl) Insert(ctx context.Context, task *proto.MigrateTask) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("DB:insert task, taskId: %s, %+v", task.TaskID, task)

	task.Ctime = time.Now().String()
	task.MTime = task.Ctime
	_, err := tbl.coll.InsertOne(ctx, task)
	return err
}

// Update update task
func (tbl *MigrateTaskTbl) Update(ctx context.Context, oldState proto.MigrateState, task *proto.MigrateTask) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("DB:update task, taskId: %s,state: %d", task.TaskID, task.State)

	task.MTime = time.Now().String()

	states := []proto.MigrateState{oldState, task.State}
	return tbl.coll.FindOneAndReplace(ctx, bson.M{"_id": task.TaskID, "state": bson.M{"$in": states}}, task).Err()
}

// Delete delete task
func (tbl *MigrateTaskTbl) Delete(ctx context.Context, taskID string) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("DB:delete task, taskID: %s", taskID)

	_, err := tbl.coll.UpdateOne(ctx, bson.M{"_id": taskID}, deleteBson())
	return err
}

// MarkDeleteByDiskID mark delete task by diskID
func (tbl *MigrateTaskTbl) MarkDeleteByDiskID(ctx context.Context, diskID proto.DiskID) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("delete db task by diskID %d", diskID)

	_, err := tbl.coll.UpdateMany(ctx, bson.M{"source_disk_id": diskID}, deleteBson())
	return err
}

// MarkDeleteByStates mark delete task by status
func (tbl *MigrateTaskTbl) MarkDeleteByStates(ctx context.Context, states []proto.MigrateState) error {
	_, err := tbl.coll.UpdateMany(ctx, bson.M{"state": bson.M{"$in": states}, deleteMark: bson.M{"$ne": true}}, deleteBson())
	return err
}

// FindAll returns all un mark delete task
func (tbl *MigrateTaskTbl) FindAll(ctx context.Context) (tasks []*proto.MigrateTask, err error) {
	cursor, err := tbl.coll.Find(ctx, bson.M{deleteMark: bson.M{"$ne": true}})
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &tasks)
	return tasks, err
}

// Find find task by taskID
func (tbl *MigrateTaskTbl) Find(ctx context.Context, taskID string) (task *proto.MigrateTask, err error) {
	err = tbl.coll.FindOne(ctx, bson.M{"_id": taskID, deleteMark: bson.M{"$ne": true}}).Decode(&task)
	return
}

// FindByDiskID find task by diskID
func (tbl *MigrateTaskTbl) FindByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error) {
	cursor, err := tbl.coll.Find(ctx, bson.M{"source_disk_id": diskID, deleteMark: bson.M{"$ne": true}})
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &tasks)
	return tasks, err
}

// QueryMarkDeleteTasks find mark delete task for archive
func (tbl *MigrateTaskTbl) QueryMarkDeleteTasks(ctx context.Context, delayMin int) (records []*proto.ArchiveRecord, err error) {
	span := trace.SpanFromContextSafe(ctx)

	type MigrateTaskEx struct {
		proto.MigrateTask `bson:",inline"`
		DelTime           int64 `bson:"del_time"`
	}
	var tasks []*MigrateTaskEx
	cursor, err := tbl.coll.Find(ctx, bson.M{deleteMark: true})
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &tasks)
	if err != nil {
		return nil, err
	}

	for _, task := range tasks {
		if inDelayTime(task.DelTime, delayMin) {
			span.Debugf("task_id %s is in delay time", task.TaskID)
			continue
		}

		r := &proto.ArchiveRecord{
			TaskID:   task.TaskID,
			TaskType: tbl.Name(),
			Content:  task,
		}
		records = append(records, r)
	}
	return records, nil
}

// RemoveMarkDelete remove mark delete task
func (tbl *MigrateTaskTbl) RemoveMarkDelete(ctx context.Context, taskID string) error {
	_, err := tbl.coll.DeleteOne(ctx, bson.M{"_id": taskID, deleteMark: true})
	return err
}

// Name return table name
func (tbl *MigrateTaskTbl) Name() string {
	return tbl.name
}
