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

	"github.com/globalsign/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

//duties:transfer the deleted records to the archive table

// IArchiveTable define the interface of db use by archive
type IArchiveTable interface {
	Insert(ctx context.Context, record *proto.ArchiveRecord) error
	FindTask(ctx context.Context, taskID string) (record *proto.ArchiveRecord, err error)
}

// IRecordSrcTbl define the interface of source record table used by archive
type IRecordSrcTbl interface {
	QueryMarkDeleteTasks(ctx context.Context, delayMin int) (records []*proto.ArchiveRecord, err error)
	RemoveMarkDelete(ctx context.Context, taskID string) error
	Name() string
}

type archiveTbl struct {
	coll *mongo.Collection
}

func openArchiveTbl(coll *mongo.Collection) (IArchiveTable, error) {
	return &archiveTbl{
		coll: coll,
	}, nil
}

// Insert insert record
func (tbl *archiveTbl) Insert(ctx context.Context, record *proto.ArchiveRecord) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("archiveTbl:insert task %s", record.TaskID)

	record.ArchiveTime = time.Now().String()
	_, err := tbl.coll.InsertOne(ctx, record)
	return err
}

// FindTask find task by taskID
func (tbl *archiveTbl) FindTask(ctx context.Context, taskID string) (record *proto.ArchiveRecord, err error) {
	err = tbl.coll.FindOne(ctx, bson.M{"_id": taskID}).Decode(&record)
	return
}
