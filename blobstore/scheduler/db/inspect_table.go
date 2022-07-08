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

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// record the current checkpoint of inspect which save min vid in last batch volumes
// service will start inspect worker from checkpoint when service start
const inspectID = "inspect_checkpoint"

// IInspectCheckPointTable define the interface of db used by inspect
type IInspectCheckPointTable interface {
	GetCheckPoint(ctx context.Context) (ck *proto.InspectCheckPoint, err error)
	SaveCheckPoint(ctx context.Context, startVid proto.Vid) error
}

// InspectCheckPointTable inspect check point table
type InspectCheckPointTbl struct {
	coll *mongo.Collection
}

// OpenInspectCheckPointTbl returns inspect check point table
func OpenInspectCheckPointTbl(coll *mongo.Collection) (IInspectCheckPointTable, error) {
	return &InspectCheckPointTbl{
		coll: coll,
	}, nil
}

// GetCheckPoint returns check point
func (tbl *InspectCheckPointTbl) GetCheckPoint(ctx context.Context) (ck *proto.InspectCheckPoint, err error) {
	err = tbl.coll.FindOne(ctx, bson.M{}).Decode(&ck)
	return ck, err
}

// SaveCheckPoint save check point
func (tbl *InspectCheckPointTbl) SaveCheckPoint(ctx context.Context, startVid proto.Vid) error {
	ck := proto.InspectCheckPoint{
		Id:       inspectID,
		StartVid: startVid,
		Ctime:    time.Now().String(),
	}
	trace.SpanFromContextSafe(ctx).Infof("save checkpoint %+v", ck)
	_, err := tbl.coll.ReplaceOne(ctx, bson.M{"_id": inspectID}, ck, options.Replace().SetUpsert(true))
	return err
}
