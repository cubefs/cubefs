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

package workutils

import (
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type record struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Vid       proto.Vid       `json:"vid"`
	Bid       proto.BlobID    `json:"bid"`
	Timestamp int64           `json:"ts"`
	ReqID     string          `json:"req_id"`
	Reason    string          `json:"reason"`
}

// DroppedBidRecorder dropped bid recorder
type DroppedBidRecorder struct {
	clusterID proto.ClusterID
	encoder   recordlog.Encoder
}

// Init init DroppedBidRecorder
func (r *DroppedBidRecorder) Init(cfg *recordlog.Config, clusterID proto.ClusterID) error {
	if r.encoder != nil {
		return nil
	}

	encoder, err := recordlog.NewEncoder(cfg)
	if err != nil {
		return err
	}

	r.clusterID = clusterID
	r.encoder = encoder
	return nil
}

// Write write dropped bid record
func (r *DroppedBidRecorder) Write(ctx context.Context, vid proto.Vid, bid proto.BlobID, reason string) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("drop blob: bid[%d], reason[%s]", bid, reason)

	if r.encoder == nil {
		return
	}

	record := record{
		ClusterID: r.clusterID,
		Vid:       vid,
		Bid:       bid,
		ReqID:     span.TraceID(),
		Reason:    reason,
		Timestamp: time.Now().Unix(),
	}
	err := r.encoder.Encode(record)
	if err != nil {
		span.Errorf("write dropped bid record failed: vid[%d], bid[%d], reason[%s], err[%+v]",
			record.Vid, record.Bid, record.Reason, err)
	}
}

// Close close safe
func (r *DroppedBidRecorder) Close() {
	r.encoder.Close()
}

var (
	droppedBidRecorder     *DroppedBidRecorder
	droppedBidRecorderOnce sync.Once
)

// DroppedBidRecorderInst make sure only one instance in global
func DroppedBidRecorderInst() *DroppedBidRecorder {
	droppedBidRecorderOnce.Do(func() {
		droppedBidRecorder = &DroppedBidRecorder{}
	})
	return droppedBidRecorder
}
