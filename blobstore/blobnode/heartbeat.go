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

package blobnode

import (
	"context"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func (s *Service) loopHeartbeatToClusterMgr() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "Heartbeat")
	span.Infof("loop heartbeat to cluster mgr")

	ticker := time.NewTicker(time.Duration(s.Conf.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop heartbeat done.")
			return
		case <-ticker.C:
			s.heartbeatToClusterMgr()
		}
	}
}

func (s *Service) heartbeatToClusterMgr() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("Heartbeat"))

	disks := s.copyDiskStorages(ctx)

	dis := make([]*cmapi.DiskHeartBeatInfo, 0)
	for _, ds := range disks {
		if ds.Status() != proto.DiskStatusNormal {
			continue
		}

		diskInfo := ds.DiskInfo()
		span.Debugf("id:%v, info: %v", diskInfo.DiskID, diskInfo)

		dis = append(dis, &diskInfo.DiskHeartBeatInfo)
	}

	// call clusterMgr client heartbeat
	heartbeatResult, err := s.ClusterMgrClient.HeartbeatDisk(ctx, dis)
	if err != nil {
		span.Errorf("heartbeat to clusterMgr failed: %v", err)
		return
	}

	// sync disk status
	s.syncDiskStatus(ctx, heartbeatResult)
}

func (s *Service) syncDiskStatus(ctx context.Context, diskInfosRet []*cmapi.DiskHeartbeatRet) {
	span := trace.SpanFromContextSafe(ctx)

	for _, diskInfo := range diskInfosRet {
		diskID := diskInfo.DiskID
		status := diskInfo.Status

		s.lock.RLock()
		ds, exist := s.Disks[diskID]
		s.lock.RUnlock()
		if !exist {
			span.Errorf("no such diskID: %d", diskID)
			continue
		}

		if !ds.IsWritable() { // local disk status, not normal disk, skip
			span.Warnf("non normal diskID(%d), but still in disks. skip.", diskID)
			continue
		}

		switch status { // remote cm disk status
		case proto.DiskStatusBroken, proto.DiskStatusRepairing, proto.DiskStatusRepaired:
			span.Warnf("notify broken. diskID:%d local.Status:%v, cm.status:%v", diskID, ds.Status(), status)
			s.handleDiskIOError(ctx, diskID, bloberr.ErrDiskBroken)
			continue

		case proto.DiskStatusDropped:
			span.Warnf("disk drop: diskID:%d local.Status:%v, cm.status:%v", diskID, ds.Status(), status)
			s.handleDiskDrop(ctx, ds)
			continue
		default:
		}
	}
}
