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

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
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

	dis := make([]*bnapi.DiskHeartBeatInfo, 0)
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
		diskId := diskInfo.DiskID
		status := diskInfo.Status

		s.lock.RLock()
		ds, exist := s.Disks[diskId]
		s.lock.RUnlock()
		if !exist {
			span.Errorf("no such disk: %v", diskId)
			continue
		}

		if ds.Status() >= proto.DiskStatusBroken {
			span.Warnf("non normal disk(%v), but still in disks. skip.", diskId)
			continue
		}

		if status >= proto.DiskStatusBroken {
			span.Warnf("notify broken. diskID:%v ds.Status:%v, status:%v", diskId, ds.Status(), status)
			s.handleDiskIOError(ctx, diskId, bloberr.ErrDiskBroken)
			continue
		}

	}
}
