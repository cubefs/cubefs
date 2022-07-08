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
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// report chunk info to clusterMgr
func (s *Service) loopReportChunkInfoToClusterMgr() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "ChunkReport")
	span.Infof("loop report chunk to cluster mgr")

	ticker := time.NewTicker(time.Duration(s.Conf.ChunkReportIntervalSec) * time.Second)
	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop report chunk done.")
			return
		case <-ticker.C:
			s.reportChunkInfoToClusterMgr()
		}
	}
}

func (s *Service) reportChunkInfoToClusterMgr() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("ChunkReport"))

	chunks := s.copyChunkStorages(ctx)

	dirtychunks := make(map[proto.Vuid]core.ChunkAPI)
	cis := make([]bnapi.ChunkInfo, 0)
	for _, cs := range chunks {
		if cs.Disk().Status() >= proto.DiskStatusBroken || !cs.IsDirty() {
			continue
		}

		info := cs.ChunkInfo(ctx)
		cis = append(cis, info)

		dirtychunks[cs.Vuid()] = cs
		cs.SetDirty(false)
	}

	if len(cis) == 0 {
		span.Debugf("do not need to report")
		return
	}

	// call clusterMgr client chunk report
	reportChunkArg := &cmapi.ReportChunkArgs{
		ChunkInfos: cis,
	}
	err := s.ClusterMgrClient.ReportChunk(ctx, reportChunkArg)
	if err != nil {
		// relay next report
		for _, chunk := range cis {
			dirtychunks[chunk.Vuid].SetDirty(true)
		}
		span.Errorf("report chunks info to clusterMgr failed: %v", err)
		return
	}
	span.Debugf("report chunks info to clusterMgr success")
}
