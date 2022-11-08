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
	"sync"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func (s *Service) loopGcRubbishChunkFile() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "GcRubbish")
	span.Infof("loop gc rubbish chunk file")

	ticker := time.NewTicker(time.Duration(s.Conf.ChunkGcIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop gc rubbish chunk file done.")
			return
		case <-ticker.C:
			s.GcRubbishChunk()
		}
	}
}

/* monitor chunkfile
 * 1. find miss: chunk file lost. report to ums
 * 2. find rubbish: no metadata, only data
 * 3. find rubbish: old epoch
 */

func (s *Service) GcRubbishChunk() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("GcRubbish"))

	span.Debugf("check rubbish chunk file start == ")
	defer span.Debugf("check rubbish chunk file end == ")

	// Step: clean rubbish chunk
	s.checkAndCleanRubbish(ctx)

	// Step: clean old epoch chunk
	s.checkAndCleanGarbageEpoch(ctx)
}

func (s *Service) checkAndCleanRubbish(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("check and clean rubbish start ==")
	defer span.Infof("chunk and clean rubbish end ==")

	disks := s.copyDiskStorages(ctx)

	// Step: check one disk.
	wg := &sync.WaitGroup{}
	for _, ds := range disks {
		if ds.Status() != proto.DiskStatusNormal {
			span.Debugf("disk(%v) is not normal, do not need to check chunk file", ds.ID())
			continue
		}
		wg.Add(1)
		go func(ds core.DiskAPI) {
			defer wg.Done()
			err := s.checkAndCleanDiskRubbish(ctx, ds)
			if err != nil {
				span.Errorf("%v check local chunk file failed: %v", ds.GetConfig().Path, err)
			}
		}(ds)
	}
	wg.Wait()
}

func (s *Service) checkAndCleanDiskRubbish(ctx context.Context, ds core.DiskAPI) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("disk:%v check and clean rubbish", ds.ID())

	// list chunks from meta
	mayBeLost, err := ds.GcRubbishChunk(ctx)
	if err != nil {
		span.Errorf("%s list chunks failed: %v", ds.GetMetaPath(), err)
		return
	}

	if len(mayBeLost) > 0 {
		span.Errorf("such chunk lost data: %v", mayBeLost)
		// lost chunk data, need manual intervention
		// todo: report to ums
		panic(err)
	}

	return
}

func (s *Service) checkAndCleanGarbageEpoch(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("check and clean garbage epoch chunk")

	disks := s.copyDiskStorages(ctx)

	for _, ds := range disks {
		cmVuids, err := s.ClusterMgrClient.ListVolumeUnit(ctx, &cmapi.ListVolumeUnitArgs{DiskID: ds.ID()})
		if err != nil {
			span.Errorf("get cm volume units(%v) info failed: %v", ds.ID(), err)
			continue
		}
		cmVuidMaps := make(map[proto.VuidPrefix]proto.Vuid)
		for _, vuidInfo := range cmVuids {
			cmVuidMaps[vuidInfo.Vuid.VuidPrefix()] = vuidInfo.Vuid
		}
		s.rangeChunks(ctx, ds, cmVuidMaps)
	}
}

func (s *Service) rangeChunks(ctx context.Context, ds core.DiskAPI, cmVuidMaps map[proto.VuidPrefix]proto.Vuid) {
	span := trace.SpanFromContextSafe(ctx)
	localChunks, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("get local chunks(%v) failed: %v", ds.ID(), err)
		return
	}
	for _, cs := range localChunks {
		createTime := time.Unix(0, int64(cs.ChunkId.UnixTime()))
		protectionPeriod := time.Duration(s.Conf.ChunkProtectionPeriodSec) * time.Second

		if time.Since(createTime) < protectionPeriod {
			span.Debugf("%s still in ctime protection", cs.ChunkId)
			continue
		}
		vuid := cs.Vuid
		vid := vuid.Vid()
		index := vuid.Index()
		vuidPre := vuid.VuidPrefix()
		if cmVuid, ok := cmVuidMaps[vuidPre]; ok {
			s.releaseEpochChunk(ctx, cs, ds, cmVuid)
			continue
		}
		volumeInfo, err := s.ClusterMgrClient.GetVolumeInfo(ctx, &cmapi.GetVolumeArgs{Vid: vid})
		if err != nil {
			span.Errorf("get volume(%v) info failed: %v", vid, err)
			continue
		}
		if index >= uint8(len(volumeInfo.Units)) {
			span.Errorf("vuid number does not match, volumeInfo:%v", volumeInfo)
			continue
		}
		// get the latest vuid
		cmVuid := volumeInfo.Units[index].Vuid
		s.releaseEpochChunk(ctx, cs, ds, cmVuid)
	}
}

func (s *Service) releaseEpochChunk(ctx context.Context, cs core.VuidMeta, ds core.DiskAPI, cmVuid proto.Vuid) {
	span := trace.SpanFromContextSafe(ctx)
	if cs.Vuid.Epoch() >= cmVuid.Epoch() {
		return
	}

	span.Warnf("vuid(%v) already expired, local epoch:%v, new epoch:%v", cs.Vuid, cs.Vuid.Epoch(), cmVuid.Epoch())

	// todo: Remove to cm
	// Important note: in the future, the logical consideration will be transferred to cm to judge
	err := ds.ReleaseChunk(ctx, cs.Vuid, true)
	if err != nil {
		span.Errorf("release ChunkStorage(%s) form disk(%v) failed: %v", cs.ChunkId, ds.ID(), err)
		return
	}
	span.Infof("vuid(%v) have been release", cs.Vuid)
}
