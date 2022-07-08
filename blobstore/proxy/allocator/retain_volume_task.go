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

package allocator

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (v *volumeMgr) retainTask() {
	ticker := time.NewTicker(time.Duration(v.RetainIntervalS) * time.Second)
	defer ticker.Stop()
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("start retain.")
	for {
		select {
		case <-ticker.C:
			v.retain(ctx)
		case <-v.closeCh:
			span.Debugf("loop retain done.")
			return
		}
	}
}

// 1.Judgment of whether the volume is full or not
// 2.Lease renewal for volumes whose leases are about to expire
func (v *volumeMgr) retain(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	v.handleFullVols(ctx)
	retainTokenArgs := v.genRetainVolume(ctx)
	if len(retainTokenArgs) == 0 {
		return
	}
	span.Debugf("retain tokens: %v, lens:%v", retainTokenArgs, len(retainTokenArgs))
	args := &clustermgr.RetainVolumeArgs{
		Tokens: retainTokenArgs,
	}
	retainVolume, err := v.clusterMgr.RetainVolume(ctx, args)
	if err != nil {
		span.Errorf("retain volume from clusterMgr failed: %v", err)
		return
	}
	span.Debugf("retain result: %#v, lens:%v\n", retainVolume, len(retainVolume.RetainVolTokens))
	v.handleRetainResult(ctx, retainTokenArgs, retainVolume.RetainVolTokens)
}

// Determining whether a volume is full or not by heartbeat reduces the time of the allocation process.
func (v *volumeMgr) handleFullVols(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	for codeMode, volInfo := range v.modeInfos {
		fullVols := make([]proto.Vid, 0)
		vols := volInfo.volumes.List()
		for _, vol := range vols {
			vid := vol.Vid
			vol.mu.Lock()
			if vol.Free < uint64(v.VolumeReserveSize) {
				vol.deleted = true
				fullVols = append(fullVols, vid)
				atomic.AddUint64(&volInfo.totalFree, -vol.Free)
			}
			vol.mu.Unlock()
		}
		if len(fullVols) > 0 {
			for _, vid := range fullVols {
				volInfo.volumes.Delete(vid)
				span.Debugf("volume is full, vid:%v,codeMode:%v", vid, codeMode)
			}
		}
	}
}

// remove retain failed volume and update success expire time
func (v *volumeMgr) handleRetainResult(ctx context.Context, retainTokenArgs []string, retainRet []clustermgr.RetainVolume) {
	span := trace.SpanFromContextSafe(ctx)
	if len(retainRet) == 0 {
		for _, token := range retainTokenArgs {
			err := v.discardVolume(ctx, token)
			if err != nil {
				span.Error(err)
			}
		}
		return
	}
	tokenMap := make(map[string]int)
	for _, infos := range retainRet {
		_, vid, err := proto.DecodeToken(infos.Token)
		if err != nil {
			span.Errorf("decodeToken %v error", infos.Token)
			continue
		}
		err = v.updateExpiretime(vid, infos.ExpireTime)
		if err != nil {
			span.Error(err, vid)
		}
		tokenMap[infos.Token] = 1
	}
	for _, token := range retainTokenArgs {
		if _, ok := tokenMap[token]; ok {
			continue
		}
		err := v.discardVolume(ctx, token)
		if err != nil {
			span.Error(err, token)
		}
	}
}

func (v *volumeMgr) discardVolume(ctx context.Context, token string) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	_, vid, err := proto.DecodeToken(token)
	if err != nil {
		return
	}
	span.Debugf("retain failed vid:%v", vid)
	for _, modeInfo := range v.modeInfos {
		if vol, ok := modeInfo.volumes.Get(vid); ok {
			vol.mu.Lock()
			atomic.AddUint64(&modeInfo.totalFree, -vol.Free)
			vol.deleted = true
			vol.mu.Unlock()
			modeInfo.volumes.Delete(vid)
			return
		}
	}
	return errors.New("discardVolume, vid not in cache ")
}

// Generate volume information for lease renewal
func (v *volumeMgr) genRetainVolume(ctx context.Context) (tokens []string) {
	span := trace.SpanFromContextSafe(ctx)
	tokens = make([]string, 0, 128)
	vids := make([]proto.Vid, 0, 128)
	for _, volInfos := range v.modeInfos {
		vols := volInfos.volumes.List()
		for _, vol := range vols {
			vids = append(vids, vol.Vid)
			tokens = append(tokens, vol.Token)
		}
	}
	if len(vids) > 0 {
		span.Debugf("will retain volumes: %v, num of volumes: %v", vids, len(vids))
	}
	return
}

func (v *volumeMgr) updateExpiretime(vid proto.Vid, expireTime int64) (err error) {
	for _, modeInfo := range v.modeInfos {
		if vol, ok := modeInfo.volumes.Get(vid); ok {
			vol.ExpireTime = expireTime
			return nil
		}
	}
	return errors.New("vid does not exist ")
}
