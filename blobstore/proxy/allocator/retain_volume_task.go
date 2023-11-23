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
	"sync"
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
			v.retainAll(ctx)
		case <-v.closeCh:
			span.Debugf("loop retain done.")
			return
		}
	}
}

func (v *volumeMgr) retainAll(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		v.retain(ctx, false)
		wg.Done()
	}()
	go func() {
		v.retain(ctx, true)
		wg.Done()
	}()
	wg.Wait()
}

// 1.Judgment of whether the volume is full or not
// 2.Lease renewal for volumes whose leases are about to expire
func (v *volumeMgr) retain(ctx context.Context, isBackup bool) {
	span := trace.SpanFromContextSafe(ctx)

	v.handleFullVols(ctx, isBackup)

	retainTokenArgs := v.genRetainVolume(ctx, isBackup)
	numTokens := len(retainTokenArgs)
	if numTokens == 0 {
		return
	}
	span.Debugf("retain tokens: %v, lens: %v", retainTokenArgs, numTokens)

	end := 0
	start := 0
	for {
		end = start + v.RetainVolumeBatchNum
		if end > numTokens {
			end = numTokens
		}
		v.retainVolumeFromCm(ctx, retainTokenArgs[start:end], isBackup)
		start = end
		if start < numTokens {
			time.Sleep(time.Duration(v.RetainBatchIntervalS) * time.Second)
			continue
		}
		return
	}
}

// Determining whether a volume is full or not by heartbeat reduces the time of the allocation process.
func (v *volumeMgr) handleFullVols(ctx context.Context, isBackup bool) {
	span := trace.SpanFromContextSafe(ctx)
	for codeMode, info := range v.modeInfos {
		fullVols := make([]proto.Vid, 0)
		vols := info.List(isBackup)
		for _, vol := range vols {
			vid := vol.Vid
			vol.mu.Lock()
			if vol.Free < uint64(v.VolumeReserveSize) {
				vol.deleted = true
				fullVols = append(fullVols, vid)
			}
			vol.mu.Unlock()
		}
		if len(fullVols) > 0 {
			for _, vid := range fullVols {
				info.Delete(vid)
				span.Debugf("volume is full, vid: %v,codeMode: %v", vid, codeMode)
			}
		}
	}
}

// remove retain failed volume and update success expire time
func (v *volumeMgr) handleRetainResult(ctx context.Context, retainTokenArgs []string,
	retainRet []clustermgr.RetainVolume, isBackup bool) {
	span := trace.SpanFromContextSafe(ctx)
	if len(retainRet) == 0 {
		for _, token := range retainTokenArgs {
			err := v.discardVolume(ctx, token, isBackup)
			if err != nil {
				span.Error(err)
			}
		}
		return
	}
	tokenMap := make(map[string]int, len(retainRet))
	for _, infos := range retainRet {
		_, vid, err := proto.DecodeToken(infos.Token)
		if err != nil {
			span.Errorf("decodeToken %v error", infos.Token)
			continue
		}
		err = v.updateExpireTime(vid, infos.ExpireTime, isBackup)
		if err != nil {
			span.Error(err, vid)
		}
		tokenMap[infos.Token] = 1
	}
	for _, token := range retainTokenArgs {
		if _, ok := tokenMap[token]; ok {
			continue
		}
		err := v.discardVolume(ctx, token, isBackup)
		if err != nil {
			span.Error(err, token)
		}
	}
}

func (v *volumeMgr) discardVolume(ctx context.Context, token string, isBackup bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	_, vid, err := proto.DecodeToken(token)
	if err != nil {
		return
	}
	span.Debugf("retain failed vid: %v", vid)
	for _, info := range v.modeInfos {
		if vol, ok := info.Get(vid, isBackup); ok {
			vol.mu.Lock()
			vol.deleted = true
			vol.mu.Unlock()
			info.Delete(vid)
			return
		}
	}
	return errors.New("discardVolume, vid not in cache")
}

// Generate volume information for lease renewal
func (v *volumeMgr) genRetainVolume(ctx context.Context, isBackup bool) (tokens []string) {
	span := trace.SpanFromContextSafe(ctx)
	tokens = make([]string, 0, 128)
	vids := make([]proto.Vid, 0, 128)
	for _, info := range v.modeInfos {
		vols := info.List(isBackup)
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

func (v *volumeMgr) updateExpireTime(vid proto.Vid, expireTime int64, isBackup bool) (err error) {
	for _, modeInfo := range v.modeInfos {
		if vol, ok := modeInfo.Get(vid, isBackup); ok {
			vol.ExpireTime = expireTime
			return nil
		}
	}
	return errors.New("vid does not exist ")
}

func (v *volumeMgr) retainVolumeFromCm(ctx context.Context, tokens []string, isBackup bool) {
	span := trace.SpanFromContextSafe(ctx)
	args := &clustermgr.RetainVolumeArgs{
		Tokens: tokens,
	}
	retainVolume, err := v.clusterMgr.RetainVolume(ctx, args)
	if err != nil {
		span.Errorf("retain volume from clusterMgr failed: %v", err)
		return
	}
	span.Debugf("retain result: %#v, lens: %v\n", retainVolume, len(retainVolume.RetainVolTokens))
	v.handleRetainResult(ctx, tokens, retainVolume.RetainVolTokens, isBackup)
}
