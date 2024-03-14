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

package access

import (
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

func (h *Handler) loopDiscardVids() {
	go func() {
		cache := make(map[discardVid]struct{})

		duration := time.Second * 3
		timer := time.NewTimer(duration)
		defer timer.Stop()

		for {
			flush := false
			select {
			case <-h.stopCh:
				return
			case dv := <-h.discardVidChan:
				cache[dv] = struct{}{}
				if len(cache) >= 8 {
					flush = true
				}
			case <-timer.C:
				if len(cache) > 0 {
					flush = true
				}
				timer.Reset(duration)
			}

			if !flush {
				continue
			}

			for dv := range cache {
				h.tryDiscardVidOnAllocator(dv.cid, &clustermgr.DiscardVolsArgs{
					CodeMode: dv.codeMode,
					Discards: []proto.Vid{dv.vid},
				})
				delete(cache, dv)
			}
		}
	}()
}

func (h *Handler) tryDiscardVidOnAllocator(cid proto.ClusterID, args *clustermgr.DiscardVolsArgs) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	span.Infof("discard vids %+v", args)
	mgr, err := h.clusterController.GetVolumeAllocator(cid)
	if err != nil {
		span.Warnf("get alloc manager for cluster[%d] failed, err: %s", cid, err)
		return
	}

	err = mgr.Discard(ctx, args)
	if err != nil {
		span.Warnf("discard vids %+v failed, err : %s", args, err)
	}
}

// This function is used to release volume at background. It is recommended that a Handler has only one cluster configured
// If consul is removed to ensure that only one cluster is configured, you can remove the clusterController.All() and pass in only one cid
func (h *Handler) loopReleaseVids() {
	go func() {
		ctx := context.Background()
		defaulter.LessOrEqual(&h.ClusterConfig.VolumeReleaseSecs, 60)
		allocGroup := h.initReleaseVids(ctx)

		tk := time.NewTicker(time.Second * time.Duration(h.ClusterConfig.VolumeReleaseSecs))
		defer tk.Stop()
		for {
			select {
			case <-h.stopCh:
				return

			case <-tk.C:
				h.releaseClusterVids(ctx, allocGroup)
			}
		}
	}()
}

func (h *Handler) initReleaseVids(ctx context.Context) map[proto.ClusterID]controller.VolumeMgr {
	span := trace.SpanFromContextSafe(ctx)
	allocGroup := make(map[proto.ClusterID]controller.VolumeMgr)

	for _, cm := range h.clusterController.All() {
		cid := cm.ClusterID
		allocMgr, err := h.clusterController.GetVolumeAllocator(cid)
		if err != nil {
			span.Warnf("fail to get alloc mgr, when releaseVolume. err[%+v]", err)
			continue
		}
		allocGroup[cid] = allocMgr
		h.releaseVids.LoadOrStore(cid, &releaseVids{
			normalVids: newVolumeMap(),
			sealedVids: newVolumeMap(),
		})
	}

	return allocGroup
}

func (h *Handler) releaseClusterVids(ctx context.Context, allocGroup map[proto.ClusterID]controller.VolumeMgr) {
	span := trace.SpanFromContextSafe(ctx)

	for cid, allocMgr := range allocGroup {
		v, ok := h.releaseVids.Load(cid)
		if !ok {
			continue
		}

		vol := v.(*releaseVids)
		if vol.normalVids.len() == 0 && vol.sealedVids.len() == 0 {
			continue
		}

		normal, sealed := vol.getAll()
		err := allocMgr.Release(ctx, &clustermgr.ReleaseVolumes{
			CodeMode:   vol.md,
			NormalVids: normal,
			SealedVids: sealed,
		})
		if err == nil {
			// may be mark sealed volume in the PUT process, after call Release and before deleteAll
			vol.deleteAll(normal, sealed)
		}
		span.Warnf("We released normal volume:%v, sealed volume:%v, err[%+v]", normal, sealed, err)
	}
}

type releaseVids struct {
	md         codemode.CodeMode
	normalVids *rVidGroup
	sealedVids *rVidGroup
}

func (v *releaseVids) getAll() ([]proto.Vid, []proto.Vid) {
	return v.normalVids.getVids(), v.sealedVids.getVids()
}

func (v *releaseVids) deleteAll(normal, sealed []proto.Vid) {
	v.normalVids.delete(normal)
	v.sealedVids.delete(sealed)
}

type rVidGroup struct {
	vids map[proto.Vid]struct{}
	lck  sync.RWMutex
}

func newVolumeMap() *rVidGroup {
	return &rVidGroup{
		vids: make(map[proto.Vid]struct{}),
	}
}

func (v *rVidGroup) len() int {
	v.lck.RLock()
	defer v.lck.RUnlock()

	return len(v.vids)
}

func (v *rVidGroup) getVids() []proto.Vid {
	v.lck.RLock()
	defer v.lck.RUnlock()

	vids := make([]proto.Vid, 0, len(v.vids))
	for vid := range v.vids {
		vids = append(vids, vid)
	}
	return vids
}

func (v *rVidGroup) addVid(vid proto.Vid) {
	v.lck.Lock()
	defer v.lck.Unlock()

	v.vids[vid] = struct{}{}
}

func (v *rVidGroup) delete(vids []proto.Vid) {
	v.lck.Lock()
	defer v.lck.Unlock()

	for _, vid := range vids {
		delete(v.vids, vid)
	}
}
