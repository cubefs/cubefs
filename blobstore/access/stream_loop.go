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

func (h *Handler) loopReleaseVids() {
	go func() {
		span, ctx := trace.StartSpanFromContext(context.Background(), "")

		allCm := h.clusterController.All()
		for _, cm := range allCm {
			cid := cm.ClusterID
			allocMgr, err := h.clusterController.GetVolumeAllocator(cid)
			if err != nil {
				span.Warnf("fail to get alloc mgr, when releaseVolume. err[%+v]", err)
				return
			}

			go func(cid proto.ClusterID, allocMgr controller.VolumeMgr) {
				tk := time.NewTicker(time.Second * 60)
				defer tk.Stop()

				for {
					select {
					case <-h.stopCh:
						return
					case <-tk.C:
						var normalVol, sealedVol *releaseVids
						if v, ok := h.releaseNormal.Load(cid); ok {
							normalVol = v.(*releaseVids)
						}
						if v, ok := h.releaseSealed.Load(cid); ok {
							sealedVol = v.(*releaseVids)
						}
						if normalVol.len() == 0 && sealedVol.len() == 0 {
							continue
						}

						normal := normalVol.getVids()
						sealed := sealedVol.getVids()
						err = allocMgr.Release(ctx, &clustermgr.ReleaseVolumes{
							CodeMode:   normalVol.md,
							NormalVids: normal,
							SealedVids: sealed,
						})
						if err == nil {
							h.releaseNormal.Delete(cid)
							h.releaseSealed.Delete(cid)
						}
						span.Warnf("We released normal volume:%v, sealed volume:%v, err[%+v]", normal, sealed, err)
					}
				}
			}(cid, allocMgr)
		}
	}()
}

type releaseVids struct {
	md   codemode.CodeMode
	vids []proto.Vid
	lck  sync.RWMutex
}

func (v *releaseVids) getVids() []proto.Vid {
	v.lck.RLock()
	defer v.lck.RUnlock()

	return v.vids
}

func (v *releaseVids) len() int {
	v.lck.RLock()
	defer v.lck.RUnlock()

	return len(v.vids)
}

func (v *releaseVids) addVid(md codemode.CodeMode, vid ...proto.Vid) {
	v.lck.Lock()
	defer v.lck.Unlock()

	v.md = md
	v.vids = append(v.vids, vid...)
}
