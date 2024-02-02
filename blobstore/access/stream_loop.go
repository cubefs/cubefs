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
	"time"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
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
				h.tryDiscardVidOnAllocator(dv.cid, &proxy.DiscardVolsArgs{
					CodeMode: dv.codeMode,
					Discards: []proto.Vid{dv.vid},
				})
				delete(cache, dv)
			}
		}
	}()
}

func (h *Handler) tryDiscardVidOnAllocator(cid proto.ClusterID, args *proxy.DiscardVolsArgs) {
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
