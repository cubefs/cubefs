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
	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type (
	vidHostKey struct {
		cid proto.ClusterID
		vid proto.Vid
	}
)

// cacheVidAllocator memory cache of vid in allocator host
var cacheVidAllocator *memcache.MemCache

func init() {
	mc, err := memcache.NewMemCache(1 << 15)
	if err != nil {
		panic(err)
	}
	cacheVidAllocator = mc
}

func setCacheVidHost(cid proto.ClusterID, vid proto.Vid, host string) {
	cacheVidAllocator.Set(vidHostKey{cid: cid, vid: vid}, host)
}

func getCacheVidHost(cid proto.ClusterID, vid proto.Vid) (string, error) {
	val := cacheVidAllocator.Get(vidHostKey{cid: cid, vid: vid})
	if val == nil {
		return "", errors.Newf("not found host of (%d %d)", cid, vid)
	}
	host, ok := val.(string)
	if !ok {
		return "", errors.Newf("not string host of (%d %d)", cid, vid)
	}
	return host, nil
}

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
				go func(dv discardVid) {
					h.tryDiscardVidOnAllocator(dv.cid, dv.vid, proxy.AllocVolsArgs{
						Fsize:    1,
						BidCount: 1,
						CodeMode: dv.codeMode,
						Discards: []proto.Vid{dv.vid},
					})
				}(dv)
				delete(cache, dv)
			}
		}
	}()
}

func (h *Handler) tryDiscardVidOnAllocator(cid proto.ClusterID, vid proto.Vid, args proxy.AllocVolsArgs) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	host, err := getCacheVidHost(cid, vid)
	if err != nil {
		span.Warnf("to discard vids %+v : %v", args, err)
		return
	}

	span.Infof("to post on %s discard vids %+v", host, args)

	_, err = h.proxyClient.VolumeAlloc(ctx, host, &args)
	if err != nil {
		span.Warnf("post on %s discard vids %+v : %v", host, args, err)
	}
}
