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

package cacher

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const keyVolumeConcurrency = "volume"

type expiryVolume struct {
	proxy.VersionVolume
	ExpiryAt int64 `json:"expiry,omitempty"` // seconds
}

func (v *expiryVolume) Expired() bool {
	return v.ExpiryAt > 0 && time.Now().Unix() >= v.ExpiryAt
}

func encodeVolume(v *expiryVolume) ([]byte, error) {
	return json.Marshal(v)
}

func decodeVolume(data []byte) (valueExpired, error) {
	volume := new(expiryVolume)
	err := json.Unmarshal(data, &volume)
	return volume, err
}

func (c *cacher) GetVolume(ctx context.Context, args *proxy.CacheVolumeArgs) (*proxy.VersionVolume, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("try to get volume %+v", args)

	vid := args.Vid
	if vol := c.getVolume(span, vid); vol != nil {
		if !args.Flush { // read cache
			return &vol.VersionVolume, nil
		}

		if args.Version > 0 && args.Version != vol.Version {
			span.Infof("request to flush, but version mismatch request(%d) != cache(%d)",
				args.Version, vol.Version)
			return &vol.VersionVolume, nil
		}
	}

	err := c.cmConcurrency.Acquire(keyVolumeConcurrency)
	if err != nil {
		return nil, err
	}
	defer c.cmConcurrency.Release(keyVolumeConcurrency)

	val, err, _ := c.singleRun.Do(diskvKeyVolume(vid), func() (interface{}, error) {
		return c.cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	})
	if err != nil {
		c.volumeReport("clustermgr", "miss")
		span.Error("get volume from clustermgr failed", errors.Detail(err))
		return nil, err
	}
	volume, ok := val.(*clustermgr.VolumeInfo)
	if !ok {
		return nil, errors.New("error convert to volume struct after singleflight")
	}
	c.volumeReport("clustermgr", "hit")

	vol := new(expiryVolume)
	vol.VersionVolume.VolumeInfo = *volume
	vol.VersionVolume.Version = vol.GetVersion()
	if expire := c.config.VolumeExpirationS; expire > 0 {
		// random expiration to reduce intensive clustermgr requests.
		expiration := rand.Intn(expire) + expire
		vol.ExpiryAt = time.Now().Add(time.Second * time.Duration(expiration)).Unix()
	}
	c.volumeCache.Set(vid, vol)

	go func() {
		key := diskvKeyVolume(vid)
		fullPath := c.DiskvFilename(key)
		if data, err := encodeVolume(vol); err == nil {
			if err := c.diskv.Write(diskvKeyVolume(vid), data); err != nil {
				span.Warnf("write diskv on path:%s data:<%s> error:%s", fullPath, string(data), err.Error())
			} else {
				span.Infof("write diskv on path:%s volume:<%s>", fullPath, string(data))
			}
		} else {
			span.Warnf("encode vid:%d volume:%+v error:%s", vid, vol, err.Error())
		}

		select {
		case c.syncChan <- struct{}{}:
		default:
		}
	}()

	return &vol.VersionVolume, nil
}

func (c *cacher) getVolume(span trace.Span, vid proto.Vid) *expiryVolume {
	if val := c.getCachedValue(span, vid, diskvKeyVolume(vid),
		c.volumeCache, decodeVolume, c.volumeReport); val != nil {
		if value, ok := val.(*expiryVolume); ok {
			return value
		}
	}
	return nil
}
