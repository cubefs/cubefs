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

package controller

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	_defaultCacheSize       = 1 << 20
	_defaultCacheExpiration = int64(2 * time.Minute)
)

// Unit alias of clustermgr.Unit
type Unit = clustermgr.Unit

// VolumePhy volume physical info
//     Vid, CodeMode and Units are from cluster
//     IsPunish is cached in memory
//     Version is versioned in proxy
//     Timestamp is cached in proxy to clear outdate volume
type VolumePhy struct {
	Vid       proto.Vid
	CodeMode  codemode.CodeMode
	IsPunish  bool
	Version   uint32
	Timestamp int64
	Units     []Unit
}

// VolumeGetter getter of volume physical location
//
// ctx: context with trace or something
//
// isCache: is false means reading from proxy cluster then updating memcache
//     otherwise reading from memcache -> proxy -> cluster
type VolumeGetter interface {
	// Get returns volume physical location of vid
	Get(ctx context.Context, vid proto.Vid, isCache bool) *VolumePhy
	// Punish punish vid with interval seconds
	Punish(ctx context.Context, vid proto.Vid, punishIntervalS int)
}
type cvid uint64

func addCVid(cid proto.ClusterID, vid proto.Vid) cvid {
	return cvid((uint64(cid) << 32) | uint64(vid))
}

// volumePhyCacher local k-v cache for (Vid, *VolumePhy)
type volumePhyCacher interface {
	Get(key cvid) *VolumePhy
	Set(key cvid, value *VolumePhy)
}

type volumeMemCache struct {
	cache *memcache.MemCache
}

var _ volumePhyCacher = (*volumeMemCache)(nil)

// Get implements volumePhyCacher.Get
func (vc *volumeMemCache) Get(key cvid) *VolumePhy {
	value := vc.cache.Get(key)
	if value == nil {
		return nil
	}
	phy, ok := value.(*VolumePhy)
	if !ok {
		return nil
	}
	return phy
}

// Set implements volumePhyCacher.Set
func (vc *volumeMemCache) Set(key cvid, value *VolumePhy) {
	vc.cache.Set(key, value)
}

type volumeGetterImpl struct {
	ctx context.Context
	cid proto.ClusterID

	volumeMemCache volumePhyCacher
	memExpiration  int64
	punishCache    *memcache.MemCache

	service   ServiceController
	proxy     proxy.Cacher
	singleRun *singleflight.Group
}

// NewVolumeGetter new a volume getter
//   memExpiration expiration of memcache, 0 means no expiration
func NewVolumeGetter(clusterID proto.ClusterID, service ServiceController,
	proxy proxy.Cacher, memExpiration time.Duration) (VolumeGetter, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	expiration := int64(memExpiration)
	if expiration < 0 {
		expiration = _defaultCacheExpiration
	}

	mc, err := memcache.NewMemCache(_defaultCacheSize)
	if err != nil {
		return nil, err
	}
	punishCache, err := memcache.NewMemCache(1024)
	if err != nil {
		return nil, err
	}

	getter := &volumeGetterImpl{
		ctx:            ctx,
		cid:            clusterID,
		volumeMemCache: &volumeMemCache{cache: mc},
		memExpiration:  expiration,
		punishCache:    punishCache,
		service:        service,
		proxy:          proxy,
		singleRun:      new(singleflight.Group),
	}

	return getter, nil
}

// Get implements interface VolumeGetter
//     1.top level cache from local
//     2.second level cache from proxy cluster
func (v *volumeGetterImpl) Get(ctx context.Context, vid proto.Vid, isCache bool) (phy *VolumePhy) {
	span := trace.SpanFromContextSafe(ctx)
	cid := v.cid.ToString()
	id := addCVid(v.cid, vid)

	// check if volume punish
	defer func() {
		if phy == nil {
			return
		}

		val := v.punishCache.Get(id)
		if val == nil {
			return
		}

		if time.Since(time.Unix(val.(int64), 0)) < 0 {
			// make a copy of phy, do not modify the pointer in cache
			punishedPhy := *phy
			punishedPhy.IsPunish = true
			phy = &punishedPhy
		} else {
			v.punishCache.Remove(id)
		}
	}()

	phy = v.getFromLocalCache(ctx, id)
	if phy != nil && isCache {
		if v.memExpiration == 0 {
			if phy.Timestamp < 0 {
				phy = nil
			}
			cacheMetric.WithLabelValues(cid, "memcache", "none").Inc()
			return
		}

		now := time.Now().UnixNano()
		// exist volume and not expired
		if phy.Timestamp > 0 && now < phy.Timestamp+v.memExpiration {
			span.Debug("got from memcache volume", cid, vid)
			cacheMetric.WithLabelValues(cid, "memcache", "hit").Inc()
			return
		}

		// not exist volume and not expired, return nil
		if phy.Timestamp < 0 && now < -phy.Timestamp+v.memExpiration {
			span.Debug("got from memcache not exist volume", cid, vid)
			cacheMetric.WithLabelValues(cid, "memcache", "none").Inc()
			phy = nil
			return
		}

		cacheMetric.WithLabelValues(cid, "memcache", "expired").Inc()
	}
	cacheMetric.WithLabelValues(cid, "memcache", "miss").Inc()

	singleID := fmt.Sprintf("get-volume-%d", id)
	ver := uint32(0)
	if phy != nil {
		ver = phy.Version
	}
	val, err, _ := v.singleRun.Do(singleID, func() (interface{}, error) {
		return v.getFromProxy(ctx, vid, !isCache, ver)
	})
	if err != nil {
		cacheMetric.WithLabelValues(cid, "proxy", "miss").Inc()
		span.Error("get volume location from proxy failed", errors.Detail(err))
		return
	}
	cacheMetric.WithLabelValues(cid, "proxy", "hit").Inc()

	phy, ok := val.(*VolumePhy)
	if !ok {
		span.Errorf("%s has bad value in singleflight group %+v", singleID, val)
		return
	}
	span.Debug("got from cluster volume", cid, vid)
	return
}

func (v *volumeGetterImpl) Punish(ctx context.Context, vid proto.Vid, punishIntervalS int) {
	v.punishCache.Set(addCVid(v.cid, vid), time.Now().Add(time.Duration(punishIntervalS)*time.Second).Unix())
}

func (v *volumeGetterImpl) setToLocalCache(ctx context.Context, id cvid, phy *VolumePhy) {
	v.volumeMemCache.Set(id, phy)
}

func (v *volumeGetterImpl) getFromLocalCache(ctx context.Context, id cvid) *VolumePhy {
	return v.volumeMemCache.Get(id)
}

func (v *volumeGetterImpl) getFromProxy(ctx context.Context, vid proto.Vid, flush bool, ver uint32) (*VolumePhy, error) {
	span := trace.SpanFromContextSafe(ctx)
	hosts, err := v.service.GetServiceHosts(ctx, proto.ServiceNameProxy)
	if err != nil {
		return nil, err
	}

	var volume *proxy.VersionVolume
	id := addCVid(v.cid, vid)
	triedHosts := make(map[string]struct{})
	if err = retry.ExponentialBackoff(3, 30).RuptOn(func() (bool, error) {
		for _, host := range hosts {
			triedHosts[host] = struct{}{}
			if volume, err = v.proxy.GetCacheVolume(ctx, host,
				&proxy.CacheVolumeArgs{Vid: vid, Flush: flush, Version: ver}); err != nil {
				if rpc.DetectStatusCode(err) == errcode.CodeVolumeNotExist {
					return true, err
				}
				span.Warnf("get from proxy(%s) volume(%d) error(%s)", host, vid, err.Error())
				continue
			}
			return true, nil
		}
		return false, errors.New("error on all proxy")
	}); err != nil {
		// set local cache as nil(Timestamp is negative) when volume not exist
		if rpc.DetectStatusCode(err) == errcode.CodeVolumeNotExist {
			phy := &VolumePhy{
				Vid:       vid,
				Timestamp: -time.Now().UnixNano(),
			}
			span.Infof("to update memcache on not exist volume(%d-%d) %+v", v.cid, vid, phy)
			v.setToLocalCache(ctx, id, phy)
		}
		return nil, errors.Base(err, "get volume from proxy", v.cid, vid)
	}

	phy := &VolumePhy{
		Vid:       volume.Vid,
		CodeMode:  volume.CodeMode,
		Version:   volume.Version,
		Timestamp: time.Now().UnixNano(),
		Units:     make([]Unit, len(volume.Units)),
	}
	copy(phy.Units, volume.Units[:])

	span.Debugf("to update memcache on volume(%d-%d) %+v", v.cid, vid, phy)
	v.setToLocalCache(ctx, id, phy)

	if flush {
		v.flush(ctx, vid, ver, hosts, triedHosts)
	}

	return phy, nil
}

// flush update all proxy cache of this idc
func (v *volumeGetterImpl) flush(ctx context.Context, vid proto.Vid, ver uint32, hosts []string, except map[string]struct{}) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("to flush volume cache %d on proxy:%v version:%d except:%v", vid, hosts, ver, except)

	for _, host := range hosts {
		if _, ok := except[host]; ok {
			continue
		}

		go func(host string) {
			retry.ExponentialBackoff(2, 10).RuptOn(func() (bool, error) {
				if _, err := v.proxy.GetCacheVolume(ctx, host,
					&proxy.CacheVolumeArgs{Vid: vid, Flush: true, Version: ver}); err != nil {
					if rpc.DetectStatusCode(err) == errcode.CodeVolumeNotExist {
						span.Info("not found volume", vid)
						return true, err
					}
					span.Warnf("flush volume:%d error:%s", vid, err.Error())
					return false, err
				}
				return true, nil
			})
		}(host)
	}
}
