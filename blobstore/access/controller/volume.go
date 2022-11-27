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
	"math/rand"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/redis"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	_defaultCacheSize       = 1 << 17
	_defaultCacheExpiration = int64(2 * time.Minute)
)

// Unit alias of clustermgr.Unit
type Unit = clustermgr.Unit

// VolumePhy volume physical info
//     Vid, CodeMode and Units are from cluster mgr
//     IsPunish is cached in memory
//     Timestamp is cached in redis to clear outdate info
type VolumePhy struct {
	Vid       proto.Vid         `json:"vid"`
	CodeMode  codemode.CodeMode `json:"codemode"`
	IsPunish  bool              `json:"-"`
	Timestamp int64             `json:"timestamp"`
	Units     []Unit            `json:"units"`
}

// VolumeGetter getter of volume physical location
//
// ctx: context with trace or something
//
// isCache: is false means reading from cluster then updating redis and memcache
//     otherwise reading from memcache -> redis -> cluster
type VolumeGetter interface {
	// Get returns volume physical location of vid
	Get(ctx context.Context, vid proto.Vid, isCache bool) *VolumePhy
	// Punish punish vid with interval seconds
	Punish(ctx context.Context, vid proto.Vid, punishIntervalS int)
}
type cvid uint64

var (
	fmtKeyGroupVolume = func(id cvid) string { return fmt.Sprintf("get-volume-%d", id) }
	fmtKeyRedisVolume = func(cid proto.ClusterID, vid proto.Vid) string {
		return fmt.Sprintf("access/volume/%d/%d", cid, vid)
	}
)

func addCVid(cid proto.ClusterID, vid proto.Vid) cvid {
	return cvid((uint64(cid) << 32) | uint64(vid))
}

// volumePhyCacher local k-v cache for (Vid, VolumePhy)
type volumePhyCacher interface {
	Get(key cvid) *VolumePhy
	Set(key cvid, value VolumePhy)
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

	phy, ok := value.(VolumePhy)
	if !ok {
		return nil
	}

	return &phy
}

// Set implements volumePhyCacher.Set
func (vc *volumeMemCache) Set(key cvid, value VolumePhy) {
	vc.cache.Set(key, value)
}

type volumeGetterImpl struct {
	ctx context.Context
	cid proto.ClusterID

	volumeMemCache volumePhyCacher
	memExpiration  int64
	punishCache    *memcache.MemCache

	redisClient *redis.ClusterClient
	cmClient    clustermgr.APIAccess

	singleRun *singleflight.Group
}

// NewVolumeGetter new a volume getter
//   memExpiration expiration of memcache, 0 means no expiration
func NewVolumeGetter(clusterID proto.ClusterID, cmCli clustermgr.APIAccess, redisCli *redis.ClusterClient,
	memExpiration time.Duration) (VolumeGetter, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	expiration := int64(memExpiration)
	if expiration < 0 {
		expiration = _defaultCacheExpiration
	}

	mc, err := memcache.NewMemCache(ctx, _defaultCacheSize)
	if err != nil {
		return nil, err
	}
	punishCache, err := memcache.NewMemCache(ctx, 1024)
	if err != nil {
		return nil, err
	}

	getter := &volumeGetterImpl{
		ctx:            ctx,
		cid:            clusterID,
		volumeMemCache: &volumeMemCache{cache: mc},
		memExpiration:  expiration,
		punishCache:    punishCache,
		redisClient:    redisCli,
		cmClient:       cmCli,
		singleRun:      new(singleflight.Group),
	}

	return getter, nil
}

// Get implements interface VolumeGetter
//     1.top level cache from local
//     2.second level cache from redis cluster
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
			phy.IsPunish = true
		} else {
			v.punishCache.Set(id, nil)
		}
	}()

	// if isCache, reading from memcache -> redis -> cluster
	if isCache {
		if phy = v.getFromLocalCache(ctx, id); phy != nil {
			if v.memExpiration == 0 {
				if phy.Timestamp < 0 {
					phy = nil
				}
				cacheMetric.WithLabelValues(cid, "memcache", "hit").Inc()
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
				cacheMetric.WithLabelValues(cid, "memcache", "hit").Inc()
				phy = nil
				return
			}

			cacheMetric.WithLabelValues(cid, "memcache", "expired").Inc()
		}
		cacheMetric.WithLabelValues(cid, "memcache", "miss").Inc()

		if phy = v.getFromRedis(ctx, fmtKeyRedisVolume(v.cid, vid)); phy != nil {
			span.Debug("got from redis volume", cid, vid)
			cacheMetric.WithLabelValues(cid, "redis", "hit").Inc()

			phy.Timestamp = time.Now().UnixNano()
			v.setToLocalCache(ctx, id, phy)
			return
		}
		cacheMetric.WithLabelValues(cid, "redis", "miss").Inc()
	}

	val, err, _ := v.singleRun.Do(fmtKeyGroupVolume(id), func() (interface{}, error) {
		return v.getFromClusterAndUpdate(ctx, vid)
	})
	if err != nil {
		cacheMetric.WithLabelValues(cid, "clustermgr", "miss").Inc()
		span.Error("get volume location from clustermgr failed", errors.Detail(err))
		return
	}
	cacheMetric.WithLabelValues(cid, "clustermgr", "hit").Inc()

	phy, ok := val.(*VolumePhy)
	if !ok {
		span.Errorf("%s has bad value in singleflight group %+v", fmtKeyGroupVolume(id), val)
		return
	}
	span.Debug("got from cluster volume", cid, vid)
	return
}

func (v *volumeGetterImpl) Punish(ctx context.Context, vid proto.Vid, punishIntervalS int) {
	v.punishCache.Set(addCVid(v.cid, vid),
		time.Now().Add(time.Duration(punishIntervalS)*time.Second).Unix())
}

func (v *volumeGetterImpl) setToLocalCache(ctx context.Context, id cvid, phy *VolumePhy) error {
	v.volumeMemCache.Set(id, *phy)
	return nil
}

func (v *volumeGetterImpl) getFromLocalCache(ctx context.Context, id cvid) *VolumePhy {
	return v.volumeMemCache.Get(id)
}

// expiration random 30 - 60 minutes
func (v *volumeGetterImpl) setToRedis(ctx context.Context, key string, phy *VolumePhy) error {
	if v.redisClient == nil {
		return nil
	}
	expiration := rand.Intn(30) + 30
	return v.redisClient.Set(ctx, key, phy, time.Minute*time.Duration(expiration))
}

func (v *volumeGetterImpl) getFromRedis(ctx context.Context, key string) *VolumePhy {
	if v.redisClient == nil {
		return nil
	}
	value := &VolumePhy{}
	if err := v.redisClient.Get(ctx, key, value); err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Info(key, err)
		return nil
	}
	return value
}

func (v *volumeGetterImpl) getFromClusterAndUpdate(ctx context.Context, vid proto.Vid) (*VolumePhy, error) {
	span := trace.SpanFromContextSafe(ctx)
	var (
		vInfo *clustermgr.VolumeInfo
		err   error
	)

	id := addCVid(v.cid, vid)
	if err = retry.ExponentialBackoff(3, 100).On(func() error {
		if vInfo, err = v.cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid}); err != nil {
			return err
		}
		return nil
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
		return nil, errors.Base(err, "get volume from clustermgr", v.cid, vid)
	}

	phy := &VolumePhy{
		Vid:       vInfo.Vid,
		CodeMode:  vInfo.CodeMode,
		Timestamp: time.Now().UnixNano(),
		Units:     make([]Unit, len(vInfo.Units)),
	}
	copy(phy.Units, vInfo.Units[:])

	span.Debugf("to update memcache and redis volume(%d-%d) %+v", v.cid, vid, phy)
	v.setToLocalCache(ctx, id, phy)
	if err := v.setToRedis(ctx, fmtKeyRedisVolume(v.cid, vid), phy); err != nil {
		span.Warn("set volume location into redis failed", err)
	}

	return phy, nil
}
