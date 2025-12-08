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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const keyVolumeConcurrency = "volume"

type expiryVolume struct {
	clustermgr.VolumeInfo
	CreateAt time.Time `json:"__create_at__,omitempty"`

	expiration time.Duration `json:"-"`
}

func (v *expiryVolume) SetExpiration(exp time.Duration) {
	v.expiration = exp
}

func (v *expiryVolume) Expired() bool {
	return v.expiration > 0 && time.Since(v.CreateAt) > v.expiration
}

func encodeVolume(v *expiryVolume) ([]byte, error) {
	return json.Marshal(v)
}

func decodeVolume(data []byte) (valueExpired, error) {
	volume := new(expiryVolume)
	err := json.Unmarshal(data, &volume)
	return volume, err
}

func (c *cacher) GetVolume(ctx context.Context, args *proxy.CacheVolumeArgs) (*clustermgr.VolumeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("try to get volume %+v", args)

	vid := args.Vid
	if vol := c.getVolume(span, vid); vol != nil {
		if !args.Flush { // read cache
			return &vol.VolumeInfo, nil
		}

		if args.Version > 0 && args.Version != uint64(vol.RouteVersion) {
			span.Infof("request to flush, but version mismatch request(%d) != cache(%d)",
				args.Version, vol.RouteVersion)
			return &vol.VolumeInfo, nil
		}
	}

	st := time.Now()
	err := c.cmConcurrency.Acquire(keyVolumeConcurrency)
	span.AppendTrackLog("wait", st, err)
	if err != nil {
		return nil, err
	}
	defer c.cmConcurrency.Release(keyVolumeConcurrency)

	st = time.Now()
	val, err, _ := c.singleRun.Do(diskvKeyVolume(vid), func() (any, error) {
		return c.cmClient.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	})
	span.AppendTrackLog("cm", st, err)
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

	var result *clustermgr.VolumeInfo
	if err := c.withVolumeLock(vid, func() error {
		vol := c.newExpiryVolume(*volume)
		c.storeVolume(ctx, vid, vol)
		result = &vol.VolumeInfo
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *cacher) getVolume(span trace.Span, vid proto.Vid) *expiryVolume {
	if val := c.getCachedValue(span, vid, diskvKeyVolume(vid), c.config.VolumeExpirationS,
		c.volumeCache, decodeVolume, c.volumeReport); val != nil {
		if value, ok := val.(*expiryVolume); ok {
			return value
		}
	}
	return nil
}

func (c *cacher) getVolRouteVersion() proto.RouteVersion {
	return proto.RouteVersion(atomic.LoadUint64(&c.volRouteVersion))
}

func (c *cacher) setVolRouteVersion(version proto.RouteVersion) {
	atomic.StoreUint64(&c.volRouteVersion, uint64(version))
}

func (c *cacher) loadVolRouteVersion() proto.RouteVersion {
	dataStr := c.diskv.ReadString(diskvKeyVolumeRouteVersion)
	if dataStr == "" {
		return proto.InvalidRouteVersion
	}
	var version uint64
	if err := util.String2Any(dataStr, &version); err != nil {
		return proto.InvalidRouteVersion
	}
	return proto.RouteVersion(version)
}

func (c *cacher) persistVolRouteVersion(ctx context.Context, version proto.RouteVersion) error {
	span := trace.SpanFromContextSafe(ctx)
	dataStr := util.Any2String(uint64(version))
	if err := c.diskv.WriteString(diskvKeyVolumeRouteVersion, dataStr); err != nil {
		span.Warnf("persist route version %d failed: %v", version, err)
		return err
	}
	span.Debugf("persist route version %d success", version)
	return nil
}

func (c *cacher) volumeRouteLoop() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "volume_route_sync")
	defer span.Finish()

	ticker := time.NewTicker(time.Duration(c.config.VolumeRouteSyncIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.syncVolumeRoutes(ctx); err != nil {
				span.Warnf("sync volume routes failed: %v", err)
			}
		case <-c.Done():
			return
		}
	}
}

func (c *cacher) syncVolumeRoutes(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	version := c.getVolRouteVersion()
	ret, err := c.cmClient.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{RouteVersion: version})
	if err != nil {
		return err
	}

	if ret == nil || len(ret.Items) == 0 {
		span.Infof("no volume routes to sync, version: %d", version)
		return nil
	}

	for _, item := range ret.Items {
		if err := c.applyVolumeRouteItem(ctx, item); err != nil {
			return err
		}
		span.Debugf("applied volume route item: %+v", item)
	}

	if err := c.persistVolRouteVersion(ctx, ret.RouteVersion); err != nil {
		return err
	}
	c.setVolRouteVersion(ret.RouteVersion)
	span.Infof("volume route version updated from %d to %d", version, ret.RouteVersion)
	return nil
}

func (c *cacher) applyVolumeRouteItem(ctx context.Context, item clustermgr.VolumeRouteItem) error {
	if item.Item == nil {
		return errors.New("volume route item missing payload")
	}
	switch item.Type {
	case proto.RouteItemTypeAddVolume:
		payload := clustermgr.RouteItemAddVolume{}
		if err := payload.Unmarshal(item.Item.Value); err != nil {
			return err
		}
		return c.applyVolumeAdd(ctx, &payload)
	case proto.RouteItemTypeUpdateVolume:
		payload := clustermgr.RouteItemUpdateVolume{}
		if err := payload.Unmarshal(item.Item.Value); err != nil {
			return err
		}
		return c.applyVolumeUpdate(ctx, &payload)
	default:
		return fmt.Errorf("unknown volume route item type: %d", item.Type)
	}
}

func (c *cacher) applyVolumeAdd(ctx context.Context, payload *clustermgr.RouteItemAddVolume) error {
	info := clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase(payload.VolumeInfoBase),
		Units:          convertVolumeUnits(payload.Units),
	}
	return c.withVolumeLock(payload.Vid, func() error {
		vol := c.newExpiryVolume(info)
		c.storeVolume(ctx, payload.Vid, vol)
		return nil
	})
}

func (c *cacher) applyVolumeUpdate(ctx context.Context, payload *clustermgr.RouteItemUpdateVolume) error {
	span := trace.SpanFromContextSafe(ctx)
	var existing *expiryVolume
	if existing = c.getVolume(span, payload.Vid); existing == nil {
		// if volume not cached, flush it from clustermgr
		_, err := c.GetVolume(ctx, &proxy.CacheVolumeArgs{Vid: payload.Vid, Flush: true})
		return err
	}

	return c.withVolumeLock(payload.Vid, func() error {
		newVersion := payload.VolumeInfoBase.RouteVersion
		if !(newVersion > existing.VolumeInfo.RouteVersion) {
			span.Infof("skip update vid:%d since route version %d <= %d",
				payload.Vid, newVersion, existing.VolumeInfo.RouteVersion)
			return nil
		}

		info := clustermgr.VolumeInfo{
			VolumeInfoBase: clustermgr.VolumeInfoBase(payload.VolumeInfoBase),
			Units:          replaceVolumeUnit(ctx, existing.VolumeInfo.Units, payload.Unit),
		}
		vol := c.newExpiryVolume(info)
		c.storeVolume(ctx, payload.Vid, vol)
		return nil
	})
}

func (c *cacher) newExpiryVolume(info clustermgr.VolumeInfo) *expiryVolume {
	vol := &expiryVolume{
		VolumeInfo: info,
		CreateAt:   time.Now(),
	}
	vol.expiration = interleaveExpiration(c.config.VolumeExpirationS)
	return vol
}

func (c *cacher) storeVolume(ctx context.Context, vid proto.Vid, vol *expiryVolume) {
	c.volumeCache.Set(vid, vol)
	go c.writeVolumeToDisk(ctx, vid, vol)
}

func (c *cacher) writeVolumeToDisk(ctx context.Context, vid proto.Vid, vol *expiryVolume) {
	span := trace.SpanFromContextSafe(ctx)
	_span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "diskkv", span.TraceID())
	key := diskvKeyVolume(vid)
	fullPath := c.DiskvFilename(key)
	if data, err := encodeVolume(vol); err == nil {
		if err := c.diskv.Write(key, data); err != nil {
			_span.Warnf("write diskv on path:%s data:<%s> error:%s", fullPath, string(data), err.Error())
		} else {
			_span.Infof("write diskv on path:%s volume:<%s>", fullPath, string(data))
		}
	} else {
		_span.Warnf("encode vid:%d volume:%+v error:%s", vid, vol, err.Error())
	}

	select {
	case c.syncChan <- struct{}{}:
	default:
	}
}

func convertVolumeUnits(units []clustermgr.VolumeUnitInfoBase) []clustermgr.Unit {
	ret := make([]clustermgr.Unit, len(units))
	for i := range units {
		ret[i] = units[i].Convert2Unit()
	}
	return ret
}

func replaceVolumeUnit(ctx context.Context, units []clustermgr.Unit, unit clustermgr.VolumeUnitInfoBase) []clustermgr.Unit {
	span := trace.SpanFromContextSafe(ctx)
	if unit.Vuid == proto.InvalidVuid {
		span.Warnf("invalid vuid, skip replace volume unit")
		return units
	}
	replacement := unit.Convert2Unit()
	for i := range units {
		if units[i].Vuid.Index() == replacement.Vuid.Index() {
			units[i] = replacement
			return units
		}
	}
	span.Warnf("replace volume unit failed, skip replace volume unit")
	return units
}

func (c *cacher) withVolumeLock(vid proto.Vid, fn func() error) error {
	idx := uint64(vid) % uint64(len(c.volumeLocks))
	mu := &c.volumeLocks[int(idx)]
	return mu.WithLockError(fn)
}
