// Copyright 2025 The CubeFS Authors.
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

package base

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	cmerrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	shardCount = 32

	defaultMarker = proto.Vid(0)
	defaultCount  = 1000
)

var errVolumeMissmatch = errors.New("volume missmatch during running task")

type (
	volumeTime struct {
		time   time.Time
		volume snproto.VolumeInfoSimple
	}
	shardCacher struct {
		sync.RWMutex
		m map[proto.Vid]*volumeTime
	}
	volumeCacher struct {
		interval time.Duration
		cache    [shardCount]*shardCacher
	}
)

func (vc *volumeCacher) getShard(vid proto.Vid) *shardCacher {
	return vc.cache[uint(vid)%shardCount]
}

func (vc *volumeCacher) Get(vid proto.Vid) (*snproto.VolumeInfoSimple, bool) {
	shard := vc.getShard(vid)
	shard.RLock()
	val, exist := shard.m[vid]
	shard.RUnlock()
	if exist {
		return &val.volume, true
	}
	return nil, false
}

func (vc *volumeCacher) Set(vid proto.Vid, volume snproto.VolumeInfoSimple) {
	shard := vc.getShard(vid)
	shard.Lock()
	shard.m[vid] = &volumeTime{
		time:   time.Now(),
		volume: volume,
	}
	shard.Unlock()
}

func (vc *volumeCacher) Settable(vid proto.Vid) bool {
	shard := vc.getShard(vid)
	shard.RLock()
	val, exist := shard.m[vid]
	shard.RUnlock()
	if !exist {
		return true
	}
	return time.Now().After(val.time.Add(vc.interval))
}

func newVolumeCacher(interval time.Duration) *volumeCacher {
	c := new(volumeCacher)
	c.interval = interval
	for idx := range c.cache {
		m := make(map[proto.Vid]*volumeTime, 32)
		c.cache[idx] = &shardCacher{m: m}
	}
	return c
}

type IVolumeCache interface {
	UpdateVolume(vid proto.Vid) (*snproto.VolumeInfoSimple, error)
	GetVolume(vid proto.Vid) (*snproto.VolumeInfoSimple, error)
	LoadVolumes() error
	DoubleCheckedRun(ctx context.Context, vid proto.Vid, task func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error)) error
}

// VolumeCache volume cache
type volumeCache struct {
	tp    VolumeTransport
	group singleflight.Group
	cache *volumeCacher
}

// NewVolumeCache returns volume cache manager.
func NewVolumeCache(tp VolumeTransport, updateInterval time.Duration) IVolumeCache {
	return &volumeCache{
		tp:    tp,
		cache: newVolumeCacher(updateInterval),
	}
}

// LoadVolumes Load list all volumes info memory cache.
func (c *volumeCache) LoadVolumes() error {
	marker := defaultMarker
	for {
		log.Infof("to load volume marker[%d], count[%d]", marker, defaultCount)

		var (
			volInfos   []*snproto.VolumeInfoSimple
			nextMarker proto.Vid
			err        error
		)
		if err = retry.Timed(3, 200).On(func() error {
			volInfos, nextMarker, err = c.tp.ListVolume(context.Background(), marker, defaultCount)
			return err
		}); err != nil {
			log.Errorf("list volume: marker[%d], count[%+v], code[%d], error[%v]",
				marker, defaultCount, rpc.DetectStatusCode(err), err)
			return err
		}

		for _, v := range volInfos {
			c.cache.Set(v.Vid, *v)
		}
		if len(volInfos) == 0 || nextMarker == defaultMarker {
			break
		}

		marker = nextMarker
	}
	return nil
}

// GetVolume returns this volume info.
func (c *volumeCache) GetVolume(vid proto.Vid) (*snproto.VolumeInfoSimple, error) {
	if vol, ok := c.cache.Get(vid); ok {
		return vol, nil
	}

	vol, err := c.UpdateVolume(vid)
	if err != nil {
		return nil, err
	}
	return vol, nil
}

// UpdateVolume this volume info cache.
func (c *volumeCache) UpdateVolume(vid proto.Vid) (*snproto.VolumeInfoSimple, error) {
	if !c.cache.Settable(vid) {
		return nil, cmerrors.ErrUpdateVolCacheFreq
	}

	val, err, _ := c.group.Do(fmt.Sprintf("volume-update-%d", vid), func() (interface{}, error) {
		vol, err := c.tp.GetVolumeInfo(context.Background(), vid)
		if err != nil {
			return nil, err
		}

		c.cache.Set(vol.Vid, *vol)
		return vol, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*snproto.VolumeInfoSimple), nil
}

// DoubleCheckedRun the scheduler updates volume mapping relation asynchronously,
// then some task(delete or repair) had started with old volume mapping.
//
// if delete on old relation, there will has garbage shard in new chunk. ==> garbage shard
// if repair on old relation, there still is missing shard in new chunk. ==> missing shard
func (c *volumeCache) DoubleCheckedRun(ctx context.Context, vid proto.Vid, task func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error)) error {
	span := trace.SpanFromContextSafe(ctx)
	volume, err := c.GetVolume(vid)
	if err != nil {
		return err
	}

	for range [3]struct{}{} {
		taskDoneVolume, err := task(volume)
		if err != nil {
			return err
		}

		newVolume, err := c.GetVolume(vid)
		if err != nil {
			return err
		}
		if taskDoneVolume.EqualWith(newVolume) {
			return nil
		}

		span.Warnf("volume changed from [%+v] to [%+v]", volume, newVolume)
		volume = newVolume
	}
	return errVolumeMissmatch
}
