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

	"github.com/peterbourgon/diskv/v3"
	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
)

// Cacher is 2-level cache of clustermgr data.
// Data flow structure
//      - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  L1  | request |   --->   | LRU cache |   -- hit -->  | response |
//                                 |                          ^
//      |                     miss | expired  <---------------+
//                                 |                          |
//  L2  |                    |  disk kv  |   ----- hit ------->
//                                 |                          ^
//      |                     miss | expired  <---------------+
//                                 |                          |
//  Data|                    | clustermgr |  ----- hit ------->
//
//      - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

const (
	_defaultCapacity    = 1 << 20
	_defaultExpirationS = 0 // 0 means no expiration

	_defaultClustermgrConcurrency = 32
)

// ConfigCache is setting of cache.
type ConfigCache struct {
	DiskvBasePath string `json:"diskv_base_path"`
	DiskvTempDir  string `json:"diskv_temp_dir"`

	VolumeCapacity    int `json:"volume_capacity"`
	VolumeExpirationS int `json:"volume_expiration_seconds"`
	DiskCapacity      int `json:"disk_capacity"`
	DiskExpirationS   int `json:"disk_expiration_seconds"`
}

func diskvKeyVolume(vid proto.Vid) string {
	return "volume-" + vid.ToString()
}

func diskvKeyDisk(diskID proto.DiskID) string {
	return "disk-" + diskID.ToString()
}

// Cacher memory cache handlers.
type Cacher interface {
	GetVolume(ctx context.Context, args *proxy.CacheVolumeArgs) (*proxy.VersionVolume, error)
	GetDisk(ctx context.Context, args *proxy.CacheDiskArgs) (*blobnode.DiskInfo, error)
}

// New returns a Cacher.
func New(clusterID proto.ClusterID, config ConfigCache, cmClient clustermgr.APIProxy) (Cacher, error) {
	defaulter.LessOrEqual(&config.VolumeCapacity, _defaultCapacity)
	defaulter.LessOrEqual(&config.VolumeExpirationS, _defaultExpirationS)
	defaulter.LessOrEqual(&config.DiskCapacity, _defaultCapacity)
	defaulter.LessOrEqual(&config.DiskExpirationS, _defaultExpirationS)

	vc, err := memcache.NewMemCache(context.Background(), config.VolumeCapacity)
	if err != nil {
		return nil, err
	}
	dc, err := memcache.NewMemCache(context.Background(), config.DiskCapacity)
	if err != nil {
		return nil, err
	}
	dv := diskv.New(diskv.Options{
		CacheSizeMax: 1 << 20,
		BasePath:     config.DiskvBasePath,
		TempDir:      config.DiskvTempDir,
		Transform:    proxy.DiskvPathTransform,
	})

	concurrency := keycount.NewBlockingKeyCountLimit(_defaultClustermgrConcurrency)
	return &cacher{
		config:        config,
		clusterID:     clusterID,
		cmClient:      cmClient,
		cmConcurrency: concurrency,
		singleRun:     new(singleflight.Group),
		volumeCache:   vc,
		diskCache:     dc,
		diskv:         dv,
	}, nil
}

type cacher struct {
	config        ConfigCache
	clusterID     proto.ClusterID
	cmClient      clustermgr.APIProxy
	cmConcurrency limit.Limiter

	singleRun   *singleflight.Group
	volumeCache *memcache.MemCache
	diskCache   *memcache.MemCache

	diskv *diskv.Diskv
}
