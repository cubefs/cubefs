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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/peterbourgon/diskv/v3"
	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
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

	prefixKeyVolume = "volume-"
	prefixKeyDisk   = "disk-"
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

type valueExpired interface {
	Expired() bool
}

func diskvKeyVolume(vid proto.Vid) string {
	return prefixKeyVolume + vid.ToString()
}

func diskvKeyDisk(diskID proto.DiskID) string {
	return prefixKeyDisk + diskID.ToString()
}

func parseID(key string) (uint32, error) {
	var id string
	switch {
	case strings.HasPrefix(key, prefixKeyVolume):
		id = key[len(prefixKeyVolume):]
	case strings.HasPrefix(key, prefixKeyDisk):
		id = key[len(prefixKeyDisk):]
	}
	if len(id) == 0 {
		return 0, fmt.Errorf("invalid key %s", key)
	}

	i, err := strconv.Atoi(id)
	if err != nil {
		return 0, err
	}
	if i <= 0 || i >= math.MaxInt32 {
		return 0, fmt.Errorf("invalid key %s", key)
	}
	return uint32(i), nil
}

// Cacher memory cache handlers.
type Cacher interface {
	GetVolume(ctx context.Context, args *proxy.CacheVolumeArgs) (*proxy.VersionVolume, error)
	GetDisk(ctx context.Context, args *proxy.CacheDiskArgs) (*blobnode.DiskInfo, error)
	// Erase remove all if key is "ALL".
	Erase(ctx context.Context, key string) error
}

// New returns a Cacher.
func New(clusterID proto.ClusterID, config ConfigCache, cmClient clustermgr.APIProxy) (Cacher, error) {
	defaulter.LessOrEqual(&config.VolumeCapacity, _defaultCapacity)
	defaulter.LessOrEqual(&config.VolumeExpirationS, _defaultExpirationS)
	defaulter.LessOrEqual(&config.DiskCapacity, _defaultCapacity)
	defaulter.LessOrEqual(&config.DiskExpirationS, _defaultExpirationS)

	vc, err := memcache.NewMemCache(config.VolumeCapacity)
	if err != nil {
		return nil, err
	}
	dc, err := memcache.NewMemCache(config.DiskCapacity)
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
		syncChan:      make(chan struct{}, 1),
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
	syncChan    chan struct{} // just for testing
	volumeCache *memcache.MemCache
	diskCache   *memcache.MemCache

	diskv *diskv.Diskv
}

func (c *cacher) Erase(ctx context.Context, key string) error {
	span := trace.SpanFromContextSafe(ctx)
	if key == "ALL" {
		span.Warn("to erase all memory cache and diskv")
		c.volumeCache.Purge()
		c.diskCache.Purge()
		return c.diskv.EraseAll()
	}

	id, err := parseID(key)
	if err != nil {
		span.Infof("erase key:%s error:%s", key, err.Error())
		return errcode.ErrIllegalArguments
	}

	switch {
	case strings.HasPrefix(key, prefixKeyVolume):
		c.volumeCache.Remove(proto.Vid(id))
	case strings.HasPrefix(key, prefixKeyDisk):
		c.diskCache.Remove(proto.DiskID(id))
	}
	span.Warnf("to erase key:%s path:%s", key, c.DiskvFilename(key))
	return c.diskv.Erase(key)
}

func (c *cacher) DiskvFilename(key string) string {
	pathKey := c.diskv.AdvancedTransform(key)
	dir := filepath.Join(c.diskv.BasePath, filepath.Join(pathKey.Path...))
	return filepath.Join(dir, pathKey.FileName)
}

func (c *cacher) getCachedValue(span trace.Span, id interface{}, key string,
	memcacher *memcache.MemCache, decoder func([]byte) (valueExpired, error),
	reporter func(string, string)) interface{} {
	if val := memcacher.Get(id); val != nil {
		if value, ok := val.(valueExpired); ok {
			if !value.Expired() {
				reporter("memcache", "hit")
				span.Debugf("hits on memory cache key:%s id:%v", key, id)
				return value
			}
			reporter("memcache", "expired")
		}
	}
	reporter("memcache", "miss")

	fullPath := c.DiskvFilename(key)
	data, err := c.diskv.Read(key)
	if err != nil {
		reporter("diskv", "miss")
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			span.Debugf("read from diskv key:%s path:%s no such file", key, fullPath)
		} else {
			span.Warnf("read from diskv key:%s path:%s %s", key, fullPath, err.Error())
		}
		return nil
	}
	value, err := decoder(data)
	if err != nil {
		reporter("diskv", "error")
		span.Warnf("decode diskv path:%s data:<%s> %s", fullPath, string(data), err.Error())
		return nil
	}

	if !value.Expired() {
		reporter("diskv", "hit")
		memcacher.Set(id, value)
		span.Debugf("hits on diskv cache key:%s path:%s", key, fullPath)
		return value
	}

	reporter("diskv", "expired")
	span.Debugf("expired at diskv path:%s value:<%s>", fullPath, string(data))
	return nil
}
