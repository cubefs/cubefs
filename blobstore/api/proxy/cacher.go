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

package proxy

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
)

// Cacher interface of proxy cache.
type Cacher interface {
	GetCacheVolume(ctx context.Context, host string, args *clustermgr.CacheVolumeArgs) (*clustermgr.VersionVolume, error)
	GetCacheDisk(ctx context.Context, host string, args *clustermgr.CacheDiskArgs) (*blobnode.DiskInfo, error)
	// Erase cache in proxy memory and diskv.
	// Volume key is "volume-{vid}", and disk key is "disk-{disk_id}".
	// Notice: Erase all if key is "ALL"!
	Erase(ctx context.Context, host string, key string) error
}

// DiskvPathTransform transform key to multi-level path.
// eg: key(with '{namespace}-{id}') --> ~/hash(key)[0:2]/hash(key)[2:4]/key
func DiskvPathTransform(key string) []string {
	paths := strings.SplitN(key, "-", 2)
	if len(paths) < 2 {
		return []string{}
	}

	sha := sha1.New()
	sha.Write([]byte(key))
	h := hex.EncodeToString(sha.Sum(nil))
	return []string{h[0:2], h[2:4]}
}
