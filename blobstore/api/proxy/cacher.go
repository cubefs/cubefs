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
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// VersionVolume volume with version.
type VersionVolume struct {
	clustermgr.VolumeInfo
	Version uint32 `json:"version,omitempty"`
}

// GetVersion calculate version with volume's units.
func (v *VersionVolume) GetVersion() uint32 {
	crcWriter := crc32.NewIEEE()
	for _, unit := range v.Units {
		binary.Write(crcWriter, binary.LittleEndian, uint64(unit.Vuid))
	}
	return crcWriter.Sum32()
}

// CacheVolumeArgs volume arguments.
type CacheVolumeArgs struct {
	Vid     proto.Vid `json:"vid"`
	Version uint32    `json:"version,omitempty"`
	Flush   bool      `json:"flush,omitempty"`
}

// CacheDiskArgs disk arguments.
type CacheDiskArgs struct {
	DiskID proto.DiskID `json:"disk_id"`
	Flush  bool         `json:"flush,omitempty"`
}

// Cacher interface of proxy cache.
type Cacher interface {
	GetCacheVolume(ctx context.Context, host string, args *CacheVolumeArgs) (*VersionVolume, error)
	GetCacheDisk(ctx context.Context, host string, args *CacheDiskArgs) (*blobnode.DiskInfo, error)
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
