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

package allocator

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func BenchmarkVolumes_Put(b *testing.B) {
	v := volumes{}
	rand.Seed(time.Now().Unix())
	for i := 0; i < b.N; i++ {
		x := rand.Int()
		volInfo := cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Free: 10 * 16 * 1024 * 1024 * 1024,
					Vid:  proto.Vid(x),
				},
			},
			ExpireTime: 100,
		}
		v.Put(&volume{AllocVolumeInfo: volInfo, deleted: false})
	}
}

func BenchmarkVolumes_Delete(b *testing.B) {
	v := volumes{}
	rand.Seed(time.Now().Unix())
	for i := 0; i < 3000; i++ {
		x := rand.Int()
		volInfo := cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Free: 10 * 16 * 1024 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		}
		volInfo.Vid = proto.Vid(x)
		v.Put(&volume{AllocVolumeInfo: volInfo, deleted: false})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Delete(proto.Vid(i))
	}
}

func TestVolumes_Put(t *testing.T) {
	v := volumes{}
	for _, i := range []int{1, 3, 2, 4} {
		vol := &volume{AllocVolumeInfo: cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Free: 10 * 16 * 1024 * 1024 * 1024,
					Vid:  proto.Vid(i),
				},
			},
			ExpireTime: 100,
		}, deleted: false}
		v.Put(vol)
	}

	idx, ok := search(v.vols, 3)
	require.Equal(t, 2, idx)
	require.True(t, ok)

	idx, ok = search(v.vols, proto.Vid(5))
	require.Equal(t, 4, idx)
	require.False(t, ok)
}

func TestVolumes_Delete(t *testing.T) {
	v := volumes{}
	for _, i := range []int{1, 3, 2, 4} {
		vol := &volume{AllocVolumeInfo: cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Free: 10 * 16 * 1024 * 1024 * 1024,
					Vid:  proto.Vid(i),
				},
			},
			ExpireTime: 100,
		}, deleted: false}
		v.Put(vol)
	}
	ok := v.Delete(proto.Vid(2))
	require.True(t, ok)

	ok = v.Delete(proto.Vid(6))
	require.False(t, ok)

	idx, ok := search(v.vols, proto.Vid(2))
	require.Equal(t, 1, idx)
	require.False(t, ok)

	idx, ok = search(v.vols, proto.Vid(3))
	require.Equal(t, 1, idx)
	require.True(t, ok)
}
