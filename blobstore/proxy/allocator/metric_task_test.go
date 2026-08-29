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
	"context"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/require"
)

func TestMetricReport(t *testing.T) {
	ctx := context.Background()
	vm := volumeMgr{}
	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)
	modeInfo := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 15 * 16 * 1024 * 1024 * 1024,
	}
	for i := 1; i <= 30; i++ {
		volInfo := clustermgr.AllocVolumeInfo{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:  proto.Vid(i),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		}
		modeInfo.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)
	}
	vm.modeInfos[codemode.CodeMode(2)] = modeInfo
	vm.metricReport(ctx)
}

// Test the reporting of bid cache metrics in the metricReport function
func TestMetricReportWithBidCache(t *testing.T) {
	ctx := context.Background()

	vm := &volumeMgr{
		modeInfos: make(map[codemode.CodeMode]*modeInfo),
	}

	vm.modeInfos[codemode.CodeMode(2)] = &modeInfo{
		current: &volumes{},
		backup:  &volumes{},
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("metricReport panicked: %v", r)
			}
		}()
		vm.metricReport(ctx)
	}()

	current, backup := vm.getBidMgrCache()
	require.Equal(t, uint64(0), current)
	require.Equal(t, uint64(0), backup)
}
