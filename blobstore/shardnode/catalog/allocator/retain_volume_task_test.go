// Copyright 2024 The CubeFS Authors.
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
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var C = gomock.NewController

func TestRetainTaskClose(t *testing.T) {
	closed := make(chan struct{})
	v := &volmgr{
		closeCh: closed,
		VolConfig: VolConfig{
			RetainIntervalS: 60,
		},
	}
	go func(v *volmgr) {
		v.closeCh <- struct{}{}
	}(v)
	v.retainTask()
}

func TestRetain(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	modeInfoMap := MockModeInfoMap()
	tp := NewMockAllocTransport(t)

	v := volmgr{
		tp:        tp,
		modeInfos: modeInfoMap,
		BlobConfig: BlobConfig{
			BidAllocNums: 100000,
		},
		VolConfig: VolConfig{
			RetainIntervalS:      60,
			VolumeReserveSize:    2 << 20,
			DefaultAllocVolsNum:  10,
			RetainBatchIntervalS: 1,
			RetainVolumeBatchNum: 400,
		},
	}

	v.retainAll(ctx)
}

func MockModeInfoMap() (modeMap map[codemode.CodeMode]*modeInfo) {
	now := time.Now().UnixNano()
	mockHost := "127.0.0.1:7788"
	modeInfoMap := make(map[codemode.CodeMode]*modeInfo)
	modeInfo1 := &modeInfo{current: &volumes{}, backup: &volumes{}}
	modeInfo2 := &modeInfo{current: &volumes{}, backup: &volumes{}}
	for i := 1; i <= 10; i++ {
		vid := proto.Vid(i)
		token := proto.EncodeToken(mockHost, vid)
		volInfo := clustermgr.AllocVolumeInfo{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:  proto.Vid(i),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			Token:      token,
			ExpireTime: 50*int64(math.Pow(10, 9)) + now,
		}
		modeInfo1.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)
	}
	modeInfoMap[codemode.CodeMode(0)] = modeInfo1
	for i := 11; i <= 20; i++ {
		vid := proto.Vid(i)
		token := proto.EncodeToken(mockHost, vid)
		volInfo := clustermgr.AllocVolumeInfo{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:  proto.Vid(i),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			Token:      token,
			ExpireTime: 50*int64(math.Pow(10, 9)) + now,
		}
		modeInfo2.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)
	}
	// test full volume
	fullVid := proto.Vid(31)
	token := proto.EncodeToken(mockHost, fullVid)
	fullVolInfo := clustermgr.AllocVolumeInfo{
		VolumeInfo: clustermgr.VolumeInfo{
			VolumeInfoBase: clustermgr.VolumeInfoBase{
				Vid:  proto.Vid(31),
				Free: 1 * 1024 * 1024,
			},
		},
		Token:      token,
		ExpireTime: 50*int64(math.Pow(10, 9)) + now,
	}

	modeInfo2.Put(&volume{
		AllocVolumeInfo: fullVolInfo,
	}, false)

	modeInfoMap[codemode.CodeMode(1)] = modeInfo2

	return modeInfoMap
}

func TestRetainVolumes(t *testing.T) {
	ctx := context.Background()
	vm := volmgr{}
	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)

	modeInfo := &modeInfo{
		current:        &volumes{totalFree: 2 * 16 * 1024 * 1024 * 1024},
		totalThreshold: 16 * 1024 * 1024 * 1024,
	}

	now := time.Now().UnixNano()
	for i := 1; i <= 2; i++ {
		volInfo := clustermgr.AllocVolumeInfo{
			VolumeInfo: clustermgr.VolumeInfo{
				VolumeInfoBase: clustermgr.VolumeInfoBase{
					Vid:  proto.Vid(i),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			Token:      "token",
			ExpireTime: now - 50*int64(math.Pow(10, 9)),
		}
		modeInfo.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)
	}

	vm.modeInfos[codemode.CodeMode(2)] = modeInfo
	tokens := vm.genRetainVolume(ctx, false)
	expectedTokens := []string{"token", "token"}
	require.Equal(t, expectedTokens, tokens)
}
