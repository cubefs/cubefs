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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/proxy/mock"
)

func TestNewVolumeMgr(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmcli := mock.ProxyMockClusterMgrCli(ctrl)
	_, err := NewVolumeMgr(ctx, BlobConfig{}, VolConfig{InitVolumeNum: 4}, cmcli)
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, err)
}

func TestGetAllocList(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cmcli := mock.ProxyMockClusterMgrCli(ctrl)
	expireTime := time.Now().UnixNano() + 300*int64(math.Pow(10, 9))
	volInfo1 := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  1,
				Free: 1 * 1024 * 1024,
			},
		},
		ExpireTime: expireTime,
	}
	volInfo2 := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  2,
				Free: 2 * 1024 * 1024,
			},
		},
		ExpireTime: expireTime,
	}
	volInfo3 := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  3,
				Free: 15 * 1024 * 1024,
			},
		},
		ExpireTime: expireTime,
	}
	volInfo4 := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  4,
				Free: 8 * 1024 * 1024,
			},
		},
		ExpireTime: expireTime,
	}
	volInfo5 := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  5,
				Free: 9 * 1024 * 1024,
			},
		},
		ExpireTime: expireTime,
	}

	modeInfoMap := make(map[codemode.CodeMode]*ModeInfo)

	volumeStateInfo1 := &ModeInfo{volumes: &volumes{}, totalThreshold: 5 * 1024 * 1024, totalFree: 3 * 1024 * 1024}
	volumeStateInfo1.volumes.Put(&volume{
		AllocVolumeInfo: volInfo1,
	})
	volumeStateInfo1.volumes.Put(&volume{
		AllocVolumeInfo: volInfo2,
	})
	modeInfoMap[codemode.CodeMode(1)] = volumeStateInfo1

	volumeStateInfo2 := &ModeInfo{volumes: &volumes{}, totalThreshold: 5 * 1024 * 1024, totalFree: 32 * 1024 * 1024}
	volumeStateInfo2.volumes.Put(&volume{
		AllocVolumeInfo: volInfo3,
	})
	volumeStateInfo2.volumes.Put(&volume{
		AllocVolumeInfo: volInfo4,
	})
	volumeStateInfo2.volumes.Put(&volume{
		AllocVolumeInfo: volInfo5,
	})
	modeInfoMap[codemode.CodeMode(2)] = volumeStateInfo2

	vm := volumeMgr{
		clusterMgr: cmcli,
		modeInfos:  modeInfoMap,
		BlobConfig: BlobConfig{
			BidAllocNums: 1000,
		},
	}

	ctx := context.Background()
	{
		vids, _, err := vm.List(ctx, codemode.CodeMode(2))
		require.NoError(t, err)
		require.Equal(t, []proto.Vid{3, 4, 5}, vids)
	}
	{
		writableVidsArgs := &proxy.AllocVolsArgs{
			Fsize:    12 * 1024 * 1024,
			BidCount: 6,
			CodeMode: 2,
			Excludes: []proto.Vid{4},
			Discards: []proto.Vid{5},
		}
		vid, err := vm.allocVid(ctx, writableVidsArgs)
		if err != nil {
			t.Log(err)
		}
		require.Equal(t, 3, int(vid))
		info3, _ := vm.modeInfos[codemode.CodeMode(2)].volumes.Get(proto.Vid(3))
		totalFree := vm.modeInfos[codemode.CodeMode(2)].totalFree
		require.Equal(t, 3*1024*1024, int(info3.Free))
		require.Equal(t, 11*1024*1024, int(totalFree))
	}
	{
		writableVidsArgs := &proxy.AllocVolsArgs{
			Fsize:    5 * 1024 * 1024,
			BidCount: 1,
			CodeMode: 2,
			Excludes: []proto.Vid{4},
		}
		vid, err := vm.allocVid(ctx, writableVidsArgs)
		require.Error(t, err)
		require.Equal(t, 0, int(vid))
	}
}

func BenchmarkVolumeMgr_Alloc(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	cmcli := mock.ProxyMockClusterMgrCli(ctrl)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*ModeInfo)
	modeInfo := &ModeInfo{
		volumes: &volumes{}, totalThreshold: 1 * 16 * 1024 * 1024 * 1024,
		totalFree: 400 * 16 * 1024 * 1024 * 1024,
	}
	for i := 1; i <= 400; i++ {
		volInfo := cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Vid:  proto.Vid(i),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		}

		modeInfo.volumes.Put(&volume{
			AllocVolumeInfo: volInfo,
		})
	}

	vm.modeInfos[codemode.CodeMode(2)] = modeInfo

	args := &proxy.AllocVolsArgs{
		Fsize:    4 * 1024,
		BidCount: 2,
		CodeMode: 2,
		Excludes: []proto.Vid{4, 5},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vm.Alloc(ctx, args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPollingAlloc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmcli := mock.ProxyMockClusterMgrCli(ctrl)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*ModeInfo)
	modeInfo := &ModeInfo{
		volumes: &volumes{}, totalThreshold: 16 * 1024 * 1024 * 1024,
		totalFree: 10 * 16 * 1024 * 1024 * 1024,
	}
	for i := 1; i <= 10; i++ {
		volInfo := cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Vid:  proto.Vid(i),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		}

		modeInfo.volumes.Put(&volume{
			AllocVolumeInfo: volInfo,
		})
	}

	vm.modeInfos[codemode.CodeMode(2)] = modeInfo
	args := &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: nil,
	}
	vid, err := vm.allocVid(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.Vid(2), vid)

	vid, err = vm.allocVid(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.Vid(3), vid)

	args = &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: []proto.Vid{3, 4},
		Discards: nil,
	}
	vid, err = vm.allocVid(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.Vid(5), vid)

	args = &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: []proto.Vid{1, 2},
		Discards: []proto.Vid{3, 4, 5, 6},
	}
	vid, err = vm.allocVid(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.Vid(9), vid)

	args = &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: nil,
	}
	vid, err = vm.allocVid(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.Vid(10), vid)

	args = &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: []proto.Vid{7, 8, 9, 10},
	}
	vid, err = vm.allocVid(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.Vid(1), vid)
}

func TestAllocVolumeRetry(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmcli := mock.ProxyMockClusterMgrCli(ctrl)
	v := volumeMgr{clusterMgr: cmcli}

	codemodes := codemode.GetAllCodeModes()
	args := &cm.AllocVolumeArgs{
		CodeMode: codemode.CodeMode(len(codemodes) + 1),
	}

	startTime := time.Now()
	vols, err := v.allocVolume(ctx, args)
	duration := time.Since(startTime)
	du := int64(duration / time.Millisecond)

	require.Nil(t, vols)
	require.Error(t, err)

	require.Less(t, int64(80), du, "duration: ", du)
}

func TestGetAvaliableVols(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cmcli := mock.ProxyMockClusterMgrCli(ctrl)
	v := &volumeMgr{
		BlobConfig: BlobConfig{},
		VolConfig:  VolConfig{},
		BidMgr:     nil,
		clusterMgr: cmcli,
		modeInfos:  make(map[codemode.CodeMode]*ModeInfo),
		mu:         sync.RWMutex{},
		allocChs:   make(map[codemode.CodeMode]chan *allocArgs),
		closeCh:    nil,
	}
	v.allocChs[codemode.EC6P6] = make(chan *allocArgs)

	modeInfo := &ModeInfo{
		volumes: &volumes{}, totalThreshold: 2 * 1024 * 1024 * 1024,
		totalFree: 15 * 1024 * 1024 * 1024,
	}
	for i := 1; i <= 5; i++ {
		volInfo := cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Vid:      proto.Vid(i),
					CodeMode: codemode.EC6P6,
					Free:     uint64(i * 1024 * 1024 * 1024),
				},
			},
			ExpireTime: 100,
		}

		modeInfo.volumes.Put(&volume{
			AllocVolumeInfo: volInfo,
		})
	}
	v.modeInfos[codemode.EC6P6] = modeInfo

	args := &proxy.AllocVolsArgs{
		Fsize:    5 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: nil,
	}
	ctx := context.Background()
	vols, err := v.getAvailableVols(ctx, args)
	vids := make([]proto.Vid, 0)
	for _, v := range vols {
		vids = append(vids, v.Vid)
	}
	require.NoError(t, err)
	require.Equal(t, []proto.Vid{1, 2, 3, 4, 5}, vids)

	args2 := &proxy.AllocVolsArgs{
		Fsize:    5 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: []proto.Vid{2, 4},
	}
	vols, err = v.getAvailableVols(ctx, args2)
	vids2 := make([]proto.Vid, 0)
	for _, v := range vols {
		vids2 = append(vids2, v.Vid)
	}
	require.NoError(t, err)
	require.Equal(t, []proto.Vid{1, 3, 5}, vids2)

	args3 := &proxy.AllocVolsArgs{
		Fsize:    5 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: []proto.Vid{1, 3, 5},
	}
	vols, err = v.getAvailableVols(ctx, args3)
	vids3 := make([]proto.Vid, 0)
	for _, v := range vols {
		vids3 = append(vids3, v.Vid)
	}
	require.Error(t, err)
	require.Equal(t, []proto.Vid{}, vids3)

	// test alloc background
	v.modeInfos[codemode.EC6P6].volumes.vols = []*volume{}
	go v.allocVolumeLoop(codemode.EC6P6)
	time.Sleep(time.Millisecond * 100)
	go v.allocNotify(ctx, codemode.EC6P6, 5)
	go v.allocNotify(ctx, codemode.EC6P6, 5)
	time.Sleep(time.Millisecond * 100)
	require.Equal(t, 5, len(v.modeInfos[codemode.EC6P6].volumes.List()))
}
