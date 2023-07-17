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

	"github.com/stretchr/testify/require"

	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/proxy/mock"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestNewVolumeMgr(t *testing.T) {
	ctx := context.Background()
	cmcli := mock.ProxyMockClusterMgrCli(t)
	vm, err := NewVolumeMgr(ctx, BlobConfig{}, VolConfig{
		InitVolumeNum:         4,
		MetricReportIntervalS: 1, RetainIntervalS: 1,
	}, cmcli)
	time.Sleep(2 * time.Second)
	require.NoError(t, err)
	vm.Close()
	time.Sleep(100 * time.Millisecond)
}

func TestGetAllocList(t *testing.T) {
	cmcli := mock.ProxyMockClusterMgrCli(t)
	bid, err := NewBidMgr(context.Background(), BlobConfig{BidAllocNums: 10000}, cmcli)
	require.NoError(t, err)

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

	volInfo6 := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  6,
				Free: 10 * 1024 * 1024,
			},
		},
		ExpireTime: expireTime,
	}

	modeInfoMap := make(map[codemode.CodeMode]*modeInfo)

	volumeStateInfo1 := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 5 * 1024 * 1024,
	}
	volumeStateInfo1.Put(&volume{AllocVolumeInfo: volInfo1}, false)
	volumeStateInfo1.Put(&volume{AllocVolumeInfo: volInfo2}, false)
	modeInfoMap[codemode.CodeMode(1)] = volumeStateInfo1

	volumeStateInfo2 := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 5 * 1024 * 1024,
	}
	volumeStateInfo2.Put(&volume{AllocVolumeInfo: volInfo3}, false)
	volumeStateInfo2.Put(&volume{AllocVolumeInfo: volInfo4}, false)
	volumeStateInfo2.Put(&volume{AllocVolumeInfo: volInfo5}, false)
	volumeStateInfo2.Put(&volume{AllocVolumeInfo: volInfo6}, true)
	modeInfoMap[codemode.CodeMode(2)] = volumeStateInfo2

	vm := volumeMgr{
		clusterMgr: cmcli,
		modeInfos:  modeInfoMap,
		BlobConfig: BlobConfig{
			BidAllocNums: 1000,
		},
		BidMgr: bid,
	}

	ctx := context.Background()
	{
		vids, _, err := vm.List(ctx, codemode.CodeMode(2))
		require.NoError(t, err)
		require.Equal(t, []proto.Vid{3, 4, 5, 6}, vids)
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
		require.Nil(t, err)
		require.Equal(t, 3, int(vid))
		info3, _ := vm.modeInfos[codemode.CodeMode(2)].Get(proto.Vid(3), false)
		totalFree := vm.modeInfos[codemode.CodeMode(2)].current.totalFree
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

	{
		alloc, err := vm.Alloc(context.Background(), &proxy.AllocVolsArgs{Fsize: 100, CodeMode: 2, BidCount: 1})
		require.NoError(t, err)
		require.NotNil(t, alloc)
	}
}

func BenchmarkVolumeMgr_Alloc(b *testing.B) {
	cmcli := mock.ProxyMockClusterMgrCli(b)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)
	info := &modeInfo{
		current:        &volumes{},
		totalThreshold: 1 * 16 * 1024 * 1024 * 1024,
		backup:         &volumes{},
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

		info.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)
		info.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, true)
	}

	vm.modeInfos[codemode.CodeMode(2)] = info

	args := &proxy.AllocVolsArgs{
		Fsize:    100,
		BidCount: 2,
		CodeMode: 2,
		Excludes: []proto.Vid{4, 5},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vm.Alloc(ctx, args)
		require.Nil(b, err)
	}
}

func BenchmarkAllocByBackup(b *testing.B) {
	cmcli := mock.ProxyMockClusterMgrCli(b)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)
	info := &modeInfo{
		current:        &volumes{totalFree: 5 * 1024},
		totalThreshold: 16 * 1024 * 1024 * 1024,
		backup:         &volumes{},
	}
	volInfo := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  proto.Vid(1),
				Free: 10 * 16 * 1024 * 1024 * 1024,
			},
		},
		ExpireTime: 100,
	}
	info.Put(&volume{
		AllocVolumeInfo: volInfo,
	}, true)

	vm.modeInfos[codemode.CodeMode(2)] = info
	args := &proxy.AllocVolsArgs{
		Fsize:    1 << 10,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: nil,
	}
	vm.allocChs = make(map[codemode.CodeMode]chan *allocArgs)
	vm.allocChs[codemode.EC6P6] = make(chan *allocArgs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vm.allocVid(context.Background(), args)
		require.NoError(b, err)
	}
}

func TestPollingAlloc(t *testing.T) {
	cmcli := mock.ProxyMockClusterMgrCli(t)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)
	info := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 16 * 1024 * 1024 * 1024,
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

		info.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)

	}
	info.Put(&volume{
		AllocVolumeInfo: cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Vid:  proto.Vid(20),
					Free: 16 * 1024 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		},
	}, true)

	vm.modeInfos[codemode.CodeMode(2)] = info
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

func TestAllocVolumeFailed(t *testing.T) {
	cmcli := mock.ProxyMockClusterMgrCli(t)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)
	vm.modeInfos[codemode.EC6P6] = nil
	args := &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: nil,
	}
	_, err := vm.allocVid(ctx, args)
	require.Error(t, err)
	require.ErrorIs(t, err, errcode.ErrNoAvaliableVolume)

	info := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 0,
	}
	vm.modeInfos[codemode.EC6P6] = info

	volInfo := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:  proto.Vid(1),
				Free: 1024,
			},
		},
		ExpireTime: 100,
	}

	info.Put(&volume{
		AllocVolumeInfo: volInfo,
	}, false)

	args = &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: nil,
	}
	vm.allocChs = make(map[codemode.CodeMode]chan *allocArgs)
	vm.allocChs[codemode.EC6P6] = make(chan *allocArgs)
	_, err = vm.allocVid(ctx, args)
	require.Error(t, err)
	require.ErrorIs(t, err, errcode.ErrNoAvaliableVolume)
}

func TestAllocVolumeRetry(t *testing.T) {
	ctx := context.Background()
	cmcli := mock.ProxyMockClusterMgrCli(t)
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
	cmcli := mock.ProxyMockClusterMgrCli(t)
	v := &volumeMgr{
		BlobConfig: BlobConfig{},
		VolConfig:  VolConfig{},
		BidMgr:     nil,
		clusterMgr: cmcli,
		modeInfos:  make(map[codemode.CodeMode]*modeInfo),
		allocChs:   make(map[codemode.CodeMode]chan *allocArgs),
		closeCh:    nil,
	}
	v.allocChs[codemode.EC6P6] = make(chan *allocArgs)

	info := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 2 * 1024 * 1024 * 1024,
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

		info.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, false)
	}

	volInfo := cm.AllocVolumeInfo{
		VolumeInfo: cm.VolumeInfo{
			VolumeInfoBase: cm.VolumeInfoBase{
				Vid:      proto.Vid(6),
				CodeMode: codemode.EC6P6,
				Free:     uint64(6 * 1024 * 1024 * 1024),
			},
		},
		ExpireTime: 100,
	}
	info.Put(&volume{
		AllocVolumeInfo: volInfo,
	}, true)

	v.modeInfos[codemode.EC6P6] = info

	args := &proxy.AllocVolsArgs{
		Fsize:    1 << 30,
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
		Fsize:    1 << 30,
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
		CodeMode: codemode.EC6P6,
		BidCount: 1,
		Excludes: nil,
		Discards: []proto.Vid{1, 3, 5},
	}
	_, err = v.getAvailableVols(ctx, args3)
	require.NoError(t, err)

	// test alloc background
	v.modeInfos[codemode.EC6P6].current.vols = []*volume{}
	go v.allocVolumeLoop(codemode.EC6P6)
	time.Sleep(time.Millisecond * 100)
	go v.allocNotify(ctx, codemode.EC6P6, 5, false)
	go v.allocNotify(ctx, codemode.EC6P6, 5, false)
	time.Sleep(time.Millisecond * 100)
	require.Equal(t, 5, len(v.modeInfos[codemode.EC6P6].List(false)))
}

func TestAllocParallel(b *testing.T) {
	log.SetOutputLevel(2)
	cmcli := mock.ProxyMockClusterMgrCli(b)
	ctx := context.Background()
	bidMgr, _ := NewBidMgr(ctx, BlobConfig{BidAllocNums: 100000}, cmcli)
	vm := volumeMgr{clusterMgr: cmcli, BidMgr: bidMgr}

	vm.modeInfos = make(map[codemode.CodeMode]*modeInfo)
	vm.allocChs = make(map[codemode.CodeMode]chan *allocArgs)
	info := &modeInfo{
		current:        &volumes{},
		backup:         &volumes{},
		totalThreshold: 0,
	}
	for i := 1; i <= 400; i++ {
		volInfo := cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Vid:      proto.Vid(i),
					CodeMode: codemode.EC6P6,
					Free:     20 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		}

		info.Put(&volume{
			AllocVolumeInfo: volInfo,
		}, true)
	}
	info.Put(&volume{
		AllocVolumeInfo: cm.AllocVolumeInfo{
			VolumeInfo: cm.VolumeInfo{
				VolumeInfoBase: cm.VolumeInfoBase{
					Vid:      proto.Vid(20),
					CodeMode: codemode.EC6P6,
					Free:     100 * 16 * 1024 * 1024,
				},
			},
			ExpireTime: 100,
		},
	}, false)

	vm.modeInfos[codemode.EC6P6] = info
	vm.allocChs[codemode.EC6P6] = make(chan *allocArgs)
	var wg sync.WaitGroup
	for i := 0; i < 8000; i++ {
		wg.Add(1)
		go func() {
			vid, err := vm.allocVid(context.Background(),
				&proxy.AllocVolsArgs{
					Fsize:    1024 * 1024,
					CodeMode: codemode.EC6P6,
					BidCount: 1,
				})
			require.Nil(b, err)
			require.Less(b, proto.Vid(0), vid)
			wg.Done()
		}()
	}
	wg.Wait()
}
