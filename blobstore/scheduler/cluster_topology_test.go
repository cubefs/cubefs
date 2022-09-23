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

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

var (
	topoDisk1 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.1:8000",
		DiskID:       1,
		FreeChunkCnt: 10,
		MaxChunkCnt:  700,
	}
	topoDisk2 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "rack1",
		Host:         "127.0.0.2:8000",
		DiskID:       2,
		FreeChunkCnt: 100,
		MaxChunkCnt:  700,
	}
	topoDisk3 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z1",
		Rack:         "rack1",
		Host:         "127.0.0.3:8000",
		DiskID:       3,
		FreeChunkCnt: 20,
		MaxChunkCnt:  700,
	}
	topoDisk4 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z1",
		Rack:         "rack2",
		Host:         "127.0.0.4:8000",
		DiskID:       4,
		FreeChunkCnt: 5,
		MaxChunkCnt:  700,
	}
	topoDisk5 = &client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z2",
		Rack:         "rack2",
		Host:         "127.0.0.4:8000",
		DiskID:       5,
		FreeChunkCnt: 200,
		MaxChunkCnt:  700,
	}
	topoDisk6 = &client.DiskInfoSimple{
		ClusterID:    123,
		Idc:          "z2",
		Rack:         "rack2",
		Host:         "127.0.0.4:8000",
		DiskID:       5,
		FreeChunkCnt: 200,
		MaxChunkCnt:  700,
	}

	topoDisks = []*client.DiskInfoSimple{topoDisk1, topoDisk2, topoDisk3, topoDisk4, topoDisk5, topoDisk6}
)

func TestNewClusterTopologyMgr(t *testing.T) {
	clusterTopMgr := &ClusterTopologyMgr{
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{}),
	}
	clusterTopMgr.buildClusterTopology(topoDisks, 1)
	require.Equal(t, 3, len(clusterTopMgr.GetIDCs()))
	disks := clusterTopMgr.GetIDCDisks("z0")
	require.Equal(t, 2, len(disks))
	disks = clusterTopMgr.GetIDCDisks("z1")
	require.Equal(t, 2, len(disks))
	disks = clusterTopMgr.GetIDCDisks("z2")
	require.Equal(t, 1, len(disks))
	disks = clusterTopMgr.GetIDCDisks("z3")
	require.True(t, disks == nil)

	ctr := gomock.NewController(t)
	clusterMgrCli := NewMockClusterMgrAPI(ctr)
	clusterMgrCli.EXPECT().ListClusterDisks(any).AnyTimes().Return([]*client.DiskInfoSimple{testDisk1}, nil)
	clusterMgrCli.EXPECT().ListBrokenDisks(any).AnyTimes().Return([]*client.DiskInfoSimple{testDisk2}, nil)
	clusterMgrCli.EXPECT().ListRepairingDisks(any).AnyTimes().Return([]*client.DiskInfoSimple{testDisk2}, nil)
	clusterMgrCli.EXPECT().ListVolume(any, any, any).Times(3).Return(nil, defaultMarker, errMock)
	clusterMgrCli.EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)
	clusterMgrCli.EXPECT().GetVolumeInfo(any, any).DoAndReturn(
		func(_ context.Context, vid proto.Vid) (*client.VolumeInfoSimple, error) {
			return &client.VolumeInfoSimple{Vid: vid}, nil
		},
	)
	conf := &clusterTopologyConfig{
		ClusterID:            1,
		UpdateInterval:       time.Microsecond,
		VolumeUpdateInterval: time.Microsecond,
		Leader:               true,
	}
	mgr := NewClusterTopologyMgr(clusterMgrCli, conf)
	defer mgr.Close()

	topology := mgr.(*ClusterTopologyMgr)
	topology.loadNormalDisks()
	topology.loadBrokenDisks()

	require.True(t, mgr.IsBrokenDisk(testDisk2.DiskID))
	require.False(t, mgr.IsBrokenDisk(testDisk1.DiskID))

	topology.loadBrokenDisks()
	require.True(t, mgr.IsBrokenDisk(testDisk2.DiskID))
	require.False(t, mgr.IsBrokenDisk(testDisk1.DiskID))

	// update volume
	err := mgr.LoadVolumes()
	require.ErrorIs(t, err, errMock)
	_, err = mgr.UpdateVolume(proto.Vid(1))
	require.ErrorIs(t, err, errMock)
	_, err = mgr.GetVolume(proto.Vid(1))
	require.NoError(t, err)
}

func TestVolumeCache(t *testing.T) {
	cmClient := NewMockClusterMgrAPI(gomock.NewController(t))
	cmClient.EXPECT().ListVolume(any, any, any).Times(2).DoAndReturn(
		func(_ context.Context, marker proto.Vid, _ int) ([]*client.VolumeInfoSimple, proto.Vid, error) {
			if marker == defaultMarker {
				return []*client.VolumeInfoSimple{{Vid: 4}}, proto.Vid(10), nil
			}
			return []*client.VolumeInfoSimple{{Vid: 9}}, defaultMarker, nil
		},
	)
	cmClient.EXPECT().GetVolumeInfo(any, any).DoAndReturn(
		func(_ context.Context, vid proto.Vid) (*client.VolumeInfoSimple, error) {
			return &client.VolumeInfoSimple{Vid: vid}, nil
		},
	)

	volCache := NewVolumeCache(cmClient, 10*time.Second)
	err := volCache.LoadVolumes()
	require.NoError(t, err)

	// no cache will update
	_, err = volCache.GetVolume(1)
	require.NoError(t, err)
	// return cache
	_, err = volCache.GetVolume(1)
	require.NoError(t, err)

	// update ErrFrequentlyUpdate
	_, err = volCache.UpdateVolume(1)
	require.ErrorIs(t, err, ErrFrequentlyUpdate)

	// list and get failed
	cmClient.EXPECT().ListVolume(any, any, any).AnyTimes().Return(nil, proto.Vid(0), errMock)
	cmClient.EXPECT().GetVolumeInfo(any, any).Return(&client.VolumeInfoSimple{}, errMock)
	volCache = NewVolumeCache(cmClient, -1)
	_, err = volCache.GetVolume(1)
	require.ErrorIs(t, err, errMock)
	err = volCache.LoadVolumes()
	require.ErrorIs(t, err, errMock)
}

func TestDoubleCheckedRun(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	{
		c := NewMockClusterTopology(ctr)
		c.EXPECT().GetVolume(any).Return(nil, errMock)
		err := DoubleCheckedRun(ctx, c, 1, func(*client.VolumeInfoSimple) error { return nil })
		require.ErrorIs(t, err, errMock)
	}
	{
		c := NewMockClusterTopology(ctr)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{}, nil)
		err := DoubleCheckedRun(ctx, c, 1, func(*client.VolumeInfoSimple) error { return errMock })
		require.ErrorIs(t, err, errMock)
	}
	{
		c := NewMockClusterTopology(ctr)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{}, nil)
		c.EXPECT().GetVolume(any).Return(nil, errMock)
		err := DoubleCheckedRun(ctx, c, 1, func(*client.VolumeInfoSimple) error { return nil })
		require.ErrorIs(t, err, errMock)
	}
	{
		c := NewMockClusterTopology(ctr)
		c.EXPECT().GetVolume(any).Times(2).Return(&client.VolumeInfoSimple{}, nil)
		err := DoubleCheckedRun(ctx, c, 1, func(*client.VolumeInfoSimple) error { return nil })
		require.NoError(t, err)
	}
	{
		c := NewMockClusterTopology(ctr)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)
		err := DoubleCheckedRun(context.Background(), c, 1, func(*client.VolumeInfoSimple) error { return nil })
		require.NoError(t, err)
	}
	{
		c := NewMockClusterTopology(ctr)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 3}}}, nil)
		c.EXPECT().GetVolume(any).Return(&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 4}}}, nil)
		err := DoubleCheckedRun(context.Background(), c, 1, func(*client.VolumeInfoSimple) error { return nil })
		require.ErrorIs(t, err, errVolumeMissmatch)
	}
}

func BenchmarkVolumeCache(b *testing.B) {
	for _, cs := range []struct {
		Name  string
		Items int
	}{
		{"tiny", 1 << 5},
		{"norm", 1 << 16},
		{"huge", 1 << 20},
	} {
		items := cs.Items
		b.Run(cs.Name, func(b *testing.B) {
			vols := make([]*client.VolumeInfoSimple, items)
			for idx := range vols {
				vols[idx] = &client.VolumeInfoSimple{Vid: proto.Vid(idx)}
			}
			cmCli := NewMockClusterMgrAPI(gomock.NewController(b))
			cmCli.EXPECT().ListVolume(any, any, any).Return(vols, defaultMarker, nil)

			cacher := NewVolumeCache(cmCli, -1)
			require.NoError(b, cacher.LoadVolumes())

			b.ResetTimer()
			for ii := 0; ii < b.N; ii++ {
				cacher.GetVolume(proto.Vid(ii % items))
			}
		})
	}
}
