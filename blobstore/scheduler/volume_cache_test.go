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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

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

	volCache := NewVolumeCache(cmClient, 10)
	err := volCache.Load()
	require.NoError(t, err)

	// no cache will update
	_, err = volCache.Get(1)
	require.NoError(t, err)
	// return cache
	_, err = volCache.Get(1)
	require.NoError(t, err)

	// update ErrFrequentlyUpdate
	_, err = volCache.Update(1)
	require.ErrorIs(t, err, ErrFrequentlyUpdate)

	// list and get failed
	cmClient.EXPECT().ListVolume(any, any, any).AnyTimes().Return(nil, proto.Vid(0), errMock)
	cmClient.EXPECT().GetVolumeInfo(any, any).Return(&client.VolumeInfoSimple{}, errMock)
	volCache = NewVolumeCache(cmClient, -1)
	_, err = volCache.Get(1)
	require.ErrorIs(t, err, errMock)
	err = volCache.Load()
	require.ErrorIs(t, err, errMock)
}

func TestDoubleCheckedRun(t *testing.T) {
	cmClient := NewMockClusterMgrAPI(gomock.NewController(t))
	{
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(&client.VolumeInfoSimple{}, nil)
		c := NewVolumeCache(cmClient, -1)
		err := DoubleCheckedRun(context.Background(), c, 1, func(*client.VolumeInfoSimple) error { return errMock })
		require.Equal(t, err, errMock)
	}
	{
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(nil, errcode.ErrVolumeNotExist)
		c := NewVolumeCache(cmClient, -1)
		err := DoubleCheckedRun(context.Background(), c, 1, func(*client.VolumeInfoSimple) error { return errMock })
		require.ErrorIs(t, err, errcode.ErrVolumeNotExist)
	}
	{
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(&client.VolumeInfoSimple{}, nil)
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(nil, errcode.ErrVolumeNotExist)
		c := NewVolumeCache(cmClient, -1)
		err := DoubleCheckedRun(context.Background(), c, 1, func(*client.VolumeInfoSimple) error { return nil })
		require.Equal(t, err, errcode.ErrVolumeNotExist)
	}
	{
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil)
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)

		c := NewVolumeCache(cmClient, -1)
		err := DoubleCheckedRun(context.Background(), c, 1, func(volInfo *client.VolumeInfoSimple) error {
			c.Update(1)
			return nil
		})
		require.NoError(t, err)
	}
	{
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 1}}}, nil)
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 3}}}, nil)
		cmClient.EXPECT().GetVolumeInfo(any, any).Return(
			&client.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 4}}}, nil)

		c := NewVolumeCache(cmClient, -1)
		err := DoubleCheckedRun(context.Background(), c, 1, func(volInfo *client.VolumeInfoSimple) error {
			c.Update(1)
			return nil
		})
		require.Equal(t, err, errVolumeMissmatch)
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
			require.NoError(b, cacher.Load())

			b.ResetTimer()
			for ii := 0; ii < b.N; ii++ {
				cacher.Get(proto.Vid(ii % items))
			}
		})
	}
}
