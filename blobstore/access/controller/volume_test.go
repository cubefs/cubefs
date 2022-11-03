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

package controller_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func proxyService() controller.ServiceController {
	service, _ := controller.NewServiceController(controller.ServiceConfig{IDC: idc}, cmcli, nil)
	return service
}

func TestAccessVolumeGetterNew(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterNew")

	getter, err := controller.NewVolumeGetter(1, proxyService(), proxycli, time.Millisecond*200)
	require.Nil(t, err)
	require.Nil(t, getter.Get(ctx, proto.Vid(0), true))

	id := proto.Vid(1)
	dataCalled[id] = 0
	info := getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1011), info.Units[0].Vuid)

	info = getter.Get(ctx, id, true)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1012), info.Units[1].Vuid)

	time.Sleep(time.Millisecond * 250)
	getter.Get(ctx, id, false)
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, 3, dataCalled[id])
	for range [10]struct{}{} {
		getter.Get(ctx, id, true)
		require.Equal(t, 3, dataCalled[id])
	}
}

func TestAccessVolumeGetterNotExistVolume(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterNotExistVolume")

	getter, err := controller.NewVolumeGetter(0xfe, proxyService(), proxycli, time.Millisecond*200)
	require.NoError(t, err)

	id := vid404
	dataCalled[id] = 0
	require.Nil(t, getter.Get(ctx, id, true))
	require.Equal(t, 1, dataCalled[id])
	for range [10]struct{}{} {
		require.Nil(t, getter.Get(ctx, id, true))
		require.Equal(t, 1, dataCalled[id])
	}

	time.Sleep(time.Millisecond * 210)
	getter.Get(ctx, id, true)
	require.Equal(t, 2, dataCalled[id])

	getter, err = controller.NewVolumeGetter(0xee, proxyService(), proxycli, 0)
	require.NoError(t, err)
	id = vid404
	dataCalled[id] = 0
	for range [10]struct{}{} {
		getter.Get(ctx, id, true)
		require.Equal(t, 1, dataCalled[id])
		time.Sleep(time.Millisecond * 10)
	}

	id = proto.Vid(500)
	dataCalled[id] = 0
	require.Nil(t, getter.Get(ctx, id, true))
	require.Equal(t, 6, dataCalled[id])
}

func TestAccessVolumeGetterExpiration(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterExpiration")

	getter, err := controller.NewVolumeGetter(1, proxyService(), proxycli, time.Millisecond*200)
	require.Nil(t, err)

	id := proto.Vid(1)
	dataCalled[id] = 0
	require.NotNil(t, getter.Get(ctx, id, true))
	require.Equal(t, 1, dataCalled[id])

	// expiration
	time.Sleep(time.Millisecond * 250)
	require.NotNil(t, getter.Get(ctx, id, true))
	require.Equal(t, 2, dataCalled[id])

	require.NotNil(t, getter.Get(ctx, id, false))
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, 4, dataCalled[id])
}

func TestAccessVolumePunish(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumePunish")

	getter, err := controller.NewVolumeGetter(1, proxyService(), proxycli, 0)
	require.Nil(t, err)
	require.Nil(t, getter.Get(ctx, proto.Vid(0), true))

	getter.Punish(ctx, proto.Vid(0), 10)
	require.Nil(t, getter.Get(ctx, proto.Vid(0), true))

	id := proto.Vid(1)
	info := getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, proto.Vuid(1011), info.Units[0].Vuid)
	require.False(t, info.IsPunish)

	getter.Punish(ctx, id, 1)
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.True(t, info.IsPunish)

	time.Sleep(time.Second)
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.False(t, info.IsPunish)
}
