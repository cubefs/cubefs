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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/redis"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var fmtKeyRedisVolume = func(cid proto.ClusterID, vid proto.Vid) string {
	return fmt.Sprintf("access/volume/%d/%d", cid, vid)
}

func TestAccessVolumeGetterNew(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterNew")

	getter, err := controller.NewVolumeGetter(1, cmcli, rediscli, time.Millisecond*200)
	require.Nil(t, err)
	info := getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	id := proto.Vid(1)
	dataCalled[id] = 0
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1011), info.Units[0].Vuid)

	info = getter.Get(ctx, id, true)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1012), info.Units[1].Vuid)

	// delete redis cache
	rediscli.Del(context.TODO(), fmtKeyRedisVolume(1, id))
	getter.Get(ctx, id, true)
	require.Equal(t, 1, dataCalled[id])

	time.Sleep(time.Millisecond * 250)
	getter.Get(ctx, id, true)
	require.Equal(t, 2, dataCalled[id])

	getter.Get(ctx, id, false)
	require.Equal(t, 3, dataCalled[id])

	rediscli.Del(context.TODO(), fmtKeyRedisVolume(1, id))
	id = proto.Vid(9)
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])

	time.Sleep(time.Millisecond * 250)
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])

	info = getter.Get(ctx, proto.Vid(10), false)
	require.Nil(t, info)
}

func TestAccessVolumeGetterNotExistVolume(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterNotExistVolume")

	getter, err := controller.NewVolumeGetter(0xfe, cmcli, rediscli, time.Millisecond*500)
	require.Nil(t, err)
	info := getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	id := proto.Vid(1)
	dataCalled[id] = 0
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1011), info.Units[0].Vuid)

	id = vid404
	dataCalled[id] = 0
	info = getter.Get(ctx, id, true)
	require.Nil(t, info)
	require.Equal(t, 3, dataCalled[id])
	require.ErrorIs(t, redis.ErrNotFound, rediscli.Get(ctx, fmtKeyRedisVolume(0xfe, id), nil))

	var wg sync.WaitGroup
	wg.Add(10)
	for ii := 0; ii < 10; ii++ {
		go func() {
			getter.Get(ctx, id, true)
			wg.Done()
		}()
	}
	wg.Wait()
	require.Equal(t, 3, dataCalled[id])

	time.Sleep(time.Millisecond * 500)
	getter.Get(ctx, id, true)
	require.Equal(t, 6, dataCalled[id])
	require.ErrorIs(t, redis.ErrNotFound, rediscli.Get(ctx, fmtKeyRedisVolume(0xfe, id), nil))

	getter, err = controller.NewVolumeGetter(0xee, cmcli, rediscli, 0)
	require.NoError(t, err)
	id = vid404
	dataCalled[id] = 0
	for ii := 0; ii < 10; ii++ {
		getter.Get(ctx, id, true)
		require.Equal(t, 3, dataCalled[id])
		time.Sleep(time.Millisecond * 100)
	}
}

func TestAccessVolumeGetterExpiration(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterExpiration")
	mockRedis := redis.NewClusterClient(&redis.ClusterConfig{
		Addrs: []string{"127.0.0.1:5678"},
	})

	getter, err := controller.NewVolumeGetter(1, cmcli, mockRedis, time.Millisecond*200)
	require.Nil(t, err)

	info := getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	id := proto.Vid(1)
	dataCalled[id] = 0
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])

	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])

	// expiration
	time.Sleep(time.Millisecond * 250)
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 2, dataCalled[id])

	info = getter.Get(ctx, id, false)
	require.NotNil(t, info)
	require.Equal(t, 3, dataCalled[id])
}

func TestAccessVolumeGetterBrokenRedis(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterBrokenRedis")
	mockRedis := redis.NewClusterClient(&redis.ClusterConfig{
		Addrs: []string{"127.0.0.1:5678"},
	})

	getter, err := controller.NewVolumeGetter(1, cmcli, mockRedis, -1)
	require.Nil(t, err)

	info := getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	id := proto.Vid(1)
	dataCalled[id] = 0
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1011), info.Units[0].Vuid)

	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1012), info.Units[1].Vuid)
}

func TestAccessVolumeGetterNoRedis(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumeGetterNoRedis")

	getter, err := controller.NewVolumeGetter(1, cmcli, nil, -1)
	require.Nil(t, err)

	info := getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	id := proto.Vid(1)
	dataCalled[id] = 0
	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1011), info.Units[0].Vuid)

	info = getter.Get(ctx, id, true)
	require.NotNil(t, info)
	require.Equal(t, 1, dataCalled[id])
	require.Equal(t, proto.Vuid(1012), info.Units[1].Vuid)
}

func TestAccessVolumePunish(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "TestAccessVolumePunish")

	getter, err := controller.NewVolumeGetter(1, cmcli, rediscli, 0)
	require.Nil(t, err)

	info := getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	getter.Punish(ctx, proto.Vid(0), 10)
	info = getter.Get(ctx, proto.Vid(0), true)
	require.Nil(t, info)

	id := proto.Vid(1)
	info = getter.Get(ctx, id, true)
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
