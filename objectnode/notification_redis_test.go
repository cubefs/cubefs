// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestNewRedisNotifier(t *testing.T) {
	conf := RedisNotifierConfig{}
	_, err := NewRedisNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Address = "127.0.0.1:6378;127.0.0.1:6379"
	_, err = NewRedisNotifier("cubefs", conf)
	require.Error(t, err)

	conf.Address = "127.0.0.1:66379"
	_, err = NewRedisNotifier("cubefs", conf)
	require.Error(t, err)

	server := miniredis.RunT(t)
	defer server.Close()

	conf.Address = server.Addr()
	notifier, err := NewRedisNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())

	conf.EnableClusterMode = true
	notifier, err = NewRedisNotifier("cubefs", conf)
	require.NoError(t, err)
	require.NoError(t, notifier.Close())
}

func TestRedisNotifier(t *testing.T) {
	server := miniredis.RunT(t)
	defer server.Close()

	conf := RedisNotifierConfig{}
	conf.Address = server.Addr()
	conf.Key = "redis-notifier-unit-test"
	notifier, err := NewRedisNotifier("cubefs", conf)
	require.NoError(t, err)

	require.Equal(t, "cubefs:redis", notifier.Name())
	require.Equal(t, NotifierID{ID: "cubefs", Name: "redis"}, notifier.ID())

	require.NoError(t, notifier.Send([]byte("test")))
	require.NoError(t, notifier.Close())
}
