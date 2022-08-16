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

package memcache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/memcache"
)

func TestNewMemCache(t *testing.T) {
	_, err := memcache.NewMemCache(context.TODO(), 1024)
	require.Nil(t, err)

	// test 0 or -1
	_, err = memcache.NewMemCache(context.TODO(), -1)
	require.Error(t, err)
	_, err = memcache.NewMemCache(context.TODO(), 0)
	require.Error(t, err)
}

func TestMemCacheGetSet(t *testing.T) {
	mc, err := memcache.NewMemCache(context.TODO(), 4)
	require.Nil(t, err)

	key, value := "foo", "bar"
	mc.Set(key, value)

	v := mc.Get(key)
	val, ok := v.(string)
	require.True(t, ok)
	require.Equal(t, value, val)
}

func TestMemCacheDel(t *testing.T) {
	mc, err := memcache.NewMemCache(context.TODO(), 4)
	require.Nil(t, err)

	key, value := "foo", "bar"
	mc.Set(key, value)

	v := mc.Get(key)
	val, ok := v.(string)
	require.True(t, ok)
	require.Equal(t, value, val)

	mc.Set(key, nil)
	v = mc.Get(key)
	require.Nil(t, v)

	// test no key
	test := mc.Get("test-no-key")
	require.Nil(t, test)
}

func BenchmarkNewMemCache(b *testing.B) {
	for ii := 0; ii < b.N; ii++ {
		memcache.NewMemCache(context.TODO(), 4)
	}
}

func BenchmarkMemCacheGet(b *testing.B) {
	mc, _ := memcache.NewMemCache(context.TODO(), 4)
	key, value := "foo", "bar"
	mc.Set(key, value)
	for ii := 0; ii < b.N; ii++ {
		mc.Get(key)
	}
}

func BenchmarkMemCacheSet(b *testing.B) {
	mc, _ := memcache.NewMemCache(context.TODO(), 4)
	key, value := "foo", "bar"
	for ii := 0; ii < b.N; ii++ {
		mc.Set(key, value)
	}
}
