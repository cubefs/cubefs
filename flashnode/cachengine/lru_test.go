// Copyright 2018 The CubeFS Authors.
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

package cachengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

var nilFunc = func(v interface{}) error { return nil }

func TestLRUManyThings(t *testing.T) {
	require.PanicsWithValue(t, "must provide a positive capacity", func() {
		_ = NewCache(0, util.MB*100, time.Hour, nilFunc, nilFunc)
	})

	c := NewCache(10, util.MB*100, time.Hour, nilFunc, nilFunc)
	defer c.Close()
	var (
		k  = 1
		v1 = &CacheBlock{blockKey: "block1"}
		v2 = &CacheBlock{blockKey: "block2"}
	)

	require.Equal(t, 0, c.Len())

	actual, err := c.Get(k)
	require.Error(t, err)
	require.Nil(t, actual)

	_, has := c.Peek(k)
	require.False(t, has)

	c.Set(k, v1, time.Hour)
	actual, err = c.Get(k)
	require.NoError(t, err)
	require.Equal(t, actual, v1)
	require.Equal(t, 1, c.Len())

	_, has = c.Peek(k)
	require.True(t, has)

	c.Set(k, v2, time.Minute)
	actual, err = c.Get(k)
	require.NoError(t, err)
	require.Equal(t, actual, v2)
	require.Equal(t, 1, c.Len())

	require.True(t, c.Evict(k))
	actual, err = c.Get(k)
	require.Error(t, err)
	require.Nil(t, actual)
	require.Equal(t, 0, c.Len())
	require.True(t, c.Evict(k))
}

func TestLRUCapacity(t *testing.T) {
	called := false
	c := NewCache(2, util.MB*100, time.Hour,
		func(v interface{}) error { called = true; return nil },
		func(v interface{}) error { return fmt.Errorf("close error") })
	c.Set(1, &CacheBlock{blockKey: "block1"}, 0)
	c.Set(2, &CacheBlock{blockKey: "block2"}, 0)
	c.Set(3, &CacheBlock{blockKey: "block3"}, 0)
	require.Equal(t, 2, c.Len())
	_, err := c.Get(1)
	require.Error(t, err)
	require.True(t, called)
	t.Logf("%+v", c.Status())
	require.Error(t, c.Close())
}

func TestLRUExpired(t *testing.T) {
	c := NewCache(2, util.MB*100, time.Hour, nilFunc, nilFunc)
	defer c.Close()
	e := -time.Hour
	c.Set(1, &CacheBlock{blockKey: "block1"}, e)
	c.Set(2, &CacheBlock{blockKey: "block2"}, 0)
	require.Equal(t, 2, c.Len())
	_, err := c.Get(1)
	require.Error(t, err)
	require.Equal(t, 1, c.Len())

	c.EvictAll()
	require.Equal(t, 0, c.Len())
}
