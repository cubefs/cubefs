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

package cache_engine

import (
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestManyThings(t *testing.T) {
	c := NewCache(10, unit.MB*100, time.Hour, func(v interface{}) error {
		return nil
	}, func(v interface{}) error {
		return nil
	})
	var (
		k  = 1
		v1 = &CacheBlock{blockKey: "block1"}
		v2 = &CacheBlock{blockKey: "block2"}
	)

	// it's empty

	assert.Equal(t, c.Len(), 0)

	// not there to start with
	actual, err := c.Get(k)
	assert.NotNil(t, err)
	assert.Nil(t, actual)

	// add it
	c.Set(k, v1, time.Hour)

	// now it's there
	actual, err = c.Get(k)
	assert.Nil(t, err)
	assert.Equal(t, actual, v1)

	// we have some items
	assert.Equal(t, c.Len(), 1)

	// replace it
	c.Set(k, v2, time.Hour)

	// now find the new value
	actual, err = c.Get(k)
	assert.Nil(t, err)
	assert.Equal(t, actual, v2)

	// we still only have 1 item
	assert.Equal(t, c.Len(), 1)

	// remove it
	c.Evict(k)

	// not there anymore
	actual, err = c.Get(k)
	assert.NotNil(t, err)
	assert.Nil(t, actual)

	// it's empty again
	assert.Equal(t, c.Len(), 0)
}

func TestPanicNewSizeZero(t *testing.T) {
	assert.PanicsWithValue(t, "must provide a positive size", func() {
		_ = NewCache(0, unit.MB*100, time.Hour, func(v interface{}) error {
			return nil
		}, func(v interface{}) error {
			return nil
		})
	})
}

func TestCacheSize(t *testing.T) {
	c := NewCache(2, unit.MB*100, time.Hour, func(v interface{}) error {
		return nil
	}, func(v interface{}) error {
		return nil
	})

	c.Set(1, &CacheBlock{blockKey: "block1"}, time.Hour)
	c.Set(2, &CacheBlock{blockKey: "block2"}, time.Hour)
	c.Set(3, &CacheBlock{blockKey: "block3"}, time.Hour)
	assert.Equal(t, c.Len(), 2)
	_, err := c.Get(1)
	assert.NotNil(t, err)
}

func TestTTLExpired(t *testing.T) {
	c := NewCache(2, unit.MB*100, time.Hour, func(v interface{}) error {
		return nil
	}, func(v interface{}) error {
		return nil
	})
	e := -time.Hour
	c.Set(1, &CacheBlock{blockKey: "block1"}, e)
	assert.Equal(t, c.Len(), 1)
	_, err := c.Get(1)
	assert.NotNil(t, err)
	assert.Equal(t, c.Len(), 0)
}
