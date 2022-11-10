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

package memcache

import (
	lru "github.com/hashicorp/golang-lru"
)

// MemCache memory cache with lru-2q
type MemCache struct {
	*lru.TwoQueueCache
}

// NewMemCache new memory cache with capacity size.
func NewMemCache(size int) (*MemCache, error) {
	cache, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}
	return &MemCache{cache}, nil
}

// Get by key, return nil if key doesn't exist or value is nil.
func (mc *MemCache) Get(key interface{}) interface{} {
	value, ok := mc.TwoQueueCache.Get(key)
	if !ok {
		return nil
	}
	return value
}

// Set key with value, delete key if value is nil.
func (mc *MemCache) Set(key interface{}, value interface{}) {
	mc.TwoQueueCache.Add(key, value)
}
