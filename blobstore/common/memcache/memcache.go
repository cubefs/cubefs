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
	"context"

	lru "github.com/hashicorp/golang-lru"
)

// MemCache memory cache with lru-2q
//     ctx with logging
type MemCache struct {
	ctx   context.Context
	cache *lru.TwoQueueCache
}

// NewMemCache new memory cache with capacity size
func NewMemCache(ctx context.Context, size int) (*MemCache, error) {
	cache, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}
	return &MemCache{ctx: ctx, cache: cache}, nil
}

// Get get by key, return nil if key doesn't exist or value is nil
func (mc *MemCache) Get(key interface{}) interface{} {
	value, ok := mc.cache.Get(key)
	if !ok {
		return nil
	}

	return value
}

// Set set to key with value, delete key if value is nil
func (mc *MemCache) Set(key interface{}, value interface{}) {
	mc.cache.Add(key, value)
}
