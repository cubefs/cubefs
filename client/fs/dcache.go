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

package fs

import (
	"sync"
	"time"
)

// DentryCache defines the dentry cache.
type DentryCache struct {
	sync.Mutex
	cache        map[string]uint64
	expiration   time.Time
	acceleration bool
}

// NewDentryCache returns a new dentry cache.
func NewDentryCache(acceleration bool) *DentryCache {
	return &DentryCache{
		cache:        make(map[string]uint64),
		expiration:   time.Now().Add(DentryValidDuration),
		acceleration: acceleration,
	}
}

// Put puts an item into the cache.
func (dc *DentryCache) Put(name string, ino uint64) {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	if dc.cache == nil {
		dc.cache = make(map[string]uint64)
	}
	dc.cache[name] = ino
	dc.expiration = time.Now().Add(DentryValidDuration)
}

// Get gets the item from the cache based on the given key.
func (dc *DentryCache) Get(name string) (uint64, bool) {
	if dc == nil {
		return 0, false
	}

	dc.Lock()
	defer dc.Unlock()
	if dc.expiration.Before(time.Now()) && !dc.acceleration {
		dc.cache = make(map[string]uint64)
		return 0, false
	}
	ino, ok := dc.cache[name]
	return ino, ok
}

// Delete deletes the item based on the given key.
func (dc *DentryCache) Delete(name string) {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	delete(dc.cache, name)
}

func (dc *DentryCache) Len() int {
	if dc == nil {
		return 0
	}
	dc.Lock()
	defer dc.Unlock()
	return len(dc.cache)
}

func (dc *DentryCache) Clear() {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	dc.cache = nil
}

// NegativeDentryCache defines the cache for non-existent dentries.
// This cache stores entries that were confirmed to not exist by the backend,
// with a very short expiration time to avoid stale cache.
type NegativeDentryCache struct {
	sync.Mutex
	cache map[string]int64 // int64 stores Unix timestamp in nanoseconds
}

// NewNegativeDentryCache returns a new negative dentry cache.
func NewNegativeDentryCache() *NegativeDentryCache {
	return &NegativeDentryCache{
		cache: make(map[string]int64),
	}
}

// Put puts a non-existent dentry into the cache with current timestamp.
func (ndc *NegativeDentryCache) Put(name string) {
	if ndc == nil {
		return
	}
	ndc.Lock()
	defer ndc.Unlock()
	if ndc.cache == nil {
		ndc.cache = make(map[string]int64)
	}
	ndc.cache[name] = time.Now().UnixNano()
}

// Get checks if the dentry is in the negative cache and still valid.
// Returns true if the dentry is cached as non-existent and the cache is still valid.
func (ndc *NegativeDentryCache) Get(name string) bool {
	if ndc == nil {
		return false
	}
	ndc.Lock()
	defer ndc.Unlock()
	if ndc.cache == nil {
		return false
	}
	timestamp, ok := ndc.cache[name]
	if !ok {
		return false
	}
	// Check if cache is still valid
	if time.Now().UnixNano()-timestamp > int64(NegativeDentryValidDuration) {
		// Cache expired, remove it
		delete(ndc.cache, name)
		return false
	}
	return true
}

// Delete deletes the negative cache entry for the given name.
func (ndc *NegativeDentryCache) Delete(name string) {
	if ndc == nil {
		return
	}
	ndc.Lock()
	defer ndc.Unlock()
	delete(ndc.cache, name)
}

// Clear clears all negative cache entries.
func (ndc *NegativeDentryCache) Clear() {
	if ndc == nil {
		return
	}
	ndc.Lock()
	defer ndc.Unlock()
	ndc.cache = nil
}
