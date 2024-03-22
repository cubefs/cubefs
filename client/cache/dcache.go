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

package cache

import (
	"sync"
	"time"
)

// DentryCache defines the dentry cache.
type DentryCache struct {
	sync.RWMutex
	cache               map[string]uint64
	expiration          time.Time
	dentryValidDuration time.Duration
}

// NewDentryCache returns a new dentry cache.
func NewDentryCache(dentryValidDuration time.Duration) *DentryCache {
	return &DentryCache{
		cache:               make(map[string]uint64),
		expiration:          time.Now().Add(dentryValidDuration),
		dentryValidDuration: dentryValidDuration,
	}
}

// Put puts an item into the cache.
func (dc *DentryCache) Put(name string, ino uint64) {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	dc.cache[name] = ino
	dc.expiration = time.Now().Add(dc.dentryValidDuration)
}

// Get gets the item from the cache based on the given key.
func (dc *DentryCache) Get(name string) (uint64, bool) {
	if dc == nil {
		return 0, false
	}

	dc.RLock()
	if dc.expiration.Before(time.Now()) {
		dc.RUnlock()
		dc.Lock()
		dc.cache = make(map[string]uint64)
		dc.Unlock()
		return 0, false
	}
	ino, ok := dc.cache[name]
	dc.RUnlock()
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

// Count gets the count of cache items.
func (dc *DentryCache) Count() int {
	if dc == nil {
		return 0
	}
	dc.RLock()
	defer dc.RUnlock()

	return len(dc.cache)
}

func (dc *DentryCache) IsEmpty() bool {
	if dc == nil {
		return true
	}
	dc.RLock()
	defer dc.RUnlock()

	if len(dc.cache) == 0 {
		return true
	}
	return false
}

func (dc *DentryCache) IsExpired() bool {
	if dc == nil {
		return false
	}
	dc.RLock()
	defer dc.RUnlock()

	if dc.expiration.Before(time.Now()) {
		return true
	}
	return false
}

func (dc *DentryCache) Expiration() time.Time {
	if dc == nil {
		return time.Now()
	}
	dc.RLock()
	defer dc.RUnlock()

	return dc.expiration
}

func (dc *DentryCache) ResetExpiration(dentryValidDuration time.Duration) {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()

	dc.expiration = time.Now().Add(dentryValidDuration)
	dc.dentryValidDuration = dentryValidDuration
}
