// Copyright 2018 The Chubao Authors.
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

package ecstorage

import (
	"container/list"
	"sync"
	"time"
)

// ExtentMapItem stores the extent entity pointer and the element
// pointer of the extent entity in a cache list.
type ExtentMapItem struct {
	e       *Extent
	element *list.Element

	latestActive int64
}

type Ratio float64

func (r Ratio) Valid() bool {
	return r >= 0 && r <= 1
}

func NewRatio(val float64) Ratio {
	if val < 0 {
		val = 0
	}
	if val > 1 {
		val = 1
	}
	return Ratio(val)
}

type CacheEvent uint8

const (
	CacheEvent_Add CacheEvent = iota
	CacheEvent_Evict
)

type CacheListener func(event CacheEvent, e *Extent)

// ExtentCache is an implementation of the ExtentCache with LRU support.
type ExtentCache struct {
	extentMap  map[uint64]*ExtentMapItem
	extentList *list.List
	lock       sync.RWMutex
	capacity   int
	ttl        int64
	ln         CacheListener
}

// NewExtentCache creates and returns a new ExtentCache instance.
func NewExtentCache(capacity int, ttl time.Duration, ln CacheListener) *ExtentCache {
	return &ExtentCache{
		extentMap:  make(map[uint64]*ExtentMapItem),
		extentList: list.New(),
		capacity:   capacity,
		ttl:        int64(ttl),
		ln:         ln,
	}
}

// Put puts an extent object into the cache.
func (cache *ExtentCache) Put(e *Extent) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	item := &ExtentMapItem{
		e:       e,
		element: cache.extentList.PushBack(e),

		latestActive: time.Now().UnixNano(),
	}
	cache.extentMap[e.extentID] = item
	if cache.ln != nil {
		cache.ln(CacheEvent_Add, e)
	}
	cache.evict()
}

// Get gets the extent from the cache.
func (cache *ExtentCache) Get(extentID uint64) (e *Extent, ok bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	var (
		item *ExtentMapItem
	)
	if item, ok = cache.extentMap[extentID]; ok {
		item.latestActive = time.Now().UnixNano()
		cache.extentList.MoveToBack(item.element)
		e = item.e
	}
	return
}

// Del deletes the extent stored in the cache.
func (cache *ExtentCache) Del(extentID uint64) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	var (
		item *ExtentMapItem
		ok   bool
	)
	if item, ok = cache.extentMap[extentID]; ok {
		delete(cache.extentMap, extentID)
		e := item.element.Value.(*Extent)
		cache.extentList.Remove(item.element)
		if cache != nil {
			cache.ln(CacheEvent_Evict, e)
		}
		_ = item.e.Close()
	}
}

// Clear closes all the extents stored in the cache.
func (cache *ExtentCache) Clear() {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	for e := cache.extentList.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*Extent)
		delete(cache.extentMap, ec.extentID)
		if cache != nil {
			cache.ln(CacheEvent_Evict, ec)
		}
		_ = ec.Close()
		cache.extentList.Remove(curr)
	}
	cache.extentList = list.New()
	cache.extentMap = make(map[uint64]*ExtentMapItem)
}

// Size returns number of extents stored in the cache.
func (cache *ExtentCache) Size() int {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.extentList.Len()
}

func (cache *ExtentCache) evict() {
	if cache.capacity <= 0 {
		return
	}
	needRemove := cache.extentList.Len() - cache.capacity
	for i := 0; i < needRemove; i++ {
		if e := cache.extentList.Front(); e != nil {
			front := e.Value.(*Extent)
			delete(cache.extentMap, front.extentID)
			cache.extentList.Remove(e)
			if cache.ln != nil {
				cache.ln(CacheEvent_Evict, front)
			}
			_ = front.Close()
		}
	}
}

func (cache *ExtentCache) EvictExpired() {
	if cache.ttl == 0 {
		return
	}
	var nowUnixNano = time.Now().UnixNano()
	var expiredMap = make([]*list.Element, 0)

	// 检查Cache的过期节点
	cache.lock.RLock()
	for element := cache.extentList.Front(); element != nil; element = element.Next() {
		extent := element.Value.(*Extent)
		item, has := cache.extentMap[extent.extentID]
		if !has || nowUnixNano-item.latestActive > cache.ttl {
			expiredMap = append(expiredMap, element)
			continue
		}
		break
	}
	cache.lock.RUnlock()

	// 释放过期节点FD
	if len(expiredMap) > 0 {
		cache.lock.Lock()
		for _, element := range expiredMap {
			extent := element.Value.(*Extent)
			delete(cache.extentMap, extent.extentID)
			cache.extentList.Remove(element)
			if cache.ln != nil {
				cache.ln(CacheEvent_Evict, extent)
			}
			_ = extent.Close()
		}
		cache.lock.Unlock()
	}
}

func (cache *ExtentCache) ForceEvict(ratio Ratio) {
	if !ratio.Valid() {
		return
	}
	evicts := make([]*list.Element, 0)
	cache.lock.RLock()
	evictTotal := int(float64(len(cache.extentMap)) * float64(ratio))
	count := 0
	for element := cache.extentList.Front(); element != nil; element = element.Next() {
		if count >= evictTotal {
			break
		}
		evicts = append(evicts, element)
		count++
	}
	cache.lock.RUnlock()

	if len(evicts) > 0 {
		cache.lock.Lock()
		for _, element := range evicts {
			extent := element.Value.(*Extent)
			delete(cache.extentMap, extent.extentID)
			cache.extentList.Remove(element)
			if cache.ln != nil {
				cache.ln(CacheEvent_Evict, extent)
			}
			_ = extent.Close()
		}
		cache.lock.Unlock()
	}
}

// Flush synchronizes the extent stored in the cache to the disk.
func (cache *ExtentCache) Flush() {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	for _, item := range cache.extentMap {
		_ = item.e.Flush()
	}
}
