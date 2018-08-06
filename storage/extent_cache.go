// Copyright 2018 The ChuBao Authors.
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

package storage

import (
	"container/list"
	"sync"
)

// ExtentCache defined a cache for Extent entity for access improvement.
type ExtentCache interface {
	// Put extent object into cache.
	Put(extent Extent)

	// Get extent from cache with specified extent identity (extentId).
	Get(extentId uint64) (extent Extent, ok bool)

	// Del extent stored in cache this specified extent identity (extentId).
	Del(extentId uint64)

	// Size returns number of extents stored in this cache.
	Size() int

	// Flush synchronize extent stored in this cache to disk immediately.
	Flush()

	// Clear close and synchronize all extent stored in this cache and remove them from cache.
	Clear()
}

// ExtentMapItem stored Extent entity pointer and the element
// pointer of the Extent entity in cache list.
type extentMapItem struct {
	ext Extent
	ele *list.Element
}

// LRUExtentCache is an implementation of ExtentCache with LRU support.
// This cache manager store extent entity into an linked table and make
// index by using an hash map.
// Details for LRU, this cache move the hot spot entity to back of link
// table and release entity from front of link table.
type lruExtentCache struct {
	extentMap  map[uint64]*extentMapItem
	extentList *list.List
	lock       sync.RWMutex
	capacity   int
}

// NewExtentCache create and returns a new ExtentCache instance.
func NewExtentCache(capacity int) ExtentCache {
	return &lruExtentCache{
		extentMap:  make(map[uint64]*extentMapItem),
		extentList: list.New(),
		capacity:   capacity,
	}
}

// Put extent object into cache.
func (cache *lruExtentCache) Put(extent Extent) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	item := &extentMapItem{
		ext: extent,
		ele: cache.extentList.PushBack(extent),
	}
	cache.extentMap[extent.ID()] = item
	cache.fireLRU()
}

// Get extent from cache with specified extent identity (extentId).
func (cache *lruExtentCache) Get(extentId uint64) (extent Extent, ok bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	var (
		item *extentMapItem
	)
	if item, ok = cache.extentMap[extentId]; ok {
		cache.extentList.MoveToBack(item.ele)
		extent = item.ext
	}
	return
}

// Del extent stored in cache this specified extent identity (extentId).
func (cache *lruExtentCache) Del(extentId uint64) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	var (
		item *extentMapItem
		ok   bool
	)
	if item, ok = cache.extentMap[extentId]; ok {
		delete(cache.extentMap, extentId)
		cache.extentList.Remove(item.ele)
		item.ext.Close()
	}
}

// Clear close and synchronize all extent stored in this cache and remove them from cache.
func (cache *lruExtentCache) Clear() {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	for e := cache.extentList.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*fsExtent)
		delete(cache.extentMap, ec.ID())
		ec.Close()
		cache.extentList.Remove(curr)
	}
	cache.extentList = list.New()
	cache.extentMap = make(map[uint64]*extentMapItem)
}

// Size returns number of extents stored in this cache.
func (cache *lruExtentCache) Size() int {
	cache.lock.RLock()
	cache.lock.RUnlock()
	return cache.extentList.Len()
}

func (cache *lruExtentCache) fireLRU() {
	if cache.capacity <= 0 {
		return
	}
	needRemove := cache.extentList.Len() - cache.capacity
	for i := 0; i < needRemove; i++ {
		if e := cache.extentList.Front(); e != nil {
			front := e.Value.(Extent)
			delete(cache.extentMap, front.ID())
			cache.extentList.Remove(e)
			front.Close()
		}
	}
}

// Flush synchronize extent stored in this cache to disk immediately.
func (cache *lruExtentCache) Flush() {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	for _, item := range cache.extentMap {
		item.ext.Flush()
	}
}
