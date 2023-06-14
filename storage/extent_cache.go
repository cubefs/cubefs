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

package storage

import (
	"container/list"
	"sync"
)

// ExtentMapItem stores the extent entity pointer and the element
// pointer of the extent entity in a cache list.
type ExtentMapItem struct {
	e       *Extent
	element *list.Element
}

// ExtentCache is an implementation of the ExtentCache with LRU support.
type ExtentCache struct {
	extentMap   map[uint64]*ExtentMapItem
	extentList  *list.List
	tinyExtents map[uint64]*Extent
	tinyLock    sync.RWMutex
	lock        sync.RWMutex
	capacity    int
}

// NewExtentCache creates and returns a new ExtentCache instance.
func NewExtentCache(capacity int) *ExtentCache {
	return &ExtentCache{
		extentMap:   make(map[uint64]*ExtentMapItem),
		extentList:  list.New(),
		capacity:    capacity,
		tinyExtents: make(map[uint64]*Extent),
	}
}

// Put puts an extent object into the cache.
func (cache *ExtentCache) Put(e *Extent) {
	if IsTinyExtent(e.extentID) {
		cache.tinyLock.Lock()
		cache.tinyExtents[e.extentID] = e
		cache.tinyLock.Unlock()
		return
	}
	cache.lock.Lock()
	defer cache.lock.Unlock()
	item := &ExtentMapItem{
		e:       e,
		element: cache.extentList.PushBack(e),
	}
	cache.extentMap[e.extentID] = item
	cache.evict()
}

// Get gets the extent from the cache.
func (cache *ExtentCache) Get(extentID uint64) (e *Extent, ok bool) {
	if IsTinyExtent(extentID) {
		cache.tinyLock.RLock()
		e, ok = cache.tinyExtents[extentID]
		cache.tinyLock.RUnlock()
		return
	}
	cache.lock.Lock()
	defer cache.lock.Unlock()
	var (
		item *ExtentMapItem
	)
	if item, ok = cache.extentMap[extentID]; ok {
		if !IsTinyExtent(extentID) {
			cache.extentList.MoveToBack(item.element)
		}
		e = item.e
	}
	return
}

// Del deletes the extent stored in the cache.
func (cache *ExtentCache) Del(extentID uint64) {
	if IsTinyExtent(extentID) {
		return
	}
	cache.lock.Lock()
	defer cache.lock.Unlock()
	var (
		item *ExtentMapItem
		ok   bool
	)
	if item, ok = cache.extentMap[extentID]; ok {
		delete(cache.extentMap, extentID)
		cache.extentList.Remove(item.element)

		item.e.Close()
	}
}

// Clear closes all the extents stored in the cache.
func (cache *ExtentCache) Clear() {
	cache.tinyLock.RLock()
	for _, extent := range cache.tinyExtents {
		extent.Close()
	}
	cache.tinyLock.RUnlock()

	cache.lock.Lock()
	defer cache.lock.Unlock()
	for e := cache.extentList.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*Extent)
		delete(cache.extentMap, ec.extentID)

		ec.Close()
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
			if IsTinyExtent(front.extentID) {
				continue
			}
			delete(cache.extentMap, front.extentID)
			cache.extentList.Remove(e)
			front.Close()
		}
	}
}

// Flush synchronizes the extent stored in the cache to the disk.
func (cache *ExtentCache) Flush() {

	cache.tinyLock.RLock()
	for _, extent := range cache.tinyExtents {
		extent.Flush()
	}
	cache.tinyLock.RUnlock()

	cache.lock.RLock()
	defer cache.lock.RUnlock()
	for _, item := range cache.extentMap {
		item.e.Flush()
	}
}
