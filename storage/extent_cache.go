// Copyright 2018 The Container File System Authors.
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

// ExtentMapItem stored Extent entity pointer and the element
// pointer of the Extent entity in cache list.
type ExtentMapItem struct {
	ext *Extent
	ele *list.Element
}

// ExtentCache is an implementation of ExtentCache with LRU support.
// This cache manager store extent entity into an linked table and make
// index by using an hash map.
// Details for LRU, this cache move the hot spot entity to back of link
// table and release entity from front of link table.
type ExtentCache struct {
	extentMap   map[uint64]*ExtentMapItem
	extentList  *list.List
	tinyExtents map[uint64]*Extent
	tinyLock    sync.RWMutex
	lock        sync.RWMutex
	capacity    int
}

// NewExtentCache create and returns a new ExtentCache instance.
func NewExtentCache(capacity int) *ExtentCache {
	return &ExtentCache{
		extentMap:   make(map[uint64]*ExtentMapItem),
		extentList:  list.New(),
		capacity:    capacity,
		tinyExtents: make(map[uint64]*Extent),
	}
}

// Put Connect extent object into cache.
func (cache *ExtentCache) Put(extent *Extent) {
	if IsTinyExtent(extent.ID()) {
		cache.tinyLock.Lock()
		cache.tinyExtents[extent.ID()] = extent
		cache.tinyLock.Unlock()
		return
	}
	cache.lock.Lock()
	defer cache.lock.Unlock()
	item := &ExtentMapItem{
		ext: extent,
		ele: cache.extentList.PushBack(extent),
	}
	cache.extentMap[extent.ID()] = item
	cache.fireLRU()
}

// Get Connect extent from cache with specified extent identity (extentID).
func (cache *ExtentCache) Get(extentID uint64) (extent *Extent, ok bool) {
	if IsTinyExtent(extentID) {
		cache.tinyLock.RLock()
		extent, ok = cache.tinyExtents[extentID]
		cache.tinyLock.RUnlock()
		return
	}
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	var (
		item *ExtentMapItem
	)
	if item, ok = cache.extentMap[extentID]; ok {
		if !IsTinyExtent(extentID) {
			cache.extentList.MoveToBack(item.ele)
		}
		extent = item.ext
	}
	return
}

// Del extent stored in cache this specified extent identity (extentID).
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
		cache.extentList.Remove(item.ele)
		item.ext.Close()
	}
}

// Clear close and synchronize all extent stored in this cache and remove them from cache.
func (cache *ExtentCache) Clear() {
	for _, extent := range cache.tinyExtents {
		extent.Close()
	}
	cache.lock.Lock()
	defer cache.lock.Unlock()
	for e := cache.extentList.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*Extent)
		delete(cache.extentMap, ec.ID())
		ec.Close()
		cache.extentList.Remove(curr)
	}
	cache.extentList = list.New()
	cache.extentMap = make(map[uint64]*ExtentMapItem)
}

// Size returns number of extents stored in this cache.
func (cache *ExtentCache) Size() int {
	cache.lock.RLock()
	cache.lock.RUnlock()
	return cache.extentList.Len()
}

func (cache *ExtentCache) fireLRU() {
	if cache.capacity <= 0 {
		return
	}
	needRemove := cache.extentList.Len() - cache.capacity
	for i := 0; i < needRemove; i++ {
		if e := cache.extentList.Front(); e != nil {
			front := e.Value.(*Extent)
			if IsTinyExtent(front.ID()) {
				continue
			}
			delete(cache.extentMap, front.ID())
			cache.extentList.Remove(e)
			front.Close()
		}
	}
}

// Flush synchronizes the extent stored in cache to the disk.
func (cache *ExtentCache) Flush() {
	for _, extent := range cache.tinyExtents {
		extent.Flush()
	}
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	for _, item := range cache.extentMap {
		item.ext.Flush()
	}
}
