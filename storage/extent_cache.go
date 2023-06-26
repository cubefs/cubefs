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

func (ln CacheListener) OnEvent(event CacheEvent, e *Extent) {
	if ln != nil {
		ln(event, e)
	}
}

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
	item := &ExtentMapItem{
		e:            e,
		element:      cache.extentList.PushBack(e),
		latestActive: time.Now().UnixNano(),
	}
	cache.extentMap[e.extentID] = item
	var evictedExtents = cache.evict()
	cache.lock.Unlock()

	cache.ln.OnEvent(CacheEvent_Add, e)
	for _, e := range evictedExtents {
		cache.ln.OnEvent(CacheEvent_Evict, e)
		_ = e.Close(true)
	}
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
	var (
		item          *ExtentMapItem = nil
		ok            bool
		evictedExtent *Extent = nil
	)
	cache.lock.Lock()
	if item, ok = cache.extentMap[extentID]; ok {
		delete(cache.extentMap, extentID)
		evictedExtent = item.element.Value.(*Extent)
		cache.extentList.Remove(item.element)
	}
	cache.lock.Unlock()
	if evictedExtent != nil {
		cache.ln.OnEvent(CacheEvent_Evict, evictedExtent)
		_ = evictedExtent.Close(false)
	}
}

// Close closes all the extents stored in the cache.
func (cache *ExtentCache) Close() {
	var evictedExtents []*Extent
	cache.lock.Lock()
	for e := cache.extentList.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*Extent)
		delete(cache.extentMap, ec.extentID)
		evictedExtents = append(evictedExtents, ec)
		cache.extentList.Remove(curr)
	}
	cache.extentList = list.New()
	cache.extentMap = make(map[uint64]*ExtentMapItem)
	cache.lock.Unlock()
	for _, e := range evictedExtents {
		cache.ln.OnEvent(CacheEvent_Evict, e)
		_ = e.Close(true)
	}
}

// Size returns number of extents stored in the cache.
func (cache *ExtentCache) Size() int {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.extentList.Len()
}

// evict方法用于从cache中释放超出capacity容积限制的extent。该方法不保证并发安全。
func (cache *ExtentCache) evict() []*Extent {
	if cache.capacity <= 0 {
		return nil
	}
	needRemove := cache.extentList.Len() - cache.capacity
	if needRemove <= 0 {
		return nil
	}
	var evicts = make([]*Extent, needRemove)
	var count = 0
	for i := 0; i < needRemove; i++ {
		if e := cache.extentList.Front(); e != nil {
			var front = e.Value.(*Extent)
			delete(cache.extentMap, front.extentID)
			cache.extentList.Remove(e)
			evicts[i] = front
			count++
		}
	}
	return evicts[:count]
}

func (cache *ExtentCache) EvictExpired() {
	if cache.ttl == 0 {
		return
	}
	var nowUnixNano = time.Now().UnixNano()

	// 检查Cache的过期节点
	cache.lock.RLock()
	var expiredElements = make([]*list.Element, cache.extentList.Len())
	var count = 0
	for element := cache.extentList.Front(); element != nil; element = element.Next() {
		var extent = element.Value.(*Extent)
		var item, has = cache.extentMap[extent.extentID]
		if !has || nowUnixNano-item.latestActive > cache.ttl {
			expiredElements[count] = element
			count++
			continue
		}
		break
	}
	cache.lock.RUnlock()

	if count == 0 {
		return
	}

	// 释放过期节点FD
	var evictExtents = make([]*Extent, count)
	cache.lock.Lock()
	for i := 0; i < count; i++ {
		var element = expiredElements[i]
		extent := element.Value.(*Extent)
		delete(cache.extentMap, extent.extentID)
		cache.extentList.Remove(element)
		evictExtents[i] = extent
	}
	cache.lock.Unlock()
	for _, e := range evictExtents {
		cache.ln.OnEvent(CacheEvent_Evict, e)
		_ = e.Close(true)
	}
}

func (cache *ExtentCache) ForceEvict(ratio Ratio) {
	if !ratio.Valid() {
		return
	}
	cache.lock.RLock()
	var (
		evictTotal    = int(float64(len(cache.extentMap)) * float64(ratio))
		evictElements = make([]*list.Element, evictTotal)
		count         = 0
	)
	for element := cache.extentList.Front(); element != nil; element = element.Next() {
		if count >= evictTotal {
			break
		}
		evictElements[count] = element
		count++
	}
	cache.lock.RUnlock()

	if count == 0 {
		return
	}

	var evictExtents = make([]*Extent, count)
	cache.lock.Lock()
	for i := 0; i < count; i++ {
		var element = evictElements[i]
		var extent = element.Value.(*Extent)
		delete(cache.extentMap, extent.extentID)
		cache.extentList.Remove(element)
		evictExtents[i] = extent
	}
	cache.lock.Unlock()
	for _, e := range evictExtents {
		cache.ln.OnEvent(CacheEvent_Evict, e)
		_ = e.Close(true)
	}
}

func (cache *ExtentCache) Flush() (cnt int) {
	cache.lock.RLock()
	var extents = make([]*Extent, cache.extentList.Len())
	var i int
	for element := cache.extentList.Front(); element != nil; element = element.Next() {
		var extent = element.Value.(*Extent)
		extents[i] = extent
		i++
	}
	cache.lock.RUnlock()
	for _, e := range extents[:i] {
		_ = e.Flush()
	}
	cnt = i
	return
}
