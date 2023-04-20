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

	"github.com/cubefs/cubefs/util/async"
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

type task func()

func (t task) run() {
	if t != nil {
		t()
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
	taskc      chan task
	closec     chan struct{}
	closeOnce  sync.Once
}

// NewExtentCache creates and returns a new ExtentCache instance.
func NewExtentCache(capacity int, ttl time.Duration, ln CacheListener) *ExtentCache {
	cache := &ExtentCache{
		extentMap:  make(map[uint64]*ExtentMapItem),
		extentList: list.New(),
		capacity:   capacity,
		ttl:        int64(ttl),
		ln:         ln,
		taskc:      make(chan task, capacity*2),
		closec:     make(chan struct{}),
	}
	go cache.worker()
	return cache
}

func (cache *ExtentCache) worker() {
	for {
		select {
		case task := <-cache.taskc:
			task.run()
		case <-cache.closec:
			return
		}
	}
}

// Put puts an extent object into the cache.
func (cache *ExtentCache) Put(e *Extent) {
	var evicts int
	cache.lock.Lock()
	item := &ExtentMapItem{
		e:            e,
		element:      cache.extentList.PushBack(e),
		latestActive: time.Now().UnixNano(),
	}
	cache.extentMap[e.extentID] = item
	evicts = cache.evict()
	cache.lock.Unlock()

	cache.ln.OnEvent(CacheEvent_Add, e)
	if evicts > 0 {
		cache.waitForEarlyTaskFinish()
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
	cache.lock.Lock()
	for e := cache.extentList.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*Extent)
		delete(cache.extentMap, ec.extentID)
		cache.extentList.Remove(curr)
		cache.pushToWorker(func() {
			cache.ln.OnEvent(CacheEvent_Evict, ec)
			_ = ec.Close(true)

		})
	}
	cache.extentList = list.New()
	cache.extentMap = make(map[uint64]*ExtentMapItem)
	cache.lock.Unlock()
	cache.waitForEarlyTaskFinish()
	cache.closeOnce.Do(func() {
		close(cache.closec)
	})
}

// Size returns number of extents stored in the cache.
func (cache *ExtentCache) Size() int {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.extentList.Len()
}

// evict方法用于从cache中释放超出capacity容积限制的extent。该方法不保证并发安全。
func (cache *ExtentCache) evict() (cnt int) {
	if cache.capacity <= 0 {
		return
	}
	needRemove := cache.extentList.Len() - cache.capacity
	for i := 0; i < needRemove; i++ {
		if e := cache.extentList.Front(); e != nil {
			front := e.Value.(*Extent)
			delete(cache.extentMap, front.extentID)
			cache.extentList.Remove(e)
			cache.pushToWorker(func() {
				cache.ln.OnEvent(CacheEvent_Evict, front)
				_ = front.Close(true)
			})
			cnt++
		}
	}
	return
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
			cache.pushToWorker(func() {
				cache.ln.OnEvent(CacheEvent_Evict, extent)
				_ = extent.Close(true)
			})
		}
		cache.lock.Unlock()
		cache.waitForEarlyTaskFinish()
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
			cache.pushToWorker(func() {
				cache.ln.OnEvent(CacheEvent_Evict, extent)
				_ = extent.Close(true)
			})
		}
		cache.lock.Unlock()
	}
	cache.waitForEarlyTaskFinish()
}

// Flush synchronizes the extent stored in the cache to the disk.
func (cache *ExtentCache) Flush() (cnt int) {
	cache.lock.RLock()
	for element := cache.extentList.Front(); element != nil; element = element.Next() {
		var extent = element.Value.(*Extent)
		cnt++
		cache.pushToWorker(func() {
			_ = extent.Flush()
		})
	}
	cache.lock.RUnlock()

	cache.waitForEarlyTaskFinish()
	return
}

func (cache *ExtentCache) pushToWorker(t task) bool {
	select {
	case <-cache.closec:
		return false
	case cache.taskc <- t:
	}
	return true
}

func (cache *ExtentCache) waitForEarlyTaskFinish() {
	var future = async.NewFuture()
	if cache.pushToWorker(func() {
		future.Respond(nil, nil)
	}) {
		_, _ = future.Response()
	}
}
