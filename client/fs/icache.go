// Copyright 2018 The Cubefs Authors.
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
	"container/list"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

const (
	// MinInodeCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinInodeCacheEvictNum = 10
	// MaxInodeCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxInodeCacheEvictNum = 200000

	BgEvictionInterval = 2 * time.Minute
)

// InodeCache defines the structure of the inode cache.
type InodeCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

// NewInodeCache returns a new inode cache.
func NewInodeCache(exp time.Duration, maxElements int) *InodeCache {
	ic := &InodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go ic.backgroundEviction()
	return ic
}

// Put puts the given inode info into the inode cache.
func (ic *InodeCache) Put(info *proto.InodeInfo) {
	ic.Lock()
	old, ok := ic.cache[info.Inode]
	if ok {
		ic.lruList.Remove(old)
		delete(ic.cache, info.Inode)
	}

	if ic.lruList.Len() >= ic.maxElements {
		ic.evict(true)
	}

	inodeSetExpiration(info, ic.expiration)
	element := ic.lruList.PushFront(info)
	ic.cache[info.Inode] = element
	ic.Unlock()
}

// Get returns the inode info based on the given inode number.
func (ic *InodeCache) Get(ino uint64) *proto.InodeInfo {
	ic.RLock()
	element, ok := ic.cache[ino]
	if !ok {
		ic.RUnlock()
		return nil
	}

	info := element.Value.(*proto.InodeInfo)
	if inodeExpired(info) {
		ic.RUnlock()
		//log.LogDebugf("InodeCache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), inode)
		return nil
	}
	ic.RUnlock()
	return info
}

// Delete deletes the inode info based on the given inode number.
func (ic *InodeCache) Delete(ino uint64) {
	//log.LogDebugf("InodeCache Delete: ino(%v)", ino)
	ic.Lock()
	element, ok := ic.cache[ino]
	if ok {
		ic.lruList.Remove(element)
		delete(ic.cache, ino)
	}
	ic.Unlock()
}

// Foreground eviction cares more about the speed.
// Background eviction evicts all expired items from the cache.
// The caller should grab the WRITE lock of the inode cache.
func (ic *InodeCache) evict(foreground bool) {
	var count int

	for i := 0; i < MinInodeCacheEvictNum; i++ {
		element := ic.lruList.Back()
		if element == nil {
			return
		}

		// For background eviction, if all expired items have been evicted, just return
		// But for foreground eviction, we need to evict at least MinInodeCacheEvictNum inodes.
		// The foreground eviction, does not need to care if the inode has expired or not.
		info := element.Value.(*proto.InodeInfo)
		if !foreground && !inodeExpired(info) {
			return
		}

		ic.lruList.Remove(element)
		delete(ic.cache, info.Inode)
		count++
	}

	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < MaxInodeCacheEvictNum; i++ {
		element := ic.lruList.Back()
		if element == nil {
			break
		}
		info := element.Value.(*proto.InodeInfo)
		if !inodeExpired(info) {
			break
		}
		ic.lruList.Remove(element)
		delete(ic.cache, info.Inode)
		count++
	}
}

func (ic *InodeCache) backgroundEviction() {
	t := time.NewTicker(BgEvictionInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			//log.LogInfof("InodeCache: start BG evict")
			//start := time.Now()
			ic.Lock()
			ic.evict(false)
			ic.Unlock()
			//elapsed := time.Since(start)
			//log.LogInfof("InodeCache: done BG evict, cost (%v)ns", elapsed.Nanoseconds())
		}
	}
}
