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

package fs

import (
	"container/list"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	// MinDentryCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinDentryCacheEvictNum = 10
	// MaxDentryCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxDentryCacheEvictNum = 200000

	DentryBgEvictionInterval = 2 * time.Minute
)

// Dcache defines the structure of the inode cache.
type Dcache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

// NewDentryCache returns a new inode cache.
func NewDcache(exp time.Duration, maxElements int) *Dcache {
	dc := &Dcache{
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go dc.backgroundEviction()
	return dc
}

// Put puts the given inode info into the inode cache.
func (dc *Dcache) Put(info *proto.DentryInfo) {
	dc.Lock()
	old, ok := dc.cache[info.Name]
	if ok {
		dc.lruList.Remove(old)
		delete(dc.cache, info.Name)
	}

	if dc.lruList.Len() >= dc.maxElements {
		dc.evict(true)
	}

	dentrySetExpiration(info, dc.expiration)
	element := dc.lruList.PushFront(info)
	dc.cache[info.Name] = element
	dc.Unlock()
	// log.LogDebugf("Dcache put inode: inode(%v)", info.Inode)
}

// Get returns the inode info based on the given inode number.
func (dc *Dcache) Get(name string) *proto.DentryInfo {
	dc.RLock()
	element, ok := dc.cache[name]
	if !ok {
		dc.RUnlock()
		return nil
	}

	info := element.Value.(*proto.DentryInfo)
	if dentryExpired(info) && DisableMetaCache {
		dc.RUnlock()
		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v), expired(%d)", time.Now().Format(LogTimeFormat), info.Inode, info.Expiration())
		return nil
	}
	dc.RUnlock()
	return info
}

// Delete deletes the dentry info based on the given name(partentId+name).
func (dc *Dcache) Delete(name string) {
	// log.LogDebugf("Dcache Delete: ino(%v)", ino)
	dc.Lock()
	element, ok := dc.cache[name]
	if ok {
		dc.lruList.Remove(element)
		delete(dc.cache, name)
	}
	dc.Unlock()
}

// Foreground eviction cares more about the speed.
// Background eviction evicts all expired items from the cache.
// The caller should grab the WRITE lock of the inode cache.
func (dc *Dcache) evict(foreground bool) {
	var count int

	for i := 0; i < MinDentryCacheEvictNum; i++ {
		element := dc.lruList.Back()
		if element == nil {
			return
		}

		// For background eviction, if all expired items have been evicted, just return
		// But for foreground eviction, we need to evict at least MinDentryCacheEvictNum inodes.
		// The foreground eviction, does not need to care if the inode has expired or not.
		info := element.Value.(*proto.DentryInfo)
		if !foreground && !dentryExpired(info) {
			return
		}

		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), info.Inode)
		dc.lruList.Remove(element)
		delete(dc.cache, info.Name)
		count++
	}

	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < MaxDentryCacheEvictNum; i++ {
		element := dc.lruList.Back()
		if element == nil {
			break
		}
		info := element.Value.(*proto.DentryInfo)
		if !dentryExpired(info) {
			break
		}
		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), info.Inode)
		dc.lruList.Remove(element)
		delete(dc.cache, info.Name)
		count++
	}
}

func (dc *Dcache) backgroundEviction() {
	t := time.NewTicker(DentryBgEvictionInterval)
	defer t.Stop()

	for range t.C {
		log.LogInfof("Dcache: start BG evict")
		if !DisableMetaCache {
			log.LogInfof("Dcache: no need to do BG evict")
			continue
		}
		start := time.Now()
		dc.Lock()
		dc.evict(false)
		dc.Unlock()
		elapsed := time.Since(start)
		log.LogInfof("Dcache: total inode cache(%d), cost(%d)ns", dc.lruList.Len(), elapsed.Nanoseconds())
	}
}
