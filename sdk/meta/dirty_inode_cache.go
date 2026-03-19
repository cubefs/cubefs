// Copyright 2024 The CubeFS Authors.
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

package meta

import (
	"container/list"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	DirtyInodeTTL             = 5 * time.Second
	MaxDirtyInodeCache        = 10000
	dirtyInodeEvictMin        = 10
	dirtyInodeEvictMax        = 1000
	dirtyInodeBgEvictInterval = 2 * time.Minute
)

type dirtyInodeEntry struct {
	ino    uint64
	expiry int64 // Unix seconds
}

// dirtyInodeCache tracks inodes recently modified by this client.
// When a nearRead hits a dirty inode, it bypasses nearRead and goes to the leader,
// ensuring the client always reads its own latest writes.
type dirtyInodeCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

func newDirtyInodeCache(exp time.Duration, maxElements int) *dirtyInodeCache {
	dc := &dirtyInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go dc.backgroundEviction()
	return dc
}

// mark records the given inode as recently modified, so subsequent nearReads
// for this inode will be redirected to the leader.
func (dc *dirtyInodeCache) mark(ino uint64) {
	dc.Lock()
	old, ok := dc.cache[ino]
	if ok {
		dc.lruList.Remove(old)
		delete(dc.cache, ino)
	}

	if dc.lruList.Len() >= dc.maxElements {
		dc.evict(true)
	}

	entry := &dirtyInodeEntry{
		ino:    ino,
		expiry: time.Now().Add(dc.expiration).Unix(),
	}
	element := dc.lruList.PushFront(entry)
	dc.cache[ino] = element
	dc.Unlock()
}

// isDirty reports whether the given inode was recently modified by this client.
func (dc *dirtyInodeCache) isDirty(ino uint64) bool {
	dc.RLock()
	element, ok := dc.cache[ino]
	if !ok {
		dc.RUnlock()
		return false
	}
	entry := element.Value.(*dirtyInodeEntry)
	expired := timeutil.GetCurrentTimeUnix() > entry.expiry
	dc.RUnlock()

	if expired {
		dc.Lock()
		// Re-check under write lock to avoid double delete.
		if el, still := dc.cache[ino]; still {
			if timeutil.GetCurrentTimeUnix() > el.Value.(*dirtyInodeEntry).expiry {
				dc.lruList.Remove(el)
				delete(dc.cache, ino)
			}
		}
		dc.Unlock()
		return false
	}
	return true
}

// evict removes entries from the LRU tail. In foreground mode it always evicts
// at least dirtyInodeEvictMin entries; in background mode it stops as soon as
// it encounters a non-expired entry.
func (dc *dirtyInodeCache) evict(foreground bool) {
	for i := 0; i < dirtyInodeEvictMin; i++ {
		element := dc.lruList.Back()
		if element == nil {
			return
		}
		entry := element.Value.(*dirtyInodeEntry)
		if !foreground && timeutil.GetCurrentTimeUnix() <= entry.expiry {
			return
		}
		dc.lruList.Remove(element)
		delete(dc.cache, entry.ino)
	}

	if foreground {
		return
	}

	for i := 0; i < dirtyInodeEvictMax; i++ {
		element := dc.lruList.Back()
		if element == nil {
			break
		}
		entry := element.Value.(*dirtyInodeEntry)
		if timeutil.GetCurrentTimeUnix() <= entry.expiry {
			break
		}
		dc.lruList.Remove(element)
		delete(dc.cache, entry.ino)
	}
}

func (dc *dirtyInodeCache) backgroundEviction() {
	t := time.NewTicker(dirtyInodeBgEvictInterval)
	defer t.Stop()
	for range t.C {
		start := time.Now()
		dc.Lock()
		dc.evict(false)
		dc.Unlock()
		log.LogDebugf("dirtyInodeCache: bg evict done, remaining(%d), cost(%v)", dc.lruList.Len(), time.Since(start))
	}
}
