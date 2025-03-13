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

package cachengine

import (
	"container/list"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type LruCache interface {
	Get(key interface{}) (interface{}, error)
	Peek(key interface{}) (interface{}, bool)
	Set(key interface{}, value interface{}, expiration time.Duration) (int, error)
	Evict(key interface{}) bool
	EvictAll(cacheEvictWorkerNum int)
	Close() error
	Status() *Status
	StatusAll() *Status
	Len() int
	GetRateStat() RateStat
	GetAllocated() int64
	GetExpiredTime(key interface{}) (time.Time, bool)
	AddMisses()
	CheckDiskSpace(dataPath string, key interface{}, size int64) (n int, err error)
	FreePreAllocatedSize(key interface{})
}

type Status struct {
	Allocated int64
	Length    int
	HitRate   RateStat
	Keys      []interface{}
}

type RateStat struct {
	Hits, Misses, Evicts int32
	HitRate              float64
}

// fCache implements a non-thread safe fixed size cache.
type fCache struct {
	cacheType          int
	capacity           int
	maxSize            int64
	allocated          int64
	preAllocated       int64
	preAllocatedKeyMap map[interface{}]int64

	hits   int32
	misses int32
	evicts int32 // evict by set
	recent *RateStat

	ttl   time.Duration
	lock  sync.RWMutex
	lru   *list.List
	items map[interface{}]*list.Element

	onDelete OnDeleteF
	onClose  OnCloseF

	closeOnce sync.Once
	closeCh   chan struct{}
}

// entry in the cache.
type entry struct {
	key       interface{}
	value     interface{}
	createAt  time.Time
	expiredAt time.Time
}

type (
	OnDeleteF func(v interface{}, reason string) error
	OnCloseF  func(v interface{}) error
)

// NewCache constructs a new LruCache of the given size that is not safe for
// concurrent use. If it will be panic, if size is not a positive integer.
func NewCache(cacheType int, capacity int, maxSize int64, ttl time.Duration, onDelete OnDeleteF, onClose OnCloseF) LruCache {
	if capacity <= 0 {
		panic("must provide a positive capacity")
	}
	c := &fCache{
		cacheType:          cacheType,
		capacity:           capacity,
		maxSize:            maxSize,
		preAllocatedKeyMap: make(map[interface{}]int64),
		ttl:                ttl,
		lru:                list.New(),
		hits:               1,
		recent:             &RateStat{},
		onDelete:           onDelete,
		onClose:            onClose,
		closeCh:            make(chan struct{}),
		items:              make(map[interface{}]*list.Element),
	}
	go func() {
		tick := time.NewTicker(time.Second * 60)
		defer tick.Stop()
		for {
			c.replaceRecent()
			select {
			case <-tick.C:
			case <-c.closeCh:
				return
			}
		}
	}()
	return c
}

func (c *fCache) replaceRecent() {
	hits := atomic.SwapInt32(&c.hits, 1)
	misses := atomic.SwapInt32(&c.misses, 0)
	evicts := atomic.SwapInt32(&c.evicts, 0)
	rs := RateStat{
		Hits:    hits,
		Misses:  misses,
		Evicts:  evicts,
		HitRate: float64(hits) / float64(hits+misses),
	}
	c.recent = &rs
}

func (c *fCache) Status() *Status {
	c.lock.RLock()
	keys := make([]interface{}, 0, len(c.items))
	for _, i := range c.items {
		v := i.Value.(*entry)
		keys = append(keys, v.key)
	}
	c.lock.RUnlock()
	return &Status{
		Allocated: atomic.LoadInt64(&c.allocated),
		Length:    c.lru.Len(),
		HitRate:   *c.recent,
		Keys:      keys,
	}
}

func (c *fCache) StatusAll() *Status {
	c.lock.RLock()
	keys := make([]interface{}, 0, len(c.items))
	for _, i := range c.items {
		v := i.Value.(*entry)
		keyInfo := v.key.(string) + "  " + v.expiredAt.Format("2006-01-02 15:04:05")
		keys = append(keys, keyInfo)
	}
	c.lock.RUnlock()
	return &Status{
		Allocated: atomic.LoadInt64(&c.allocated),
		Length:    c.lru.Len(),
		HitRate:   *c.recent,
		Keys:      keys,
	}
}

func GenerateRandTime(expiration time.Duration) time.Duration {
	if expiration <= 0 {
		return expiration
	}

	// 计算过期时间前后 10% 的时间范围
	minDuration := expiration - time.Duration(float64(expiration)*0.1)
	maxDuration := expiration + time.Duration(float64(expiration)*0.1)
	diff := maxDuration - minDuration
	randomDuration := minDuration + time.Duration(rand.Int63n(int64(diff)))
	return randomDuration
}

func (c *fCache) DeleteKeyFromPreAllocatedKeyMap(key interface{}) {
	if size, ok := c.preAllocatedKeyMap[key]; ok {
		atomic.AddInt64(&c.preAllocated, -size)
		delete(c.preAllocatedKeyMap, key)
	}
}

func (c *fCache) FreePreAllocatedSize(key interface{}) {
	c.lock.Lock()
	if c.cacheType != LRUCacheBlockCacheType {
		c.lock.Unlock()
		return
	}
	c.DeleteKeyFromPreAllocatedKeyMap(key)
	c.lock.Unlock()
}

func (c *fCache) CheckDiskSpace(dataPath string, key interface{}, size int64) (n int, err error) {
	var diskSpaceLeft int64

	c.lock.Lock()
	if c.cacheType != LRUCacheBlockCacheType {
		c.lock.Unlock()
		return
	}

	fs := syscall.Statfs_t{}
	if err = syscall.Statfs(dataPath, &fs); err != nil {
		c.lock.Unlock()
		return 0, fmt.Errorf("[CheckDiskSpace] stats disk(%v): %s", dataPath, err.Error())
	}
	diskSpaceLeft = int64(fs.Bavail * uint64(fs.Bsize))
	if _, ok := c.preAllocatedKeyMap[key]; !ok {
		c.preAllocatedKeyMap[key] = size
		atomic.AddInt64(&c.preAllocated, size)
	}

	preAllocated := atomic.LoadInt64(&c.preAllocated)
	diskSpaceLeft -= preAllocated

	if diskSpaceLeft > 0 {
		c.lock.Unlock()
		return 0, nil
	}

	toEvicts := make(map[interface{}]interface{})
	for diskSpaceLeft <= 0 {
		ent := c.lru.Back()
		if ent == nil {
			break
		}
		toEvicts[ent.Value.(*entry).key] = c.deleteElement(ent)
		atomic.AddInt32(&c.evicts, 1)
		n++
		diskSpaceLeft += ent.Value.(*entry).value.(*CacheBlock).getAllocSize()

	}
	for k, e := range toEvicts {
		_ = c.onDelete(e, fmt.Sprintf("lru disk space is full(%d / %d) diskSpaceLeft(%d)", atomic.LoadInt64(&c.allocated), c.maxSize, diskSpaceLeft))
		log.LogInfof("delete(%s) cos disk space full, len(%d) size(%d / %d) diskSpaceLeft(%d)", k, c.lru.Len(), atomic.LoadInt64(&c.allocated), c.maxSize, diskSpaceLeft)
	}
	c.lock.Unlock()
	if diskSpaceLeft <= 0 {
		return n, fmt.Errorf("diskSpaceLeft(%v) is not larger than 0, lru has no more entry can be deleted", diskSpaceLeft)
	}
	return n, nil
}

func (c *fCache) Set(key, value interface{}, expiration time.Duration) (n int, err error) {
	if expiration == 0 {
		expiration = c.ttl
	}

	expiration = GenerateRandTime(expiration)
	c.lock.Lock()
	if ent, ok := c.items[key]; ok {
		c.lru.MoveToFront(ent)
		v := ent.Value.(*entry)
		v.value = value
		v.createAt = time.Now()
		v.expiredAt = time.Now().Add(expiration)
		c.lock.Unlock()
		return 0, nil
	}

	if c.cacheType == LRUCacheBlockCacheType {
		newCb := value.(*CacheBlock)
		cbSize := newCb.getAllocSize()
		atomic.AddInt64(&c.allocated, cbSize)
	}

	c.items[key] = c.lru.PushFront(&entry{
		key:       key,
		value:     value,
		createAt:  time.Now(),
		expiredAt: time.Now().Add(expiration),
	})

	toEvicts := make(map[interface{}]interface{})
	for c.lru.Len() > c.capacity || (c.cacheType == LRUCacheBlockCacheType && atomic.LoadInt64(&c.allocated) > c.maxSize) {
		ent := c.lru.Back()
		if ent != nil {
			if c.cacheType == LRUCacheBlockCacheType {
				c.DeleteKeyFromPreAllocatedKeyMap(ent.Value.(*entry).key)
			}
			toEvicts[ent.Value.(*entry).key] = c.deleteElement(ent)
			atomic.AddInt32(&c.evicts, 1)
			n++
		}
	}

	for k, e := range toEvicts {
		_ = c.onDelete(e, fmt.Sprintf("lru is full(%d / %d)", atomic.LoadInt64(&c.allocated), c.maxSize))
		if c.cacheType == LRUCacheBlockCacheType {
			log.LogInfof("delete(%s) cos lru full, len(%d) size(%d / %d)", k, c.lru.Len(), atomic.LoadInt64(&c.allocated), c.maxSize)
		}
	}
	c.lock.Unlock()
	return n, nil
}

func (c *fCache) Get(key interface{}) (interface{}, error) {
	c.lock.Lock()
	if ent, ok := c.items[key]; ok {
		v := ent.Value.(*entry)
		if v.expiredAt.After(time.Now()) {
			atomic.AddInt32(&c.hits, 1)
			c.lru.MoveToFront(ent)
			c.lock.Unlock()
			return v.value, nil
		}
		atomic.AddInt32(&c.misses, 1)
		if c.cacheType == LRUCacheBlockCacheType {
			log.LogInfof("delete(%s) on get, create_time:(%v)  expired_time:(%v)",
				key, v.createAt.Format("2006-01-02 15:04:05"), v.expiredAt.Format("2006-01-02 15:04:05"))
			c.DeleteKeyFromPreAllocatedKeyMap(key)
		}
		e := c.deleteElement(ent)
		_ = c.onDelete(e, fmt.Sprintf("created: %v get expired: %v", v.createAt.Format("2006-01-02 15:04:05"),
			v.expiredAt.Format("2006-01-02 15:04:05")))
		c.lock.Unlock()
		return nil, fmt.Errorf("expired key[%v]", key)
	}
	c.lock.Unlock()
	atomic.AddInt32(&c.misses, 1)
	return nil, fmt.Errorf("key[%s] not found", key)
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *fCache) Peek(key interface{}) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if ent, ok := c.items[key]; ok {
		v := ent.Value.(*entry)
		return v.value, true
	}
	return nil, false
}

// EvictAll is used to completely clear the cache.
func (c *fCache) EvictAll(cacheEvictWorkerNum int) {
	c.lock.Lock()
	var wg sync.WaitGroup
	toEvicts := make(chan interface{}, cacheEvictWorkerNum)
	for i := 0; i < cacheEvictWorkerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range toEvicts {
				_ = c.onDelete(e, "execute evictAll operation")
			}
		}()
	}
	for _, ent := range c.items {
		if c.cacheType == LRUCacheBlockCacheType {
			c.DeleteKeyFromPreAllocatedKeyMap(ent.Value.(*entry).key)
		}
		toEvicts <- c.deleteElement(ent)
	}
	close(toEvicts)
	wg.Wait()
	c.lock.Unlock()
}

func (c *fCache) Evict(key interface{}) bool {
	c.lock.Lock()
	if ent, ok := c.items[key]; ok {
		if c.cacheType == LRUCacheBlockCacheType {
			log.LogInfof("delete(%s) manually", key)
			c.DeleteKeyFromPreAllocatedKeyMap(key)
		}
		e := c.deleteElement(ent)
		c.lock.Unlock()
		_ = c.onDelete(e, "execute evict operation")
		return true
	}
	c.lock.Unlock()
	return true
}

func (c *fCache) deleteElement(ent *list.Element) interface{} {
	v := ent.Value.(*entry)
	c.removeElement(ent)
	return v.value
}

func (c *fCache) Len() int {
	return c.lru.Len()
}

// removeElement is used to remove a given list element from the cache
func (c *fCache) removeElement(e *list.Element) {
	c.lru.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	if c.cacheType == LRUCacheBlockCacheType {
		cb := kv.value.(*CacheBlock)
		atomic.AddInt64(&c.allocated, -cb.getAllocSize())
	}
}

func (c *fCache) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})
	c.lock.Lock()
	defer c.lock.Unlock()
	var errCount int
	// todo: split close action with big lock
	for _, item := range c.items {
		kv := item.Value.(*entry)
		err := c.onClose(kv.value)
		if err != nil {
			errCount++
		}
	}
	if errCount > 0 {
		return fmt.Errorf("error count(%v) on close", errCount)
	}
	return nil
}

func (c *fCache) GetRateStat() RateStat {
	return *c.recent
}

func (c *fCache) GetAllocated() int64 {
	return atomic.LoadInt64(&c.allocated)
}

func (c *fCache) GetExpiredTime(key interface{}) (time.Time, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if ent, ok := c.items[key]; ok {
		v := ent.Value.(*entry)
		return v.expiredAt, true
	}
	return time.Time{}, false
}

func (c *fCache) AddMisses() {
	atomic.AddInt32(&c.misses, 1)
}
