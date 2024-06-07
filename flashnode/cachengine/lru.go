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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type LruCache interface {
	Get(key interface{}) (interface{}, error)
	Peek(key interface{}) (interface{}, bool)
	Set(key interface{}, value interface{}, expiration time.Duration) (int, error)
	Evict(key interface{}) bool
	EvictAll()
	Close() error
	Status() *Status
	Len() int
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
	capacity  int
	maxSize   int64
	allocated int64

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
	expiredAt time.Time
}

type (
	OnDeleteF func(v interface{}) error
	OnCloseF  func(v interface{}) error
)

// NewCache constructs a new LruCache of the given size that is not safe for
// concurrent use. If it will be panic, if size is not a positive integer.
func NewCache(capacity int, maxSize int64, ttl time.Duration, onDelete OnDeleteF, onClose OnCloseF) LruCache {
	if capacity <= 0 {
		panic("must provide a positive capacity")
	}
	c := &fCache{
		capacity: capacity,
		maxSize:  maxSize,
		ttl:      ttl,
		lru:      list.New(),
		hits:     1,
		recent:   &RateStat{},
		onDelete: onDelete,
		onClose:  onClose,
		closeCh:  make(chan struct{}),
		items:    make(map[interface{}]*list.Element),
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

func (c *fCache) Set(key, value interface{}, expiration time.Duration) (n int, err error) {
	if expiration == 0 {
		expiration = c.ttl
	}
	c.lock.Lock()
	if ent, ok := c.items[key]; ok {
		c.lru.MoveToFront(ent)
		v := ent.Value.(*entry)
		v.value = value
		v.expiredAt = time.Now().Add(expiration)
		c.lock.Unlock()
		return 0, nil
	}

	c.items[key] = c.lru.PushFront(&entry{
		key:       key,
		value:     value,
		expiredAt: time.Now().Add(expiration),
	})
	newCb := value.(*CacheBlock)
	atomic.AddInt64(&c.allocated, newCb.getAllocSize())

	toEvicts := make(map[interface{}]interface{})
	for c.lru.Len() > c.capacity || atomic.LoadInt64(&c.allocated) > c.maxSize {
		ent := c.lru.Back()
		if ent != nil {
			toEvicts[ent.Value.(*entry).key] = c.deleteElement(ent)
			atomic.AddInt32(&c.evicts, 1)
			n++
		}
	}
	c.lock.Unlock()
	for k, e := range toEvicts {
		_ = c.onDelete(e)
		log.LogInfof("delete(%s) cos full, len(%d) size(%d / %d)", k, c.lru.Len(), atomic.LoadInt64(&c.allocated), c.maxSize)
	}
	return n, nil
}

func (c *fCache) Get(key interface{}) (interface{}, error) {
	c.lock.Lock()
	if ent, ok := c.items[key]; ok {
		v := ent.Value.(*entry)
		atomic.AddInt32(&c.hits, 1)
		if v.expiredAt.After(time.Now()) {
			c.lru.MoveToFront(ent)
			c.lock.Unlock()
			return v.value, nil
		}
		log.LogInfof("delete(%s) on get", key)
		e := c.deleteElement(ent)
		c.lock.Unlock()
		_ = c.onDelete(e)
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
func (c *fCache) EvictAll() {
	c.lock.Lock()
	toEvicts := make([]interface{}, 0, len(c.items))
	for _, ent := range c.items {
		toEvicts = append(toEvicts, c.deleteElement(ent))
	}
	c.lock.Unlock()
	for _, e := range toEvicts {
		_ = c.onDelete(e)
	}
}

func (c *fCache) Evict(key interface{}) bool {
	c.lock.Lock()
	if ent, ok := c.items[key]; ok {
		log.LogInfof("delete(%s) manually", key)
		e := c.deleteElement(ent)
		c.lock.Unlock()
		_ = c.onDelete(e)
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
	cb := kv.value.(*CacheBlock)
	atomic.AddInt64(&c.allocated, -cb.getAllocSize())
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
