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

package cache_engine

import (
	"container/list"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

type LruCache interface {
	Get(key interface{}) (interface{}, error)
	Peek(key interface{}) (interface{}, bool)
	Set(key interface{}, value interface{}, expiration time.Duration) (int, error)
	Evict(key interface{}) bool
	EvictAll()
	Close() error
	Status() *Status
	HitRate() float64
	RecentEvict() int
	Len() int
}

type Status struct {
	Used int64
	Num  int
	Keys []interface{}
}

// fCache implements a non-thread safe fixed size cache.
type fCache struct {
	sync.RWMutex
	size     int
	hits     int
	misses   int
	hitRate  float64
	evictsN  int
	evicts   int
	lru      *list.List
	ttl      time.Duration
	onDelete OnDeleteF
	onClose  OnCloseF
	maxAlloc int64
	alloc    int64
	closeCh  chan bool
	items    map[interface{}]*list.Element
}

// entry in the cache.
type entry struct {
	key       interface{}
	value     interface{}
	expiresAt time.Time
}

type OnDeleteF func(v interface{}) error
type OnCloseF func(v interface{}) error

// NewCache constructs a new LruCache of the given size that is not safe for
// concurrent use. If it will be panic, if size is not a positive integer.
func NewCache(size int, maxAlloc int64, expiration time.Duration, onDelete OnDeleteF, onClose OnCloseF) LruCache {
	if size <= 0 {
		panic("must provide a positive size")
	}
	cache := &fCache{
		size:     size,
		lru:      list.New(),
		ttl:      expiration,
		hits:     1,
		misses:   0,
		hitRate:  1,
		maxAlloc: maxAlloc,
		onDelete: onDelete,
		onClose:  onClose,
		closeCh:  make(chan bool, 0),
		items:    make(map[interface{}]*list.Element),
	}
	go func() {
		tick := time.NewTicker(time.Second * 60)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				cache.hitRate = float64(cache.hits) / float64(cache.hits+cache.misses)
				cache.misses = 0
				cache.hits = 1
				cache.evicts = cache.evictsN
				cache.evictsN = 0
			case <-cache.closeCh:
				return
			}
		}
	}()
	return cache
}

func (c *fCache) Status() *Status {
	c.RLock()
	defer c.RUnlock()
	keys := make([]interface{}, 0)
	for _, i := range c.items {
		v := i.Value.(*entry)
		keys = append(keys, v.key)
	}
	return &Status{
		Used: c.alloc,
		Num:  c.lru.Len(),
		Keys: keys,
	}
}

func (c *fCache) Set(key, value interface{}, expiration time.Duration) (n int, err error) {
	c.Lock()
	if ent, ok := c.items[key]; ok {
		// update existing entry
		c.lru.MoveToFront(ent)
		v := ent.Value.(*entry)
		v.value = value
		if expiration == time.Duration(0) {
			v.expiresAt = time.Now().Add(c.ttl)
		} else {
			v.expiresAt = time.Now().Add(expiration)
		}
		c.Unlock()
		return 0, nil
	}
	// add new entry
	c.items[key] = c.lru.PushFront(&entry{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(expiration),
	})
	newCb := value.(*CacheBlock)
	c.alloc += newCb.allocSize
	// remove oldest
	if c.lru.Len() > c.size {
		ent := c.lru.Back()
		if ent != nil {
			log.LogWarnf("delete(%s) on len:%v max:%v", ent.Value.(*entry).key, c.lru.Len(), c.size)
			e := c.deleteElement(ent)
			err = c.onDelete(e)
			if err != nil {
				c.Unlock()
				return n, err
			}
			c.evictsN++
			n++
		}
	}
	for c.alloc > c.maxAlloc {
		ent := c.lru.Back()
		if ent != nil {
			log.LogWarnf("delete(%s) on allocSize:%v max:%v", ent.Value.(*entry).key, c.alloc, c.maxAlloc)
			e := c.deleteElement(ent)
			err = c.onDelete(e)
			if err != nil {
				c.Unlock()
				return n, err
			}
			c.evictsN++
			n++
		}
	}
	c.Unlock()
	return n, err
}

// Get
// todo lock top 1 at first
func (c *fCache) Get(key interface{}) (interface{}, error) {
	/*	c.RLock()
		c.lru.Front()
		c.RUnlock()*/
	c.Lock()
	if ent, ok := c.items[key]; ok {
		v := ent.Value.(*entry)
		c.hits++
		if v.expiresAt.After(time.Now()) {
			c.lru.MoveToFront(ent)
			c.Unlock()
			return v.value, nil
		}
		log.LogWarnf("delete(%s) on get", key)
		e := c.deleteElement(ent)
		c.Unlock()
		_ = c.onDelete(e)
		return nil, fmt.Errorf("expired key[%v]", key)
	}
	c.misses++
	c.Unlock()
	return nil, fmt.Errorf("key[%s] not found", key)
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *fCache) Peek(key interface{}) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()
	if ent, ok := c.items[key]; ok {
		v := ent.Value.(*entry)
		return v.value, true
	}
	return nil, false
}

// EvictAll is used to completely clear the cache.
func (c *fCache) EvictAll() {
	toEvicts := make([]interface{}, 0)
	c.Lock()
	for _, ent := range c.items {
		toEvicts = append(toEvicts, c.deleteElement(ent))
	}
	c.Unlock()
	for _, e := range toEvicts {
		_ = c.onDelete(e)
	}
	return
}

func (c *fCache) Evict(key interface{}) bool {
	c.Lock()
	if ent, ok := c.items[key]; ok {
		log.LogWarnf("delete(%s) manually", key)
		e := c.deleteElement(ent)
		c.Unlock()
		_ = c.onDelete(e)
		return true
	}
	c.Unlock()
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
	c.alloc -= cb.allocSize
}

func (c *fCache) Close() error {
	var errCount int
	close(c.closeCh)
	c.Lock()
	defer c.Unlock()
	//todo: split close action with big lock
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

func (c *fCache) HitRate() float64 {
	return c.hitRate
}

func (c *fCache) RecentEvict() int {
	return c.evicts
}
