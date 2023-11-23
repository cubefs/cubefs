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
package meta

import (
	"container/list"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	MinQuotaCacheEvictNum = 10
)

type QuotaCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

type QuotaCacheInfo struct {
	quotaInfos map[uint32]*proto.MetaQuotaInfo
	expiration int64
	inode      uint64
}

func NewQuotaCache(exp time.Duration, maxElements int) *QuotaCache {
	qc := &QuotaCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go qc.backgroundEviction()
	return qc
}

func (qc *QuotaCache) Put(ino uint64, qinfo *QuotaCacheInfo) {
	qc.Lock()
	defer qc.Unlock()
	old, ok := qc.cache[ino]
	if ok {
		qc.lruList.Remove(old)
		delete(qc.cache, ino)
	}

	if qc.lruList.Len() >= qc.maxElements {
		qc.evict(true)
	}
	qinfo.quotaSetExpiration(qc.expiration)
	element := qc.lruList.PushFront(qinfo)
	qc.cache[ino] = element
}

func (qc *QuotaCache) Get(ino uint64) *QuotaCacheInfo {
	qc.RLock()
	defer qc.RUnlock()
	element, ok := qc.cache[ino]
	if !ok {
		return nil
	}

	info := element.Value.(*QuotaCacheInfo)
	if info.quotaExpired() {
		return nil
	}
	return info
}

func (qc *QuotaCache) Delete(ino uint64) {
	qc.Lock()
	defer qc.Unlock()
	element, ok := qc.cache[ino]
	if ok {
		qc.lruList.Remove(element)
		delete(qc.cache, ino)
	}
}

func (qc *QuotaCache) evict(foreground bool) {
	for i := 0; i < MinQuotaCacheEvictNum; i++ {
		element := qc.lruList.Back()
		if element == nil {
			return
		}

		info := element.Value.(*QuotaCacheInfo)
		if !foreground && !info.quotaExpired() {
			return
		}

		qc.lruList.Remove(element)
		delete(qc.cache, info.inode)
	}

	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < qc.maxElements; i++ {
		element := qc.lruList.Back()
		if element == nil {
			break
		}
		info := element.Value.(*QuotaCacheInfo)
		if !info.quotaExpired() {
			break
		}
		qc.lruList.Remove(element)
		delete(qc.cache, info.inode)
	}
}

func (qc *QuotaCache) backgroundEviction() {
	t := time.NewTicker(qc.expiration)
	defer t.Stop()

	for range t.C {
		log.LogInfof("QuotaCache: start BG evict")
		qc.Lock()
		qc.evict(false)
		qc.Unlock()
		log.LogInfof("QuotaCache: end BG evict")
	}
}

func (qinfo *QuotaCacheInfo) quotaSetExpiration(expiration time.Duration) {
	qinfo.expiration = time.Now().Add(expiration).UnixNano()
}

func (qinfo *QuotaCacheInfo) quotaExpired() bool {
	return time.Now().UnixNano() > qinfo.expiration
}
