package fs

import (
	"container/list"
	"github.com/cubefs/cubefs/sdk/meta"
	"sync"
	"time"
)

const (
	MinSummaryCacheEvictNum    = 10
	MaxSummaryCacheEvictNum    = 200000
	SummaryBgEvictionInterval  = 2 * time.Minute
	DefaultSummaryExpiration   = 2 * time.Minute
	MaxSummaryCache            = 1000000
	DefaultDirChildrenNumLimit = 10000000
)

// SummaryCache defines the structure of the content-summary cache.
type SummaryCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

// summaryCacheElement defines the structure of the content-summary cache's element.
type summaryCacheElement struct {
	ino        uint64
	info       *meta.SummaryInfo
	expiration int64
}

// NewSummaryCache returns a new content-summary cache.
func NewSummaryCache(exp time.Duration, maxElement int) *SummaryCache {
	sc := &SummaryCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElement,
	}
	go sc.backgroundEviction()
	return sc
}

// Put puts the given summary info into the content-summary cache.
func (sc *SummaryCache) Put(inode uint64, summaryInfo *meta.SummaryInfo) {
	sc.Lock()
	old, ok := sc.cache[inode]
	if ok {
		sc.lruList.Remove(old)
		delete(sc.cache, inode)
	}
	if sc.lruList.Len() >= sc.maxElements {
		sc.evict(true)
	}
	element := sc.lruList.PushFront(&summaryCacheElement{
		ino:        inode,
		info:       summaryInfo,
		expiration: time.Now().Add(sc.expiration).UnixNano(),
	})
	sc.cache[inode] = element
	sc.Unlock()
}

// Get returns the content-summary info based on the given inode number.
func (sc *SummaryCache) Get(inode uint64) *meta.SummaryInfo {
	sc.RLock()
	element, ok := sc.cache[inode]
	if !ok {
		sc.RUnlock()
		return nil
	}
	info := element.Value.(*summaryCacheElement)
	if cacheExpired(info) {
		sc.RUnlock()
		return nil
	}
	sc.RUnlock()
	return info.info
}

// Delete deletes the content-summary info based on the given inode number.
func (sc *SummaryCache) Delete(inode uint64) {
	sc.Lock()
	element, ok := sc.cache[inode]
	if ok {
		sc.lruList.Remove(element)
		delete(sc.cache, inode)
	}
	sc.Unlock()
}

func (sc *SummaryCache) evict(foreground bool) {
	for i := 0; i < MinSummaryCacheEvictNum; i++ {
		element := sc.lruList.Back()
		if element == nil {
			return
		}
		info := element.Value.(*summaryCacheElement)
		if !foreground && !cacheExpired(info) {
			return
		}
		sc.lruList.Remove(element)
		delete(sc.cache, info.ino)
	}
	if foreground {
		return
	}

	for i := 0; i < MaxSummaryCacheEvictNum; i++ {
		element := sc.lruList.Back()
		if element == nil {
			break
		}
		info := element.Value.(*summaryCacheElement)
		if !cacheExpired(info) {
			break
		}
		sc.lruList.Remove(element)
		delete(sc.cache, info.ino)
	}
}

func (sc *SummaryCache) backgroundEviction() {
	t := time.NewTicker(SummaryBgEvictionInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			sc.Lock()
			sc.evict(false)
			sc.Unlock()
		}
	}
}

func cacheExpired(info *summaryCacheElement) bool {
	if time.Now().UnixNano() > info.expiration {
		return true
	}
	return false
}
