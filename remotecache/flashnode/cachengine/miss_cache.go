package cachengine

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	// MinMissCacheEvictNum is used in the foreground eviction.
	// When clearing the miss keys from the cache, it stops as soon as 10 inodes have been evicted.
	MinMissCacheEvictNum = 10
	// MaxMissCacheEvictNum is used in the background. We can evict 200000 inodes at max.
	MaxMissCacheEvictNum = 100000

	BgEvictionInterval = 1 * time.Minute
)

// MissCache defines the structure of the missed key cache.
type MissCache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

func NewMissCache(exp time.Duration, maxElements int) *MissCache {
	mc := &MissCache{
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go mc.backgroundEviction()
	return mc
}

func (mc *MissCache) Increment(key string) int32 {
	mc.Lock()
	defer mc.Unlock()
	e, ok := mc.cache[key]
	if !ok {
		cme := &proto.CacheMissEntry{
			UniKey:    key,
			MissCount: 1,
		}
		if mc.lruList.Len() >= mc.maxElements {
			mc.evict(true)
		}
		proto.SetMissEntryExpiration(cme, mc.expiration)
		element := mc.lruList.PushFront(cme)
		mc.cache[cme.UniKey] = element
		if log.EnableDebug() {
			log.LogDebugf("MissCache put miss cache: unikey(%v) expire(%v)",
				cme.UniKey, proto.GetMissEntryExpiration(cme))
		}
		return 1
	}

	info := e.Value.(*proto.CacheMissEntry)
	if proto.CacheMissExpired(info) {
		mc.lruList.Remove(e)
		delete(mc.cache, info.UniKey)
		cme := &proto.CacheMissEntry{
			UniKey:    key,
			MissCount: 1,
		}
		if mc.lruList.Len() >= mc.maxElements {
			mc.evict(true)
		}
		proto.SetMissEntryExpiration(cme, mc.expiration)

		element := mc.lruList.PushFront(cme)
		mc.cache[cme.UniKey] = element
		return 1
	}
	return AtomicLoadAndAddWithCAS(&info.MissCount)
}

func AtomicLoadAndAddWithCAS(addr *int32) int32 {
	for {
		old := atomic.LoadInt32(addr)
		newVal := old + 1
		if atomic.CompareAndSwapInt32(addr, old, newVal) {
			return newVal
		}
	}
}

func (mc *MissCache) Get(uniKey string) *proto.CacheMissEntry {
	mc.RLock()
	element, ok := mc.cache[uniKey]
	if !ok {
		mc.RUnlock()
		log.LogDebugf("Miss Cache not found %v", uniKey)
		return nil
	}

	info := element.Value.(*proto.CacheMissEntry)
	if proto.CacheMissExpired(info) {
		mc.RUnlock()
		log.LogDebugf("Miss Cache %v expired", info)
		return nil
	}
	mc.RUnlock()
	return info
}

func (mc *MissCache) backgroundEviction() {
	t := time.NewTicker(BgEvictionInterval)
	defer t.Stop()

	for range t.C {
		log.LogInfof("MissCache: start BG evict")
		start := time.Now()
		mc.Lock()
		mc.evict(false)
		mc.Unlock()
		elapsed := time.Since(start)
		log.LogInfof("MissCache: total miss cache(%d), cost(%d)ns", mc.lruList.Len(), elapsed.Nanoseconds())
	}
}

func (mc *MissCache) Delete(uniKey string) {
	log.LogDebugf("MissCache Delete: key(%v)", uniKey)
	mc.Lock()
	element, ok := mc.cache[uniKey]
	if ok {
		mc.lruList.Remove(element)
		delete(mc.cache, uniKey)
	}
	mc.Unlock()
}

// Foreground eviction cares more about the speed.
// Background eviction evicts all expired items from the cache.
// The caller should grab the WRITE lock of the inode cache.
func (mc *MissCache) evict(foreground bool) {
	var count int

	for i := 0; i < MinMissCacheEvictNum; i++ {
		element := mc.lruList.Back()
		if element == nil {
			return
		}

		// For background eviction, if all expired items have been evicted, just return
		// But for foreground eviction, we need to evict at least MinInodeCacheEvictNum inodes.
		// The foreground eviction, does not need to care if the inode has expired or not.
		info := element.Value.(*proto.CacheMissEntry)
		log.LogDebugf("MissCache check miss key(%v)", info.UniKey)
		if !foreground && !proto.CacheMissExpired(info) {
			log.LogDebugf("MissCache check key(%v) expired(%v)",
				info.UniKey, proto.CacheMissExpired(info))
			return
		}

		log.LogDebugf("MissCache remove key(%v)", info.UniKey)
		mc.lruList.Remove(element)
		delete(mc.cache, info.UniKey)
		count++
	}
	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < MaxMissCacheEvictNum; i++ {
		element := mc.lruList.Back()
		if element == nil {
			break
		}
		info := element.Value.(*proto.CacheMissEntry)
		if !proto.CacheMissExpired(info) {
			log.LogDebugf("MissCache check key(%v) expired(%v)",
				info.UniKey, proto.CacheMissExpired(info))
			break
		}
		log.LogDebugf("MissCache remove key(%v)", info.UniKey)
		mc.lruList.Remove(element)
		delete(mc.cache, info.UniKey)
		count++
	}
}
