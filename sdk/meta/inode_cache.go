package meta

import (
	"container/list"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	// MinDirInodeCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinDirInodeCacheEvictNum = 10
	// MaxDirInodeCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxDirInodeCacheEvictNum = 200000

	DirInodeBgEvictionInterval = 2 * time.Minute
	DefaultDirInodeExpiration  = 120 * time.Second
	DefaultMaxDirInode         = 2000000
)

type DirInodeCache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

type DirInodeInfo struct {
	name string
	info *proto.InodeInfo
}

func NewDirInodeCache(exp time.Duration, maxElements int) *DirInodeCache {
	dc := &DirInodeCache{
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go dc.backgroundEviction()
	return dc
}

// Put puts the given inode info into the inode cache.
func (dc *DirInodeCache) Put(name string, info *proto.InodeInfo) {
	dc.Lock()
	old, ok := dc.cache[name]
	if ok {
		dc.lruList.Remove(old)
		delete(dc.cache, name)
	}

	if dc.lruList.Len() >= dc.maxElements {
		dc.evict(true)
	}
	info.SetExpiration(time.Now().Add(dc.expiration).UnixNano())
	element := dc.lruList.PushFront(&DirInodeInfo{info: info, name: name})
	dc.cache[name] = element
	dc.Unlock()
	// log.LogDebugf("Dcache put inode: inode(%v)", info.Inode)
}

// Get returns the inode info based on the given inode number.
func (dc *DirInodeCache) Get(name string) *proto.InodeInfo {
	dc.RLock()
	element, ok := dc.cache[name]
	if !ok {
		dc.RUnlock()
		return nil
	}

	info := element.Value.(*DirInodeInfo).info
	if time.Now().UnixNano() > info.Expiration() {
		dc.RUnlock()
		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v), expired(%d)", time.Now().Format(LogTimeFormat), info.Inode, info.Expiration())
		return nil
	}
	dc.RUnlock()
	return info
}

// Delete deletes the dentry info based on the given name(partentId+name).
func (dc *DirInodeCache) Delete(name string) {
	// log.LogDebugf("Dcache Delete: ino(%v)", ino)
	dc.Lock()
	element, ok := dc.cache[name]
	if ok {
		dc.lruList.Remove(element)
		delete(dc.cache, name)
	}
	dc.Unlock()
}

func (dc *DirInodeCache) Clear() {
	dc.Lock()
	for key, value := range dc.cache {
		dc.lruList.Remove(value)
		delete(dc.cache, key)
	}
	dc.Unlock()
}

// Foreground eviction cares more about the speed.
// Background eviction evicts all expired items from the cache.
// The caller should grab the WRITE lock of the inode cache.
func (dc *DirInodeCache) evict(foreground bool) {
	var count int

	for i := 0; i < MinDirInodeCacheEvictNum; i++ {
		element := dc.lruList.Back()
		if element == nil {
			return
		}

		// For background eviction, if all expired items have been evicted, just return
		// But for foreground eviction, we need to evict at least MinDentryCacheEvictNum inodes.
		// The foreground eviction, does not need to care if the inode has expired or not.
		dirInfo := element.Value.(*DirInodeInfo)
		if !foreground && !(time.Now().UnixNano() > dirInfo.info.Expiration()) {
			return
		}

		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), info.Inode)
		dc.lruList.Remove(element)
		delete(dc.cache, dirInfo.name)
		count++
	}

	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < MaxDirInodeCacheEvictNum; i++ {
		element := dc.lruList.Back()
		if element == nil {
			break
		}
		dirInfo := element.Value.(*DirInodeInfo)
		if !(time.Now().UnixNano() > dirInfo.info.Expiration()) {
			break
		}
		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), info.Inode)
		dc.lruList.Remove(element)
		delete(dc.cache, dirInfo.name)
		count++
	}
}

func (dc *DirInodeCache) backgroundEviction() {
	t := time.NewTicker(DirInodeBgEvictionInterval)
	defer t.Stop()

	for range t.C {
		log.LogInfof("Dcache: start BG evict")
		start := time.Now()
		dc.Lock()
		dc.evict(false)
		dc.Unlock()
		elapsed := time.Since(start)
		log.LogInfof("Dcache: total inode cache(%d), cost(%d)ns", dc.lruList.Len(), elapsed.Nanoseconds())
	}
}
