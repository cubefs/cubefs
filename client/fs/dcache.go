package fs

import (
	"sync"
)

type DentryCache struct {
	sync.RWMutex
	cache map[string]uint64
}

func NewDentryCache() *DentryCache {
	return &DentryCache{
		cache: make(map[string]uint64),
	}
}

func (dc *DentryCache) Put(name string, ino uint64) {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	dc.cache[name] = ino
}

func (dc *DentryCache) Get(name string) (uint64, bool) {
	if dc == nil {
		return 0, false
	}

	dc.RLock()
	defer dc.RUnlock()
	ino, ok := dc.cache[name]
	return ino, ok
}

func (dc *DentryCache) Delete(name string) {
	if dc == nil {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	delete(dc.cache, name)
}
