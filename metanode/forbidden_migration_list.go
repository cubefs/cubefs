package metanode

import (
	"container/list"
	"sync"
	"time"
)

const (
	BgEvictionInterval = 2 * time.Minute
)

type forbiddenMigrationList struct {
	sync.RWMutex
	index      map[uint64]*list.Element
	list       *list.List
	expiration time.Duration
}

type forbiddenInodeInfo struct {
	ino        uint64
	expiration int64
}

func newForbiddenMigrationList(exp time.Duration) *forbiddenMigrationList {
	fmList := &forbiddenMigrationList{
		index:      make(map[uint64]*list.Element),
		list:       list.New(),
		expiration: exp,
	}
	return fmList
}

func (fmList *forbiddenMigrationList) Put(ino uint64) {
	fmList.Lock()
	old, ok := fmList.index[ino]
	if ok {
		fmList.list.Remove(old)
		delete(fmList.index, ino)
	}
	info := &forbiddenInodeInfo{ino: ino,
		expiration: time.Now().Add(fmList.expiration).UnixNano()}
	element := fmList.list.PushFront(info)
	fmList.index[ino] = element
	fmList.Unlock()
}

func (fmList *forbiddenMigrationList) Delete(ino uint64) {
	fmList.Lock()
	element, ok := fmList.index[ino]
	if ok {
		fmList.list.Remove(element)
		delete(fmList.index, ino)
	}
	fmList.Unlock()
}

func (fmList *forbiddenMigrationList) getExpiredForbiddenMigrationInodes() []uint64 {
	fmList.Lock()
	defer fmList.Unlock()
	var expiredInos []uint64
	currentTime := time.Now().UnixNano()
	for e := fmList.list.Back(); e != nil; e = e.Prev() {
		info := e.Value.(*forbiddenInodeInfo)
		//the first one that has not expired
		if info.expiration > currentTime {
			return expiredInos
		}
		//reset
		expiredInos = append(expiredInos, info.ino)
		fmList.list.Remove(e)
		delete(fmList.index, info.ino)
	}
	return expiredInos
}

func (fmList *forbiddenMigrationList) getAllForbiddenMigrationInodes() []uint64 {
	fmList.Lock()
	defer fmList.Unlock()
	var allInos []uint64
	for e := fmList.list.Back(); e != nil; e = e.Prev() {
		info := e.Value.(*forbiddenInodeInfo)
		allInos = append(allInos, info.ino)
	}
	return allInos
}
