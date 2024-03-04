package metanode

import (
	"container/list"
	"github.com/cubefs/cubefs/util/log"
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
	expiration := time.Now().Add(fmList.expiration).Unix()
	info := &forbiddenInodeInfo{ino: ino,
		expiration: expiration}
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

func (fmList *forbiddenMigrationList) getExpiredForbiddenMigrationInodes(id uint64) []uint64 {
	fmList.Lock()
	defer fmList.Unlock()
	var expiredInos []uint64
	currentTime := time.Now().Unix()
	log.LogDebugf("[getExpiredForbiddenMigrationInodes] mp(%v) len(%v)", id, fmList.list.Len())
	for e := fmList.list.Back(); e != nil; {
		info := e.Value.(*forbiddenInodeInfo)
		//the first one that has not expired
		if info.expiration > currentTime {
			log.LogDebugf("[getExpiredForbiddenMigrationInodes] mp(%v) ino %v is not expired:%v", id, info.ino, info.expiration)
			return expiredInos
		}
		//reset
		expiredInos = append(expiredInos, info.ino)
		next := e.Prev()
		fmList.list.Remove(e)
		delete(fmList.index, info.ino)
		log.LogDebugf("[getExpiredForbiddenMigrationInodes] mp(%v) remove expired ino %v[%v]", id, info.ino, info.expiration)
		e = next
	}
	return expiredInos
}

func (fmList *forbiddenMigrationList) getAllForbiddenMigrationInodes(mpId uint64) []uint64 {
	fmList.RLock()
	defer fmList.RUnlock()
	var allInos []uint64
	log.LogDebugf("[getAllForbiddenMigrationInodes] mp %v len %v:", mpId, fmList.list.Len())
	for e := fmList.list.Back(); e != nil; e = e.Prev() {
		if info, ok := e.Value.(*forbiddenInodeInfo); ok {
			allInos = append(allInos, info.ino)
		} else {
			log.LogWarnf("[getAllForbiddenMigrationInodes] mp %v value %v", mpId, e.Value)
		}

	}
	return allInos
}
