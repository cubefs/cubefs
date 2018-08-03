package fs

import (
	"container/list"
	"sync"
)

type OrphanInodeList struct {
	sync.RWMutex
	cache map[uint64]*list.Element
	list  *list.List
}

func NewOrphanInodeList() *OrphanInodeList {
	return &OrphanInodeList{
		cache: make(map[uint64]*list.Element),
		list:  list.New(),
	}
}

func (l *OrphanInodeList) Put(ino uint64) {
	l.Lock()
	defer l.Unlock()
	_, ok := l.cache[ino]
	if !ok {
		element := l.list.PushFront(ino)
		l.cache[ino] = element
	}
}

func (l *OrphanInodeList) Evict(ino uint64) bool {
	l.Lock()
	defer l.Unlock()
	element, ok := l.cache[ino]
	if !ok {
		return false
	}
	l.list.Remove(element)
	delete(l.cache, ino)
	return true
}
