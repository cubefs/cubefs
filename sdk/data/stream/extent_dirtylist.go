package stream

import (
	"container/list"
	"sync"
)

type ExtentDirtyList struct {
	sync.RWMutex
	list *list.List
}

func NewExtentDirtyList() *ExtentDirtyList {
	return &ExtentDirtyList{
		list: list.New(),
	}
}

func (dl *ExtentDirtyList) Put(eh *ExtentHandler) {
	dl.Lock()
	defer dl.Unlock()
	dl.list.PushBack(eh)
}

func (dl *ExtentDirtyList) Get() *list.Element {
	dl.RLock()
	defer dl.RUnlock()
	return dl.list.Front()
}

func (dl *ExtentDirtyList) Remove(e *list.Element) {
	dl.Lock()
	defer dl.Unlock()
	dl.list.Remove(e)
}

func (dl *ExtentDirtyList) Len() int {
	dl.RLock()
	defer dl.RUnlock()
	return dl.list.Len()
}
