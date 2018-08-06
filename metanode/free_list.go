package metanode

import (
	"container/list"
	"sync"
)

type freeList struct {
	sync.RWMutex
	list *list.List
}

func newFreeList() *freeList {
	return &freeList{
		list: list.New(),
	}
}

// Pop get the first item of list and delete it from list
// if list is empty, return nil
func (i *freeList) Pop() (ino *Inode) {
	i.Lock()
	defer i.Unlock()
	item := i.list.Front()
	if item == nil {
		return
	}
	val := i.list.Remove(item)
	ino = val.(*Inode)
	return
}

// Push inserts a new item at the back of list
func (i *freeList) Push(ino *Inode) {
	i.Lock()
	defer i.Unlock()
	i.list.PushBack(ino)
}

// Only get the first item of list, don't delete item
// if list is empty, return nil
func (i *freeList) GetFront() (ino *Inode) {
	i.Lock()
	defer i.Unlock()
	item := i.list.Front()
	if item == nil {
		return
	}
	ino = item.Value.(*Inode)
	return
}

// Move Front item to the back of list
func (i *freeList) FrontMoveToBack() {
	i.Lock()
	defer i.Unlock()
	item := i.list.Front()
	if item == nil {
		return
	}
	i.list.MoveToBack(item)
}
