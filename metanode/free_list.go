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

package metanode

import (
	"container/list"
	"sync"
)

type freeList struct {
	sync.Mutex
	list  *list.List
	index map[uint64]*list.Element
}

func newFreeList() *freeList {
	return &freeList{
		list:  list.New(),
		index: make(map[uint64]*list.Element),
	}
}

// Pop removes the first item on the list and returns it.
func (fl *freeList) Pop() (ino uint64) {
	fl.Lock()
	defer fl.Unlock()
	item := fl.list.Front()
	if item == nil {
		return
	}
	val := fl.list.Remove(item)
	ino = val.(uint64)
	delete(fl.index, ino)
	return
}

// Push inserts a new item at the back of the list.
func (fl *freeList) Push(ino uint64) {
	fl.Lock()
	defer fl.Unlock()
	if _, ok := fl.index[ino]; !ok {
		item := fl.list.PushBack(ino)
		fl.index[ino] = item
	}
}

func (fl *freeList) Remove(ino uint64) {
	fl.Lock()
	defer fl.Unlock()
	if item, ok := fl.index[ino]; ok {
		fl.list.Remove(item)
		delete(fl.index, ino)
	}
}

func (fl *freeList) Len() int {
	fl.Lock()
	defer fl.Unlock()
	return len(fl.index)
}

func (fl *freeList) PushFront(ino uint64) {
	fl.Lock()
	defer fl.Unlock()
	//remove
	item, ok := fl.index[ino]
	if ok {
		fl.list.Remove(item)
	}
	item = fl.list.PushFront(ino)
	fl.index[ino] = item
}

func (fl *freeList) Get(count int) (inos []uint64) {
	if count <= 0 {
		return
	}
	fl.Lock()
	defer fl.Unlock()

	if len(fl.index) < count {
		count = len(fl.index)
	}
	inos = make([]uint64, 0, count)
	for count > 0 {
		item := fl.list.Front()
		if item == nil {
			break
		}
		val := fl.list.Remove(item)
		ino := val.(uint64)
		element := fl.list.PushBack(ino)
		fl.index[ino] = element
		inos = append(inos, ino)
		count--
	}
	return
}

func (fl *freeList) Range(f func(ino uint64) bool) {
	fl.Lock()
	defer fl.Unlock()
	for ino := range fl.index {
		if ok := f(ino); !ok {
			break
		}
	}
}