// Copyright 2018 The Chubao Authors.
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

package fs

import (
	"container/list"
	"sync"
)

// OrphanInodeList defines the orphan inode list, which is a list of orphan inodes.
// An orphan inode is the inode whose nlink value is 0.
type OrphanInodeList struct {
	sync.RWMutex
	cache map[uint64]*list.Element
	list  *list.List
}

// NewOrphanInodeList returns a new orphan inode list.
func NewOrphanInodeList() *OrphanInodeList {
	return &OrphanInodeList{
		cache: make(map[uint64]*list.Element),
		list:  list.New(),
	}
}

// Put puts an inode into the orphan inode list.
func (l *OrphanInodeList) Put(ino uint64) {
	l.Lock()
	defer l.Unlock()
	_, ok := l.cache[ino]
	if !ok {
		element := l.list.PushFront(ino)
		l.cache[ino] = element
	}
}

// Evict remove the given inode from the orphan inode list, and evicts it.
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
