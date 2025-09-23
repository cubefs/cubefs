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

// freeList represents a thread-safe queue for managing free inode numbers.
// It maintains both a linked list for FIFO operations and a map for O(1) lookups.
type freeList struct {
	mu    sync.RWMutex // Use RWMutex for better read performance
	list  *list.List
	index map[uint64]*list.Element
}

// newFreeList creates a new instance of freeList.
func newFreeList() *freeList {
	return &freeList{
		list:  list.New(),
		index: make(map[uint64]*list.Element),
	}
}

// Pop removes and returns the first item from the list.
// Returns 0 if the list is empty.
func (fl *freeList) Pop() uint64 {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	item := fl.list.Front()
	if item == nil {
		return 0
	}

	val := fl.list.Remove(item)
	ino := val.(uint64)
	delete(fl.index, ino)
	return ino
}

// Push adds a new inode number to the back of the list.
// If the inode already exists, it will be ignored.
func (fl *freeList) Push(ino uint64) {
	if ino == 0 {
		return // Avoid storing invalid inode numbers
	}

	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Check if inode already exists to avoid duplicates
	if _, exists := fl.index[ino]; !exists {
		item := fl.list.PushBack(ino)
		fl.index[ino] = item
	}
}

// Remove removes a specific inode number from the list.
func (fl *freeList) Remove(ino uint64) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	if item, exists := fl.index[ino]; exists {
		fl.list.Remove(item)
		delete(fl.index, ino)
	}
}

// Len returns the current number of items in the list.
func (fl *freeList) Len() int {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	return len(fl.index)
}
