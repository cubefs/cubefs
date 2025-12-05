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
// The design ensures:
// - O(1) insertion and removal from both ends
// - O(1) duplicate detection
// - O(1) arbitrary element removal
// - Thread-safe operations
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
// Returns 0 if the list is empty (0 is considered an invalid inode number).
func (fl *freeList) Pop() uint64 {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	item := fl.list.Front()
	if item == nil {
		return 0
	}

	val := fl.list.Remove(item)
	ino, ok := val.(uint64)
	if !ok {
		// This should never happen if the list is used correctly
		// Log error but don't panic to maintain stability
		return 0
	}
	delete(fl.index, ino)
	return ino
}

// Push adds a new inode number to the back of the list.
// If the inode already exists, it will be ignored (idempotent operation).
// Invalid inode numbers (0) are rejected.
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
// If the inode does not exist, the operation is a no-op.
func (fl *freeList) Remove(ino uint64) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	if item, exists := fl.index[ino]; exists {
		fl.list.Remove(item)
		delete(fl.index, ino)
	}
}

// Len returns the current number of items in the list.
// This operation is O(1) and thread-safe.
func (fl *freeList) Len() int {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	// Use index length for consistency and O(1) performance
	// Both should be equal, but index is more reliable
	return len(fl.index)
}
