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

package stream

import (
	"container/list"
	"sync"
)

// DirtyExtentList defines the struct of the dirty extent list.
type DirtyExtentList struct {
	sync.RWMutex
	list *list.List
}

// NewDirtyExtentList returns a new DirtyExtentList instance.
func NewDirtyExtentList() *DirtyExtentList {
	return &DirtyExtentList{
		list: list.New(),
	}
}

// Put puts a new extent handler into the dirty extent list.
func (dl *DirtyExtentList) Put(eh *ExtentHandler) {
	dl.Lock()
	defer dl.Unlock()
	dl.list.PushBack(eh)
}

// Get gets the next element in the dirty extent list.
func (dl *DirtyExtentList) Get() *list.Element {
	dl.RLock()
	defer dl.RUnlock()
	return dl.list.Front()
}

// Remove removes the element from the dirty extent list.
func (dl *DirtyExtentList) Remove(e *list.Element) {
	dl.Lock()
	defer dl.Unlock()
	dl.list.Remove(e)
}

// Len returns the size of the dirty extent list.
func (dl *DirtyExtentList) Len() int {
	dl.RLock()
	defer dl.RUnlock()
	return dl.list.Len()
}
