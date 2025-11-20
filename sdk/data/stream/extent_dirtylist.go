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
	"runtime/debug"
	"sync"

	"github.com/cubefs/cubefs/util/log"
)

// DirtyExtentList defines the struct of the dirty extent list.
type DirtyExtentList struct {
	sync.RWMutex
	list  *list.List
	index sync.Map // key: handler.id (uint64) -> struct{}
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
	if _, loaded := dl.index.LoadOrStore(eh.id, struct{}{}); loaded {
		return
	}
	dl.list.PushBack(eh)
	if log.EnableDebug() {
		log.LogDebugf("DirtyExtentList: put handler(%v) to dirtyList trace(%v)", eh, string(debug.Stack()))
	}
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
	if e != nil {
		if eh, ok := e.Value.(*ExtentHandler); ok {
			dl.index.Delete(eh.id)
			if log.EnableDebug() {
				log.LogDebugf("DirtyExtentList: remove handler(%v) to dirtyList trace(%v)", eh, string(debug.Stack()))
			}
		}
	}
	dl.list.Remove(e)
}

// Len returns the size of the dirty extent list.
func (dl *DirtyExtentList) Len() int {
	dl.RLock()
	defer dl.RUnlock()
	return dl.list.Len()
}
