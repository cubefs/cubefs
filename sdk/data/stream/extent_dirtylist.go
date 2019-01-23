// Copyright 2018 The Container File System Authors.
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
