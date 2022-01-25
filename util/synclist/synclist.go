// Copyright 2018 The Cubefs Authors.
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

package synclist

import (
	"container/list"
	"sync"
)

type SyncList struct {
	list.List
	mu sync.RWMutex
}

func New() *SyncList {
	l := new(SyncList)
	l.Init()
	return l
}

func (l *SyncList) Init() *SyncList {
	l.mu.Lock()
	l.List.Init()
	l.mu.Unlock()
	return l
}

func (l *SyncList) Remove(e *list.Element) interface{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.List.Remove(e)
}

func (l *SyncList) PushFront(v interface{}) *list.Element {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.List.PushFront(v)
}

func (l *SyncList) Back() *list.Element {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.List.Back()
}

func (l *SyncList) PushBack(v interface{}) *list.Element {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.List.PushBack(v)
}

func (l *SyncList) InsertBefore(v interface{}, mark *list.Element) *list.Element {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.List.InsertBefore(v, mark)
}

func (l *SyncList) InsertAfter(v interface{}, mark *list.Element) *list.Element {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.List.InsertAfter(v, mark)
}

func (l *SyncList) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.List.Len()
}

func (l *SyncList) Front() *list.Element {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.List.Front()
}

func (l *SyncList) MoveToFront(e *list.Element) {
	l.mu.Lock()
	l.List.MoveToFront(e)
	l.mu.Unlock()
}

func (l *SyncList) MoveToBack(e *list.Element) {
	l.mu.Lock()
	l.List.MoveToBack(e)
	l.mu.Unlock()
}

func (l *SyncList) MoveBefore(e, mark *list.Element) {
	l.mu.Lock()
	l.List.MoveBefore(e, mark)
	l.mu.Unlock()
}

func (l *SyncList) MoveAfter(e, mark *list.Element) {
	l.mu.Lock()
	l.List.MoveAfter(e, mark)
	l.mu.Unlock()
}

func (l *SyncList) PushBackList(other *SyncList) {
	l.mu.Lock()
	l.List.PushBackList(&other.List)
	l.mu.Unlock()
}

func (l *SyncList) PushFrontList(other *SyncList) {
	l.mu.Lock()
	l.List.PushFrontList(&other.List)
	l.mu.Unlock()
}
