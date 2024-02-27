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
// permissions and limitations under the License.k

package util

import "sync"

type Null struct {
}
type Set struct {
	m map[string]Null
}
type SyncSet struct {
	sync.RWMutex
	Set
}

func NewSet() *Set {
	return &Set{
		m: make(map[string]Null),
	}
}

func NewSyncSet() *SyncSet {
	return &SyncSet{
		Set: Set{
			m: make(map[string]Null),
		},
	}
}

func (s *Set) Add(val string) {
	s.m[val] = Null{}
}

func (s *SyncSet) Add(val string) {
	s.Lock()
	defer s.Unlock()
	s.Set.Add(val)
}

func (s *Set) Remove(val string) {
	delete(s.m, val)
}

func (s *SyncSet) Remove(val string) {
	s.Lock()
	defer s.Unlock()
	s.Set.Remove(val)
}

func (s *Set) Has(key string) bool {
	_, ok := s.m[key]
	return ok
}

func (s *SyncSet) Has(key string) bool {
	s.RLock()
	defer s.RUnlock()
	return s.Set.Has(key)
}

func (s *Set) Len() int {
	return len(s.m)
}

func (s *SyncSet) Len() int {
	s.RLock()
	defer s.RUnlock()
	return s.Set.Len()
}

func (s *Set) Clear() {
	s.m = make(map[string]Null)
}

func (s *SyncSet) Clear() {
	s.Lock()
	defer s.Unlock()
	s.Set.Clear()
}

func (s *Set) Range(fun func(key interface{}) bool) {
	for k := range s.m {
		if !fun(k) {
			return
		}
	}
}

func (s *SyncSet) Range(fun func(key interface{}) bool) {
	s.Lock()
	defer s.Unlock()
	s.Set.Range(fun)
}
