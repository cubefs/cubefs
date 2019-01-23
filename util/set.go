// Copyright 2018 The Containerfs Authors.
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

package util

import (
	"sync"
)

type Set struct {
	sync.RWMutex
	m map[int]struct{}
}

func NewSet() *Set {
	return &Set{
		m: make(map[int]struct{}),
	}
}

func (s *Set) Add(key int) {
	s.Lock()
	defer s.Unlock()
	s.m[key] = struct{}{}
}

func (s *Set) Remove(key int) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, key)
}

func (s *Set) Has(key int) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *Set) List() []int {
	list := make([]int, 0)
	s.RLock()
	defer s.RUnlock()
	for key := range s.m {
		list = append(list, key)
	}
	return list
}

func (s *Set) RemoveAll() {
	s.Lock()
	defer s.Unlock()
	s.m = make(map[int]struct{})
}

func (s *Set) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}
