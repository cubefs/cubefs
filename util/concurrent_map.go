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

type ConcurrentMap struct {
	sync.RWMutex
	m map[string]struct{}
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		m: make(map[string]struct{}),
	}
}

func (s *ConcurrentMap) Add(key string) {
	s.Lock()
	defer s.Unlock()
	s.m[key] = struct{}{}
}

func (s *ConcurrentMap) Remove(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, key)
}

func (s *ConcurrentMap) Contains(key string) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *ConcurrentMap) List() []string {
	list := make([]string, 0)
	s.RLock()
	defer s.RUnlock()
	for key := range s.m {
		list = append(list, key)
	}
	return list
}

func (s *ConcurrentMap) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}
