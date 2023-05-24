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
	sync.RWMutex
	m map[string]Null
}

func NewSet() *Set {
	return &Set{
		m: map[string]Null{},
	}
}

func (s *Set) Add(val string) {
	s.Lock()
	defer s.Unlock()
	s.m[val] = Null{}
}
func (s *Set) Remove(val string) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, val)
}

func (s *Set) Has(key string) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *Set) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}

func (s *Set) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = make(map[string]Null)
}
