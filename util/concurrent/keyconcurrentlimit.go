// Copyright 2023 The CubeFS Authors.
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

package concurrent

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrLimit = errors.New("limit exceeded")

type KeyConcurrentLimit struct {
	mutex   sync.RWMutex
	current map[string]*int64 // uid --> concurrency
}

func NewLimit() *KeyConcurrentLimit {
	return &KeyConcurrentLimit{current: make(map[string]*int64)}
}

func (s *KeyConcurrentLimit) Acquire(key string, limit int64) error {
	s.mutex.RLock()
	count := s.current[key]
	s.mutex.RUnlock()
	if count == nil {
		s.mutex.Lock()
		count = s.current[key]
		if count == nil {
			count = new(int64)
			s.current[key] = count
		}
		s.mutex.Unlock()
	}
	newVal := atomic.AddInt64(count, 1)
	if newVal > limit && limit != 0 {
		atomic.AddInt64(count, -1)
		return ErrLimit
	}
	return nil
}

func (s *KeyConcurrentLimit) Release(key string) {
	s.mutex.RLock()
	count, ok := s.current[key]
	s.mutex.RUnlock()
	if !ok {
		return
	}
	val := atomic.AddInt64(count, -1)
	if val < 0 {
		atomic.StoreInt64(count, 0)
	}
}

func (s *KeyConcurrentLimit) Running() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var all int64
	for _, count := range s.current {
		all += atomic.LoadInt64(count)
	}
	return int(all)
}

func (s *KeyConcurrentLimit) Get(key string) int64 {
	s.mutex.RLock()
	count, ok := s.current[key]
	s.mutex.RUnlock()
	if !ok {
		return 0
	}
	return atomic.LoadInt64(count)
}
