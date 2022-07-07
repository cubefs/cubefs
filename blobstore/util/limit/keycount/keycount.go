// Copyright 2022 The CubeFS Authors.
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

package keycount

import (
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/limit"
)

type keyCountLimit struct {
	mutex   sync.Mutex
	limit   uint32
	current map[interface{}]uint32
}

// New returns limiter with concurrent n by everyone key
func New(n int) limit.ResettableLimiter {
	return &keyCountLimit{
		limit:   uint32(n),
		current: make(map[interface{}]uint32),
	}
}

func (l *keyCountLimit) Running() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	all := uint32(0)
	for _, v := range l.current {
		all += v
	}
	return int(all)
}

func (l *keyCountLimit) Acquire(keys ...interface{}) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, key := range keys {
		n := l.current[key]
		if n >= l.limit {
			return limit.ErrLimited
		}
	}
	for _, key := range keys {
		l.current[key]++
	}

	return nil
}

func (l *keyCountLimit) Release(keys ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	for _, key := range keys {
		n, ok := l.current[key]
		if !ok || n == 0 {
			panic("released by 0")
		}
		if n == 1 {
			delete(l.current, key)
		} else {
			l.current[key]--
		}
	}
}

func (l *keyCountLimit) Reset(n int) {
	l.mutex.Lock()
	l.limit = uint32(n)
	l.mutex.Unlock()
}

type blocker struct {
	ref   int32
	ready chan struct{}
}

func newBlocker(n int) *blocker {
	s := &blocker{
		ref:   0,
		ready: make(chan struct{}, n),
	}
	for i := 0; i < n; i++ {
		s.ready <- struct{}{}
	}
	return s
}

func (s *blocker) acquire() {
	<-s.ready
}

func (s *blocker) release() {
	s.ready <- struct{}{}
}

func (s *blocker) addRef() {
	s.ref++
}

func (s *blocker) subRef() int32 {
	s.ref--
	return s.ref
}

type blockingKeyCountLimit struct {
	lock   sync.RWMutex
	limit  int
	keyMap map[interface{}]*blocker
}

// NewBlockingKeyCountLimit returns blocking limiter
//     with concurrent n by everyone key
func NewBlockingKeyCountLimit(n int) limit.Limiter {
	return &blockingKeyCountLimit{
		limit:  n,
		keyMap: make(map[interface{}]*blocker),
	}
}

func (l *blockingKeyCountLimit) Running() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	all := 0
	for _, v := range l.keyMap {
		all += l.limit - len(v.ready)
	}
	return all
}

func (l *blockingKeyCountLimit) Acquire(keys ...interface{}) error {
	if len(keys) == 0 {
		return limit.ErrLimited
	}
	kls := make([]*blocker, 0, len(keys))
	l.lock.Lock()
	for _, key := range keys {
		kl, ok := l.keyMap[key]
		if !ok {
			kl = newBlocker(l.limit)
			l.keyMap[key] = kl
		}
		kl.addRef()
		kls = append(kls, kl)
	}
	l.lock.Unlock()
	for _, kl := range kls {
		kl.acquire()
	}
	return nil
}

func (l *blockingKeyCountLimit) Release(keys ...interface{}) {
	kls := make([]*blocker, 0, len(keys))
	l.lock.Lock()
	for _, key := range keys {
		kl, ok := l.keyMap[key]
		if !ok {
			l.lock.Unlock()
			panic("key not in map. Possible reason: Release without Acquire.")
		}
		ref := kl.subRef()
		if ref < 0 {
			l.lock.Unlock()
			panic("internal error: refs < 0")
		}
		if ref == 0 {
			delete(l.keyMap, key)
		}
		kls = append(kls, kl)
	}
	l.lock.Unlock()
	for _, kl := range kls {
		kl.release()
	}
}
