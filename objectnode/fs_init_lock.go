// Copyright 2026 The CubeFS Authors.
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

package objectnode

import "sync"

// keyedMutex hands out one mutex per key. An entry stays in the map while
// any caller holds or waits for it, so two callers with the same key never
// get two different mutexes. The zero value is ready to use.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	sync.Mutex
	refs int
}

// lock locks the mutex of key and returns the func that unlocks it.
func (k *keyedMutex) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedMutexEntry)
	}
	e, ok := k.locks[key]
	if !ok {
		e = &keyedMutexEntry{}
		k.locks[key] = e
	}
	e.refs++
	k.mu.Unlock()

	e.Lock()
	return func() {
		e.Unlock()
		k.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
