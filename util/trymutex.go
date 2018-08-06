// Copyright 2018 The ChuBao Authors.
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
	"time"
)

type TryMutex struct {
	once sync.Once
	lock chan struct{}
}

func (tm *TryMutex) init() {
	tm.lock = make(chan struct{}, 1)
	tm.lock <- struct{}{}
}

func (tm *TryMutex) Lock() {
	tm.once.Do(tm.init)

	<-tm.lock
}

func (tm *TryMutex) TryLock() bool {
	tm.once.Do(tm.init)
	select {
	case <-tm.lock:
		return true
	default:
		return false
	}
}

func (tm *TryMutex) TryLockTimed(t time.Duration) bool {
	tm.once.Do(tm.init)

	select {
	case <-tm.lock:
		return true
	case <-time.After(t):
		return false
	}
}

func (tm *TryMutex) Unlock() {
	tm.once.Do(tm.init)

	select {
	case tm.lock <- struct{}{}:
		// Success - do nothing.
	default:
		panic("unlock called when TryMutex is not locked")
	}
}
