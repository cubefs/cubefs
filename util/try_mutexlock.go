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
	"time"
)

type TryMutexLock struct {
	sync.Once
	lockChan chan struct{}
}

func (lock *TryMutexLock) initlock() {
	lock.lockChan = make(chan struct{}, 1)
	lock.lockChan <- struct{}{}
}

func (lock *TryMutexLock) Lock() {
	lock.Do(lock.initlock)

	<-lock.lockChan
}

func (lock *TryMutexLock) TryLock() bool {
	lock.Do(lock.initlock)
	select {
	case <-lock.lockChan:
		return true
	default:
		return false
	}
}

func (lock *TryMutexLock) TryLockTimed(t time.Duration) bool {
	lock.Do(lock.initlock)

	select {
	case <-lock.lockChan:
		return true
	case <-time.After(t):
		return false
	}
}

func (lock *TryMutexLock) Unlock() {
	lock.Do(lock.initlock)

	select {
	case lock.lockChan <- struct{}{}:
		// Success - do nothing.
	default:
		panic("TryMutexLock is not locked")
	}
}
