// Copyright 2018 The Containerfs Authors.
// Copyright 2018 The Sia Authors
//from https://github.com/NebulousLabs/Sia.git,thanks
//
//// The MIT License (MIT)
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
