//Copyright (c) 2016 Nebulous Inc.
//
// The MIT License (MIT)
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
// A Limiter restricts access to a resource.
//
// Units of the resource are reserved via Request and returned via Release.
// Conventionally, a caller who reserves n units is responsible for ensuring
// that all n are eventually returned. Once the number of reserved units
// exceeds the Limiters limit, further calls to Request will block until
// sufficient units are returned via Release.
//
// This Limiter differs from others in that it allows requesting more than the
// limit. This request is only fulfilled once all other units have been
// returned. Once the request is fulfilled, calls to Request will block until
// enough units have been returned to bring the total outlay below the limit.
// This design choice prevents any call to Request from blocking forever,
// striking a balance between precise resource management and flexibility.

package trymutex

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
