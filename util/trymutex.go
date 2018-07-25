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
