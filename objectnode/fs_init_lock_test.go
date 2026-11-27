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

import (
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// blockedInLock counts goroutines that are waiting for a mutex inside fn.
func blockedInLock(fn string) int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, fn) && strings.Contains(g, "Mutex).Lock") {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// testInitLockExclusive checks that the per key init lock lets only one
// caller in at a time, also after the first holder releases it while
// another caller is still waiting.
func testInitLockExclusive(t *testing.T, fn string, acquire func() func()) {
	releaseA := acquire()

	var inside, maxInside int32
	entered := make(chan struct{}, 2)
	leave := make(chan struct{})
	done := make(chan struct{}, 2)
	worker := func() {
		release := acquire()
		n := atomic.AddInt32(&inside, 1)
		for {
			m := atomic.LoadInt32(&maxInside)
			if n <= m || atomic.CompareAndSwapInt32(&maxInside, m, n) {
				break
			}
		}
		entered <- struct{}{}
		<-leave
		atomic.AddInt32(&inside, -1)
		release()
		done <- struct{}{}
	}

	// B waits for the lock that A holds.
	go worker()
	waitFor(t, "B to wait for the lock", func() bool { return blockedInLock(fn) == 1 })

	// A releases. Wait until B holds the lock, then C comes in for the
	// same key. C must wait for B, not get in too.
	releaseA()
	<-entered
	go worker()
	waitFor(t, "C to wait or enter", func() bool {
		return atomic.LoadInt32(&inside) == 2 || blockedInLock(fn) == 1
	})
	twoInside := atomic.LoadInt32(&inside) == 2

	close(leave)
	<-entered
	<-done
	<-done
	if twoInside || atomic.LoadInt32(&maxInside) != 1 {
		t.Fatalf("two callers held the init lock for the same key at once")
	}
}

func TestSyncVolumeInitExclusive(t *testing.T) {
	loader := &VolumeLoader{}
	testInitLockExclusive(t, "syncVolumeInit", func() func() {
		return loader.syncVolumeInit("vol")
	})
}

func TestSyncUserInitExclusive(t *testing.T) {
	loader := &CacheUserInfoLoader{}
	testInitLockExclusive(t, "syncUserInit", func() func() {
		return loader.syncUserInit("ak")
	})
}

func TestKeyedMutexRemovesUnusedKeys(t *testing.T) {
	var k keyedMutex
	unlockA := k.lock("a")
	unlockB := k.lock("b")
	unlockA()
	unlockB()
	if len(k.locks) != 0 {
		t.Fatalf("keyedMutex kept %d unused keys", len(k.locks))
	}
}
