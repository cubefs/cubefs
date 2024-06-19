// Copyright 2024 The CubeFS Authors.
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

package mutex_test

import (
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/mutex"
)

func ExampleMutex() {
	var m mutex.Mutex
	m.Lock()
	fmt.Println("mutex locked")
	m.Unlock()

	err := m.WithLock(func() error { return mutex.Nil })
	fmt.Println("mutex got error:", err)

	fmt.Println()
	l := mutex.NewLocker(&sync.Mutex{})
	l.Lock()
	fmt.Println("locker locked")
	l.Unlock()

	err = l.WithLock(func() error { return fmt.Errorf("locker error") })
	fmt.Println("locker got error:", err)

	// Output:
	// mutex locked
	// mutex got error: mutex.Nil
	//
	// locker locked
	// locker got error: locker error
}

func ExampleRWMutex() {
	var m mutex.RWMutex
	m.Lock()
	fmt.Println("rwmutex locked")
	m.Unlock()

	m.RLock()
	fmt.Println("rwmutex rlocked")
	m.RUnlock()

	err := m.WithLock(func() error { return mutex.Nil })
	fmt.Println("rwmutex got error:", err)
	err = m.WithRLock(func() error { return nil })
	fmt.Println("rwmutex got nil:", err == nil)

	rl := m.RLocker()
	rl.Lock()
	rl.Lock()
	fmt.Println("rwmutex RLock as Lock")
	rl.Unlock()
	rl.Unlock()

	fmt.Println()
	l := mutex.NewRLocker(&sync.RWMutex{})
	l.Lock()
	fmt.Println("rlocker locked")
	l.Unlock()

	l.RLock()
	fmt.Println("rlocker rlocked")
	l.RUnlock()

	err = l.WithLock(func() error { return mutex.Nil })
	fmt.Println("rlocker got error:", err)
	err = l.WithRLock(func() error { return nil })
	fmt.Println("rlocker got nil:", err == nil)

	// Output:
	// rwmutex locked
	// rwmutex rlocked
	// rwmutex got error: mutex.Nil
	// rwmutex got nil: true
	// rwmutex RLock as Lock
	//
	// rlocker locked
	// rlocker rlocked
	// rlocker got error: mutex.Nil
	// rlocker got nil: true
}
