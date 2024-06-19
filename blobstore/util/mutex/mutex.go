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

package mutex

import (
	"sync"
)

// TODO: add TryLock and TryRLock upgrating to Go1.18+

type nilError struct{}

func (nilError) Error() string { return "mutex.Nil" }

// Nil is the error which you want an error in function WithLock or WithRLock,
// and then returns it outside to do other choice.
var Nil error = nilError{}

// Locker represents an object that can be locked, unlocked.
type Locker interface {
	sync.Locker
}

// WithLocker represents a function to run with locker.
type WithLocker interface {
	Locker
	// WithLock runs function in lock.
	WithLock(func() error) error
}

// RLocker represents a reader/writer mutual exclusion locker.
type RLocker interface {
	Locker
	RLock()
	RUnlock()
}

// WithRLocker represents a function to run with reading locker.
type WithRLocker interface {
	RLocker
	WithLocker
	// WithRLock runs function in reading lock.
	WithRLock(func() error) error
}

// Mutex is a WithLocker with sync.Mutex.
type Mutex struct{ sync.Mutex }

var _ WithLocker = (*Mutex)(nil)

func (m *Mutex) WithLock(f func() error) error {
	m.Lock()
	defer m.Unlock()
	return f()
}

type mutex struct{ Locker }

func (m *mutex) WithLock(f func() error) error {
	m.Locker.Lock()
	defer m.Locker.Unlock()
	return f()
}

// NewLocker returns a WithLocker with the Locker.
func NewLocker(locker Locker) WithLocker {
	return &mutex{Locker: locker}
}

// RWMutex is a WithRLocker with sync.RWMutex.
type RWMutex struct{ sync.RWMutex }

var _ WithRLocker = (*RWMutex)(nil)

func (m *RWMutex) WithLock(f func() error) error {
	m.Lock()
	defer m.Unlock()
	return f()
}

func (m *RWMutex) WithRLock(f func() error) error {
	m.RLock()
	defer m.RUnlock()
	return f()
}

type rwMutex struct{ RLocker }

func (m *rwMutex) WithLock(f func() error) error {
	m.RLocker.Lock()
	defer m.RLocker.Unlock()
	return f()
}

func (m *rwMutex) WithRLock(f func() error) error {
	m.RLocker.RLock()
	defer m.RLocker.RUnlock()
	return f()
}

// NewRLocker returns a WithRLocker with the RLocker.
func NewRLocker(rlocker RLocker) WithRLocker {
	return &rwMutex{RLocker: rlocker}
}
