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

package selector

import (
	"errors"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

// Selector select hosts or something from somewhere.
type Selector interface {
	GetHashN(n int, key []byte) []string
	GetRandomN(n int) []string
	GetRoundRobinN(n int) []string
}

type selector struct {
	sync.RWMutex

	syncLock     limit.Limiter
	interval     int64
	lastUpdate   int64
	nextIndex    int
	cachedValues []string
	getter       func() ([]string, error)
}

// NewSelector implements Selector
func NewSelector(intervalMs int64, getter func() ([]string, error)) (Selector, error) {
	values, err := getter()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New("not found initial values from getter")
	}

	s := &selector{
		interval:     int64(time.Millisecond) * intervalMs,
		lastUpdate:   time.Now().UnixNano(),
		nextIndex:    0,
		cachedValues: values,
		getter:       getter,
		syncLock:     count.New(1),
	}
	return s, nil
}

// MakeSelector always return a selector
func MakeSelector(intervalMs int64, getter func() ([]string, error)) Selector {
	values, _ := getter()
	return &selector{
		interval:     int64(time.Millisecond) * intervalMs,
		lastUpdate:   time.Now().UnixNano(),
		nextIndex:    0,
		cachedValues: values,
		getter:       getter,
		syncLock:     count.New(1),
	}
}

func (s *selector) GetHashN(n int, key []byte) []string {
	if n <= 0 {
		return nil
	}
	values := s.getValues(n, false)
	if len(values) <= 1 {
		return values
	}

	idx := int(keyHash(key) % uint64(len(values)))
	values[0], values[idx] = values[idx], values[0]

	hashed := values[1:]
	rand.Shuffle(len(hashed), func(i, j int) {
		hashed[i], hashed[j] = hashed[j], hashed[i]
	})

	if n <= len(values) {
		values = values[:n]
	}
	return values
}

func (s *selector) GetRandomN(n int) []string {
	if n <= 0 {
		return nil
	}
	values := s.getValues(n, false)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	if n <= len(values) {
		values = values[:n]
	}
	return values
}

func (s *selector) GetRoundRobinN(n int) []string {
	if n <= 0 {
		return nil
	}
	values := s.getValues(n, true)
	if n <= len(values) {
		values = values[:n]
	}
	return values
}

func (s *selector) sync() {
	if err := s.syncLock.Acquire(); err != nil {
		return
	}
	defer s.syncLock.Release()

	values, err := s.getter()
	if err != nil {
		return
	}
	if len(values) == 0 {
		return
	}
	sort.Strings(values)

	s.Lock()
	if s.nextIndex >= len(values) {
		s.nextIndex = 0
	}
	s.cachedValues = values
	s.lastUpdate = time.Now().UnixNano()
	s.Unlock()
}

func (s *selector) getValues(n int, rr bool) []string {
	s.RLock()
	interval := s.interval
	lastUpdate := s.lastUpdate
	idx := s.nextIndex
	values := make([]string, len(s.cachedValues))
	copy(values, s.cachedValues)
	s.RUnlock()

	if rr && len(values) > 0 {
		if n > len(values) {
			n = len(values)
		}

		s.Lock()
		s.nextIndex = (s.nextIndex + n) % len(s.cachedValues)
		s.Unlock()

		values = append(values[idx:], values[0:idx]...)
	}

	if time.Now().UnixNano() >= lastUpdate+interval {
		go s.sync()
	}

	return values
}

func keyHash(key []byte) uint64 {
	f := fnv.New64()
	f.Write(key)
	return f.Sum64()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
