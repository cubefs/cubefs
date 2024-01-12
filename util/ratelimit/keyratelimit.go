// Copyright 2023 The CubeFS Authors.
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

package ratelimit

import (
	"sync"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
)

type rateLimitWrapper struct {
	r        *ratelimit.RateLimiter
	refCount int
}

func newRateLimitWrapper(rate int) *rateLimitWrapper {
	return &rateLimitWrapper{
		r:        ratelimit.New(10*rate, 10*time.Second),
		refCount: 0,
	}
}

type KeyRateLimit struct {
	mutex   sync.RWMutex
	current map[string]*rateLimitWrapper // uid -> ratelimit
}

func NewKeyRateLimit() *KeyRateLimit {
	return &KeyRateLimit{current: make(map[string]*rateLimitWrapper)}
}

func (k *KeyRateLimit) Acquire(key string, rate int) *ratelimit.RateLimiter {
	k.mutex.Lock()
	limit, ok := k.current[key]
	if !ok {
		limit = newRateLimitWrapper(rate)
		k.current[key] = limit
	}
	limit.refCount++
	k.mutex.Unlock()
	return limit.r
}

func (k *KeyRateLimit) Release(key string) {
	k.mutex.Lock()
	defer k.mutex.Unlock()
	limit, ok := k.current[key]
	if !ok {
		panic("key not in map. Possible reason: Release without Acquire.")
	}
	limit.refCount--
	if limit.refCount < 0 {
		panic("internal error: refs < 0")
	}
	if limit.refCount == 0 {
		delete(k.current, key)
	}
}
