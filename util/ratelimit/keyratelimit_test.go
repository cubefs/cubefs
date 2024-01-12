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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyController(t *testing.T) {
	k := NewKeyRateLimit()
	key1, key2, key3 := "10001", "10002", "10003"
	k.Acquire(key1, 1024)
	assert.Equal(t, 1, k.current[key1].refCount)
	k.Acquire(key1, 1024)
	assert.Equal(t, 2, k.current[key1].refCount)

	k.Acquire(key2, 1024)
	assert.Equal(t, 2, len(k.current))
	k.Acquire(key3, 1024)
	assert.Equal(t, 3, len(k.current))

	k.Release(key1)
	assert.Equal(t, 1, k.current[key1].refCount)
	assert.Equal(t, 3, len(k.current))
	k.Release(key1)
	_, ok := k.current[key1]
	assert.Equal(t, false, ok)
	assert.Equal(t, 2, len(k.current))
	k.Release(key2)
	assert.Equal(t, 1, len(k.current))
	k.Release(key3)
	assert.Equal(t, 0, len(k.current))

	assert.Panics(t, func() {
		k.Release(key3)
	})
}
