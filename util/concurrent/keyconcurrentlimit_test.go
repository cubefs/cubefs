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

package concurrent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncKeyLimit(t *testing.T) {
	l := NewLimit()
	key1, key2, key3 := string("1"), string("2"), string("3")
	require.Equal(t, 0, l.Running())

	require.NoError(t, l.Acquire(key1, 2))
	require.NoError(t, l.Acquire(key2, 2))
	require.NoError(t, l.Acquire(key1, 2))
	require.NoError(t, l.Acquire(key2, 2))
	require.NoError(t, l.Acquire(key3, 2))
	require.Equal(t, ErrLimit, l.Acquire(key2, 2))
	require.NoError(t, l.Acquire(key2, 3))
	l.Release(key2)

	require.Equal(t, 5, l.Running())

	require.Equal(t, ErrLimit, l.Acquire(key1, 2))
	require.Equal(t, ErrLimit, l.Acquire(key2, 2))
	require.NoError(t, l.Acquire(key3, 2))
	require.Equal(t, 6, l.Running())

	l.Release(key1)
	l.Release(key2)
	require.Equal(t, 4, l.Running())
	require.NoError(t, l.Acquire(key1, 2))
	require.NoError(t, l.Acquire(key2, 2))
	require.Equal(t, ErrLimit, l.Acquire(key3, 2))
	require.Equal(t, 6, l.Running())
}

func TestKeyLimit_ReleaseNonExistKey(t *testing.T) {
	ast := require.New(t)
	key := "1"

	l := NewLimit()
	// should not panic
	l.Release(key)
	ast.Equal(int64(0), l.Get(key))

	// should not decrease to minus integer
	l.Acquire(key, 10)
	ast.Equal(int64(1), l.Get(key))
	for i := 0; i < 100; i++ {
		l.Release(key)
		ast.Equal(int64(0), l.Get(key))
	}
}

func TestKeyLimit_Switch(t *testing.T) {
	ast := require.New(t)
	l := NewLimit()
	key := "1"

	ast.NoError(l.Acquire(key, 1))
	ast.Error(l.Acquire(key, 1))
	ast.Equal(int64(1), l.Get(key))

	ast.NoError(l.Acquire(key, 0))
	ast.Equal(int64(2), l.Get(key))

	ast.Error(l.Acquire(key, 1))
	ast.Equal(int64(2), l.Get(key))

	ast.NoError(l.Acquire(key, 10))
	ast.Equal(int64(3), l.Get(key))
}
