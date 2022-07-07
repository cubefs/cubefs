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

package count

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/limit"
)

func init() {
	runtime.GOMAXPROCS(4)
}

func TestCountLimit(t *testing.T) {
	l := New(2)
	key := "a"
	require.Equal(t, 0, l.Running())

	require.NoError(t, l.Acquire(key))
	require.NoError(t, l.Acquire(key))
	require.ErrorIs(t, limit.ErrLimited, l.Acquire(key))
	require.Equal(t, 2, l.Running())

	l.Release(key)
	require.Equal(t, 1, l.Running())
	require.NoError(t, l.Acquire(key))
	require.ErrorIs(t, limit.ErrLimited, l.Acquire(key))
	require.Equal(t, 2, l.Running())

	l.Release(key)
	l.Release(key)
	require.Equal(t, 0, l.Running())
	require.NoError(t, l.Acquire(key))
	require.NoError(t, l.Acquire(key))
	require.ErrorIs(t, limit.ErrLimited, l.Acquire(key))
	require.Equal(t, 2, l.Running())
}

func TestBlockingCountLimit(t *testing.T) {
	l := NewBlockingCount(2)
	var key interface{} = nil
	require.Equal(t, 0, l.Running())

	require.NoError(t, l.Acquire(key))
	require.NoError(t, l.Acquire(key))
	require.Equal(t, 2, l.Running())

	l.Release(key)
	require.Equal(t, 1, l.Running())
	require.NoError(t, l.Acquire(key))
	require.Equal(t, 2, l.Running())

	l.Release(key)
	l.Release(key)
	require.Equal(t, 0, l.Running())
	require.NoError(t, l.Acquire(key))
	require.NoError(t, l.Acquire(key))
	require.Equal(t, 2, l.Running())

	done := &atomicBool{}
	go func() {
		require.NoError(t, l.Acquire(key)) // blocking
		done.Set(true)
	}()
	time.Sleep(.5e9)
	require.Equal(t, 2, l.Running())
	require.False(t, done.Get())
	l.Release(key)
	time.Sleep(.5e9)
	require.Equal(t, 2, l.Running())
	require.True(t, done.Get())
}

type atomicBool struct {
	ret int32
}

func (a *atomicBool) Set(v bool) {
	if v {
		atomic.StoreInt32(&a.ret, 1)
	} else {
		atomic.StoreInt32(&a.ret, 0)
	}
}

func (a *atomicBool) Get() (v bool) {
	return atomic.LoadInt32(&a.ret) == 1
}
