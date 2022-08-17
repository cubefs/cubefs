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

package keycount

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/limit"
)

func TestKeyLimitPanic(t *testing.T) {
	l := New(2)
	require.Panics(t, func() { l.Release("not acquired") })
	require.Panics(t, func() { l.Acquire([]byte("unhashable type as map key")) })
}

func TestKeyLimitBase(t *testing.T) {
	l := New(2)
	key1, key2, key3 := 1, "2", [1]byte{'3'}
	require.Equal(t, 0, l.Running())

	require.NoError(t, l.Acquire(key1))
	require.NoError(t, l.Acquire(key2))
	require.NoError(t, l.Acquire(key1))
	require.NoError(t, l.Acquire(key2))
	require.NoError(t, l.Acquire(key3))
	require.Equal(t, limit.ErrLimited, l.Acquire(key2))

	require.Equal(t, 5, l.Running())

	require.Equal(t, limit.ErrLimited, l.Acquire(key1))
	require.Equal(t, limit.ErrLimited, l.Acquire(key2))
	require.NoError(t, l.Acquire(key3))
	require.Equal(t, 6, l.Running())

	l.Release(key1)
	l.Release(key2)
	require.Equal(t, 4, l.Running())
	require.NoError(t, l.Acquire(key1))
	require.NoError(t, l.Acquire(key2))
	require.Equal(t, limit.ErrLimited, l.Acquire(key3))
	require.Equal(t, 6, l.Running())

	l.Reset(3)
	require.NoError(t, l.Acquire(key1))
	require.Equal(t, limit.ErrLimited, l.Acquire(key1))
	l.Reset(2)
	l.Release(key1)
	require.Equal(t, limit.ErrLimited, l.Acquire(key1))
	l.Release(key1)
	l.Release(key1)
	require.NoError(t, l.Acquire(key1))
	require.NoError(t, l.Acquire(key1))
	require.Equal(t, limit.ErrLimited, l.Acquire(key1))
}

func TestBlockingKeyLimitPanic(t *testing.T) {
	l := NewBlockingKeyCountLimit(2)
	require.Panics(t, func() { l.Release("not acquired") })
	require.Panics(t, func() { l.Acquire([]byte("unhashable type as map key")) })
}

func TestBlockingKeyLimitBase(t *testing.T) {
	l := NewBlockingKeyCountLimit(2)
	key1, key2, key3 := 1, "2", [1]byte{'3'}
	var done1, done2, done3 atomicBool
	require.Equal(t, 0, l.Running())

	require.NoError(t, l.Acquire(key1))
	require.NoError(t, l.Acquire(key2))
	require.NoError(t, l.Acquire(key1))
	require.NoError(t, l.Acquire(key2))
	require.NoError(t, l.Acquire(key3))
	go func() {
		require.NoError(t, l.Acquire(key2)) // blocking
		done2.Set(true)
	}()

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 5, l.Running())

	go func() {
		require.NoError(t, l.Acquire(key1)) // blocking
		done1.Set(true)
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, l.Acquire(key3))
	require.Equal(t, 6, l.Running())
	require.False(t, done1.Get())
	require.False(t, done2.Get())

	l.Release(key1)
	l.Release(key2)
	time.Sleep(50 * time.Millisecond)
	require.True(t, done1.Get())
	require.True(t, done2.Get())
	require.Equal(t, 6, l.Running())

	done1, done2 = atomicBool{}, atomicBool{}
	go func() {
		require.NoError(t, l.Acquire(key1)) // blocking
		done1.Set(true)
	}()
	go func() {
		require.NoError(t, l.Acquire(key2)) // blocking
		done2.Set(true)
	}()
	go func() {
		require.NoError(t, l.Acquire(key3)) // blocking
		done3.Set(true)
	}()
	time.Sleep(50 * time.Millisecond)
	require.False(t, done1.Get())
	require.False(t, done2.Get())
	require.False(t, done3.Get())

	// for deadlock test
	key := "1"
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(10)
	limiter := NewBlockingKeyCountLimit(1)
	for range [33]struct{}{} {
		wg.Add(1)
		go func() {
			limiter.Acquire(key)
			time.Sleep(time.Millisecond * 10)
			limiter.Release(key)
			wg.Done()
		}()
	}
	wg.Wait()
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

func testRelease(limiter limit.Limiter, number int, key []interface{}) {
	for i := 0; i < number; i++ {
		limiter.Acquire(key...)
	}
	var wg sync.WaitGroup
	wg.Add(number)
	for i := 0; i < number; i++ {
		go func() {
			limiter.Release(key...)
			wg.Done()
		}()
	}
	wg.Wait()
}

func testAcquire(limiter limit.Limiter, number int, key []interface{}) {
	var wg sync.WaitGroup
	wg.Add(number)
	for i := 0; i < number; i++ {
		go func() {
			limiter.Acquire(key...)
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkKeyCountLimit_Release(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, thread := range []int{500} {
			l := New(thread)
			for _, value := range []int{1} {
				testRelease(l, thread, getKeys(value))
			}
		}
	}
}

func getKeys(num int) []interface{} {
	ints := make([]interface{}, num)
	for i := 0; i < num; i++ {
		ints[i] = i
	}
	return ints
}

func BenchmarkKeyCountLimit_Acquire(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, thread := range []int{500} {
			l := New(thread)
			for _, value := range []int{1} {
				testAcquire(l, thread, getKeys(value))
			}
		}
	}
}

func BenchmarkBlockingKeyCountLimit_Acquire(b *testing.B) {
	l := NewBlockingKeyCountLimit(b.N)
	key1 := 1
	for i := 0; i < b.N; i++ {
		l.Acquire(key1)
	}
}

func BenchmarkBlockingKeyCountLimit_Release(b *testing.B) {
	l := NewBlockingKeyCountLimit(b.N)
	key1 := 1
	for i := 0; i < b.N; i++ {
		l.Acquire(key1)
		l.Release(key1)
	}
}
