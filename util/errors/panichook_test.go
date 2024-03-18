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

package errors_test

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/stretchr/testify/require"
)

func requireArch(t *testing.T) (ok bool) {
	if !errors.SupportPanicHook() {
		err := errors.AtPanic(func() {})
		require.ErrorIs(t, err, errors.ErrUnsupportedArch)
		return
	}
	ok = true
	return
}

func testPanicHook(t *testing.T) {
	token := false
	defer func() {
		r := recover()
		if r != nil {
			require.True(t, token)
		}
	}()
	err := errors.AtPanic(func() {
		token = true
	})
	require.NoError(t, err)
	t.Logf("start panic")
	panic("test")
}

func TestPanicHook(t *testing.T) {
	if !requireArch(t) {
		return
	}
	testPanicHook(t)
}

func testNilPointerPanic(t *testing.T) {
	token := false
	defer func() {
		r := recover()
		if r != nil {
			require.True(t, token)
		}
	}()
	err := errors.AtPanic(func() {
		token = true
	})
	require.NoError(t, err)
	t.Logf("start panic")
	var i *int
	*i = 1
}

func TestNilPointPanic(t *testing.T) {
	if !requireArch(t) {
		return
	}
	testNilPointerPanic(t)
}

func testSegfaultPanic(t *testing.T) {
	token := false
	defer func() {
		r := recover()
		if r != nil {
			require.True(t, token)
		}
	}()
	err := errors.AtPanic(func() {
		token = true
	})
	require.NoError(t, err)
	t.Logf("start panic")
	debug.SetPanicOnFault(true)
	p := unsafe.Pointer(uintptr(0xFFFF))
	var i *int = (*int)(p)
	*i = 1
}

func TestSegfaultPanic(t *testing.T) {
	if !requireArch(t) {
		return
	}
	testSegfaultPanic(t)
}

const loopTestCount = 1000

func TestLoopPanicHook(t *testing.T) {
	for i := 0; i != loopTestCount; i++ {
		TestPanicHook(t)
		TestNilPointPanic(t)
	}
}

func TestCurrentPanic(t *testing.T) {
	if !requireArch(t) {
		return
	}
	cnt := int32(0)
	err := errors.AtPanic(func() {
		atomic.AddInt32(&cnt, 1)
	})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < loopTestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Logf("%v cnt panic", atomic.LoadInt32(&cnt))
				}
			}()
			t.Logf("start panic")
			panic("test")
		}()
	}
	wg.Wait()
	require.EqualValues(t, loopTestCount, cnt)
}

func TestCurrentNilPointerPanic(t *testing.T) {
	if !requireArch(t) {
		return
	}
	cnt := int32(0)
	err := errors.AtPanic(func() {
		atomic.AddInt32(&cnt, 1)
	})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < loopTestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Logf("%v cnt panic", atomic.LoadInt32(&cnt))
				}
			}()
			debug.SetPanicOnFault(true)
			t.Logf("start panic")
			var i *int
			*i = 1
		}()
	}
	wg.Wait()
	require.EqualValues(t, loopTestCount, cnt)
}

func TestCurrentSegfaultPanic(t *testing.T) {
	if !requireArch(t) {
		return
	}
	cnt := int32(0)
	err := errors.AtPanic(func() {
		atomic.AddInt32(&cnt, 1)
	})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < loopTestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Logf("%v cnt panic", atomic.LoadInt32(&cnt))
				}
			}()
			debug.SetPanicOnFault(true)
			t.Logf("start panic")
			p := unsafe.Pointer(uintptr(0xFFFF))
			var i *int = (*int)(p)
			*i = 1
		}()
	}
	wg.Wait()
	require.EqualValues(t, loopTestCount, cnt)
}
