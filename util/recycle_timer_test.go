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

package util_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/cubefs/cubefs/util/strutil"
	"github.com/stretchr/testify/require"
)

func TestRecycleTimer(t *testing.T) {
	const smallBufSize uint64 = 1 * util.GB
	const largeBufSize uint64 = 2 * util.GB
	buf := make([]byte, smallBufSize)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}

	actual, err := loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	inUsed := loadutil.GetGoInUsedHeap()
	t.Logf("Alloc: Total used %v heap %v", strutil.FormatSize(actual), strutil.FormatSize(inUsed))
	require.GreaterOrEqual(t, actual, smallBufSize)
	require.GreaterOrEqual(t, inUsed, smallBufSize)

	timer, err := util.NewRecycleTimer(1*time.Second, 0, largeBufSize)
	require.NoError(t, err)
	defer timer.Stop()

	buf = nil
	runtime.GC()

	actual, err = loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	inUsed = loadutil.GetGoInUsedHeap()
	t.Logf("After GC: Total used %v heap %v", strutil.FormatSize(actual), strutil.FormatSize(inUsed))
	require.GreaterOrEqual(t, actual, smallBufSize)
	require.Less(t, inUsed, smallBufSize)

	buf = make([]byte, largeBufSize)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}

	actual, err = loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	inUsed = loadutil.GetGoInUsedHeap()
	t.Logf("Alloc: Total used %v heap %v", strutil.FormatSize(actual), strutil.FormatSize(inUsed))
	require.GreaterOrEqual(t, actual, largeBufSize)
	require.GreaterOrEqual(t, inUsed, largeBufSize)

	buf = nil
	runtime.GC()

	actual, err = loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	inUsed = loadutil.GetGoInUsedHeap()
	t.Logf("After GC: Total used %v heap %v", strutil.FormatSize(actual), strutil.FormatSize(inUsed))
	require.GreaterOrEqual(t, actual, smallBufSize)
	require.Less(t, inUsed, smallBufSize)

	time.Sleep(2 * time.Second)

	actual, err = loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	inUsed = loadutil.GetGoInUsedHeap()
	t.Logf("After Recycle: Total used %v heap %v", strutil.FormatSize(actual), strutil.FormatSize(inUsed))
	require.Less(t, actual, largeBufSize)
	require.Less(t, inUsed, largeBufSize)
}
