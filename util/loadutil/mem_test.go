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

package loadutil_test

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/cubefs/cubefs/util/strutil"
	"github.com/stretchr/testify/require"
)

func TestMemoryUsed(t *testing.T) {
	used, err := loadutil.GetMemoryUsedPercent()
	require.NoError(t, err)
	t.Logf("Mem Used:%v\n", used)
}

func TestGetTotalMem(t *testing.T) {
	total, err := loadutil.GetTotalMemory()
	require.NoError(t, err)
	t.Logf("Mem Total:%v\n", total)
}

func TestGetCurrMemory(t *testing.T) {
	curr, err := loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	require.Greater(t, curr, uint64(0))
}

func TestAlloc(t *testing.T) {
	allocSize := uint64(4 * util.GB)
	total, err := loadutil.GetTotalMemory()
	t.Logf("Total Memory %v", strutil.FormatSize(total))
	require.NoError(t, err)
	for allocSize > total {
		allocSize /= 2
	}

	buff := make([]byte, allocSize)
	for i := 0; i < int(allocSize); i++ {
		buff[i] = 0
	}

	curr, err := loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	require.Greater(t, curr, allocSize)
	t.Logf("Memory %v Inused %v", strutil.FormatSize(curr), strutil.FormatSize(loadutil.GetGoInUsedHeap()))

	buff = nil
	curr, err = loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	require.Greater(t, curr, allocSize)
	runtime.GC()
	t.Logf("Memory %v Inused %v", strutil.FormatSize(curr), strutil.FormatSize(loadutil.GetGoInUsedHeap()))

	old := curr
	debug.FreeOSMemory()
	curr, err = loadutil.GetCurrentProcessMemory()
	require.NoError(t, err)
	require.Less(t, curr, old)
	t.Logf("Memory %v", strutil.FormatSize(curr))
}

func TestGetTotalSwapedMemory(t *testing.T) {
	total, err := loadutil.GetTotalSwapMemory()
	require.NoError(t, err)
	t.Logf("Swap Memory %v", strutil.FormatSize(total))
}
