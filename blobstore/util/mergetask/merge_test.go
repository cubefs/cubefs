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

package mergetask

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMergeTaskOK(t *testing.T) {
	var n int32
	taskOk := func(i interface{}) error {
		t.Log("run index:", i)
		atomic.AddInt32(&n, 1)
		time.Sleep(30 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	const limit = 10000
	wg.Add(limit)
	runner := NewMergeTask(limit, taskOk)
	for i := range [limit]struct{}{} {
		go func(i int) {
			defer wg.Done()
			e := runner.Do(i)
			require.NoError(t, e)
		}(i)
	}
	wg.Wait()

	t.Log("run merged n:", n)
	require.True(t, n < limit)
}

func TestMergeTaskError(t *testing.T) {
	var n int32
	taskError := func(i interface{}) error {
		t.Log("run index:", i)
		atomic.AddInt32(&n, 1)
		time.Sleep(30 * time.Millisecond)
		return fmt.Errorf("run error at index: %d", i)
	}

	var wg sync.WaitGroup
	const limit = 10000
	wg.Add(limit)
	runner := NewMergeTask(limit, taskError)
	for i := range [limit]struct{}{} {
		go func(i int) {
			defer wg.Done()
			e := runner.Do(i)
			require.Error(t, e)
		}(i)
	}
	wg.Wait()

	t.Log("run merged n:", n)
	require.True(t, n < limit)
}

func TestMergeTaskPanic(t *testing.T) {
	var n int32
	taskPanic := func(i interface{}) error {
		t.Log("run index:", i)
		atomic.AddInt32(&n, 1)
		panic(fmt.Sprintf("panic at index: %d", i))
	}

	runner := NewMergeTask(0, taskPanic)
	require.Panics(t, func() {
		runner.Do(0)
	})
}
