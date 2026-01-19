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

package task_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/task"
)

func TestTaskExecuteConcurrent(t *testing.T) {
	unique := uint32(0)
	task.Concurrent(func(index int, arg uint32) {
		time.Sleep(time.Millisecond * 100)
		atomic.AddUint32(&unique, arg)
	}, 1, 2, 4) // copy mode, call Concurrent will create args = []uint32{1, 2, 4}
	require.Equal(t, uint32(7), atomic.LoadUint32(&unique))

	atomic.StoreUint32(&unique, 0)
	args := []uint32{1, 2, 4}
	task.Concurrent(func(index int, arg uint32) {
		time.Sleep(time.Millisecond * 100)
		atomic.AddUint32(&unique, arg)
	}, args...) // reference mode
	require.Equal(t, uint32(7), atomic.LoadUint32(&unique))
}

func TestTaskExecuteContextCancel(t *testing.T) {
	unique := uint32(0)
	ctx, cancel := context.WithCancel(context.Background())
	err := task.Run(ctx, func() error {
		task.Concurrent(func(index int, arg uint32) {
			time.Sleep(time.Millisecond * 2000)
			atomic.AddUint32(&unique, 1)
		}, 1, 2, 4)
		return nil
	}, func() error {
		time.Sleep(time.Millisecond * 5000)
		return errors.New("test")
	}, func() error {
		cancel()
		return nil
	})
	require.Contains(t, err.Error(), "canceled")
	require.Equal(t, uint32(0), atomic.LoadUint32(&unique))
}

func TestTaskExecuteConcurrentResult(t *testing.T) {
	{
		results := task.ConcurrentResult(func(index int, arg string) int {
			return len(arg)
		})
		require.Empty(t, len(results))
	}
	{
		results := task.ConcurrentResult(func(index int, arg int) int {
			return arg * arg
		}, 1, 2, 4, 7)
		require.Equal(t, []int{1, 4, 16, 49}, results)
	}
}
