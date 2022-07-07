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

package taskpool_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

func TestTaskpoolBase(t *testing.T) {
	runner := taskpool.New(0, 0)
	ok := runner.TryRun(func() {
		t.Fatal("can not be here")
	})
	require.False(t, ok)
	runner.Close()

	runner = taskpool.New(1, 0)
	time.Sleep(time.Millisecond * 100)
	runner.Run(func() {
		time.Sleep(time.Second)
	})
	require.False(t, runner.TryRun(func() {}))
	runner.Close()
}

func BenchmarkGoroutine(b *testing.B) {
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			math.Pow(math.Pi, float64(i%10))
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func BenchmarkTaskPool(b *testing.B) {
	var wg sync.WaitGroup
	runner := taskpool.New(1024, 0)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		idx := i
		runner.Run(func() {
			math.Pow(math.Pi, float64(idx%10))
			wg.Done()
		})
	}
	wg.Wait()
}
