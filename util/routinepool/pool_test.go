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

package routinepool_test

import (
	"runtime"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/stretchr/testify/require"
)

func TestRoutinePool(t *testing.T) {
	maxRoutineNum := 10
	pool := routinepool.NewRoutinePool(maxRoutineNum)

	var max int32 = 0
	totalRoutineNum := 100
	lock := &sync.Mutex{}

	value := 0

	for i := 0; i < totalRoutineNum; i++ {
		pool.Submit(func() {
			lock.Lock()
			value++
			for i := 0; i < 200; i++ {
				currentNum := pool.RunningNum()
				if currentNum > max {
					max = currentNum
				}
				runtime.Gosched()
			}
			lock.Unlock()
		})
	}

	pool.WaitAndClose()

	require.EqualValues(t, max, maxRoutineNum)

	require.EqualValues(t, totalRoutineNum, value)
}
