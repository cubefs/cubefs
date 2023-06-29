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

package routinepool

import (
	"sync"
	"testing"
)

func TestExample(t *testing.T) {
	maxRoutineNum := 10
	pool := NewRoutinePool(maxRoutineNum)

	var max int32 = 0
	totalRoutineNum := 100
	lock := &sync.Mutex{}

	value := 0

	for i := 0; i < totalRoutineNum; i++ {
		pool.Submit(func() {
			currentNum := pool.RunningNum()
			lock.Lock()
			value++
			if currentNum > max {
				max = currentNum
			}
			lock.Unlock()
		})
	}

	pool.WaitAndClose()

	if max != int32(maxRoutineNum) {
		t.Fatalf("expected max running num(%v), got(%v)", maxRoutineNum, max)
	}

	if value != totalRoutineNum {
		t.Fatalf("expected value(%v), got(%v)", totalRoutineNum, value)
	}
}
