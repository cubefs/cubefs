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

package common

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	poolSize = 10
	tpool    = []TaskPool{New(10, 10), New(10, 10)}
	batchCnt = 10000
	listNum  = 2000000
)

func BenchmarkCheck_With_Chan_Con(b *testing.B) {
	t := &testing.T{}
	for i := 0; i < b.N; i++ {
		Test_Check_With_Chan_Con(t)
	}
}

func BenchmarkCheck_With_Chan_Serial(b *testing.B) {
	t := &testing.T{}
	for i := 0; i < b.N; i++ {
		Test_Check_With_Chan_Serial(t)
	}
}

func Test_Check_With_Chan_Con(t *testing.T) {
	nums := make([]int, 0, listNum)
	for i := 0; i < listNum; i++ {
		nums = append(nums, i)
	}
	in := func() <-chan int {
		out := make(chan int)
		go func() {
			for _, n := range nums {
				out <- n
			}
			close(out)
		}()
		return out
	}()
	getChanged := func(in <-chan int, batchCnt int) <-chan int {
		out := make(chan int)
		changed := make([]int, 0)
		tpool[0].Run(func() {
			tmp := make([]int, 0, batchCnt)
			for i := range in {
				tmp = append(tmp, i)
				if len(tmp) == batchCnt {
					changed = append(changed, batch(tmp)...)
					tmp = tmp[:0]
				}
			}
			if len(tmp) != 0 {
				changed = append(changed, batch(tmp)...)
			}

			for i := range changed {
				out <- changed[i]
			}
			close(out)
		})

		return out
	}
	chans := make([]<-chan int, poolSize)
	for i := range chans {
		chans[i] = getChanged(in, batchCnt)
	}
	var wg sync.WaitGroup
	var cnt int32

	for n := range merge(chans[:]) {
		wg.Add(1)
		nn := n
		tpool[1].Run(func() {
			if nn%2 == 1 {
				atomic.AddInt32(&cnt, 1)
				time.Sleep(time.Microsecond)
			}
			wg.Done()
		})
	}
	wg.Wait()
	require.Equal(t, cnt, int32(listNum/2))
}

func Test_Check_With_Chan_Serial(t *testing.T) {
	nums := make([]int, 0, listNum)
	for i := 0; i < listNum; i++ {
		nums = append(nums, i)
	}
	in := func() <-chan int {
		out := make(chan int)
		go func() {
			for _, n := range nums {
				out <- n
			}
			close(out)
		}()
		return out
	}()
	getChanged := func(in <-chan int, batchCnt int) <-chan int {
		out := make(chan int)
		changed := make([]int, 0)
		tpool[0].Run(func() {
			tmp := make([]int, 0, batchCnt)
			for i := range in {
				tmp = append(tmp, i)
				if len(tmp) == batchCnt {
					changed = append(changed, batch(tmp)...)
					tmp = tmp[:0]
				}
			}
			if len(tmp) != 0 {
				changed = append(changed, batch(tmp)...)
			}

			for i := range changed {
				out <- changed[i]
			}
			close(out)
		})

		return out
	}
	chans := make([]<-chan int, poolSize)
	for i := range chans {
		chans[i] = getChanged(in, batchCnt)
	}
	var cnt int32
	for n := range merge(chans[:]) {
		if n%2 == 1 {
			atomic.AddInt32(&cnt, 1)
			time.Sleep(time.Microsecond)
		}
	}

	require.Equal(t, cnt, int32(listNum/2))
}

func batch(inodes []int) []int {
	changed := make([]int, 0)
	for i := range inodes {
		if inodes[i]%2 == 1 {
			changed = append(changed, inodes[i])
		}
	}
	return changed
}

func merge(cs []<-chan int) <-chan int {
	var wg sync.WaitGroup
	out := make(chan int)

	wg.Add(len(cs))
	for i := range cs {
		cc := cs[i]
		go func() {
			for n := range cc {
				out <- n
			}
			wg.Done()
		}()

	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
