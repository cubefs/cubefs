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

package counter

import (
	"sync"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
)

const second clock.Duration = 1e9

var mockTime *clock.Mock

func init() {
	mockTime = clock.NewMock()
	mockTime.Add(1483950782 * second)
	time = mockTime
}

func TestCounterBase(t *testing.T) {
	var c Counter
	var exp [SLOT]int
	require.Equal(t, exp, c.Show())

	c.Add()
	exp[SLOT-1] = 1
	require.Equal(t, exp, c.Show())

	for ii := int64(0); ii < SLOT; ii++ {
		mockTime.Add(clock.Duration(interval) * second)
		c.AddN(int(ii))
		copy(exp[:], exp[1:])
		exp[SLOT-1] = int(ii)
		require.Equal(t, exp, c.Show())
	}

	for range [10]struct{}{} {
		c := &Counter{}
		c.Add()
		mockTime.Add(clock.Duration(interval) * second)
		exp := [SLOT]int{}
		exp[SLOT-2] = 1
		require.Equal(t, exp, c.Show())

		c.Add()
		c.Add()
		exp[SLOT-1] = 2
		require.Equal(t, exp, c.Show())

		mockTime.Add(clock.Duration(SLOT-1) * clock.Duration(interval) * second)
		exp = [SLOT]int{}
		exp[0] = 2
		require.Equal(t, exp, c.Show())

		c.Add()
		exp[0] = 2
		exp[SLOT-1] = 1
		require.Equal(t, exp, c.Show())
	}
}

func TestCounterSafe(t *testing.T) {
	var c Counter
	var wg sync.WaitGroup
	wg.Add(1000)
	for idx := range [1000]struct{}{} {
		go func(ii int) {
			mockTime.Add(clock.Duration(ii) * second)
			c.AddN(ii)
			if ii%3 == 0 {
				c.Show()
			}
			wg.Done()
		}(idx)
	}
	wg.Wait()
}

func BenchmarkCounterAdd(b *testing.B) {
	var c Counter
	for ii := 0; ii < b.N; ii++ {
		c.Add()
	}
}

func BenchmarkCounterShow(b *testing.B) {
	var c Counter
	c.AddN(100)
	mockTime.Add(100 * second)
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		c.Show()
	}
}
