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

package datanode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimitIOBase(t *testing.T) {
	f := func() {}
	for _, flowIO := range [][2]int{
		{-1, -1},
		{0, -1},
		{-1, 0},
		{100, -1},
		{1 << 20, 4},
	} {
		l := newIOLimiter(flowIO[0], flowIO[1])
		l.ResetFlow(flowIO[0])
		l.ResetIO(flowIO[1])
		l.Run(0, f)
		l.Run(10, f)
		require.True(t, l.TryRun(1, f))
		l.Close()
	}

	{
		l := newIOLimiter(1<<10, 0)
		l.Run(10, f)
		st := l.Status()
		t.Logf("status: %+v", st)
		require.Equal(t, 1<<10, st.FlowLimit)
		require.True(t, st.FlowUsed > 0)
		require.True(t, st.FlowUsed <= 10)
		require.True(t, l.TryRun(10, f))
		l.Run(1<<20, f)
		l.TryRun(1<<20, f)
		l.Close()
	}
	{
		done := make(chan struct{})
		l := newIOLimiter(-1, 2)
		st := l.Status()
		t.Logf("before status: %+v", st)
		for ii := 0; ii < st.IOConcurrency; ii++ {
			go func() {
				l.Run(0, func() { <-done })
			}()
		}
		for ii := 0; ii < st.IOQueue*2; ii++ {
			go func() {
				l.Run(0, func() { <-done })
			}()
		}
		time.Sleep(100 * time.Millisecond)
		t.Logf("after status: %+v", l.Status())
		require.False(t, l.TryRun(0, f))
		close(done)
		q := l.getIO()
		l.Close()
		q.Run(f)
		require.True(t, q.TryRun(f))
		t.Logf("closed status: %+v", q.Status())
	}
}

func TestLimitIOConcurrency(t *testing.T) {
	l := newIOLimiter(1<<10, 10)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			time.Sleep(time.Microsecond)
			l.ResetFlow(1 << 10)
			l.ResetIO(10)
		}
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			time.Sleep(time.Microsecond)
			go func() {
				l.Run(1, func() { <-done })
			}()
		}
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			time.Sleep(time.Microsecond)
			go func() {
				l.TryRun(1, func() { <-done })
			}()
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(done)
	l.Close()
}
