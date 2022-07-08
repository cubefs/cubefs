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

package raftserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitTime(t *testing.T) {
	wt := NewTimeList()
	var chs [10]<-chan struct{}

	for i := 0; i < 10; i++ {
		chs[i] = wt.Wait(uint64(i + 1))
	}

	ch := wt.Wait(8)
	require.Equal(t, chs[7], ch)
	wt.Wait(20)
	ch19 := wt.Wait(19)
	wt.Trigger(30)
	v := false
	select {
	case <-ch19:
		v = true
	default:
	}
	assert.True(t, v)

	for i := 0; i < 10; i++ {
		v = false
		select {
		case <-chs[i]:
			v = true
		default:
		}
		assert.True(t, v)
	}

	ch = wt.Wait(29)
	v = false
	select {
	case <-ch:
		v = true
	default:
	}
	assert.True(t, v)
	for i := 0; i < 10; i++ {
		wt.Wait(uint64(30 + i))
	}
	wt.Trigger(35)
	assert.Equal(t, 4, len(wt.(*timeList).items))
}
