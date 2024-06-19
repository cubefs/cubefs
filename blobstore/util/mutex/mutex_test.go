// Copyright 2024 The CubeFS Authors.
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

package mutex_test

import (
	"errors"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/mutex"
	"github.com/stretchr/testify/require"
)

func TestMutexWith(t *testing.T) {
	var m mutex.Mutex
	val := -1
	f := func() error {
		val++
		switch val {
		case 0:
			return nil
		case 1:
			return mutex.Nil
		default:
			return errors.New("")
		}
	}
	require.Nil(t, m.WithLock(f))
	nilErr := m.WithLock(f)
	require.ErrorIs(t, nilErr, mutex.Nil)
	require.Equal(t, mutex.Nil, nilErr)
	require.Error(t, m.WithLock(f))
}

func Benchmark_Span_Assertion(b *testing.B) {
	var m mutex.RWMutex
	f := func() error { return nil }
	b.Run("Lock", func(b *testing.B) {
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			m.Lock()
			_ = struct{}{}
			m.Unlock()
		}
	})
	b.Run("WithLock", func(b *testing.B) {
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			m.WithLock(f)
		}
	})

	b.Run("RLock", func(b *testing.B) {
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			m.RLock()
			_ = struct{}{}
			m.RUnlock()
		}
	})
	b.Run("WithRLock", func(b *testing.B) {
		b.ResetTimer()
		for ii := 0; ii <= b.N; ii++ {
			m.WithRLock(f)
		}
	})
}
