// Copyright 2025 The CubeFS Authors.
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

package qos

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLimiter(t *testing.T) {
	limiter := NewLimiter(0, 10)
	require.NotNil(t, limiter)
	assert.Equal(t, 0, limiter.limit)
	assert.NotNil(t, limiter.limiter)
	assert.NotNil(t, limiter.io)
	assert.False(t, limiter.IsEnabled())

	limiter = NewLimiter(100, 5)
	require.NotNil(t, limiter)
	assert.Equal(t, 100, limiter.limit)
	assert.NotNil(t, limiter.limiter)
	assert.NotNil(t, limiter.io)
	assert.False(t, limiter.IsEnabled())
}

func TestLimiter_EnableDisable(t *testing.T) {
	limiter := NewLimiter(100, 10)
	defer limiter.Close()

	assert.False(t, limiter.IsEnabled())

	limiter.Enable()
	assert.True(t, limiter.IsEnabled())

	limiter.Disable()
	assert.False(t, limiter.IsEnabled())

	limiter.Enable()
	limiter.Enable()
	assert.True(t, limiter.IsEnabled())

	limiter.Disable()
	limiter.Disable()
	assert.False(t, limiter.IsEnabled())
}

func TestLimiter_ResetLimit(t *testing.T) {
	limiter := NewLimiter(100, 10)
	defer limiter.Close()

	limiter.ResetLimit(200)
	assert.Equal(t, 200, limiter.limit)

	limiter.ResetLimit(0)
	assert.Equal(t, 0, limiter.limit)

	limiter.ResetLimit(-1)
	assert.Equal(t, -1, limiter.limit)
}

func TestLimiter_Run(t *testing.T) {
	limiter := NewLimiter(10, 5)
	defer limiter.Close()

	var executed int32

	// test limiter disable
	err := limiter.Run(1, false, func() {
		atomic.StoreInt32(&executed, 1)
	})

	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))

	// test limiter enable
	atomic.StoreInt32(&executed, 0)
	limiter.Enable()
	err = limiter.Run(1, false, func() {
		atomic.StoreInt32(&executed, 1)
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
}

func TestLimiter_TryRun(t *testing.T) {
	limiter := NewLimiter(1000, 2)
	defer limiter.Close()

	var executed int32

	ok := limiter.TryRun(1, func() {
		atomic.StoreInt32(&executed, 1)
		time.Sleep(10 * time.Millisecond)
	})

	assert.True(t, ok)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
}

func TestLimiter_TryRun_ConcurrencyLimit(t *testing.T) {
	limiter := NewLimiter(1000, 1)
	defer limiter.Close()

	var wg sync.WaitGroup
	var successCount int32
	var failureCount int32

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok := limiter.TryRun(1, func() {
				atomic.AddInt32(&successCount, 1)
				time.Sleep(100 * time.Millisecond)
			})
			if !ok {
				atomic.AddInt32(&failureCount, 1)
			}
		}()
	}

	wg.Wait()

	total := atomic.LoadInt32(&successCount) + atomic.LoadInt32(&failureCount)
	assert.Equal(t, int32(20), total)

	t.Logf("Success: %d, Failures: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&failureCount))
	assert.True(t, atomic.LoadInt32(&successCount) >= 1, "At least one operation should succeed")
}

func TestLimiter_Close(t *testing.T) {
	limiter := NewLimiter(100, 5)

	assert.NotPanics(t, func() {
		limiter.Close()
	})

	assert.NotPanics(t, func() {
		limiter.Close()
	})
}

func TestLimiter_ResetIO(t *testing.T) {
	limiter := NewLimiter(100, 5)
	defer limiter.Close()

	assert.NotPanics(t, func() {
		limiter.ResetIO(10, 1)
	})

	assert.NotPanics(t, func() {
		limiter.ResetIOEx(10, 1, 1000)
	})
}

func TestLimiter_ConcurrentOperations(t *testing.T) {
	limiter := NewLimiter(10, 10)
	defer limiter.Close()

	limiter.Enable()
	var wg sync.WaitGroup
	var successCount int32
	var errorCount int32

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.Run(1, false, func() {
				time.Sleep(1 * time.Millisecond)
				atomic.AddInt32(&successCount, 1)
			})
			if err != nil {
				atomic.AddInt32(&errorCount, 1)
			}
		}()
	}

	wg.Wait()

	assert.True(t, atomic.LoadInt32(&successCount) > 0)
	t.Logf("Success: %d, Errors: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&errorCount))
}

func TestLimiter_RateLimitingBehavior(t *testing.T) {
	limiter := NewLimiter(10, 20)
	defer limiter.Close()

	limiter.Enable()

	start := time.Now()
	var completed int32

	for i := 0; i < 50; i++ {
		go func() {
			limiter.AllocCheckLimit()
			err := limiter.Run(1, false, func() {
				atomic.AddInt32(&completed, 1)
			})
			assert.NoError(t, err)
		}()
	}

	time.Sleep(2 * time.Second)
	duration := time.Since(start)
	completedCount := atomic.LoadInt32(&completed)

	expectedMin := int32(duration.Seconds() * 5)
	expectedMax := int32(duration.Seconds() * 15)

	t.Logf("Completed %d operations in %v (expected %d-%d)", completedCount, duration, expectedMin, expectedMax)
	assert.True(t, completedCount >= expectedMin, "Should complete at least minimum expected operations")
	assert.True(t, completedCount <= expectedMax, "Should not exceed maximum expected operations significantly")
}

// Benchmark tests
func BenchmarkLimiter_Run_Disabled(b *testing.B) {
	limiter := NewLimiter(0, 100)
	defer limiter.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Run(1, false, func() {
				// Minimal work
			})
		}
	})
}

func BenchmarkLimiter_Run_Enabled(b *testing.B) {
	limiter := NewLimiter(10000, 100)
	defer limiter.Close()
	limiter.Enable()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Run(1, false, func() {
				// Minimal work
			})
		}
	})
}

func BenchmarkLimiter_TryRun(b *testing.B) {
	limiter := NewLimiter(10000, 100)
	defer limiter.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.TryRun(1, func() {
				// Minimal work
			})
		}
	})
}
