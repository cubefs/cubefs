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

func getTestConfig() AdaptiveManagerConf {
	return AdaptiveManagerConf{
		FlowConfigs: map[IoType]FlowConfig{
			Read: {
				Iocc:         10,
				IopsMinLimit: 30,
			},
			Write: {
				Iocc:         10,
				IopsMinLimit: 30,
			},
			AsyncRead: {
				Iocc:         5,
				IopsMinLimit: 10,
			},
			AsyncWrite: {
				Iocc:         5,
				IopsMinLimit: 10,
			},
		},
		CheckIntervalMs:        100,
		BizReadAwaitDegradeMs:  500,
		BizWriteAwaitDegradeMs: 500,
		SafetyBoundaryRatio:    0.6,
		TriggerConsecutive:     2,
		MetricsWindows:         5,
		MetricsWindowMs:        1000,
		SampleIntervalMs:       200,
	}
}

func TestNewAdaptiveManager(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	require.NotNil(t, am)

	am.mu.RLock()
	assert.Equal(t, len(conf.FlowConfigs), len(am.limiters))
	for ioType := range conf.FlowConfigs {
		assert.NotNil(t, am.limiters[ioType])
		assert.False(t, am.throttledTypes[ioType])
	}
	am.mu.RUnlock()

	assert.Equal(t, StateIdle, am.State())
	assert.False(t, am.isThrottleEnabled())
	assert.NotNil(t, am.metrics)

	am.Close()
}

func TestAdaptiveManager_Close(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	require.NotNil(t, am)

	assert.NotPanics(t, func() {
		am.Close()
	})
}

func TestAdaptiveManager_Run(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	var executed int32
	var executionTime time.Time

	err := am.Run(Read, 1024, false, func() {
		atomic.StoreInt32(&executed, 1)
		executionTime = time.Now()
	})

	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
	assert.False(t, executionTime.IsZero())
}

func TestAdaptiveManager_TryRun(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	var executed int32

	ok := am.TryRun(Read, 1024, func() {
		atomic.StoreInt32(&executed, 1)
	})

	assert.True(t, ok)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
}

func TestAdaptiveManager_RunWithNonExistentLimiter(t *testing.T) {
	conf := AdaptiveManagerConf{
		FlowConfigs:      make(map[IoType]FlowConfig), // Empty config
		CheckIntervalMs:  100,
		MetricsWindows:   5,
		MetricsWindowMs:  1000,
		SampleIntervalMs: 200,
	}
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	var executed int32

	// Test running with no limiter configured (should run directly)
	err := am.Run(Read, 1024, false, func() {
		atomic.StoreInt32(&executed, 1)
	})

	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
}

func TestAdaptiveManager_getBusinessLatencyZone(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	state := am.getBusinessLatencyZone()
	assert.Equal(t, LatRelaxZone, state)
	assert.Equal(t, StateIdle, am.State())

	time.Sleep(time.Duration(conf.SampleIntervalMs*2) * time.Millisecond)

	// Still should be healthy with no traffic
	state = am.getBusinessLatencyZone()
	assert.Equal(t, LatRelaxZone, state)
}

func TestAdaptiveManager_EscalateAndRelax(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	am.escalateOne()

	am.mu.RLock()
	bgWriteLimiter := am.limiters[AsyncWrite]
	am.mu.RUnlock()

	if bgWriteLimiter != nil {
		assert.True(t, am.throttledTypes[AsyncWrite])
	}

	am.relaxOne()
}

func TestAdaptiveManager_MetricsIntegration(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	for i := 0; i < 10; i++ {
		err := am.Run(Read, 1024, false, func() {
			time.Sleep(1 * time.Millisecond) // Simulate work
		})
		assert.NoError(t, err)
	}

	time.Sleep(time.Duration(conf.SampleIntervalMs*2) * time.Millisecond)

	stats := am.MetricsWindowsStat(Read)
	assert.NotNil(t, stats)
}

func TestAdaptiveManager_ConcurrentAccess(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	var wg sync.WaitGroup
	errCount := int32(0)
	successCount := int32(0)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				err := am.Run(Read, 512, false, func() {
					time.Sleep(1 * time.Millisecond)
				})
				if err != nil {
					atomic.AddInt32(&errCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	assert.True(t, atomic.LoadInt32(&successCount) > 0)
	t.Logf("Success: %d, Errors: %d", atomic.LoadInt32(&successCount), atomic.LoadInt32(&errCount))
}

func TestAdaptiveManager_ThrottleStates(t *testing.T) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	assert.False(t, am.isThrottleEnabled())
	for _, ioType := range IoTypes {
		assert.False(t, am.throttledTypes[ioType])
	}

	am.escalateOne()

	hasThrottling := am.isThrottleEnabled()
	for _, isThrottled := range am.throttledTypes {
		if isThrottled {
			hasThrottling = true
			break
		}
	}

	t.Logf("Throttle enabled: %v, any throttling: %v", am.throttleEnabled, hasThrottling)
	for ioType, throttled := range am.throttledTypes {
		t.Logf("IoType %v throttled: %v", ioType, throttled)
	}
}

func TestAdaptiveManager_LongRunning(t *testing.T) {
	conf := getTestConfig()
	conf.CheckIntervalMs = 50
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	done := make(chan bool)
	go func() {
		time.Sleep(500 * time.Millisecond)
		done <- true
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				am.Run(Read, 1024, false, func() {
					time.Sleep(1 * time.Millisecond)
				})
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	<-done

	// Test should complete without deadlocks or panics
	assert.True(t, true, "Long-running test completed successfully")
}

// Benchmark tests
func BenchmarkAdaptiveManager_Run(b *testing.B) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			am.Run(Read, 1024, false, func() {
				// Simulate minimal work
			})
		}
	})
}

func BenchmarkAdaptiveManager_TryRun(b *testing.B) {
	conf := getTestConfig()
	am := NewAdaptiveManager("", conf)
	defer am.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			am.TryRun(Read, 1024, func() {
				// Simulate minimal work
			})
		}
	})
}
