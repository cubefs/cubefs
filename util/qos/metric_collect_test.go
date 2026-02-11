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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWindowedSeries(t *testing.T) {
	windowsNum := 5
	winSize := 100 * time.Millisecond

	ws := newWindowedSeries(windowsNum, winSize)
	require.NotNil(t, ws)

	assert.Equal(t, windowsNum+1, len(ws.windows))
	assert.Equal(t, 0, ws.idx)
	assert.Equal(t, winSize, ws.winSize)
	assert.False(t, ws.winStart.IsZero())

	for i, win := range ws.windows {
		assert.Equal(t, metricWindow{}, win, "Window %d should be zero-initialized", i)
	}
}

func TestWindowedSeries_AddOp(t *testing.T) {
	ws := newWindowedSeries(3, 100*time.Millisecond)

	ws.addOp(1024, 50*time.Millisecond, false)

	ws.mu.Lock()
	w := ws.windows[ws.idx]
	ws.mu.Unlock()

	assert.Equal(t, int64(1), w.opCnt)
	assert.Equal(t, int64(1024), w.byteSize)
	assert.Equal(t, int64(50*time.Millisecond), w.latencyNs)
	assert.Equal(t, int64(1), w.successCnt)
	assert.Equal(t, int64(0), w.errorCnt)

	ws.addOp(512, 25*time.Millisecond, true)

	ws.mu.Lock()
	w = ws.windows[ws.idx]
	ws.mu.Unlock()

	assert.Equal(t, int64(2), w.opCnt)
	assert.Equal(t, int64(1536), w.byteSize)
	assert.Equal(t, int64(75*time.Millisecond), w.latencyNs)
	assert.Equal(t, int64(1), w.successCnt)
	assert.Equal(t, int64(1), w.errorCnt)
}

func TestWindowedSeries_AddSample(t *testing.T) {
	ws := newWindowedSeries(3, 100*time.Millisecond)

	ws.addSample(5, 3) //, nil

	ws.mu.Lock()
	w := ws.windows[ws.idx]
	ws.mu.Unlock()

	assert.Equal(t, int64(1), w.sampleCnt)
	assert.Equal(t, int64(5), w.queueSum)
	assert.Equal(t, int64(3), w.runSum)
	assert.Equal(t, int64(5), w.queueMax)
	assert.Equal(t, int64(3), w.runMax)

	ws.addSample(8, 2) //, nil

	ws.mu.Lock()
	w = ws.windows[ws.idx]
	ws.mu.Unlock()

	assert.Equal(t, int64(2), w.sampleCnt)
	assert.Equal(t, int64(13), w.queueSum)
	assert.Equal(t, int64(5), w.runSum)
	assert.Equal(t, int64(8), w.queueMax)
	assert.Equal(t, int64(3), w.runMax)
}

func TestWindowedSeries_Rotate(t *testing.T) {
	ws := newWindowedSeries(3, 50*time.Millisecond)

	ws.addOp(1024, 10*time.Millisecond, false)
	initialIdx := ws.idx

	time.Sleep(60 * time.Millisecond)
	ws.rotate(time.Now())

	assert.NotEqual(t, initialIdx, ws.idx)

	ws.mu.Lock()
	currentWin := ws.windows[ws.idx]
	ws.mu.Unlock()
	assert.Equal(t, metricWindow{}, currentWin)
}

func TestWindowedSeries_GetHistoryMetrics(t *testing.T) {
	ws := newWindowedSeries(3, 100*time.Millisecond)

	for i := 0; i < 3; i++ {
		ws.addOp(1024*(i+1), time.Duration(i+1)*10*time.Millisecond, false)
		if i < 2 {
			ws.idx = (ws.idx + 1) % len(ws.windows)
			ws.windows[ws.idx] = metricWindow{}
		}
	}

	history := ws.getHistoryMetrics()
	assert.Equal(t, 3, len(history))

	assert.Equal(t, int64(0), history[0].byteSize)
	assert.Equal(t, int64(1024), history[1].byteSize)
	assert.Equal(t, int64(2048), history[2].byteSize)
}

func TestNewMetricsCollector(t *testing.T) {
	windowsNum := 5
	winSize := 100 * time.Millisecond
	sampleInterval := 20 * time.Millisecond
	ioTypes := []IoType{Read, Write, AsyncRead}

	mc := newMetricsCollector(windowsNum, winSize, sampleInterval, ioTypes)
	require.NotNil(t, mc)
	defer mc.close()

	assert.Equal(t, winSize, mc.winSize)
	assert.Equal(t, sampleInterval, mc.sampleInterval)
	assert.Equal(t, len(ioTypes), len(mc.flowMetricsMap))

	for _, ioType := range ioTypes {
		assert.NotNil(t, mc.flowMetricsMap[ioType])
	}

	assert.NotNil(t, &mc.ioWaiting)
	assert.NotNil(t, &mc.ioRunning)
}

func TestMetricsCollector_Counters(t *testing.T) {
	mc := newMetricsCollector(3, 100*time.Millisecond, 50*time.Millisecond, []IoType{Read})
	defer mc.close()

	// Test increment/decrement operations
	mc.incWaiting(Read)
	mc.incRunning(Read)

	waiting, running := mc.currIoWaitingAndIoRunning(Read)
	assert.Equal(t, 1, waiting)
	assert.Equal(t, 1, running)

	mc.decWaiting(Read)
	mc.decRunning(Read)

	waiting, running = mc.currIoWaitingAndIoRunning(Read)
	assert.Equal(t, 0, waiting)
	assert.Equal(t, 0, running)
}

func TestMetricsCollector_AddOp(t *testing.T) {
	mc := newMetricsCollector(3, 100*time.Millisecond, 50*time.Millisecond, []IoType{Read})
	defer mc.close()

	mc.addOp(Read, 1024, 50*time.Millisecond, false)

	assert.Eventually(t, func() bool {
		mc.rotateAll()
		windows := mc.windows(Read)
		for _, win := range windows {
			if win.opCnt > 0 {
				return win.opCnt == 1 &&
					win.byteSize == 1024 &&
					win.latencyNs == int64(50*time.Millisecond) &&
					win.successCnt == 1 &&
					win.errorCnt == 0
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond, "Operation should be recorded in one of the windows")
}

func TestMetricsCollector_AddReject(t *testing.T) {
	mc := newMetricsCollector(3, 100*time.Millisecond, 50*time.Millisecond, []IoType{Read})
	defer mc.close()

	mc.addReject(Read)

	assert.Eventually(t, func() bool {
		mc.rotateAll()
		windows := mc.windows(Read)
		for _, win := range windows {
			if win.rejectCnt > 0 {
				return win.rejectCnt == 1
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond, "Reject should be recorded in one of the windows")
}

func TestMetricsCollector_WindowsStats(t *testing.T) {
	mc := newMetricsCollector(3, 1*time.Second, 200*time.Millisecond, []IoType{Read})
	defer mc.close()

	// Add some operations and samples
	mc.addOp(Read, 1024, 100*time.Millisecond, false)
	mc.addOp(Read, 2048, 200*time.Millisecond, false)
	mc.addOp(Read, 512, 50*time.Millisecond, true) // error

	// Add samples for queue depth
	ws := mc.flowMetricsMap[Read]
	ws.addSample(5, 3) //, nil
	ws.addSample(8, 2) //, nil

	// Get window stats
	stats := mc.metricsWindowsStat(Read)
	require.NotNil(t, stats)
	require.True(t, len(stats) > 0)

	// Find the window with data
	var stat WindowStat
	found := false
	for _, s := range stats {
		if s.Iops > 0 || s.Bps > 0 {
			stat = s
			found = true
			break
		}
	}

	if found {
		assert.Equal(t, int64(3), stat.Iops)
		assert.Equal(t, int64(1024+2048+512), stat.Bps)
		assert.Equal(t, int64((1024+2048+512)/3), stat.Avgrq)
		assert.Equal(t, int64((100+200+50)*time.Millisecond.Nanoseconds()/3), stat.Await)
		assert.Equal(t, int64((5+8)/2), stat.Avgqu)
		assert.Equal(t, int64(8), stat.QMax)
		assert.Equal(t, float64(2)/float64(3), stat.SuccessRate)
		assert.Equal(t, float64(1)/float64(3), stat.ErrorRate)
	}
}

func TestMetricsCollector_ConcurrentAccess(t *testing.T) {
	mc := newMetricsCollector(3, 100*time.Millisecond, 50*time.Millisecond, []IoType{Read, Write})
	defer mc.close()

	var wg sync.WaitGroup

	// Concurrent operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				mc.incWaiting(Read)
				time.Sleep(5 * time.Millisecond) // Simulating the process of waiting in the queue
				mc.decWaiting(Read)
				mc.incRunning(Read)
				time.Sleep(10 * time.Millisecond) // Simulating the execution process of an I/O task
				mc.decRunning(Read)
				mc.addOp(Read, 1024, time.Duration(j+1)*10*time.Millisecond, j%3 == 0)
			}
		}(i)
	}

	wg.Wait()

	// Should not panic and should have some recorded operations
	windows := mc.windows(Read)
	assert.NotNil(t, windows)
}

func TestMetricsCollector_RotateAndSample(t *testing.T) {
	mc := newMetricsCollector(3, 50*time.Millisecond, 10*time.Millisecond, []IoType{Read})
	defer mc.close()

	mc.incWaiting(Read)
	mc.incRunning(Read)

	mc.rotateAndSample()

	assert.Eventually(t, func() bool {
		mc.rotateAll()
		windows := mc.windows(Read)
		for _, win := range windows {
			if win.sampleCnt > 0 {
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond, "Samples should be recorded after rotateAndSample")
}

func TestMetricsCollector_EmptyWindows(t *testing.T) {
	mc := newMetricsCollector(3, 100*time.Millisecond, 50*time.Millisecond, []IoType{Read})
	defer mc.close()

	stats := mc.metricsWindowsStat(Read)
	if len(stats) > 0 {
		for _, stat := range stats {
			if stat.Iops == 0 && stat.Bps == 0 {
				assert.Equal(t, int64(0), stat.Avgrq)
				assert.Equal(t, int64(0), stat.Avgqu)
				assert.Equal(t, int64(0), stat.Await)
				assert.Equal(t, float64(0), stat.SuccessRate)
				assert.Equal(t, float64(0), stat.ErrorRate)
			}
		}
	}
}

func TestMetricsCollector_NonExistentIoType(t *testing.T) {
	mc := newMetricsCollector(3, 100*time.Millisecond, 50*time.Millisecond, []IoType{Read})
	defer mc.close()

	mc.addOp(Write, 1024, 100*time.Millisecond, false) // Write not in ioTypes
	mc.addReject(Write)

	waiting, running := mc.currIoWaitingAndIoRunning(Write)
	assert.Equal(t, 0, waiting)
	assert.Equal(t, 0, running)

	windows := mc.windows(Write)
	assert.Nil(t, windows)

	stats := mc.metricsWindowsStat(Write)
	assert.Nil(t, stats)
}

// Benchmark tests
func BenchmarkMetricsCollector_AddOp(b *testing.B) {
	mc := newMetricsCollector(5, 1*time.Second, 100*time.Millisecond, []IoType{Read})
	defer mc.close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mc.addOp(Read, 1024, 50*time.Millisecond, false)
		}
	})
}

func BenchmarkMetricsCollector_Counters(b *testing.B) {
	mc := newMetricsCollector(5, 1*time.Second, 100*time.Millisecond, []IoType{Read})
	defer mc.close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mc.incWaiting(Read)
			mc.decWaiting(Read)
		}
	})
}

func BenchmarkMetricsCollector_WindowStats(b *testing.B) {
	mc := newMetricsCollector(5, 1*time.Second, 100*time.Millisecond, []IoType{Read})
	defer mc.close()

	for i := 0; i < 100; i++ {
		mc.addOp(Read, 1024, 50*time.Millisecond, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc.metricsWindowsStat(Read)
	}
}
