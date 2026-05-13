// Copyright 2026 The CubeFS Authors.
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

package syncnode

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/exporter"
)

// Phase A node-level gauges (§13.2). More metrics (task / backend layer)
// will land alongside the executor and backend implementations in later
// phases.
var (
	metricUp              *exporter.Gauge
	metricUptimeSeconds   *exporter.Gauge
	metricConcurrentTasks *exporter.Gauge

	metricsOnce sync.Once

	startedAt       time.Time
	concurrentTasks atomic.Int64
)

// initMetrics registers the node-level gauges with the global exporter.
// Idempotent: safe to call multiple times (tests, restarts).
func initMetrics() {
	metricsOnce.Do(func() {
		metricUp = exporter.NewGauge("cubefs_syncnode_up")
		metricUptimeSeconds = exporter.NewGauge("cubefs_syncnode_uptime_seconds")
		metricConcurrentTasks = exporter.NewGauge("cubefs_syncnode_concurrent_tasks")
	})
}

// startMetricsLoop sets the static "up" gauge and starts a goroutine that
// updates uptime + concurrent_tasks on a tick. Goroutine exits when stopC
// is closed.
func startMetricsLoop(stopC <-chan struct{}) {
	startedAt = time.Now()
	if metricUp != nil {
		metricUp.Set(1)
	}
	go func() {
		t := time.NewTicker(time.Duration(metricsRefreshInterval) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopC:
				if metricUp != nil {
					metricUp.Set(0)
				}
				return
			case <-t.C:
				if metricUptimeSeconds != nil {
					metricUptimeSeconds.Set(time.Since(startedAt).Seconds())
				}
				if metricConcurrentTasks != nil {
					metricConcurrentTasks.Set(float64(concurrentTasks.Load()))
				}
			}
		}
	}()
}

// IncConcurrentTasks / DecConcurrentTasks are called by task executor (Phase D)
// when a task enters / leaves the running state. Exposed package-level so
// future executor code can update the gauge without owning the metrics layer.
func IncConcurrentTasks() { concurrentTasks.Add(1) }
func DecConcurrentTasks() { concurrentTasks.Add(-1) }
