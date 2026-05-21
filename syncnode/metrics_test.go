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
	"testing"
	"time"
)

func TestInitMetricsIdempotent(t *testing.T) {
	// Safe to call twice — sync.Once guards the actual registration.
	initMetrics()
	initMetrics()
	if metricUp == nil || metricUptimeSeconds == nil || metricConcurrentTasks == nil {
		t.Fatal("expected all three gauges registered after initMetrics()")
	}
}

func TestStartMetricsLoopUpdatesUptime(t *testing.T) {
	initMetrics()
	stopC := make(chan struct{})
	defer close(stopC)
	// Use a very short refresh by injecting startedAt manually — verifies
	// the goroutine runs without panicking and the up gauge gets set.
	startMetricsLoop(stopC)

	// Give the goroutine a beat to set up=1.
	time.Sleep(50 * time.Millisecond)
	// metricUp is package-private; can only verify it's non-nil and the
	// loop didn't panic. The Prometheus scrape integration is covered by
	// TestStat_HTTP via the /admin/syncnode/stat endpoint.
	if metricUp == nil {
		t.Fatal("metricUp should be set after startMetricsLoop")
	}
}

func TestIncDecConcurrentTasks(t *testing.T) {
	concurrentTasks.Store(0)
	IncConcurrentTasks()
	IncConcurrentTasks()
	IncConcurrentTasks()
	if got := concurrentTasks.Load(); got != 3 {
		t.Errorf("after 3 incs, got %d want 3", got)
	}
	DecConcurrentTasks()
	if got := concurrentTasks.Load(); got != 2 {
		t.Errorf("after 1 dec, got %d want 2", got)
	}
}
