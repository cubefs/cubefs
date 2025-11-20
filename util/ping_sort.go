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

package util

import (
	"sort"
	"sync"
	"time"
)

type hostPingElapsed struct {
	host    string
	elapsed time.Duration
}

// PingElapsedSortedHosts provides a sorted list of hosts based on their ping latency.
// This is used for proximity-based host selection in both data and meta modules.
type PingElapsedSortedHosts struct {
	sortedHosts  []string
	updateTSUnix int64 // Timestamp (unix second) of latest update.
	getHosts     func() (hosts []string)
	getElapsed   func(host string) (elapsed time.Duration, ok bool)
}

// isNeedUpdate checks if the sorted hosts list needs to be updated.
// Updates are needed every 10 seconds or if never updated.
func (h *PingElapsedSortedHosts) isNeedUpdate() bool {
	return h.updateTSUnix == 0 || time.Now().Unix()-h.updateTSUnix > 10
}

// update refreshes the sorted hosts list based on current ping latencies.
func (h *PingElapsedSortedHosts) update(getHosts func() []string, getElapsed func(host string) (time.Duration, bool)) []string {
	hosts := getHosts()
	var withLatency, withoutLatency []*hostPingElapsed
	for _, host := range hosts {
		if elapsed, ok := getElapsed(host); ok {
			withLatency = append(withLatency, &hostPingElapsed{host: host, elapsed: elapsed})
		} else {
			withoutLatency = append(withoutLatency, &hostPingElapsed{host: host, elapsed: 0})
		}
	}
	sort.SliceStable(withLatency, func(i, j int) bool {
		return withLatency[i].elapsed < withLatency[j].elapsed
	})
	sorted := make([]string, 0, len(hosts))
	for _, item := range withLatency {
		sorted = append(sorted, item.host)
	}
	for _, item := range withoutLatency {
		sorted = append(sorted, item.host)
	}
	h.sortedHosts = sorted
	h.updateTSUnix = time.Now().Unix()
	return sorted
}

// GetSortedHosts returns the sorted list of hosts based on ping latency.
// The list is cached and updated every 10 seconds.
func (h *PingElapsedSortedHosts) GetSortedHosts() []string {
	if h.isNeedUpdate() {
		return h.update(h.getHosts, h.getElapsed)
	}
	return h.sortedHosts
}

// NewPingElapsedSortHosts creates a new PingElapsedSortedHosts instance.
func NewPingElapsedSortHosts(getHosts func() []string, getElapsed func(host string) (time.Duration, bool)) *PingElapsedSortedHosts {
	return &PingElapsedSortedHosts{
		getHosts:   getHosts,
		getElapsed: getElapsed,
	}
}

// AddressPingStats maintains ping statistics for a host with a sliding window of measurements.
type AddressPingStats struct {
	sync.Mutex
	durations []time.Duration
	index     int
}

// Add adds a new ping duration measurement to the statistics.
// Maintains a sliding window of up to 5 measurements.
func (as *AddressPingStats) Add(duration time.Duration) {
	as.Lock()
	defer as.Unlock()
	if as.index < 5 {
		as.durations = append(as.durations, duration)
	} else {
		as.durations[as.index%5] = duration
	}
	as.index++
}

// Average returns the average ping duration from all stored measurements.
// Returns 0 if no measurements are available.
func (as *AddressPingStats) Average() time.Duration {
	as.Lock()
	defer as.Unlock()
	if len(as.durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range as.durations {
		total += d
	}
	return total / time.Duration(len(as.durations))
}
