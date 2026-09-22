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

package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestBuildHourTable(t *testing.T) {
	// single normal window [9, 18)
	ht, err := buildHourTable([]QPSHourRange{{From: 9, To: 18, QPS: 50}})
	require.NoError(t, err)
	for h := 0; h < 24; h++ {
		if h >= 9 && h < 18 {
			require.Equal(t, 0, ht[h], "hour %d should be covered", h)
		} else {
			require.Equal(t, noSegment, ht[h], "hour %d should be uncovered", h)
		}
	}

	// cross-night window [22, 6) covers 22,23,0..5
	ht, err = buildHourTable([]QPSHourRange{{From: 22, To: 6, QPS: 200}})
	require.NoError(t, err)
	cross := map[int]bool{22: true, 23: true, 0: true, 1: true, 2: true, 3: true, 4: true, 5: true}
	for h := 0; h < 24; h++ {
		if cross[h] {
			require.Equal(t, 0, ht[h])
		} else {
			require.Equal(t, noSegment, ht[h])
		}
	}

	// full day by two windows
	ht, err = buildHourTable([]QPSHourRange{{From: 0, To: 9, QPS: 100}, {From: 9, To: 24, QPS: 200}})
	require.NoError(t, err)
	for h := 0; h < 24; h++ {
		require.NotEqual(t, noSegment, ht[h], "hour %d should be covered", h)
	}
	require.Equal(t, 0, ht[0])
	require.Equal(t, 1, ht[9])
	require.Equal(t, 1, ht[23])

	// invalid: from == to (empty window)
	_, err = buildHourTable([]QPSHourRange{{From: 5, To: 5, QPS: 100}})
	require.ErrorContains(t, err, "invalid delete_rate_schedule")

	// invalid: qps negative (qps 0 is valid and means block)
	_, err = buildHourTable([]QPSHourRange{{From: 0, To: 6, QPS: -1}})
	require.ErrorContains(t, err, "invalid delete_rate_schedule")

	// invalid: from < 0
	_, err = buildHourTable([]QPSHourRange{{From: -1, To: 6, QPS: 100}})
	require.ErrorContains(t, err, "invalid delete_rate_schedule")

	// invalid: to > 24
	_, err = buildHourTable([]QPSHourRange{{From: 0, To: 25, QPS: 100}})
	require.ErrorContains(t, err, "invalid delete_rate_schedule")

	// overlap: two normal windows overlap at hour 10,11
	_, err = buildHourTable([]QPSHourRange{{From: 0, To: 12, QPS: 100}, {From: 10, To: 20, QPS: 200}})
	require.ErrorContains(t, err, "overlapped delete_rate_schedule")

	// overlap: cross-night [22,6) overlaps [5,10) at hour 5
	_, err = buildHourTable([]QPSHourRange{{From: 22, To: 6, QPS: 200}, {From: 5, To: 10, QPS: 100}})
	require.ErrorContains(t, err, "overlapped delete_rate_schedule")
}

func TestRateScheduleAllowDeleteAndSwitch(t *testing.T) {
	// three segments covering the whole day
	segs := []QPSHourRange{
		{From: 0, To: 9, QPS: 100},   // off-peak boost: hours 0..8
		{From: 9, To: 22, QPS: 50},   // peak slow-down: hours 9..21
		{From: 22, To: 24, QPS: 200}, // night boost: hours 22,23
	}
	ht, err := buildHourTable(segs)
	require.NoError(t, err)

	mgr := &BlobDeleteMgr{
		rateSchedule:  segs,
		hourTable:     ht,
		currentQPS:    0,
		deleteLimiter: rate.NewLimiter(rate.Limit(0), 0),
	}

	day := time.Date(2026, 9, 16, 0, 0, 0, 0, time.Local)
	cases := []struct {
		hour int
		qps  rate.Limit
	}{
		{0, 100},
		{3, 100},
		{8, 100},
		{9, 50},
		{12, 50},
		{21, 50},
		{22, 200},
		{23, 200},
	}
	for _, c := range cases {
		now := day.Add(time.Duration(c.hour) * time.Hour)
		require.True(t, mgr.allowDelete(now), "hour %d should allow delete", c.hour)
		require.Equal(t, c.qps, mgr.deleteLimiter.Limit(), "hour %d wrong limit", c.hour)
		require.Equal(t, int(c.qps), mgr.currentQPS, "hour %d wrong currentQPS", c.hour)
	}

	// re-querying the last tier (hour 23, qps 200 == currentQPS) is a no-op
	prev := mgr.currentQPS
	mgr.allowDelete(day.Add(23 * time.Hour))
	require.Equal(t, prev, mgr.currentQPS)
}

func TestRateScheduleUncoveredUsesDefault(t *testing.T) {
	// only [9,18) is covered; other hours fall back to the default QPS
	segs := []QPSHourRange{{From: 9, To: 18, QPS: 50}}
	ht, err := buildHourTable(segs)
	require.NoError(t, err)

	mgr := &BlobDeleteMgr{
		rateSchedule:  segs,
		hourTable:     ht,
		currentQPS:    50,
		defaultQPS:    100,
		deleteLimiter: rate.NewLimiter(rate.Limit(50), 50),
	}

	day := time.Date(2026, 9, 16, 0, 0, 0, 0, time.Local)
	// uncovered hour 3 -> allowed at the default QPS
	require.True(t, mgr.allowDelete(day.Add(3*time.Hour)))
	require.Equal(t, rate.Limit(100), mgr.deleteLimiter.Limit())
	require.Equal(t, 100, mgr.currentQPS)
	// covered hour 12 -> allowed, limiter back to the segment QPS
	require.True(t, mgr.allowDelete(day.Add(12*time.Hour)))
	require.Equal(t, rate.Limit(50), mgr.deleteLimiter.Limit())
	require.Equal(t, 50, mgr.currentQPS)
}

func TestRateScheduleZeroQPSBlocks(t *testing.T) {
	// [0,6) blocks deletion (qps 0); [6,24) allows at 100
	segs := []QPSHourRange{
		{From: 0, To: 6, QPS: 0},
		{From: 6, To: 24, QPS: 100},
	}
	ht, err := buildHourTable(segs)
	require.NoError(t, err)

	mgr := &BlobDeleteMgr{
		rateSchedule:  segs,
		hourTable:     ht,
		currentQPS:    100,
		defaultQPS:    100,
		deleteLimiter: rate.NewLimiter(rate.Limit(100), 100),
	}

	day := time.Date(2026, 9, 16, 0, 0, 0, 0, time.Local)
	// hour 3 falls in the qps-0 segment -> blocked, limiter untouched
	require.False(t, mgr.allowDelete(day.Add(3*time.Hour)))
	require.Equal(t, rate.Limit(100), mgr.deleteLimiter.Limit())
	require.Equal(t, 100, mgr.currentQPS)
	// hour 12 falls in the 100-qps segment -> allowed, limiter stays 100
	require.True(t, mgr.allowDelete(day.Add(12*time.Hour)))
	require.Equal(t, rate.Limit(100), mgr.deleteLimiter.Limit())
	require.Equal(t, 100, mgr.currentQPS)
}
