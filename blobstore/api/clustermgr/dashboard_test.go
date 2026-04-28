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

package clustermgr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxScore(t *testing.T) {
	require.Equal(t, DashboardScoreCritical, DashboardScoreOK.Max(DashboardScoreCritical))
	require.Equal(t, DashboardScoreMajor, DashboardScoreMajor.Max(DashboardScoreWarning))
	require.Equal(t, DashboardScoreOK, DashboardScoreOK.Max(DashboardScoreOK))
}

// rawStat is a test helper alias for the inner map type of DiskStat.
type rawStat = map[string]map[string]DiskEntry

func diskScore(raw rawStat) DashboardScore {
	s := DiskStat{ByStatusIDC: raw}
	s.CalcScore()
	return s.Score
}

func TestDiskScore(t *testing.T) {
	require.Equal(t, DashboardScoreOK, diskScore(nil))
	require.Equal(t, DashboardScoreOK, diskScore(rawStat{}))

	require.Equal(t, DashboardScoreOK, diskScore(rawStat{
		"normal":    {"idc1": {Count: 500}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 9/100 = 9% < 10% → OK
	require.Equal(t, DashboardScoreOK, diskScore(rawStat{
		"broken":    {"idc1": {Count: 9}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// 10/100 = 10% = threshold → Notice
	require.Equal(t, DashboardScoreNotice, diskScore(rawStat{
		"broken":    {"idc1": {Count: 10}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// 20/100 = 20% = 2×threshold → Warning
	require.Equal(t, DashboardScoreWarning, diskScore(rawStat{
		"broken":    {"idc1": {Count: 20}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// 30/100 = 30% = 3×threshold → Major
	require.Equal(t, DashboardScoreMajor, diskScore(rawStat{
		"broken":    {"idc1": {Count: 30}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// broken+repairing both count as unavailable: 5+5=10/100=10% → Notice
	require.Equal(t, DashboardScoreNotice, diskScore(rawStat{
		"broken":    {"idc1": {Count: 5}},
		"repairing": {"idc1": {Count: 5}},
		"__total__": {"idc1": {Count: 100}},
	}))

	// 24/500 = 4.8% < 5% → OK
	require.Equal(t, DashboardScoreOK, diskScore(rawStat{
		"broken":    {"idc1": {Count: 24}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 25/500 = 5% → Notice
	require.Equal(t, DashboardScoreNotice, diskScore(rawStat{
		"broken":    {"idc1": {Count: 25}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 50/500 = 10% = 2×5% → Warning
	require.Equal(t, DashboardScoreWarning, diskScore(rawStat{
		"broken":    {"idc1": {Count: 50}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 15+10=25 / 250+250=500 = 5% → Notice
	require.Equal(t, DashboardScoreNotice, diskScore(rawStat{
		"broken":    {"idc1": {Count: 15}, "idc2": {Count: 10}},
		"__total__": {"idc1": {Count: 250}, "idc2": {Count: 250}},
	}))

	// 19/2000 = 0.95% < 1% → OK
	require.Equal(t, DashboardScoreOK, diskScore(rawStat{
		"broken":    {"idc1": {Count: 19}},
		"__total__": {"idc1": {Count: 2000}},
	}))
	// 20/2000 = 1% → Notice
	require.Equal(t, DashboardScoreNotice, diskScore(rawStat{
		"broken":    {"idc1": {Count: 20}},
		"__total__": {"idc1": {Count: 2000}},
	}))
	// 60/2000 = 3% = 3×1% → Major
	require.Equal(t, DashboardScoreMajor, diskScore(rawStat{
		"broken":    {"idc1": {Count: 60}},
		"__total__": {"idc1": {Count: 2000}},
	}))
}
