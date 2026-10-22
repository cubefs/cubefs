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

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestMaxScore(t *testing.T) {
	require.Equal(t, DashboardScoreCritical, DashboardScore{}.Max(DashboardScore{Score: DashboardScoreCritical}).Score)
	require.Equal(t, DashboardScoreMajor, DashboardScore{Score: DashboardScoreMajor}.Max(DashboardScore{Score: DashboardScoreWarning}).Score)
	require.Equal(t, DashboardScoreOK, DashboardScore{}.Max(DashboardScore{Score: DashboardScoreOK}).Score)
}

// rawStat is a test helper alias for the inner map type of DiskStat.
type rawStat = map[string]map[string]DiskEntry

func diskScoreLevel(raw rawStat) int {
	s := DiskStat{ByStatusIDC: raw}
	s.CalcScore()
	return s.Score.Score
}

func TestDiskScore(t *testing.T) {
	require.Equal(t, DashboardScoreOK, diskScoreLevel(nil))
	require.Equal(t, DashboardScoreOK, diskScoreLevel(rawStat{}))

	require.Equal(t, DashboardScoreOK, diskScoreLevel(rawStat{
		"normal":    {"idc1": {Count: 500}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 9/100 = 9% < 10% → OK
	require.Equal(t, DashboardScoreOK, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 9}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// 10/100 = 10% = threshold → Notice
	require.Equal(t, DashboardScoreNotice, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 10}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// 20/100 = 20% = 2×threshold → Warning
	require.Equal(t, DashboardScoreWarning, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 20}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// 30/100 = 30% = 3×threshold → Major
	require.Equal(t, DashboardScoreMajor, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 30}},
		"__total__": {"idc1": {Count: 100}},
	}))
	// broken+repairing both count as unavailable: 5+5=10/100=10% → Notice
	require.Equal(t, DashboardScoreNotice, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 5}},
		"repairing": {"idc1": {Count: 5}},
		"__total__": {"idc1": {Count: 100}},
	}))

	// 24/500 = 4.8% < 5% → OK
	require.Equal(t, DashboardScoreOK, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 24}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 25/500 = 5% → Notice
	require.Equal(t, DashboardScoreNotice, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 25}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 50/500 = 10% = 2×5% → Warning
	require.Equal(t, DashboardScoreWarning, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 50}},
		"__total__": {"idc1": {Count: 500}},
	}))
	// 15+10=25 / 250+250=500 = 5% → Notice
	require.Equal(t, DashboardScoreNotice, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 15}, "idc2": {Count: 10}},
		"__total__": {"idc1": {Count: 250}, "idc2": {Count: 250}},
	}))

	// 19/2000 = 0.95% < 1% → OK
	require.Equal(t, DashboardScoreOK, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 19}},
		"__total__": {"idc1": {Count: 2000}},
	}))
	// 20/2000 = 1% → Notice
	require.Equal(t, DashboardScoreNotice, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 20}},
		"__total__": {"idc1": {Count: 2000}},
	}))
	// 60/2000 = 3% = 3×1% → Major
	require.Equal(t, DashboardScoreMajor, diskScoreLevel(rawStat{
		"broken":    {"idc1": {Count: 60}},
		"__total__": {"idc1": {Count: 2000}},
	}))
}

type typeIDC = map[string]map[string]int

func makeSvcStat(m typeIDC, expiredDisks int) ServiceStat {
	return ServiceStat{OnlineByTypeIDC: m, ExpiredDisks: expiredDisks}
}

var healthyIDC = typeIDC{
	proto.ServiceNameProxy:     {"idc1": 2, "idc2": 2},
	proto.ServiceNameScheduler: {"idc1": 1},
	proto.ServiceNameWorker:    {"idc1": 2},
	proto.ServiceNameBlobNode:  {"idc1": 3},
}

func TestServiceScore_ServiceOnline(t *testing.T) {
	cases := []struct {
		name  string
		m     typeIDC
		score int
	}{
		{
			name:  "no services → Major",
			m:     nil,
			score: DashboardScoreMajor,
		},
		{
			name:  "all healthy → OK",
			m:     healthyIDC,
			score: DashboardScoreOK,
		},
		// PROXY
		{
			name: "proxy 1 per idc → Warning",
			m: typeIDC{
				proto.ServiceNameProxy:     {"idc1": 1, "idc2": 1},
				proto.ServiceNameScheduler: {"idc1": 1},
				proto.ServiceNameWorker:    {"idc1": 2},
				proto.ServiceNameBlobNode:  {"idc1": 2},
			},
			score: DashboardScoreWarning,
		},
		{
			name: "proxy 0 in idc2 → Major",
			m: typeIDC{
				proto.ServiceNameProxy:     {"idc1": 2, "idc2": 0},
				proto.ServiceNameScheduler: {"idc1": 1},
				proto.ServiceNameWorker:    {"idc1": 2},
				proto.ServiceNameBlobNode:  {"idc1": 2},
			},
			score: DashboardScoreMajor,
		},
		// WORKER
		{
			name: "worker 1 per idc → Warning",
			m: typeIDC{
				proto.ServiceNameProxy:     {"idc1": 2},
				proto.ServiceNameScheduler: {"idc1": 1},
				proto.ServiceNameWorker:    {"idc1": 1},
				proto.ServiceNameBlobNode:  {"idc1": 2},
			},
			score: DashboardScoreWarning,
		},
		// BLOBNODE
		{
			name: "blobnode 0 in idc2 → Major",
			m: typeIDC{
				proto.ServiceNameProxy:     {"idc1": 2, "idc2": 2},
				proto.ServiceNameScheduler: {"idc1": 1},
				proto.ServiceNameWorker:    {"idc1": 2, "idc2": 2},
				proto.ServiceNameBlobNode:  {"idc1": 3, "idc2": 0},
			},
			score: DashboardScoreMajor,
		},
		// SCHEDULER
		{
			name: "scheduler absent → Warning",
			m: typeIDC{
				proto.ServiceNameProxy:    {"idc1": 2},
				proto.ServiceNameWorker:   {"idc1": 2},
				proto.ServiceNameBlobNode: {"idc1": 2},
			},
			score: DashboardScoreWarning,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := makeSvcStat(tc.m, 0)
			s.CalcScore()
			require.Equal(t, tc.score, s.Score.Score)
		})
	}
}

func TestServiceScore_ExpiredDisks(t *testing.T) {
	cases := []struct {
		name         string
		expiredDisks int
		score        int
	}{
		{"few expired disks → Notice", 3, DashboardScoreNotice},
		{"99 expired disks → Notice", 99, DashboardScoreNotice},
		{"121 expired disks → Warning", 121, DashboardScoreWarning},
		// Major from IDC gap dominates expired-disk Warning/Notice.
		{"IDC gap + expired disks → Major", 2, DashboardScoreMajor},
	}

	majorIDC := typeIDC{
		proto.ServiceNameProxy:     {"idc1": 2, "idc2": 0}, // triggers Major
		proto.ServiceNameScheduler: {"idc1": 1},
		proto.ServiceNameWorker:    {"idc1": 2},
		proto.ServiceNameBlobNode:  {"idc1": 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := healthyIDC
			if tc.score == DashboardScoreMajor {
				m = majorIDC
			}
			s := makeSvcStat(m, tc.expiredDisks)
			s.CalcScore()
			require.Equal(t, tc.score, s.Score.Score)
		})
	}
}
