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

package executor

import (
	"math"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// TestComputeRampSchedule_NoRamp confirms the fallback path: when the
// stage doesn't request any ramp/steady/rampDown the schedule collapses
// to "steady = caller-supplied fallback".
func TestComputeRampSchedule_NoRamp(t *testing.T) {
	ctrl := spec.StageControl{TargetIOPS: 100}
	sched := computeRampSchedule(ctrl, 100, 30*time.Second)
	if sched.rampUp != 0 || sched.rampDown != 0 {
		t.Errorf("expected zero ramp windows, got %+v", sched)
	}
	if sched.steady != 30*time.Second {
		t.Errorf("expected fallback steady=30s, got %v", sched.steady)
	}
	if sched.target != 100 {
		t.Errorf("expected target=100, got %v", sched.target)
	}
}

// TestComputeRampSchedule_WithRamp verifies the ramp-bearing path picks
// the rampUp/steady/rampDown directly from StageControl.
func TestComputeRampSchedule_WithRamp(t *testing.T) {
	ctrl := spec.StageControl{
		RampUpSec:   5,
		SteadySec:   20,
		RampDownSec: 5,
		TargetIOPS:  200,
	}
	sched := computeRampSchedule(ctrl, 200, time.Minute /* ignored */)
	if sched.rampUp != 5*time.Second || sched.steady != 20*time.Second || sched.rampDown != 5*time.Second {
		t.Errorf("unexpected schedule windows: %+v", sched)
	}
	if sched.totalDuration() != 30*time.Second {
		t.Errorf("totalDuration mismatch: got %v want 30s", sched.totalDuration())
	}
}

// TestCurrentRate_RampShape covers all three phases (up / steady / down)
// of the schedule and verifies the linear interpolation lands close to
// the expected curve at sample points.
func TestCurrentRate_RampShape(t *testing.T) {
	sched := rampSchedule{
		rampUp:   10 * time.Second,
		steady:   10 * time.Second,
		rampDown: 10 * time.Second,
		target:   100,
	}
	cases := []struct {
		t    time.Duration
		want float64
	}{
		{0, 0},                       // start of rampUp
		{5 * time.Second, 50},        // halfway through rampUp
		{10 * time.Second, 100},      // end of rampUp == start of steady
		{15 * time.Second, 100},      // mid steady
		{20 * time.Second, 100},      // end of steady == start of rampDown
		{25 * time.Second, 50},       // halfway through rampDown
		{30 * time.Second, 0},        // end of rampDown (returns 0)
		{31 * time.Second, 0},        // past end
	}
	for _, c := range cases {
		got := currentRate(sched, c.t)
		if math.Abs(got-c.want) > 0.5 {
			t.Errorf("currentRate(t=%v) = %v; want %v", c.t, got, c.want)
		}
	}
}
