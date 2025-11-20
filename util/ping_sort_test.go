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

package util_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util"
)

func TestPingElapsedSortedHosts(t *testing.T) {
	hosts := []string{"host1", "host2", "host3", "host4"}
	getHosts := func() []string { return hosts }
	getElapsed := func(host string) (time.Duration, bool) {
		switch host {
		case "host1":
			return 50 * time.Millisecond, true
		case "host2":
			return 30 * time.Millisecond, true
		case "host3":
			return 70 * time.Millisecond, true
		case "host4":
			return 0, false
		}
		return 0, false
	}

	sortedHosts := util.NewPingElapsedSortHosts(getHosts, getElapsed)
	result := sortedHosts.GetSortedHosts()

	expected := []string{"host2", "host1", "host3", "host4"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, but got %v", expected, result)
	}
}

// TestAddressPingStats_AddAndAverage 测试添加和计算平均值
func TestAddressPingStats_AddAndAverage(t *testing.T) {
	stats := &util.AddressPingStats{}

	stats.Add(10 * time.Millisecond)
	stats.Add(20 * time.Millisecond)
	stats.Add(30 * time.Millisecond)

	average := stats.Average()
	expected := 20 * time.Millisecond

	if average != expected {
		t.Errorf("Expected average %v, but got %v", expected, average)
	}
}
