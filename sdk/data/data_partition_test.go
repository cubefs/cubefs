// Copyright 2020 The CubeFS Authors.
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

package data

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func TestKmin(t *testing.T) {
	partitions := make([]*DataPartition, 0)

	rand.Seed(time.Now().UnixNano())
	length := rand.Intn(100) + 2

	for i := 0; i < length; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Int63n(100)

		dp := new(DataPartition)
		dp.Metrics = new(proto.DataPartitionMetrics)
		dp.Metrics.AvgWriteLatencyNano = i
		partitions = append(partitions, dp)
	}
	fmt.Printf("%-20s", "origin partitions:")
	for _, v := range partitions {
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	kth := selectKminDataPartition(partitions, (length-1)*80/100+1)

	kmin := partitions[kth].GetAvgWrite()

	fmt.Printf("%-20s%v/%v", "kth of length:", kth, length)
	fmt.Println()

	fmt.Printf("%-20s%v", "kmin:", kmin)
	fmt.Println()

	fmt.Printf("%-20s", "faster partitions:")
	for _, v := range partitions[:kth] {
		if v.GetAvgWrite() > kmin {
			fmt.Println()
			fmt.Println("select error!")
			t.Fail()
		}
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	fmt.Printf("%-20s", "slower partitions:")
	for _, v := range partitions[kth:len(partitions)] {
		fmt.Printf("%v ", v.GetAvgWrite())
		if v.GetAvgWrite() < kmin {
			fmt.Println()
			fmt.Println("select error!")
			t.Fail()
		}
	}
	fmt.Println()
}

func TestExcludeDp(t *testing.T) {
	testsForExcludeDp := []struct {
		name          string
		hosts         []string
		excludeMap    map[string]struct{}
		quorum        int
		expectExclude bool
	}{
		{
			name:          "test3quorum_no_exclude",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030"},
			excludeMap:    map[string]struct{}{},
			quorum:        3,
			expectExclude: false,
		},
		{
			name:          "test3quorum_exclude",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030"},
			excludeMap:    map[string]struct{}{"192.168.0.32:17030": {}},
			quorum:        3,
			expectExclude: true,
		},
		{
			name:          "test0quorum_3host_no_exclude",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030"},
			excludeMap:    map[string]struct{}{},
			quorum:        0,
			expectExclude: false,
		},
		{
			name:          "test0quorum_5host_no_exclude",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030", "192.168.0.34:17030", "192.168.0.35:17030"},
			excludeMap:    map[string]struct{}{},
			quorum:        0,
			expectExclude: false,
		},
		{
			name:          "test0quorum_3host",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030"},
			excludeMap:    map[string]struct{}{"192.168.0.33:17030": {}},
			quorum:        0,
			expectExclude: true,
		},
		{
			name:          "test0quorum_5host",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030", "192.168.0.34:17030", "192.168.0.35:17030"},
			excludeMap:    map[string]struct{}{"192.168.0.33:17030": {}, "192.168.0.32:17030": {}},
			quorum:        0,
			expectExclude: true,
		},
		{
			name:          "test3quorum_5host_01",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030", "192.168.0.34:17030", "192.168.0.35:17030"},
			excludeMap:    map[string]struct{}{},
			quorum:        3,
			expectExclude: false,
		},
		{
			name:          "test3quorum_5host_01",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030", "192.168.0.34:17030", "192.168.0.35:17030"},
			excludeMap:    map[string]struct{}{"192.168.0.32:17030": {}, "192.168.0.33:17030": {}},
			quorum:        3,
			expectExclude: false,
		},
		{
			name:          "test3quorum_5host_02",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030", "192.168.0.34:17030", "192.168.0.35:17030"},
			excludeMap:    map[string]struct{}{"192.168.0.31:17030": {}},
			quorum:        3,
			expectExclude: true,
		},
		{
			name:          "test3quorum_5host_02",
			hosts:         []string{"192.168.0.31:17030", "192.168.0.32:17030", "192.168.0.33:17030", "192.168.0.34:17030", "192.168.0.35:17030"},
			excludeMap:    map[string]struct{}{"192.168.0.34:17030": {}, "192.168.0.32:17030": {}, "192.168.0.33:17030": {}},
			quorum:        3,
			expectExclude: true,
		},
	}

	for _, tt := range testsForExcludeDp {
		t.Run(tt.name, func(t *testing.T) {
			dp := &DataPartition{}
			dp.Hosts = tt.hosts
			exclude := isExcludedByHost(dp, tt.excludeMap, tt.quorum)
			if exclude != tt.expectExclude {
				t.Errorf("TestExcludeDp: test(%v) expect(%v) but(%v)", tt.name, tt.expectExclude, exclude)
				return
			}
		})
	}
}

func TestDpLeaderAddressSerialization(t *testing.T) {
	var dp *DataPartition
	w, _ := NewDataPartitionWrapper(ltptestVolume, strings.Split(ltptestMaster, ","))
	w.partitions.Range(func(k, v interface{}) bool {
		dp = v.(*DataPartition)
		return false
	})
	host := dp.Hosts[0]
	dp.LeaderAddr=proto.NewAtomicString(host)
	dpJson, err := json.Marshal(dp)
	if err != nil {
		t.Errorf("TestDpLeaderAddressSerialization Marshal dp fail: dp(%v)", dp)
	}
	err = json.Unmarshal(dpJson, &dp)
	if err != nil {
		t.Errorf("TestDpLeaderAddressSerialization Unmarshal dp fail: dp(%v)", dp)
	}
	leader := dp.GetLeaderAddr()
	if leader != host {
		t.Errorf("TestDpLeaderAddressSerialization: expect(%v) but(%v)", host, leader)
	}
}
