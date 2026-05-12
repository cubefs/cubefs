// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package stream

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
)

func TestSortByStatus(t *testing.T) {
	// Mock DataPartition
	dp := &wrapper.DataPartition{
		ClientWrapper: &wrapper.Wrapper{
			HostsStatus: map[string]bool{
				"host1": true,
				"host2": true,
				"host3": false,
				"host4": false,
			},
		},
		NearHosts: []string{"host1", "host3"},
	}

	dp.PartitionID = 1
	dp.Hosts = []string{"host1", "host2", "host3", "host4"}
	dp.MediaType = proto.MediaType_HDD
	// Test case 1: selectAll = true nearRead = false followerRead = false
	hosts := sortByStatus(dp, true)
	expected := []string{"host1", "host2", "host3", "host4"}
	if !reflect.DeepEqual(hosts, expected) {
		t.Errorf("Expected %v, got %v", expected, hosts)
	}

	// Test case 2: selectAll = false nearRead = false followerRead = false
	hosts = sortByStatus(dp, false)
	expected = []string{"host1", "host2"}
	if !reflect.DeepEqual(hosts, expected) {
		t.Errorf("Expected %v, got %v", expected, hosts)
	}

	dp.ClientWrapper.InitFollowerRead(true)
	dp.ClientWrapper.SetNearRead(true)

	// Test case 3: selectAll = true nearRead = true followerRead = true readFailedHostsMap = nil
	hosts = sortByStatus(dp, true)
	expected = []string{"host1", "host3"}
	if !reflect.DeepEqual(hosts, expected) {
		t.Errorf("Expected %v, got %v", expected, hosts)
	}

	// Test case 4: selectAll = false nearRead = true followerRead = true readFailedHostsMap = nil
	hosts = sortByStatus(dp, false)
	expected = []string{"host1"}
	if !reflect.DeepEqual(hosts, expected) {
		t.Errorf("Expected %v, got %v", expected, hosts)
	}

	dp.NearHosts = []string{"host1", "host3", "host2", "host4"}
	dp.ClientWrapper.InitReadFailedHosts()
	dp.ClientWrapper.AddReadFailedHosts(1, "host1")

	// Test case 5: selectAll = true nearRead = true followerRead = true readFailedHostsMap != nil
	hosts = sortByStatus(dp, true)
	expected = []string{"host2", "host1", "host3", "host4"}
	if !reflect.DeepEqual(hosts, expected) {
		t.Errorf("Expected %v, got %v", expected, hosts)
	}

	// Test case 6: selectAll = false nearRead = true followerRead = true readFailedHostsMap != nil
	hosts = sortByStatus(dp, false)
	expected = []string{"host2", "host1"}
	if !reflect.DeepEqual(hosts, expected) {
		t.Errorf("Expected %v, got %v", expected, hosts)
	}
}

func TestStreamConn_Backoff(t *testing.T) {
	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 1,
			Hosts:       []string{"127.0.0.1:65534", "127.0.0.1:65535"},
			LeaderAddr:  "127.0.0.1:65534",
		},
	}
	dp.ClientWrapper = &wrapper.Wrapper{
		HostsStatus: map[string]bool{
			"127.0.0.1:65534": true,
			"127.0.0.1:65535": true,
		},
	}

	sc := NewStreamConn(dp, false, 150*time.Millisecond)
	req := NewReadPacket(&proto.ExtentKey{PartitionId: 1, ExtentId: 1}, 0, 4096, 1, 0, false)
	retry := true

	// Test sendToDataPartitionLeader
	sc.maxRetryTimeout = 150 * time.Millisecond
	err := sc.sendToDataPartitionLeader(req, &retry, func(conn *net.TCPConn) (error, bool) {
		return TryOtherAddrError, false
	})
	if err == nil {
		t.Errorf("expected error, got nil")
	}

	// Test readQuorumHosts
	sc.maxRetryTimeout = 150 * time.Millisecond
	err = sc.readQuorumHosts(dp, req, func(conn *net.TCPConn) (error, bool) {
		return TryOtherAddrError, false
	})
	if err == nil {
		t.Errorf("readQuorumHosts expected error, got nil")
	}

	// Test readActiveHosts
	// Need to reset timeout since it was modified by previous calls
	sc.maxRetryTimeout = 150 * time.Millisecond
	err = sc.readActiveHosts(dp, req, func(conn *net.TCPConn) (error, bool) {
		return fmt.Errorf("mock error"), false
	})
	t.Logf("readActiveHosts returned err: %v", err)
}
