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

package meta

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSortHostsByPingElapsed(t *testing.T) {
	// Create a mock MetaWrapper
	createMockMetaWrapper := func(followerRead, nearRead bool, hostLatencies map[string]time.Duration) *MetaWrapper {
		mw := &MetaWrapper{
			FollowerRead: followerRead,
			NearRead:     nearRead,
			HostLatency:  sync.Map{},
		}

		for host, latency := range hostLatencies {
			mw.HostLatency.Store(host, latency)
		}

		return mw
	}

	t.Run("FollowerRead disabled returns original members", func(t *testing.T) {
		mp := &MetaPartition{
			Members: []string{"host1", "host2", "host3"},
		}

		mw := createMockMetaWrapper(false, true, map[string]time.Duration{
			"host1": 50 * time.Millisecond,
			"host2": 30 * time.Millisecond,
		})

		result := mp.SortHostsByPingElapsed(mw)

		// Should return original members without sorting
		expected := []string{"host1", "host2", "host3"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("NearRead disabled returns original members", func(t *testing.T) {
		mp := &MetaPartition{
			Members: []string{"host1", "host2", "host3"},
		}

		mw := createMockMetaWrapper(true, false, map[string]time.Duration{
			"host1": 50 * time.Millisecond,
			"host2": 30 * time.Millisecond,
		})

		result := mp.SortHostsByPingElapsed(mw)

		// Should return original members without sorting
		expected := []string{"host1", "host2", "host3"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("Both FollowerRead and NearRead enabled with latency data", func(t *testing.T) {
		mp := &MetaPartition{
			Members: []string{"host1", "host2", "host3", "host4"},
		}

		mw := createMockMetaWrapper(true, true, map[string]time.Duration{
			"host1": 50 * time.Millisecond,
			"host2": 30 * time.Millisecond,
			"host3": 70 * time.Millisecond,
			// host4 has no latency data
		})

		result := mp.SortHostsByPingElapsed(mw)

		// Should return sorted hosts: with latency sorted ascending, then hosts without latency
		expected := []string{"host2", "host1", "host3", "host4"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify that pingElapsedSortedHosts was initialized
		if mp.pingElapsedSortedHosts == nil {
			t.Error("Expected pingElapsedSortedHosts to be initialized")
		}
	})

	t.Run("Both enabled with all hosts having latency data", func(t *testing.T) {
		mp := &MetaPartition{
			Members: []string{"host1", "host2", "host3"},
		}

		mw := createMockMetaWrapper(true, true, map[string]time.Duration{
			"host1": 100 * time.Millisecond,
			"host2": 20 * time.Millisecond,
			"host3": 50 * time.Millisecond,
		})

		result := mp.SortHostsByPingElapsed(mw)

		// Should return sorted by latency ascending
		expected := []string{"host2", "host3", "host1"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})
}
