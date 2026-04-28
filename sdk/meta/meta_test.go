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
	"sync"
	"testing"
	"time"

	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/require"
)

func TestPingHosts(t *testing.T) {
	hostLatency := &sync.Map{}
	hostTimeoutCount := &sync.Map{}

	// Mock addTimeoutCount
	addTimeoutCount := func(host string) {
		v, _ := hostTimeoutCount.LoadOrStore(host, int32(0))
		count := v.(int32)
		count++

		if count >= 3 {
			hostLatency.Delete(host)
			t.Logf("Timeout threshold triggered, deleted latency record for %s", host)
		} else {
			hostTimeoutCount.Store(host, count)
		}
	}

	// Mock reset logic
	resetTimeoutCount := func(host string, avgLatency time.Duration) {
		if timeoutCount, exists := hostTimeoutCount.Load(host); exists {
			count := timeoutCount.(int32)
			if count > 0 && avgLatency > 0 && avgLatency < 100*time.Millisecond {
				hostTimeoutCount.Store(host, int32(0))
				t.Logf("Reset timeout count for %s", host)
			}
		}
	}

	t.Run("Recovery after timeout", func(t *testing.T) {
		host := "recover-host"

		// Simulate two timeouts
		addTimeoutCount(host)
		addTimeoutCount(host)

		// Verify count is 2
		if count, _ := hostTimeoutCount.Load(host); count.(int32) != 2 {
			t.Errorf("Expected count 2, got: %v", count)
		}

		// Simulate successful ping and reset
		resetTimeoutCount(host, 50*time.Millisecond)

		// Verify count is reset
		if count, _ := hostTimeoutCount.Load(host); count.(int32) != 0 {
			t.Errorf("Expected count 0 after reset, got: %v", count)
		}
	})

	t.Run("Cannot recover after reaching threshold", func(t *testing.T) {
		host := "threshold-host"
		hostLatency.Store(host, 100*time.Millisecond)

		// Simulate reaching threshold
		addTimeoutCount(host) // 1
		addTimeoutCount(host) // 2
		addTimeoutCount(host) // 3 - trigger deletion

		// Verify HostLatency is deleted
		if _, exists := hostLatency.Load(host); exists {
			t.Error("HostLatency should be deleted after reaching threshold")
		}

		// Even with successful ping afterwards, cannot recover HostLatency
		resetTimeoutCount(host, 50*time.Millisecond)

		// Verify HostLatency still doesn't exist
		if _, exists := hostLatency.Load(host); exists {
			t.Error("HostLatency should not be recovered after threshold is reached")
		}
	})
}

// Covers updateHostLatency in meta.go (start/defer logging and early-return on master failure).
func TestMetaWrapper_updateHostLatency_noPanicWhenMasterUnreachable(t *testing.T) {
	t.Parallel()
	mw := &MetaWrapper{
		mc: masterSDK.NewMasterClient([]string{"127.0.0.1:1"}, false),
	}
	mw.updateHostLatency()
}

// Covers getMetaHostsMap in meta.go when AdminAPI fails (same path as updateHostLatency early return).
func TestMetaWrapper_getMetaHostsMap_errorFromMaster(t *testing.T) {
	t.Parallel()
	mw := &MetaWrapper{
		mc: masterSDK.NewMasterClient([]string{"127.0.0.1:1"}, false),
	}
	hosts, err := mw.getMetaHostsMap()
	require.Error(t, err)
	require.NotNil(t, hosts)
	require.Equal(t, 0, len(hosts))
}
