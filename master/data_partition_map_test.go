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

package master

import (
	"testing"

	"github.com/cubefs/cubefs/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestGetMaxDataPartitionID_empty exercises the scan path on an empty map.
func TestGetMaxDataPartitionID_empty(t *testing.T) {
	t.Parallel()
	dpMap := newDataPartitionMap("vol-empty")
	id := dpMap.getMaxDataPartitionID()
	require.Equal(t, uint64(0), id)
}

// TestGetMaxDataPartitionID_scanMax exercises the branch that scans partitionMap
// when the cache window has expired (curtime >= lastUpdateMaxDpIdTime+interval).
func TestGetMaxDataPartitionID_scanMax(t *testing.T) {
	t.Parallel()
	dpMap := newDataPartitionMap("vol-scan")
	dpMap.put(&DataPartition{PartitionID: 7, VolName: "vol-scan"})
	dpMap.put(&DataPartition{PartitionID: 42, VolName: "vol-scan"})
	dpMap.lastUpdateMaxDpIdTime = timeutil.GetCurrentTimeUnix() - updateMaxDpIdInterval - 1

	id := dpMap.getMaxDataPartitionID()
	require.Equal(t, uint64(42), id)
}

// TestGetMaxDataPartitionID_cacheHit exercises the fast path:
// curtime < lastUpdateMaxDpIdTime+updateMaxDpIdInterval returns cached maxDpId
// without rescanning partitionMap (even if a larger id exists in the map).
func TestGetMaxDataPartitionID_cacheHit(t *testing.T) {
	t.Parallel()
	dpMap := newDataPartitionMap("vol-cache")
	dpMap.put(&DataPartition{PartitionID: 200, VolName: "vol-cache"})
	dpMap.maxDpId = 9
	dpMap.lastUpdateMaxDpIdTime = timeutil.GetCurrentTimeUnix()

	id := dpMap.getMaxDataPartitionID()
	require.Equal(t, uint64(9), id, "within interval, cached maxDpId must be returned")
}
