// Copyright 2022 The CubeFS Authors.
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

package workutils

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/counter"
)

func TestStatistics(t *testing.T) {
	mgr := WorkerStatsInst()
	mgr.AddCancel()
	mgr.AddReclaim()
	cancelCount, reclaimCount := WorkerStatsInst().Stats()
	var cancelVec [counter.SLOT]int
	cancelVec[counter.SLOT-1] = 1
	var reclaimVec [counter.SLOT]int
	reclaimVec[counter.SLOT-1] = 1
	require.Equal(t, cancelVec, cancelCount)
	require.Equal(t, reclaimVec, reclaimCount)
}
