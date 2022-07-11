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
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/counter"
)

// WorkerStats worker stats
type WorkerStats struct {
	cancelCount  counter.Counter
	reclaimCount counter.Counter
}

// AddCancel add cancel statistics
func (stats *WorkerStats) AddCancel() {
	stats.cancelCount.Add()
}

// AddReclaim add reclaim statistics
func (stats *WorkerStats) AddReclaim() {
	stats.reclaimCount.Add()
}

// Stats returns stats
func (stats *WorkerStats) Stats() (cancel, reclaim [counter.SLOT]int) {
	cancel = stats.cancelCount.Show()
	reclaim = stats.reclaimCount.Show()
	return
}

var (
	workerStats        *WorkerStats
	newWorkerStatsOnce sync.Once
)

// WorkerStatsInst make sure only one instance in global
func WorkerStatsInst() *WorkerStats {
	newWorkerStatsOnce.Do(func() {
		workerStats = &WorkerStats{}
	})
	return workerStats
}
