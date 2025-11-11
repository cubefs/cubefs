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

package base

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// ErrorStats error stats
type ErrorStats struct {
	lock        sync.Mutex
	errMap      map[string]uint64
	totalErrCnt uint64
}

// ErrorPercent error percent
type ErrorPercent struct {
	err     string
	percent float64
	errCnt  uint64
}

// NewErrorStats returns error stats
func NewErrorStats() *ErrorStats {
	es := ErrorStats{
		errMap: make(map[string]uint64),
	}
	return &es
}

// AddFail add fail statistics
func (es *ErrorStats) AddFail(err error) {
	es.lock.Lock()
	defer es.lock.Unlock()
	es.totalErrCnt++

	errStr := errStrFormat(err)
	if _, ok := es.errMap[errStr]; !ok {
		es.errMap[errStr] = 0
	}
	es.errMap[errStr]++
}

// Stats returns stats
func (es *ErrorStats) Stats() (statsResult []ErrorPercent, totalErrCnt uint64) {
	es.lock.Lock()
	defer es.lock.Unlock()

	var totalCnt uint64
	for _, cnt := range es.errMap {
		totalCnt += cnt
	}

	for err, cnt := range es.errMap {
		percent := ErrorPercent{
			err:     err,
			percent: float64(cnt) / float64(totalCnt),
			errCnt:  cnt,
		}
		statsResult = append(statsResult, percent)
	}

	sort.Slice(statsResult, func(i, j int) bool {
		return statsResult[i].percent > statsResult[j].percent
	})

	return statsResult, es.totalErrCnt
}

// FormatPrint format print message
func FormatPrint(statsInfos []ErrorPercent) (res []string) {
	for _, info := range statsInfos {
		res = append(res, fmt.Sprintf("%s: %0.2f%%[%d]", info.err, info.percent*100, info.errCnt))
	}
	return
}

func errStrFormat(err error) string {
	if err == nil || len(err.Error()) == 0 {
		return ""
	}

	strSlice := strings.Split(err.Error(), ":")
	return strings.TrimSpace(strSlice[len(strSlice)-1])
}

const (
	Namespace                = "shardnode"
	ShardRepair              = "shard_repair" // same as scheduler
	ChunkMissMigrateAbnormal = "chunk_miss_migrate"
)

type AbnormalReporter struct {
	lock             sync.RWMutex
	abnormalReporter *prometheus.GaugeVec
	reportedVuids    map[proto.Vuid]struct{}
}

// NewAbnormalReporter returns abnormal reporter
func NewAbnormalReporter(clusterID proto.ClusterID, taskType string, abnormalKind string) *AbnormalReporter {
	abnormalReporter := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "task",
		Name:      "abnormal_task",
		Help:      "abnormal task",
		ConstLabels: map[string]string{
			"cluster_id": fmt.Sprintf("%d", clusterID),
			"task_type":  taskType,
			"kind":       abnormalKind,
		},
	}, []string{"diskID", "vuid"})
	if err := prometheus.Register(abnormalReporter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			abnormalReporter = are.ExistingCollector.(*prometheus.GaugeVec)
			return &AbnormalReporter{
				abnormalReporter: abnormalReporter,
				reportedVuids:    make(map[proto.Vuid]struct{}),
			}
		}
		panic(err)
	}
	return &AbnormalReporter{
		abnormalReporter: abnormalReporter,
		reportedVuids:    make(map[proto.Vuid]struct{}),
	}
}

// ReportAbnormal report abnormal task
func (abr *AbnormalReporter) ReportAbnormal(diskID proto.DiskID, vuid proto.Vuid) {
	abr.abnormalReporter.WithLabelValues(
		fmt.Sprintf("%d", diskID),
		fmt.Sprintf("%d", vuid),
	).Set(1)
}

// CancelAbnormal cancel abnormal report
func (abr *AbnormalReporter) CancelAbnormal(diskID proto.DiskID, vuid proto.Vuid) {
	abr.abnormalReporter.WithLabelValues(
		fmt.Sprintf("%d", diskID),
		fmt.Sprintf("%d", vuid),
	).Set(0)
}

// IsVuidReported check if vuid abnormal is reported
func (abr *AbnormalReporter) IsVuidReported(vuid proto.Vuid) bool {
	abr.lock.RLock()
	defer abr.lock.RUnlock()
	_, ok := abr.reportedVuids[vuid]
	return ok
}

// SetVuidReported set vuid reported
func (abr *AbnormalReporter) SetVuidReported(vuid proto.Vuid) {
	abr.lock.Lock()
	abr.reportedVuids[vuid] = struct{}{}
	abr.lock.Unlock()
}
