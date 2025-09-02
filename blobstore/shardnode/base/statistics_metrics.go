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
