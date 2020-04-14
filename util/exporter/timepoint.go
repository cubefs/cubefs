// Copyright 2018 The Chubao Authors.
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

package exporter

import (
	"fmt"
	"time"

	"github.com/chubaofs/chubaofs/util/ump"
)

// timepoint for a operator metric
type TimePoint struct {
	Gauge
	startTime time.Time
}

func NewTP(name string) (tp *TimePoint) {
	if !IsEnabled() {
		return
	}
	tp = new(TimePoint)
	tp.name = name
	tp.startTime = time.Now()
	return
}

func (tp *TimePoint) Set() {
	if !IsEnabled() {
		return
	}
	val := time.Since(tp.startTime).Nanoseconds()
	tp.val = float64(val)
	CollectorInstance().Collect(tp)
}

// timepoint and counter for operator metric
type TimePointCount struct {
	tp  *TimePoint
	cnt *Counter
	to  *ump.TpObject
}

func NewTPCnt(name string) (tpc *TimePointCount) {
	tpc = new(TimePointCount)
	tpc.to = ump.BeforeTP(fmt.Sprintf("%v_%v_%v", ClusterName(), Role(), name))
	tpc.tp = NewTP(name)
	tpc.cnt = NewCounter(fmt.Sprintf("%s_count", name))
	return
}

func (tpc *TimePointCount) Set(err error) {
	ump.AfterTP(tpc.to, err)
	tpc.tp.Set()
	tpc.cnt.Add(1)
}
