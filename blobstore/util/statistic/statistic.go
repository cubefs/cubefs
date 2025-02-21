// Copyright 2025 The Cuber Authors.
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

package statistic

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type DelayStatistic struct {
	module       string
	start        time.Time
	unit         time.Duration
	unitname     string
	interval     time.Duration
	bucket       int
	requests     int64
	delays       int64
	buckets      []int64
	bucketDelays []int64
}

type DelayResult struct {
	Module   string
	Unit     time.Duration
	UnitName string
	Duration time.Duration
	Requests int64
	Delays   int64
	AvgRps   float64
	AvgDelay float64
	Metrices []struct {
		Percentage float64
		Delay      float64
	}
}

func (r *DelayResult) String() (str string) {
	str += fmt.Sprintf("%s | request: %d | rps[avg]: %.2f | delay[avg]: %.2f %s", r.Module, r.Requests,
		float64(r.Requests)/(float64(r.Duration)/float64(time.Second)),
		float64(r.Delays)/float64(r.Requests)/float64(r.Unit), r.UnitName)

	name := func(p float64) string {
		str := fmt.Sprintf("p%.4f", p)
		return strings.TrimRight(strings.TrimRight(str, "0"), ".")
	}
	for _, metric := range r.Metrices {
		str += fmt.Sprintf(" | delay[%s]: %.2f %s", name(metric.Percentage), metric.Delay, r.UnitName)
	}
	return
}

// NewDelayStatistic return thread-safe time delay statistic.
func NewDelayStatistic(module string, unit, interval time.Duration, bucket int) *DelayStatistic {
	var unitname string
	switch unit {
	case time.Nanosecond:
		unitname = "ns"
	case time.Microsecond:
		unitname = "us"
	case time.Millisecond:
		unitname = "ms"
	case time.Second:
		unitname = "s"
	default:
		unit, unitname = time.Millisecond, "ms"
	}
	return &DelayStatistic{
		module:       module,
		start:        time.Now(),
		unit:         unit,
		unitname:     unitname,
		interval:     interval,
		bucket:       bucket,
		buckets:      make([]int64, bucket+1),
		bucketDelays: make([]int64, bucket+1),
	}
}

func (t *DelayStatistic) Add(delay time.Duration) {
	atomic.AddInt64(&t.requests, 1)
	atomic.AddInt64(&t.delays, int64(delay))
	idx := int(delay / t.interval)
	if idx > t.bucket {
		idx = t.bucket
	}
	atomic.AddInt64(&t.buckets[idx], 1)
	atomic.AddInt64(&t.bucketDelays[idx], int64(delay%t.interval))
}

func (t *DelayStatistic) Report() {
	fmt.Println(t.Reports(50, 95, 99, 99.9))
}

func (t *DelayStatistic) Reports(percentages ...float64) (r *DelayResult) {
	r = &DelayResult{
		Module:   t.module,
		Unit:     t.unit,
		UnitName: t.unitname,
		Duration: time.Since(t.start),
	}

	reqs := atomic.LoadInt64(&t.requests)
	delays := atomic.LoadInt64(&t.delays)
	if reqs <= 0 || delays <= 0 {
		return
	}
	r.Requests = reqs
	r.Delays = delays
	r.AvgRps = float64(reqs) / (float64(t.unit) / float64(time.Second))
	r.AvgDelay = float64(delays) / float64(reqs) / float64(t.unit)

	r.Metrices = make([]struct {
		Percentage float64
		Delay      float64
	}, len(percentages))

	metrics := make([]struct {
		percentage float64
		offset     int64
	}, len(percentages))
	for idx, per := range percentages {
		metrics[idx].percentage = per
		metrics[idx].offset = int64(float64(reqs) * per / 100)
	}
	for index, tuple := range metrics {
		offset, all := tuple.offset, int64(0)
		for idx, num := range t.buckets {
			all += num
			if all < offset {
				continue
			}
			r.Metrices[index].Percentage = tuple.percentage
			r.Metrices[index].Delay = float64(time.Duration(idx)*t.interval)/float64(t.unit) +
				float64(t.bucketDelays[idx])/float64(num)/float64(t.unit)
			break
		}
	}
	return
}
