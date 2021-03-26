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
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/ump"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	TPPool = &sync.Pool{New: func() interface{} {
		return new(TimePoint)
	}}
	TPCh chan *TimePoint
)

func collectTP() {
	TPCh = make(chan *TimePoint, ChSize)
	for {
		m := <-TPCh
		metric := m.Metric()
		metric.Set(float64(m.val))
		TPPool.Put(m)
	}
}

type TimePoint struct {
	Gauge
	startTime time.Time
}

func NewTP(name string) (tp *TimePoint) {
	tp = TPPool.Get().(*TimePoint)
	tp.name = metricsName(name)
	tp.labels = make(map[string]string)
	tp.val = 0
	tp.startTime = time.Now()
	return
}

func (tp *TimePoint) Set() {
	if !enabledPrometheus {
		return
	}
	val := time.Since(tp.startTime).Nanoseconds()
	tp.val = val
	tp.publish()
}

func (tp *TimePoint) SetWithLabels(labels map[string]string) {
	if !enabledPrometheus {
		return
	}
	tp.labels = labels
	tp.Set()
}

type TimePointVec struct {
	*GaugeVec
}

type TPMetric struct {
	metric    prometheus.Gauge
	startTime time.Time
}

func NewTPVec(name, help string, labels []string) (tp *TimePointVec) {
	return &TimePointVec{
		GaugeVec: NewGaugeVec(name, help, labels),
	}
}

func (tpv *TimePointVec) GetWithLabelVals(lvs ...string) (*TPMetric, error) {
	if m, err := tpv.GetMetricWithLabelValues(lvs...); err == nil {
		return &TPMetric{
			metric:    m,
			startTime: time.Now(),
		}, nil
	} else {
		return nil, err
	}
}

func (tp *TPMetric) Set() {
	if !enabledPrometheus {
		return
	}

	val := float64(time.Since(tp.startTime).Nanoseconds())
	tp.metric.Set(val)
}

type TimePointCount struct {
	tp  *TimePoint
	cnt *Counter
	to  *ump.TpObject
}

func NewTPCnt(name string) (tpc *TimePointCount) {
	tpc = new(TimePointCount)
	tpc.to = ump.BeforeTP(fmt.Sprintf("%v_%v_%v", clustername, modulename, name))
	tpc.tp = NewTP(name)
	tpc.cnt = NewCounter(fmt.Sprintf("%s_count", name))
	return
}

func (tpc *TimePointCount) Set(err error) {
	ump.AfterTP(tpc.to, err)
	tpc.tp.Set()
	tpc.cnt.Add(1)
}

func (tpc *TimePointCount) SetWithLabels(err error, labels map[string]string) {
	ump.AfterTP(tpc.to, err)
	if !enabledPrometheus {
		return
	}
	tpc.tp.SetWithLabels(labels)
	tpc.cnt.AddWithLabels(1, labels)
}

type TimePointCountVec struct {
	tpv  *TimePointVec
	cntv *CounterVec
	name string
}

func NewTPCntVec(name, help string, labels []string) (tpc *TimePointCountVec) {
	return &TimePointCountVec{
		tpv:  NewTPVec(name, help, labels),
		cntv: NewCounterVec(fmt.Sprintf("%s_cnt", name), help, labels),
		name: name,
	}
}

type TPCMetric struct {
	tp  *TPMetric
	cnt prometheus.Counter
	to  *ump.TpObject
}

func (tpc *TimePointCountVec) GetWithLabelVals(lvs ...string) *TPCMetric {
	metric := &TPCMetric{
		to: ump.BeforeTP(fmt.Sprintf("%v_%v_%v", clustername, modulename, tpc.name)),
	}

	if !enabledPrometheus {
		return metric
	}

	if tp, err := tpc.tpv.GetWithLabelVals(lvs...); err == nil {
		metric.tp = tp
	}
	if cnt, err := tpc.cntv.GetMetricWithLabelValues(lvs...); err == nil {
		metric.cnt = cnt
	}

	return metric
}

func (tpc *TimePointCountVec) Put(m *TPCMetric) {
	tpc.PutWithError(m, nil)
}

func (tpc *TimePointCountVec) PutWithError(m *TPCMetric, err error) {
	m.CountWithError(err)
	//release m
}

func (m *TPCMetric) Count() {
	m.CountWithError(nil)
}

func (m *TPCMetric) CountWithError(err error) {
	if m.to != nil {
		ump.AfterTP(m.to, err)
	}
	if !enabledPrometheus {
		return
	}
	m.tp.Set()
	m.cnt.Add(1)
}
