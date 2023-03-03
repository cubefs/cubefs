// Copyright 2018 The CubeFS Authors.
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

package prom

import (
	"sync"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	summaryGroup        sync.Map
	summaryVecGroup     sync.Map
	summaryCh           chan *summary
	summaryVecCh        chan *summaryVec
	summaryTPObjectives = map[float64]float64{
		0.50:  0.05,
		0.99:  0.001,
		0.999: 0.0001,
	}
	summaryPool = sync.Pool{
		New: func() interface{} {
			return &summary{}
		},
	}
	summaryVecPool = sync.Pool{
		New: func() interface{} {
			return &summaryVec{}
		},
	}
)

func collectorSummary() {
	defer wg.Done()
	summaryCh = make(chan *summary, chSize)
	summaryVecCh = make(chan *summaryVec, chSize)
	log.LogInfof("Exporter: summary collector start")
	for {
		select {
		case <-stopC:
			log.LogInfof("Exporter: summary collector stopped")
			return
		case m := <-summaryCh:
			var metric = m.metric()
			metric.Observe(m.val)
			summaryPool.Put(m)
		case m := <-summaryVecCh:
			var metric = m.metric()
			var labels = make(map[string]string, len(m.lvs))
			for _, lv := range m.lvs {
				labels[lv.Label] = lv.Value
			}
			metric.With(labels).Observe(m.val)
			summaryVecPool.Put(m)
		}
	}
}

type Summary interface {
	Observe(val float64)
}

type summary struct {
	name string
	val  float64
}

func (s *summary) Observe(val float64) {
	s.val = val
	s.publish()
}

func (s *summary) publish() {
	select {
	case summaryCh <- s:
	default:
	}
}

func (s *summary) metric() prometheus.Summary {

	var key = stringMD5(s.name)
	var actualMetric interface{}
	var load bool
	if actualMetric, load = summaryGroup.Load(key); load {
		return actualMetric.(prometheus.Summary)
	}
	var metric = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       s.name,
			Objectives: summaryTPObjectives,
		})
	if actualMetric, load = summaryGroup.LoadOrStore(key, metric); !load {
		if err := prometheus.Register(actualMetric.(prometheus.Collector)); err == nil {
			log.LogInfof("Exporter: register summary %v", s.name)
		} else {
			log.LogErrorf("Exporter: register summary %v failed: %v", s.name, err)
		}
	}

	return actualMetric.(prometheus.Summary)
}

func GetSummary(name string, lvs ...LabelValue) Summary {
	if len(lvs) > 0 {
		var s = summaryVecPool.Get().(*summaryVec)
		s.name = name
		s.lvs = lvs
		return s
	}
	var s = summaryPool.Get().(*summary)
	s.name = name
	return s
}

type summaryVec struct {
	name string
	val  float64
	lvs  []LabelValue
}

func (s *summaryVec) Observe(val float64) {
	s.val = val
	s.publish()
}

func (s *summaryVec) publish() {
	select {
	case summaryVecCh <- s:
	default:
	}
}

func (s *summaryVec) metric() *prometheus.SummaryVec {

	var key = stringMD5(s.name)
	var actualMetric interface{}
	var load bool
	if actualMetric, load = summaryVecGroup.Load(key); load {
		return actualMetric.(*prometheus.SummaryVec)
	}
	var metric = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       s.name,
			Objectives: summaryTPObjectives,
		},
		func() []string {
			var labelNames = make([]string, 0, len(s.lvs))
			for _, lv := range s.lvs {
				labelNames = append(labelNames, lv.Label)
			}
			return labelNames
		}())
	if actualMetric, load = summaryVecGroup.LoadOrStore(key, metric); !load {
		if err := prometheus.Register(actualMetric.(prometheus.Collector)); err == nil {
			log.LogInfof("Exporter: register summary %v", s.name)
		} else {
			log.LogErrorf("Exporter: register summary %v failed: %v", s.name, err)
		}
	}

	return actualMetric.(*prometheus.SummaryVec)
}
