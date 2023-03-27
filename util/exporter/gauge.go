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

package exporter

import (
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	GaugeGroup sync.Map
	GaugeCh    chan *Gauge
)

func collectGauge() {
	defer wg.Done()
	GaugeCh = make(chan *Gauge, ChSize)
	for {
		select {
		case <-stopC:
			return
		case m := <-GaugeCh:
			metric := m.Metric()
			metric.Set(float64(m.val))
		}
	}
}

type Gauge struct {
	name   string
	labels map[string]string
	val    int64
	ch     chan interface{}
}

func NewGauge(name string) (g *Gauge) {
	g = new(Gauge)
	g.name = metricsName(name)
	return
}

func (c *Gauge) Key() (key string) {
	return stringMD5(c.Name())
}

func (g *Gauge) Name() string {
	return fmt.Sprintf("{%s: %s}", g.name, stringMapToString(g.labels))
}

func (g *Gauge) String() string {
	if g == nil {
		return "nil"
	}
	return fmt.Sprintf("{name: %s, labels: %s, val: %v}", g.name, stringMapToString(g.labels), g.val)
}

func (c *Gauge) Metric() prometheus.Gauge {
	metric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        c.name,
			ConstLabels: c.labels,
		})
	key := c.Key()
	actualMetric, load := GaugeGroup.LoadOrStore(key, metric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfof("register metric %v", c.Name())
		} else {
			log.LogErrorf("register metric %v, %v", c.Name(), err)
		}
	}

	return actualMetric.(prometheus.Gauge)
}

func (g *Gauge) Set(val int64) {
	if !enabledPrometheus {
		return
	}
	g.val = val
	g.publish()
}

func (c *Gauge) publish() {
	select {
	case GaugeCh <- c:
	default:
	}
}

func (g *Gauge) SetWithLabels(val int64, labels map[string]string) {
	if !enabledPrometheus {
		return
	}
	g.labels = labels
	g.Set(val)
}

type GaugeVec struct {
	*prometheus.GaugeVec
}

func NewGaugeVec(name, help string, labels []string) *GaugeVec {
	v := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricsName(name),
			Help: help,
		},
		labels,
	)

	if err := prometheus.Register(v); err != nil {
		log.LogErrorf("prometheus register gaugevec name:%v, labels:{%v} error: %v", name, labels, err)
		return nil
	}

	return &GaugeVec{GaugeVec: v}
}

func (v *GaugeVec) SetWithLabelValues(val float64, lvs ...string) {
	if !enabledPrometheus {
		return
	}
	if m, err := v.GetMetricWithLabelValues(lvs...); err == nil {
		m.Set(val)
	}
}
