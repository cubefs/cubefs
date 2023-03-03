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
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gaugeGroup    sync.Map
	gaugeVecGroup sync.Map
	gaugeCh       chan *gauge
	gaugeVecCh    chan *gaugeVec
	gaugeVecDelCh chan *gaugeVec
)

func collectGauge() {
	defer wg.Done()
	gaugeCh = make(chan *gauge, chSize)
	gaugeVecCh = make(chan *gaugeVec, chSize)
	for {
		select {
		case <-stopC:
			return
		case m := <-gaugeCh:
			metric := m.metric()
			metric.Set(m.val)
		case m := <-gaugeVecCh:
			metric := m.metric()
			var labels = make(map[string]string, len(m.lvs))
			for _, lv := range m.lvs {
				labels[lv.Label] = lv.Value
			}
			metric.With(labels).Set(m.val)
		case m := <-gaugeVecDelCh:
			metric := m.metric()
			var labels = make(map[string]string, len(m.lvs))
			for _, lv := range m.lvs {
				labels[lv.Label] = lv.Value
			}
			metric.Delete(labels)
		}
	}
}

type Gauge interface {
	Set(val float64)
}

type gauge struct {
	name string
	val  float64
}

func (g *gauge) String() string {
	if g == nil {
		return "nil"
	}
	return fmt.Sprintf("{name: %s, val: %v}", g.name, g.val)
}

func (g *gauge) Set(val float64) {
	g.val = val
	g.publish()
}

func (g *gauge) publish() {
	select {
	case gaugeCh <- g:
	default:
	}
}

func (g *gauge) metric() prometheus.Gauge {
	var key = stringMD5(g.name)
	var actualMetric interface{}
	var load bool
	if actualMetric, load = gaugeGroup.Load(key); load {
		return actualMetric.(prometheus.Gauge)
	}
	var metric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: g.name,
		})
	if actualMetric, load = gaugeGroup.LoadOrStore(key, metric); !load {
		if err := prometheus.Register(actualMetric.(prometheus.Collector)); err == nil {
			log.LogInfof("register gauge %v", g.name)
		} else {
			log.LogErrorf("register gauge %v failed: %v", g.name, err)
		}
	}

	return actualMetric.(prometheus.Gauge)
}

type gaugeVec struct {
	name string
	val  float64
	lvs  []LabelValue
}

func (g *gaugeVec) Set(val float64) {
	g.val = val
	g.publish()
}

func (g *gaugeVec) publish() {
	select {
	case gaugeVecCh <- g:
	default:
	}
}

func (g *gaugeVec) metric() *prometheus.GaugeVec {
	var key = stringMD5(g.name)
	var actualMetric interface{}
	var load bool
	if actualMetric, load = gaugeVecGroup.Load(key); load {
		return actualMetric.(*prometheus.GaugeVec)
	}
	var metric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: g.name,
		},
		func() []string {
			var labelNames = make([]string, 0, len(g.lvs))
			for _, lv := range g.lvs {
				labelNames = append(labelNames, lv.Label)
			}
			return labelNames
		}())
	if actualMetric, load = gaugeVecGroup.LoadOrStore(key, metric); !load {
		if err := prometheus.Register(actualMetric.(prometheus.Collector)); err == nil {
			log.LogInfof("register gauge %v", g.name)
		} else {
			log.LogErrorf("register gauge %v failed: %v", g.name, err)
		}
	}

	return actualMetric.(*prometheus.GaugeVec)
}

func GetGauge(name string, lvs ...LabelValue) Gauge {
	if len(lvs) > 0 {
		return &gaugeVec{
			name: name,
			lvs:  lvs,
		}
	}
	return &gauge{
		name: name,
	}
}

func DeleteGaugeLabelValues(name string, lvs ...LabelValue) {
	if len(lvs) > 0 {
		select {
		case gaugeVecDelCh <- &gaugeVec{
			name: name,
			lvs:  lvs,
		}:
		default:
		}
	}
}
