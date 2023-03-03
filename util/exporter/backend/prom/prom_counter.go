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
	counterGroup    sync.Map
	counterVecGroup sync.Map
	counterCh       chan *counter
	counterVecCh    chan *counterVec
	counterPool     = sync.Pool{
		New: func() interface{} {
			return &counter{}
		},
	}
	counterVecPool = sync.Pool{
		New: func() interface{} {
			return &counterVec{}
		},
	}
)

func collectCounter() {
	defer wg.Done()
	log.LogInfof("Exporter: counter collector start")
	counterCh = make(chan *counter, chSize)
	counterVecCh = make(chan *counterVec, chSize)
	for {
		select {
		case <-stopC:
			log.LogInfof("Exporter: counter collector stopped")
			return
		case m := <-counterCh:
			metric := m.metric()
			metric.Add(m.val)
			counterPool.Put(m)
		case m := <-counterVecCh:
			metric := m.metric()
			var labels = make(map[string]string, len(m.lvs))
			for _, lv := range m.lvs {
				labels[lv.Label] = lv.Value
			}
			metric.With(labels).Add(m.val)
			counterVecPool.Put(m)
		}
	}
}

type Counter interface {
	Add(val float64)
}

type counter struct {
	name   string
	labels map[string]string
	val    float64
}

func (c *counter) Add(val float64) {
	c.val = val
	c.publish()
}

func (c *counter) publish() {
	select {
	case counterCh <- c:
	default:
	}
}

func (c *counter) metric() prometheus.Counter {
	var key = stringMD5(c.name)
	var actualMetric interface{}
	var load bool
	if actualMetric, load = counterGroup.Load(key); load {
		return actualMetric.(prometheus.Counter)
	}

	var metric = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        c.name,
			ConstLabels: c.labels,
		})
	if actualMetric, load = counterGroup.LoadOrStore(key, metric); !load {
		if err := prometheus.Register(actualMetric.(prometheus.Collector)); err == nil {
			log.LogInfof("Exporter: register counter %v", c.name)
		} else {
			log.LogErrorf("Exporter: register summary %v failed: %v", c.name, err)
		}
	}

	return actualMetric.(prometheus.Counter)
}

type counterVec struct {
	name string
	val  float64
	lvs  []LabelValue
}

func (c *counterVec) Add(val float64) {
	c.val = val
	c.publish()
}

func (c *counterVec) publish() {
	select {
	case counterVecCh <- c:
	default:
	}
}

func (c *counterVec) metric() *prometheus.CounterVec {
	var key = stringMD5(c.name)
	var actualMetric interface{}
	var load bool
	if actualMetric, load = counterVecGroup.Load(key); load {
		return actualMetric.(*prometheus.CounterVec)
	}

	var metric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: c.name,
		},
		func() []string {
			var labelNames = make([]string, 0, len(c.lvs))
			for _, lv := range c.lvs {
				labelNames = append(labelNames, lv.Label)
			}
			return labelNames
		}())
	if actualMetric, load = counterVecGroup.LoadOrStore(key, metric); !load {
		if err := prometheus.Register(actualMetric.(prometheus.Collector)); err == nil {
			log.LogInfof("Exporter: register counter %v", c.name)
		} else {
			log.LogErrorf("Exporter: register summary %v failed: %v", c.name, err)
		}
	}

	return actualMetric.(*prometheus.CounterVec)
}

func GetCounter(name string, lvs ...LabelValue) Counter {
	if len(lvs) > 0 {
		var c = counterVecPool.Get().(*counterVec)
		c.name = name
		c.lvs = lvs
		return c
	}
	var c = counterPool.Get().(*counter)
	c.name = name
	return c
}
