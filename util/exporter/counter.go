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
	"sync"

	"github.com/chubaofs/chubaofs/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CounterGroup sync.Map
	CounterPool  = &sync.Pool{New: func() interface{} {
		return new(Counter)
	}}
	CounterCh chan *Counter
)

func collectCounter() {
	CounterCh = make(chan *Counter, ChSize)
	for {
		m := <-CounterCh
		metric := m.Metric()
		metric.Add(float64(m.val))
	}
}

type Counter struct {
	Gauge
}

func NewCounter(name string) (c *Counter) {
	c = new(Counter)
	c.name = metricsName(name)
	return
}

func (c *Counter) Add(val int64) {
	if !enabledPrometheus {
		return
	}
	c.val = float64(val)
	c.publish()
}

func (c *Counter) publish() {
	select {
	case CounterCh <- c:
	default:
	}
}

func (c *Counter) AddWithLabels(val int64, labels map[string]string) {
	if !enabledPrometheus {
		return
	}
	c.labels = labels
	c.Add(val)
}

func (c *Counter) Metric() prometheus.Counter {
	metric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        c.name,
			ConstLabels: c.labels,
		})
	key := c.Key()
	actualMetric, load := CounterGroup.LoadOrStore(key, metric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfo("register metric ", c.name)
		}
	}

	return actualMetric.(prometheus.Counter)
}
