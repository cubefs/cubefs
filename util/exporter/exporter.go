// Copyright 2018 The Container File System Authors.
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
	"net/http"
	"sync"
	"time"

	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	PromHandlerPattern    = "/metrics"
	AppName               = "cfs"
	ConfigKeyExporterPort = "exporterPort"
	ConfigKeyConsulAddr   = "consulAddr"
)

type MetricType int

const (
	_ MetricType = iota
	Counter
	Gauge
)


var (
	metricGroups sync.Map
	TpMetricPool = &sync.Pool{New: func() interface{} {
		return new(TpMetric)
	}}
	namespace string
	metricC   = make(chan prometheus.Collector, 1)
	enabled   = false
)

func Init(cluster string, role string, cfg *config.Config) {
	port := cfg.GetInt64(ConfigKeyExporterPort)
	if port == 0 {
		log.LogInfof("exporter port not set")
		return
	}
	enabled = true

	http.Handle(PromHandlerPattern, promhttp.Handler())

	namespace = AppName + "_" + role
	addr := fmt.Sprintf(":%d", port)
	go func() {
		http.ListenAndServe(addr, nil)
	}()

	consulAddr := cfg.GetString(ConfigKeyConsulAddr)
	if len(consulAddr) > 0 {
		RegistConsul(consulAddr, AppName, role, cluster, port)
	}

	m := RegistMetric("start_time", Gauge)
	m.Set(float64(time.Now().Unix() * 1000))

	log.LogInfof("exporter Start: %v", addr)
}

type PromeMetric struct {
	Name string
	Labels map[string]string
	tp MetricType
}


type TpMetric struct {
	Name  string
	Start time.Time
}

func metricsName(name string) string {
	return namespace + "_" + name
}

func RegistMetricWithLabels(name string, tp MetricType, labels map[string]string) (m *PromeMetric) {
	name = metricsName(name)
	m = &PromeMetric{
		Name: name,
		Labels: labels,
		tp: tp,
	}
	go m.registMetric()
	return
}
func RegistMetric(name string, tp MetricType) (m *PromeMetric) {
	return RegistMetricWithLabels(name, tp, nil)
}

func (m *PromeMetric) registMetric() {
	if m.tp == Counter {
		newMetrics := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: m.Name,
				ConstLabels: m.Labels,
			})
		go metricGroups.LoadOrStore(m.Name, newMetrics)
	} else if m.tp == Gauge {
		newMetrics := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: m.Name,
				ConstLabels: m.Labels,
			})
		go metricGroups.LoadOrStore(m.Name, newMetrics)
	}
}

func (m *PromeMetric) Set(val float64 ) {
	go func() {
		metric, load := metricGroups.Load(m.Name)
		if !load {
			if m.tp == Counter {
				newMetrics := prometheus.NewCounter(
					prometheus.CounterOpts{
						Name: m.Name,
						ConstLabels: m.Labels,
					})
				metricGroups.LoadOrStore(m.Name, newMetrics)
				newMetrics.Add(val)
			} else if m.tp == Gauge {
				newMetrics := prometheus.NewGauge(
					prometheus.GaugeOpts{
						Name: m.Name,
						ConstLabels: m.Labels,
					})
				metricGroups.LoadOrStore(m.Name, newMetrics)
				newMetrics.Set(val)
			}

		} else{
			switch me := metric.(type) {
			case prometheus.Counter:
				me.Add(val)
			case prometheus.Gauge:
				me.Set(val)
			default:
			}
		}
	}()

	return
}

func RegistCounter(name string) (o prometheus.Counter) {
	return RegistCounterLables(name, nil)
}

func RegistCounterLables(name string, labels map[string]string) (o prometheus.Counter) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistGauge panic,err[%v]", err)
		}
	}()
	name = metricsName(name)
	newMetrics := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: name,
			Help: name,
			ConstLabels: labels,
		})
	m, load := metricGroups.LoadOrStore(name, newMetrics)
	if load {
		o = m.(prometheus.Counter)
		return
	} else {
		o = newMetrics
		if enabled {
			prometheus.MustRegister(newMetrics)
		}
	}

	return
}

func RegistGaugeLables(name string, labels map[string]string) (o prometheus.Gauge) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistGauge panic,err[%v]", err)
		}
	}()
	name = metricsName(name)

	newGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: name,
			Help: name,
			ConstLabels: labels,
		})
	m, load := metricGroups.LoadOrStore(name, newGauge)
	if load {
		o = m.(prometheus.Gauge)
		return
	} else {
		o = newGauge
		if enabled {
			prometheus.MustRegister(newGauge)
		}
	}

	return
}

func RegistGauge(name string) (o prometheus.Gauge) {
	return RegistGaugeLables(name, nil)
}

func RegistTp(name string) (tp *TpMetric) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistTp panic,err[%v]", err)
		}
	}()

	tp = TpMetricPool.Get().(*TpMetric)
	tp.Name = metricsName(name)
	tp.Start = time.Now()

	return
}

func (tp *TpMetric) CalcTp() {
	if tp == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("CalcTp panic,err[%v]", err)
		}
	}()

	go func() {
		tpGauge := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: tp.Name,
			})

		metric, load := metricGroups.LoadOrStore(tp.Name, tpGauge)
		if !load {
			if enabled {
				prometheus.MustRegister(metric.(prometheus.Gauge))
			}
		}

		metric.(prometheus.Gauge).Set(float64(time.Since(tp.Start).Nanoseconds()))
	}()
}

func Alarm(name, detail string) {
	name = metricsName(name + "_alarm")

	go func() {
		newMetric := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: name,
			})
		m, load := metricGroups.LoadOrStore(name, newMetric)
		if load {
			o := m.(prometheus.Counter)
			o.Add(1)
		} else {
			if enabled {
				prometheus.MustRegister(newMetric)
			}
		}
	}()

	return
}
