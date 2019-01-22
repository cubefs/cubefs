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
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	"net/http"
	"sync"
	"time"

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
	Name   string
	Labels map[string]string
	Key    string
	Metric prometheus.Metric
	tp     MetricType
}

type TpMetric struct {
	Name  string
	Start time.Time
}

func metricsName(name string) string {
	return namespace + "_" + name
}

func RegistMetricWithLabels(name string, tp MetricType, labels map[string]string) (m *PromeMetric) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistMetric panic,err[%v]", err)
		}
	}()
	name = metricsName(name)
	m = &PromeMetric{
		Name:   name,
		Labels: labels,
		tp:     tp,
	}
	m.registMetric()
	return
}
func RegistMetric(name string, tp MetricType) (m *PromeMetric) {
	return RegistMetricWithLabels(name, tp, nil)
}

func (m *PromeMetric) getMetricKey() (key string, metric prometheus.Metric) {
	if m.tp == Counter {
		metric = prometheus.NewCounter(
			prometheus.CounterOpts{
				Name:        m.Name,
				ConstLabels: m.Labels,
			})
		key = metric.Desc().String()
		m.Metric = metric
		m.Key = key
	} else if m.tp == Gauge {
		metric = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        m.Name,
				ConstLabels: m.Labels,
			})
		key = metric.Desc().String()
		m.Metric = metric
		m.Key = key
	}

	return
}
func (m *PromeMetric) registMetric() {
	key, metric := m.getMetricKey()
	go metricGroups.LoadOrStore(key, metric)
}

func (m *PromeMetric) SetWithLabels(val float64, labels map[string]string) {
	if !enabled {
		return
	}
	if labels != nil {
		m.Labels = labels
	}
	key, tmpMetric := m.getMetricKey()
	actualMetric, load := metricGroups.LoadOrStore(key, tmpMetric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfo("register metric ", key)
		}
	}

	m.SetMetricVal(actualMetric, val)
	return
}

func (m *PromeMetric) Set(val float64) {
	m.SetWithLabels(val, nil)
	return
}

func (m *PromeMetric) SetMetricVal(metric interface{}, val float64) {
	if metric != nil {
		switch metric := metric.(type) {
		case prometheus.Counter:
			metric.Add(val)
		case prometheus.Gauge:
			metric.Set(val)
		default:
		}
	}
}

func RegistCounterLabels(name string, labels map[string]string) (o prometheus.Counter) {
	pm := RegistMetricWithLabels(name, Counter, labels)
	m, load := metricGroups.Load(pm.Key)
	if load {
		o = m.(prometheus.Counter)
	} else {
		o = prometheus.NewCounter(
			prometheus.CounterOpts{
				Name:        pm.Name,
				ConstLabels: pm.Labels,
			})
		metricGroups.LoadOrStore(pm.Key, o)
		err := prometheus.Register(o)
		if err == nil {
			log.LogInfo("register metric ", pm.Key)
		}
	}
	return
}

func RegistCounter(name string) (o prometheus.Counter) {
	return RegistCounterLabels(name, nil)
}

func RegistGaugeLabels(name string, labels map[string]string) (o prometheus.Gauge) {
	pm := RegistMetricWithLabels(name, Gauge, labels)
	m, load := metricGroups.Load(pm.Key)
	if load {
		o = m.(prometheus.Gauge)
	} else {
		o = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        pm.Name,
				ConstLabels: pm.Labels,
			})
		metricGroups.LoadOrStore(pm.Key, o)
		err := prometheus.Register(o)
		if err == nil {
			log.LogInfo("register metric ", pm.Key)
		}
	}
	return
}

func RegistGauge(name string) (o prometheus.Gauge) {
	return RegistGaugeLabels(name, nil)
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
		mk := tpGauge.Desc().String()
		metric, load := metricGroups.LoadOrStore(mk, tpGauge)
		if !load {
			if enabled {
				err := prometheus.Register(metric.(prometheus.Gauge))
				if err != nil {
				}
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
		mk := newMetric.Desc().String()
		m, load := metricGroups.LoadOrStore(mk, newMetric)
		if load {
			o := m.(prometheus.Counter)
			o.Add(1)
		} else {
			if enabled {
				err := prometheus.Register(newMetric)
				if err != nil {
				}
			}
		}
	}()

	return
}
