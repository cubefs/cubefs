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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	PromHandlerPattern    = "/metrics" // TODO what is prom?
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

// Init initializes the exporter.
func Init(cluster string, role string, cfg *config.Config) {
	port := cfg.GetInt64(ConfigKeyExporterPort)
	if port == 0 {
		log.LogInfof("exporter port not set")
		return
	}
	enabled = true
	http.Handle(PromHandlerPattern, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		Timeout: 5 * time.Second,
	}))
	namespace = AppName + "_" + role
	addr := fmt.Sprintf(":%d", port)
	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.LogError("exporter http serve error: ", err)
		}
	}()

	consulAddr := cfg.GetString(ConfigKeyConsulAddr)
	if len(consulAddr) > 0 {
		RegisterConsul(consulAddr, AppName, role, cluster, port)
	}

	m := RegisterMetric("start_time", Gauge)
	m.Set(float64(time.Now().Unix() * 1000))

	log.LogInfof("exporter Start: %v", addr)
}

// PromeMetric defines the struct of the metrics.
type PromeMetric struct {
	Name   string
	Labels map[string]string
	Key    string
	Metric prometheus.Metric
	tp     MetricType
}

// TODO explain
type TpMetric struct {
	Name  string
	Start time.Time
}

func metricsName(name string) string {
	return namespace + "_" + name
}

// RegisterMetricWithLabels registers the given metric with the given labels.
func RegisterMetricWithLabels(name string, tp MetricType, labels map[string]string) (m *PromeMetric) {
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
	m.registerMetric()
	return
}

// RegisterMetric registers the metric.
func RegisterMetric(name string, tp MetricType) (m *PromeMetric) {
	return RegisterMetricWithLabels(name, tp, nil)
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
func (m *PromeMetric) registerMetric() {
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
			log.LogInfo("register metric ", key, m.tp)
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
	if m.tp == Counter {
		metric := metric.(prometheus.Counter)
		metric.Add(val)
	} else if m.tp == Gauge {
		metric := metric.(prometheus.Gauge)
		metric.Set(val)
	}
}

func RegisterCounterLabels(name string, labels map[string]string) (o prometheus.Counter) {
	pm := RegisterMetricWithLabels(name, Counter, labels)
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

func RegisterCounter(name string) (o prometheus.Counter) {
	return RegisterCounterLabels(name, nil)
}

func RegistGaugeLabels(name string, labels map[string]string) (o prometheus.Gauge) {
	pm := RegisterMetricWithLabels(name, Gauge, labels)
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

func RegisterGauge(name string) (o prometheus.Gauge) {
	return RegistGaugeLabels(name, nil)
}

// TODO explain
func RegisterTp(name string) (tp *TpMetric) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegisterTp panic,err[%v]", err)
		}
	}()

	tp = TpMetricPool.Get().(*TpMetric)
	tp.Name = metricsName(name)
	tp.Start = time.Now()

	return
}

// TODO explain
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

		tpCount := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: tp.Name + "_count",
			})
		mk2 := tpCount.Desc().String()
		metric2, load2 := metricGroups.LoadOrStore(mk2, tpCount)
		if !load2 {
			if enabled {
				err := prometheus.Register(metric2.(prometheus.Counter))
				if err != nil {
				}
			}
		}
		metric2.(prometheus.Counter).Add(1)
	}()
}

// TODO explain
// TODO detail is unused
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
