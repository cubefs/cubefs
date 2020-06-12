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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	PromBackendName       = "prometheus"
	PromHandlerPattern    = "/metrics"     // prometheus handler
	ConfigKeyExporterPort = "exporterPort" //exporter port
)

var (
	exporterPort int64 //prometheus exporter port used by prom_backend and consul
)

// prometheus metric collector backend
type PromBackend struct {
	config      *PromConfig
	metricGroup sync.Map
}

// prom_backend config
type PromConfig struct {
	enabled bool
	port    int64
	router  *mux.Router
}

func ParsePromConfig(cfg *config.Config) *PromConfig {
	promCfg := new(PromConfig)

	exporterPort = cfg.GetInt64(ConfigKeyExporterPort)
	promCfg.port = exporterPort
	promCfg.enabled = true

	return promCfg
}

func ParsePromConfigWithRouter(cfg *config.Config, router *mux.Router, exPort string) *PromConfig {
	promCfg := ParsePromConfig(cfg)
	promCfg.router = router
	promCfg.port, _ = strconv.ParseInt(exPort, 10, 64)
	if promCfg.port == 0 {
		promCfg.enabled = false
	} else {
		promCfg.enabled = true
		exporterPort = promCfg.port
	}

	return promCfg
}

func NewPromBackend(cfg *PromConfig) (b *PromBackend) {
	b = new(PromBackend)
	b.config = cfg
	return
}

func (b *PromBackend) Start() {
	if b.config.router == nil {
		http.Handle(PromHandlerPattern, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			Timeout: 10 * time.Second,
		}))
		addr := fmt.Sprintf(":%d", b.config.port)
		go func() {
			err := http.ListenAndServe(addr, nil)
			if err != nil {
				log.LogError("exporter http serve error: ", err)
			}
		}()
	} else {
		b.config.router.NewRoute().Name(PromHandlerPattern).
			Methods(http.MethodGet).
			Path(PromHandlerPattern).
			Handler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
				Timeout: 10 * time.Second,
			}))

		log.LogDebugf("exporter: prom_backend start router %v", PromHandlerPattern)
	}

	log.LogInfof("exporter: prometheus backend start")
}

func (b *PromBackend) Stop() {
}

// publish prometheus metrics
func (b *PromBackend) Publish(m Metric) {
	switch mt := m.(type) {
	case *Gauge:
		metric := b.GaugeMetric(mt)
		metric.Set(mt.Val())
	case *Counter:
		metric := b.CounterMetric(mt)
		metric.Add(mt.Val())
	case *TimePoint:
		metric := b.GaugeMetric(mt)
		metric.Set(mt.Val())
	case *Alarm:
		metric := b.CounterMetric(mt)
		metric.Add(mt.Val())
	default:
	}
}

func (b *PromBackend) GaugeMetric(c Metric) prometheus.Gauge {
	promName := PromMetricsName(c.Name())
	metric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        promName,
			ConstLabels: c.Labels(),
		})
	key := c.Key()
	actualMetric, load := b.metricGroup.LoadOrStore(key, metric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfof("register metric %v", promName)
		} else {
			log.LogErrorf("register metric %v, %v", promName, err)
		}
	}

	return actualMetric.(prometheus.Gauge)
}

func (b *PromBackend) CounterMetric(c Metric) prometheus.Counter {
	promName := PromMetricsName(c.Name())
	metric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        promName,
			ConstLabels: c.Labels(),
		})
	key := c.Key()
	actualMetric, load := b.metricGroup.LoadOrStore(key, metric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfo("register metric ", promName)
		}
	}

	return actualMetric.(prometheus.Counter)
}

func PromMetricsName(name string) string {
	return replacer.Replace(fmt.Sprintf("%s_%s", Namespace(), name))
}
