package exporter

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/tiglabs/containerfs/util/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	ExporterHandlerPattern = "/metrics"
	MetricsPrefix          = "cfs"
	DefaultExporterPort    = 9510
)

var (
	metricGroups sync.Map
	TpMetricPool = &sync.Pool{New: func() interface{} {
		return new(TpMetric)
	}}
	namespace string
)

func Init(name string, port int64) {
	if port == 0 {
		log.LogInfof("exporter port not set")
		return
	}

	http.Handle(ExporterHandlerPattern, promhttp.Handler())

	namespace = fmt.Sprintf("%s_%s", MetricsPrefix, name)
	addr := fmt.Sprintf(":%d", port)
	go func() {
		http.ListenAndServe(addr, nil)
	}()

	m := RegistGauge("start_time")
	defer m.Set(float64(time.Now().Unix() * 1000))
	log.LogInfof("exporter Start: %v", addr)
}

type TpMetric struct {
	Start  time.Time
	metric prometheus.Gauge
}

func metricsName(name string) string {
	return fmt.Sprintf("%s_%s", namespace, name)
}

func RegistGauge(name string) (o prometheus.Gauge) {
	name = metricsName(name)
	m, ok := metricGroups.Load(name)
	if ok {
		o = m.(prometheus.Gauge)
		return
	} else {
		o = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: name,
				Help: name,
			})
		prometheus.MustRegister(o)

		metricGroups.Store(name, o)
	}

	return
}

func RegistTp(name string) (o *TpMetric) {
	name = metricsName(name)
	m, ok := metricGroups.Load(name)
	if ok {
		o = m.(*TpMetric)
		o.Start = time.Now()
		return
	} else {
		o = TpMetricPool.Get().(*TpMetric)
		o.Start = time.Now()
		o.metric = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: name,
				Help: name,
			})
		prometheus.MustRegister(o.metric)

		metricGroups.Store(name, o)
	}

	return
}

func (o *TpMetric) CalcTpMS() {
	o.metric.Set(float64(time.Since(o.Start).Nanoseconds() / 1e6))
}

func Alarm(name, detail string) {
	name = metricsName(name + "_alarm")
	o, ok := metricGroups.Load(name)
	if ok {
		m := o.(prometheus.Counter)
		m.Add(1)
	} else {
		m := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: name,
				Help: name,
			})
		prometheus.MustRegister(m)
		metricGroups.Store(name, m)
	}

	return
}
