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

var (
	metricGroups sync.Map
	TpMetricPool = &sync.Pool{New: func() interface{} {
		return new(TpMetric)
	}}
	namespace string
	metricC  = make(chan prometheus.Collector, 1)
	enabled = false
)

func Init(cluster string, role string, cfg *config.Config) {
	port := cfg.GetInt64(ConfigKeyExporterPort)
	if port == 0 {
		log.LogInfof("exporter port not set")
		return
	}
	enabled = true

	http.Handle(PromHandlerPattern, promhttp.Handler())

	namespace = fmt.Sprintf("%s_%s", AppName, role)
	addr := fmt.Sprintf(":%d", port)
	go func() {
		http.ListenAndServe(addr, nil)
	}()

	consulAddr := cfg.GetString(ConfigKeyConsulAddr)
	RegistConsul(consulAddr, AppName, role, cluster, port)

	m := RegistGauge("start_time")
	m.Set(float64(time.Now().Unix() * 1000))

	log.LogInfof("exporter Start: %v", addr)
}

type TpMetric struct {
	Start  time.Time
	metric prometheus.Gauge
}

func metricsName(name string) string {
	return namespace + "_" + name
}

func RegistGauge(name string) (o prometheus.Gauge) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistGauge panic,err[%v]", err)
		}
	}()
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
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistTp panic,err[%v]", err)
		}
	}()

	if ! enabled {
		return
	}

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
	if ! enabled {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistTp panic,err[%v]", err)
		}
	}()

	o.metric.Set(float64(time.Since(o.Start).Nanoseconds() / 1e6))
}

func Alarm(name, detail string) {
	if ! enabled {
		return
	}

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
