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

	namespace = AppName + "_" + role
	addr := fmt.Sprintf(":%d", port)
	go func() {
		http.ListenAndServe(addr, nil)
	}()

	consulAddr := cfg.GetString(ConfigKeyConsulAddr)
	if len(consulAddr) > 0 {
		RegistConsul(consulAddr, AppName, role, cluster, port)
	}

	m := RegistGauge("start_time")
	m.Set(float64(time.Now().Unix() * 1000))

	log.LogInfof("exporter Start: %v", addr)
}

type TpMetric struct {
	Start  time.Time
	metricName string
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

	newGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: name,
			Help: name,
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

func RegistTp(name string) (tp *TpMetric) {
	defer func() {
		if err := recover(); err != nil {
			log.LogErrorf("RegistTp panic,err[%v]", err)
		}
	}()

	tp = TpMetricPool.Get().(*TpMetric)
	tp.metricName = metricsName(name)
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
				Name: tp.metricName,
				Help: tp.metricName,
			})

		metric, load := metricGroups.LoadOrStore(tp.metricName, tpGauge)
		if !load {
			if enabled {
				prometheus.MustRegister(metric.(prometheus.Gauge))
			}
		}

		metric.(prometheus.Gauge).Set(float64(time.Since(tp.Start).Nanoseconds()))
	} ()
}

func Alarm(name, detail string) {
	name = metricsName(name + "_alarm")

	go func() {
		newMetric := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: name,
				Help: name,
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
