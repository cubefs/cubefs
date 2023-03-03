package exporter

import "github.com/cubefs/cubefs/util/exporter/backend/prom"

type Gauge interface {
	Set(val float64)
}

type noopGauge struct {}

func (g noopGauge) Set(val float64) {}

var singletonNoopGauge = &noopGauge{}

func NewGauge(name string, lvs ...LabelValue) Gauge {
	if promEnabled {
		return prom.GetGauge(name, lvs...)
	}
	return singletonNoopGauge
}

func DeleteGaugeLabelValues(name string, lvs ...LabelValue) {
	if promEnabled {
		prom.DeleteGaugeLabelValues(name, lvs...)
	}
}