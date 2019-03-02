package exporter

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tiglabs/containerfs/util/log"
)

var (
	GaugePool = &sync.Pool{New: func() interface{} {
		return new(Gauge)
	}}

	GaugeGroup sync.Map
	GaugeCh    chan *Gauge
)

func collectGauge() {
	GaugeCh = make(chan *Gauge, ChSize)
	for {
		m := <-GaugeCh
		metric := m.Metric()
		metric.Set(float64(m.val))
		GaugePool.Put(m)
	}
}

type Gauge struct {
	name   string
	labels map[string]string
	val    int64
	ch     chan interface{}
}

func NewGauge(name string) (g *Gauge) {
	if !enabled {
		return
	}
	g = GaugePool.Get().(*Gauge)
	g.name = metricsName(name)
	return
}

func (c *Gauge) Key() (key string) {
	str := c.name
	if len(c.labels) > 0 {
		str = fmt.Sprintf("%s-%s", c.name, stringMapToString(c.labels))
	}

	return stringMD5(str)
}

func (c *Gauge) Metric() prometheus.Gauge {
	metric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        c.name,
			ConstLabels: c.labels,
		})
	key := c.Key()
	actualMetric, load := GaugeGroup.LoadOrStore(key, metric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfo("register metric ", c.name)
		}
	}

	return actualMetric.(prometheus.Gauge)
}

func (g *Gauge) Set(val int64) {
	if !enabled {
		return
	}
	g.val = val
	g.publish()
}

func (c *Gauge) publish() {
	select {
	case GaugeCh <- c:
	default:
	}
}

func (g *Gauge) SetWithLabels(val int64, labels map[string]string) {
	if !enabled {
		return
	}
	g.labels = labels
	g.Set(val)
}
