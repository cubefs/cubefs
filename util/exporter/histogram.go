package exporter

import (
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// us 1us, 100us, 500us, 1ms, 5ms, 50ms, 200ms, 500ms, 1s, 3s
	buckets = []float64{1, 50, 250, 500, 2500, 5000, 25000, 50000, 250000, 500000, 2500000, 5000000}

	HistogramGroup sync.Map
	HistogramCh    chan *Histogram
	once           = sync.Once{}
)

func collectHistogram() {
	HistogramCh = make(chan *Histogram, ChSize)
	for {
		m := <-HistogramCh
		metric := m.Metric()
		metric.Observe(m.val / 1000)
		log.LogDebugf("collect metric %v", m)
	}
}

type Histogram struct {
	name   string
	labels map[string]string
	val    float64
}

func (c *Histogram) Key() (key string) {
	return stringMD5(c.Name())
}

func (g *Histogram) Name() string {
	return fmt.Sprintf("{%s: %s}", g.name, stringMapToString(g.labels))
}

func (g *Histogram) String() string {
	return fmt.Sprintf("{name: %s, labels: %s, val: %v}", g.name, stringMapToString(g.labels), g.val)
}

func (c *Histogram) Metric() prometheus.Histogram {
	if enablePush {
		once.Do(func() {
			buckets = []float64{1, 300, 1000, 5000, 500000, 2500000}
		})
	}

	metric := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:        c.name,
			ConstLabels: c.labels,
			Buckets:     buckets,
		})

	key := c.Key()
	actualMetric, load := HistogramGroup.LoadOrStore(key, metric)
	if load {
		return actualMetric.(prometheus.Histogram)
	}

	if enablePush {
		registry.MustRegister(actualMetric.(prometheus.Collector))
		return actualMetric.(prometheus.Histogram)
	}

	err := prometheus.Register(actualMetric.(prometheus.Collector))
	if err == nil {
		log.LogInfof("register metric %v", c.Name())
	} else {
		log.LogErrorf("register metric %v, %v", c.Name(), err)
	}

	return actualMetric.(prometheus.Histogram)
}

func (h *Histogram) publish() {
	select {
	case HistogramCh <- h:
	default:
	}
}
