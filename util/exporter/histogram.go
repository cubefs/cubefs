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
	HistogramPool  = sync.Pool{New: func() interface{} {
		return new(Histogram)
	}}
	once = sync.Once{}
)

func collectHistogram() {
	HistogramCh = make(chan *Histogram, ChSize)
	for {
		m := <-HistogramCh
		metric := m.Metric()
		metric.Observe(m.val / 1000)
		putHistogramToPool(m)
	}
}

func getHistogramFromPool() *Histogram {
	return HistogramPool.Get().(*Histogram)
}

func putHistogramToPool(h *Histogram) {
	h.name = ""
	h.labels = nil
	h.metricKey = ""
	h.val = 0
	HistogramPool.Put(h)
}

func SetBuckets(bks []float64) {
	buckets = bks
	log.LogWarnf("set buckets to %v", bks)
}

type Histogram struct {
	name      string
	labels    map[string]string
	metricKey string
	val       float64
}

func (c *Histogram) ensureMetricKey() string {
	if c.metricKey != "" {
		return c.metricKey
	}
	c.metricKey = labelsMetricKey(c.name, c.labels)
	return c.metricKey
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

	key := c.ensureMetricKey()
	if v, ok := HistogramGroup.Load(key); ok {
		return v.(prometheus.Histogram)
	}

	metric := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:        c.name,
			ConstLabels: c.labels,
			Buckets:     buckets,
		})
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

func publishHistogram(name string, val float64, labels map[string]string) {
	h := getHistogramFromPool()
	h.name = name
	h.labels = labels
	h.metricKey = labelsMetricKey(name, labels)
	h.val = val

	select {
	case HistogramCh <- h:
	default:
		putHistogramToPool(h)
	}
}

func (h *Histogram) publish() {
	publishHistogram(h.name, h.val, h.labels)
}
