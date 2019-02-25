package exporter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tiglabs/containerfs/util/log"
	"sync"
)

var (
	CounterGroup sync.Map
	CounterPool  = &sync.Pool{New: func() interface{} {
		return new(Counter)
	}}
	CounterCh = make(chan *Counter, ChSize)
)

func collectCounter() {
	for {
		m := <-CounterCh
		metric := m.Metric()
		metric.Add(float64(m.val))
		CounterPool.Put(m)
	}
}

type Counter struct {
	Gauge
}

func NewCounter(name string) (c *Counter) {
	if !enabled {
		return
	}
	c = CounterPool.Get().(*Counter)
	c.name = metricsName(name)
	return
}

func (c *Counter) Add(val int64) {
	if !enabled {
		return
	}
	c.val = val
	c.publish()
}

func (c *Counter) publish() {
	select {
	case CounterCh <- c:
	default:
	}
}

func (c *Counter) AddWithLabels(val int64, labels map[string]string) {
	if !enabled {
		return
	}
	c.labels = labels
	c.Add(val)
}

func (c *Counter) Metric() prometheus.Counter {
	metric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        c.name,
			ConstLabels: c.labels,
		})
	key := c.Key()
	actualMetric, load := CounterGroup.LoadOrStore(key, metric)
	if !load {
		err := prometheus.Register(actualMetric.(prometheus.Collector))
		if err == nil {
			log.LogInfo("register metric ", c.name)
		}
	}

	return actualMetric.(prometheus.Counter)
}
