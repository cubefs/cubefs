package exporter

import (
	"sync"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	CollectChSize = 1024 * 32 //collect chan size
)

var (
	collectorInstance *Collector //collector instance
	once              sync.Once
)

// backend to publish metrics
type Backend interface {
	Start()           //start with backend
	Stop()            //
	Publish(m Metric) //push metric with backend
}

// collect for metrics
type Collector struct {
	collectCh chan Metric        //metrics collect channel
	stopCh    chan NULL          //stop signal channel
	backends  map[string]Backend //backends for metrics to publish
	mu        sync.RWMutex
}

func CollectorInstance() *Collector {
	once.Do(func() {
		collectorInstance = &Collector{
			collectCh: make(chan Metric, CollectChSize),
			stopCh:    make(chan NULL, 1),
			backends:  make(map[string]Backend),
		}
	})

	return collectorInstance
}

// register backend for metric collector
func (c *Collector) RegisterBackend(name string, b Backend) {
	if !IsEnabled() {
		return
	}
	c.mu.Lock()
	c.backends[name] = b
	c.mu.Unlock()

	log.LogInfof("exporter: metric collector register backend: %v", name)
}

// collector start
func (c *Collector) Start() {
	if !IsEnabled() {
		log.LogDebugf("exporter: collector disabled for start")
		return
	}
	for _, b := range c.backends {
		b.Start()
	}

	go c.publish()

	startCounter()
}

// stop metrics collector
func (c *Collector) Stop() {
	c.stopCh <- null

	for _, b := range c.backends {
		b.Stop()
	}

	stopCounter()
}

// collect metrics into collect channel
func (c *Collector) Collect(m Metric) {
	select {
	case c.collectCh <- m:
	default:
	}
}

// publish metrics by backends
func (c *Collector) publish() {
	for {
		select {
		case <-c.stopCh:
			log.LogInfof("exporter: stopping exporter collector publish")
			return
		case m := <-c.collectCh:
			for _, b := range c.backends {
				b.Publish(m)
			}
		}
	}
}
