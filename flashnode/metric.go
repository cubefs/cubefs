package flashnode

import (
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const (
	StatPeriod                = time.Minute * time.Duration(1)
	MetricFlashNodeReadBytes  = "flashNodeReadBytes"
	MetricFlashNodeReadCount  = "flashNodeReadCount"
	MetricFlashNodeHitRate    = "flashNodeHitRate"
	MetricFlashNodeEvictCount = "flashNodeEvictCount"
)

type MetricStat struct {
	ReadBytes uint64
	ReadCount uint64
}

type FlashNodeMetrics struct {
	flashNode        *FlashNode
	stopC            chan struct{}
	MetricReadBytes  *exporter.Counter
	MetricReadCount  *exporter.Counter
	MetricEvictCount *exporter.Gauge
	MetricHitRate    *exporter.Gauge
	Stat             MetricStat
}

func (f *FlashNode) registerMetrics() {
	f.metrics = &FlashNodeMetrics{
		flashNode: f,
		stopC:     make(chan struct{}),
	}
	f.metrics.MetricReadBytes = exporter.NewCounter(MetricFlashNodeReadBytes)
	f.metrics.MetricReadCount = exporter.NewCounter(MetricFlashNodeReadCount)
	f.metrics.MetricEvictCount = exporter.NewGauge(MetricFlashNodeEvictCount)
	f.metrics.MetricHitRate = exporter.NewGauge(MetricFlashNodeHitRate)
	log.LogInfof("registerMetrics")
}

func (f *FlashNode) startMetrics() {
	go f.metrics.statMetrics()
	log.LogInfof("startMetrics")
}

func (fm *FlashNodeMetrics) statMetrics() {
	ticker := time.NewTicker(StatPeriod)

	for {
		select {
		case <-fm.stopC:
			ticker.Stop()
			log.LogInfof("stop metrics ticker")
			return
		case <-ticker.C:
			fm.doStat()
		}
	}
}

func (fm *FlashNodeMetrics) doStat() {
	log.LogInfof("FlashNodeMetrics: doStat")
	fm.setReadBytesMetric()
	fm.setReadCountMetric()
	fm.setEvictCountMetric()
	fm.setHitRateMetric()
}

func (fm *FlashNodeMetrics) setReadBytesMetric() {
	readBytes := atomic.SwapUint64(&fm.Stat.ReadBytes, 0)
	fm.MetricReadBytes.AddWithLabels(int64(readBytes), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr})
}

func (fm *FlashNodeMetrics) setReadCountMetric() {
	readCount := atomic.SwapUint64(&fm.Stat.ReadCount, 0)
	fm.MetricReadCount.AddWithLabels(int64(readCount), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr})
}

func (fm *FlashNodeMetrics) setEvictCountMetric() {
	evictCountMap := fm.flashNode.cacheEngine.GetEvictCount()
	for dataPath, evictCount := range evictCountMap {
		fm.MetricEvictCount.SetWithLabels(float64(evictCount), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr, exporter.Disk: dataPath})
	}
}

func (fm *FlashNodeMetrics) setHitRateMetric() {
	hitRateMap := fm.flashNode.cacheEngine.GetHitRate()
	for dataPath, hitRate := range hitRateMap {
		fm.MetricHitRate.SetWithLabels(hitRate, map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr, exporter.Disk: dataPath})
	}
}

func (f *FlashNode) updateReadBytesMetric(size uint64) {
	if f.metrics != nil {
		atomic.AddUint64(&f.metrics.Stat.ReadBytes, size)
	}
}

func (f *FlashNode) updateReadCountMetric() {
	if f.metrics != nil {
		atomic.AddUint64(&f.metrics.Stat.ReadCount, 1)
	}
}
