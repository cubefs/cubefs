package flashnode

import (
	"path"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const (
	StatPeriod                = time.Minute * time.Duration(1)
	MetricFlashNodeReadBytes  = "flashNodeReadBytes"
	MetricFlashNodeReadCount  = "flashNodeReadCount"
	MetricFlashNodeWriteBytes = "flashNodeWriteBytes"
	MetricFlashNodeWriteCount = "flashNodeWriteCount"
	MetricFlashNodeHitRate    = "flashNodeHitRate"
	MetricFlashNodeEvictCount = "flashNodeEvictCount"
)

type FlashNodeMetrics struct {
	flashNode        *FlashNode
	stopC            chan struct{}
	MetricReadBytes  *exporter.Gauge
	MetricReadCount  *exporter.Gauge
	MetricWriteBytes *exporter.Gauge
	MetricWriteCount *exporter.Gauge
	MetricEvictCount *exporter.Gauge
	MetricHitRate    *exporter.Gauge
}

func (f *FlashNode) registerMetrics(disks []*cachengine.Disk) {
	f.metrics = &FlashNodeMetrics{
		flashNode: f,
		stopC:     make(chan struct{}),
	}

	f.metrics.MetricReadBytes = exporter.NewGauge(MetricFlashNodeReadBytes)
	f.metrics.MetricReadCount = exporter.NewGauge(MetricFlashNodeReadCount)
	f.metrics.MetricWriteBytes = exporter.NewGauge(MetricFlashNodeWriteBytes)
	f.metrics.MetricWriteCount = exporter.NewGauge(MetricFlashNodeWriteCount)
	f.metrics.MetricEvictCount = exporter.NewGauge(MetricFlashNodeEvictCount)
	f.metrics.MetricHitRate = exporter.NewGauge(MetricFlashNodeHitRate)
	for _, d := range disks {
		cachengine.StatMap[path.Join(d.Path, cachengine.DefaultCacheDirName)] = new(cachengine.MetricStat)
	}

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
	fm.setWriteBytesMetric()
	fm.setWriteCountMetric()
	fm.setEvictCountMetric()
	fm.setHitRateMetric()
}

func (fm *FlashNodeMetrics) setReadBytesMetric() {
	for d, stat := range cachengine.StatMap {
		readBytes := atomic.SwapUint64(&stat.ReadBytes, 0)
		fm.MetricReadBytes.SetWithLabels(float64(readBytes), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr, exporter.Disk: d})
	}
}

func (fm *FlashNodeMetrics) setReadCountMetric() {
	for d, stat := range cachengine.StatMap {
		readCount := atomic.SwapUint64(&stat.ReadCount, 0)
		fm.MetricReadCount.SetWithLabels(float64(readCount), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr, exporter.Disk: d})
	}
}

func (fm *FlashNodeMetrics) setWriteBytesMetric() {
	for d, stat := range cachengine.StatMap {
		writeBytes := atomic.SwapUint64(&stat.WriteBytes, 0)
		fm.MetricWriteBytes.SetWithLabels(float64(writeBytes), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr, exporter.Disk: d})
	}
}

func (fm *FlashNodeMetrics) setWriteCountMetric() {
	for d, stat := range cachengine.StatMap {
		writeCount := atomic.SwapUint64(&stat.WriteCount, 0)
		fm.MetricWriteCount.SetWithLabels(float64(writeCount), map[string]string{"cluster": fm.flashNode.clusterID, exporter.FlashNode: fm.flashNode.localAddr, exporter.Disk: d})
	}
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

func (fm *FlashNodeMetrics) updateReadBytesMetric(size uint64, d string) {
	if stat, ok := cachengine.StatMap[d]; ok {
		atomic.AddUint64(&stat.ReadBytes, size)
	}
}

func (fm *FlashNodeMetrics) updateReadCountMetric(d string) {
	if stat, ok := cachengine.StatMap[d]; ok {
		atomic.AddUint64(&stat.ReadCount, 1)
	}
}
