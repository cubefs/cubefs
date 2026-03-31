package flashnode

import (
	"math"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/remotecache/flashnode/cachengine"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	StatPeriod                         = time.Minute * time.Duration(1)
	metricLabelTopoName                = "topoName"
	MetricFlashNodeReadBytes           = "flashNodeReadBytes"
	MetricFlashNodeReadCount           = "flashNodeReadCount"
	MetricFlashNodeWriteBytes          = "flashNodeWriteBytes"
	MetricFlashNodeWriteCount          = "flashNodeWriteCount"
	MetricFlashNodeHitRate             = "flashNodeHitRate"
	MetricFlashNodeEvictCount          = "flashNodeEvictCount"
	MetricFlashNodeCacheErrorCount     = "flashNodeCacheErrorCount"
	MetricFlashNodeCacheBytes          = "flashNodeCacheBytes"
	MetricFlashNodeHandleReadLatency   = "flashNodeHandleReadLatency"
	MetricFlashNodeSourceDataLatency   = "flashNodeSourceDataLatency"
	MetricFlashNodeHitCacheReadLatency = "flashNodeHitCacheReadLatency"
	MetricFlashNodeFlowLimitedCount    = "flashNodeFlowLimitedCount"
	MetricFlashNodeRunLimitedCount     = "flashNodeRunLimitedCount"
	MetricFlashNodeLruUsageRatio       = "flashNodeLruUsageRatio"
	MetricFlashNodeDiskUsageRatio      = "flashNodeDiskUsageRatio"
	MetricFlashNodeMemoryRatio         = "flashNodeMemoryRatio"
	MetricFlashNodeVolHitRate          = "flashNodeVolHitRate"
	MetricFlashNodeVolEvictCount       = "flashNodeVolEvictCount"
	MetricFlashNodeVolSize             = "flashNodeVolSize"
	MetricFlashNodeVolReadBytes        = "flashNodeVolReadBytes"
	MetricFlashNodeVolReadCount        = "flashNodeVolReadCount"
	MetricFlashNodeVolWriteBytes       = "flashNodeVolWriteBytes"
	MetricFlashNodeVolWriteCount       = "flashNodeVolWriteCount"
	MetricFlashNodePreheatReadBytes    = "flashNodePreheatReadBytes"
	MetricFlashNodePreheatErrorCount   = "flashNodePreheatErrorCount"
)

type FlashNodeMetrics struct {
	flashNode                 *FlashNode
	stopC                     chan struct{}
	MetricReadBytes           *exporter.Gauge
	MetricReadCount           *exporter.Gauge
	MetricWriteBytes          *exporter.Gauge
	MetricWriteCount          *exporter.Gauge
	MetricPreheatReadBytes    *exporter.Gauge
	MetricEvictCount          *exporter.Gauge
	MetricCacheErrorCount     *exporter.Gauge
	MetricHitRate             *exporter.Gauge
	MetricCacheBytes          *exporter.Gauge
	MetricHandleReadLatency   *exporter.Gauge
	MetricSourceDataLatency   *exporter.Gauge
	MetricHitCacheReadLatency *exporter.Gauge
	MetricFlowLimitedCount    *exporter.Gauge
	MetricRunLimitedCount     *exporter.Gauge
	MetricLruUsageRatio       *exporter.Gauge
	MetricDiskUsageRatio      *exporter.Gauge
	MetricMemoryRatio         *exporter.Gauge
	MetricVolHitRate          *exporter.Gauge
	MetricVolEvictCount       *exporter.Gauge
	MetricVolSize             *exporter.Gauge
	MetricVolReadBytes        *exporter.Gauge
	MetricVolReadCount        *exporter.Gauge
	MetricVolWriteBytes       *exporter.Gauge
	MetricVolWriteCount       *exporter.Gauge
	MetricPreheatErrorCount   *exporter.Gauge
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
	f.metrics.MetricPreheatReadBytes = exporter.NewGauge(MetricFlashNodePreheatReadBytes)
	f.metrics.MetricEvictCount = exporter.NewGauge(MetricFlashNodeEvictCount)
	f.metrics.MetricCacheErrorCount = exporter.NewGauge(MetricFlashNodeCacheErrorCount)
	f.metrics.MetricHitRate = exporter.NewGauge(MetricFlashNodeHitRate)
	f.metrics.MetricCacheBytes = exporter.NewGauge(MetricFlashNodeCacheBytes)
	f.metrics.MetricHandleReadLatency = exporter.NewGauge(MetricFlashNodeHandleReadLatency)
	f.metrics.MetricSourceDataLatency = exporter.NewGauge(MetricFlashNodeSourceDataLatency)
	f.metrics.MetricHitCacheReadLatency = exporter.NewGauge(MetricFlashNodeHitCacheReadLatency)
	f.metrics.MetricFlowLimitedCount = exporter.NewGauge(MetricFlashNodeFlowLimitedCount)
	f.metrics.MetricRunLimitedCount = exporter.NewGauge(MetricFlashNodeRunLimitedCount)
	f.metrics.MetricLruUsageRatio = exporter.NewGauge(MetricFlashNodeLruUsageRatio)
	f.metrics.MetricDiskUsageRatio = exporter.NewGauge(MetricFlashNodeDiskUsageRatio)
	f.metrics.MetricMemoryRatio = exporter.NewGauge(MetricFlashNodeMemoryRatio)
	f.metrics.MetricVolHitRate = exporter.NewGauge(MetricFlashNodeVolHitRate)
	f.metrics.MetricVolEvictCount = exporter.NewGauge(MetricFlashNodeVolEvictCount)
	f.metrics.MetricVolSize = exporter.NewGauge(MetricFlashNodeVolSize)
	f.metrics.MetricVolReadBytes = exporter.NewGauge(MetricFlashNodeVolReadBytes)
	f.metrics.MetricVolReadCount = exporter.NewGauge(MetricFlashNodeVolReadCount)
	f.metrics.MetricVolWriteBytes = exporter.NewGauge(MetricFlashNodeVolWriteBytes)
	f.metrics.MetricVolWriteCount = exporter.NewGauge(MetricFlashNodeVolWriteCount)
	f.metrics.MetricPreheatErrorCount = exporter.NewGauge(MetricFlashNodePreheatErrorCount)
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
	fm.setCacheErrorCountMetric()
	fm.setHitRateMetric()
	fm.setCacheBytesMetric()
	fm.setLatencyMetric()
	fm.setLimitedCountMetric()
	fm.setLruUsageRatioMetric()
	fm.setDiskUsageRatioMetric()
	fm.setMemoryRatioMetric()
	fm.setVolCacheStatsMetric()
}

func (fm *FlashNodeMetrics) baseLabels() map[string]string {
	return map[string]string{
		"cluster":           fm.flashNode.clusterID,
		exporter.FlashNode:  fm.flashNode.localAddr,
		metricLabelTopoName: fm.flashNode.getTopoName(),
	}
}

func (fm *FlashNodeMetrics) labelsWithDisk(disk string) map[string]string {
	labels := fm.baseLabels()
	labels[exporter.Disk] = disk
	return labels
}

func (fm *FlashNodeMetrics) labelsWithVol(vol string) map[string]string {
	labels := fm.baseLabels()
	labels[exporter.Vol] = vol
	return labels
}

func (fm *FlashNodeMetrics) setReadBytesMetric() {
	for d, stat := range cachengine.StatMap {
		readBytes := atomic.SwapUint64(&stat.ReadBytes, 0)
		fm.MetricReadBytes.SetWithLabels(float64(readBytes), fm.labelsWithDisk(d))
	}
}

func (fm *FlashNodeMetrics) setReadCountMetric() {
	for d, stat := range cachengine.StatMap {
		readCount := atomic.SwapUint64(&stat.ReadCount, 0)
		fm.MetricReadCount.SetWithLabels(float64(readCount), fm.labelsWithDisk(d))
	}
}

func (fm *FlashNodeMetrics) setWriteBytesMetric() {
	for d, stat := range cachengine.StatMap {
		writeBytes := atomic.SwapUint64(&stat.WriteBytes, 0)
		fm.MetricWriteBytes.SetWithLabels(float64(writeBytes), fm.labelsWithDisk(d))
	}
}

func (fm *FlashNodeMetrics) setWriteCountMetric() {
	for d, stat := range cachengine.StatMap {
		writeCount := atomic.SwapUint64(&stat.WriteCount, 0)
		fm.MetricWriteCount.SetWithLabels(float64(writeCount), fm.labelsWithDisk(d))
	}
}

func (fm *FlashNodeMetrics) setEvictCountMetric() {
	evictCountMap := fm.flashNode.cacheEngine.GetEvictCount()
	for dataPath, evictCount := range evictCountMap {
		fm.MetricEvictCount.SetWithLabels(float64(evictCount), fm.labelsWithDisk(dataPath))
	}
}

func (fm *FlashNodeMetrics) setCacheErrorCountMetric() {
	cacheErrorCountMap := fm.flashNode.cacheEngine.GetCacheErrorCount()
	for dataPath, cacheErrorCount := range cacheErrorCountMap {
		fm.MetricCacheErrorCount.SetWithLabels(float64(cacheErrorCount), fm.labelsWithDisk(dataPath))
	}
}

func (fm *FlashNodeMetrics) setHitRateMetric() {
	hitRateMap := fm.flashNode.cacheEngine.GetHitRate()
	for dataPath, hitRate := range hitRateMap {
		fm.MetricHitRate.SetWithLabels(hitRate, fm.labelsWithDisk(dataPath))
	}
}

func (fm *FlashNodeMetrics) setCacheBytesMetric() {
	cacheBytesMap := fm.flashNode.cacheEngine.GetCacheBytes()
	for dataPath, bytes := range cacheBytesMap {
		fm.MetricCacheBytes.SetWithLabels(float64(bytes), fm.labelsWithDisk(dataPath))
	}
}

func (fm *FlashNodeMetrics) setLatencyMetric() {
	handleReadLatency := stat.GetAvgLatencyMs("FlashNode:opCacheRead")
	sourceDataLatency := stat.GetAvgLatencyMs("MissCacheRead:ReadFromDN")
	hitCacheReadLatency := stat.GetAvgLatencyMs("HitCacheRead")

	fm.MetricHandleReadLatency.SetWithLabels(float64(handleReadLatency), fm.baseLabels())
	fm.MetricSourceDataLatency.SetWithLabels(float64(sourceDataLatency), fm.baseLabels())
	fm.MetricHitCacheReadLatency.SetWithLabels(float64(hitCacheReadLatency), fm.baseLabels())
}

func (fm *FlashNodeMetrics) setLimitedCountMetric() {
	flowLimitedCount := stat.GetCount("FlashNode:opCacheRead[flow limited]")
	runLimitedCount := stat.GetCount("FlashNode:opCacheRead[run limited]")

	fm.MetricFlowLimitedCount.SetWithLabels(float64(flowLimitedCount), fm.baseLabels())
	fm.MetricRunLimitedCount.SetWithLabels(float64(runLimitedCount), fm.baseLabels())
}

func (fm *FlashNodeMetrics) setLruUsageRatioMetric() {
	usageRatio := fm.flashNode.cacheEngine.GetLruUsageRatio()
	fm.MetricLruUsageRatio.SetWithLabels(usageRatio, fm.baseLabels())
}

func (fm *FlashNodeMetrics) setDiskUsageRatioMetric() {
	diskUsageRatioMap := fm.flashNode.cacheEngine.GetDiskUsageRatio()
	for dataPath, usageRatio := range diskUsageRatioMap {
		fm.MetricDiskUsageRatio.SetWithLabels(usageRatio, fm.labelsWithDisk(dataPath))
	}
}

func (fm *FlashNodeMetrics) setMemoryRatioMetric() {
	// Get system total memory
	totalMem, _, err := util.GetMemInfo()
	if err != nil {
		log.LogErrorf("setMemoryRatioMetric: failed to get system memory info, err: %v", err)
		return
	}
	if totalMem == 0 {
		log.LogWarnf("setMemoryRatioMetric: system total memory is 0")
		return
	}

	// Get process memory usage
	processMem, err := util.GetProcessMemory(os.Getpid())
	if err != nil {
		log.LogErrorf("setMemoryRatioMetric: failed to get process memory, err: %v", err)
		return
	}

	// Calculate memory ratio: process memory / system total memory
	memoryRatio := float64(processMem) / float64(totalMem)
	fm.MetricMemoryRatio.SetWithLabels(memoryRatio, fm.baseLabels())
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

func (fm *FlashNodeMetrics) setVolCacheStatsMetric() {
	volStats := fm.flashNode.cacheEngine.GetAndResetVolStats()
	log.LogDebugf("MetricVolSize: volStats %v", volStats)
	for vol, stats := range volStats {
		labels := fm.labelsWithVol(vol)
		if stats.Hits+stats.Misses > 0 {
			hitRate := float64(stats.Hits) / float64(stats.Hits+stats.Misses)
			fm.MetricVolHitRate.SetWithLabels(math.Trunc(hitRate*1e4+0.5)*1e-4, labels)
		}
		fm.MetricVolEvictCount.SetWithLabels(float64(stats.Evicts), labels)
		fm.MetricVolSize.SetWithLabels(float64(stats.CacheSize), labels)
		fm.MetricVolReadBytes.SetWithLabels(float64(stats.ReadBytes), labels)
		fm.MetricVolReadCount.SetWithLabels(float64(stats.ReadCount), labels)
		fm.MetricVolWriteBytes.SetWithLabels(float64(stats.WriteBytes), labels)
		fm.MetricVolWriteCount.SetWithLabels(float64(stats.WriteCount), labels)
		fm.MetricPreheatReadBytes.SetWithLabels(float64(stats.PreheatReadBytes), labels)
		fm.MetricPreheatErrorCount.SetWithLabels(float64(stats.PreheatErrorNum), labels)
		log.LogDebugf("MetricVolSize: set %v for vol %v", float64(stats.CacheSize), vol)
	}
}
