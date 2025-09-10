package qos

import (
	"sync"
	"sync/atomic"
	"time"
)

type metricWindow struct {
	opCnt      int64
	byteSize   int64
	latencyNs  int64
	queueSum   int64
	runSum     int64
	sampleCnt  int64
	queueMax   int64
	runMax     int64
	successCnt int64
	errorCnt   int64
	rejectCnt  int64
}

type windowedSeries struct {
	mu       sync.Mutex
	windows  []metricWindow
	idx      int
	winStart time.Time
	winSize  time.Duration
}

type WindowStat struct {
	Bps         int64
	Iops        int64
	Avgrq       int64
	Avgqu       int64
	Await       int64
	QMax        int64
	RunAvg      int64
	RunMax      int64
	SuccessRate float64 // success / total ops in window
	ErrorRate   float64 // error / total ops in window
	RejectRate  float64 // rejects / (ops + rejects) in window
}

type metricsCollector struct {
	mu             sync.RWMutex
	am             *AdaptiveManager
	flowMetricsMap map[IoType]*windowedSeries
	winSize        time.Duration
	sampleInterval time.Duration
	stopCh         chan struct{}
	ioWaiting      sync.Map
	ioRunning      sync.Map
	ioTypes        []IoType
}

func newWindowedSeries(windowsNum int, winSize time.Duration) *windowedSeries {
	return &windowedSeries{
		windows:  make([]metricWindow, windowsNum+1),
		idx:      0,
		winStart: time.Now(),
		winSize:  winSize,
	}
}

func (ws *windowedSeries) rotate(now time.Time) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for now.Sub(ws.winStart) >= ws.winSize {
		ws.idx = (ws.idx + 1) % len(ws.windows)
		ws.windows[ws.idx] = metricWindow{}
		ws.winStart = ws.winStart.Add(ws.winSize)
	}
}

func (ws *windowedSeries) addOp(opBytes int, totalLatency time.Duration, isError bool) {
	ws.mu.Lock()
	w := &ws.windows[ws.idx]
	w.opCnt++
	w.byteSize += int64(opBytes)
	w.latencyNs += int64(totalLatency)
	if isError {
		w.errorCnt++
	} else {
		w.successCnt++
	}
	ws.mu.Unlock()
}

func (ws *windowedSeries) addSample(inqueue, run int) {
	ws.mu.Lock()
	w := &ws.windows[ws.idx]
	w.sampleCnt++
	w.queueSum += int64(inqueue)
	w.runSum += int64(run)
	if int64(inqueue) > w.queueMax {
		w.queueMax = int64(inqueue)
	}
	if int64(run) > w.runMax {
		w.runMax = int64(run)
	}
	ws.mu.Unlock()
}

func (ws *windowedSeries) getHistoryMetrics() []metricWindow {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	n := len(ws.windows) - 1
	out := make([]metricWindow, n)
	for i := 0; i < n; i++ {
		pos := (ws.idx + 1 + i) % (n + 1)
		out[i] = ws.windows[pos]
	}
	return out
}

func newMetricsCollector(windowsNum int, winSize time.Duration, sampleInterval time.Duration, ioTypes []IoType) *metricsCollector {
	mc := &metricsCollector{
		flowMetricsMap: make(map[IoType]*windowedSeries),
		winSize:        winSize,
		sampleInterval: sampleInterval,
		stopCh:         make(chan struct{}),
		ioTypes:        append([]IoType(nil), ioTypes...),
	}
	for _, ioType := range ioTypes {
		mc.flowMetricsMap[ioType] = newWindowedSeries(windowsNum, winSize)
	}
	go func() {
		mc.loop()
	}()
	return mc
}

func (mc *metricsCollector) reconfigure(windowsNum int, winSize, sampleInterval time.Duration) {
	if windowsNum <= 0 {
		return
	}
	mc.mu.Lock()
	mc.winSize = winSize
	mc.sampleInterval = sampleInterval
	newMap := make(map[IoType]*windowedSeries, len(mc.ioTypes))
	for _, ioType := range mc.ioTypes {
		newMap[ioType] = newWindowedSeries(windowsNum, winSize)
	}
	mc.flowMetricsMap = newMap
	mc.mu.Unlock()
}

func (mc *metricsCollector) close() {
	close(mc.stopCh)
}

func (mc *metricsCollector) rotateAll() {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	now := time.Now()
	for _, ws := range mc.flowMetricsMap {
		ws.rotate(now)
	}
}

func (mc *metricsCollector) addOp(ioType IoType, bytes int, totalLatency time.Duration, isError bool) {
	mc.mu.RLock()
	ws := mc.flowMetricsMap[ioType]
	mc.mu.RUnlock()
	if ws != nil {
		ws.addOp(bytes, totalLatency, isError)
	}
}

func (mc *metricsCollector) addReject(ioType IoType) {
	mc.mu.RLock()
	ws := mc.flowMetricsMap[ioType]
	mc.mu.RUnlock()
	if ws != nil {
		ws.mu.Lock()
		w := &ws.windows[ws.idx]
		w.rejectCnt++
		ws.mu.Unlock()
	}
}

func (mc *metricsCollector) incWaiting(ioType IoType) {
	if v, ok := mc.ioWaiting.Load(ioType); ok {
		ptr := v.(*uint32)
		atomic.AddUint32(ptr, 1)
		return
	}
	ptr := new(uint32)
	*ptr = 1
	actual, loaded := mc.ioWaiting.LoadOrStore(ioType, ptr)
	if loaded {
		atomic.AddUint32(actual.(*uint32), 1)
	}
}

func (mc *metricsCollector) decWaiting(ioType IoType) {
	if v, ok := mc.ioWaiting.Load(ioType); ok {
		ptr := v.(*uint32)
		atomic.AddUint32(ptr, ^uint32(0))
	}
}

func (mc *metricsCollector) incRunning(ioType IoType) {
	if v, ok := mc.ioRunning.Load(ioType); ok {
		ptr := v.(*uint32)
		atomic.AddUint32(ptr, 1)
		return
	}
	ptr := new(uint32)
	*ptr = 1
	actual, loaded := mc.ioRunning.LoadOrStore(ioType, ptr)
	if loaded {
		atomic.AddUint32(actual.(*uint32), 1)
	}
}

func (mc *metricsCollector) decRunning(ioType IoType) {
	if v, ok := mc.ioRunning.Load(ioType); ok {
		ptr := v.(*uint32)
		atomic.AddUint32(ptr, ^uint32(0))
	}
}

func (mc *metricsCollector) currIoWaitingAndIoRunning(ioType IoType) (waiting, running int) {
	if v, ok := mc.ioWaiting.Load(ioType); ok {
		ptr := v.(*uint32)
		waiting = int(atomic.LoadUint32(ptr))
	}
	if v, ok := mc.ioRunning.Load(ioType); ok {
		ptr := v.(*uint32)
		running = int(atomic.LoadUint32(ptr))
	}
	return
}

func (mc *metricsCollector) addSample(ioType IoType, inqueue, run int) {
	mc.mu.RLock()
	ws := mc.flowMetricsMap[ioType]
	mc.mu.RUnlock()
	if ws != nil {
		ws.addSample(inqueue, run)
	}
}

func (mc *metricsCollector) windows(ioType IoType) []metricWindow {
	mc.mu.RLock()
	ws := mc.flowMetricsMap[ioType]
	mc.mu.RUnlock()
	if ws == nil {
		return nil
	}
	return ws.getHistoryMetrics()
}

func (mc *metricsCollector) rotateAndSample() {
	mc.rotateAll()
	for _, ioType := range IoTypes {
		w, r := mc.currIoWaitingAndIoRunning(ioType)
		mc.addSample(ioType, w, r)
	}
}

func (mc *metricsCollector) loop() {
	interval := mc.sampleInterval
	if interval <= 0 {
		interval = time.Duration(defaultSampleIntervalMs) * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-mc.stopCh:
			return
		case <-ticker.C:
			mc.rotateAndSample()
			mc.mu.RLock()
			newInterval := mc.sampleInterval
			mc.mu.RUnlock()
			if newInterval <= 0 {
				newInterval = time.Duration(defaultSampleIntervalMs) * time.Millisecond
			}
			if newInterval != interval {
				ticker.Reset(newInterval)
				interval = newInterval
			}
		}
	}
}

func (mc *metricsCollector) metricsWindowsStat(ioType IoType) []WindowStat {
	wins := mc.windows(ioType)
	if len(wins) == 0 {
		return nil
	}

	out := make([]WindowStat, 0, len(wins))
	for _, w := range wins {
		if w.opCnt == 0 && w.sampleCnt == 0 && w.byteSize == 0 {
			out = append(out, WindowStat{})
			continue
		}
		var iops, bps, avgrq, avgqu, await, runAvg int64
		if mc.winSize > 0 {
			iops = (w.opCnt * int64(time.Second)) / mc.winSize.Nanoseconds()
			bps = (w.byteSize * int64(time.Second)) / mc.winSize.Nanoseconds()
		}
		if w.opCnt > 0 {
			avgrq = w.byteSize / w.opCnt
			await = w.latencyNs / w.opCnt
		}
		if w.sampleCnt > 0 {
			avgqu = w.queueSum / w.sampleCnt
			runAvg = w.runSum / w.sampleCnt
		}

		totalOps := w.opCnt
		totalWithReject := totalOps + w.rejectCnt
		var errRate, successRate, rejectRate float64
		if totalWithReject > 0 {
			errRate = float64(w.errorCnt) / float64(totalWithReject)
			successRate = float64(w.successCnt) / float64(totalWithReject)
			rejectRate = float64(w.rejectCnt) / float64(totalWithReject)
		}
		out = append(out, WindowStat{
			Bps:         bps,
			Iops:        iops,
			Avgrq:       avgrq,
			Avgqu:       avgqu,
			Await:       await,
			QMax:        w.queueMax,
			RunAvg:      runAvg,
			RunMax:      w.runMax,
			SuccessRate: successRate,
			ErrorRate:   errRate,
			RejectRate:  rejectRate,
		})
	}
	return out
}

func (mc *metricsCollector) getAllTypeActualIops() (iopsMap map[IoType]int64) {
	iopsMap = make(map[IoType]int64)
	for _, ioType := range IoTypes {
		wins := mc.windows(ioType)
		if len(wins) == 0 {
			continue
		}
		var iops int64
		if mc.winSize > 0 {
			iops = (wins[len(wins)-1].opCnt * int64(time.Second)) / mc.winSize.Nanoseconds()
			iopsMap[ioType] = iops
		}
	}
	return iopsMap
}
