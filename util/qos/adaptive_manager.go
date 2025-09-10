// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package qos

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type IoType int

const (
	Read IoType = iota // highest priority
	Write
	Create
	Delete
	AsyncRead
	AsyncWrite
	IoTypeMax
)

type IoPriority int

const (
	HighPriority IoPriority = iota
	MediumPriority
	LowPriority
)

type LatencyZone int

const (
	LatRelaxZone  LatencyZone = iota // Below safety boundary - can relax throttling
	LatSafetyZone                    // Between safety boundary and max tolerance - maintain current state
	LatDangerZone                    // Above max tolerance - need to escalate throttling
)

type ManagerState int

const (
	StateIdle ManagerState = iota
	StateThrottling
	StateRecovering
)

var (
	IoTypes = []IoType{Read, Write, Create, Delete, AsyncRead, AsyncWrite}

	IoPriorityMap = map[IoType]IoPriority{
		Read:       HighPriority,
		Write:      HighPriority,
		Create:     HighPriority,
		Delete:     MediumPriority,
		AsyncRead:  LowPriority,
		AsyncWrite: LowPriority,
	}

	IoTypeNames = map[IoType]string{
		Read:       "Read",
		Write:      "Write",
		Create:     "Create",
		Delete:     "Delete",
		AsyncRead:  "AsyncRead",
		AsyncWrite: "AsyncWrite",
	}

	managerStateNames = map[ManagerState]string{
		StateIdle:       "Idle",
		StateThrottling: "Throttling",
		StateRecovering: "Recovering",
	}

	latencyZoneNames = map[LatencyZone]string{
		LatRelaxZone:  "Relax",
		LatSafetyZone: "Safety",
		LatDangerZone: "Danger",
	}
)

const (
	auditOpAdaptiveEscalate = "DiskAdaptiveEscalate"
	auditOpAdaptiveRelax    = "DiskAdaptiveRelax"
)

func managerStateString(state ManagerState) string {
	if name, ok := managerStateNames[state]; ok {
		return name
	}
	return fmt.Sprintf("UnknownState(%d)", state)
}

func latencyZoneString(zone LatencyZone) string {
	if name, ok := latencyZoneNames[zone]; ok {
		return name
	}
	return fmt.Sprintf("UnknownLatencyZone(%d)", zone)
}

const (
	defaultDecayStep              = 5
	defaultCheckIntervalMs        = 1000
	defaultBizReadAwaitDegradeMs  = 500
	defaultBizWriteAwaitDegradeMs = 500
	defaultSafetyBoundaryRatio    = 0.8
	defaultTriggerConsecutive     = 3
	defaultRelaxDisableFactor     = 1.5
	defaultMetricsWindows         = 10
	defaultMetricsWindowMs        = 1000
	defaultSampleIntervalMs       = 10
	defaultTrendProjectionWindows = 3
)

type ManagerStatus struct {
	LimiterStatus map[string]LimiterStatus
	LatZone       string
	State         string
}

type AdaptiveManager struct {
	mu                 sync.RWMutex
	diskPath           string
	conf               AdaptiveManagerConf
	limiters           map[IoType]*Limiter
	metrics            *metricsCollector
	throttleEnabled    atomic.Value
	throttledTypes     map[IoType]bool
	manualLimitEnabled map[IoType]bool
	manualIopsLimit    map[IoType]int
	state              atomic.Value
	latZone            atomic.Value
	stopCh             chan struct{}
	wg                 sync.WaitGroup
}

type FlowConfig struct {
	Iocc         int
	IopsMinLimit int
}

type AdaptiveManagerConf struct {
	FlowConfigs                map[IoType]FlowConfig
	DecayStep                  int
	CheckIntervalMs            int64
	BizReadAwaitDegradeMs      int64
	BizWriteAwaitDegradeMs     int64
	SafetyBoundaryRatio        float64
	SafetyBoundaryReadAwaitMs  int64
	SafetyBoundaryWriteAwaitMs int64
	TriggerConsecutive         int
	RelaxDisableFactor         float64
	MetricsWindows             int
	MetricsWindowMs            int64
	SampleIntervalMs           int64
	TrendProjectionWindows     int
}

func (am *AdaptiveManager) loadState() ManagerState {
	if v := am.state.Load(); v != nil {
		if state, ok := v.(ManagerState); ok {
			return state
		}
	}
	return StateIdle
}

func (am *AdaptiveManager) loadLatZone() LatencyZone {
	if v := am.latZone.Load(); v != nil {
		if zone, ok := v.(LatencyZone); ok {
			return zone
		}
	}
	return LatRelaxZone
}

func (am *AdaptiveManager) isThrottleEnabled() bool {
	if v := am.throttleEnabled.Load(); v != nil {
		if enabled, ok := v.(bool); ok {
			return enabled
		}
	}
	return false
}

func (am *AdaptiveManager) setThrottleEnabled(enabled bool) {
	am.throttleEnabled.Store(enabled)
}

func (am *AdaptiveManager) validConf() {
	if am.conf.DecayStep <= 0 {
		am.conf.DecayStep = defaultDecayStep
	}
	if am.conf.CheckIntervalMs <= 0 {
		am.conf.CheckIntervalMs = defaultCheckIntervalMs
	}
	if am.conf.BizReadAwaitDegradeMs <= 0 {
		am.conf.BizReadAwaitDegradeMs = defaultBizReadAwaitDegradeMs
	}
	if am.conf.BizWriteAwaitDegradeMs <= 0 {
		am.conf.BizWriteAwaitDegradeMs = defaultBizWriteAwaitDegradeMs
	}
	if am.conf.SafetyBoundaryRatio <= 0.0 || am.conf.SafetyBoundaryRatio >= 1.0 {
		am.conf.SafetyBoundaryRatio = defaultSafetyBoundaryRatio
	}
	am.conf.SafetyBoundaryReadAwaitMs = int64(float64(am.conf.BizReadAwaitDegradeMs) * am.conf.SafetyBoundaryRatio)
	am.conf.SafetyBoundaryWriteAwaitMs = int64(float64(am.conf.BizWriteAwaitDegradeMs) * am.conf.SafetyBoundaryRatio)
	if am.conf.TriggerConsecutive <= 0 {
		am.conf.TriggerConsecutive = defaultTriggerConsecutive
	}
	if am.conf.RelaxDisableFactor <= 0 {
		am.conf.RelaxDisableFactor = defaultRelaxDisableFactor
	}
	if am.conf.MetricsWindows <= 0 {
		am.conf.MetricsWindows = defaultMetricsWindows
	}
	if am.conf.MetricsWindowMs <= 0 {
		am.conf.MetricsWindowMs = defaultMetricsWindowMs
	}
	if am.conf.SampleIntervalMs <= 0 {
		am.conf.SampleIntervalMs = defaultSampleIntervalMs
	}
	if am.conf.TrendProjectionWindows <= 0 {
		am.conf.TrendProjectionWindows = defaultTrendProjectionWindows
	}
}

func NewAdaptiveManager(diskPath string, conf AdaptiveManagerConf) *AdaptiveManager {
	am := &AdaptiveManager{
		diskPath:           diskPath,
		limiters:           make(map[IoType]*Limiter),
		throttledTypes:     make(map[IoType]bool),
		manualLimitEnabled: make(map[IoType]bool),
		manualIopsLimit:    make(map[IoType]int),
		stopCh:             make(chan struct{}),
	}

	am.conf = conf
	am.validConf()
	am.state.Store(StateIdle)
	am.latZone.Store(LatRelaxZone)
	am.throttleEnabled.Store(false)
	for ioType, cfg := range conf.FlowConfigs {
		limiter := NewLimiter(0, cfg.Iocc)
		am.limiters[ioType] = limiter
		am.throttledTypes[ioType] = false
		am.manualLimitEnabled[ioType] = false
		am.manualIopsLimit[ioType] = 0
	}
	windowsNum := am.conf.MetricsWindows
	winSize := time.Duration(am.conf.MetricsWindowMs) * time.Millisecond
	sampleInterval := time.Duration(am.conf.SampleIntervalMs) * time.Millisecond
	am.metrics = newMetricsCollector(windowsNum, winSize, sampleInterval, IoTypes)
	am.metrics.am = am

	am.wg.Add(1)
	go func() {
		defer am.wg.Done()
		am.loop()
	}()

	return am
}

func (am *AdaptiveManager) Close() {
	close(am.stopCh)
	am.wg.Wait()
	if am.metrics != nil {
		am.metrics.close()
	}
	am.mu.Lock()
	for _, l := range am.limiters {
		l.Close()
	}
	am.mu.Unlock()
}

func (am *AdaptiveManager) loop() {
	interval := time.Duration(am.conf.CheckIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Duration(defaultCheckIntervalMs) * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-am.stopCh:
			return
		case <-ticker.C:
			am.evaluateAndAdjust()
			am.mu.RLock()
			newInterval := time.Duration(am.conf.CheckIntervalMs) * time.Millisecond
			am.mu.RUnlock()
			if newInterval <= 0 {
				newInterval = time.Duration(defaultCheckIntervalMs) * time.Millisecond
			}
			if newInterval != interval {
				ticker.Reset(newInterval)
				interval = newInterval
			}
		}
	}
}

func (am *AdaptiveManager) UpdateControlParams(params interface{}) {
	if am == nil {
		return
	}

	cfg, ok := params.(struct {
		DecayStep              int
		CheckIntervalMs        int64
		BizReadAwaitDegradeMs  int64
		BizWriteAwaitDegradeMs int64
		SafetyBoundaryRatio    float64
		TriggerConsecutive     int
		RelaxDisableFactor     float64
		MetricsWindows         int
		MetricsWindowMs        int64
		SampleIntervalMs       int64
	})
	if !ok {
		return
	}

	am.mu.Lock()
	prevWindows := am.conf.MetricsWindows
	prevWindowMs := am.conf.MetricsWindowMs
	prevSampleMs := am.conf.SampleIntervalMs

	am.conf.DecayStep = cfg.DecayStep
	am.conf.CheckIntervalMs = cfg.CheckIntervalMs
	am.conf.BizReadAwaitDegradeMs = cfg.BizReadAwaitDegradeMs
	am.conf.BizWriteAwaitDegradeMs = cfg.BizWriteAwaitDegradeMs
	am.conf.SafetyBoundaryRatio = cfg.SafetyBoundaryRatio
	am.conf.TriggerConsecutive = cfg.TriggerConsecutive
	am.conf.RelaxDisableFactor = cfg.RelaxDisableFactor
	am.conf.MetricsWindows = cfg.MetricsWindows
	am.conf.MetricsWindowMs = cfg.MetricsWindowMs
	am.conf.SampleIntervalMs = cfg.SampleIntervalMs

	am.validConf()

	newWindows := am.conf.MetricsWindows
	newWindowMs := am.conf.MetricsWindowMs
	newSampleMs := am.conf.SampleIntervalMs
	am.mu.Unlock()

	if am.metrics != nil && (newWindows != prevWindows || newWindowMs != prevWindowMs || newSampleMs != prevSampleMs) {
		am.metrics.reconfigure(newWindows, time.Duration(newWindowMs)*time.Millisecond, time.Duration(newSampleMs)*time.Millisecond)
	}
}

func (am *AdaptiveManager) evaluateAndAdjust() {
	latZone := am.getBusinessLatencyZone()
	am.latZone.Store(latZone)
	am.applyStateTransition(latZone)
}

func (am *AdaptiveManager) applyStateTransition(latZone LatencyZone) {
	current := am.loadState()

	switch current {
	case StateIdle:
		switch latZone {
		case LatDangerZone:
			// Start throttling
			am.state.Store(StateThrottling)
			if am.canEscalate() {
				am.escalateOne()
			}
		case LatRelaxZone, LatSafetyZone:
			// remain idle
		}
	case StateThrottling:
		switch latZone {
		case LatDangerZone:
			if am.canEscalate() {
				am.escalateOne()
			}
		case LatRelaxZone:
			// Move to recovering and begin relaxing
			am.state.Store(StateRecovering)
			if am.canRelax() {
				am.relaxOne()
			}
		case LatSafetyZone:
			// maintain current throttling
		}
	case StateRecovering:
		switch latZone {
		case LatRelaxZone:
			if am.canRelax() {
				am.relaxOne()
			}
			// if throttling fully disabled, return to idle
			if !am.isThrottleEnabled() {
				am.state.Store(StateIdle)
			}
		case LatDangerZone:
			// bounce back to throttling
			am.state.Store(StateThrottling)
			if am.canEscalate() {
				am.escalateOne()
			}
		case LatSafetyZone:
			// maintain gradual recovery; do not change state without relax signal
		}
	}
}

func (am *AdaptiveManager) GetLatestWindowStat(ioType IoType) (lastWs WindowStat) {
	if am.metrics == nil {
		return
	}
	windows := am.MetricsWindowsStat(ioType)
	if len(windows) > 0 {
		lastWs = windows[len(windows)-1]
	}
	return lastWs
}

func (am *AdaptiveManager) getLatestLatencyMs(ioType IoType) int64 {
	lastWs := am.GetLatestWindowStat(ioType)
	latNs := lastWs.Await
	if latNs <= 0 {
		return 0
	}
	return latNs / int64(time.Millisecond)
}

func (am *AdaptiveManager) scaleStepForEscalate(base int) int {
	rdLatMs := am.getLatestLatencyMs(Read)
	wRLatMs := am.getLatestLatencyMs(Write)
	latMs := rdLatMs
	safe := am.conf.SafetyBoundaryReadAwaitMs
	maxTol := am.conf.BizReadAwaitDegradeMs
	if rdLatMs < wRLatMs {
		latMs = wRLatMs
		safe = am.conf.SafetyBoundaryWriteAwaitMs
		maxTol = am.conf.BizWriteAwaitDegradeMs
	}

	// If we are between safety and max tolerance, shrink steps linearly from
	// base*0 at safety to base*1.0 at max tolerance.
	if latMs > safe && latMs < maxTol {
		den := float64(maxTol - safe)
		scale := float64(latMs-safe) / den // 0..1
		step := int(math.Ceil(float64(base) * scale))
		if step < 1 {
			step = 1
		}
		return step
	}
	// If we are above max tolerance, grow steps with overage up to 2x base.
	if latMs >= maxTol {
		overage := float64(latMs-maxTol) / float64(maxTol)
		if overage < 0 {
			overage = 0
		}
		scale := 1.0 + math.Min(1.0, overage) // 1..2
		step := int(math.Ceil(float64(base) * scale))
		if step < 1 {
			step = 1
		}
		return step
	}
	// At or below safety, should not escalate; return minimal step to be safe
	return 1
}

func (am *AdaptiveManager) scaleStepForRelax(base int) int {
	rdLatMs := am.getLatestLatencyMs(Read)
	wrLatMs := am.getLatestLatencyMs(Write)

	latMs := rdLatMs
	safe := am.conf.SafetyBoundaryReadAwaitMs
	if rdLatMs < wrLatMs {
		latMs = wrLatMs
		safe = am.conf.SafetyBoundaryWriteAwaitMs
	}

	if latMs >= safe {
		// Close to or above safety boundary, relax very carefully
		return 1
	}
	// Below safety: scale from 0.5x at boundary to 2x when latency near 0
	r := 1.0 - (float64(latMs) / float64(safe)) // 0 at boundary .. 1 deep healthy
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}
	scale := 2 * r // 0..2.0
	step := int(math.Ceil(float64(base) * scale))
	if step < 1 {
		step = 1
	}
	return step
}

func (am *AdaptiveManager) canEscalate() bool {
	// Trend-aware gating: if latency is trending down and likely to reach
	// safety boundary soon, postpone escalation to avoid over-throttling.
	rd := am.MetricsWindowsStat(Read)
	rd = rd[len(rd)-am.conf.TrendProjectionWindows:]
	wr := am.MetricsWindowsStat(Write)
	wr = wr[len(rd)-am.conf.TrendProjectionWindows:]

	const threshold = 0.20
	latDecrease := func(prevWs WindowStat, currWs WindowStat) bool {
		prev := prevWs.Await
		curr := currWs.Await
		if prev <= 0 || curr < 0 {
			return false
		}
		if curr >= prev { // not decreasing
			return false
		}
		rel := float64(prev-curr) / float64(prev)
		return rel > threshold // not more than threshold
	}

	windowLatIncrease := func(idx int) bool {
		var prevWsRD, prevWsWR, currWsRD, currWsWR WindowStat
		if idx >= 1 && idx < len(rd) {
			prevWsRD = rd[idx]
			currWsRD = rd[idx-1]
		}
		if idx >= 1 && idx < len(wr) {
			prevWsWR = wr[idx]
			currWsWR = wr[idx-1]
		}
		return latDecrease(prevWsRD, currWsRD) || latDecrease(prevWsWR, currWsWR)
	}

	windowsCount := 0
	for i := am.conf.TrendProjectionWindows - 1; i >= 1; i-- {
		if windowLatIncrease(i) {
			windowsCount++
		} else {
			break
		}
	}
	if windowsCount < am.conf.TrendProjectionWindows {
		return true
	} else {
		return false
	}
}

func (am *AdaptiveManager) canRelax() bool {
	// Trend-aware gating: if latency is trending up and may cross safety
	// boundary soon, postpone relaxation to avoid oscillation or overshoot.
	rd := am.MetricsWindowsStat(Read)
	rd = rd[len(rd)-am.conf.TrendProjectionWindows:]
	wr := am.MetricsWindowsStat(Write)
	wr = wr[len(rd)-am.conf.TrendProjectionWindows:]

	const threshold = 0.20
	latIncrease := func(prevWs WindowStat, currWs WindowStat) bool {
		prev := prevWs.Await
		curr := currWs.Await
		if prev <= 0 || curr < 0 {
			return false
		}
		if curr <= prev { // not increasing
			return false
		}
		rel := float64(curr-prev) / float64(prev)
		return rel > threshold // not more than threshold
	}

	windowLatIncrease := func(idx int) bool {
		var prevWsRD, prevWsWR, currWsRD, currWsWR WindowStat
		if idx >= 1 && idx < len(rd) {
			prevWsRD = rd[idx]
			currWsRD = rd[idx-1]
		}
		if idx >= 1 && idx < len(wr) {
			prevWsWR = wr[idx]
			currWsWR = wr[idx-1]
		}
		return latIncrease(prevWsRD, currWsRD) || latIncrease(prevWsWR, currWsWR)
	}

	windowsCount := 0
	for i := am.conf.TrendProjectionWindows - 1; i >= 1; i-- {
		if windowLatIncrease(i) {
			windowsCount++
		} else {
			break
		}
	}
	if windowsCount < am.conf.TrendProjectionWindows {
		return true
	} else {
		return false
	}
}

func (am *AdaptiveManager) MetricsWindowsStat(ioType IoType) []WindowStat {
	if am.metrics == nil {
		return nil
	}
	return am.metrics.metricsWindowsStat(ioType)
}

func (am *AdaptiveManager) GetLimiterByType(ioType IoType) *Limiter {
	return am.limiters[ioType]
}

func (am *AdaptiveManager) UpdateIopsMinLimitByType(ioType IoType, value int) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if cfg, ok := am.conf.FlowConfigs[ioType]; ok {
		cfg.IopsMinLimit = value
		am.conf.FlowConfigs[ioType] = cfg
	}
}

func (am *AdaptiveManager) UpdateIOByType(ioType IoType, iocc int, factor int) {
	am.mu.Lock()
	if cfg, ok := am.conf.FlowConfigs[ioType]; ok {
		cfg.Iocc = iocc
		am.conf.FlowConfigs[ioType] = cfg
	}
	am.mu.Unlock()

	l := am.GetLimiterByType(ioType)
	if l != nil {
		l.ResetIO(iocc, factor)
	}
}

func (am *AdaptiveManager) SetManualIopsLimit(ioType IoType, limit int) {
	am.mu.Lock()
	defer am.mu.Unlock()
	lim := am.limiters[ioType]
	if lim == nil {
		return
	}
	if limit > 0 {
		am.manualLimitEnabled[ioType] = true
		am.manualIopsLimit[ioType] = limit
		lim.ResetLimit(limit)
		lim.Enable()
	} else {
		am.manualLimitEnabled[ioType] = false
		am.manualIopsLimit[ioType] = 0
		lim.ResetLimit(0)
		lim.Disable()
	}
}

func (am *AdaptiveManager) DisableManualIopsLimit(ioType IoType) {
	am.SetManualIopsLimit(ioType, 0)
}

func (am *AdaptiveManager) getStateString() string {
	state := am.State()
	switch state {
	case StateIdle:
		return "StateIdle"
	case StateThrottling:
		return "StateThrottling"
	case StateRecovering:
		return "StateRecovering"
	default:
		return ""
	}
}

func (am *AdaptiveManager) getLatZoneString() string {
	latZone := am.loadLatZone()
	switch latZone {
	case LatRelaxZone:
		return "LatRelaxZone"
	case LatSafetyZone:
		return "LatSafetyZone"
	case LatDangerZone:
		return "LatDangerZone"
	default:
		return ""
	}
}

func (am *AdaptiveManager) GetManagerStatus() *ManagerStatus {
	if am.metrics == nil {
		return nil
	}
	limiterStatusMap := make(map[string]LimiterStatus)
	for i := 0; i < len(IoTypes); i++ {
		ioType := IoTypes[i]
		lim := am.limiters[ioType]
		if lim == nil {
			continue
		}
		status := lim.Status()
		limiterStatusMap[IoTypeNames[ioType]] = status
	}
	info := &ManagerStatus{LimiterStatus: limiterStatusMap, State: am.getStateString(), LatZone: am.getLatZoneString()}
	return info
}

func (am *AdaptiveManager) GetLimiterStatus(ioType IoType) *LimiterStatus {
	if am.metrics == nil {
		return nil
	}
	lim := am.limiters[ioType]
	if lim == nil {
		return nil
	}
	status := lim.Status()
	return &status
}

func (am *AdaptiveManager) getBusinessLatencyZone() LatencyZone {
	if am.metrics == nil {
		return LatSafetyZone
	}

	rd := am.MetricsWindowsStat(Read)
	wr := am.MetricsWindowsStat(Write)

	// Check if metrics exceed max tolerance (danger zone)
	exceedMaxTolerance := func(ws WindowStat, ioType IoType) bool {
		var maxTol int64
		if ioType == Read {
			maxTol = am.conf.BizReadAwaitDegradeMs
		} else {
			maxTol = am.conf.BizWriteAwaitDegradeMs
		}
		if maxTol > 0 && ws.Await >= maxTol*int64(time.Millisecond) {
			return true
		}
		return false
	}

	// Check if metrics exceed safety boundary
	exceedSafetyBoundary := func(ws WindowStat, ioType IoType) bool {
		var safe int64
		if ioType == Read {
			safe = am.conf.SafetyBoundaryReadAwaitMs
		} else {
			safe = am.conf.SafetyBoundaryWriteAwaitMs
		}
		if safe > 0 && ws.Await >= safe*int64(time.Millisecond) {
			return true
		}
		return false
	}

	windowExceedMaxTolerance := func(idx int) bool {
		var wsRD, wsWR WindowStat
		if idx >= 0 && idx < len(rd) {
			wsRD = rd[idx]
		}
		if idx >= 0 && idx < len(wr) {
			wsWR = wr[idx]
		}
		return exceedMaxTolerance(wsRD, Read) || exceedMaxTolerance(wsWR, Write)
	}

	windowExceedSafetyBoundary := func(idx int) bool {
		var wsRD, wsWR WindowStat
		if idx >= 0 && idx < len(rd) {
			wsRD = rd[idx]
		}
		if idx >= 0 && idx < len(wr) {
			wsWR = wr[idx]
		}
		return exceedSafetyBoundary(wsRD, Read) || exceedSafetyBoundary(wsWR, Write)
	}

	// Count consecutive windows exceeding max tolerance
	dangerWindowsCount := 0
	for i := am.conf.MetricsWindows - 1; i >= 0; i-- {
		if windowExceedMaxTolerance(i) {
			dangerWindowsCount++
		} else {
			break
		}
	}

	// Count consecutive windows exceeding safety boundary but not max tolerance
	safetyWindowsCount := 0
	for i := am.conf.MetricsWindows - 1; i >= 0; i-- {
		if windowExceedSafetyBoundary(i) && !windowExceedMaxTolerance(i) {
			safetyWindowsCount++
		} else {
			break
		}
	}

	// Count consecutive healthy windows (below relax boundary with hysteresis)
	relaxWindowsCount := 0
	for i := am.conf.MetricsWindows - 1; i >= 0; i-- {
		if !windowExceedSafetyBoundary(i) {
			relaxWindowsCount++
		} else {
			break
		}
	}

	// Determine QoS state with hysteresis
	if dangerWindowsCount >= am.conf.TriggerConsecutive {
		return LatDangerZone
	} else if relaxWindowsCount >= am.conf.TriggerConsecutive {
		return LatRelaxZone
	} else {
		// In safety zone - maintain current state
		return LatSafetyZone
	}
}

// State returns the current ManagerState of the AdaptiveManager
func (am *AdaptiveManager) State() ManagerState {
	return am.loadState()
}

func (am *AdaptiveManager) LatZone() LatencyZone {
	return am.loadLatZone()
}

func (am *AdaptiveManager) getAllTypeActualIops() map[IoType]int64 {
	if am.metrics == nil {
		return nil
	}
	return am.metrics.getAllTypeActualIops()
}

func (am *AdaptiveManager) escalateOne() {
	latZone := am.LatZone()
	state := am.State()

	am.mu.Lock()
	defer am.mu.Unlock()
	actualIopsMap := am.getAllTypeActualIops()
	for i := len(IoTypes) - 1; i >= 0; i-- {
		ioType := IoTypes[i]
		cfg, ok := am.conf.FlowConfigs[ioType]
		// Skip if manual override is active for this type
		if !ok || am.manualLimitEnabled[ioType] {
			continue
		}

		lim := am.limiters[ioType]
		if lim == nil {
			continue
		}
		currLimit := lim.limit
		minLimit := cfg.IopsMinLimit

		if currLimit == minLimit && am.throttledTypes[ioType] {
			continue
		}

		if !am.isThrottleEnabled() {
			am.setThrottleEnabled(true)
		}
		if !am.throttledTypes[ioType] {
			lim.Enable()
			am.throttledTypes[ioType] = true
		}

		actualIops := actualIopsMap[ioType]
		step := am.scaleStepForEscalate(am.conf.DecayStep)

		var next int
		if currLimit <= 0 {
			if actualIops > 0 {
				next = int(actualIops)
				if next < minLimit {
					next = minLimit
				}
			} else {
				next = minLimit
			}
			lim.ResetLimit(next)
			log.LogInfof("action=%s disk=%s state=%s latZone=%s ioType=%s actual=%d curr=%d next=%d step=%d",
				auditOpAdaptiveEscalate, am.diskPath, managerStateString(state), latencyZoneString(latZone), IoTypeNames[ioType], actualIops, currLimit, next, step,
			)
			return
		}
		if currLimit == minLimit {
			continue
		}

		next = currLimit - step
		if next < minLimit {
			next = minLimit
		}
		lim.ResetLimit(next)
		log.LogInfof("action=%s disk=%s state=%s latZone=%s ioType=%s actual=%d curr=%d next=%d step=%d",
			auditOpAdaptiveEscalate, am.diskPath, managerStateString(state), latencyZoneString(latZone), IoTypeNames[ioType], actualIops, currLimit, next, step,
		)
		return
	}
}

func (am *AdaptiveManager) relaxOne() {
	latZone := am.LatZone()
	state := am.State()

	am.mu.Lock()
	defer am.mu.Unlock()
	actualIopsMap := am.getAllTypeActualIops()
	for _, ioType := range IoTypes {
		lim := am.limiters[ioType]
		// Skip if manual override is active for this type
		if lim == nil || am.manualLimitEnabled[ioType] {
			continue
		}
		if !am.throttledTypes[ioType] {
			continue
		}

		currLimit := lim.limit
		step := am.scaleStepForRelax(am.conf.DecayStep)
		actualIops := int(actualIopsMap[ioType])
		factor := am.conf.RelaxDisableFactor
		if currLimit >= int(float64(actualIops)*factor) {
			lim.ResetLimit(0)
			lim.Disable()
			am.throttledTypes[ioType] = false
			allUntroubled := true
			for _, isThrottled := range am.throttledTypes {
				if isThrottled {
					allUntroubled = false
					break
				}
			}
			if allUntroubled {
				am.setThrottleEnabled(false)
			}
			log.LogInfof("action=%s disk=%s state=%s latZone=%s ioType=%s actual=%d curr=%d next=%d step=%d",
				auditOpAdaptiveRelax, am.diskPath, managerStateString(state), latencyZoneString(latZone), IoTypeNames[ioType], actualIops, currLimit, 0, step,
			)
			return
		}

		next := currLimit + step
		lim.ResetLimit(next)
		log.LogInfof("action=%s disk=%s state=%s latZone=%s ioType=%s actual=%d curr=%d next=%d step=%d",
			auditOpAdaptiveRelax, am.diskPath, managerStateString(state), latencyZoneString(latZone), IoTypeNames[ioType], actualIops, currLimit, next, step,
		)
		return
	}
}

func (am *AdaptiveManager) Run(ioType IoType, size int, allowHang bool, fn func()) (err error) {
	am.mu.RLock()
	limiter := am.limiters[ioType]
	am.mu.RUnlock()
	if limiter == nil {
		if am.metrics != nil {
			start := time.Now()
			am.metrics.incRunning(ioType)
			defer func() {
				am.metrics.decRunning(ioType)
				am.metrics.addOp(ioType, size, time.Since(start), false)
			}()
		}
		fn()
		return nil
	}

	if am.metrics != nil {
		start := time.Now()
		am.metrics.incWaiting(ioType)
		defer func() {
			if err != nil {
				am.metrics.decWaiting(ioType)
			}
			am.metrics.addOp(ioType, size, time.Since(start), err != nil)
		}()
	}
	err = limiter.Run(size, allowHang, func() {
		if am.metrics != nil {
			am.metrics.decWaiting(ioType)
			am.metrics.incRunning(ioType)
			defer am.metrics.decRunning(ioType)
		}
		fn()
	})
	return err
}

func (am *AdaptiveManager) TryRun(ioType IoType, size int, fn func()) (ok bool) {
	am.mu.RLock()
	limiter := am.limiters[ioType]
	am.mu.RUnlock()
	if limiter == nil {
		if am.metrics != nil {
			start := time.Now()
			am.metrics.incRunning(ioType)
			defer func() {
				am.metrics.decRunning(ioType)
				am.metrics.addOp(ioType, size, time.Since(start), false)
			}()
		}
		fn()
		return true
	}

	if am.metrics != nil {
		start := time.Now()
		am.metrics.incWaiting(ioType)
		defer func() {
			if !ok {
				am.metrics.decWaiting(ioType)
				am.metrics.addReject(ioType)
			} else {
				am.metrics.addOp(ioType, size, time.Since(start), false)
			}
		}()
	}
	ok = limiter.TryRun(size, func() {
		if am.metrics != nil {
			am.metrics.decWaiting(ioType)
			am.metrics.incRunning(ioType)
			defer am.metrics.decRunning(ioType)
		}
		fn()
	})
	return ok
}
