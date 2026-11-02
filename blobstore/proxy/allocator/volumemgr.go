// Copyright 2022 The CubeFS Authors.
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

package allocator

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	defaultAllocVolsNum        = 4
	defaultTotalThresholdRatio = 0.1
	defaultRetainVolumeNum     = 400

	defaultRetainIntervalS      = int64(40)
	defaultMetricIntervalS      = 60
	defaultRetainBatchIntervalS = int64(1)

	defaultTotalVolNumThresholdRatio = 0.1
)

type CodeModeVolConfig struct {
	DefaultAllocVolsNum       int     `json:"default_alloc_vols_num"`
	TotalThresholdRatio       float64 `json:"total_threshold_ratio"`
	TotalVolNumThresholdRatio float64 `json:"total_vol_num_threshold_ratio"`
}

type VolConfig struct {
	ClusterID             proto.ClusterID `json:"cluster_id"`
	Idc                   string          `json:"idc"`
	Host                  string          `json:"host"`
	RetainIntervalS       int64           `json:"retain_interval_s"`
	MetricReportIntervalS int             `json:"metric_report_interval_s"`
	RetainVolumeBatchNum  int             `json:"retain_volume_batch_num"`
	RetainBatchIntervalS  int64           `json:"retain_batch_interval_s"`
	VolumeReserveSize     int             `json:"-"`

	CodeModeVolConfig                                         // use this config if code_mode_configs is not set
	CodeModeConfigs   map[codemode.CodeMode]CodeModeVolConfig `json:"code_mode_configs"`
}

//======================modeInfo======================================

type modeInfo struct {
	current              *volumes
	backup               *volumes
	totalThreshold       uint64
	totalVolNumThreshold uint64

	lock sync.RWMutex
}

func (m *modeInfo) List(isBackUp bool) []*volume {
	var res []*volume
	m.lock.RLock()
	if isBackUp {
		res = m.backup.List()
		m.lock.RUnlock()
		return res
	}
	res = m.current.List()
	m.lock.RUnlock()
	return res
}

func (m *modeInfo) ListAll() (res []*volume) {
	m.lock.RLock()
	c := m.current.List()
	b := m.backup.List()
	res = make([]*volume, len(c)+len(b))
	copy(res, c)
	copy(res[len(c):], b)
	m.lock.RUnlock()
	return res
}

func (m *modeInfo) VolumeNum() int {
	m.lock.RLock()
	num := m.backup.Len() + m.current.Len()
	m.lock.RUnlock()
	return num
}

func (m *modeInfo) Delete(vid proto.Vid) {
	m.lock.RLock()
	if !m.current.Delete(vid) {
		m.backup.Delete(vid)
	}
	m.lock.RUnlock()
}

func (m *modeInfo) Put(vol *volume, isBackUp bool) {
	m.lock.RLock()
	if isBackUp {
		m.backup.Put(vol)
		m.lock.RUnlock()
		return
	}
	m.current.Put(vol)
	m.lock.RUnlock()
}

func (m *modeInfo) Get(vid proto.Vid, isBackup bool) (res *volume, ok bool) {
	m.lock.RLock()
	if isBackup {
		res, ok = m.backup.Get(vid)
		m.lock.RUnlock()
		return
	}
	res, ok = m.current.Get(vid)
	m.lock.RUnlock()
	return
}

func (m *modeInfo) GetAny(vid proto.Vid) (res *volume, ok bool) {
	m.lock.RLock()
	if res, ok = m.current.Get(vid); ok {
		m.lock.RUnlock()
		return
	}
	res, ok = m.backup.Get(vid)
	m.lock.RUnlock()
	return
}

func (m *modeInfo) TotalFree() int64 {
	m.lock.RLock()
	totalFree := m.backup.TotalFree() + m.current.TotalFree()
	m.lock.RUnlock()
	return totalFree
}

func (m *modeInfo) needSwitchToBackup(fSize int64) (bool, error) {
	m.lock.RLock()
	totalFree := m.current.UpdateTotalFree(-fSize)
	if totalFree <= int64(m.totalThreshold) || int64(m.current.Len())-1 < int64(m.totalVolNumThreshold) {
		m.current.UpdateTotalFree(fSize)
		if len(m.backup.List()) == 0 { // allocating from clusterMgr, can not switch to backup
			m.lock.RUnlock()
			return false, errcode.ErrNoCodemodeVolume
		}
		m.lock.RUnlock()
		return true, nil
	}
	m.lock.RUnlock()
	return false, nil
}

func (m *modeInfo) getAvailableList(fsize int64, switchable bool) (vols []*volume) {
	if !switchable {
		return m.List(false)
	}
	m.lock.Lock()
	totalFree := m.current.TotalFree()
	if totalFree < int64(m.totalThreshold) ||
		totalFree < fsize ||
		int64(m.current.Len())-1 < int64(m.totalVolNumThreshold) {
		// switch current and backup
		tmp := m.current
		m.current = m.backup
		m.backup = tmp
		log.Infof("switch current and backup, current total free: %d, total num: %d", totalFree, m.current.Len())
	}
	if m.current.TotalFree() < fsize {
		m.lock.Unlock()
		return nil
	}
	m.current.UpdateTotalFree(-fsize)
	vols = m.current.List()
	m.lock.Unlock()
	return vols
}

func (m *modeInfo) UpdateTotalFree(isBackup bool, free int64) {
	m.lock.RLock()
	if isBackup {
		m.backup.UpdateTotalFree(free)
		m.lock.RUnlock()
		return
	}
	m.current.UpdateTotalFree(free)
	m.lock.RUnlock()
}

func (m *modeInfo) dealDisCards(discards []proto.Vid) {
	if len(discards) == 0 {
		return
	}
	m.lock.RLock()
	for _, vid := range discards {
		vol, ok := m.current.Get(vid)
		if ok {
			vol.mu.Lock()
			if vol.deleted {
				vol.mu.Unlock()
				continue
			}
			vol.deleted = true
			vol.mu.Unlock()
			m.current.Delete(vid)
		}
	}
	m.lock.RUnlock()
}

type allocArgs struct {
	isInit   bool
	isBackup bool
	codeMode codemode.CodeMode
	count    int
}

type modeSlot struct {
	info *modeInfo
	ch   chan allocArgs
}

type volumeMgr struct {
	BlobConfig
	VolConfig

	BidMgr

	clusterMgr clustermgr.APIProxy
	codemodeMu sync.Mutex // serializes codemode policy reload
	modes      sync.Map   // CodeMode -> *modeSlot
	preIdx     uint64
	closeCh    chan struct{}
}

// rangeSlots iterates all code mode slots, fn returns false to stop the iteration.
// It reports whether the iteration was stopped by fn.
func (v *volumeMgr) rangeSlots(fn func(codemode.CodeMode, *modeSlot) bool) (stopped bool) {
	v.modes.Range(func(key, val any) bool {
		if !fn(key.(codemode.CodeMode), val.(*modeSlot)) {
			stopped = true
			return false
		}
		return true
	})
	return
}

func volConfCheck(cfg *VolConfig) {
	defaulter.LessOrEqual(&cfg.DefaultAllocVolsNum, defaultAllocVolsNum)
	if cfg.CodeModeConfigs == nil {
		cfg.CodeModeConfigs = make(map[codemode.CodeMode]CodeModeVolConfig)
	}
	// check each code mode config
	for codeMode, modeCfg := range cfg.CodeModeConfigs {
		defaulter.LessOrEqual(&modeCfg.DefaultAllocVolsNum, defaultAllocVolsNum)
		defaulter.LessOrEqual(&modeCfg.TotalThresholdRatio, defaultTotalThresholdRatio)
		defaulter.LessOrEqual(&modeCfg.TotalVolNumThresholdRatio, defaultTotalVolNumThresholdRatio)
		if modeCfg.TotalThresholdRatio >= 0.2 {
			log.Fatalf("TotalThresholdRatio for codeMode %v must less than 0.2, current: %v", codeMode, modeCfg.TotalThresholdRatio)
		}
		if modeCfg.TotalVolNumThresholdRatio >= 1 {
			log.Fatalf("TotalVolNumThresholdRatio for codeMode %v must less than 1, current: %v", codeMode, modeCfg.TotalVolNumThresholdRatio)
		}
		cfg.CodeModeConfigs[codeMode] = modeCfg
	}
	defaulter.LessOrEqual(&cfg.RetainIntervalS, defaultRetainIntervalS)
	defaulter.LessOrEqual(&cfg.TotalThresholdRatio, defaultTotalThresholdRatio)
	defaulter.LessOrEqual(&cfg.MetricReportIntervalS, defaultMetricIntervalS)
	defaulter.LessOrEqual(&cfg.RetainVolumeBatchNum, defaultRetainVolumeNum)
	defaulter.LessOrEqual(&cfg.RetainBatchIntervalS, defaultRetainBatchIntervalS)
	defaulter.LessOrEqual(&cfg.TotalVolNumThresholdRatio, defaultTotalVolNumThresholdRatio)

	if cfg.TotalThresholdRatio >= 0.2 {
		log.Fatalf("TotalThresholdRatio must less than 0.2, current: %v", cfg.TotalThresholdRatio)
	}
	if cfg.TotalVolNumThresholdRatio >= 1 {
		log.Fatalf("TotalVolNumThresholdRatio must less than 1, current: %v", cfg.TotalVolNumThresholdRatio)
	}
}

// getCodeModeConfig returns the CodeModeVolConfig for the given codeMode, or default values if not set
func (cfg *VolConfig) getCodeModeConfig(codeMode codemode.CodeMode) CodeModeVolConfig {
	if modeCfg, ok := cfg.CodeModeConfigs[codeMode]; ok {
		return modeCfg
	}
	return cfg.CodeModeVolConfig
}

type VolumeMgr interface {
	// Alloc the required volumes to access module
	Alloc(ctx context.Context, args *proxy.AllocVolsArgs) (allocVols []proxy.AllocRet, err error)
	// List the volumes in the allocator
	List(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []clustermgr.AllocVolumeInfo, err error)
	// Discard just used for proxy volume management, remove invalid volumes
	Discard(ctx context.Context, args *proxy.DiscardVolsArgs) error
	Close()
}

func (v *volumeMgr) Close() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "volumeMgrClose")
	close(v.closeCh)
	span.Warnf("close closeCh done")
}

func NewVolumeMgr(ctx context.Context, blobCfg BlobConfig, volCfg VolConfig, clusterMgr clustermgr.APIProxy) (VolumeMgr, error) {
	span := trace.SpanFromContextSafe(ctx)
	volConfCheck(&volCfg)
	bidMgr, err := NewBidMgr(ctx, blobCfg, clusterMgr)
	if err != nil {
		span.Fatalf("fail to new bidMgr, error:%v", err)
	}
	rand.Seed(int64(time.Now().Nanosecond()))
	v := &volumeMgr{
		clusterMgr: clusterMgr,
		BidMgr:     bidMgr,
		BlobConfig: blobCfg,
		VolConfig:  volCfg,
		closeCh:    make(chan struct{}),
	}
	atomic.StoreUint64(&v.preIdx, rand.Uint64())
	err = v.initModeInfo(ctx)
	if err != nil {
		return nil, err
	}

	go v.retainTask()
	go v.metricReportTask()

	return v, err
}

func (v *volumeMgr) initModeInfo(ctx context.Context) (err error) {
	volumeReserveSize, err := v.clusterMgr.GetConfig(ctx, proto.VolumeReserveSizeKey)
	if err != nil {
		return errors.Base(err, "Get volume_reserve_size config from clusterMgr err").Detail(err)
	}
	v.VolConfig.VolumeReserveSize, err = strconv.Atoi(volumeReserveSize)
	if err != nil {
		return errors.Base(err, "strconv.Atoi volumeReserveSize err").Detail(err)
	}
	return v.reloadCodeModePolicies(ctx, nil)
}

func (v *volumeMgr) reloadCodeModePolicies(ctx context.Context, verify func() bool) error {
	v.codemodeMu.Lock()
	defer v.codemodeMu.Unlock()

	// Requests blocking on the lock may be raced by a finished reload, skip
	// when verify() already reports the requested mode slot as ready.
	if verify != nil && verify() {
		return nil
	}
	policies, err := clustermgr.LoadPolicies(ctx, v.clusterMgr)
	if err != nil {
		return err
	}
	volumeChunkSize, err := v.clusterMgr.GetConfig(ctx, proto.VolumeChunkSizeKey)
	if err != nil {
		return errors.Base(err, "Get volume_chunk_size config from clusterMgr err").Detail(err)
	}
	volumeChunkSizeInt, err := strconv.Atoi(volumeChunkSize)
	if err != nil {
		return errors.Base(err, "strconv.Atoi volumeChunkSize err").Detail(err)
	}
	span := trace.SpanFromContextSafe(ctx)
	newModes := make([]codemode.CodeMode, 0)
	newSlots := make([]*modeSlot, 0)
	for _, codeModeConfig := range policies {
		if !codeModeConfig.Enable {
			continue
		}
		codeMode := codeModeConfig.ModeName.GetCodeMode()
		if _, ok := v.modes.Load(codeMode); ok {
			continue
		}
		tactic := codeMode.Tactic()
		modeCfg := v.getCodeModeConfig(codeMode)
		threshold := float64(modeCfg.DefaultAllocVolsNum*tactic.N*volumeChunkSizeInt) * modeCfg.TotalThresholdRatio
		volNumThreshold := math.Ceil(float64(modeCfg.DefaultAllocVolsNum) * modeCfg.TotalVolNumThresholdRatio)
		slot := &modeSlot{
			info: &modeInfo{
				current:              &volumes{},
				backup:               &volumes{},
				totalThreshold:       uint64(threshold),
				totalVolNumThreshold: uint64(volNumThreshold),
			},
			ch: make(chan allocArgs),
		}
		v.modes.Store(codeMode, slot)
		newModes = append(newModes, codeMode)
		newSlots = append(newSlots, slot)
		span.Infof("codeMode: %v, defaultAllocVolsNum: %v, threshold: %v, volNumThreshold: %v",
			codeModeConfig.ModeName, modeCfg.DefaultAllocVolsNum, threshold, volNumThreshold)
	}
	for i, mode := range newModes {
		modeCfg := v.getCodeModeConfig(mode)
		span.Infof("codeMode: %v, config: %+v", mode, modeCfg)
		slot := newSlots[i]
		go v.allocVolumeLoop(slot)
		slot.ch <- allocArgs{
			isInit:   true,
			codeMode: mode,
			count:    modeCfg.DefaultAllocVolsNum,
		}
	}
	return nil
}

func (v *volumeMgr) Alloc(ctx context.Context, args *proxy.AllocVolsArgs) (allocRets []proxy.AllocRet, err error) {
	allocBidScopes, err := v.BidMgr.Alloc(ctx, args.BidCount)
	if err != nil {
		return nil, err
	}
	vid, err := v.allocVid(ctx, args)
	if err != nil {
		return nil, err
	}
	allocRets = make([]proxy.AllocRet, 0, 128)
	for _, bidScope := range allocBidScopes {
		volRet := proxy.AllocRet{
			BidStart: bidScope.StartBid,
			BidEnd:   bidScope.EndBid,
			Vid:      vid,
		}
		allocRets = append(allocRets, volRet)
	}

	return
}

func (v *volumeMgr) Discard(ctx context.Context, args *proxy.DiscardVolsArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	val, ok := v.modes.Load(args.CodeMode)
	if ok {
		span.Debugf("discard code mode[%s], vols[%v]", args.CodeMode, args.Discards)
		val.(*modeSlot).info.dealDisCards(args.Discards)
		return nil
	}
	return errors.New("code mode not exist")
}

func (v *volumeMgr) List(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []clustermgr.AllocVolumeInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)
	val, ok := v.modes.Load(codeMode)
	if !ok {
		return nil, nil, errcode.ErrNoCodemodeVolume
	}
	modeInfo := val.(*modeSlot).info
	vids = make([]proto.Vid, 0, 128)
	volumes = make([]clustermgr.AllocVolumeInfo, 0, 128)
	vols := modeInfo.ListAll()
	for _, vol := range vols {
		vol.mu.RLock()
		vids = append(vids, vol.Vid)
		volumes = append(volumes, vol.AllocVolumeInfo)
		vol.mu.RUnlock()
	}
	span.Debugf("[list]code mode: %v, available volumes: %v,count: %v", codeMode, volumes, len(volumes))
	return
}

func (v *volumeMgr) getNextVid(ctx context.Context, vols []*volume, modeInfo *modeInfo, args *proxy.AllocVolsArgs) (proto.Vid, error) {
	curIdx := int(atomic.AddUint64(&v.preIdx, uint64(1)) % uint64(len(vols)))
	l := len(vols) + curIdx
	for i := curIdx; i < l; i++ {
		idx := i % len(vols)
		if v.modifySpace(ctx, vols[idx], modeInfo, args) {
			return vols[idx].Vid, nil
		}
	}
	return 0, errcode.ErrNoCodemodeVolume
}

func (v *volumeMgr) modifySpace(ctx context.Context, volInfo *volume, modeInfo *modeInfo, args *proxy.AllocVolsArgs) bool {
	span := trace.SpanFromContextSafe(ctx)
	for _, id := range args.Excludes {
		if volInfo.Vid == id {
			return false
		}
	}
	volInfo.mu.Lock()
	if volInfo.Free < args.Fsize || volInfo.deleted {
		span.Warnf("reselect vid: %v, free: %v, size: %v", volInfo.Vid, volInfo.Free, args.Fsize)
		volInfo.mu.Unlock()
		return false
	}
	volInfo.Free -= args.Fsize
	volInfo.Used += args.Fsize
	span.Debugf("selectVid: %v, this vid allocated Size: %v, freeSize: %v, reserve size: %v",
		volInfo.Vid, volInfo.Used, volInfo.Free, v.VolumeReserveSize)
	deleteFlag := false
	if volInfo.Free < uint64(v.VolumeReserveSize) {
		span.Infof("volume is full, remove vid:%v", volInfo.Vid)
		volInfo.deleted = true
		deleteFlag = true
	}
	volInfo.mu.Unlock()
	if deleteFlag {
		modeInfo.Delete(volInfo.Vid)
	}
	return true
}

func (v *volumeMgr) allocVid(ctx context.Context, args *proxy.AllocVolsArgs) (proto.Vid, error) {
	span := trace.SpanFromContextSafe(ctx)
	val, ok := v.modes.Load(args.CodeMode)
	if !ok {
		if err := v.reloadCodeModePolicies(ctx, func() bool {
			_, ok := v.modes.Load(args.CodeMode)
			return ok
		}); err != nil {
			span.Warnf("passive codemode policy reload failed: %v", err)
		}
		val, ok = v.modes.Load(args.CodeMode)
		if !ok {
			return 0, errcode.ErrNoCodemodeVolume
		}
	}
	info := val.(*modeSlot).info
	vols, err := v.getAvailableVols(ctx, args)
	if err != nil {
		span.Errorf("get available volumes failed, current total free: %d, err: %v", info.current.TotalFree(), err)
		return 0, err
	}
	span.Debugf("codeMode: %v, available volumes: %v", args.CodeMode, vols)
	vid, err := v.getNextVid(ctx, vols, info, args)
	if err != nil {
		span.Errorf("get next vid failed, err: %v", err)
		return 0, err
	}

	return vid, nil
}

func (v *volumeMgr) getAvailableVols(ctx context.Context, args *proxy.AllocVolsArgs) (vols []*volume, err error) {
	span := trace.SpanFromContextSafe(ctx)
	val, _ := v.modes.Load(args.CodeMode)
	info := val.(*modeSlot).info
	info.dealDisCards(args.Discards)
	modeCfg := v.getCodeModeConfig(args.CodeMode)

	needSwitch, err := info.needSwitchToBackup(int64(args.Fsize))
	if err != nil {
		v.allocNotify(ctx, args.CodeMode, modeCfg.DefaultAllocVolsNum, true)
		span.Errorf("no available volumes to alloc and current allocating from clustermgr")
		return nil, err
	}
	vols = info.getAvailableList(int64(args.Fsize), needSwitch)

	if len(vols) == 0 {
		v.allocNotify(ctx, args.CodeMode, modeCfg.DefaultAllocVolsNum, false)
		span.Errorf("no available volumes to alloc")
		return nil, errcode.ErrNoCodemodeVolume
	}

	backupLen := info.backup.Len()
	if backupLen < modeCfg.DefaultAllocVolsNum {
		v.allocNotify(ctx, args.CodeMode, modeCfg.DefaultAllocVolsNum-backupLen, true)
	}

	span.Debugf("codeMode: %v, info.currentTotalFree: %v, info.totalThreshold: %v", args.CodeMode,
		info.current.TotalFree(), info.totalThreshold)

	return vols, nil
}

// send message to apply channel, apply volume from CM
func (v *volumeMgr) allocNotify(ctx context.Context, mode codemode.CodeMode, count int, isBackup bool) {
	span := trace.SpanFromContextSafe(ctx)
	applyArg := allocArgs{
		codeMode: mode,
		count:    count,
		isBackup: isBackup,
	}
	if val, ok := v.modes.Load(mode); ok {
		slot := val.(*modeSlot)
		select {
		case slot.ch <- applyArg:
			span.Infof("allocNotify {codeMode %s count %v, backup %v} success", mode.String(), count, isBackup)
		default:
			span.Infof("the codeMode %s is allocating volume, count: %d", mode.String(), count)
		}
		return
	}
	span.Panicf("the codeMode %v not exist", mode)
}

func (v *volumeMgr) allocVolume(ctx context.Context, args *clustermgr.AllocVolumeArgs) (
	ret []clustermgr.AllocVolumeInfo, err error,
) {
	span := trace.SpanFromContextSafe(ctx)
	err = retry.ExponentialBackoff(2, 200).On(func() error {
		allocVolumes, err_ := v.clusterMgr.AllocVolume(ctx, args)
		span.Infof("alloc volume from clusterMgr: %#v, err: %v", allocVolumes, err)
		if err_ == nil && len(allocVolumes.AllocVolumeInfos) != 0 {
			ret = allocVolumes.AllocVolumeInfos
		}
		return err_
	})
	return ret, err
}

func (v *volumeMgr) allocVolumeLoop(slot *modeSlot) {
	for {
		args := <-slot.ch
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		requireCount := args.count
		if requireCount < 1 {
			continue
		}
		for {
			allocArg := &clustermgr.AllocVolumeArgs{
				IsInit:   args.isInit,
				CodeMode: args.codeMode,
				Count:    requireCount,
			}
			span.Infof("allocVolumeLoop arguments: %+v, backup: %v", *allocArg, args.isBackup)
			volumeRets, err := v.allocVolume(ctx, allocArg)
			if err != nil {
				span.Warnf("alloc volume from clustermgr failed, codeMode: %s, err: %v", args.codeMode.String(), err)
				time.Sleep(time.Duration(10) * time.Second)
				args.isInit = false
				continue
			}
			modeCfg := v.getCodeModeConfig(allocArg.CodeMode)
			for index, vol := range volumeRets {
				if allocArg.IsInit && index >= 2*modeCfg.DefaultAllocVolsNum {
					break
				}
				allocVolInfo := &volume{
					AllocVolumeInfo: vol,
				}
				// volumes more than defaultAllocVolsNum, put rest to backup
				if allocArg.IsInit && index >= modeCfg.DefaultAllocVolsNum {
					slot.info.Put(allocVolInfo, true)
					continue
				}
				slot.info.Put(allocVolInfo, args.isBackup)
			}
			if len(volumeRets) < requireCount {
				span.Warnf("clusterMgr volume num not enough, codeMode: %v, need: %v, got: %v", allocArg.CodeMode,
					requireCount, len(volumeRets))
				requireCount -= len(volumeRets)
				args.isInit = false
				continue
			}
			break
		}
	}
}
