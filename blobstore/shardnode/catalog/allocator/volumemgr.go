// Copyright 2024 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	defaultAllocVolsNum        = 4
	defaultTotalThresholdRatio = 0.1
	defaultInitVolumeNum       = 4
	defaultRetainVolumeNum     = 400

	defaultRetainIntervalS      = int64(40)
	defaultRetainBatchIntervalS = int64(1)

	defaultTotalVolNumThresholdRatio = 0.1
)

type (
	AllocVolsArgs struct {
		Fsize    uint64
		CodeMode codemode.CodeMode
		BidCount uint64
		Excludes []proto.Vid
		Discards []proto.Vid
	}
	AllocRet struct {
		BidStart proto.BlobID
		BidEnd   proto.BlobID
		Vid      proto.Vid
	}
	DiscardVolsArgs struct {
		CodeMode codemode.CodeMode
		Discards []proto.Vid
	}
)

type VolConfig struct {
	RetainIntervalS      int64   `json:"retain_interval_s"`
	DefaultAllocVolsNum  int     `json:"default_alloc_vols_num"`
	InitVolumeNum        int     `json:"init_volume_num"`
	TotalThresholdRatio  float64 `json:"total_threshold_ratio"`
	RetainVolumeBatchNum int     `json:"retain_volume_batch_num"`
	RetainBatchIntervalS int64   `json:"retain_batch_interval_s"`
	VolumeReserveSize    int     `json:"-"`

	TotalVolNumThresholdRatio float64 `json:"total_vol_num_threshold_ratio"`
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

type volumeMgr interface {
	alloc(ctx context.Context, args *AllocVolsArgs) (allocVols []AllocRet, err error)
	// Discard(ctx context.Context, args *proxy.DiscardVolsArgs) error
	listVolume(ctx context.Context, codeMode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error)
	close()
}

type modeSlot struct {
	info *modeInfo
	ch   chan allocArgs
}

type volmgr struct {
	BlobConfig
	VolConfig

	bidMgr

	tp         base.Transport
	codemodeMu sync.Mutex // serializes codemode policy reload
	modes      sync.Map   // CodeMode -> *modeSlot
	preIdx     uint64
	closeCh    chan struct{}
}

// rangeSlots iterates all code mode slots, fn returns false to stop the iteration.
// It reports whether the iteration was stopped by fn.
func (v *volmgr) rangeSlots(fn func(codemode.CodeMode, *modeSlot) bool) (stopped bool) {
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
	defaulter.Equal(&cfg.DefaultAllocVolsNum, defaultAllocVolsNum)
	defaulter.Equal(&cfg.RetainIntervalS, defaultRetainIntervalS)
	defaulter.Equal(&cfg.TotalThresholdRatio, defaultTotalThresholdRatio)
	defaulter.Equal(&cfg.InitVolumeNum, defaultInitVolumeNum)
	defaulter.Equal(&cfg.RetainVolumeBatchNum, defaultRetainVolumeNum)
	defaulter.Equal(&cfg.RetainBatchIntervalS, defaultRetainBatchIntervalS)
	defaulter.Equal(&cfg.TotalVolNumThresholdRatio, defaultTotalVolNumThresholdRatio)

	if cfg.TotalThresholdRatio >= 0.2 {
		log.Fatalf("TotalThresholdRatio must less than 0.2, current: %v", cfg.TotalThresholdRatio)
	}
	if cfg.TotalVolNumThresholdRatio >= 1 {
		log.Fatalf("TotalVolNumThresholdRatio must less than 1, current: %v", cfg.TotalVolNumThresholdRatio)
	}

	need := int(cfg.TotalThresholdRatio*float64(cfg.InitVolumeNum)) + 1
	if cfg.DefaultAllocVolsNum <= need {
		cfg.DefaultAllocVolsNum = need
	}
}

func newVolumeMgr(ctx context.Context, blobCfg BlobConfig, volCfg VolConfig, tp base.Transport) (volumeMgr, error) {
	span := trace.SpanFromContextSafe(ctx)
	volConfCheck(&volCfg)
	bm, err := newBidMgr(ctx, blobCfg, tp)
	if err != nil {
		span.Fatalf("fail to new bm, error:%v", err)
	}
	rand.Seed(int64(time.Now().Nanosecond()))
	v := &volmgr{
		tp:         tp,
		bidMgr:     bm,
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
	return v, nil
}

func (v *volmgr) initModeInfo(ctx context.Context) (err error) {
	volumeReserveSize, err := v.tp.GetConfig(ctx, proto.VolumeReserveSizeKey)
	if err != nil {
		return errors.Base(err, "Get volume_reserve_size config from clusterMgr err").Detail(err)
	}
	v.VolConfig.VolumeReserveSize, err = strconv.Atoi(volumeReserveSize)
	if err != nil {
		return errors.Base(err, "strconv.Atoi volumeReserveSize err").Detail(err)
	}
	return v.reloadCodeModePolicies(ctx, nil)
}

func (v *volmgr) reloadCodeModePolicies(ctx context.Context, verify func() bool) error {
	v.codemodeMu.Lock()
	defer v.codemodeMu.Unlock()

	// Requests blocking on the lock may be raced by a finished reload, skip
	// when verify() already reports the requested mode slot as ready.
	if verify != nil && verify() {
		return nil
	}
	policies, err := clustermgr.LoadPolicies(ctx, v.tp)
	if err != nil {
		return err
	}
	volumeChunkSize, err := v.tp.GetConfig(ctx, proto.VolumeChunkSizeKey)
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
		threshold := float64(v.InitVolumeNum*tactic.N*volumeChunkSizeInt) * v.TotalThresholdRatio
		volNumThreshold := math.Ceil(float64(v.InitVolumeNum) * v.TotalVolNumThresholdRatio)
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
		span.Infof("codeMode: %v, initVolumeNum: %v, threshold: %v", codeModeConfig.ModeName, v.InitVolumeNum, threshold)
	}
	for i, mode := range newModes {
		slot := newSlots[i]
		go v.allocVolumeLoop(slot)
		slot.ch <- allocArgs{
			isInit:   true,
			codeMode: mode,
			count:    v.InitVolumeNum,
		}
	}
	return nil
}

func (v *volmgr) alloc(ctx context.Context, args *AllocVolsArgs) (allocRets []AllocRet, err error) {
	allocBidScopes, err := v.bidMgr.alloc(ctx, args.BidCount)
	if err != nil {
		err = errors.Info(err, "bidMgr alloc failed")
		return
	}
	vid, err := v.allocVid(ctx, args)
	if err != nil {
		err = errors.Info(err, "alloc vid failed")
		return
	}
	allocRets = make([]AllocRet, 0, 128)
	for _, bidScope := range allocBidScopes {
		volRet := AllocRet{
			BidStart: bidScope.startBid,
			BidEnd:   bidScope.endBid,
			Vid:      vid,
		}
		allocRets = append(allocRets, volRet)
	}

	return
}

func (v *volmgr) listVolume(ctx context.Context, codeMode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error) {
	val, ok := v.modes.Load(codeMode)
	if !ok {
		return nil, errcode.ErrNoCodemodeVolume
	}
	modeInfo := val.(*modeSlot).info
	ret := make([]clustermgr.AllocVolumeInfo, 0, 128)
	vols := modeInfo.ListAll()
	for _, vol := range vols {
		vol.mu.RLock()
		ret = append(ret, vol.AllocVolumeInfo)
		vol.mu.RUnlock()
	}
	return ret, nil
}

func (v *volmgr) close() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "volumeMgrClose")
	close(v.closeCh)
	span.Warnf("close closeCh done")
}

func (v *volmgr) getNextVid(ctx context.Context, vols []*volume, modeInfo *modeInfo, args *AllocVolsArgs) (proto.Vid, error) {
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

func (v *volmgr) modifySpace(ctx context.Context, volInfo *volume, modeInfo *modeInfo, args *AllocVolsArgs) bool {
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

func (v *volmgr) allocVid(ctx context.Context, args *AllocVolsArgs) (proto.Vid, error) {
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

func (v *volmgr) getAvailableVols(ctx context.Context, args *AllocVolsArgs) (vols []*volume, err error) {
	span := trace.SpanFromContextSafe(ctx)
	val, _ := v.modes.Load(args.CodeMode)
	info := val.(*modeSlot).info
	info.dealDisCards(args.Discards)

	needSwitch, err := info.needSwitchToBackup(int64(args.Fsize))
	if err != nil {
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum, true)
		span.Errorf("no available volumes to alloc and current allocating from clustermgr")
		return nil, err
	}
	vols = info.getAvailableList(int64(args.Fsize), needSwitch)

	if len(vols) == 0 {
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum, false)
		span.Errorf("no available volumes to alloc")
		return nil, errcode.ErrNoCodemodeVolume
	}

	backupLen := info.backup.Len()
	if backupLen < v.DefaultAllocVolsNum {
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum-backupLen, true)
	}

	span.Debugf("codeMode: %v, info.currentTotalFree: %v, info.totalThreshold: %v", args.CodeMode,
		info.current.TotalFree(), info.totalThreshold)

	return vols, nil
}

// send message to apply channel, apply volume from CM
func (v *volmgr) allocNotify(ctx context.Context, mode codemode.CodeMode, count int, isBackup bool) {
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

func (v *volmgr) allocVolume(ctx context.Context, isInit bool, mode codemode.CodeMode, count int) (
	ret []clustermgr.AllocVolumeInfo, err error,
) {
	span := trace.SpanFromContextSafe(ctx)
	err = retry.ExponentialBackoff(2, 200).On(func() error {
		allocVolumes, err_ := v.tp.AllocVolume(ctx, isInit, mode, count)
		span.Infof("alloc volume from clusterMgr: %#v, err: %v", allocVolumes, err)
		if err_ == nil && len(allocVolumes.AllocVolumeInfos) != 0 {
			ret = allocVolumes.AllocVolumeInfos
		}
		return err_
	})
	return ret, err
}

func (v *volmgr) allocVolumeLoop(slot *modeSlot) {
	for {
		args := <-slot.ch
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		requireCount := args.count
		if requireCount < 1 {
			continue
		}
		for {
			span.Infof("allocVolumeLoop arguments: isInit:%v, codeMode:%v, count:%d, backup: %v",
				args.isInit, args.codeMode, args.count, args.isBackup)
			volumeRets, err := v.allocVolume(ctx, args.isInit, args.codeMode, requireCount)
			if err != nil {
				span.Warnf("alloc volume from clustermgr failed, codeMode: %s, err: %v", args.codeMode.String(), err)
				time.Sleep(time.Duration(10) * time.Second)
				args.isInit = false
				continue
			}
			for index, vol := range volumeRets {
				allocVolInfo := &volume{
					AllocVolumeInfo: vol,
				}
				if args.isInit && len(volumeRets) >= 2*v.InitVolumeNum && index >= v.InitVolumeNum {
					slot.info.Put(allocVolInfo, true)
					continue
				}
				slot.info.Put(allocVolInfo, args.isBackup)
			}
			if len(volumeRets) < requireCount {
				span.Warnf("clusterMgr volume num not enough, codeMode: %v, need: %v, got: %v", args.codeMode,
					requireCount, len(volumeRets))
				requireCount -= len(volumeRets)
				args.isInit = false
				continue
			}
			break
		}
	}
}
