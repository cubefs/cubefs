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
	"encoding/json"
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
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	defaultAllocVolsNum        = 1
	defaultTotalThresholdRatio = 0.2
	defaultInitVolumeNum       = 4
	defaultRetainVolumeNum     = 400

	defaultRetainIntervalS      = int64(40)
	defaultMetricIntervalS      = 60
	defaultRetainBatchIntervalS = int64(1)
)

type VolConfig struct {
	ClusterID             proto.ClusterID `json:"cluster_id"`
	Idc                   string          `json:"idc"`
	Host                  string          `json:"host"`
	RetainIntervalS       int64           `json:"retain_interval_s"`
	DefaultAllocVolsNum   int             `json:"default_alloc_vols_num"`
	InitVolumeNum         int             `json:"init_volume_num"`
	TotalThresholdRatio   float64         `json:"total_threshold_ratio"`
	MetricReportIntervalS int             `json:"metric_report_interval_s"`
	RetainVolumeBatchNum  int             `json:"retain_volume_batch_num"`
	RetainBatchIntervalS  int64           `json:"retain_batch_interval_s"`
	VolumeReserveSize     int             `json:"-"`
}

//======================modeInfo======================================

type modeInfo struct {
	current        *volumes
	backup         *volumes
	totalThreshold uint64

	sync.RWMutex
}

func (m *modeInfo) List(isBackUp bool) []*volume {
	m.RLock()
	defer m.RUnlock()
	if isBackUp {
		return m.backup.List()
	}
	return m.current.List()
}

func (m *modeInfo) ListAll() (res []*volume) {
	m.RLock()
	defer m.RUnlock()
	c := m.current.List()
	b := m.backup.List()
	res = make([]*volume, len(c)+len(b))
	copy(res, c[:])
	copy(res[len(c):], b[:])
	return res
}

func (m *modeInfo) VolumeNum() int {
	m.RLock()
	defer m.RUnlock()
	return m.backup.Len() + m.current.Len()
}

func (m *modeInfo) Delete(vid proto.Vid) {
	m.RLock()
	defer m.RUnlock()
	if !m.current.Delete(vid) {
		m.backup.Delete(vid)
	}
}

func (m *modeInfo) Put(vol *volume, isBackUp bool) {
	m.RLock()
	defer m.RUnlock()
	if isBackUp {
		m.backup.Put(vol)
		return
	}
	m.current.Put(vol)
}

func (m *modeInfo) Get(vid proto.Vid, isBackup bool) (*volume, bool) {
	m.RLock()
	defer m.RUnlock()
	if isBackup {
		return m.backup.Get(vid)
	}
	return m.current.Get(vid)
}

func (m *modeInfo) TotalFree() uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.backup.TotalFree() + m.current.TotalFree()
}

func (m *modeInfo) needSwitch2Backup(fSize uint64) bool {
	m.RLock()
	defer m.RUnlock()
	return m.current.UpdateTotalFree(-fSize) < m.totalThreshold
}

func (m *modeInfo) UpdateTotalFree(isBackup bool, free uint64) {
	m.RLock()
	defer m.RUnlock()
	if isBackup {
		m.backup.UpdateTotalFree(free)
		return
	}
	m.current.UpdateTotalFree(free)
}

func (m *modeInfo) dealDisCards(discards []proto.Vid) {
	if len(discards) == 0 {
		return
	}

	m.RLock()
	defer m.RUnlock()
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
}

type allocArgs struct {
	isInit   bool
	isBackup bool
	codeMode codemode.CodeMode
	count    int
}

type volumeMgr struct {
	BlobConfig
	VolConfig

	BidMgr

	clusterMgr clustermgr.APIProxy
	modeInfos  map[codemode.CodeMode]*modeInfo
	allocChs   map[codemode.CodeMode]chan *allocArgs
	preIdx     uint64
	closeCh    chan struct{}
}

func volConfCheck(cfg *VolConfig) {
	defaulter.Equal(&cfg.DefaultAllocVolsNum, defaultAllocVolsNum)
	defaulter.Equal(&cfg.RetainIntervalS, defaultRetainIntervalS)
	defaulter.Equal(&cfg.TotalThresholdRatio, defaultTotalThresholdRatio)
	defaulter.Equal(&cfg.InitVolumeNum, defaultInitVolumeNum)
	defaulter.Equal(&cfg.MetricReportIntervalS, defaultMetricIntervalS)
	defaulter.Equal(&cfg.RetainVolumeBatchNum, defaultRetainVolumeNum)
	defaulter.Equal(&cfg.RetainBatchIntervalS, defaultRetainBatchIntervalS)

	need := int(cfg.TotalThresholdRatio*float64(cfg.InitVolumeNum)) + 1
	if cfg.DefaultAllocVolsNum <= need {
		cfg.DefaultAllocVolsNum = need
	}
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
		modeInfos:  make(map[codemode.CodeMode]*modeInfo),
		allocChs:   make(map[codemode.CodeMode]chan *allocArgs),
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
	span := trace.SpanFromContextSafe(ctx)
	volumeReserveSize, err := v.clusterMgr.GetConfig(ctx, proto.VolumeReserveSizeKey)
	if err != nil {
		return errors.Base(err, "Get volume_reserve_size config from clusterMgr err").Detail(err)
	}
	v.VolConfig.VolumeReserveSize, err = strconv.Atoi(volumeReserveSize)
	if err != nil {
		return errors.Base(err, "strconv.Atoi volumeReserveSize err").Detail(err)
	}
	volumeChunkSize, err := v.clusterMgr.GetConfig(ctx, proto.VolumeChunkSizeKey)
	if err != nil {
		return errors.Base(err, "Get volume_chunk_size config from clusterMgr err").Detail(err)
	}
	volumeChunkSizeInt, err := strconv.Atoi(volumeChunkSize)
	if err != nil {
		return errors.Base(err, "strconv.Atoi volumeChunkSize err").Detail(err)
	}
	codeModeInfos, err := v.clusterMgr.GetConfig(ctx, proto.CodeModeConfigKey)
	if err != nil {
		return errors.Base(err, "Get code_mode config from clusterMgr err").Detail(err)
	}
	codeModeConfigInfos := make([]codemode.Policy, 0)
	err = json.Unmarshal([]byte(codeModeInfos), &codeModeConfigInfos)
	if err != nil {
		return errors.Base(err, "json.Unmarshal code_mode policy err").Detail(err)
	}
	for _, codeModeConfig := range codeModeConfigInfos {
		allocCh := make(chan *allocArgs)
		codeMode := codeModeConfig.ModeName.GetCodeMode()
		if !codeModeConfig.Enable {
			continue
		}
		v.allocChs[codeMode] = allocCh
		tactic := codeMode.Tactic()
		threshold := float64(v.InitVolumeNum*tactic.N*volumeChunkSizeInt) * v.TotalThresholdRatio
		info := &modeInfo{
			current:        &volumes{},
			backup:         &volumes{},
			totalThreshold: uint64(threshold),
		}
		v.modeInfos[codeMode] = info
		span.Infof("codeMode: %v, initVolumeNum: %v, threshold: %v", codeModeConfig.ModeName, v.InitVolumeNum, threshold)
	}

	for mode := range v.allocChs {
		applyArg := &allocArgs{
			isInit:   true,
			codeMode: mode,
			count:    v.InitVolumeNum,
		}

		go v.allocVolumeLoop(mode)
		v.allocChs[mode] <- applyArg

	}
	return
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
	info := v.modeInfos[args.CodeMode]
	if info != nil {
		span.Debugf("discard code mode[%s], vols[%v]", args.CodeMode, args.Discards)
		info.dealDisCards(args.Discards)
		return nil
	}
	return errors.New("code mode not exist")
}

func (v *volumeMgr) List(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []clustermgr.AllocVolumeInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)
	modeInfo, ok := v.modeInfos[codeMode]
	if !ok {
		return nil, nil, errcode.ErrNoAvaliableVolume
	}
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
	return 0, errcode.ErrNoAvailableVolume
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
	info := v.modeInfos[args.CodeMode]
	if info == nil {
		return 0, errcode.ErrNoAvaliableVolume
	}
	vols, err := v.getAvailableVols(ctx, args)
	if err != nil {
		return 0, err
	}
	span.Debugf("codeMode: %v, available volumes: %v", args.CodeMode, vols)
	vid, err := v.getNextVid(ctx, vols, info, args)
	if err != nil {
		return 0, err
	}

	span.Debugf("codeMode: %v, info.currentTotalFree: %v, info.totalThreshold: %v", args.CodeMode,
		info.TotalFree(), info.totalThreshold)

	return vid, nil
}

func (v *volumeMgr) getAvailableVols(ctx context.Context, args *proxy.AllocVolsArgs) (vols []*volume, err error) {
	info := v.modeInfos[args.CodeMode]
	info.dealDisCards(args.Discards)

	switch2Backup := info.needSwitch2Backup(args.Fsize)
	if switch2Backup {
		info.Lock()
		if info.current.TotalFree() < info.totalThreshold {
			info.current = info.backup
			info.backup = &volumes{}
		}
		info.Unlock()
	}

	vols = info.List(false)
	if len(vols) == 0 {
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum, false)
		return nil, errcode.ErrNoAvaliableVolume
	}

	if switch2Backup {
		info.UpdateTotalFree(false, -args.Fsize)
	}

	if len(info.List(true)) == 0 {
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum, true)
	}
	return vols, nil
}

// send message to apply channel, apply volume from CM
func (v *volumeMgr) allocNotify(ctx context.Context, mode codemode.CodeMode, count int, isBackup bool) {
	span := trace.SpanFromContextSafe(ctx)
	applyArg := &allocArgs{
		codeMode: mode,
		count:    count,
		isBackup: isBackup,
	}
	if _, ok := v.allocChs[mode]; ok {
		select {
		case v.allocChs[mode] <- applyArg:
			span.Infof("allocNotify {codeMode %s count %v} success", mode.String(), count)
		default:
			span.Infof("the codeMode %s is allocating volume, count: %d", mode.String(), count)
		}
		return
	}
	span.Panicf("the codeMode %v not exist", mode)
}

func (v *volumeMgr) allocVolume(ctx context.Context, args *clustermgr.AllocVolumeArgs) (ret []clustermgr.AllocVolumeInfo,
	err error) {
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

func (v *volumeMgr) allocVolumeLoop(mode codemode.CodeMode) {
	for {
		args := <-v.allocChs[mode]
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		requireCount := args.count
		for {
			allocArg := &clustermgr.AllocVolumeArgs{
				IsInit:   args.isInit,
				CodeMode: args.codeMode,
				Count:    requireCount,
			}
			span.Infof("allocVolumeLoop arguments: %+v", *allocArg)
			volumeRets, err := v.allocVolume(ctx, allocArg)
			if err != nil {
				span.Warnf("alloc volume failed, codeMode: %s, err: %v", mode.String(), err)
				time.Sleep(time.Duration(10) * time.Second)
				args.isInit = false
				continue
			}
			for index, vol := range volumeRets {
				allocVolInfo := &volume{
					AllocVolumeInfo: vol,
				}
				if allocArg.IsInit && len(volumeRets) >= 2*v.InitVolumeNum && index >= v.InitVolumeNum {
					v.modeInfos[allocArg.CodeMode].Put(allocVolInfo, true)
					continue
				}
				v.modeInfos[allocArg.CodeMode].Put(allocVolInfo, args.isBackup)
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
