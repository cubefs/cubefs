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

package volumemgr

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	NoDiskLoadThreshold = int(^uint(0) >> 1)
	healthiestScore     = 0
)

type allocConfig struct {
	allocatableDiskLoadThreshold int
	allocFactor                  int
	allocatableSize              uint64
	codeModes                    map[codemode.CodeMode]codeModeConf
}

type idleItem struct {
	head    *list.List
	element *list.Element
}

type idleVolumes struct {
	m              map[proto.Vid]idleItem
	allocatable    *list.List
	notAllocatable *list.List

	sync.RWMutex
}

func (i *idleVolumes) getAllIdles() []*volume {
	i.RLock()
	ret := make([]*volume, 0, i.allocatable.Len())
	head := i.allocatable.Front()
	for head != nil {
		ret = append(ret, head.Value.(*volume))
		head = head.Next()
	}
	i.RUnlock()
	return ret
}

func (i *idleVolumes) statAllocatableNum() int {
	i.RLock()
	defer i.RUnlock()
	return i.allocatable.Len()
}

func (i *idleVolumes) addAllocatable(vol *volume) {
	i.Lock()
	if item, ok := i.m[vol.vid]; ok {
		item.head.Remove(item.element)
	}
	e := i.allocatable.PushFront(vol)
	i.m[vol.vid] = idleItem{element: e, head: i.allocatable}
	i.Unlock()
}

func (i *idleVolumes) addNotAllocatable(vol *volume) {
	i.Lock()
	if item, ok := i.m[vol.vid]; ok {
		item.head.Remove(item.element)
	}
	e := i.notAllocatable.PushFront(vol)
	i.m[vol.vid] = idleItem{element: e, head: i.notAllocatable}
	i.Unlock()
}

func (i *idleVolumes) delete(vid proto.Vid) {
	i.Lock()
	if item, ok := i.m[vid]; ok {
		item.head.Remove(item.element)
		delete(i.m, vid)
	}
	i.Unlock()
}

func (i *idleVolumes) get(vid proto.Vid) (vol *volume) {
	i.RLock()
	if item, ok := i.m[vid]; ok {
		vol = item.element.Value.(*volume)
	}
	i.RUnlock()
	return vol
}

func (i *idleVolumes) allocFromOptions(optionalVids []proto.Vid, count int) (succeed []proto.Vid) {
	i.Lock()
	defer i.Unlock()
	for _, vid := range optionalVids {
		if item, ok := i.m[vid]; ok {
			item.head.Remove(item.element)
			delete(i.m, vid)
			succeed = append(succeed, vid)
			if len(succeed) >= count {
				return
			}
		}
	}
	return
}

type volumeMap map[proto.Vid]*volume

type activeVolumes struct {
	allocatorVols map[string]volumeMap
	diskLoad      map[proto.DiskID]int
	sync.RWMutex
}

// volume allocator, use for allocating volume
type volumeAllocator struct {
	// idle volumes
	idles map[codemode.CodeMode]*idleVolumes
	// actives volumes
	actives *activeVolumes

	allocConfig
}

type sortVid []vidLoad

type vidLoad struct {
	vid    proto.Vid
	load   int
	health int
}

func (v sortVid) Len() int           { return len(v) }
func (v sortVid) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v sortVid) Less(i, j int) bool { return v[i].health > v[j].health || v[i].load < v[j].load }

func newVolumeAllocator(cfg allocConfig) *volumeAllocator {
	idles := make(map[codemode.CodeMode]*idleVolumes)
	for _, modeConf := range cfg.codeModes {
		idles[modeConf.mode] = &idleVolumes{
			m:              make(map[proto.Vid]idleItem),
			allocatable:    list.New(),
			notAllocatable: list.New(),
		}
	}
	return &volumeAllocator{
		idles: idles,
		actives: &activeVolumes{
			allocatorVols: make(map[string]volumeMap),
			diskLoad:      make(map[proto.DiskID]int),
		},
		allocConfig: cfg,
	}
}

// volume free size or volume health change event callback, check if move volume into idle's allocatable head
func (a *volumeAllocator) VolumeFreeHealthCallback(ctx context.Context, vol *volume) error {
	allocatableScoreThreshold := a.codeModes[vol.volInfoBase.CodeMode].tactic.PutQuorum - a.getShardNum(vol.volInfoBase.CodeMode)
	if vol.canAlloc(a.allocatableSize, allocatableScoreThreshold) {
		a.idles[vol.volInfoBase.CodeMode].addAllocatable(vol)
	}
	return nil
}

// volume status change event callback, idle change should Insert into volume allocator's idle head
func (a *volumeAllocator) VolumeStatusIdleCallback(ctx context.Context, vol *volume) error {
	span := trace.SpanFromContextSafe(ctx)
	allocatableScoreThreshold := a.codeModes[vol.volInfoBase.CodeMode].tactic.PutQuorum - a.getShardNum(vol.volInfoBase.CodeMode)
	span.Debugf("vid: %d set status idle callback, status is %d,free is %d,health is %d", vol.vid, vol.volInfoBase.Status, vol.volInfoBase.Free, vol.volInfoBase.HealthScore)
	if vol.canAlloc(a.allocatableSize, allocatableScoreThreshold) {
		a.idles[vol.volInfoBase.CodeMode].addAllocatable(vol)
	} else {
		a.idles[vol.volInfoBase.CodeMode].addNotAllocatable(vol)
	}

	if vol.token != nil {
		host, _, err := proto.DecodeToken(vol.token.tokenID)
		if err != nil {
			span.Errorf("decode token error,%s", vol.token.String())
			return err
		}
		a.removeAllocatedVolumes(vol.vid, host)
	}
	return nil
}

// volume status change event callback, active change should delete from volume allocator's idle head
// and Insert into allocated head
func (a *volumeAllocator) VolumeStatusActiveCallback(ctx context.Context, vol *volume) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("vid: %d set status active callback, status is %d", vol.vid, vol.volInfoBase.Status)
	host, _, err := proto.DecodeToken(vol.token.tokenID)
	if err != nil {
		span.Errorf("decode token error,%s", vol.token.String())
		return err
	}
	a.insertAllocatedVolumes(vol, host)
	a.idles[vol.volInfoBase.CodeMode].delete(vol.vid)
	return nil
}

// volume status change event callback, lock change should delete from volume allocator's idle head
func (a *volumeAllocator) VolumeStatusLockCallback(ctx context.Context, vol *volume) error {
	a.idles[vol.volInfoBase.CodeMode].delete(vol.vid)
	return nil
}

// Insert a volume into volume allocator's idles head
// please ensure that this volume must be idle status
func (a *volumeAllocator) Insert(v *volume, mode codemode.CodeMode) {
	a.idles[mode].addAllocatable(v)
}

// PreAlloc select volumes which can alloc
// 1. when EnableDiskLoad=false, all volume will range by health, the more healthier volume will range in front of the optional head
// 2. when EnableDiskLoad=true, if do not hash enough volumes to alloc ,
//      1) first add disk's load and retry, each time add one until disk's load equal to diskLoadThreshold will set EnableDiskLoad=false
//      2) second minus volume score and retry , each time minus one until volume's score equal to scoreThreshold
func (a *volumeAllocator) PreAlloc(ctx context.Context, mode codemode.CodeMode, count int) ([]proto.Vid, int) {
	span := trace.SpanFromContextSafe(ctx)
	idleVolumes := a.idles[mode]
	if idleVolumes == nil {
		return nil, 0
	}

	allIdles := idleVolumes.getAllIdles()
	availableVolCount := len(allIdles)
	allocatableScoreThreshold := a.codeModes[mode].tactic.PutQuorum - a.getShardNum(mode)
	isEnableDiskLoad := a.isEnableDiskLoad()
	scoreThreshold := healthiestScore
	// diskLoadThreshold start half of allocatableDiskLoadThreshold,avoid loop too much times
	diskLoadThreshold := a.allocatableDiskLoadThreshold / 2
	// optionalVids include all volume id which satisfied with our condition(idle/enough free size/health/not over disk load)
	// all vid will range by health, the more healthier volume will range in front of the optional head
	optionalVids := make([]proto.Vid, 0)

RETRY:
	index := 0
	var assignable []*volume
	span.Debugf("prealloc volume length is %d,isEnableDiskLoad:%v", len(allIdles), isEnableDiskLoad)
	now := time.Now()
	for _, volume := range allIdles {
		volume.lock.RLock()
		if volume.canAlloc(a.allocatableSize, scoreThreshold) && (!isEnableDiskLoad || !a.isOverload(volume.vUnits, diskLoadThreshold)) {
			optionalVids = append(optionalVids, volume.vid)
			// only insufficient free size or unhealthy volume move to temporary head,
			// ignore over diskLoad volume
		} else if !volume.canAlloc(a.allocatableSize, allocatableScoreThreshold) && volume.canInsert() {
			idleVolumes.addNotAllocatable(volume)
		} else {
			assignable = append(assignable, volume)
		}
		volume.lock.RUnlock()

		if len(optionalVids) >= a.allocFactor*count {
			break
		}

		// go to the end, first retry with high disk load volume
		// second  lower health score volume
		if index == availableVolCount-1 {
			span.Infof("assignable volume length is %d", len(assignable))
			if len(assignable) == 0 {
				span.Warnf("has no assignable volume,enableDiskLoad:%v,diskLoadThreshold:%d", isEnableDiskLoad, diskLoadThreshold)
				break
			}
			if isEnableDiskLoad && diskLoadThreshold < a.allocatableDiskLoadThreshold {
				diskLoadThreshold += 1
			} else if isEnableDiskLoad {
				isEnableDiskLoad = false
			} else if scoreThreshold > allocatableScoreThreshold {
				scoreThreshold -= 1
			}
			allIdles = assignable
			availableVolCount = len(allIdles)
			goto RETRY
		}
		index++
	}

	span.Infof("optional vids length is %d, vids is %v", len(optionalVids), optionalVids)
	if a.isEnableDiskLoad() {
		optionalVids = a.sortVidByLoad(mode, optionalVids)
	}
	ret := idleVolumes.allocFromOptions(optionalVids, count)
	span.Debugf("preAlloc volume cost time:%v", time.Since(now))
	return ret, diskLoadThreshold
}

// StatAllocatable return allocatable volume num about every kind of code mode
func (a *volumeAllocator) StatAllocatable() (ret map[codemode.CodeMode]int) {
	allocVolNum := make(map[codemode.CodeMode]int)
	for mode := range a.idles {
		allocVolNum[mode] = a.idles[mode].statAllocatableNum()
	}
	return allocVolNum
}

func (a *volumeAllocator) GetExpiredVolumes() (expiredVids []proto.Vid) {
	a.actives.RLock()
	actives := make([]*volume, 0)
	for _, m := range a.actives.allocatorVols {
		for _, vol := range m {
			actives = append(actives, vol)
		}
	}
	a.actives.RUnlock()

	for _, vol := range actives {
		vol.lock.RLock()
		if vol.isExpired() {
			expiredVids = append(expiredVids, vol.vid)
		}
		vol.lock.RUnlock()
	}
	return
}

func (a *volumeAllocator) LisAllocatedVolumesByHost(host string) (ret []*volume) {
	a.actives.RLock()
	volM, ok := a.actives.allocatorVols[host]
	if !ok {
		a.actives.RUnlock()
		return nil
	}
	a.actives.RUnlock()

	for _, volume := range volM {
		ret = append(ret, volume)
	}

	return
}

func (a *volumeAllocator) insertAllocatedVolumes(v *volume, host string) {
	a.actives.Lock()
	volM, ok := a.actives.allocatorVols[host]
	if !ok {
		volM = make(volumeMap)
		a.actives.allocatorVols[host] = volM
	}
	volM[v.vid] = v

	for _, unit := range v.vUnits {
		a.actives.diskLoad[unit.vuInfo.DiskID]++
	}
	a.actives.Unlock()
}

func (a *volumeAllocator) removeAllocatedVolumes(vid proto.Vid, host string) {
	a.actives.Lock()
	volM, ok := a.actives.allocatorVols[host]
	if ok {
		vol, ok := volM[vid]
		if ok {
			for _, unit := range vol.vUnits {
				a.actives.diskLoad[unit.vuInfo.DiskID]--
			}
		}
		delete(volM, vid)
	}
	a.actives.Unlock()
}

func (a *volumeAllocator) isOverload(vUnits []*volumeUnit, diskLoadThreshold int) bool {
	a.actives.RLock()
	defer a.actives.RUnlock()

	for _, unit := range vUnits {
		if a.actives.diskLoad[unit.vuInfo.DiskID] > diskLoadThreshold {
			return true
		}
	}
	return false
}

func (a *volumeAllocator) isEnableDiskLoad() bool {
	return a.allocatableDiskLoadThreshold != NoDiskLoadThreshold
}

func (a *volumeAllocator) getShardNum(mode codemode.CodeMode) int {
	modeConf := a.codeModes[mode]
	return modeConf.tactic.N + modeConf.tactic.M + modeConf.tactic.L
}

func (a *volumeAllocator) sortVidByLoad(mode codemode.CodeMode, vids []proto.Vid) (ret []proto.Vid) {
	if len(vids) <= 1 {
		return vids
	}

	var arrVids sortVid
	for _, vid := range vids {
		volume := a.idles[mode].get(vid)
		if volume != nil {
			load := 0
			volume.lock.RLock()
			diskIDs := make([]proto.DiskID, 0, len(volume.vUnits))
			for _, unit := range volume.vUnits {
				diskIDs = append(diskIDs, unit.vuInfo.DiskID)
			}
			score := volume.volInfoBase.HealthScore
			vid := volume.vid
			volume.lock.RUnlock()

			a.actives.RLock()
			for _, diskID := range diskIDs {
				load += a.actives.diskLoad[diskID]
			}
			a.actives.RUnlock()
			arrVids = append(arrVids, vidLoad{vid, load, score})
		}
	}
	sort.Sort(arrVids)
	ret = make([]proto.Vid, 0, len(arrVids))
	for _, arrVid := range arrVids {
		ret = append(ret, arrVid.vid)
	}

	return ret
}
