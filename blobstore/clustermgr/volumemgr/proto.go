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
	"context"
	"fmt"
	"sync"
	"time"

	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var defaultVolumeNotifyQueue = &volumeNotifyQueue{waits: make(map[interface{}][]NotifyFunc)}

// internal volume struct
type volume struct {
	vid           proto.Vid
	vUnits        []*volumeUnit
	volInfoBase   cm.VolumeInfoBase
	token         *token
	smallestVUIdx uint8
	lock          sync.RWMutex
}

func (vol *volume) ToRecord() *volumedb.VolumeRecord {
	vuidPrefixs := make([]proto.VuidPrefix, 0, len(vol.vUnits))
	for _, unit := range vol.vUnits {
		vuidPrefixs = append(vuidPrefixs, unit.vuidPrefix)
	}
	return &volumedb.VolumeRecord{
		Vid:            vol.vid,
		VuidPrefixs:    vuidPrefixs,
		CodeMode:       vol.volInfoBase.CodeMode,
		HealthScore:    vol.volInfoBase.HealthScore,
		Status:         vol.volInfoBase.Status,
		Total:          vol.volInfoBase.Total,
		Free:           vol.volInfoBase.Free,
		Used:           vol.volInfoBase.Used,
		CreateByNodeID: vol.volInfoBase.CreateByNodeID,
	}
}

func (vol *volume) ToVolumeInfo() cm.VolumeInfo {
	units := make([]cm.Unit, len(vol.vUnits))
	for i, vUnit := range vol.vUnits {
		units[i] = cm.Unit{
			Vuid:   vUnit.vuInfo.Vuid,
			DiskID: vUnit.vuInfo.DiskID,
			Host:   vUnit.vuInfo.Host,
		}
	}
	return cm.VolumeInfo{
		VolumeInfoBase: vol.volInfoBase,
		Units:          units,
	}
}

func (vol *volume) getStatus() proto.VolumeStatus {
	return vol.volInfoBase.Status
}

func (vol *volume) setStatus(ctx context.Context, status proto.VolumeStatus) {
	vol.volInfoBase.Status = status
	// volume status statistic
	defaultVolumeStatusStat.Add(vol, status)
	// volume status change notify
	defaultVolumeNotifyQueue.Notify(ctx, volStatusNottifyKeyPrefix+status.String(), vol)
}

func (vol *volume) setFree(ctx context.Context, free uint64) {
	vol.volInfoBase.Free = free
	defaultVolumeNotifyQueue.Notify(ctx, VolFreeHealthChangeNotifyKey, vol)
}

func (vol *volume) setHealthScore(ctx context.Context, score int) {
	vol.volInfoBase.HealthScore = score
	defaultVolumeNotifyQueue.Notify(ctx, VolFreeHealthChangeNotifyKey, vol)
}

// only idle volume can Insert into volume allocator
func (vol *volume) canInsert() bool {
	return vol.volInfoBase.Status == proto.VolumeStatusIdle
}

func (vol *volume) canAlloc(allocatableSize uint64, allocatableScoreThreshold int) bool {
	if vol.canInsert() && vol.volInfoBase.Free > allocatableSize && vol.volInfoBase.HealthScore >= allocatableScoreThreshold {
		return true
	}
	return false
}

func (vol *volume) canRetain(freezeThreshold uint64, retainThreshold int) bool {
	if vol.volInfoBase.Free > freezeThreshold &&
		vol.volInfoBase.HealthScore >= retainThreshold &&
		vol.token.expireTime >= time.Now().UnixNano() &&
		vol.getStatus() == proto.VolumeStatusActive {
		return true
	}
	return false
}

func (vol *volume) canLock() bool {
	return vol.getStatus() == proto.VolumeStatusIdle
}

func (vol *volume) canUnlock() bool {
	return vol.getStatus() == proto.VolumeStatusLock
}

func (vol *volume) isExpired() bool {
	return vol.getStatus() == proto.VolumeStatusActive && vol.token.expireTime < time.Now().UnixNano()
}

func (vol *volume) isValid() bool {
	if vol.vid == proto.InvalidVid {
		return false
	}
	for _, unit := range vol.vUnits {
		if !unit.isValid() {
			return false
		}
	}
	return true
}

func (vol *volume) getScoreThreshold() int {
	scoreThreshold := vol.volInfoBase.CodeMode.Tactic().PutQuorum - vol.volInfoBase.CodeMode.GetShardNum()
	return scoreThreshold
}

// internal volume unit struct
type volumeUnit struct {
	vuidPrefix proto.VuidPrefix
	epoch      uint32
	nextEpoch  uint32
	vuInfo     *cm.VolumeUnitInfo
}

func (vUnit *volumeUnit) ToVolumeUnitRecord() (ret *volumedb.VolumeUnitRecord) {
	return &volumedb.VolumeUnitRecord{
		VuidPrefix: vUnit.vuidPrefix,
		DiskID:     vUnit.vuInfo.DiskID,
		Epoch:      vUnit.epoch,
		NextEpoch:  vUnit.nextEpoch,
		Free:       vUnit.vuInfo.Free,
		Used:       vUnit.vuInfo.Used,
		Total:      vUnit.vuInfo.Total,
		Compacting: vUnit.vuInfo.Compacting,
	}
}

func (vUnit *volumeUnit) isValid() bool {
	vuid := proto.EncodeVuid(vUnit.vuidPrefix, vUnit.epoch)
	return vUnit.vuInfo.Vuid.IsValid() && vuid.IsValid() && vUnit.epoch <= vUnit.nextEpoch
}

type token struct {
	vid        proto.Vid
	tokenID    string
	expireTime int64
}

func (tok *token) ToTokenRecord() (ret *volumedb.TokenRecord) {
	return &volumedb.TokenRecord{
		Vid:        tok.vid,
		TokenID:    tok.tokenID,
		ExpireTime: tok.expireTime,
	}
}

func (tok *token) String() string {
	return fmt.Sprintf("{\"vid\": %d, \"tokenID\": \"%s\",\"expireTime\": %d}", tok.vid, tok.tokenID, tok.expireTime)
}

type codeModeConf struct {
	mode      codemode.CodeMode
	tactic    codemode.Tactic
	sizeRatio float64
	enable    bool
}

func newShardedVolumes(sliceMapNum uint32) *shardedVolumes {
	m := &shardedVolumes{
		num:   sliceMapNum,
		m:     make(map[uint32]map[proto.Vid]*volume),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < sliceMapNum; i++ {
		m.locks[i] = &sync.RWMutex{}
		m.m[i] = make(map[proto.Vid]*volume)
	}

	return m
}

// shardedVolumes is an effective data struct (concurrent map implements)
type shardedVolumes struct {
	num   uint32
	m     map[uint32]map[proto.Vid]*volume
	locks map[uint32]*sync.RWMutex
}

// get volume from shardedVolumes
func (s *shardedVolumes) getVol(vid proto.Vid) *volume {
	idx := uint32(vid) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.m[idx][vid]
}

// put new volume into shardedVolumes
func (s *shardedVolumes) putVol(v *volume) error {
	idx := uint32(v.vid) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][v.vid]
	// volume already exist
	if ok {
		return nil
	}
	s.m[idx][v.vid] = v
	return nil
}

// range shardedVolumes, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *shardedVolumes) rangeVol(f func(v *volume) error) {
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		for _, v := range s.m[i] {
			err := f(v)
			if err != nil {
				l.RUnlock()
				return
			}
		}
		l.RUnlock()
	}
}

type NotifyFunc func(ctx context.Context, vol *volume) error

type volumeNotifyQueue struct {
	waits map[interface{}][]NotifyFunc
	sync.RWMutex
}

// Add add a notify function in specified key
func (w *volumeNotifyQueue) Add(key interface{}, f NotifyFunc) {
	w.Lock()
	w.waits[key] = append(w.waits[key], f)
	w.Unlock()
}

// Notify will call all notify queue function in specified key
func (w *volumeNotifyQueue) Notify(ctx context.Context, key interface{}, vol *volume) {
	var (
		span = trace.SpanFromContextSafe(ctx)
		fs   []NotifyFunc
	)
	w.RLock()
	fs = append(fs, w.waits[key]...)
	w.RUnlock()
	for _, f := range fs {
		if err := f(ctx, vol); err != nil {
			span.Fatalf("wakeup function callback failed, name(%v), volume(%v), err: %v", key, vol, err)
		}
	}
}

/*
	util transform function
*/

func volumeRecordToVolumeInfoBase(volRecord *volumedb.VolumeRecord) cm.VolumeInfoBase {
	return cm.VolumeInfoBase{
		Vid:            volRecord.Vid,
		CodeMode:       volRecord.CodeMode,
		HealthScore:    volRecord.HealthScore,
		Used:           volRecord.Used,
		Total:          volRecord.Total,
		Free:           volRecord.Free,
		CreateByNodeID: volRecord.CreateByNodeID,
	}
}

func volumeUnitsToVolumeUnitRecords(units []*volumeUnit) []*volumedb.VolumeUnitRecord {
	ret := make([]*volumedb.VolumeUnitRecord, len(units))
	for i, unit := range units {
		ret[i] = unit.ToVolumeUnitRecord()
	}
	return ret
}

// transform db's volumeUnitRecord into volumeMgr's volumeUnit
func volumeUnitRecordToVolumeUnit(record *volumedb.VolumeUnitRecord) (ret *volumeUnit) {
	return &volumeUnit{
		vuidPrefix: record.VuidPrefix,
		epoch:      record.Epoch,
		nextEpoch:  record.NextEpoch,
		vuInfo: &cm.VolumeUnitInfo{
			Vuid:       proto.EncodeVuid(record.VuidPrefix, record.Epoch),
			DiskID:     record.DiskID,
			Free:       record.Free,
			Used:       record.Used,
			Total:      record.Total,
			Compacting: record.Compacting,
		},
	}
}

func tokenRecordToToken(record *volumedb.TokenRecord) (ret *token) {
	return &token{
		vid:        record.Vid,
		tokenID:    record.TokenID,
		expireTime: record.ExpireTime,
	}
}
