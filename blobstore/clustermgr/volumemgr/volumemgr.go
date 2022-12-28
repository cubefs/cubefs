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
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/configmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	// scope name
	vidScopeName = "vid"
	// volume status notify key prefix
	volStatusNottifyKeyPrefix = "status-"

	// defaultValue
	defaultRetainTimeS                 = 600
	defaultRetainThreshold             = 0
	defaultCheckExpiredVolumeIntervalS = 60
	defaultFlushIntervalS              = 600
	defaultApplyConcurrency            = 20
	defaultMinAllocatableVolumeCount   = 5
	defaultVolumeSliceMapNum           = 10
	defaultListVolumeMaxCount          = 2000
	defaultAllocFactor                 = 5
	defaultAllocatableSize             = 1 << 30
)

// notify queue key definition
var (
	VolFreeHealthChangeNotifyKey = "freeOrHealthChange"
)

var (
	ErrVolumeNotExist           = apierrors.ErrVolumeNotExist
	ErrVolumeUnitNotExist       = apierrors.ErrVolumeUnitNotExist
	ErrOldVuidNotMatch          = apierrors.ErrOldVuidNotMatch
	ErrNewVuidNotMatch          = apierrors.ErrNewVuidNotMatch
	ErrNewDiskIDNotMatch        = apierrors.ErrNewDiskIDNotMatch
	ErrInvalidCodeMode          = apierrors.ErrInvalidCodeMode
	ErrVolumeNotAlloc           = apierrors.ErrRetainVolumeNotAlloc
	ErrCreateVolumeAlreadyExist = errors.New("create volume already exist")
	ErrInvalidVolume            = errors.New(" volume is invalid ")
	ErrInvalidToken             = errors.New("retain token is invalid")
	ErrRepeatUpdateUnit         = errors.New("repeat update volume unit")
)

// VolumeMgr defines volume manager interface
type VolumeMgrAPI interface {
	// GetVolumeInfo get volume  details info
	GetVolumeInfo(ctx context.Context, vid proto.Vid) (ret *cm.VolumeInfo, err error)

	// ListVolumeInfo list volumes
	ListVolumeInfo(ctx context.Context, args *cm.ListVolumeArgs) (ret []*cm.VolumeInfo, err error)

	// ListVolumeV2 list specified status volumes
	ListVolumeInfoV2(ctx context.Context, status proto.VolumeStatus) (ret []*cm.VolumeInfo, err error)

	// VolumeAlloc alloc volumes
	AllocVolume(ctx context.Context, mode codemode.CodeMode, count int, host string) (ret *cm.AllocatedVolumeInfos, err error)

	// ListAllocatedVolume list allocated volumes
	ListAllocatedVolume(ctx context.Context, host string, mode codemode.CodeMode) (ret *cm.AllocatedVolumeInfos)

	// PreUpdateVolumeUnit update volume
	PreUpdateVolumeUnit(ctx context.Context, args *cm.UpdateVolumeArgs) (err error)

	// PreRetainVolume retain volume
	PreRetainVolume(ctx context.Context, tokens []string, host string) (ret *cm.RetainVolumes, err error)

	// DiskWritableChange call when disk broken or heartbeat timeout or switch readonly, it'll refresh volume's health
	DiskWritableChange(ctx context.Context, diskID proto.DiskID) (err error)

	// AllocVolumeUnit alloc a new chunk to volume unit, it will increase volumeUnit's nextEpoch in memory
	AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (*cm.AllocVolumeUnit, error)

	// ReleaseVolumeUnit release old volume unit's chunk
	ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID, force bool) (err error)

	// ListVolumeUnitInfo head all volume unit info in the disk
	ListVolumeUnitInfo(ctx context.Context, args *cm.ListVolumeUnitArgs) ([]*cm.VolumeUnitInfo, error)
	LockVolume(ctx context.Context, vid proto.Vid) error
	UnlockVolume(ctx context.Context, vid proto.Vid) error

	// Stat return volume statistic info
	Stat(ctx context.Context) (stat cm.VolumeStatInfo)
}

// volumeMgr implements VolumeMgr interface.
type VolumeMgr struct {
	module        string
	raftServer    raftserver.RaftServer
	all           *shardedVolumes
	allocator     *volumeAllocator
	taskMgr       *taskManager
	lastTaskIdMap sync.Map
	dirty         atomic.Value
	applyTaskPool *base.TaskDistribution

	createVolChan chan struct{}
	closeLoopChan chan struct{}

	volumeTbl    *volumedb.VolumeTable
	transitedTbl *volumedb.TransitedTable

	diskMgr        diskmgr.DiskMgrAPI
	scopeMgr       scopemgr.ScopeMgrAPI
	configMgr      configmgr.ConfigMgrAPI
	blobNodeClient blobnode.StorageAPI

	lastFlushTime  time.Time
	pendingEntries sync.Map
	codeMode       map[codemode.CodeMode]codeModeConf

	VolumeMgrConfig
}

func (v *VolumeMgr) GetVolumeInfo(ctx context.Context, vid proto.Vid) (ret *cm.VolumeInfo, err error) {
	vol := v.all.getVol(vid)
	if vol == nil {
		return nil, ErrVolumeNotExist
	}
	vol.lock.RLock()
	volInfo := vol.ToVolumeInfo()
	vol.lock.RUnlock()
	return &volInfo, nil
}

func (v *VolumeMgr) ListVolumeInfo(ctx context.Context, args *cm.ListVolumeArgs) (ret []*cm.VolumeInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if args.Count > defaultListVolumeMaxCount {
		args.Count = defaultListVolumeMaxCount
	}
	vids, err := v.volumeTbl.ListVolume(args.Count, args.Marker)
	if err != nil {
		span.Errorf("head volume failed:%v", err)
		return nil, errors.Info(err, "volumeMgr head volume failed").Detail(err)
	}
	if len(vids) == 0 {
		return
	}

	for _, vid := range vids {
		vol := v.all.getVol(vid)
		if vol == nil {
			return nil, ErrVolumeNotExist
		}
		vol.lock.RLock()
		volInfo := vol.ToVolumeInfo()
		vol.lock.RUnlock()
		ret = append(ret, &volInfo)
	}

	return ret, nil
}

func (v *VolumeMgr) ListVolumeInfoV2(ctx context.Context, status proto.VolumeStatus) (ret []*cm.VolumeInfo, err error) {
	vids := defaultVolumeStatusStat.GetVidsByStatus(status)
	for _, vid := range vids {
		vol := v.all.getVol(vid)
		if vol == nil {
			return nil, ErrVolumeNotExist
		}
		vol.lock.RLock()
		volInfo := vol.ToVolumeInfo()
		vol.lock.RUnlock()
		ret = append(ret, &volInfo)
	}
	return ret, nil
}

func (v *VolumeMgr) ListAllocatedVolume(ctx context.Context, host string, mode codemode.CodeMode) (ret *cm.AllocatedVolumeInfos) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("head allocated volume,host is %s", host)

	var allcoVolumes []cm.AllocVolumeInfo
	volumes := v.allocator.LisAllocatedVolumesByHost(host)
	for _, volume := range volumes {
		volume.lock.RLock()
		if volume.volInfoBase.CodeMode == mode && volume.canRetain(v.FreezeThreshold, v.RetainThreshold) {
			volInfo := volume.ToVolumeInfo()
			allocVol := cm.AllocVolumeInfo{
				VolumeInfo: volInfo,
				Token:      volume.token.tokenID,
				ExpireTime: volume.token.expireTime,
			}
			allcoVolumes = append(allcoVolumes, allocVol)
		}
		volume.lock.RUnlock()
	}

	ret = &cm.AllocatedVolumeInfos{
		AllocVolumeInfos: allcoVolumes,
	}

	return
}

func (v *VolumeMgr) PreRetainVolume(ctx context.Context, tokens []string, host string) (ret *cm.RetainVolumes, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start preRetain volume, tokens is %#v,host is %s", tokens, host)

	var retainVolumes []cm.RetainVolume
	var errCnt int
	for _, tok := range tokens {
		tokHost, vid, err := proto.DecodeToken(tok)
		if err != nil || tokHost != host {
			span.Errorf("retain tokenID:%v is error:%v", tok, err)
			errCnt++
			continue
		}
		vol := v.all.getVol(vid)
		if vol == nil {
			span.Errorf("retain volume not exist:%d", vid)
			errCnt++
			continue
		}

		vol.lock.RLock()
		if vol.token == nil || vol.token.tokenID != tok {
			vol.lock.RUnlock()
			// each volume has a tokenID, one tokenID mismatch do not affect other volume
			span.Errorf("volume not alloc,can not alloc:%d", vid)
			errCnt++
			continue
		}
		if vol.canRetain(v.FreezeThreshold, v.RetainThreshold) {
			retainVolume := cm.RetainVolume{
				Token:      tok,
				ExpireTime: time.Now().UnixNano() + int64(time.Duration(v.RetainTimeS)*time.Second),
			}
			retainVolumes = append(retainVolumes, retainVolume)
		}
		vol.lock.RUnlock()
	}

	// when errCnt > 0 report retain error metric to alert
	if errCnt > 0 {
		span.Infof("retain volume error metric report,errCnt:%d,region:%s,clusterID:%d", errCnt, v.Region, v.ClusterID)
		v.reportVolRetainError(float64(errCnt))
	}

	if retainVolumes == nil {
		return nil, nil
	}

	return &cm.RetainVolumes{RetainVolTokens: retainVolumes}, nil
}

func (v *VolumeMgr) AllocVolume(ctx context.Context, mode codemode.CodeMode, count int, host string) (ret *cm.AllocatedVolumeInfos, err error) {
	if _, ok := v.codeMode[mode]; !ok {
		return nil, ErrInvalidCodeMode
	}
	span := trace.SpanFromContextSafe(ctx)

	isAllocSucc := true
	pendingKey := uuid.New().String()
	v.pendingEntries.Store(pendingKey, nil)
	defer v.pendingEntries.Delete(pendingKey)

	preAllocVids, diskLoadThreshold := v.allocator.PreAlloc(ctx, mode, count)
	span.Debugf("preAlloc vids is %v,now disk load is %d", preAllocVids, diskLoadThreshold)
	defer func() {
		// check if need to insert volume back
		if !isAllocSucc {
			for _, vid := range preAllocVids {
				volume := v.all.getVol(vid)
				volume.lock.RLock()
				if volume.canInsert() {
					v.allocator.Insert(volume, volume.volInfoBase.CodeMode)
				}
				volume.lock.RUnlock()
			}
			// send create volume channel when alloc failed
			select {
			case v.createVolChan <- struct{}{}:
				span.Debug("apply alloc volume finish, notify to if need create volume")
			default:
			}
		}
		if diskLoadThreshold >= v.allocator.allocatableDiskLoadThreshold {
			span.Warnf("now disk load is %d reach the allocatable threshold %d", diskLoadThreshold, v.allocator.allocatableDiskLoadThreshold)
			v.reportVolAllocOverDiskLoad(float64(diskLoadThreshold))
		}
	}()
	if len(preAllocVids) == 0 {
		isAllocSucc = false
		return nil, apierrors.ErrNoAvailableVolume
	}

	allocArgs := &AllocVolumeCtx{
		Vids:               preAllocVids,
		Host:               host,
		PendingAllocVolKey: pendingKey,
		ExpireTime:         time.Now().Add(time.Second * time.Duration(v.RetainTimeS)).UnixNano(),
	}
	span.Debugf("alloc volume is %+v", allocArgs)
	data, err := json.Marshal(allocArgs)
	if err != nil {
		isAllocSucc = false
		span.Errorf("marshal data error,args:%+v", allocArgs)
		return nil, err
	}

	proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeAllocVolume, data, base.ProposeContext{ReqID: span.TraceID()})
	err = v.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		isAllocSucc = false
		span.Errorf("raft propose failed, error is %v", err)
		return nil, errors.Info(err, "propose failed").Detail(err)
	}

	value, _ := v.pendingEntries.Load(pendingKey)
	if value == nil {
		isAllocSucc = false
		span.Error("load pending entry error")
		return nil, errors.New("propose success without set pending key")
	}
	ret = value.(*cm.AllocatedVolumeInfos)

	allocatedVidM := make(map[proto.Vid]bool)
	for i := range ret.AllocVolumeInfos {
		allocatedVidM[ret.AllocVolumeInfos[i].Vid] = true
	}

	// Insert unallocated volume back
	for _, vid := range preAllocVids {
		if !allocatedVidM[vid] {
			volume := v.all.getVol(vid)
			volume.lock.RLock()
			if volume.canInsert() {
				v.allocator.Insert(volume, volume.volInfoBase.CodeMode)
			}
			volume.lock.RUnlock()
		}
	}

	if len(allocatedVidM) == 0 {
		span.Errorf("no available volume, alloc args:%+v, alloc volume is:%d", allocArgs, len(ret.AllocVolumeInfos))
		return nil, apierrors.ErrNoAvailableVolume
	}

	return ret, nil
}

func (v *VolumeMgr) DiskWritableChange(ctx context.Context, diskID proto.DiskID) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	vuidPrefixes, err := v.volumeTbl.ListVolumeUnit(diskID)
	if err != nil {
		return err
	}
	if len(vuidPrefixes) == 0 {
		return nil
	}
	data, err := json.Marshal(vuidPrefixes)
	if err != nil {
		return errors.Info(err, "json marshal failed").Detail(err)
	}

	proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeVolumeUnitSetWritable, data, base.ProposeContext{ReqID: span.TraceID()})
	err = v.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		return errors.Info(err, "raft propose err").Detail(err)
	}

	return
}

func (v *VolumeMgr) LockVolume(ctx context.Context, vid proto.Vid) error {
	span := trace.SpanFromContextSafe(ctx)
	vol := v.all.getVol(vid)
	if vol == nil {
		span.Errorf("volume not found, vid: %d", vid)
		return apierrors.ErrVolumeNotExist
	}

	vol.lock.RLock()
	status := vol.getStatus()
	// volume already locked
	if status == proto.VolumeStatusLock {
		vol.lock.RUnlock()
		return nil
	}
	if !vol.canLock() {
		vol.lock.RUnlock()
		span.Warnf("can't lock volume, volume %d, current status(%d)", vid, status)
		return apierrors.ErrLockNotAllow
	}
	vol.lock.RUnlock()

	param := ChangeVolStatusCtx{
		Vid:      vid,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeLock,
	}
	data, err := json.Marshal(param)
	if err != nil {
		span.Errorf("json marshal failed, vid: %d, error: %v", vid, err)
		return apierrors.ErrCMUnexpect
	}

	proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeChangeVolumeStatus, data, base.ProposeContext{ReqID: span.TraceID()})
	err = v.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Errorf("raft propose error: %v", err)
		return apierrors.ErrRaftPropose
	}

	vol.lock.RLock()
	status = vol.getStatus()
	vol.lock.RUnlock()

	if status != proto.VolumeStatusLock {
		span.Errorf("volume %d status(%d) is not locked", vid, status)
		return apierrors.ErrCMUnexpect
	}

	return nil
}

func (v *VolumeMgr) UnlockVolume(ctx context.Context, vid proto.Vid) error {
	span := trace.SpanFromContextSafe(ctx)

	vol := v.all.getVol(vid)
	if vol == nil {
		span.Errorf("volume not found, vid: %d", vid)
		return apierrors.ErrVolumeNotExist
	}

	vol.lock.RLock()
	status := vol.getStatus()
	if status == proto.VolumeStatusUnlocking || status == proto.VolumeStatusIdle {
		vol.lock.RUnlock()
		return nil
	}

	if status == proto.VolumeStatusActive {
		vol.lock.RUnlock()
		span.Warnf("can't unlock volume, volume %d, current status(%d)", vid, vol.getStatus())
		return apierrors.ErrUnlockNotAllow
	}
	vol.lock.RUnlock()

	param := ChangeVolStatusCtx{
		Vid:      vid,
		TaskID:   uuid.New().String(),
		TaskType: base.VolumeTaskTypeUnlock,
	}
	data, err := json.Marshal(param)
	if err != nil {
		span.Errorf("json marshal failed, vid: %d, error: %v", vid, err)
		return apierrors.ErrCMUnexpect
	}

	proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeChangeVolumeStatus, data, base.ProposeContext{ReqID: span.TraceID()})
	err = v.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		span.Errorf("raft propose error:%v", err)
		return apierrors.ErrRaftPropose
	}

	return nil
}

func (v *VolumeMgr) Stat(ctx context.Context) (stat cm.VolumeStatInfo) {
	stat.TotalVolume = defaultVolumeStatusStat.StatTotal()
	statAllocatable := v.allocator.StatAllocatable()
	for _, count := range statAllocatable {
		stat.AllocatableVolume += count
	}
	statusNumM := defaultVolumeStatusStat.StatStatusNum()
	stat.ActiveVolume = statusNumM[proto.VolumeStatusActive]
	stat.IdleVolume = statusNumM[proto.VolumeStatusIdle]
	stat.LockVolume = statusNumM[proto.VolumeStatusLock]
	stat.UnlockingVolume = statusNumM[proto.VolumeStatusUnlocking]

	return
}

func (v *VolumeMgr) Report(ctx context.Context, region string, clusterID proto.ClusterID) {
	stat := v.Stat(ctx)
	v.reportVolStatusInfo(stat, region, clusterID)
}

func (v *VolumeMgr) applyRetainVolume(ctx context.Context, retainVolTokens []cm.RetainVolume) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply retain volume, retain tokens  is %#v", retainVolTokens)

	for _, retainVol := range retainVolTokens {
		_, vid, err := proto.DecodeToken(retainVol.Token)
		if err != nil {
			span.Errorf("token:%s decode error", retainVol.Token)
			return errors.Info(ErrInvalidToken, "decode retain token failed").Detail(ErrInvalidToken)
		}

		vol := v.all.getVol(vid)
		if vol == nil {
			span.Errorf("vid is nil,:%s decode error", retainVol.Token)
			return errors.Info(ErrVolumeNotExist, "get volume failed").Detail(ErrVolumeNotExist)
		}

		vol.lock.Lock()
		if vol.token == nil {
			vol.lock.Unlock()
			span.Errorf("volume not alloc,can not alloc:%d", vid)
			return ErrVolumeNotAlloc
		}

		tokenRecord := &volumedb.TokenRecord{
			Vid:        vid,
			TokenID:    retainVol.Token,
			ExpireTime: retainVol.ExpireTime,
		}
		err = v.volumeTbl.PutToken(vid, tokenRecord)
		if err != nil {
			vol.lock.Unlock()
			return errors.Info(err, "put token failed").Detail(err)
		}
		vol.token.expireTime = tokenRecord.ExpireTime
		vol.lock.Unlock()
	}

	span.Debugf("finish apply retain volume")

	return nil
}

func (v *VolumeMgr) applyAllocVolume(ctx context.Context, vid proto.Vid, host string, expireTime int64) (ret cm.AllocVolumeInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply alloc volume,vid is %d", vid)

	volume := v.all.getVol(vid)
	if volume == nil {
		span.Errorf("volume not exist,vid is %d", vid)
		return cm.AllocVolumeInfo{}, ErrVolumeNotExist
	}

	// ensure all raft node will receive create volume signal
	defer func() {
		select {
		case v.createVolChan <- struct{}{}:
			span.Debug("apply alloc volume finish, notify to if need create volume")
		default:
		}
	}()

	volume.lock.Lock()

	allocatableScoreThreshold := volume.getScoreThreshold()
	// when propose data, volume status may change , check to ensure volume can alloc,
	if !volume.canAlloc(v.AllocatableSize, allocatableScoreThreshold) {
		span.Warnf("volume can not alloc,volume info is %+v", volume.volInfoBase)
		volume.lock.Unlock()
		return
	}

	token := &token{
		vid:        volume.vid,
		tokenID:    proto.EncodeToken(host, volume.vid),
		expireTime: expireTime,
	}
	volume.token = token
	// set volume status into active, it'll call change status event function
	volume.setStatus(ctx, proto.VolumeStatusActive)
	volRecord := volume.ToRecord()
	tokenRecord := token.ToTokenRecord()
	err = v.volumeTbl.PutVolumeAndToken([]*volumedb.VolumeRecord{volRecord}, []*volumedb.TokenRecord{tokenRecord})
	if err != nil {
		volume.lock.Unlock()
		err = errors.Info(err, "put volume and tokenID in db error").Detail(err)
		return
	}
	volInfo := volume.ToVolumeInfo()
	ret = cm.AllocVolumeInfo{
		VolumeInfo: volInfo,
		Token:      volume.token.tokenID,
		ExpireTime: volume.token.expireTime,
	}

	volume.lock.Unlock()

	span.Debugf("finish apply alloc volume,vid is %d", vid)

	return
}

func (v *VolumeMgr) applyExpireVolume(ctx context.Context, vids []proto.Vid) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply expire volume, vids is %#v", vids)

	for _, vid := range vids {
		vol := v.all.getVol(vid)
		if vol == nil {
			span.Errorf("apply expired volume,vid %d not exist", vid)
			return ErrVolumeNotExist
		}
		// already been proceed, then just return success
		vol.lock.Lock()
		if vol.getStatus() == proto.VolumeStatusIdle {
			vol.lock.Unlock()
			continue
		}
		// double check if expired
		if !vol.isExpired() {
			vol.lock.Unlock()
			continue
		}

		span.Debugf("volume info:  %#v expired,token is %#v", vol.volInfoBase, vol.token)
		// set volume status idle, it'll call volume status change event function
		vol.setStatus(ctx, proto.VolumeStatusIdle)
		volRecord := vol.ToRecord()
		err = v.volumeTbl.PutVolumeRecord(volRecord)
		if err != nil {
			vol.lock.Unlock()
			return err
		}
		vol.lock.Unlock()
	}

	span.Debugf("finish apply expire volume, vids is %#v", vids)
	return
}

func (v *VolumeMgr) applyAdminUpdateVolume(ctx context.Context, volInfo *cm.VolumeInfoBase) error {
	span := trace.SpanFromContextSafe(ctx)
	vol := v.all.getVol(volInfo.Vid)
	if vol == nil {
		span.Errorf("apply admin update volume,vid %d not exist", volInfo.Vid)
		return ErrVolumeNotExist
	}
	vol.lock.Lock()
	vol.volInfoBase.Used = volInfo.Used
	vol.volInfoBase.Total = volInfo.Total
	vol.setFree(ctx, volInfo.Free)
	vol.setStatus(ctx, volInfo.Status)
	vol.setHealthScore(ctx, volInfo.HealthScore)
	volRecord := vol.ToRecord()
	err := v.volumeTbl.PutVolumeRecord(volRecord)
	vol.lock.Unlock()
	return err
}

func (v *VolumeMgr) applyAdminUpdateVolumeUnit(ctx context.Context, unitInfo *cm.AdminUpdateUnitArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	vol := v.all.getVol(unitInfo.Vuid.Vid())
	if vol == nil {
		span.Errorf("apply admin update volume unit,vid %d not exist", unitInfo.Vuid.Vid())
		return ErrVolumeNotExist
	}
	index := unitInfo.Vuid.Index()
	vol.lock.RLock()
	if int(index) >= len(vol.vUnits) {
		span.Errorf("apply admin update volume unit,index:%d over vuids length ", index)
		vol.lock.RUnlock()
		return ErrVolumeUnitNotExist
	}
	vol.lock.RUnlock()

	vol.lock.Lock()
	if proto.IsValidEpoch(unitInfo.Epoch) {
		vol.vUnits[index].epoch = unitInfo.Epoch
		vol.vUnits[index].vuInfo.Vuid = proto.EncodeVuid(vol.vUnits[index].vuidPrefix, unitInfo.Epoch)
	}
	if proto.IsValidEpoch(unitInfo.NextEpoch) {
		vol.vUnits[index].nextEpoch = unitInfo.NextEpoch
	}
	diskInfo, err := v.diskMgr.GetDiskInfo(ctx, unitInfo.DiskID)
	if err != nil {
		return err
	}
	vol.vUnits[index].vuInfo.DiskID = diskInfo.DiskID
	vol.vUnits[index].vuInfo.Host = diskInfo.Host
	vol.vUnits[index].vuInfo.Compacting = unitInfo.Compacting

	unitRecord := vol.vUnits[index].ToVolumeUnitRecord()
	err = v.volumeTbl.PutVolumeUnit(unitInfo.Vuid.VuidPrefix(), unitRecord)
	vol.lock.Unlock()
	return err
}

// only leader node can create volume and check expire volume
func (v *VolumeMgr) loop() {
	// notify to create volume

	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	if v.raftServer.IsLeader() {
		select {
		case v.createVolChan <- struct{}{}:
			span.Debugf("notify create volume")
		default:

		}
	}

	ticker := time.NewTicker(time.Second * time.Duration(v.CheckExpiredVolumeIntervalS))
	defer ticker.Stop()

	for {
		select {
		case <-v.createVolChan:
			// finish last create volume job firstly
			// return and wait for create volume channel if any failed
			if err := v.finishLastCreateJob(ctx); err != nil {
				span.Errorf("finish last create volume job failed ==> %s", errors.Detail(err))
				continue
			}
			if !v.raftServer.IsLeader() {
				continue
			}

			span_, ctx_ := trace.StartSpanFromContext(context.Background(), "")
			span_.Debug("leader node start create volume")

			allocatableVolCounts := v.allocator.StatAllocatable()
			diskNums := v.diskMgr.Stat(ctx_).TotalDisk

		CREATE:
			for _, modeConfig := range v.codeMode {
				// do not create new volume when enable is false
				if !modeConfig.enable {
					continue
				}

				count := v.getModeUnitCount(modeConfig.mode)
				minVolCount := int(float64(diskNums) * modeConfig.sizeRatio / float64(count))
				if minVolCount < v.MinAllocableVolumeCount {
					minVolCount = v.MinAllocableVolumeCount
				}

				curVolCount := allocatableVolCounts[modeConfig.mode]
				span_.Debugf("code mode %v,min allocatable volume count is %d, current count is %d", modeConfig.mode, v.MinAllocableVolumeCount, curVolCount)
				for i := curVolCount; i < minVolCount; i++ {
					select {
					case <-ctx.Done():
						break CREATE
					default:
					}

					err := v.createVolume(ctx, modeConfig.mode)
					if err != nil {
						span_.Errorf("create volume failed ==> %s", errors.Detail(err))
						break
					}
				}
			}
			// check expired volume
		case <-ticker.C:
			if !v.raftServer.IsLeader() {
				continue
			}

			span_, _ := trace.StartSpanFromContext(context.Background(), "")
			span_.Debug("start check expiredVolume")

			expiredVids := v.allocator.GetExpiredVolumes()
			if len(expiredVids) == 0 {
				continue
			}
			span_.Debugf("expired vids is %v", expiredVids)

			data, err := json.Marshal(expiredVids)
			if err != nil {
				span_.Errorf("json Marshal error:%v", err)
				continue
			}
			proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeExpireVolume, data, base.ProposeContext{ReqID: span.TraceID()})
			err = v.raftServer.Propose(ctx, proposeInfo)
			if err != nil {
				span_.Errorf("raft propose data error:%v", err)
				continue
			}
		case <-v.closeLoopChan:
			ctx.Done()
			return
		}
	}
}

// refreshHealth use for refreshing volume healthScore
// healthScore only correspond with writable volumeUnit num
func (v *VolumeMgr) refreshHealth(ctx context.Context, vid proto.Vid) error {
	span := trace.SpanFromContextSafe(ctx)
	vol := v.all.getVol(vid)
	if vol == nil {
		return ErrVolumeNotExist
	}
	health := 0
	badCount := 0

	vol.lock.RLock()
	for _, vu := range vol.vUnits {
		if vu.vuInfo.Compacting {
			span.Debugf("disk chunk is compacting, bad index increase. disk_id: %d, vuid: %d", vu.vuInfo.DiskID, vu.vuInfo.Vuid)
			badCount++
			continue
		}
		writable, err := v.diskMgr.IsDiskWritable(ctx, vu.vuInfo.DiskID)
		if err != nil {
			vol.lock.RUnlock()
			return err
		}
		if !writable {
			span.Debugf("disk is not writable, bad index increase. disk_id: %d", vu.vuInfo.DiskID)
			badCount++
		}
	}
	vol.lock.RUnlock()

	health -= badCount
	vol.lock.Lock()
	vol.setHealthScore(ctx, health)
	vol.lock.Unlock()
	return nil
}

func (v *VolumeMgr) getModeUnitCount(mode codemode.CodeMode) int {
	unitCount := v.codeMode[mode].tactic.N + v.codeMode[mode].tactic.M + v.codeMode[mode].tactic.L
	return unitCount
}
