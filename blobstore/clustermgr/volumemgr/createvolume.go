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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	IncreaseEpochInterval = 3

	defaulRetrySleepInterval = 1
)

// 1. get unfinished volume from transited table, finish last create volume job when current node id is equal to CreateByNodeID
// 2. increase epoch of volume and save into transited table(raft propose).
//    this step is an optimized operation to avoiding raft propose every retry alloc chunks,
//    so that we don't need to save current epoch of volume unit every retry alloc chunks
// 3. alloc chunks for volume
// 4. raft propose apply create volume if 3 step success
// 5. success and return
func (v *VolumeMgr) finishLastCreateJob(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	var volumeRecs []*volumedb.VolumeRecord
	v.transitedTbl.RangeVolume(func(rec *volumedb.VolumeRecord) error {
		if modeConf, ok := v.codeMode[rec.CodeMode]; ok {
			if !modeConf.enable {
				return nil
			}
		}
		if rec.CreateByNodeID == v.raftServer.Status().Id {
			volumeRecs = append(volumeRecs, rec)
		}
		return nil
	})
	for _, rec := range volumeRecs {
		span.Debugf("finish create volume job, volume: %+v", rec)
		unitRecs := make([]*volumedb.VolumeUnitRecord, 0, len(rec.VuidPrefixs))
		volumeUnits := make([]*clustermgr.VolumeUnitInfo, 0, len(rec.VuidPrefixs))
		for _, vuidPrefix := range rec.VuidPrefixs {
			unitRec, err := v.transitedTbl.GetVolumeUnit(vuidPrefix)
			if err != nil {
				return errors.Info(err, "get transited volume unit failed").Detail(err)
			}
			// must increase epoch of volume unit firstly
			unitRec.Epoch += IncreaseEpochInterval
			unitRecs = append(unitRecs, unitRec)
			volumeUnits = append(volumeUnits, volumeUnitRecordToVolumeUnit(unitRec).vuInfo)
		}
		// save volume units into transited table(raft propose)
		data, err := json.Marshal(unitRecs)
		if err != nil {
			return errors.Info(err, "marshal propose units data failed").Detail(err)
		}
		proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeIncreaseVolumeUnitsEpoch, data, base.ProposeContext{ReqID: span.TraceID()})
		if err := v.raftServer.Propose(ctx, proposeInfo); err != nil {
			return errors.Info(err, "raft propose increase volume units epoch failed").Detail(err)
		}

		createVolCtx := &CreateVolumeCtx{
			Vid:     rec.Vid,
			VuInfos: volumeUnits,
			VolInfo: volumeRecordToVolumeInfoBase(rec),
		}
		// alloc chunk for all units
		err = v.allocChunkForAllUnits(ctx, createVolCtx)
		if err != nil {
			return errors.Info(err, "alloc chunk for volume unit failed").Detail(err)
		}

		data, err = createVolCtx.Encode()
		if err != nil {
			return errors.Info(err, "encode create volume context failed").Detail(err)
		}
		proposeInfo = base.EncodeProposeInfo(v.GetModuleName(), OperTypeCreateVolume, data, base.ProposeContext{ReqID: span.TraceID()})
		if err := v.raftServer.Propose(ctx, proposeInfo); err != nil {
			return errors.Info(err, "raft propose create volume failed").Detail(err)
		}
	}
	return nil
}

// 1. alloc vid
// 2. initial volume unit basic info
// 3. save volume and unit info into transited table(raft propose)
// 4. alloc chunks for volume, return if failed (the rest jobs will be finished by finishLastCreateJob)
// 5. raft propose apply create volume if 4 step success
// 6. success and return
func (v *VolumeMgr) createVolume(ctx context.Context, mode codemode.CodeMode) error {
	span := trace.SpanFromContextSafe(ctx)
	_, newVid, err := v.scopeMgr.Alloc(ctx, vidScopeName, 1)
	if err != nil {
		return errors.Info(err, "scope alloc vid failed").Detail(err)
	}
	vid := proto.Vid(newVid)
	// check avoid vol already exist
	if oldVol := v.all.getVol(vid); oldVol != nil {
		return errors.Info(ErrCreateVolumeAlreadyExist, fmt.Sprintf("create volume vid:%d already exist,please check scopeMgr alloc", vid))
	}
	span, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", span.TraceID()+"/"+vid.ToString())

	unitCount := v.getModeUnitCount(mode)
	vuInfos := make([]*clustermgr.VolumeUnitInfo, unitCount)
	for index := 0; index < unitCount; index++ {
		vuid := proto.EncodeVuid(proto.EncodeVuidPrefix(vid, uint8(index)), proto.MinEpoch)
		vuInfos[index] = &clustermgr.VolumeUnitInfo{
			DiskID: proto.InvalidDiskID,
			Free:   v.ChunkSize,
			Total:  v.ChunkSize,
			Vuid:   vuid,
		}
	}

	volInfo := clustermgr.VolumeInfoBase{
		Vid:            vid,
		CodeMode:       mode,
		HealthScore:    healthiestScore,
		Status:         proto.VolumeStatusIdle,
		Free:           v.ChunkSize * uint64(v.codeMode[mode].tactic.N),
		Total:          v.ChunkSize * uint64(v.codeMode[mode].tactic.N),
		CreateByNodeID: v.raftServer.Status().Id,
	}
	createVolCtx := &CreateVolumeCtx{
		Vid:     vid,
		VuInfos: vuInfos,
		VolInfo: volInfo,
	}
	span.Debugf("create volume, code mode[%d], create volume context[%+v]", mode, createVolCtx)

	// save volume and unit info into transited table(raft propose)
	data, _ := createVolCtx.Encode()
	proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeInitCreateVolume, data, base.ProposeContext{ReqID: span.TraceID()})
	if err := v.raftServer.Propose(ctx, proposeInfo); err != nil {
		return errors.Info(err, fmt.Sprintf("raft propose initial create volume[%d] failed", vid)).Detail(err)
	}

	// alloc chunk for all units
	err = v.allocChunkForAllUnits(ctx, createVolCtx)
	if err != nil {
		return errors.Info(err, fmt.Sprintf("alloc chunk for volume[%d] unit failed", vid)).Detail(err)
	}
	span.Debugf("alloc chunk for unit success, volume context[%+v]", createVolCtx)

	data, err = createVolCtx.Encode()
	if err != nil {
		return errors.Info(err, fmt.Sprintf("encode create volume[%d] context failed", vid)).Detail(err)
	}
	proposeInfo = base.EncodeProposeInfo(v.GetModuleName(), OperTypeCreateVolume, data, base.ProposeContext{ReqID: span.TraceID()})
	if err := v.raftServer.Propose(ctx, proposeInfo); err != nil {
		return errors.Info(err, fmt.Sprintf("raft propose create volume[%d] failed", vid)).Detail(err)
	}

	return nil
}

func (v *VolumeMgr) applyInitCreateVolume(ctx context.Context, vol *volume) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply initial create volume, vid[%d]", vol.vid)

	if !vol.isValid() {
		return errors.Info(ErrInvalidVolume, "initial create volume is invalid, vid[%d], units[%qv]", vol.vid, vol.vUnits).Detail(ErrInvalidVolume)
	}

	// volume already create finish, direct return
	if v.all.getVol(vol.vid) != nil {
		return nil
	}

	unitRecords := volumeUnitsToVolumeUnitRecords(vol.vUnits)
	volumeRecord := vol.ToRecord()
	if err := v.transitedTbl.PutVolumeAndVolumeUnit(volumeRecord, unitRecords); err != nil {
		return errors.Info(err, fmt.Sprintf("put transited volume[%v] and volume unit[%+v] into volume table failed", volumeRecord, unitRecords)).Detail(err)
	}
	return nil
}

func (v *VolumeMgr) applyIncreaseVolumeUnitsEpoch(ctx context.Context, units []*volumedb.VolumeUnitRecord) error {
	return v.transitedTbl.PutVolumeUnits(units)
}

func (v *VolumeMgr) applyCreateVolume(ctx context.Context, vol *volume) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start apply create volume, vid[%d]", vol.vid)

	if !vol.isValid() {
		return errors.Info(ErrInvalidVolume, "create volume is invalid, vid[%d], units[%qv]", vol.vid, vol.vUnits).Detail(ErrInvalidVolume)
	}

	// already create, then return
	if v.all.getVol(vol.vid) != nil {
		return nil
	}

	vol.lock.Lock()
	defer vol.lock.Unlock()
	// set volume status into idle
	vol.setStatus(ctx, proto.VolumeStatusIdle)

	unitRecords := volumeUnitsToVolumeUnitRecords(vol.vUnits)
	volumeRecord := vol.ToRecord()
	// delete transited table firstly, put volume and volume units secondly.
	// it's idempotent when wal log replay
	if err := v.transitedTbl.DeleteVolumeAndUnits(volumeRecord, unitRecords); err != nil {
		return errors.Info(err, fmt.Sprintf("delete volume [%+v] and  units[%+v] from transited table failed", volumeRecord, unitRecords)).Detail(err)
	}
	if err := v.volumeTbl.PutVolumeAndVolumeUnit([]*volumedb.VolumeRecord{volumeRecord}, [][]*volumedb.VolumeUnitRecord{unitRecords}); err != nil {
		return errors.Info(err, fmt.Sprintf("put volume[%+v] and volume unit[%+v] into volume table failed", volumeRecord, unitRecords)).Detail(err)
	}
	v.all.putVol(vol)

	return nil
}

// alloc chunk for all volume units
func (v *VolumeMgr) allocChunkForAllUnits(ctx context.Context, vol *CreateVolumeCtx) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start alloc chunk for all units,volume is %d", vol.Vid)
	codeInfo, ok := v.codeMode[vol.VolInfo.CodeMode]
	if !ok {
		return errors.New("volumeMgr codeMode not set")
	}
	idcCnt := codeInfo.tactic.AZCount
	// codeMode EC15P12 return {{0, 1, 2, 3, 4, 15, 16, 17, 18}, {5, 6, 7, 8, 9, 19, 20, 21, 22}, {10, 11, 12, 13, 14, 23, 24, 25, 26}}
	idcIndexes := codeInfo.tactic.GetECLayoutByAZ()
	// random shuffle idcIndexs:avoid index{0, 1, 2, 3, 4, 15, 16, 17, 18} only alloc chunk in z0
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idcIndexes), func(i, j int) {
		idcIndexes[i], idcIndexes[j] = idcIndexes[j], idcIndexes[i]
	})
	span.Debugf("now idcIndexes is %#v", idcIndexes)

	availableIDC := make([]string, 0)
	for i := range v.IDC {
		if v.IDC[i] == v.UnavailableIDC {
			continue
		}
		availableIDC = append(availableIDC, v.IDC[i])
	}
	if len(availableIDC) != idcCnt {
		span.Errorf("available idc count:%d not match codeMode idc count:%d", len(availableIDC), idcCnt)
		return errors.New("available idc count not match codeMode idc count")
	}

	errChan := make(chan error, idcCnt)
	wg := sync.WaitGroup{}
	wg.Add(idcCnt)
	for i, indexs := range idcIndexes {
		idcVuInfos := make(map[proto.VuidPrefix]*clustermgr.VolumeUnitInfo)
		for _, index := range indexs {
			vuInfo := vol.VuInfos[index]
			idcVuInfos[vuInfo.Vuid.VuidPrefix()] = vol.VuInfos[index]
		}

		// alloc chunk for each idc volume unit
		span.Debugf("start alloc chunk for volume unit,volume is %#v", vol)
		go func(ctx context.Context, idc string, idcUnits map[proto.VuidPrefix]*clustermgr.VolumeUnitInfo) {
			defer wg.Done()
			err := v.allocChunkForIdcUnits(ctx, idc, idcVuInfos)
			span.Debugf("alloc chunk in idc:%v, error is %#v", idc, err)
			errChan <- err
		}(ctx, availableIDC[i], idcVuInfos)
	}
	wg.Wait()

	for i := 0; i < idcCnt; i++ {
		err = <-errChan
		if err != nil {
			return err
		}
	}

	return
}

// alloc chunk for each idc unit
func (v *VolumeMgr) allocChunkForIdcUnits(ctx context.Context, idc string, vuInfos map[proto.VuidPrefix]*clustermgr.VolumeUnitInfo) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	vuids := make([]proto.Vuid, 0, len(vuInfos))
	excludes := make([]proto.DiskID, 0)

	for _, vuInfo := range vuInfos {
		vuids = append(vuids, vuInfo.Vuid)
	}
	policy := &diskmgr.AllocPolicy{
		Idc:   idc,
		Vuids: vuids,
	}

	// Notice: retryTime should never large than IncreaseEpochInterval
	retryTime := IncreaseEpochInterval
	for i := 0; i < retryTime; i++ {
		var (
			disks     []proto.DiskID
			failVuids []proto.Vuid
		)
		disks, err = v.diskMgr.AllocChunks(ctx, policy)
		span.Debugf("alloc chunks, policy is %v, actives disk is %v, error is %v", policy, disks, err)
		// no enough space error return directly, do not retry.
		if err == diskmgr.ErrNoEnoughSpace {
			time.Sleep(defaulRetrySleepInterval * time.Second)
			return err
		}

		for i, vuid := range policy.Vuids {
			vuidPrefix := vuid.VuidPrefix()
			if disks[i] == proto.InvalidDiskID {
				newVuid := proto.EncodeVuid(vuidPrefix, vuid.Epoch()+1)
				failVuids = append(failVuids, newVuid)
				continue
			}
			diskInfo, err := v.diskMgr.GetDiskInfo(ctx, disks[i])
			if err != nil {
				span.Errorf("allocated disk ,get diskInfo [diskID:%d] error:%v", disks[i], err)
				return err
			}
			excludes = append(excludes, disks[i])
			vuInfos[vuidPrefix].DiskID = disks[i]
			vuInfos[vuidPrefix].Host = diskInfo.Host
			vuInfos[vuidPrefix].Vuid = vuid
		}

		if err == nil {
			break
		}
		policy.Excludes = excludes
		policy.Vuids = failVuids
	}

	return err
}
