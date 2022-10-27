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
	"strconv"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/configmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/diskmgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// VolumeMgrConfig defines volume manager configuration
type VolumeMgrConfig struct {
	LocalHost      string          `json:"local_host"`
	BlobNodeConfig blobnode.Config `json:"blob_node_config"`
	// volume database path,include volumeTbl vuidTbl tokenTbl indexTbl taskTbl
	VolumeDBPath   string                `json:"volume_db_path"`
	VolumeDBOption kvstore.RocksDBOption `json:"volume_db_option"`
	// retain successful volume add another  retainTimeS second
	RetainTimeS                 int `json:"retain_time_s"`
	RetainThreshold             int `json:"retain_threshold"`
	FlushIntervalS              int `json:"flush_interval_s"`
	CheckExpiredVolumeIntervalS int `json:"check_expired_volume_interval_s"`

	VolumeSliceMapNum            uint32 `json:"volume_slice_map_num"`
	ApplyConcurrency             uint32 `json:"apply_concurrency"`
	MinAllocableVolumeCount      int    `json:"min_allocable_volume_count"`
	AllocatableDiskLoadThreshold int    `json:"allocatable_disk_load_threshold"`

	// the volume in Proxy which free size small than FreezeThreshold treat filled
	FreezeThreshold uint64 `json:"-"`
	// the volume free size must big than AllocatableSize can alloc
	AllocatableSize  uint64            `json:"-"`
	IDC              []string          `json:"-"`
	UnavailableIDC   string            `json:"-"`
	ChunkSize        uint64            `json:"-"`
	CodeModePolicies []codemode.Policy `json:"-"`
	Region           string            `json:"-"`
	ClusterID        proto.ClusterID   `json:"-"`
}

func (c *VolumeMgrConfig) checkAndFix() {
	if c.RetainTimeS < 60 {
		c.RetainTimeS = defaultRetainTimeS
	}
	if c.RetainThreshold > 0 {
		c.RetainThreshold = defaultRetainThreshold
	}
	if c.FlushIntervalS <= 0 {
		c.FlushIntervalS = defaultFlushIntervalS
	}
	if c.VolumeSliceMapNum == 0 {
		c.VolumeSliceMapNum = defaultVolumeSliceMapNum
	}
	if c.ApplyConcurrency == 0 {
		c.ApplyConcurrency = defaultApplyConcurrency
	}
	if c.MinAllocableVolumeCount <= 0 {
		c.MinAllocableVolumeCount = defaultMinAllocatableVolumeCount
	}
	if c.CheckExpiredVolumeIntervalS <= 0 {
		c.CheckExpiredVolumeIntervalS = defaultCheckExpiredVolumeIntervalS
	}
	if c.AllocatableDiskLoadThreshold <= 0 {
		c.AllocatableDiskLoadThreshold = NoDiskLoadThreshold
	}
	if c.AllocatableSize <= 0 {
		c.AllocatableSize = defaultAllocatableSize
	}
}

// NewVolumeMgr constructs a new volume manager.
func NewVolumeMgr(conf VolumeMgrConfig, diskMgr diskmgr.DiskMgrAPI, scopeMgr scopemgr.ScopeMgrAPI,
	configMgr configmgr.ConfigMgrAPI, volumeDB kvstore.KVStore) (*VolumeMgr, error,
) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "new-volume-mgr")
	conf.checkAndFix()
	volumeTable, err := volumedb.OpenVolumeTable(volumeDB)
	if err != nil {
		return nil, errors.Info(err, "open volume table failed").Detail(err)
	}
	transitedTable, err := volumedb.OpenTransitedTable(volumeDB)
	if err != nil {
		return nil, errors.Info(err, "open transited table failed").Detail(err)
	}
	freezeSize, err := configMgr.Get(ctx, proto.VolumeReserveSizeKey)
	if err != nil {
		return nil, errors.Info(err, "get volume reserve size from config manager failed").Detail(err)
	}
	conf.FreezeThreshold, err = strconv.ParseUint(freezeSize, 10, 64)
	if err != nil {
		return nil, errors.Info(err, "parse volume reserve size failed").Detail(err)
	}

	// initial volumeMgr
	volumeMgr := &VolumeMgr{
		all:             newShardedVolumes(conf.VolumeSliceMapNum),
		volumeTbl:       volumeTable,
		transitedTbl:    transitedTable,
		createVolChan:   make(chan struct{}, 1),
		closeLoopChan:   make(chan struct{}, 1),
		codeMode:        make(map[codemode.CodeMode]codeModeConf),
		taskMgr:         newTaskManager(10),
		applyTaskPool:   base.NewTaskDistribution(int(conf.ApplyConcurrency), 1),
		diskMgr:         diskMgr,
		scopeMgr:        scopeMgr,
		configMgr:       configMgr,
		blobNodeClient:  blobnode.New(&conf.BlobNodeConfig),
		VolumeMgrConfig: conf,
	}

	for _, policy := range conf.CodeModePolicies {
		codeMode := policy.ModeName.GetCodeMode()
		modeConf := codeModeConf{
			mode:      codeMode,
			sizeRatio: policy.SizeRatio,
			tactic:    codeMode.Tactic(),
			enable:    policy.Enable,
		}
		volumeMgr.codeMode[codeMode] = modeConf
	}
	allocConfig := allocConfig{
		codeModes:                    volumeMgr.codeMode,
		allocatableSize:              conf.AllocatableSize,
		allocatableDiskLoadThreshold: conf.AllocatableDiskLoadThreshold,
	}
	volAllocator := newVolumeAllocator(allocConfig)
	volumeMgr.allocator = volAllocator

	// initial register change status callback func
	// idle status volume will call volume allocator.VolumeStatusIdleCallback
	defaultVolumeNotifyQueue.Add(volStatusNottifyKeyPrefix+proto.VolumeStatusIdle.String(), volAllocator.VolumeStatusIdleCallback)
	// active status volume will call volume allocator.VolumeStatusActiveCallback
	defaultVolumeNotifyQueue.Add(volStatusNottifyKeyPrefix+proto.VolumeStatusActive.String(), volAllocator.VolumeStatusActiveCallback)
	// lock status volume will call volume allocator.VolumeStatusLockCallback
	defaultVolumeNotifyQueue.Add(volStatusNottifyKeyPrefix+proto.VolumeStatusLock.String(), volAllocator.VolumeStatusLockCallback)
	// volume free size or volume health change will call volume allocator.VolumeFreeHealthCallback
	defaultVolumeNotifyQueue.Add(VolFreeHealthChangeNotifyKey, volAllocator.VolumeFreeHealthCallback)

	// initial dirty volumes
	volumeMgr.dirty.Store(newShardedVolumes(conf.VolumeSliceMapNum))

	// initial load data
	if err := volumeMgr.LoadData(ctx); err != nil {
		return nil, err
	}

	return volumeMgr, nil
}

func (v *VolumeMgr) SetRaftServer(raftServer raftserver.RaftServer) {
	v.raftServer = raftServer
}

func (v *VolumeMgr) Start() {
	go v.taskLoop()
	go v.loop()
}

func (v *VolumeMgr) loadVolume(ctx context.Context) error {
	return v.volumeTbl.RangeVolumeRecord(func(volRecord *volumedb.VolumeRecord) error {
		// volumeUnits use for internal
		var volumeUnits []*volumeUnit

		if _, ok := v.codeMode[volRecord.CodeMode]; !ok {
			return errors.New("codeMode not exist in config")
		}

		for _, vuidPrefix := range volRecord.VuidPrefixs {
			unitRecord, err := v.volumeTbl.GetVolumeUnit(vuidPrefix)
			if err != nil {
				log.Errorf("get volume unit error:%v", err)
				return err
			}
			diskInfo, err := v.diskMgr.GetDiskInfo(ctx, unitRecord.DiskID)
			if err != nil {
				log.Errorf("get [diskid:%d]disk info error:%v", unitRecord.DiskID, err)
				return err
			}
			vUnit := volumeUnitRecordToVolumeUnit(unitRecord)
			vUnit.vuInfo.Host = diskInfo.Host
			volumeUnits = append(volumeUnits, vUnit)
		}

		volInfo := volumeRecordToVolumeInfoBase(volRecord)
		volume := &volume{
			vid:         volRecord.Vid,
			vUnits:      volumeUnits,
			volInfoBase: volInfo,
		}
		tokenRecord, _ := v.volumeTbl.GetToken(volRecord.Vid)
		if tokenRecord != nil {
			token := tokenRecordToToken(tokenRecord)
			volume.token = token
		}

		v.all.putVol(volume)

		// refresh volume health
		err := v.refreshHealth(ctx, volRecord.Vid)
		if err != nil {
			log.Errorf("refresh HealthScore error:%v", err)
			return err
		}
		// set volume status same with volume record' status.
		// it will call change volume status event function
		volume.setStatus(ctx, volRecord.Status)
		return err
	})
}

func (v *VolumeMgr) Close() {
	close(v.closeLoopChan)
}
