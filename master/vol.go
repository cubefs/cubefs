// Copyright 2018 The Chubao Authors.
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

package master

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	stringutil "github.com/chubaofs/chubaofs/util/string"
	"github.com/chubaofs/chubaofs/util/unit"
)

// Vol represents a set of meta partitionMap and data partitionMap
type Vol struct {
	ID                   uint64
	Name                 string
	Owner                string
	OSSAccessKey         string
	OSSSecretKey         string
	OSSBucketPolicy      proto.BucketAccessPolicy
	dpReplicaNum         uint8
	mpReplicaNum         uint8
	dpLearnerNum         uint8
	mpLearnerNum         uint8
	Status               uint8
	mpMemUsageThreshold  float32
	dataPartitionSize    uint64
	Capacity             uint64 // GB
	NeedToLowerReplica   bool
	FollowerRead         bool
	FollowerReadDelayCfg proto.DpFollowerReadDelayConfig //sec
	FollReadHostWeight   int
	NearRead             bool
	ForceROW             bool
	forceRowModifyTime   int64
	enableWriteCache     bool
	authenticate         bool
	autoRepair           bool
	zoneName             string
	crossZone            bool
	CrossRegionHAType    proto.CrossRegionHAType
	enableToken          bool
	tokens               map[string]*proto.Token
	tokensLock           sync.RWMutex
	MetaPartitions       map[uint64]*MetaPartition `graphql:"-"`
	mpsLock              sync.RWMutex
	dataPartitions       *DataPartitionMap
	mpsCache             []byte
	viewCache            []byte
	createDpMutex        sync.RWMutex
	createMpMutex        sync.RWMutex
	createTime           int64
	dpWriteableThreshold float64
	description          string
	dpSelectorName       string
	dpSelectorParm       string
	DPConvertMode        proto.ConvertMode
	MPConvertMode        proto.ConvertMode
	volWriteMutexEnable  bool
	volWriteMutex        sync.Mutex
	volWriteMutexClient  string
	ExtentCacheExpireSec int64
	writableMpCount      int64
	MinWritableMPNum     int
	MinWritableDPNum     int
	trashRemainingDays   uint32
	convertState         proto.VolConvertState
	DefaultStoreMode     proto.StoreMode
	MpLayout             proto.MetaPartitionLayout
	isSmart              bool
	smartEnableTime      int64
	smartRules           []string
	CreateStatus         proto.VolCreateStatus
	compactTag           proto.CompactTag
	compactTagModifyTime int64
	EcEnable             bool
	EcDataNum            uint8
	EcParityNum          uint8
	EcMigrationWaitTime  int64
	EcMigrationSaveTime  int64
	EcMigrationTimeOut   int64
	EcMigrationRetryWait int64
	EcMaxUnitSize        uint64
	ecDataPartitions     *EcDataPartitionCache
	ChildFileMaxCount    uint32
	TrashCleanInterval   uint64
	sync.RWMutex
}

func newVol(id uint64, name, owner, zoneName string, dpSize, capacity uint64, dpReplicaNum, mpReplicaNum uint8,
	followerRead, authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW, isSmart, enableWriteCache bool, createTime, smartEnableTime int64, description, dpSelectorName,
	dpSelectorParm string, crossRegionHAType proto.CrossRegionHAType, dpLearnerNum, mpLearnerNum uint8, dpWriteableThreshold float64, trashDays, childFileMaxCnt uint32,
	defStoreMode proto.StoreMode, convertSt proto.VolConvertState, mpLayout proto.MetaPartitionLayout, smartRules []string, compactTag proto.CompactTag, dpFolReadDelayCfg proto.DpFollowerReadDelayConfig) (vol *Vol) {
	vol = &Vol{ID: id, Name: name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}
	vol.dataPartitions = newDataPartitionMap(name)
	vol.ecDataPartitions = newEcDataPartitionCache(vol)
	if dpReplicaNum < defaultReplicaNum {
		dpReplicaNum = defaultReplicaNum
	}
	vol.dpReplicaNum = dpReplicaNum
	vol.mpMemUsageThreshold = defaultMetaPartitionMemUsageThreshold
	if mpReplicaNum < defaultReplicaNum {
		mpReplicaNum = defaultReplicaNum
	}
	vol.mpReplicaNum = mpReplicaNum
	vol.Owner = owner
	if dpSize == 0 {
		dpSize = unit.DefaultDataPartitionSize
	}
	if dpSize < unit.GB {
		dpSize = unit.DefaultDataPartitionSize
	}
	vol.ExtentCacheExpireSec = defaultExtentCacheExpireSec
	vol.MinWritableMPNum = defaultVolMinWritableMPNum
	vol.MinWritableDPNum = defaultVolMinWritableDPNum

	if trashDays > maxTrashRemainingDays {
		trashDays = maxTrashRemainingDays
	}
	vol.dataPartitionSize = dpSize
	vol.Capacity = capacity
	vol.FollowerRead = followerRead
	vol.ForceROW = forceROW
	vol.enableWriteCache = enableWriteCache
	vol.authenticate = authenticate
	vol.zoneName = zoneName
	vol.viewCache = make([]byte, 0)
	vol.mpsCache = make([]byte, 0)
	vol.createTime = createTime
	vol.enableToken = enableToken
	vol.autoRepair = autoRepair
	vol.volWriteMutexEnable = volWriteMutexEnable
	vol.tokens = make(map[string]*proto.Token, 0)
	vol.description = description
	vol.dpSelectorName = dpSelectorName
	vol.dpSelectorParm = dpSelectorParm
	vol.dpWriteableThreshold = dpWriteableThreshold
	vol.CrossRegionHAType = crossRegionHAType
	vol.dpLearnerNum = dpLearnerNum
	vol.mpLearnerNum = mpLearnerNum
	vol.trashRemainingDays = trashDays
	vol.convertState = convertSt
	if defStoreMode == proto.StoreModeDef {
		defStoreMode = proto.StoreModeMem
	}
	vol.DefaultStoreMode = defStoreMode
	vol.MpLayout = mpLayout
	vol.isSmart = isSmart
	vol.smartEnableTime = smartEnableTime
	if smartRules != nil {
		vol.smartRules = smartRules
	}
	vol.compactTag = compactTag
	vol.FollowerReadDelayCfg = dpFolReadDelayCfg
	vol.FollReadHostWeight = defaultFollReadHostWeight

	vol.EcDataNum = defaultEcDataNum
	vol.EcParityNum = defaultEcParityNum
	vol.EcEnable = defaultEcEnable
	vol.EcMigrationWaitTime = defaultEcMigrationWaitTime
	vol.EcMigrationSaveTime = defaultEcMigrationSaveTime
	vol.EcMigrationTimeOut = defaultEcMigrationTimeOut
	vol.EcMigrationRetryWait = defaultEcMigrationRetryWait
	vol.EcMaxUnitSize = defaultEcMaxUnitSize
	vol.ChildFileMaxCount = childFileMaxCnt
	return
}

func newVolFromVolValue(vv *volValue) (vol *Vol) {
	vol = newVol(
		vv.ID,
		vv.Name,
		vv.Owner,
		vv.ZoneName,
		vv.DataPartitionSize,
		vv.Capacity,
		vv.DpReplicaNum,
		vv.ReplicaNum,
		vv.FollowerRead,
		vv.Authenticate,
		vv.EnableToken,
		vv.AutoRepair,
		vv.VolWriteMutexEnable,
		vv.ForceROW,
		vv.IsSmart,
		vv.EnableWriteCache,
		vv.CreateTime,
		vv.SmartEnableTime,
		vv.Description,
		vv.DpSelectorName,
		vv.DpSelectorParm,
		vv.CrossRegionHAType,
		vv.DpLearnerNum,
		vv.MpLearnerNum,
		vv.DpWriteableThreshold,
		vv.TrashRemainingDays,
		vv.ChildFileMaxCnt,
		vv.DefStoreMode,
		vv.ConverState,
		vv.MpLayout,
		vv.SmartRules,
		vv.CompactTag,
		vv.FollowerReadDelayCfg)
	// overwrite oss secure
	vol.OSSAccessKey, vol.OSSSecretKey = vv.OSSAccessKey, vv.OSSSecretKey
	vol.Status = vv.Status
	vol.crossZone = vv.CrossZone
	vol.OSSBucketPolicy = vv.OSSBucketPolicy
	vol.DPConvertMode = vv.DPConvertMode
	vol.MPConvertMode = vv.MPConvertMode
	vol.ExtentCacheExpireSec = vv.ExtentCacheExpireSec
	vol.volWriteMutexClient = vv.VolWriteMutexClient
	vol.MinWritableMPNum = vv.MinWritableMPNum
	vol.MinWritableDPNum = vv.MinWritableDPNum
	vol.NearRead = vv.NearRead
	vol.EcMigrationSaveTime = vv.EcSaveTime
	vol.EcMigrationWaitTime = vv.EcWaitTime
	vol.EcMigrationRetryWait = vv.EcRetryWait
	vol.EcMigrationTimeOut = vv.EcTimeOut
	vol.EcMaxUnitSize = vv.EcMaxUnitSize
	vol.EcDataNum = vv.EcDataNum
	vol.EcParityNum = vv.EcParityNum
	vol.EcEnable = vv.EcEnable
	if vol.EcDataNum == 0 || vol.EcParityNum == 0 {
		vol.EcDataNum = defaultEcDataNum
		vol.EcParityNum = defaultEcParityNum
	}
	if vol.EcMigrationSaveTime == 0 {
		vol.EcMigrationSaveTime = defaultEcMigrationSaveTime
	}
	if vol.EcMigrationWaitTime == 0 {
		vol.EcMigrationWaitTime = defaultEcMigrationWaitTime
	}
	if vol.EcMigrationRetryWait == 0 {
		vol.EcMigrationRetryWait = defaultEcMigrationRetryWait
	}
	if vol.EcMigrationTimeOut == 0 {
		vol.EcMigrationTimeOut = defaultEcMigrationTimeOut
	}
	if vol.EcMaxUnitSize == 0 {
		vol.EcMaxUnitSize = defaultEcMaxUnitSize
	}
	vol.forceRowModifyTime = vv.ForceRowModifyTime
	vol.compactTagModifyTime = vv.CompactTagModifyTime
	vol.TrashCleanInterval = vv.TrashCleanInterval
	return vol
}

func (vol *Vol) refreshOSSSecure() (key, secret string) {
	vol.OSSAccessKey = stringutil.RandomString(16, stringutil.Numeric|stringutil.LowerLetter|stringutil.UpperLetter)
	vol.OSSSecretKey = stringutil.RandomString(32, stringutil.Numeric|stringutil.LowerLetter|stringutil.UpperLetter)
	return vol.OSSAccessKey, vol.OSSSecretKey
}

func (vol *Vol) getToken(token string) (tokenObj *proto.Token, err error) {
	vol.tokensLock.Lock()
	defer vol.tokensLock.Unlock()
	tokenObj, ok := vol.tokens[token]
	if !ok {
		return nil, proto.ErrTokenNotFound
	}
	return
}

func (vol *Vol) deleteToken(token string) {
	vol.tokensLock.RLock()
	defer vol.tokensLock.RUnlock()
	delete(vol.tokens, token)
}

func (vol *Vol) putToken(token *proto.Token) {
	vol.tokensLock.Lock()
	defer vol.tokensLock.Unlock()
	vol.tokens[token.Value] = token
	return
}

func (vol *Vol) addMetaPartition(mp *MetaPartition) {
	vol.mpsLock.Lock()
	defer vol.mpsLock.Unlock()
	if _, ok := vol.MetaPartitions[mp.PartitionID]; !ok {
		vol.MetaPartitions[mp.PartitionID] = mp
		return
	}
	// replace the old partition in the map with mp
	vol.MetaPartitions[mp.PartitionID] = mp
}

func (vol *Vol) allMetaPartition() []*MetaPartition {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	result := make([]*MetaPartition, 0, len(vol.MetaPartitions))
	for _, mp := range vol.MetaPartitions {
		result = append(result, mp)
	}
	return result
}

func (vol *Vol) allDataPartition() []*DataPartition {
	dps := vol.dataPartitions
	dps.RLock()
	defer dps.RUnlock()
	return append([]*DataPartition{}, dps.partitions...)
}

func (vol *Vol) metaPartition(partitionID uint64) (mp *MetaPartition, err error) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	mp, ok := vol.MetaPartitions[partitionID]
	if !ok {
		err = proto.ErrMetaPartitionNotExists
	}
	return
}

func (vol *Vol) maxPartitionID() (maxPartitionID uint64) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for id := range vol.MetaPartitions {
		if id > maxPartitionID {
			maxPartitionID = id
		}
	}
	return
}

func (vol *Vol) getMpCnt() (mpCnt int) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	mpCnt = len(vol.MetaPartitions)
	return
}

func (vol *Vol) getDpCnt() (dpCnt int) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	dpCnt = len(vol.dataPartitions.partitionMap)
	return
}

func (vol *Vol) getDataPartitionsView() (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(vol.ecDataPartitions, false, 0)
}

func (vol *Vol) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return vol.dataPartitions.get(partitionID)
}

func (vol *Vol) initMetaPartitions(c *Cluster, count int) (err error) {
	// initialize k meta partitionMap at a time
	var (
		start uint64
		end   uint64
	)
	if count < defaultInitMetaPartitionCount {
		count = defaultInitMetaPartitionCount
	}
	if count > defaultMaxInitMetaPartitionCount {
		count = defaultMaxInitMetaPartitionCount
	}
	for index := 0; index < count; index++ {
		if index != 0 {
			start = end + 1
		}
		end = defaultMetaPartitionInodeIDStep * uint64(index+1)
		if index == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}
		if err = vol.createMetaPartition(c, start, end); err != nil {
			log.LogErrorf("action[initMetaPartitions] vol[%v] init meta partition err[%v]", vol.Name, err)
			break
		}
	}
	vol.setWritableMpCount(int64(count))
	mpCount := vol.getMpCnt()
	if mpCount != count {
		err = fmt.Errorf("action[initMetaPartitions] vol[%v] init meta partition failed,mpCount[%v],expectCount[%v],err[%v]",
			vol.Name, mpCount, count, err)
	}
	return
}

func (vol *Vol) initDataPartitions(c *Cluster) (err error) {
	// initialize k data partitionMap at a time
	var wg sync.WaitGroup
	errChannel := make(chan error, defaultInitDataPartitionCnt)
	for i := 0; i < defaultInitDataPartitionCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.DisableAutoAllocate {
				return
			}
			if _, err = c.createDataPartition(vol.Name, ""); err != nil {
				log.LogErrorf("action[batchCreateDataPartition] after create [%v] data partition,occurred error,err[%v]", i, err)
				errChannel <- err
			}
		}()
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return
	default:
	}
	return
}

func (vol *Vol) checkDataPartitions(c *Cluster) (cnt int, dataNodeBadDisksOfVol map[string][]string) {
	dataNodeBadDisksOfVol = make(map[string][]string, 0)
	if vol.getDpCnt() == 0 && vol.Status != proto.VolStMarkDelete {
		c.batchCreateDataPartition(vol, 1, "")
	}
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitionMap {
		dp.checkReplicaStatus(c.Name, c.cfg.DataPartitionTimeOutSec)
		dp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec, vol.dpWriteableThreshold, vol.CrossRegionHAType, c, vol.getDataPartitionQuorum())
		dp.checkLeader(c.cfg.DataPartitionTimeOutSec)
		dp.checkMissingReplicas(c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		dp.checkReplicaNumAndSize(c, vol)
		if dp.Status == proto.ReadWrite && !dp.isFrozen() {
			cnt++
		}
		diskErrorAddrs := dp.checkDiskError(c.Name, c.leaderInfo.addr)
		for addr, diskPath := range diskErrorAddrs {
			dataNodeBadDisksOfVol[addr] = append(dataNodeBadDisksOfVol[addr], diskPath)
		}
		dp.checkReplicationTask(c, vol.dataPartitionSize, int(vol.dpReplicaNum))
	}
	return
}

func (vol *Vol) loadDataPartition(c *Cluster) {
	partitions, startIndex := vol.dataPartitions.getDataPartitionsToBeChecked(c.cfg.PeriodToLoadALLDataPartitions)
	if len(partitions) == 0 {
		return
	}
	c.waitForResponseToLoadDataPartition(partitions)
	msg := fmt.Sprintf("action[loadDataPartition] vol[%v],checkStartIndex:%v checkCount:%v",
		vol.Name, startIndex, len(partitions))
	log.LogInfo(msg)
}

func (vol *Vol) releaseDataPartitions(releaseCount int, afterLoadSeconds int64) {
	partitions, startIndex := vol.dataPartitions.getDataPartitionsToBeReleased(releaseCount, afterLoadSeconds)
	if len(partitions) == 0 {
		return
	}
	vol.dataPartitions.freeMemOccupiedByDataPartitions(partitions)
	msg := fmt.Sprintf("action[freeMemOccupiedByDataPartitions] vol[%v] release data partition start:%v releaseCount:%v",
		vol.Name, startIndex, len(partitions))
	log.LogInfo(msg)
}

func (vol *Vol) checkReplicaNum(c *Cluster) {
	if !vol.NeedToLowerReplica {
		return
	}
	var err error
	dps := vol.cloneDataPartitionMap()
	for _, dp := range dps {
		host := dp.getToBeDecommissionHost(int(vol.dpReplicaNum + vol.dpLearnerNum))
		if host == "" {
			continue
		}
		if err = dp.removeOneReplicaByHost(c, host); err != nil {
			log.LogErrorf("action[checkReplicaNum],vol[%v],err[%v]", vol.Name, err)
			continue
		}
	}

	vol.checkEcReplicaNum(c)

	vol.NeedToLowerReplica = false
}
func (vol *Vol) checkRepairMetaPartitions(c *Cluster) {
	var err error
	mps := vol.cloneMetaPartitionMap()
	for _, mp := range mps {
		if err = mp.RepairZone(vol, c); err != nil {
			log.LogErrorf("action[checkRepairMetaPartitions],vol[%v],partitionID[%v],err[%v]", vol.Name, mp.PartitionID, err)
			continue
		}
	}
}

func (vol *Vol) checkRepairDataPartitions(c *Cluster) {
	var err error
	dps := vol.cloneDataPartitionMap()
	for _, dp := range dps {
		if err = dp.RepairZone(vol, c); err != nil {
			log.LogErrorf("action[checkRepairDataPartitions],vol[%v],partitionID[%v],err[%v]", vol.Name, dp.PartitionID, err)
			continue
		}
	}
}

func (vol *Vol) checkMetaPartitions(c *Cluster) (writableMpCount int) {
	vol.checkSplitMetaPartition(c)
	maxPartitionID := vol.maxPartitionID()
	mps := vol.cloneMetaPartitionMap()
	var (
		doSplit bool
		err     error
	)
	for _, mp := range mps {
		doSplit = mp.checkStatus(c.Name, true, int(vol.mpReplicaNum), maxPartitionID)
		if doSplit && mp.MaxInodeID != 0 {
			nextStart := mp.MaxInodeID + defaultMetaPartitionInodeIDStep
			if err = vol.splitMetaPartition(c, mp, nextStart); err != nil {
				Warn(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]", c.Name, vol.Name, mp.PartitionID, err))
			}
		}

		mp.checkLeader()
		mp.checkReplicaNum(c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(c, maxPartitionID)
		mp.reportMissingReplicas(c.Name, c.leaderInfo.addr, defaultMetaPartitionTimeOutSec, defaultIntervalToAlarmMissingMetaPartition)
		mp.replicaCreationTasks(c, vol.Name)
		if mp.Status == proto.ReadWrite {
			writableMpCount++
		}
	}
	return
}

func (vol *Vol) checkSplitMetaPartition(c *Cluster) {
	maxPartitionID := vol.maxPartitionID()
	partition, ok := vol.MetaPartitions[maxPartitionID]
	if !ok {
		return
	}
	liveReplicas := partition.getLiveReplicas()
	foundReadonlyReplica := false
	var readonlyReplica *MetaReplica
	for _, replica := range liveReplicas {
		if replica.Status == proto.ReadOnly {
			foundReadonlyReplica = true
			readonlyReplica = replica
			break
		}
	}
	if !foundReadonlyReplica {
		return
	}
	if readonlyReplica.metaNode.isWritable(proto.StoreModeMem) {
		msg := fmt.Sprintf("action[checkSplitMetaPartition] vol[%v],max meta parition[%v] status is readonly\n",
			vol.Name, partition.PartitionID)
		Warn(c.Name, msg)
		return
	}
	end := partition.MaxInodeID + defaultMetaPartitionInodeIDStep
	if err := vol.splitMetaPartition(c, partition, end); err != nil {
		msg := fmt.Sprintf("action[checkSplitMetaPartition],split meta partition[%v] failed,err[%v]\n",
			partition.PartitionID, err)
		Warn(c.Name, msg)
	}
}

func (vol *Vol) cloneMetaPartitionMap() (mps map[uint64]*MetaPartition) {
	mps = make(map[uint64]*MetaPartition, 0)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		mps[mp.PartitionID] = mp
	}
	return
}

func (vol *Vol) cloneDataPartitionMap() (dps map[uint64]*DataPartition) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	dps = make(map[uint64]*DataPartition, 0)
	for _, dp := range vol.dataPartitions.partitionMap {
		dps[dp.PartitionID] = dp
	}
	return
}

func (vol *Vol) setStatus(status uint8) {
	vol.Lock()
	defer vol.Unlock()
	vol.Status = status
}

func (vol *Vol) status() uint8 {
	vol.RLock()
	defer vol.RUnlock()
	return vol.Status
}

func (vol *Vol) capacity() uint64 {
	vol.RLock()
	defer vol.RUnlock()
	return vol.Capacity
}

func (vol *Vol) compact() uint8 {
	vol.RLock()
	defer vol.RUnlock()
	return uint8(vol.compactTag)
}

func (vol *Vol) checkAutoDataPartitionCreation(c *Cluster) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkAutoDataPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoDataPartitionCreation occurred panic")
		}
	}()
	if vol.status() == proto.VolStMarkDelete {
		return
	}
	if vol.capacity() == 0 {
		return
	}
	usedSpace := vol.totalUsedSpace() / unit.GB
	if usedSpace >= vol.capacity() {
		vol.setAllDataPartitionsToReadOnly()
		return
	}
	vol.setStatus(proto.VolStNormal)

	if vol.status() == proto.VolStNormal && !c.DisableAutoAllocate {
		vol.autoCreateDataPartitions(c)
	}
}

func (vol *Vol) autoCreateDataPartitions(c *Cluster) {
	if (vol.Capacity > 200000 && vol.dataPartitions.readableAndWritableCnt < 200) || vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions ||
		vol.dataPartitions.readableAndWritableCnt < vol.MinWritableDPNum {
		count := vol.calculateExpansionNum()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(vol, count, "")
	}
}

// Calculate the expansion number (the number of data partitions to be allocated to the given volume)
func (vol *Vol) calculateExpansionNum() (count int) {
	lackCount := float64(vol.MinWritableDPNum - vol.dataPartitions.readableAndWritableCnt)
	c := float64(vol.Capacity) * float64(volExpansionRatio) * float64(unit.GB) / float64(unit.DefaultDataPartitionSize)
	if lackCount > c {
		c = lackCount
	}
	switch {
	case c < minNumOfRWDataPartitions:
		count = minNumOfRWDataPartitions
	case c > maxNumberOfDataPartitionsForExpansion:
		count = maxNumberOfDataPartitionsForExpansion
	default:
		count = int(c)
	}
	return
}

func (vol *Vol) setAllDataPartitionsToReadOnly() {
	vol.dataPartitions.setAllDataPartitionsToReadOnly()
}

func (vol *Vol) totalUsedSpace() uint64 {
	totalUsed := vol.ecDataPartitions.totalUsedSpace() + vol.dataPartitions.totalUsedSpace()
	return totalUsed
}

func (vol *Vol) updateViewCache(c *Cluster) {
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.isSmart, vol.createTime)
	view.ForceROW = vol.ForceROW
	view.EnableWriteCache = vol.enableWriteCache
	view.CrossRegionHAType = vol.CrossRegionHAType
	view.SetOwner(vol.Owner)
	view.SetSmartRules(vol.smartRules)
	view.SetSmartEnableTime(vol.smartEnableTime)
	view.SetOSSSecure(vol.OSSAccessKey, vol.OSSSecretKey)
	view.SetOSSBucketPolicy(vol.OSSBucketPolicy)
	mpViews := vol.getMetaPartitionsView()
	view.MetaPartitions = mpViews
	mpViewsReply := newSuccessHTTPReply(mpViews)
	mpsBody, err := json.Marshal(mpViewsReply)
	if err != nil {
		log.LogErrorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setMpsCache(mpsBody)
	dpResps := vol.dataPartitions.getDataPartitionsView(vol.ecDataPartitions, 0)
	ecResps := vol.ecDataPartitions.getEcPartitionsView(0)
	view.DataPartitions = dpResps
	view.EcPartitions = ecResps
	viewReply := newSuccessHTTPReply(view)
	body, err := json.Marshal(viewReply)
	if err != nil {
		log.LogErrorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setViewCache(body)
}

func (vol *Vol) getMetaPartitionsView() (mpViews []*proto.MetaPartitionView) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	mpViews = make([]*proto.MetaPartitionView, 0)
	for _, mp := range vol.MetaPartitions {
		mpViews = append(mpViews, getMetaPartitionView(mp))
	}
	return
}

func (vol *Vol) setMpsCache(body []byte) {
	vol.Lock()
	defer vol.Unlock()
	vol.mpsCache = body
}

func (vol *Vol) getMpsCache() []byte {
	vol.RLock()
	defer vol.RUnlock()
	return vol.mpsCache
}

func (vol *Vol) setViewCache(body []byte) {
	vol.Lock()
	defer vol.Unlock()
	vol.viewCache = body
}

func (vol *Vol) getViewCache() []byte {
	vol.RLock()
	defer vol.RUnlock()
	return vol.viewCache
}

// Periodically check the volume's status.
// If an volume is marked as deleted, then generate corresponding delete task (meta partition or data partition)
// If all the meta partition and data partition of this volume have been deleted, then delete this volume.
func (vol *Vol) checkStatus(c *Cluster) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkStatus occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkStatus occurred panic")
		}
	}()
	vol.updateViewCache(c)
	vol.Lock()
	defer vol.Unlock()
	if vol.Status != proto.VolStMarkDelete {
		return
	}
	log.LogInfof("action[volCheckStatus] vol[%v],status[%v]", vol.Name, vol.Status)
	metaTasks := vol.getTasksToDeleteMetaPartitions()
	dataTasks := vol.getTasksToDeleteDataPartitions()
	ecTasks := vol.getTasksToDeleteEcDataPartitions()

	if len(metaTasks) == 0 && len(dataTasks) == 0 && len(ecTasks) == 0 {
		vol.deleteVolFromStore(c)
	}
	go func() {
		for _, metaTask := range metaTasks {
			vol.deleteMetaPartitionFromMetaNode(c, metaTask)
		}

		for _, dataTask := range dataTasks {
			vol.deleteDataPartitionFromDataNode(c, dataTask)
		}

		for _, ecTask := range ecTasks {
			vol.deleteEcDataPartitionFromEcNode(c, ecTask)
		}

	}()

	return
}

func (vol *Vol) deleteMetaPartitionFromMetaNode(c *Cluster, task *proto.AdminTask) {
	mp, err := vol.metaPartition(task.PartitionID)
	if err != nil {
		return
	}
	metaNode, err := c.metaNode(task.OperatorAddr)
	if err != nil {
		return
	}
	_, err = metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
		return
	}
	mp.Lock()
	mp.removeReplicaByAddr(metaNode.Addr)
	mp.removeMissingReplica(metaNode.Addr)
	mp.Unlock()
	return
}

func (vol *Vol) deleteDataPartitionFromDataNode(c *Cluster, task *proto.AdminTask) {
	dp, err := vol.getDataPartitionByID(task.PartitionID)
	if err != nil {
		return
	}
	dataNode, err := c.dataNode(task.OperatorAddr)
	if err != nil {
		return
	}
	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}
	dp.Lock()
	dp.removeReplicaByAddr(dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)
	if err = dp.update("deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, dp.Learners, c); err != nil {
		dp.Unlock()
		return
	}
	dp.Unlock()

	return
}

func (vol *Vol) deleteVolFromStore(c *Cluster) (err error) {

	if err = c.syncDeleteVol(vol); err != nil {
		return
	}

	// delete the metadata of the meta and data partitionMap first
	vol.deleteDataPartitionsFromStore(c)
	vol.deleteMetaPartitionsFromStore(c)
	vol.deleteTokensFromStore(c)
	// then delete the volume
	c.deleteVol(vol.Name)
	c.volStatInfo.Delete(vol.Name)
	return
}

func (vol *Vol) deleteTokensFromStore(c *Cluster) {
	vol.tokensLock.RLock()
	defer vol.tokensLock.RUnlock()
	for _, token := range vol.tokens {
		c.syncDeleteToken(token)
	}
}

func (vol *Vol) deleteMetaPartitionsFromStore(c *Cluster) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		c.syncDeleteMetaPartition(mp)
	}
	return
}

func (vol *Vol) deleteDataPartitionsFromStore(c *Cluster) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitions {
		c.syncDeleteDataPartition(dp)
	}

}

func (vol *Vol) getTasksToDeleteMetaPartitions() (tasks []*proto.AdminTask) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	tasks = make([]*proto.AdminTask, 0)

	for _, mp := range vol.MetaPartitions {
		for _, replica := range mp.Replicas {
			tasks = append(tasks, replica.createTaskToDeleteReplica(mp.PartitionID))
		}
	}
	return
}

func (vol *Vol) getTasksToDeleteDataPartitions() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()

	for _, dp := range vol.dataPartitions.partitions {
		for _, replica := range dp.Replicas {
			tasks = append(tasks, dp.createTaskToDeleteDataPartition(replica.Addr))
		}
	}
	return
}

func (vol *Vol) getDataPartitionsCount() (count int) {
	vol.RLock()
	count = len(vol.dataPartitions.partitionMap)
	vol.RUnlock()
	return
}

func (vol *Vol) getWritableDataPartitionsCount() (count int) {
	vol.RLock()
	count = vol.dataPartitions.readableAndWritableCnt
	vol.RUnlock()
	return
}

func (vol *Vol) String() string {
	return fmt.Sprintf("name[%v],dpNum[%v],mpNum[%v],cap[%v],status[%v]",
		vol.Name, vol.dpReplicaNum, vol.mpReplicaNum, vol.Capacity, vol.Status)
}

func (vol *Vol) doSplitMetaPartition(c *Cluster, mp *MetaPartition, end uint64) (nextMp *MetaPartition, err error) {
	mp.Lock()
	defer mp.Unlock()
	if err = mp.canSplit(end); err != nil {
		return
	}
	log.LogWarnf("action[splitMetaPartition],partition[%v],start[%v],end[%v],new end[%v]", mp.PartitionID, mp.Start, mp.End, end)
	cmdMap := make(map[string]*RaftCmd, 0)
	oldEnd := mp.End
	mp.End = end
	updateMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncUpdateMetaPartition, mp)
	if err != nil {
		return
	}
	cmdMap[updateMpRaftCmd.K] = updateMpRaftCmd
	if nextMp, err = vol.doCreateMetaPartition(c, mp.End+1, defaultMaxMetaPartitionInodeID); err != nil {
		Warn(c.Name, fmt.Sprintf("action[updateEnd] clusterID[%v] partitionID[%v] create meta partition err[%v]",
			c.Name, mp.PartitionID, err))
		log.LogErrorf("action[updateEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
		return
	}
	addMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncAddMetaPartition, nextMp)
	if err != nil {
		return
	}
	cmdMap[addMpRaftCmd.K] = addMpRaftCmd
	if err = c.syncBatchCommitCmd(cmdMap); err != nil {
		mp.End = oldEnd
		return nil, errors.NewError(err)
	}
	mp.updateInodeIDRangeForAllReplicas()
	mp.addUpdateMetaReplicaTask(c)
	return
}

func (vol *Vol) splitMetaPartition(c *Cluster, mp *MetaPartition, end uint64) (err error) {
	if c.DisableAutoAllocate {
		return
	}
	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()
	maxPartitionID := vol.maxPartitionID()
	if maxPartitionID != mp.PartitionID {
		err = fmt.Errorf("mp[%v] is not the last meta partition[%v]", mp.PartitionID, maxPartitionID)
		return
	}
	nextMp, err := vol.doSplitMetaPartition(c, mp, end)
	if err != nil {
		return
	}
	vol.addMetaPartition(nextMp)
	log.LogWarnf("action[splitMetaPartition],vol[%v] next partition[%v],start[%v],end[%v]", vol.Name, nextMp.PartitionID, nextMp.Start, nextMp.End)
	return
}

func (vol *Vol) createMetaPartition(c *Cluster, start, end uint64) (err error) {
	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()
	var mp *MetaPartition
	if mp, err = vol.doCreateMetaPartition(c, start, end); err != nil {
		return
	}
	if err = c.syncAddMetaPartition(mp); err != nil {
		return errors.NewError(err)
	}
	vol.addMetaPartition(mp)
	return
}

func (vol *Vol) doCreateMetaPartition(c *Cluster, start, end uint64) (mp *MetaPartition, err error) {
	var (
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
		learners    []proto.Learner
		wg          sync.WaitGroup
		storeMode   proto.StoreMode
	)
	learners = make([]proto.Learner, 0)
	storeMode = vol.DefaultStoreMode
	errChannel := make(chan error, vol.mpReplicaNum+vol.mpLearnerNum)
	if IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
		if hosts, peers, learners, err = c.chooseTargetMetaHostsForCreateQuorumMetaPartition(int(vol.mpReplicaNum), int(vol.mpLearnerNum), vol.zoneName, storeMode); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] chooseTargetMetaHosts for cross region quorum vol,err[%v]", err)
			return nil, errors.NewError(err)
		}
	} else {
		if hosts, peers, err = c.chooseTargetMetaHosts("", nil, nil, int(vol.mpReplicaNum), vol.zoneName, false, storeMode); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] chooseTargetMetaHosts err[%v]", err)
			return nil, errors.NewError(err)
		}
		//mpLearnerNum of vol is always 0 for other type vols except cross region quorum vol.
		//if it will be used in other vol, should choose new learner replica
	}
	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return nil, errors.NewError(err)
	}
	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, vol.mpLearnerNum, vol.Name, vol.ID)
	mp.setHosts(hosts)
	mp.setPeers(peers)
	mp.setLearners(learners)
	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			if err = c.syncCreateMetaPartitionToMetaNode(host, mp, storeMode, vol.trashRemainingDays); err != nil {
				errChannel <- err
				return
			}
			mp.Lock()
			defer mp.Unlock()
			if err = mp.afterCreation(host, c, storeMode); err != nil {
				errChannel <- err
			}
		}(host)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		for _, host := range hosts {
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				mr, err := mp.getMetaReplica(host)
				if err != nil {
					return
				}
				task := mr.createTaskToDeleteReplica(mp.PartitionID)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.addMetaNodeTasks(tasks)
			}(host)
		}
		wg.Wait()
		return nil, errors.NewError(err)
	default:
		mp.Status = proto.ReadWrite
	}
	log.LogInfof("action[doCreateMetaPartition] success,volName[%v],partition[%v]", vol.Name, partitionID)
	return
}

func (vol *Vol) getDataPartitionQuorum() (quorum int) {
	switch vol.CrossRegionHAType {
	case proto.CrossRegionHATypeQuorum:
		if vol.dpReplicaNum <= 3 {
			quorum = int(vol.dpReplicaNum)
		} else {
			quorum = int(vol.dpReplicaNum/2 + 1)
		}
	default:
		quorum = int(vol.dpReplicaNum)
	}
	return
}

func (vol *Vol) setWritableMpCount(count int64) {
	atomic.StoreInt64(&vol.writableMpCount, count)
}

func (vol *Vol) getWritableMpCount() int64 {
	return atomic.LoadInt64(&vol.writableMpCount)
}

func (vol *Vol) backupConfig() *Vol {
	return &Vol{
		Capacity:             vol.Capacity,
		dpReplicaNum:         vol.dpReplicaNum,
		FollowerRead:         vol.FollowerRead,
		NearRead:             vol.NearRead,
		authenticate:         vol.authenticate,
		enableToken:          vol.enableToken,
		autoRepair:           vol.autoRepair,
		description:          vol.description,
		dpSelectorName:       vol.dpSelectorName,
		dpSelectorParm:       vol.dpSelectorParm,
		DefaultStoreMode:     vol.DefaultStoreMode,
		convertState:         vol.convertState,
		MpLayout:             vol.MpLayout,
		crossZone:            vol.crossZone,
		zoneName:             vol.zoneName,
		OSSBucketPolicy:      vol.OSSBucketPolicy,
		trashRemainingDays:   vol.trashRemainingDays,
		mpLearnerNum:         vol.mpLearnerNum,
		CrossRegionHAType:    vol.CrossRegionHAType,
		DPConvertMode:        vol.DPConvertMode,
		MPConvertMode:        vol.MPConvertMode,
		dpWriteableThreshold: vol.dpWriteableThreshold,
		mpReplicaNum:         vol.mpReplicaNum,
		ForceROW:             vol.ForceROW,
		volWriteMutexEnable:  vol.volWriteMutexEnable,
		volWriteMutexClient:  vol.volWriteMutexClient,
		enableWriteCache:     vol.enableWriteCache,
		ExtentCacheExpireSec: vol.ExtentCacheExpireSec,
		isSmart:              vol.isSmart,
		smartRules:           vol.smartRules,
		compactTag:           vol.compactTag,
		compactTagModifyTime: vol.compactTagModifyTime,
		FollowerReadDelayCfg: vol.FollowerReadDelayCfg,
		TrashCleanInterval : vol.TrashCleanInterval,
	}
}

func (vol *Vol) rollbackConfig(backupVol *Vol) {
	if backupVol == nil {
		return
	}
	vol.Capacity = backupVol.Capacity
	vol.dpReplicaNum = backupVol.dpReplicaNum
	vol.FollowerRead = backupVol.FollowerRead
	vol.NearRead = backupVol.NearRead
	vol.authenticate = backupVol.authenticate
	vol.enableToken = backupVol.enableToken
	vol.autoRepair = backupVol.autoRepair
	vol.description = backupVol.description
	vol.dpSelectorParm = backupVol.dpSelectorParm
	vol.dpSelectorName = backupVol.dpSelectorName
	vol.DefaultStoreMode = backupVol.DefaultStoreMode
	vol.convertState = backupVol.convertState
	vol.MpLayout = backupVol.MpLayout
	vol.crossZone = backupVol.crossZone
	vol.zoneName = backupVol.zoneName
	vol.trashRemainingDays = backupVol.trashRemainingDays
	vol.OSSBucketPolicy = backupVol.OSSBucketPolicy
	vol.mpLearnerNum = backupVol.mpLearnerNum
	vol.CrossRegionHAType = backupVol.CrossRegionHAType
	vol.DPConvertMode = backupVol.DPConvertMode
	vol.MPConvertMode = backupVol.MPConvertMode
	vol.dpWriteableThreshold = backupVol.dpWriteableThreshold
	vol.mpReplicaNum = backupVol.mpReplicaNum
	vol.ForceROW = backupVol.ForceROW
	vol.volWriteMutexEnable = backupVol.volWriteMutexEnable
	vol.volWriteMutexClient = backupVol.volWriteMutexClient
	vol.enableWriteCache = backupVol.enableWriteCache
	vol.ExtentCacheExpireSec = backupVol.ExtentCacheExpireSec
	vol.isSmart = backupVol.isSmart
	vol.smartRules = backupVol.smartRules
	vol.compactTag = backupVol.compactTag
	vol.compactTagModifyTime = backupVol.compactTagModifyTime
	vol.TrashCleanInterval = backupVol.TrashCleanInterval
}

func (vol *Vol) getEcPartitionByID(partitionID uint64) (ep *EcDataPartition, err error) {
	return vol.ecDataPartitions.get(partitionID)
}

func (vol *Vol) getEcPartitionsView() (body []byte, err error) {
	return vol.ecDataPartitions.updateResponseCache(false, 0)
}

func (vol *Vol) dpIsCanEc(dp *DataPartition) bool {
	if dp.Status == proto.ReadOnly && time.Now().Unix()-getDpLastUpdateTime(dp) > EcTimeMinute*vol.EcMigrationWaitTime && dp.used > 0 {
		return true
	}
	return false
}

//getCanOperDataPartitions oper-> false: get can migration dps true: get can del dps
func (vol *Vol) getCanOperDataPartitions(oper bool) []*proto.DataPartitionResponse {
	dpResp := make([]*proto.DataPartitionResponse, 0)
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitionMap {
		if !vol.dpIsCanEc(dp) {
			continue
		}
		if !oper && dp.EcMigrateStatus != proto.NotEcMigrate && dp.EcMigrateStatus != proto.RollBack {
			continue
		}

		if (dp.EcMigrateStatus == proto.FinishEC) == oper {
			dpResp = append(dpResp, dp.convertToDataPartitionResponse())
		}
	}
	return dpResp
}

func (vol *Vol) cloneEcPartitionMap() (eps map[uint64]*EcDataPartition) {
	vol.ecDataPartitions.RLock()
	defer vol.ecDataPartitions.RUnlock()
	eps = make(map[uint64]*EcDataPartition, 0)
	for _, ep := range vol.ecDataPartitions.partitions {
		if !proto.IsEcFinished(ep.EcMigrateStatus) {
			continue
		}
		eps[ep.PartitionID] = ep
	}
	return
}

func (vol *Vol) checkEcReplicaNum(c *Cluster) {
	var err error
	eps := vol.cloneEcPartitionMap()
	volEcReplicaNum := vol.EcDataNum + vol.EcParityNum
	for _, ep := range eps {
		if !proto.IsEcFinished(ep.EcMigrateStatus) {
			continue
		}
		host := ep.getToBeDecommissionHost(int(volEcReplicaNum))
		if host == "" {
			continue
		}
		if err = ep.removeOneEcReplicaByHost(c, host); err != nil {
			log.LogErrorf("action[checkReplicaNum],vol[%v],err[%v]", vol.Name, err)
			continue
		}
	}
}

func (vol *Vol) getTasksToDeleteEcDataPartitions() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	vol.ecDataPartitions.RLock()
	defer vol.ecDataPartitions.RUnlock()

	for _, ep := range vol.ecDataPartitions.partitions { //delete all ecPartitions
		for _, replica := range ep.ecReplicas {
			tasks = append(tasks, ep.createTaskToDeleteEcPartition(replica.Addr))
		}
	}
	return
}

func (vol *Vol) deleteEcDataPartitionFromEcNode(c *Cluster, task *proto.AdminTask) {
	ep, err := vol.getEcPartitionByID(task.PartitionID)
	if err != nil {
		return
	}
	ecNode, err := c.ecNode(task.OperatorAddr)
	if err != nil {
		return
	}
	_, err = ecNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteEcDataReplica] vol[%v],ec data partition[%v],err[%v]", ep.VolName, ep.PartitionID, err)
		return
	}
	ep.Lock()
	defer ep.Unlock()
	ep.removeReplicaByAddr(ecNode.Addr)
	ep.checkAndRemoveMissReplica(ecNode.Addr)
	if err = ep.update("deleteEcDataReplica", ep.VolName, ep.Peers, ep.Hosts, c); err != nil {
		log.LogErrorf("deleteEcDataReplica err[%v]", err)
	}
	return
}
