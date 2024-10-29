// Copyright 2018 The CubeFS Authors.
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
	"math"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/routinepool"
)

type VolVarargs struct {
	zoneName                 string
	description              string
	capacity                 uint64 // GB
	deleteLockTime           int64  // h
	followerRead             bool
	authenticate             bool
	dpSelectorName           string
	dpSelectorParm           string
	coldArgs                 *coldVolArgs
	domainId                 uint64
	dpReplicaNum             uint8
	enablePosixAcl           bool
	dpReadOnlyWhenVolFull    bool
	enableQuota              bool
	enableTransaction        proto.TxOpMask
	txTimeout                int64
	txConflictRetryNum       int64
	txConflictRetryInterval  int64
	txOpLimit                int
	trashInterval            int64
	crossZone                bool
	accessTimeInterval       int64
	enableAutoDpMetaRepair   bool
	accessTimeValidInterval  int64
	enablePersistAccessTime  bool
	volStorageClass          uint32
	allowedStorageClass      []uint32
	forbidWriteOpOfProtoVer0 bool
	quotaByClass             map[uint32]uint64
}

type CacheSubItem struct {
	EbsBlkSize       int
	CacheCapacity    uint64
	CacheAction      int
	CacheThreshold   int
	CacheTTL         int
	CacheHighWater   int
	CacheLowWater    int
	CacheLRUInterval int
	CacheRule        string
	PreloadCacheOn   bool
	preloadCapacity  uint64
}

type TxSubItem struct {
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	txOpLimit               int
	enableTransaction       proto.TxOpMask
}

type TopoSubItem struct {
	crossZone       bool
	domainOn        bool
	dpSelectorName  string
	dpSelectorParm  string
	domainId        uint64
	defaultPriority bool // old default zone first
	createDpMutex   sync.RWMutex
	createMpMutex   sync.RWMutex
}

type AuthenticSubItem struct {
	OSSAccessKey   string
	OSSSecretKey   string
	authenticate   bool
	enablePosixAcl bool
	authKey        string
}

type VolDeletionSubItem struct {
	Deleting       bool
	DeleteLockTime int64
	Forbidden      bool
	DeleteExecTime time.Time
}

// Vol represents a set of meta partitionMap and data partitionMap
type Vol struct {
	ID            uint64
	Name          string
	Owner         string
	Status        uint8
	VolType       int
	zoneName      string
	user          *User
	createTime    int64
	description   string
	TrashInterval int64

	dpReplicaNum      uint8
	mpReplicaNum      uint8
	dataPartitionSize uint64 // byte
	Capacity          uint64 // GB
	dpRepairBlockSize uint64

	MetaPartitions map[uint64]*MetaPartition `graphql:"-"`
	dataPartitions *DataPartitionMap
	mpsCache       []byte
	viewCache      []byte

	NeedToLowerReplica       bool
	FollowerRead             bool
	enableQuota              bool
	DisableAuditLog          bool
	DpReadOnlyWhenVolFull    bool // only if this switch is on, all dp becomes readonly when vol is full
	ReadOnlyForVolFull       bool // only if the switch DpReadOnlyWhenVolFull is on, mark vol is readonly when is full
	AccessTimeInterval       int64
	EnablePersistAccessTime  bool
	AccessTimeValidInterval  int64
	EnableAutoMetaRepair     atomicutil.Bool
	ForbidWriteOpOfProtoVer0 atomicutil.Bool

	TopoSubItem
	CacheSubItem
	TxSubItem
	AuthenticSubItem
	VolDeletionSubItem

	qosManager      *QosCtrlManager
	aclMgr          AclManager
	uidSpaceManager *UidSpaceManager
	quotaManager    *MasterQuotaManager
	VersionMgr      *VolVersionManager

	mpsLock *mpsLockManager
	volLock sync.RWMutex

	// hybrid cloud
	allowedStorageClass     []uint32 // specifies which storageClasses the vol use, a cluster may have multiple StorageClasses
	volStorageClass         uint32   // specifies which storageClass is written, unless dirStorageClass is set in file path
	cacheDpStorageClass     uint32   // for SDK those access cache/preload dp of cold volume
	StatByStorageClass      []*proto.StatOfStorageClass
	StatMigrateStorageClass []*proto.StatOfStorageClass
	StatByDpMediaType       []*proto.StatOfStorageClass
	QuotaByClass            []*proto.StatOfStorageClass
}

func newVol(vv volValue) (vol *Vol) {
	vol = &Vol{ID: vv.ID, Name: vv.Name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}

	vol.dataPartitions = newDataPartitionMap(vv.Name)
	vol.VersionMgr = newVersionMgr(vol)
	vol.dpReplicaNum = vv.DpReplicaNum
	vol.mpReplicaNum = vv.ReplicaNum
	vol.Owner = vv.Owner

	vol.dataPartitionSize = vv.DataPartitionSize
	vol.Capacity = vv.Capacity
	vol.FollowerRead = vv.FollowerRead
	vol.authenticate = vv.Authenticate
	vol.crossZone = vv.CrossZone
	vol.zoneName = vv.ZoneName
	vol.viewCache = make([]byte, 0)
	vol.mpsCache = make([]byte, 0)
	vol.createTime = vv.CreateTime
	vol.DeleteLockTime = vv.DeleteLockTime
	vol.description = vv.Description
	vol.defaultPriority = vv.DefaultPriority
	vol.domainId = vv.DomainId
	vol.enablePosixAcl = vv.EnablePosixAcl
	vol.enableQuota = vv.EnableQuota
	vol.enableTransaction = vv.EnableTransaction
	vol.txTimeout = vv.TxTimeout
	vol.txConflictRetryNum = vv.TxConflictRetryNum
	vol.txConflictRetryInterval = vv.TxConflictRetryInterval
	vol.txOpLimit = vv.TxOpLimit

	vol.VolType = vv.VolType
	vol.EbsBlkSize = vv.EbsBlkSize
	vol.CacheCapacity = vv.CacheCapacity
	vol.CacheAction = vv.CacheAction
	vol.CacheThreshold = vv.CacheThreshold
	vol.CacheTTL = vv.CacheTTL
	vol.CacheHighWater = vv.CacheHighWater
	vol.CacheLowWater = vv.CacheLowWater
	vol.CacheLRUInterval = vv.CacheLRUInterval
	vol.CacheRule = vv.CacheRule
	vol.Status = vv.Status

	limitQosVal := &qosArgs{
		qosEnable:     vv.VolQosEnable,
		diskQosEnable: vv.DiskQosEnable,
		iopsRVal:      vv.IopsRLimit,
		iopsWVal:      vv.IopsWLimit,
		flowRVal:      vv.FlowRlimit,
		flowWVal:      vv.FlowWlimit,
	}
	vol.initQosManager(limitQosVal)

	magnifyQosVal := &qosArgs{
		iopsRVal: uint64(vv.IopsRMagnify),
		iopsWVal: uint64(vv.IopsWMagnify),
		flowRVal: uint64(vv.FlowWMagnify),
		flowWVal: uint64(vv.FlowWMagnify),
	}
	vol.qosManager.volUpdateMagnify(magnifyQosVal)
	vol.DpReadOnlyWhenVolFull = vv.DpReadOnlyWhenVolFull
	vol.DisableAuditLog = false
	vol.mpsLock = newMpsLockManager(vol)
	vol.preloadCapacity = math.MaxUint64 // mark as special value to trigger calculate
	vol.dpRepairBlockSize = proto.DefaultDpRepairBlockSize
	vol.EnableAutoMetaRepair.Store(defaultEnableDpMetaRepair)
	vol.TrashInterval = vv.TrashInterval
	vol.AccessTimeValidInterval = vv.AccessTimeInterval
	vol.EnablePersistAccessTime = vv.EnablePersistAccessTime

	vol.allowedStorageClass = make([]uint32, len(vv.AllowedStorageClass))
	copy(vol.allowedStorageClass, vv.AllowedStorageClass)
	vol.volStorageClass = vv.VolStorageClass
	vol.cacheDpStorageClass = vv.CacheDpStorageClass
	vol.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
	vol.StatMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
	vol.ForbidWriteOpOfProtoVer0.Store(defaultVolForbidWriteOpOfProtoVersion0)

	vol.QuotaByClass = vv.QuotaOfClass
	if len(vol.QuotaByClass) == 0 {
		for _, c := range vol.allowedStorageClass {
			vol.QuotaByClass = append(vol.QuotaByClass, proto.NewStatOfStorageClass(c))
		}
	}

	return
}

func newVolFromVolValue(vv *volValue) (vol *Vol) {
	vol = newVol(*vv)
	// overwrite oss secure
	vol.OSSAccessKey, vol.OSSSecretKey = vv.OSSAccessKey, vv.OSSSecretKey
	vol.Status = vv.Status
	vol.dpSelectorName = vv.DpSelectorName
	vol.dpSelectorParm = vv.DpSelectorParm

	if vol.txTimeout == 0 {
		vol.txTimeout = proto.DefaultTransactionTimeout
	}
	if vol.txConflictRetryNum == 0 {
		vol.txConflictRetryNum = proto.DefaultTxConflictRetryNum
	}
	if vol.txConflictRetryInterval == 0 {
		vol.txConflictRetryInterval = proto.DefaultTxConflictRetryInterval
	}
	vol.TrashInterval = vv.TrashInterval
	vol.DisableAuditLog = vv.DisableAuditLog
	vol.Forbidden = vv.Forbidden
	vol.authKey = vv.AuthKey
	vol.DeleteExecTime = vv.DeleteExecTime
	vol.user = vv.User
	vol.dpRepairBlockSize = vv.DpRepairBlockSize
	if vol.dpRepairBlockSize == 0 {
		vol.dpRepairBlockSize = proto.DefaultDpRepairBlockSize
	}
	vol.EnableAutoMetaRepair.Store(vv.EnableAutoMetaRepair)
	vol.EnablePersistAccessTime = vv.EnablePersistAccessTime
	vol.AccessTimeValidInterval = vv.AccessTimeInterval
	if vol.AccessTimeValidInterval == 0 {
		vol.AccessTimeValidInterval = proto.DefaultAccessTimeValidInterval
	}
	vol.ForbidWriteOpOfProtoVer0.Store(vv.ForbidWriteOpOfProtoVer0)
	return vol
}

type mpsLockManager struct {
	mpsLock         sync.RWMutex
	lastEffectStack string
	lockTime        time.Time
	innerLock       sync.RWMutex
	onLock          bool
	hang            bool
	vol             *Vol
	enable          int32 // only config debug log enable lock
}

var (
	lockCheckInterval  = time.Second
	lockExpireInterval = time.Minute
)

func newMpsLockManager(vol *Vol) *mpsLockManager {
	lc := &mpsLockManager{vol: vol}
	go lc.CheckExceptionLock(lockCheckInterval, lockExpireInterval)
	if log.EnableDebug() {
		atomic.StoreInt32(&lc.enable, 0)
	}
	return lc
}

func (mpsLock *mpsLockManager) Lock() {
	mpsLock.mpsLock.Lock()
	if log.EnableDebug() && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.innerLock.Lock()
		mpsLock.onLock = true
		mpsLock.lockTime = time.Now()
		mpsLock.lastEffectStack = fmt.Sprintf("Lock stack %v", string(debug.Stack()))
	}
}

func (mpsLock *mpsLockManager) UnLock() {
	mpsLock.mpsLock.Unlock()
	if log.EnableDebug() && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.onLock = false
		mpsLock.lockTime = time.Unix(0, 0)
		mpsLock.lastEffectStack = fmt.Sprintf("UnLock stack %v", string(debug.Stack()))
		mpsLock.innerLock.Unlock()
	}
}

func (mpsLock *mpsLockManager) RLock() {
	mpsLock.mpsLock.RLock()
	if log.EnableDebug() && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.innerLock.RLock()
		mpsLock.hang = false
		mpsLock.onLock = true
		mpsLock.lockTime = time.Now()
		mpsLock.lastEffectStack = fmt.Sprintf("RLock stack %v", string(debug.Stack()))
	}
}

func (mpsLock *mpsLockManager) RUnlock() {
	mpsLock.mpsLock.RUnlock()
	if log.EnableDebug() && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.onLock = false
		mpsLock.hang = false
		mpsLock.lockTime = time.Unix(0, 0)
		mpsLock.lastEffectStack = fmt.Sprintf("RUnlock stack %v", string(debug.Stack()))
		mpsLock.innerLock.RUnlock()
	}
}

func (mpsLock *mpsLockManager) CheckExceptionLock(interval time.Duration, expireTime time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			if mpsLock.vol.status() == proto.VolStatusMarkDelete || atomic.LoadInt32(&mpsLock.enable) == 0 {
				break
			}
			if !log.EnableDebug() {
				continue
			}
			if !mpsLock.onLock {
				continue
			}
			tm := time.Now()
			if tm.After(mpsLock.lockTime.Add(expireTime)) {
				log.LogWarnf("vol %v mpsLock hang more than %v since time %v stack(%v)",
					mpsLock.vol.Name, expireTime, mpsLock.lockTime, mpsLock.lastEffectStack)
				mpsLock.hang = true
			}
		}
	}
}

func (vol *Vol) CheckStrategy(c *Cluster) {
	// make sure resume all the processing ver deleting tasks before checking
	if !atomic.CompareAndSwapInt32(&vol.VersionMgr.checkStrategy, 0, 1) {
		return
	}

	go func() {
		waitTime := 5 * time.Second * defaultIntervalToCheck
		waited := false
		for {
			time.Sleep(waitTime)
			if vol.Status == proto.VolStatusMarkDelete {
				break
			}
			if c != nil && c.IsLeader() {
				if !waited {
					log.LogInfof("wait for %v seconds once after becoming leader to make sure all the ver deleting tasks are resumed",
						waitTime)
					time.Sleep(waitTime)
					waited = true
				}
				if !proto.IsHot(vol.VolType) {
					return
				}
				vol.VersionMgr.RLock()
				if vol.VersionMgr.strategy.GetPeriodicSecond() == 0 || vol.VersionMgr.strategy.Enable == false { // strategy not be set
					vol.VersionMgr.RUnlock()
					continue
				}
				vol.VersionMgr.RUnlock()
				vol.VersionMgr.checkCreateStrategy(c)
				vol.VersionMgr.checkDeleteStrategy(c)
			}
		}
	}()
}

func (vol *Vol) CalculatePreloadCapacity() uint64 {
	total := uint64(0)

	dps := vol.dataPartitions.partitions
	for _, dp := range dps {
		if proto.IsPreLoadDp(dp.PartitionType) {
			total += dp.total / util.GB
		}
	}

	if overSoldFactor <= 0 {
		return total
	}

	return uint64(float32(total) / overSoldFactor)
}

func (vol *Vol) getPreloadCapacity() uint64 {
	if vol.preloadCapacity != math.MaxUint64 {
		return vol.preloadCapacity
	}
	vol.preloadCapacity = vol.CalculatePreloadCapacity()
	log.LogDebugf("[getPreloadCapacity] vol(%v) calculated preload capacity: %v", vol.Name, vol.preloadCapacity)
	return vol.preloadCapacity
}

func (vol *Vol) initQosManager(limitArgs *qosArgs) {
	vol.qosManager = &QosCtrlManager{
		cliInfoMgrMap:        make(map[uint64]*ClientInfoMgr, 0),
		serverFactorLimitMap: make(map[uint32]*ServerFactorLimit, 0),
		qosEnable:            limitArgs.qosEnable,
		vol:                  vol,
		ClientHitTriggerCnt:  defaultClientTriggerHitCnt,
		ClientReqPeriod:      defaultClientReqPeriodSeconds,
	}

	if limitArgs.iopsRVal == 0 {
		limitArgs.iopsRVal = defaultIopsRLimit
	}
	if limitArgs.iopsWVal == 0 {
		limitArgs.iopsWVal = defaultIopsWLimit
	}
	if limitArgs.flowRVal == 0 {
		limitArgs.flowRVal = defaultFlowRLimit
	}
	if limitArgs.flowWVal == 0 {
		limitArgs.flowWVal = defaultFlowWLimit
	}
	arrLimit := [defaultLimitTypeCnt]uint64{limitArgs.iopsRVal, limitArgs.iopsWVal, limitArgs.flowRVal, limitArgs.flowWVal}
	arrType := [defaultLimitTypeCnt]uint32{proto.IopsReadType, proto.IopsWriteType, proto.FlowReadType, proto.FlowWriteType}

	for i := 0; i < defaultLimitTypeCnt; i++ {
		vol.qosManager.serverFactorLimitMap[arrType[i]] = &ServerFactorLimit{
			Name:       proto.QosTypeString(arrType[i]),
			Type:       arrType[i],
			Total:      arrLimit[i],
			Buffer:     arrLimit[i],
			requestCh:  make(chan interface{}, 10240),
			qosManager: vol.qosManager,
			done:       make(chan interface{}, 1),
		}
		go vol.qosManager.serverFactorLimitMap[arrType[i]].dispatch()
	}
}

func (vol *Vol) refreshOSSSecure() (key, secret string) {
	vol.OSSAccessKey = util.RandomString(16, util.Numeric|util.LowerLetter|util.UpperLetter)
	vol.OSSSecretKey = util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	return vol.OSSAccessKey, vol.OSSSecretKey
}

func (vol *Vol) addMetaPartition(mp *MetaPartition) {
	vol.mpsLock.Lock()
	defer vol.mpsLock.UnLock()
	if _, ok := vol.MetaPartitions[mp.PartitionID]; !ok {
		vol.MetaPartitions[mp.PartitionID] = mp
		return
	}
	// replace the old partition in the map with mp
	vol.MetaPartitions[mp.PartitionID] = mp
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

func (vol *Vol) getRWMetaPartitionNum() (num uint64, isHeartBeatDone bool) {
	if time.Now().Unix()-vol.createTime <= defaultMetaPartitionTimeOutSec {
		log.LogInfof("The vol[%v] is being created.", vol.Name)
		return num, false
	}
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		if !mp.heartBeatDone {
			log.LogInfof("The mp[%v] of vol[%v] is not done", mp.PartitionID, vol.Name)
			return num, false
		}
		if mp.Status == proto.ReadWrite {
			num++
		} else {
			log.LogWarnf("The mp[%v] of vol[%v] is not RW", mp.PartitionID, vol.Name)
		}
	}
	return num, true
}

func (vol *Vol) getDataPartitionsView() (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(false, 0, vol)
}

func (vol *Vol) getDataPartitionViewCompress() (body []byte, err error) {
	return vol.dataPartitions.updateCompressCache(false, 0, vol)
}

func (vol *Vol) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return vol.dataPartitions.get(partitionID)
}

func (vol *Vol) addMetaPartitions(c *Cluster, count int) (err error) {
	// add extra meta partitions at a time
	var (
		start uint64
		end   uint64
	)

	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()

	// update End of the maxMetaPartition range
	maxPartitionId := vol.maxPartitionID()
	rearMetaPartition := vol.MetaPartitions[maxPartitionId]
	oldEnd := rearMetaPartition.End
	end = rearMetaPartition.MaxInodeID + gConfig.MetaPartitionInodeIdStep

	if err = rearMetaPartition.canSplit(end, gConfig.MetaPartitionInodeIdStep, false); err != nil {
		return err
	}

	rearMetaPartition.End = end
	if err = c.syncUpdateMetaPartition(rearMetaPartition); err != nil {
		rearMetaPartition.End = oldEnd
		log.LogErrorf("action[addMetaPartitions] split partition partitionID[%v] err[%v]", rearMetaPartition.PartitionID, err)
		return
	}

	// create new meta partitions
	for i := 0; i < count; i++ {
		start = end + 1
		end = start + gConfig.MetaPartitionInodeIdStep

		if end > (defaultMaxMetaPartitionInodeID - gConfig.MetaPartitionInodeIdStep) {
			end = defaultMaxMetaPartitionInodeID
			log.LogWarnf("action[addMetaPartitions] vol[%v] add too many meta partition ,partition range overflow ! ", vol.Name)
		}

		if i == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}

		if err = vol.createMetaPartition(c, start, end); err != nil {
			log.LogErrorf("action[addMetaPartitions] vol[%v] add meta partition err[%v]", vol.Name, err)
			break
		}

		if end == defaultMaxMetaPartitionInodeID {
			break
		}
	}

	return
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

	vol.createMpMutex.Lock()
	for index := 0; index < count; index++ {
		if index != 0 {
			start = end + 1
		}
		end = gConfig.MetaPartitionInodeIdStep * uint64(index+1)
		if index == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}
		if err = vol.createMetaPartition(c, start, end); err != nil {
			log.LogErrorf("action[initMetaPartitions] vol[%v] init meta partition err[%v]", vol.Name, err)
			break
		}
	}
	vol.createMpMutex.Unlock()

	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	if len(vol.MetaPartitions) != count {
		err = fmt.Errorf("action[initMetaPartitions] vol[%v] init meta partition failed,mpCount[%v],expectCount[%v],err[%v]",
			vol.Name, len(vol.MetaPartitions), count, err)
	}
	return
}

func (vol *Vol) initDataPartitions(c *Cluster, dpCount int, mediaType uint32) (err error) {
	if dpCount == 0 {
		dpCount = defaultInitDataPartitionCnt
	}

	// initialize k data partitionMap at a time
	err = c.batchCreateDataPartition(vol, dpCount, true, mediaType)
	return
}

func (vol *Vol) checkDataPartitions(c *Cluster) (cnt int) {
	shouldDpInhibitWriteByVolFull := vol.shouldInhibitWriteBySpaceFull()
	vol.SetReadOnlyForVolFull(shouldDpInhibitWriteByVolFull)

	statsByClass := vol.getStorageStatWithClass()
	for _, stat := range statsByClass {
		log.LogDebugf("checkDataPartitions: try setPartitionsRdOnlyWithMediaType, rdOnly(%v), stat %s, name %s",
			vol.DpReadOnlyWhenVolFull, stat.String(), vol.Name)
	}

	if vol.Status != proto.VolStatusMarkDelete && proto.IsHot(vol.VolType) &&
		(time.Now().Unix()-vol.createTime >= defaultIntervalToCheckDataPartition) {
		for _, asc := range vol.allowedStorageClass {
			// check if need create dp for each allowedStorageClass of vol
			if !proto.IsStorageClassReplica(asc) {
				continue
			}

			mediaType := proto.GetMediaTypeByStorageClass(asc)
			dpCntOfMediaType := vol.dataPartitions.getDataPartitionsCountOfMediaType(mediaType)
			if dpCntOfMediaType == 0 {
				log.LogInfof("[checkDataPartitions] vol(%v) mediaType(%v) dp count is 0, try to create 1 dp",
					vol.Name, proto.MediaTypeString(mediaType))
				c.batchCreateDataPartition(vol, 1, false, mediaType)
			}
		}
	}

	totalPreloadCapacity := uint64(0)
	var rwDpCountOfSSD int
	var rwDpCountOfHDD int

	partitions := vol.dataPartitions.clonePartitions()
	statByMedia := map[uint32]uint64{}
	defer func() {
		datas := make([]*proto.StatOfStorageClass, 0, len(statByMedia))
		for t, c := range statByMedia {
			datas = append(datas, &proto.StatOfStorageClass{
				StorageClass:  t,
				UsedSizeBytes: c,
			})
		}
		vol.StatByDpMediaType = datas
	}()

	for _, dp := range partitions {
		statByMedia[dp.MediaType] += dp.getMaxUsedSpace()

		if proto.IsPreLoadDp(dp.PartitionType) {
			now := time.Now().Unix()
			if now > dp.PartitionTTL {
				log.LogWarnf("[checkDataPartitions] dp(%d) is deleted because of ttl expired, now(%d), ttl(%d)",
					dp.PartitionID, now, dp.PartitionTTL)
				vol.deleteDataPartition(c, dp)
				continue
			}

			startTime := dp.dataNodeStartTime()
			if now-dp.createTime > 600 && dp.used == 0 && now-startTime > 600 {
				log.LogWarnf("[checkDataPartitions] dp(%d) is deleted because of clear, now(%d), create(%d), start(%d)",
					dp.PartitionID, now, dp.createTime, startTime)
				vol.deleteDataPartition(c, dp)
				continue
			}

			totalPreloadCapacity += dp.total / util.GB
		}

		dpRdOnly := shouldDpInhibitWriteByVolFull
		if stat := statsByClass[proto.GetStorageClassByMediaType(dp.MediaType)]; stat.Full() && vol.DpReadOnlyWhenVolFull {
			dpRdOnly = true
		}

		dp.checkReplicaStatus(c.getDataPartitionTimeoutSec())
		dp.checkStatus(c.Name, true, c.getDataPartitionTimeoutSec(), c, dpRdOnly, vol.Forbidden)
		dp.checkLeader(c, c.Name, c.getDataPartitionTimeoutSec())
		dp.checkMissingReplicas(c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		dp.checkReplicaNum(c, vol)

		if time.Now().Unix()-vol.createTime < defaultIntervalToCheckHeartbeat*3 && !vol.Forbidden {
			dp.setReadWrite()
		}

		if dp.Status == proto.ReadWrite {
			cnt++
			if dp.MediaType == proto.MediaType_HDD {
				rwDpCountOfHDD++
			}
			if dp.MediaType == proto.MediaType_SSD {
				rwDpCountOfSSD++
			}
		}

		dp.checkDiskError(c.Name, c.leaderInfo.addr)

		dp.checkReplicationTask(c.Name, vol.dataPartitionSize)
	}

	if overSoldFactor > 0 {
		totalPreloadCapacity = uint64(float32(totalPreloadCapacity) / overSoldFactor)
	}
	vol.preloadCapacity = totalPreloadCapacity
	if vol.preloadCapacity != 0 {
		log.LogDebugf("[checkDataPartitions] vol(%v) totalPreloadCapacity(%v GB), overSoldFactor(%v)",
			vol.Name, totalPreloadCapacity, overSoldFactor)
	}

	vol.dataPartitions.setReadWriteDataPartitionCntByMediaType(rwDpCountOfHDD, proto.MediaType_HDD)
	vol.dataPartitions.setReadWriteDataPartitionCntByMediaType(rwDpCountOfSSD, proto.MediaType_SSD)
	log.LogInfof("[checkDataPartitions] vol(%v), rwDpCountOfHDD(%v), rwDpCountOfSSD(%v)",
		vol.Name, rwDpCountOfHDD, rwDpCountOfSSD)
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

func (vol *Vol) tryUpdateDpReplicaNum(c *Cluster, partition *DataPartition) (err error) {
	partition.RLock()
	defer partition.RUnlock()

	if partition.isRecover || vol.dpReplicaNum != 2 || partition.ReplicaNum != 3 || len(partition.Hosts) != 2 {
		return
	}

	if partition.isSpecialReplicaCnt() {
		return
	}
	oldReplicaNum := partition.ReplicaNum
	partition.ReplicaNum = partition.ReplicaNum - 1

	if err = c.syncUpdateDataPartition(partition); err != nil {
		partition.ReplicaNum = oldReplicaNum
	}
	return
}

func (vol *Vol) isOkUpdateRepCnt() (ok bool, rsp []uint64) {
	if proto.IsCold(vol.VolType) {
		return
	}
	ok = true
	dps := vol.cloneDataPartitionMap()
	for _, dp := range dps {
		if vol.dpReplicaNum != dp.ReplicaNum {
			rsp = append(rsp, dp.PartitionID)
			ok = false
			// output dps detail info
			if len(rsp) > 20 {
				return
			}
		}
	}
	return ok, rsp
}

func (vol *Vol) getQuotaByClass() map[uint32]uint64 {
	m := make(map[uint32]uint64)

	vol.volLock.RLock()
	defer vol.volLock.RUnlock()

	for _, c := range vol.QuotaByClass {
		m[c.StorageClass] = c.QuotaGB
	}
	return m
}

func (vol *Vol) getStorageStatWithClass() map[uint32]*proto.StatOfStorageClass {
	usedByClass := make(map[uint32]uint64)
	quotaByClass := vol.getQuotaByClass()

	vol.rangeMetaPartition(func(mp *MetaPartition) bool {
		stats := mp.StatByStorageClass
		for _, mpStat := range stats {
			usedByClass[mpStat.StorageClass] += mpStat.UsedSizeBytes
		}
		return true
	})

	totalStats := make(map[uint32]*proto.StatOfStorageClass, len(usedByClass))
	for t, u := range usedByClass {
		totalStats[t] = &proto.StatOfStorageClass{
			UsedSizeBytes: u,
			QuotaGB:       quotaByClass[t],
		}
	}

	return totalStats
}

func (vol *Vol) checkMetaPartitions(c *Cluster) {
	var tasks []*proto.AdminTask
	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	maxPartitionID := vol.maxPartitionID()
	mps := vol.cloneMetaPartitionMap()

	var (
		doSplit                    bool
		err                        error
		stat                       *proto.StatOfStorageClass
		volMigrateStat             *proto.StatOfStorageClass
		ok                         bool
		statByStorageClassMap      map[uint32]*proto.StatOfStorageClass
		statMigrateStorageClassMap map[uint32]*proto.StatOfStorageClass
	)
	statByStorageClassMap = make(map[uint32]*proto.StatOfStorageClass)
	statMigrateStorageClassMap = make(map[uint32]*proto.StatOfStorageClass)
	quotaByClass := vol.getQuotaByClass()

	for _, mp := range mps {
		doSplit = mp.checkStatus(c.Name, true, int(vol.mpReplicaNum), maxPartitionID, metaPartitionInodeIdStep, vol.Forbidden)
		if doSplit && !c.cfg.DisableAutoCreate {
			nextStart := mp.MaxInodeID + metaPartitionInodeIdStep
			log.LogInfof(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits start[%v] maxinodeid:[%v] default step:[%v],nextStart[%v]",
				c.Name, vol.Name, mp.PartitionID, mp.Start, mp.MaxInodeID, metaPartitionInodeIdStep, nextStart))
			if err = vol.splitMetaPartition(c, mp, nextStart, metaPartitionInodeIdStep, false); err != nil {
				Warn(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]", c.Name, vol.Name, mp.PartitionID, err))
			}
		}

		mp.checkLeader(c.Name)
		mp.checkReplicaNum(c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(c, maxPartitionID)
		mp.reportMissingReplicas(c.Name, c.leaderInfo.addr, defaultMetaPartitionTimeOutSec, defaultIntervalToAlarmMissingMetaPartition)
		tasks = append(tasks, mp.replicaCreationTasks(c.Name, vol.Name)...)

		for _, mpStat := range mp.StatByStorageClass {
			if stat, ok = statByStorageClassMap[mpStat.StorageClass]; !ok {
				stat = proto.NewStatOfStorageClassEx(mpStat.StorageClass, quotaByClass[mpStat.StorageClass])
				statByStorageClassMap[mpStat.StorageClass] = stat
			}

			stat.InodeCount += mpStat.InodeCount
			stat.UsedSizeBytes += mpStat.UsedSizeBytes
		}

		for _, mpMigrateStat := range mp.StatByMigrateStorageClass {
			if volMigrateStat, ok = statMigrateStorageClassMap[mpMigrateStat.StorageClass]; !ok {
				volMigrateStat = proto.NewStatOfStorageClass(mpMigrateStat.StorageClass)
				statMigrateStorageClassMap[mpMigrateStat.StorageClass] = volMigrateStat
			}

			volMigrateStat.InodeCount += mpMigrateStat.InodeCount
			volMigrateStat.UsedSizeBytes += mpMigrateStat.UsedSizeBytes
		}
	}

	StatOfStorageClassSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat = range statByStorageClassMap {
		StatOfStorageClassSlice = append(StatOfStorageClassSlice, stat)
	}

	vol.StatByStorageClass = StatOfStorageClassSlice

	StatMigrateStorageClassSlice := make([]*proto.StatOfStorageClass, 0)
	for _, volMigrateStat = range statMigrateStorageClassMap {
		StatMigrateStorageClassSlice = append(StatMigrateStorageClassSlice, volMigrateStat)
	}
	vol.StatMigrateStorageClass = StatMigrateStorageClassSlice

	c.addMetaNodeTasks(tasks)
	vol.checkSplitMetaPartition(c, metaPartitionInodeIdStep)
}

func (vol *Vol) checkSplitMetaPartition(c *Cluster, metaPartitionInodeStep uint64) {
	maxPartitionID := vol.maxPartitionID()
	maxMP, err := vol.metaPartition(maxPartitionID)
	if err != nil {
		return
	}
	// Any of the following conditions will trigger max mp split
	// 1. The memory of the metanode which max mp belongs to reaches the threshold
	// 2. The number of inodes managed by max mp reaches the threshold(0.75)
	// 3. The number of RW mp is less than 3
	maxMPInodeUsedRatio := float64(maxMP.MaxInodeID-maxMP.Start) / float64(metaPartitionInodeStep)
	RWMPNum, isHeartBeatDone := vol.getRWMetaPartitionNum()
	if !isHeartBeatDone {
		log.LogInfof("Not all volume[%s] mp heartbeat is done, skip mp split", vol.Name)
		return
	}
	if maxMP.memUsedReachThreshold(c.Name, vol.Name) || RWMPNum < lowerLimitRWMetaPartition ||
		maxMPInodeUsedRatio > metaPartitionInodeUsageThreshold {
		end := maxMP.MaxInodeID + metaPartitionInodeStep/4
		if RWMPNum < lowerLimitRWMetaPartition {
			end = maxMP.MaxInodeID + metaPartitionInodeStep
		}
		if err := vol.splitMetaPartition(c, maxMP, end, metaPartitionInodeStep, true); err != nil {
			msg := fmt.Sprintf("action[checkSplitMetaPartition],split meta maxMP[%v] failed,err[%v]\n",
				maxMP.PartitionID, err)
			Warn(c.Name, msg)
		}
		log.LogInfof("volume[%v] split MaxMP[%v], MaxInodeID[%d] Start[%d] RWMPNum[%d] maxMPInodeUsedRatio[%.2f]",
			vol.Name, maxPartitionID, maxMP.MaxInodeID, maxMP.Start, RWMPNum, maxMPInodeUsedRatio)
	}
	return
}

func (mp *MetaPartition) memUsedReachThreshold(clusterName, volName string) bool {
	liveReplicas := mp.getLiveReplicas()
	foundReadonlyReplica := false
	var readonlyReplica *MetaReplica
	for _, replica := range liveReplicas {
		if replica.Status == proto.ReadOnly {
			foundReadonlyReplica = true
			readonlyReplica = replica
			break
		}
	}
	if !foundReadonlyReplica || readonlyReplica == nil {
		return false
	}
	if readonlyReplica.metaNode.IsWriteAble() {
		msg := fmt.Sprintf("action[checkSplitMetaPartition] vol[%v],max meta parition[%v] status is readonly\n",
			volName, mp.PartitionID)
		Warn(clusterName, msg)
		return false
	}
	return true
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

func (vol *Vol) rangeMetaPartition(f func(m *MetaPartition) bool) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	for _, mp := range vol.MetaPartitions {
		if !f(mp) {
			return
		}
	}
}

func (vol *Vol) setMpForbid() {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		if mp.Status != proto.Unavailable {
			mp.Status = proto.ReadOnly
		}
	}
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

func (vol *Vol) setDpForbid() {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitionMap {
		if dp.Status != proto.Unavailable {
			dp.Status = proto.ReadOnly
		}
	}
}

func (vol *Vol) setStatus(status uint8) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	vol.Status = status
}

func (vol *Vol) status() uint8 {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.Status
}

func (vol *Vol) capacity() uint64 {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.Capacity
}

func (vol *Vol) SetReadOnlyForVolFull(isFull bool) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()

	if isFull {
		if vol.DpReadOnlyWhenVolFull {
			vol.ReadOnlyForVolFull = isFull
		}
	} else {
		vol.ReadOnlyForVolFull = isFull
	}
}

func (vol *Vol) IsReadOnlyForVolFull() bool {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.ReadOnlyForVolFull
}

func (vol *Vol) autoDeleteDp(c *Cluster) {
	if vol.dataPartitions == nil {
		return
	}

	maxSize := overSoldCap(vol.CacheCapacity * util.GB)
	maxCnt := maxSize / vol.dataPartitionSize

	if maxSize%vol.dataPartitionSize != 0 {
		maxCnt++
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, dp := range partitions {
		if !proto.IsCacheDp(dp.PartitionType) {
			continue
		}

		if maxCnt > 0 {
			maxCnt--
			continue
		}

		log.LogInfof("[autoDeleteDp] start delete dp, id[%d]", dp.PartitionID)
		vol.deleteDataPartition(c, dp)
	}
}

func (vol *Vol) checkAutoDataPartitionCreation(c *Cluster) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkAutoDataPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoDataPartitionCreation occurred panic")
		}
	}()

	if ok, _ := vol.needCreateDataPartition(); !ok {
		return
	}

	vol.setStatus(proto.VolStatusNormal)
	log.LogInfof("[checkAutoDataPartitionCreation] before autoCreateDataPartitions, vol[%v] clusterDisableAutoAllocate[%v] vol.Forbidden[%v]",
		vol.Name, c.DisableAutoAllocate, vol.Forbidden)
	if !c.DisableAutoAllocate && !vol.Forbidden {
		vol.autoCreateDataPartitions(c)
	}
}

func (vol *Vol) shouldInhibitWriteBySpaceFull() bool {
	if !vol.DpReadOnlyWhenVolFull {
		return false
	}

	if vol.capacity() == 0 {
		return false
	}

	if !proto.IsHot(vol.VolType) {
		return false
	}

	usedSpace := vol.totalUsedSpace() / util.GB
	if usedSpace >= vol.capacity() {
		return true
	}

	vol.ReadOnlyForVolFull = false
	return false
}

func (vol *Vol) needCreateDataPartition() (ok bool, err error) {
	ok = false
	if vol.status() == proto.VolStatusMarkDelete {
		err = proto.ErrVolNotExists
		return
	}

	if vol.capacity() == 0 {
		err = proto.ErrVolNoAvailableSpace
		return
	}

	if proto.IsStorageClassReplica(vol.volStorageClass) {
		if vol.IsReadOnlyForVolFull() {
			vol.setAllDataPartitionsToReadOnly()
			err = proto.ErrVolNoAvailableSpace
			return
		}
		ok = true
		return
	}

	// cold
	if vol.CacheAction == proto.NoCache && vol.CacheRule == "" {
		err = proto.ErrVolNoCacheAndRule
		return
	}

	ok = true
	return
}

func (vol *Vol) autoCreateDataPartitions(c *Cluster) {
	if time.Since(vol.dataPartitions.lastAutoCreateTime) < time.Minute {
		return
	}

	if c.cfg.DisableAutoCreate {
		// if disable auto create, once alloc size is over capacity, not allow to create new dp
		allocSize := uint64(len(vol.dataPartitions.partitions)) * vol.dataPartitionSize
		totalSize := vol.capacity() * util.GB
		if allocSize > totalSize {
			return
		}

		for _, asc := range vol.allowedStorageClass {
			if !proto.IsStorageClassReplica(asc) {
				continue
			}
			mediaType := proto.GetMediaTypeByStorageClass(asc)
			dpCntOfMediaType := vol.dataPartitions.getDataPartitionsCountOfMediaType(mediaType)

			if dpCntOfMediaType < minNumOfRWDataPartitions {
				log.LogWarnf("autoCreateDataPartitions: vol(%v) mediaType(%v) less than %v, alloc new partitions",
					vol.Name, proto.MediaTypeString(mediaType), minNumOfRWDataPartitions)
				c.batchCreateDataPartition(vol, minNumOfRWDataPartitions, false, mediaType)
			}
		}

		return
	}

	if proto.IsStorageClassBlobStore(vol.volStorageClass) {
		vol.dataPartitions.lastAutoCreateTime = time.Now()
		maxSize := overSoldCap(vol.CacheCapacity * util.GB)
		allocSize := uint64(0)
		for _, dp := range vol.cloneDataPartitionMap() {
			if !proto.IsCacheDp(dp.PartitionType) {
				continue
			}

			allocSize += dp.total
		}

		if maxSize <= allocSize {
			log.LogInfof("action[autoCreateDataPartitions] (%s) no need to create again, alloc [%d], max [%d]",
				vol.Name, allocSize, maxSize)
			return
		}

		if vol.cacheDpStorageClass == proto.StorageClass_Unspecified {
			log.LogErrorf("action[autoCreateDataPartitions] no resource to create cache data partition, vol(%v)", vol.Name)
			return
		}

		cacheMediaType := proto.GetMediaTypeByStorageClass(vol.cacheDpStorageClass)
		count := (maxSize-allocSize-1)/vol.dataPartitionSize + 1
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v] volStorageClass[%v], cacheMediaType(%v)",
			vol.Name, count, proto.StorageClassString(vol.volStorageClass), proto.MediaTypeString(cacheMediaType))

		c.batchCreateDataPartition(vol, int(count), false, cacheMediaType)
		return
	}

	statByClass := vol.getStorageStatWithClass()

	// check for hot vol
	for _, asc := range vol.allowedStorageClass {
		if !proto.IsStorageClassReplica(asc) {
			continue
		}

		stat := statByClass[asc]
		if vol.DpReadOnlyWhenVolFull && stat.Full() {
			log.LogInfof("action[autoCreateDataPartitions] target class meet cap limit, can't create, vol %s, asc %s",
				vol.Name, proto.StorageClassString(asc))
			continue
		}

		mediaType := proto.GetMediaTypeByStorageClass(asc)
		rwDpCountOfMediaType := vol.dataPartitions.getReadWriteDataPartitionCntByMediaType(mediaType)
		log.LogInfof("action[autoCreateDataPartitions] vol(%v) mediaType:%v, rwDpCountOfMediaType:%v",
			vol.Name, proto.MediaTypeString(mediaType), rwDpCountOfMediaType)
		var createDpCount int
		if asc == vol.volStorageClass && vol.Capacity > 200000 && rwDpCountOfMediaType < 200 {
			createDpCount = vol.calculateExpansionNum()
			log.LogInfof("action[autoCreateDataPartitions] vol(%v) volStorageClass(%v), calculated createDpCount:%v",
				vol.Name, asc, createDpCount)
		} else if rwDpCountOfMediaType < minNumOfRWDataPartitions {
			createDpCount = minNumOfRWDataPartitions
			log.LogInfof("action[autoCreateDataPartitions] vol(%v) volStorageClass(%v), min createDpCount:%v",
				vol.Name, asc, createDpCount)
		} else {
			continue
		}

		vol.dataPartitions.lastAutoCreateTime = time.Now()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] createDpCount[%v] for mediaType(%v)",
			vol.Name, createDpCount, proto.MediaTypeString(mediaType))
		c.batchCreateDataPartition(vol, createDpCount, false, mediaType)
	}
}

// Calculate the expansion number (the number of data partitions to be allocated to the given volume)
func (vol *Vol) calculateExpansionNum() (count int) {
	c := float64(vol.Capacity) * volExpansionRatio * float64(util.GB) / float64(util.DefaultDataPartitionSize)
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
	return vol.totalUsedSpaceByMeta(false)
}

func (vol *Vol) totalUsedSpaceByMeta(byMeta bool) uint64 {
	if proto.IsCold(vol.VolType) || byMeta {
		return vol.ebsUsedSpace()
	}

	return vol.cfsUsedSpace()
}

func (vol *Vol) cfsUsedSpace() uint64 {
	return vol.dataPartitions.totalUsedSpace()
}

func (vol *Vol) sendViewCacheToFollower(c *Cluster) {
	var err error
	log.LogInfof("action[asyncSendPartitionsToFollower]")

	metadata := new(RaftCmd)
	metadata.Op = opSyncDataPartitionsView
	metadata.K = vol.Name
	metadata.V = vol.dataPartitions.getDataPartitionResponseCache()

	if err = c.submit(metadata); err != nil {
		log.LogErrorf("action[asyncSendPartitionsToFollower] error [%v]", err)
	}
	log.LogInfof("action[asyncSendPartitionsToFollower] finished")
}

func (vol *Vol) ebsUsedSpace() uint64 {
	size := uint64(0)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	for _, mp := range vol.MetaPartitions {
		size += mp.dataSize()
	}

	return size
}

func (vol *Vol) updateViewCache(c *Cluster) {
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.createTime, vol.CacheTTL, vol.VolType, vol.DeleteLockTime)
	view.SetOwner(vol.Owner)
	view.SetOSSSecure(vol.OSSAccessKey, vol.OSSSecretKey)
	mpViews := vol.getMetaPartitionsView()
	view.MetaPartitions = mpViews
	mpViewsReply := newSuccessHTTPReply(mpViews)
	mpsBody, err := json.Marshal(mpViewsReply)
	if err != nil {
		log.LogErrorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setMpsCache(mpsBody)
	// dpResps := vol.dataPartitions.getDataPartitionsView(0)
	// view.DataPartitions = dpResps
	view.DomainOn = vol.domainOn
	viewReply := newSuccessHTTPReply(view)
	body, err := json.Marshal(viewReply)
	if err != nil {
		log.LogErrorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setViewCache(body)
}

func (vol *Vol) getMetaPartitionsView() (mpViews []*proto.MetaPartitionView) {
	mps := make(map[uint64]*MetaPartition)
	vol.mpsLock.RLock()
	for key, mp := range vol.MetaPartitions {
		mps[key] = mp
	}
	vol.mpsLock.RUnlock()

	mpViews = make([]*proto.MetaPartitionView, 0)
	for _, mp := range mps {
		mpViews = append(mpViews, getMetaPartitionView(mp))
	}
	return
}

func (vol *Vol) setMpsCache(body []byte) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	vol.mpsCache = body
}

func (vol *Vol) getMpsCache() []byte {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.mpsCache
}

func (vol *Vol) setViewCache(body []byte) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	vol.viewCache = body
}

func (vol *Vol) getViewCache() []byte {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.viewCache
}

func (vol *Vol) deleteDataPartition(c *Cluster, dp *DataPartition) {
	var addrs []string
	for _, replica := range dp.Replicas {
		addrs = append(addrs, replica.Addr)
	}

	for _, addr := range addrs {
		if err := vol.deleteDataPartitionFromDataNode(c, dp.createTaskToDeleteDataPartition(addr, false)); err != nil {
			log.LogErrorf("[deleteDataPartitionFromDataNode] delete data replica from datanode fail, id %d, err %s", dp.PartitionID, err.Error())
		}
	}

	vol.dataPartitions.del(dp)

	err := c.syncDeleteDataPartition(dp)
	if err != nil {
		log.LogErrorf("[deleteDataPartition] delete data partition from store fail, [%d], err: %s", dp.PartitionID, err.Error())
		return
	}

	log.LogInfof("[deleteDataPartition] delete data partition success, [%d]", dp.PartitionID)
}

// Periodically check the volume's status.
// If an volume is marked as deleted, then generate corresponding delete task (meta partition or data partition)
// If all the meta partition and data partition of this volume have been deleted, then delete this volume.
func (vol *Vol) checkStatus(c *Cluster) {
	if !atomic.CompareAndSwapInt32(&vol.VersionMgr.checkStatus, 0, 1) {
		return
	}
	defer func() {
		atomic.StoreInt32(&vol.VersionMgr.checkStatus, 0)
		if r := recover(); r != nil {
			log.LogWarnf("checkStatus occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkStatus occurred panic")
		}
	}()
	vol.updateViewCache(c)
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	if vol.Status != proto.VolStatusMarkDelete {
		return
	}

	if vol.Forbidden && len(c.delayDeleteVolsInfo) != 0 {
		var value *delayDeleteVolInfo
		c.deleteVolMutex.RLock()
		for _, value = range c.delayDeleteVolsInfo {
			if value.volName == vol.Name {
				break
			}
		}
		c.deleteVolMutex.RUnlock()
		if value.volName == vol.Name {
			return
		}
	}

	log.LogInfof("action[volCheckStatus] vol[%v],status[%v]", vol.Name, vol.Status)
	metaTasks := vol.getTasksToDeleteMetaPartitions()
	dataTasks := vol.getTasksToDeleteDataPartitions()

	if vol.Deleting {
		log.LogWarnf("action[volCheckStatus] vol[%v] is already in deleting status", vol.Name)
		return
	}

	if len(metaTasks) == 0 && len(dataTasks) == 0 {
		go func() {
			vol.Deleting = true
			vol.deleteVolFromStore(c)
			vol.Deleting = false
		}()
	}

	go func() {
		vol.Deleting = true
		for _, metaTask := range metaTasks {
			vol.deleteMetaPartitionFromMetaNode(c, metaTask)
		}

		for _, dataTask := range dataTasks {
			vol.deleteDataPartitionFromDataNode(c, dataTask)
		}
		vol.Deleting = false
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

	mp.RLock()
	_, err = mp.getMetaReplica(task.OperatorAddr)
	mp.RUnlock()
	if err != nil {
		log.LogWarnf("deleteMetaPartitionFromMetaNode (%s) maybe alread been deleted", task.ToString())
		return
	}

	_, err = metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
	}
	mp.Lock()
	mp.removeReplicaByAddr(metaNode.Addr)
	mp.removeMissingReplica(metaNode.Addr)
	mp.Unlock()
	return
}

func (vol *Vol) deleteDataPartitionFromDataNode(c *Cluster, task *proto.AdminTask) (err error) {
	dp, err := vol.getDataPartitionByID(task.PartitionID)
	if err != nil {
		return
	}

	dataNode, err := c.dataNode(task.OperatorAddr)
	if err != nil {
		return
	}

	dp.RLock()
	_, ok := dp.hasReplica(task.OperatorAddr)
	dp.RUnlock()
	if !ok {
		log.LogWarnf("deleteDataPartitionFromDataNode task(%s) maybe already executed", task.ToString())
		return
	}

	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		err = nil
	}

	dp.Lock()
	dp.removeReplicaByAddr(dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)
	if err = dp.update("deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		dp.Unlock()
		return
	}
	dp.Unlock()

	return
}

func (vol *Vol) deleteVolFromStore(c *Cluster) (err error) {
	start := time.Now()
	log.LogWarnf("deleteVolFromStore: start delete volume from store, name %s", vol.Name)
	defer func() {
		log.LogWarnf("deleteVolFromStore: finish delete volume, name %s, cost %d ms", vol.Name, time.Since(start).Milliseconds())
	}()

	if err = c.syncDeleteVol(vol); err != nil {
		return
	}

	// delete the metadata of the meta and data partitionMap first
	vol.deleteDataPartitionsFromStore(c)
	vol.deleteMetaPartitionsFromStore(c)
	// then delete the volume
	c.deleteVol(vol.Name)
	c.volStatInfo.Delete(vol.Name)

	c.DelBucketLifecycle(vol.Name)
	return
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
		log.LogDebugf("get delete task from vol(%s) mp(%d)", vol.Name, mp.PartitionID)
		for _, replica := range mp.Replicas {
			log.LogDebugf("get delete task from vol(%s) mp(%d),replica(%v)", vol.Name, mp.PartitionID, replica.Addr)
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
			tasks = append(tasks, dp.createTaskToDeleteDataPartition(replica.Addr, false))
		}
	}
	return
}

func (vol *Vol) getDataPartitionsCount() (count int) {
	vol.volLock.RLock()
	count = len(vol.dataPartitions.partitionMap)
	vol.volLock.RUnlock()
	return
}

func (vol *Vol) String() string {
	return fmt.Sprintf("name[%v],id[%v],dpNum[%v],mpNum[%v],cap[%v],status[%v]",
		vol.Name, vol.ID, vol.dpReplicaNum, vol.mpReplicaNum, vol.Capacity, vol.Status)
}

func (vol *Vol) doSplitMetaPartition(c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool) (nextMp *MetaPartition, err error) {
	mp.Lock()
	defer mp.Unlock()

	if err = mp.canSplit(end, metaPartitionInodeIdStep, ignoreNoLeader); err != nil {
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

func (vol *Vol) splitMetaPartition(c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool) (err error) {
	if c.DisableAutoAllocate {
		err = errors.NewErrorf("cluster auto allocate is disable")
		return
	}
	if vol.Forbidden {
		err = errors.NewErrorf("volume %v is forbidden", vol.Name)
		return
	}

	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()

	maxPartitionID := vol.maxPartitionID()
	if maxPartitionID != mp.PartitionID {
		err = fmt.Errorf("mp[%v] is not the last meta partition[%v]", mp.PartitionID, maxPartitionID)
		return
	}

	nextMp, err := vol.doSplitMetaPartition(c, mp, end, metaPartitionInodeIdStep, ignoreNoLeader)
	if err != nil {
		return
	}

	vol.addMetaPartition(nextMp)
	log.LogWarnf("action[splitMetaPartition],next partition[%v],start[%v],end[%v]", nextMp.PartitionID, nextMp.Start, nextMp.End)
	return
}

func (vol *Vol) createMetaPartition(c *Cluster, start, end uint64) (err error) {
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
		wg          sync.WaitGroup
	)

	errChannel := make(chan error, vol.mpReplicaNum)

	if c.isFaultDomain(vol) {
		if hosts, peers, err = c.getHostFromDomainZone(vol.domainId, TypeMetaPartition, vol.mpReplicaNum, proto.StorageClass_Unspecified); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromDomainZone err[%v]", err)
			return nil, errors.NewError(err)
		}
	} else {
		var excludeZone []string
		zoneNum := c.decideZoneNum(vol, proto.StorageClass_Unspecified)
		if hosts, peers, err = c.getHostFromNormalZone(TypeMetaPartition, excludeZone, nil, nil,
			int(vol.mpReplicaNum), zoneNum, vol.zoneName, proto.StorageClass_Unspecified); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromNormalZone err[%v]", err)
			return nil, errors.NewError(err)
		}
	}

	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return nil, errors.NewError(err)
	}

	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, vol.Name, vol.ID, vol.VersionMgr.getLatestVer())
	mp.setHosts(hosts)
	mp.setPeers(peers)

	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			if err = c.syncCreateMetaPartitionToMetaNode(host, mp); err != nil {
				errChannel <- err
				return
			}
			mp.Lock()
			defer mp.Unlock()
			if err = mp.afterCreation(host, c); err != nil {
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
	log.LogInfof("action[doCreateMetaPartition] success,volName[%v],partition[%v],start[%v],end[%v]", vol.Name, partitionID, start, end)
	return
}

func setVolFromArgs(args *VolVarargs, vol *Vol) {
	vol.zoneName = args.zoneName
	vol.Capacity = args.capacity
	vol.DeleteLockTime = args.deleteLockTime
	vol.FollowerRead = args.followerRead
	vol.authenticate = args.authenticate
	vol.enablePosixAcl = args.enablePosixAcl
	vol.DpReadOnlyWhenVolFull = args.dpReadOnlyWhenVolFull
	vol.enableQuota = args.enableQuota
	vol.enableTransaction = args.enableTransaction
	vol.txTimeout = args.txTimeout
	vol.txConflictRetryNum = args.txConflictRetryNum
	vol.txConflictRetryInterval = args.txConflictRetryInterval
	vol.txOpLimit = args.txOpLimit
	vol.dpReplicaNum = args.dpReplicaNum
	vol.crossZone = args.crossZone

	if proto.IsVolSupportStorageClass(args.allowedStorageClass, proto.StorageClass_BlobStore) {
		vol.EbsBlkSize = args.coldArgs.objBlockSize
	}

	if args.volStorageClass == proto.StorageClass_BlobStore {
		coldArgs := args.coldArgs
		vol.CacheLRUInterval = coldArgs.cacheLRUInterval
		vol.CacheLowWater = coldArgs.cacheLowWater
		vol.CacheHighWater = coldArgs.cacheHighWater
		vol.CacheTTL = coldArgs.cacheTtl
		vol.CacheThreshold = coldArgs.cacheThreshold
		vol.CacheAction = coldArgs.cacheAction
		vol.CacheRule = coldArgs.cacheRule
		vol.CacheCapacity = coldArgs.cacheCap
		vol.EbsBlkSize = coldArgs.objBlockSize
	}

	vol.description = args.description

	vol.dpSelectorName = args.dpSelectorName
	vol.dpSelectorParm = args.dpSelectorParm
	vol.TrashInterval = args.trashInterval
	vol.AccessTimeValidInterval = args.accessTimeValidInterval
	vol.AccessTimeInterval = args.accessTimeInterval
	vol.EnableAutoMetaRepair.Store(args.enableAutoDpMetaRepair)
	vol.EnablePersistAccessTime = args.enablePersistAccessTime
	vol.volStorageClass = args.volStorageClass
	vol.allowedStorageClass = append([]uint32{}, args.allowedStorageClass...)
	vol.ForbidWriteOpOfProtoVer0.Store(args.forbidWriteOpOfProtoVer0)

	quotaClass := make([]*proto.StatOfStorageClass, 0, len(args.quotaByClass))
	for t, c := range args.quotaByClass {
		quotaClass = append(quotaClass, proto.NewStatOfStorageClassEx(t, c))
	}
	vol.QuotaByClass = quotaClass
}

func getVolVarargs(vol *Vol) *VolVarargs {
	args := &coldVolArgs{
		objBlockSize:            vol.EbsBlkSize,
		cacheCap:                vol.CacheCapacity,
		cacheAction:             vol.CacheAction,
		cacheThreshold:          vol.CacheThreshold,
		cacheTtl:                vol.CacheTTL,
		cacheHighWater:          vol.CacheHighWater,
		cacheLowWater:           vol.CacheLowWater,
		cacheLRUInterval:        vol.CacheLRUInterval,
		cacheRule:               vol.CacheRule,
		accessTimeValidInterval: vol.AccessTimeValidInterval,
		trashInterval:           vol.TrashInterval,
		enablePersistAccessTime: vol.EnablePersistAccessTime,
	}

	quotaByClass := make(map[uint32]uint64)
	for _, c := range vol.QuotaByClass {
		quotaByClass[c.StorageClass] = c.QuotaGB
	}

	return &VolVarargs{
		zoneName:                 vol.zoneName,
		crossZone:                vol.crossZone,
		description:              vol.description,
		capacity:                 vol.Capacity,
		deleteLockTime:           vol.DeleteLockTime,
		followerRead:             vol.FollowerRead,
		authenticate:             vol.authenticate,
		dpSelectorName:           vol.dpSelectorName,
		dpSelectorParm:           vol.dpSelectorParm,
		enablePosixAcl:           vol.enablePosixAcl,
		enableQuota:              vol.enableQuota,
		dpReplicaNum:             vol.dpReplicaNum,
		enableTransaction:        vol.enableTransaction,
		txTimeout:                vol.txTimeout,
		txConflictRetryNum:       vol.txConflictRetryNum,
		txConflictRetryInterval:  vol.txConflictRetryInterval,
		txOpLimit:                vol.txOpLimit,
		coldArgs:                 args,
		dpReadOnlyWhenVolFull:    vol.DpReadOnlyWhenVolFull,
		accessTimeValidInterval:  vol.AccessTimeValidInterval,
		trashInterval:            vol.TrashInterval,
		enablePersistAccessTime:  vol.EnablePersistAccessTime,
		enableAutoDpMetaRepair:   vol.EnableAutoMetaRepair.Load(),
		volStorageClass:          vol.volStorageClass,
		allowedStorageClass:      append([]uint32{}, vol.allowedStorageClass...),
		forbidWriteOpOfProtoVer0: vol.ForbidWriteOpOfProtoVer0.Load(),
		quotaByClass:             quotaByClass,
	}
}

func (vol *Vol) initQuotaManager(c *Cluster) {
	vol.quotaManager = &MasterQuotaManager{
		MpQuotaInfoMap: make(map[uint64][]*proto.QuotaReportInfo),
		IdQuotaInfoMap: make(map[uint32]*proto.QuotaInfo),
		c:              c,
		vol:            vol,
	}
}

func (vol *Vol) loadQuotaManager(c *Cluster) (err error) {
	vol.quotaManager = &MasterQuotaManager{
		MpQuotaInfoMap: make(map[uint64][]*proto.QuotaReportInfo),
		IdQuotaInfoMap: make(map[uint32]*proto.QuotaInfo),
		c:              c,
		vol:            vol,
	}

	result, err := c.fsm.store.SeekForPrefix([]byte(quotaPrefix + strconv.FormatUint(vol.ID, 10) + keySeparator))
	if err != nil {
		err = fmt.Errorf("loadQuotaManager get quota failed, err [%v]", err)
		return err
	}

	for _, value := range result {
		quotaInfo := &proto.QuotaInfo{}

		if err = json.Unmarshal(value, quotaInfo); err != nil {
			log.LogErrorf("loadQuotaManager Unmarshal fail err [%v]", err)
			return err
		}
		log.LogDebugf("loadQuotaManager info [%v]", quotaInfo)
		if vol.Name != quotaInfo.VolName {
			panic(fmt.Sprintf("vol name do not match vol name [%v], quotaInfo vol name [%v]", vol.Name, quotaInfo.VolName))
		}
		vol.quotaManager.IdQuotaInfoMap[quotaInfo.QuotaId] = quotaInfo
	}

	return err
}

func (vol *Vol) checkDataReplicaMeta(c *Cluster) (cnt int) {
	partitions := vol.dataPartitions.clonePartitions()
	checkMetaDp := make(map[uint64]*DataPartition)
	checkMetaPool := routinepool.NewRoutinePool(c.GetAutoDpMetaRepairParallelCnt())
	defer checkMetaPool.WaitAndClose()
	var checkMetaDpWg sync.WaitGroup

	for _, dp := range partitions {
		// NOTE: cluster or enable meta repair
		if c.getEnableAutoDpMetaRepair() || vol.EnableAutoMetaRepair.Load() {
			checkMetaDp[dp.PartitionID] = dp
			localDp := dp
			checkMetaDpWg.Add(1)
			checkMetaPool.Submit(func() {
				defer checkMetaDpWg.Done()
				log.LogDebugf("[checkDataPartitions] check meta for vol(%v) dp(%v)", dp.VolName, dp.PartitionID)
				localDp.checkReplicaMeta(c)
			})
			continue
		}
	}

	if len(checkMetaDp) != 0 {
		checkMetaDpWg.Wait()
	}
	return
}

func (vol *Vol) isStorageClassInAllowed(storageClass uint32) (in bool) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()

	for _, asc := range vol.allowedStorageClass {
		if asc == storageClass {
			in = true
		}
	}

	return in
}
