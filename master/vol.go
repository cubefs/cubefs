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
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
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
	metaFollowerRead         bool
	metaNearRead             bool
	directRead               bool
	ignoreTinyRecover        bool
	maximallyRead            bool
	authenticate             bool
	dpSelectorName           string
	dpSelectorParm           string
	coldArgs                 *coldVolArgs
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
	enableAutoMpMetaRepair   bool
	accessTimeValidInterval  int64
	enablePersistAccessTime  bool
	leaderRetryTimeout       int64
	volStorageClass          uint32
	allowedStorageClass      []uint32
	forbidWriteOpOfProtoVer0 bool
	quotaByClass             map[uint32]uint64
	quotaByPool              map[uint8]uint64

	remoteCacheEnable            bool
	remoteCachePath              string
	remoteCacheAutoPrepare       bool
	remoteCacheTTL               int64
	remoteCacheReadTimeout       int64 // ms
	remoteCacheMaxFileSizeGB     int64
	remoteCacheMaxFileSizeMB     int64
	remoteCacheOnlyForNotSSD     bool
	remoteCacheMultiRead         bool
	flashNodeTimeoutCount        int64
	remoteCacheSameZoneTimeout   int64 // microsecond
	remoteCacheSameRegionTimeout int64 // ms
	remoteCacheDisableTTL        bool
	DefaultStoreMode             proto.StoreMode
	defaultPoolId                uint8
	allowedPools                 []uint8
	DpTag                        string
	MpTag                        string

	// Meta Region
	defaultRegion  string
	allowedRegions []string

	// MP Policy
	mpPolicy map[string]*proto.VolMpPolicy // by region
}

// nolint: structcheck
type CacheSubItem struct {
	EbsBlkSize int
}

// nolint: structcheck
type TxSubItem struct {
	remoteCacheEnable            bool
	remoteCachePath              string
	remoteCacheAutoPrepare       bool
	remoteCacheTTL               int64
	remoteCacheReadTimeout       int64 // ms
	remoteCacheMaxFileSizeGB     int64
	remoteCacheMaxFileSizeMB     int64
	remoteCacheOnlyForNotSSD     bool
	remoteCacheMultiRead         bool
	flashNodeTimeoutCount        int64
	remoteCacheSameZoneTimeout   int64 // microsecond
	remoteCacheSameRegionTimeout int64 // ms
	remoteCacheDisableTTL        bool

	PreloadCacheOn          bool
	NeedToLowerReplica      bool
	FollowerRead            bool
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	txOpLimit               int
	enableTransaction       proto.TxOpMask
}

// nolint: structcheck
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

// nolint: structcheck
type AuthenticSubItem struct {
	OSSAccessKey   string
	OSSSecretKey   string
	authenticate   bool
	enablePosixAcl bool
	authKey        string
}

// nolint: structcheck
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
	MetaFollowerRead         bool
	MetaNearRead             bool
	DirectRead               bool
	IgnoreTinyRecover        bool
	MaximallyRead            bool
	enableQuota              bool
	DisableAuditLog          bool
	DpReadOnlyWhenVolFull    bool // only if this switch is on, all dp becomes readonly when vol is full
	ReadOnlyForVolFull       bool // only if the switch DpReadOnlyWhenVolFull is on, mark vol is readonly when is full
	AccessTimeInterval       int64
	EnablePersistAccessTime  bool
	AccessTimeValidInterval  int64
	LeaderRetryTimeout       int64 // s
	EnableAutoDpMetaRepair   atomicutil.Bool
	EnableAutoMpMetaRepair   atomicutil.Bool
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
	hasAclMgr       bool

	mpsLock *mpsLockManager
	volLock sync.RWMutex

	// hybrid cloud
	allowedStorageClass []uint32 // specifies which storageClasses the vol use, a cluster may have multiple StorageClasses
	volStorageClass     uint32   // specifies which storageClass is written, unless dirStorageClass is set in file path

	StatByStorageClass      []*proto.StatOfStorageClass
	StatMigrateStorageClass []*proto.StatOfStorageClass
	StatByPool              []*proto.StatOfStorageClass
	StatByMigratePool       []*proto.StatOfStorageClass

	StatByDpMediaType []*proto.StatOfStorageClass
	StatByDpPool      []*proto.StatOfStorageClass

	QuotaByClass  []*proto.StatOfStorageClass
	QuotaByPoolId []*proto.StatOfStorageClass

	DefaultStoreMode proto.StoreMode

	// Storage Pool
	defaultPoolId uint8   // default pool ID for writing
	allowedPools  []uint8 // allowed pool IDs for this volume
	SelectType    int32
	DpTag         string // format: 'group1,group2,group3'. or ',,group'. Default value is ""
	MpTag         string // format: 'group1,group2,group3'. or ',,group'. Default value is ""

	// Meta Region
	defaultRegion        string   // default region for this volume
	allowedRegions       []string // allowed regions for this volume
	lastAutoCreateMpTime time.Time
	mpPolicy             map[string]*proto.VolMpPolicy // by region
}

func newVol(vv volValue) (vol *Vol) {
	vol = &Vol{ID: vv.ID, Name: vv.Name, MetaPartitions: make(map[uint64]*MetaPartition)}

	vol.lastAutoCreateMpTime = time.Now()
	vol.dataPartitions = newDataPartitionMap(vv.Name)
	vol.VersionMgr = newVersionMgr(vol)
	vol.dpReplicaNum = vv.DpReplicaNum
	vol.mpReplicaNum = vv.ReplicaNum
	vol.Owner = vv.Owner

	vol.dataPartitionSize = vv.DataPartitionSize
	vol.Capacity = vv.Capacity
	vol.FollowerRead = vv.FollowerRead
	vol.MetaFollowerRead = vv.MetaFollowerRead
	vol.MetaNearRead = vv.MetaNearRead
	vol.DirectRead = vv.DirectRead
	vol.IgnoreTinyRecover = vv.IgnoreTinyRecover
	vol.MaximallyRead = vv.MaximallyRead
	vol.LeaderRetryTimeout = vv.LeaderRetryTimeOut
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
	vol.Status = vv.Status
	vol.remoteCachePath = vv.RemoteCachePath
	vol.remoteCacheAutoPrepare = vv.RemoteCacheAutoPrepare
	vol.remoteCacheTTL = vv.RemoteCacheTTL
	vol.remoteCacheReadTimeout = vv.RemoteCacheReadTimeout
	vol.remoteCacheEnable = vv.RemoteCacheEnable
	vol.remoteCacheMaxFileSizeGB = vv.RemoteCacheMaxFileSizeGB
	vol.remoteCacheMaxFileSizeMB = vv.RemoteCacheMaxFileSizeMB
	vol.remoteCacheOnlyForNotSSD = vv.RemoteCacheOnlyForNotSSD
	vol.remoteCacheMultiRead = vv.RemoteCacheMultiRead
	vol.flashNodeTimeoutCount = vv.FlashNodeTimeoutCount
	vol.remoteCacheSameZoneTimeout = vv.RemoteCacheSameZoneTimeout
	vol.remoteCacheSameRegionTimeout = vv.RemoteCacheSameRegionTimeout
	vol.remoteCacheDisableTTL = vv.RemoteCacheDisableTTL

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
	vol.dpRepairBlockSize = proto.DefaultDpRepairBlockSize
	vol.EnableAutoDpMetaRepair.Store(defaultEnableDpMetaRepair)
	vol.EnableAutoMpMetaRepair.Store(defaultEnableMpMetaRepair)
	vol.TrashInterval = vv.TrashInterval
	vol.AccessTimeValidInterval = vv.AccessTimeInterval
	vol.EnablePersistAccessTime = vv.EnablePersistAccessTime

	vol.allowedStorageClass = make([]uint32, len(vv.AllowedStorageClass))
	copy(vol.allowedStorageClass, vv.AllowedStorageClass)
	vol.volStorageClass = vv.VolStorageClass
	vol.StatByStorageClass = make([]*proto.StatOfStorageClass, 0)
	vol.StatMigrateStorageClass = make([]*proto.StatOfStorageClass, 0)
	vol.StatByPool = make([]*proto.StatOfStorageClass, 0)
	vol.StatByMigratePool = make([]*proto.StatOfStorageClass, 0)
	vol.ForbidWriteOpOfProtoVer0.Store(defaultVolForbidWriteOpOfProtoVersion0)

	// Load pool configuration
	vol.defaultPoolId = vv.DefaultPoolId
	vol.allowedPools = make([]uint8, len(vv.AllowedPools))
	copy(vol.allowedPools, vv.AllowedPools)

	// Load region configuration
	vol.defaultRegion = vv.DefaultRegion
	if len(vv.AllowedRegions) > 0 {
		vol.allowedRegions = make([]string, len(vv.AllowedRegions))
		copy(vol.allowedRegions, vv.AllowedRegions)
	} else {
		// Default to default region if not set
		if vol.defaultRegion == "" {
			vol.defaultRegion = proto.DefaultRegion
		}
		vol.allowedRegions = []string{vol.defaultRegion}
	}

	vol.QuotaByClass = vv.QuotaOfClass
	if len(vol.QuotaByClass) == 0 {
		for _, c := range vol.allowedStorageClass {
			vol.QuotaByClass = append(vol.QuotaByClass, proto.NewStatOfStorageClass(c))
		}
	}

	vol.QuotaByPoolId = vv.QuotaOfPool
	if len(vol.QuotaByPoolId) == 0 {
		for _, c := range vol.allowedPools {
			vol.QuotaByPoolId = append(vol.QuotaByPoolId, proto.NewStatOfStorageClassByPool(c))
		}
	}

	vol.quotaManager = &MasterQuotaManager{
		MpQuotaInfoMap: make(map[uint64][]*proto.QuotaReportInfo),
		IdQuotaInfoMap: make(map[uint32]*proto.QuotaInfo),
		vol:            vol,
	}
	vol.DefaultStoreMode = vv.DefaultStoreMode

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
	vol.EnableAutoDpMetaRepair.Store(vv.EnableAutoDpMetaRepair)
	vol.EnableAutoMpMetaRepair.Store(vv.EnableAutoMpMetaRepair)
	vol.EnablePersistAccessTime = vv.EnablePersistAccessTime
	vol.AccessTimeValidInterval = vv.AccessTimeInterval
	if vol.AccessTimeValidInterval == 0 {
		vol.AccessTimeValidInterval = proto.DefaultAccessTimeValidInterval
	}
	vol.ForbidWriteOpOfProtoVer0.Store(vv.ForbidWriteOpOfProtoVer0)
	vol.DefaultStoreMode = vv.DefaultStoreMode

	if vol.remoteCacheTTL == 0 {
		vol.remoteCacheTTL = proto.DefaultRemoteCacheTTL
	}
	if vol.remoteCacheReadTimeout == 0 {
		vol.remoteCacheReadTimeout = proto.DefaultRemoteCacheClientReadTimeout
	}
	if vol.remoteCacheReadTimeout == proto.ReadDeadlineTime {
		vol.remoteCacheReadTimeout = proto.DefaultRemoteCacheClientReadTimeout
	}
	if vol.remoteCacheMaxFileSizeGB == 0 {
		vol.remoteCacheMaxFileSizeGB = proto.DefaultRemoteCacheMaxFileSizeGB
	}
	if vol.remoteCacheMaxFileSizeMB == 0 {
		vol.remoteCacheMaxFileSizeMB = proto.DefaultRemoteCacheMaxFileSizeMB
	}
	if vol.flashNodeTimeoutCount == 0 {
		vol.flashNodeTimeoutCount = proto.DefaultFlashNodeTimeoutCount
	}
	if vol.remoteCacheSameZoneTimeout == 0 {
		vol.remoteCacheSameZoneTimeout = proto.DefaultRemoteCacheSameZoneTimeout
	}
	if vol.remoteCacheSameRegionTimeout == 0 {
		vol.remoteCacheSameRegionTimeout = proto.DefaultRemoteCacheSameRegionTimeout
	}
	vol.DpTag = vv.DpTag
	vol.MpTag = vv.MpTag

	// MP Policy
	if vv.MpPolicy != nil && len(vv.MpPolicy) > 0 {
		vol.mpPolicy = make(map[string]*proto.VolMpPolicy)
		for k, v := range vv.MpPolicy {
			if v != nil {
				vol.mpPolicy[k] = v.Copy()
			}
		}
	}

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
	for range ticker.C {
		{
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

func (vol *Vol) IsDeleted() bool {
	return (vol.Status == proto.VolStatusMarkDelete && !vol.Forbidden) || (vol.Status == proto.VolStatusMarkDelete && vol.Forbidden && vol.DeleteExecTime.Before(time.Now()))
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
			if vol.isUnavailable() {
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
				if vol.VersionMgr.strategy.GetPeriodicSecond() == 0 || !vol.VersionMgr.strategy.Enable { // strategy not be set
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

func (vol *Vol) initQosManager(limitArgs *qosArgs) {
	vol.qosManager = &QosCtrlManager{
		cliInfoMgrMap:        make(map[uint64]*ClientInfoMgr),
		serverFactorLimitMap: make(map[uint32]*ServerFactorLimit),
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

func (vol *Vol) maxMetaPartitionID() (maxPartitionID uint64) {
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
			log.LogInfof("The mp[%v] of vol[%v] is not RW", mp.PartitionID, vol.Name)
		}
	}
	return num, true
}

func (vol *Vol) getDataPartitionsView(poolAware bool) (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(false, 0, vol, poolAware)
}

func (vol *Vol) getDataPartitionViewCompress(poolAware bool) (body []byte, err error) {
	return vol.dataPartitions.updateCompressCache(false, 0, vol, poolAware)
}

func (vol *Vol) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return vol.dataPartitions.get(partitionID)
}

func (vol *Vol) createModeString() string {
	return fmt.Sprintf("crossZone: %v, zoneName: %v, mediaType: %v, mpReplicaNum: %v, dpReplicaNum: %v, storeMode: %v, allowedStorageClass: %v",
		vol.crossZone, vol.zoneName, vol.volStorageClass, vol.mpReplicaNum, vol.dpReplicaNum, vol.DefaultStoreMode, vol.allowedStorageClass)
}

func (vol *Vol) sameCreateMode(v1 *Vol) bool {
	if v1 == nil {
		return false
	}

	if vol.crossZone != v1.crossZone ||
		vol.zoneName != v1.zoneName ||
		vol.VolType != v1.VolType ||
		vol.mpReplicaNum != v1.mpReplicaNum ||
		vol.dpReplicaNum != v1.dpReplicaNum ||
		vol.DefaultStoreMode != v1.DefaultStoreMode {
		return false
	}

	return vol.compareStorageClasses(v1.allowedStorageClass)
}

func (vol *Vol) compareStorageClasses(other []uint32) bool {
	if len(vol.allowedStorageClass) != len(other) {
		return false
	}

	volMap := make(map[uint32]bool, len(vol.allowedStorageClass))
	for _, sc := range vol.allowedStorageClass {
		volMap[sc] = true
	}

	for _, sc := range other {
		if !volMap[sc] {
			return false
		}
	}

	return true
}

func (vol *Vol) addMetaPartitions(c *Cluster, count int, region string) (err error) {
	// add extra meta partitions at a time
	var (
		start uint64
		end   uint64
	)

	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()

	// Use specified region, or fall back to volume's default region
	if region == "" {
		region = vol.defaultRegion
	}

	// update End of the maxMetaPartition range
	maxPartitionId := vol.maxMetaPartitionID()
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

		if err = vol.createMetaPartition(c, start, end, region); err != nil {
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

	vol.mpsLock.RLock()
	existingCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()
	end = gConfig.MetaPartitionInodeIdStep * uint64(existingCount)

	vol.createMpMutex.Lock()
	for index := existingCount; index < count; index++ {
		if index != 0 {
			start = end + 1
		}
		end = gConfig.MetaPartitionInodeIdStep * uint64(index+1)
		if index == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}
		if err = vol.createMetaPartition(c, start, end, vol.defaultRegion); err != nil {
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

func (vol *Vol) isAllowedPool(poolId uint8) bool {
	for _, p := range vol.allowedPools {
		if p == poolId {
			return true
		}
	}
	return false
}

func (vol *Vol) checkDataPartitions(c *Cluster) (cnt int) {
	shouldDpInhibitWriteByVolFull := vol.shouldInhibitWriteBySpaceFull()
	vol.SetReadOnlyForVolFull(shouldDpInhibitWriteByVolFull)

	statsByPoolId := vol.getStorageStatWithPoolId()
	for _, stat := range statsByPoolId {
		log.LogDebugf("checkDataPartitions: try setPartitionsRdOnlyWithPoolId, rdOnly(%v), stat %s, name %s",
			vol.DpReadOnlyWhenVolFull, stat.String(), vol.Name)
	}

	if vol.Status != proto.VolStatusMarkDelete && vol.Status != proto.VolStatusInitFailed && vol.Status != proto.VolStatusInitializing &&
		(time.Now().Unix()-vol.createTime >= defaultIntervalToCheckDataPartition) {
		for _, poolId := range vol.allowedPools {
			pool, err := c.getStoragePool(poolId)
			if err != nil {
				log.LogWarnf("[checkDataPartitions] vol(%v) poolId(%v) not found, skip", vol.Name, poolId)
				continue
			}

			if !proto.IsStorageClassReplica(uint32(pool.StorageClass)) {
				continue
			}

			dpCntOfPoolId := vol.dataPartitions.getDataPartitionsCountOfPool(poolId)
			if dpCntOfPoolId == 0 {
				log.LogInfof("[checkDataPartitions] vol(%v) poolId(%v) dp count is 0, try to create 1 dp",
					vol.Name, poolId)
				c.batchCreateDataPartition(vol, 1, false, poolId)
			}
		}
	}

	var rwDpCountOfSSD int
	var rwDpCountOfHDD int

	partitions := vol.dataPartitions.clonePartitions()

	statByMedia := map[uint32]uint64{}
	statByPoolId := map[uint8]uint64{}
	dpCntByPoolId := map[uint8]int{}

	defer func() {
		datas := make([]*proto.StatOfStorageClass, 0, len(statByMedia))
		for t, c := range statByMedia {
			datas = append(datas, &proto.StatOfStorageClass{
				StorageClass:  t,
				UsedSizeBytes: c,
			})
		}

		datasByPoolId := make([]*proto.StatOfStorageClass, 0, len(statByPoolId))
		for p, c := range statByPoolId {
			datasByPoolId = append(datasByPoolId, &proto.StatOfStorageClass{
				PoolId:        p,
				UsedSizeBytes: c,
			})
		}

		vol.StatByDpMediaType = datas
		vol.StatByDpPool = datasByPoolId
	}()

	for _, dp := range partitions {
		if dp.IsDiscard {
			continue
		}

		statByMedia[dp.MediaType] += dp.getMaxUsedSpace()
		statByPoolId[dp.PoolId] += dp.getMaxUsedSpace()

		dpRdOnly := shouldDpInhibitWriteByVolFull
		if stat := statsByPoolId[dp.PoolId]; stat.Full() && vol.DpReadOnlyWhenVolFull {
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
			dpCntByPoolId[dp.PoolId]++
		}

		dp.checkDiskError(c.Name, c.leaderInfo.addr)

		dp.checkReplicationTask(c.Name, vol.dataPartitionSize)
	}

	vol.dataPartitions.setReadWriteDataPartitionCntByMediaType(rwDpCountOfHDD, proto.MediaType_HDD)
	vol.dataPartitions.setReadWriteDataPartitionCntByMediaType(rwDpCountOfSSD, proto.MediaType_SSD)
	vol.dataPartitions.setReadWriteCntByPoolId(dpCntByPoolId)

	log.LogInfof("[checkDataPartitions] vol(%v), rwDpCountOfHDD(%v), rwDpCountOfSSD(%v), dpCntByPoolId(%v)",
		vol.Name, rwDpCountOfHDD, rwDpCountOfSSD, dpCntByPoolId)
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

func (vol *Vol) getQuotaByPoolId() map[uint8]uint64 {
	m := make(map[uint8]uint64)
	for _, c := range vol.QuotaByPoolId {
		m[c.PoolId] = c.QuotaGB
	}
	return m
}

func (vol *Vol) getStorageStatWithPoolId() map[uint8]*proto.StatOfStorageClass {
	usedByPoolId := make(map[uint8]uint64)
	quotaByPoolId := vol.getQuotaByPoolId()

	vol.rangeMetaPartition(func(mp *MetaPartition) bool {
		for _, mpStat := range mp.StatByPool {
			usedByPoolId[mpStat.PoolId] += mpStat.UsedSizeBytes
		}
		return true
	})

	totalStats := make(map[uint8]*proto.StatOfStorageClass, len(usedByPoolId))
	for p, u := range usedByPoolId {
		totalStats[p] = &proto.StatOfStorageClass{
			UsedSizeBytes: u,
			QuotaGB:       quotaByPoolId[p],
		}
	}

	return totalStats
}

func (vol *Vol) checkMetaPartitions(c *Cluster) {
	var tasks []*proto.AdminTask
	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	maxPartitionID := vol.maxMetaPartitionID()
	mps := vol.cloneMetaPartitionMap()

	var (
		doSplit                    bool
		err                        error
		stat                       *proto.StatOfStorageClass
		volMigrateStat             *proto.StatOfStorageClass
		ok                         bool
		statByStorageClassMap      map[uint32]*proto.StatOfStorageClass
		statMigrateStorageClassMap map[uint32]*proto.StatOfStorageClass
		statByPoolMap              map[uint8]*proto.StatOfStorageClass
		statByMigratePoolMap       map[uint8]*proto.StatOfStorageClass
	)

	statByStorageClassMap = make(map[uint32]*proto.StatOfStorageClass)
	statMigrateStorageClassMap = make(map[uint32]*proto.StatOfStorageClass)
	statByPoolMap = make(map[uint8]*proto.StatOfStorageClass)
	statByMigratePoolMap = make(map[uint8]*proto.StatOfStorageClass)
	quotaByClass := vol.getQuotaByClass()
	quotaByPoolId := vol.getQuotaByPoolId()

	for _, pool := range vol.allowedPools {
		statByPoolMap[pool] = proto.NewStatOfStorageClassByPoolWithQuota(pool, quotaByPoolId[pool])
		statByMigratePoolMap[pool] = proto.NewStatOfStorageClassByPool(pool)
	}

	for _, mp := range mps {
		doSplit = mp.checkStatus(c.Name, true, int(vol.mpReplicaNum), maxPartitionID, metaPartitionInodeIdStep, vol.Forbidden, c.getMetaPartitionTimeoutSec())
		if doSplit && !c.cfg.DisableAutoCreate {
			nextStart := mp.MaxInodeID + metaPartitionInodeIdStep
			log.LogInfof(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits start[%v] maxinodeid:[%v] default step:[%v],nextStart[%v]",
				c.Name, vol.Name, mp.PartitionID, mp.Start, mp.MaxInodeID, metaPartitionInodeIdStep, nextStart))
			if err = vol.splitMetaPartition(c, mp, nextStart, metaPartitionInodeIdStep, false, mp.Region); err != nil {
				Warn(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]", c.Name, vol.Name, mp.PartitionID, err))
			}
		}

		mp.checkLeader(c.Name, c.getMetaPartitionTimeoutSec())
		mp.checkReplicaNum(c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(c, maxPartitionID)
		mp.reportMissingReplicas(c.Name, c.leaderInfo.addr, c.getMetaPartitionTimeoutSec(), defaultIntervalToAlarmMissingMetaPartition)
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

		for _, mpStat := range mp.StatByPool {
			if stat, ok = statByPoolMap[mpStat.PoolId]; !ok {
				stat = proto.NewStatOfStorageClassByPoolWithQuota(mpStat.PoolId, quotaByPoolId[mpStat.PoolId])
				statByPoolMap[mpStat.PoolId] = stat
			}

			stat.InodeCount += mpStat.InodeCount
			stat.UsedSizeBytes += mpStat.UsedSizeBytes
		}

		for _, mpMigrateStat := range mp.StatByMigratePool {
			if stat, ok = statByMigratePoolMap[mpMigrateStat.PoolId]; !ok {
				stat = proto.NewStatOfStorageClassByPool(mpMigrateStat.PoolId)
				statByMigratePoolMap[mpMigrateStat.PoolId] = stat
			}

			stat.InodeCount += mpMigrateStat.InodeCount
			stat.UsedSizeBytes += mpMigrateStat.UsedSizeBytes
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

	StatByPoolSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat = range statByPoolMap {
		StatByPoolSlice = append(StatByPoolSlice, stat)
	}
	vol.StatByPool = StatByPoolSlice

	StatByMigratePoolSlice := make([]*proto.StatOfStorageClass, 0)
	for _, stat = range statByMigratePoolMap {
		StatByMigratePoolSlice = append(StatByMigratePoolSlice, stat)
	}
	vol.StatByMigratePool = StatByMigratePoolSlice

	c.addMetaNodeTasks(tasks)
	vol.checkSplitMetaPartition(c, metaPartitionInodeIdStep)

	vol.checkAutoMetaPartitionCreationByRegion(c)
}

func (vol *Vol) checkSplitMetaPartition(c *Cluster, metaPartitionInodeStep uint64) {
	maxPartitionID := vol.maxMetaPartitionID()
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
		if err := vol.splitMetaPartition(c, maxMP, end, metaPartitionInodeStep, true, maxMP.Region); err != nil {
			msg := fmt.Sprintf("action[checkSplitMetaPartition],split meta maxMP[%v] failed,err[%v]\n",
				maxMP.PartitionID, err)
			Warn(c.Name, msg)
		}
		log.LogInfof("volume[%v] split MaxMP[%v], MaxInodeID[%d] Start[%d] RWMPNum[%d] maxMPInodeUsedRatio[%.2f]",
			vol.Name, maxPartitionID, maxMP.MaxInodeID, maxMP.Start, RWMPNum, maxMPInodeUsedRatio)
	}
}

func (mp *MetaPartition) memUsedReachThreshold(clusterName, volName string) bool {
	liveReplicas := mp.getLiveReplicas(defaultMetaPartitionTimeOutSec)
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
	if readonlyReplica.metaNode.isWritable(readonlyReplica.StoreMode) {
		msg := fmt.Sprintf("action[checkSplitMetaPartition] vol[%v],max meta parition[%v] status is readonly\n",
			volName, mp.PartitionID)
		Warn(clusterName, msg)
		return false
	}
	return true
}

func (vol *Vol) cloneMetaPartitionMap() (mps map[uint64]*MetaPartition) {
	mps = make(map[uint64]*MetaPartition)
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
	dps = make(map[uint64]*DataPartition)
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

func (vol *Vol) AllPartitionForbidVer0() bool {
	if !vol.ForbidWriteOpOfProtoVer0.Load() {
		return false
	}

	fobidden := true
	vol.dataPartitions.RLock()
	for _, dp := range vol.dataPartitions.partitionMap {
		if !dp.ForbidWriteOpOfProtoVer0 {

			// consider abnormal dp
			if dp.allUnavailable() {
				log.LogWarnf("AllPartitionForbidVer0: dp %d may be abnormal, no need to check.", dp.PartitionID)
				continue
			}

			log.LogWarnf("AllPartitionForbidVer0: dp %d is still forbidden false.", dp.PartitionID)
			fobidden = false
			break
		}
	}
	vol.dataPartitions.RUnlock()

	if !fobidden {
		return false
	}

	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		if !mp.ForbidWriteOpOfProtoVer0 {
			log.LogWarnf("AllPartitionForbidVer0: mp %d is still forbidden false.", mp.PartitionID)
			return false
		}
	}

	return true
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

	if vol.status() == proto.VolStatusInitFailed || vol.status() == proto.VolStatusInitializing {
		err = proto.ErrVolNotReady
		return
	}

	if vol.capacity() == 0 {
		err = proto.ErrVolNoAvailableSpace
		return
	}

	if !proto.IsStorageClassBlobStore(vol.volStorageClass) {
		if vol.IsReadOnlyForVolFull() {
			vol.setAllDataPartitionsToReadOnly()
			err = proto.ErrVolNoAvailableSpace
			return
		}
		ok = true
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
		for _, poolId := range vol.allowedPools {
			pool, err := c.getStoragePool(poolId)
			if err != nil {
				log.LogWarnf("autoCreateDataPartitions: vol(%v) poolId(%v) not found, skip", vol.Name, poolId)
				continue
			}

			if !proto.IsStorageClassReplica(uint32(pool.StorageClass)) {
				continue
			}

			rwDpCntOfPool := vol.dataPartitions.getReadWriteDataPartitionCntByPool(poolId)
			if rwDpCntOfPool < minNumOfRWDataPartitions {
				log.LogWarnf("autoCreateDataPartitions: vol(%v) poolId(%v) rwDpCount less than %v, alloc new partitions",
					vol.Name, poolId, minNumOfRWDataPartitions)
				c.batchCreateDataPartition(vol, minNumOfRWDataPartitions-rwDpCntOfPool, false, poolId)
			}
		}
		return
	}

	statByPoolId := vol.getStorageStatWithPoolId()
	// check for hot vol
	for _, poolId := range vol.allowedPools {
		pool, err := c.getStoragePool(poolId)
		if err != nil {
			log.LogWarnf("autoCreateDataPartitions: vol(%v) poolId(%v) not found, skip", vol.Name, poolId)
			continue
		}

		if !proto.IsStorageClassReplica(uint32(pool.StorageClass)) {
			continue
		}

		stat := statByPoolId[poolId]
		if vol.DpReadOnlyWhenVolFull && stat.Full() {
			log.LogInfof("action[autoCreateDataPartitions] target poolId meet cap limit, can't create, vol %v, poolId %v", vol.Name, poolId)
			continue
		}

		rwDpCntOfPool := vol.dataPartitions.getReadWriteDataPartitionCntByPool(poolId)
		log.LogInfof("action[autoCreateDataPartitions] vol(%v) poolId:%v, rwDpCntOfPool:%v", vol.Name, poolId, rwDpCntOfPool)

		var createDpCount int
		if poolId == vol.defaultPoolId && vol.Capacity > 200000 && rwDpCntOfPool < 200 {
			createDpCount = vol.calculateExpansionNum()
			log.LogInfof("action[autoCreateDataPartitions] vol(%v) defaultPoolId(%v), calculated createDpCount:%v",
				vol.Name, poolId, createDpCount)
		} else if rwDpCntOfPool < minNumOfRWDataPartitions {
			createDpCount = minNumOfRWDataPartitions - rwDpCntOfPool
			log.LogInfof("action[autoCreateDataPartitions] vol(%v) poolId(%v), min createDpCount:%v",
				vol.Name, poolId, createDpCount)
		} else {
			continue
		}

		vol.dataPartitions.lastAutoCreateTime = time.Now()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] createDpCount[%v] for poolId(%v)",
			vol.Name, createDpCount, poolId)
		c.batchCreateDataPartition(vol, createDpCount, false, poolId)
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
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.createTime, vol.VolType, vol.DeleteLockTime)
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
}

func (vol *Vol) checkInitFailed(c *Cluster) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	deleteTime := int64(5 * 60)
	if vol.Status != proto.VolStatusInitFailed || vol.createTime > time.Now().Unix()-deleteTime {
		return
	}

	vol.Status = proto.VolStatusMarkDelete
	if err := c.syncUpdateVol(vol); err != nil {
		vol.Status = proto.VolStatusInitFailed
		log.LogErrorf("action[initFailed] vol[%v] update vol status to mark delete failed, err[%v]", vol.Name, err)
	}
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
			tasks = append(tasks, replica.createTaskToDeleteReplica(mp.PartitionID, false))
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

func (vol *Vol) doSplitMetaPartition(c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool, region string) (nextMp *MetaPartition, err error) {
	mp.Lock()
	defer mp.Unlock()

	if err = mp.canSplit(end, metaPartitionInodeIdStep, ignoreNoLeader); err != nil {
		return
	}

	log.LogWarnf("action[splitMetaPartition],partition[%v],start[%v],end[%v],new end[%v]", mp.PartitionID, mp.Start, mp.End, end)
	cmdMap := make(map[string]*RaftCmd)
	oldEnd := mp.End
	mp.End = end

	updateMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncUpdateMetaPartition, mp)
	if err != nil {
		return
	}

	cmdMap[updateMpRaftCmd.K] = updateMpRaftCmd
	if nextMp, err = vol.doCreateMetaPartition(c, mp.End+1, defaultMaxMetaPartitionInodeID, region); err != nil {
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

func (vol *Vol) splitMetaPartition(c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool, region string) (err error) {
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

	maxPartitionID := vol.maxMetaPartitionID()
	if maxPartitionID != mp.PartitionID {
		err = fmt.Errorf("mp[%v] is not the last meta partition[%v]", mp.PartitionID, maxPartitionID)
		return
	}

	nextMp, err := vol.doSplitMetaPartition(c, mp, end, metaPartitionInodeIdStep, ignoreNoLeader, region)
	if err != nil {
		return
	}

	vol.addMetaPartition(nextMp)
	log.LogWarnf("action[splitMetaPartition],next partition[%v],start[%v],end[%v]", nextMp.PartitionID, nextMp.Start, nextMp.End)
	return
}

func (vol *Vol) createMetaPartition(c *Cluster, start, end uint64, region string) (err error) {
	var mp *MetaPartition
	if mp, err = vol.doCreateMetaPartition(c, start, end, region); err != nil {
		return
	}
	if err = c.syncAddMetaPartition(mp); err != nil {
		return errors.NewError(err)
	}
	vol.addMetaPartition(mp)
	return
}

func (vol *Vol) doCreateMetaPartition(c *Cluster, start, end uint64, region string) (mp *MetaPartition, err error) {
	var (
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
		wg          sync.WaitGroup
	)

	errChannel := make(chan error, vol.mpReplicaNum)
	nodeType := TypeMetaPartition
	if vol.DefaultStoreMode == proto.StoreModeRocksDb {
		nodeType = TypeRocksdbPartition
	}
	if c.isFaultDomain(vol) {
		if hosts, peers, err = c.getHostFromDomainZone(vol.domainId, nodeType, vol.mpReplicaNum, proto.StorageClass_Unspecified); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromDomainZone err[%v]", err)
			return nil, errors.NewError(err)
		}
	} else {
		// Get hosts from specified region
		if hosts, peers, err = c.getHostFromNormalZoneForCreate(nodeType,
			int(vol.mpReplicaNum), vol.zoneName, proto.UnSpecifiedPoolId, c.getRackAwareLevel(), vol, region); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromNormalZoneForCreateWithRegion err[%v]", err)
			return nil, errors.NewError(err)
		}
	}

	if err = c.checkMultipleReplicasOnSameMachine(hosts); err != nil {
		return nil, err
	}

	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return nil, errors.NewError(err)
	}

	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, vol.Name, vol.ID, vol.VersionMgr.getLatestVer())
	mp.setHosts(hosts)
	mp.setPeers(peers)
	mp.Region = region // Set region for MP

	storeMode := proto.StoreModeMem
	if vol.DefaultStoreMode == proto.StoreModeRocksDb {
		storeMode = proto.StoreModeRocksDb
	}
	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			if err = c.syncCreateMetaPartitionToMetaNode(host, mp, storeMode); err != nil {
				log.LogErrorf("doCreateMetaPartition: create mp to metanode failed, mp %d, err %s", mp.PartitionID, err.Error())
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
				task := mr.createTaskToDeleteReplica(mp.PartitionID, false)
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
	vol.MetaFollowerRead = args.metaFollowerRead
	vol.MetaNearRead = args.metaNearRead
	vol.DirectRead = args.directRead
	vol.IgnoreTinyRecover = args.ignoreTinyRecover
	vol.MaximallyRead = args.maximallyRead
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
	vol.LeaderRetryTimeout = args.leaderRetryTimeout

	if proto.IsVolSupportStorageClass(args.allowedStorageClass, proto.StorageClass_BlobStore) {
		vol.EbsBlkSize = args.coldArgs.objBlockSize
	}

	if args.volStorageClass == proto.StorageClass_BlobStore {
		coldArgs := args.coldArgs
		vol.EbsBlkSize = coldArgs.objBlockSize
	}

	vol.description = args.description

	vol.dpSelectorName = args.dpSelectorName
	vol.dpSelectorParm = args.dpSelectorParm
	vol.TrashInterval = args.trashInterval
	vol.AccessTimeValidInterval = args.accessTimeValidInterval
	vol.AccessTimeInterval = args.accessTimeInterval
	vol.EnableAutoDpMetaRepair.Store(args.enableAutoDpMetaRepair)
	vol.EnableAutoMpMetaRepair.Store(args.enableAutoMpMetaRepair)
	vol.EnablePersistAccessTime = args.enablePersistAccessTime
	vol.volStorageClass = args.volStorageClass
	vol.allowedStorageClass = append([]uint32{}, args.allowedStorageClass...)
	vol.ForbidWriteOpOfProtoVer0.Store(args.forbidWriteOpOfProtoVer0)

	quotaClass := make([]*proto.StatOfStorageClass, 0, len(args.quotaByClass))
	for t, c := range args.quotaByClass {
		quotaClass = append(quotaClass, proto.NewStatOfStorageClassEx(t, c))
	}
	vol.QuotaByClass = quotaClass

	// Update quota by pool - merge with existing quotas
	if args.quotaByPool != nil && len(args.quotaByPool) > 0 {
		quotaByPool := make([]*proto.StatOfStorageClass, 0, len(args.quotaByPool))
		for poolId, quotaGB := range args.quotaByPool {
			quotaByPool = append(quotaByPool, proto.NewStatOfStorageClassByPoolWithQuota(poolId, quotaGB))
		}
		vol.QuotaByPoolId = quotaByPool
	}

	vol.remoteCacheEnable = args.remoteCacheEnable
	vol.remoteCachePath = args.remoteCachePath
	vol.remoteCacheAutoPrepare = args.remoteCacheAutoPrepare
	vol.remoteCacheTTL = args.remoteCacheTTL
	vol.remoteCacheReadTimeout = args.remoteCacheReadTimeout
	vol.remoteCacheMaxFileSizeGB = args.remoteCacheMaxFileSizeGB
	vol.remoteCacheMaxFileSizeMB = args.remoteCacheMaxFileSizeMB
	vol.remoteCacheOnlyForNotSSD = args.remoteCacheOnlyForNotSSD
	vol.remoteCacheMultiRead = args.remoteCacheMultiRead
	vol.flashNodeTimeoutCount = args.flashNodeTimeoutCount
	vol.remoteCacheSameZoneTimeout = args.remoteCacheSameZoneTimeout
	vol.remoteCacheSameRegionTimeout = args.remoteCacheSameRegionTimeout
	vol.remoteCacheDisableTTL = args.remoteCacheDisableTTL
	vol.DefaultStoreMode = args.DefaultStoreMode

	// Update pool configuration if provided
	if args.defaultPoolId != 0 {
		vol.defaultPoolId = args.defaultPoolId
	}
	if len(args.allowedPools) > 0 {
		vol.allowedPools = make([]uint8, len(args.allowedPools))
		copy(vol.allowedPools, args.allowedPools)
	}
	vol.DpTag = args.DpTag
	vol.MpTag = args.MpTag

	// Update region configuration if provided
	if args.defaultRegion != "" {
		vol.defaultRegion = args.defaultRegion
	}
	if len(args.allowedRegions) > 0 {
		vol.allowedRegions = make([]string, len(args.allowedRegions))
		copy(vol.allowedRegions, args.allowedRegions)
	}

	// Update MP Policy if provided
	if args.mpPolicy != nil {
		if vol.mpPolicy == nil {
			vol.mpPolicy = make(map[string]*proto.VolMpPolicy)
		}
		for k, v := range args.mpPolicy {
			if v != nil {
				vol.mpPolicy[k] = v.Copy()
			} else {
				delete(vol.mpPolicy, k)
			}
		}
	}
}

func getVolVarargs(vol *Vol) *VolVarargs {
	args := &coldVolArgs{
		objBlockSize:            vol.EbsBlkSize,
		accessTimeValidInterval: vol.AccessTimeValidInterval,
		trashInterval:           vol.TrashInterval,
		enablePersistAccessTime: vol.EnablePersistAccessTime,
	}

	quotaByClass := make(map[uint32]uint64)
	for _, c := range vol.QuotaByClass {
		quotaByClass[c.StorageClass] = c.QuotaGB
	}

	quotaByPool := make(map[uint8]uint64)
	for _, c := range vol.QuotaByPoolId {
		quotaByPool[c.PoolId] = c.QuotaGB
	}

	result := &VolVarargs{
		zoneName:                 vol.zoneName,
		crossZone:                vol.crossZone,
		description:              vol.description,
		capacity:                 vol.Capacity,
		deleteLockTime:           vol.DeleteLockTime,
		followerRead:             vol.FollowerRead,
		metaFollowerRead:         vol.MetaFollowerRead,
		metaNearRead:             vol.MetaNearRead,
		directRead:               vol.DirectRead,
		ignoreTinyRecover:        vol.IgnoreTinyRecover,
		maximallyRead:            vol.MaximallyRead,
		leaderRetryTimeout:       vol.LeaderRetryTimeout,
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
		enableAutoDpMetaRepair:   vol.EnableAutoDpMetaRepair.Load(),
		enableAutoMpMetaRepair:   vol.EnableAutoMpMetaRepair.Load(),
		volStorageClass:          vol.volStorageClass,
		allowedStorageClass:      append([]uint32{}, vol.allowedStorageClass...),
		forbidWriteOpOfProtoVer0: vol.ForbidWriteOpOfProtoVer0.Load(),
		quotaByClass:             quotaByClass,
		quotaByPool:              quotaByPool,

		remoteCacheEnable:            vol.remoteCacheEnable,
		remoteCachePath:              vol.remoteCachePath,
		remoteCacheAutoPrepare:       vol.remoteCacheAutoPrepare,
		remoteCacheTTL:               vol.remoteCacheTTL,
		remoteCacheReadTimeout:       vol.remoteCacheReadTimeout,
		remoteCacheMaxFileSizeGB:     vol.remoteCacheMaxFileSizeGB,
		remoteCacheMaxFileSizeMB:     vol.remoteCacheMaxFileSizeMB,
		remoteCacheOnlyForNotSSD:     vol.remoteCacheOnlyForNotSSD,
		remoteCacheMultiRead:         vol.remoteCacheMultiRead,
		flashNodeTimeoutCount:        vol.flashNodeTimeoutCount,
		remoteCacheSameZoneTimeout:   vol.remoteCacheSameZoneTimeout,
		remoteCacheSameRegionTimeout: vol.remoteCacheSameRegionTimeout,
		remoteCacheDisableTTL:        vol.remoteCacheDisableTTL,
		DefaultStoreMode:             vol.DefaultStoreMode,
		defaultPoolId:                vol.defaultPoolId,
		allowedPools:                 append([]uint8{}, vol.allowedPools...),
		DpTag:                        vol.DpTag,
		MpTag:                        vol.MpTag,
		defaultRegion:                vol.defaultRegion,
		allowedRegions:               append([]string{}, vol.allowedRegions...),
	}

	// MP Policy
	if vol.mpPolicy != nil && len(vol.mpPolicy) > 0 {
		result.mpPolicy = make(map[string]*proto.VolMpPolicy)
		for k, v := range vol.mpPolicy {
			if v != nil {
				result.mpPolicy[k] = v.Copy()
			}
		}
	}

	return result
}

func (vol *Vol) initQuotaManager(c *Cluster) {
	vol.quotaManager.c = c
}

func (vol *Vol) loadQuotaManager(c *Cluster) (err error) {
	vol.quotaManager.c = c

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
		if dp.IsDiscard {
			continue
		}
		// NOTE: cluster or enable meta repair
		if c.getEnableAutoDpMetaRepair() || vol.EnableAutoDpMetaRepair.Load() {
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

func (vol *Vol) checkMetaReplicaMeta(c *Cluster) (cnt int) {
	partitions := vol.getSortMetaPartitions()
	checkMetaMp := make(map[uint64]*MetaPartition)
	checkMetaPool := routinepool.NewRoutinePool(c.GetAutoMpMetaRepairParallelCnt())
	defer checkMetaPool.WaitAndClose()
	var checkMetaMpWg sync.WaitGroup

	for _, mp := range partitions {
		// cluster or vol level enable meta repair
		if c.getEnableAutoMpMetaRepair() || vol.EnableAutoMpMetaRepair.Load() {
			checkMetaMp[mp.PartitionID] = mp
			localMp := mp
			checkMetaMpWg.Add(1)
			checkMetaPool.Submit(func() {
				defer checkMetaMpWg.Done()
				log.LogDebugf("[checkMetaPartitions] check meta for vol(%v) mp(%v)", mp.volName, mp.PartitionID)
				localMp.checkReplicaMeta(c)
			})
			continue
		}
	}

	if len(checkMetaMp) != 0 {
		checkMetaMpWg.Wait()
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

func (vol *Vol) isPoolInAllowed(poolId uint8) (in bool) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()

	for _, ap := range vol.allowedPools {
		if ap == poolId {
			in = true
			return
		}
	}

	return
}

func (vol *Vol) isRegionInAllowed(region string) bool {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()

	for _, r := range vol.allowedRegions {
		if r == region {
			return true
		}
	}
	return false
}

func (vol *Vol) getSortMetaPartitions() (mps []*MetaPartition) {
	vol.mpsLock.RLock()
	mps = make([]*MetaPartition, 0, len(vol.MetaPartitions))
	for _, mp := range vol.MetaPartitions {
		mps = append(mps, mp)
	}
	vol.mpsLock.RUnlock()

	sort.Slice(mps, func(i, j int) bool { return mps[i].Start < mps[j].Start })

	return
}

func (vol *Vol) isInitializingOrInitFailed() bool {
	return vol.Status == proto.VolStatusInitializing || vol.Status == proto.VolStatusInitFailed
}

func (vol *Vol) isUnavailable() bool {
	return vol.isInitializingOrInitFailed() || vol.Status == proto.VolStatusMarkDelete
}

// getWritableMpCntByRegion returns the count of read-write meta partitions in a specific region
func (vol *Vol) getWritableMpCntByRegion(region string) int {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	count := 0
	for _, mp := range vol.MetaPartitions {
		if mp.Region == region && mp.Status == proto.ReadWrite {
			count++
		}
	}
	return count
}

// isMaxMpExceedThresholdInRegion checks if the max meta partition in a specific region exceeds the threshold
func (vol *Vol) isMaxMpExceedThresholdInRegion(region string) bool {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	threshold := 0.8 // 80% threshold

	// Find the max MP in this region
	var maxMP *MetaPartition
	var maxMPID uint64
	for mpID, mp := range vol.MetaPartitions {
		if mp.Region == region && mp.Status == proto.ReadWrite {
			if mpID > maxMPID {
				maxMP = mp
				maxMPID = mpID
			}
		}
	}

	if maxMP == nil {
		return false
	}

	// Calculate usage ratio: (MaxInodeID - Start) / InodeIdStep
	usageRatio := float64(maxMP.MaxInodeID-maxMP.Start) / float64(metaPartitionInodeIdStep)
	ok := usageRatio >= threshold
	if ok {
		log.LogInfof("action[isMaxMpExceedThresholdInRegion] vol(%v) region(%v), max MP exceeds threshold, usageRatio:%.2f, threshold:%v, mp ID:%v, start:%v, maxInodeID:%v",
			vol.Name, region, usageRatio, threshold, maxMP.PartitionID, maxMP.Start, maxMP.MaxInodeID)
	}

	return ok
}

// checkAutoMetaPartitionCreationByRegion checks each region and creates meta partitions if needed
func (vol *Vol) checkAutoMetaPartitionCreationByRegion(c *Cluster) {
	if time.Since(vol.lastAutoCreateMpTime) < time.Minute {
		return
	}

	log.LogInfof("action[checkAutoMetaPartitionCreationByRegion] vol(%v) lastAutoCreateMpTime(%v)",
		vol.Name, vol.lastAutoCreateMpTime)

	defer func() {
		vol.lastAutoCreateMpTime = time.Now()
		log.LogInfof("action[checkAutoMetaPartitionCreationByRegion] finished vol(%v) lastAutoCreateMpTime(%v)",
			vol.Name, vol.lastAutoCreateMpTime)
	}()

	if c.cfg.DisableAutoCreate {
		return
	}

	// Check each allowed region
	for _, region := range vol.allowedRegions {

		rwMpCntOfRegion := vol.getWritableMpCntByRegion(region)
		maxMpExceedThreshold := vol.isMaxMpExceedThresholdInRegion(region)
		log.LogInfof("action[checkAutoMetaPartitionCreationByRegion] vol(%v) region:%v, rwMpCntOfRegion:%v, maxMpExceedThreshold:%v",
			vol.Name, region, rwMpCntOfRegion, maxMpExceedThreshold)

		// Check if we need to create more MPs
		var createMpCount int
		minNumOfRWMetaPartitions := defaultInitMetaPartitionCount
		if rwMpCntOfRegion < minNumOfRWMetaPartitions {
			createMpCount = minNumOfRWMetaPartitions - rwMpCntOfRegion
			log.LogInfof("action[checkAutoMetaPartitionCreationByRegion] vol(%v) region(%v), min createMpCount:%v",
				vol.Name, region, createMpCount)
		} else if maxMpExceedThreshold {
			// If max MP exceeds threshold, create a new MP by splitting
			createMpCount = 1
			log.LogInfof("action[checkAutoMetaPartitionCreationByRegion] vol(%v) region(%v), max MP exceeds threshold, createMpCount:1",
				vol.Name, region)
		}
		vol.autoCreateMetaPartitionsForRegion(c, region, createMpCount)
	}
}

// autoCreateMetaPartitionsForRegion creates meta partitions in a specific region
func (vol *Vol) autoCreateMetaPartitionsForRegion(c *Cluster, region string, count int) {
	for i := 0; i < count; i++ {
		maxPartitionID := vol.maxMetaPartitionID()
		maxMP, err := vol.metaPartition(maxPartitionID)
		if err != nil {
			log.LogErrorf("action[autoCreateMetaPartitionsForRegion] vol(%v) region(%v), get max MP failed, err: %v", vol.Name, region, err)
			return
		}

		maxInodeID := maxMP.MaxInodeID
		if maxInodeID == 0 {
			maxInodeID = maxMP.Start
		}

		end := maxInodeID + gConfig.MetaPartitionInodeIdStep
		if err := vol.splitMetaPartition(c, maxMP, end, gConfig.MetaPartitionInodeIdStep, true, region); err != nil {
			log.LogErrorf("action[autoCreateMetaPartitionsForRegion],split meta maxMP[%v] failed,err[%v]\n", maxMP.PartitionID, err)
			return
		}

		log.LogInfof("action[autoCreateMetaPartitionsForRegion] vol[%v] region[%v] create meta partition success", vol.Name, region)
	}
}

// checkAndCreateMpLearnersByPolicy checks mpPolicy and creates learner replicas in other regions
func (vol *Vol) checkAndCreateMpLearnersByPolicy(c *Cluster) {
	log.LogInfof("checkAndCreateMpLearnersByPolicy: vol[%v], mpPolicy[%v]", vol.Name, vol.mpPolicy)

	type replicaInfo struct {
		peer      proto.Peer
		storeMode proto.StoreMode
		region    string
	}

	mps := vol.getSortMetaPartitions()
	// Iterate through all meta partitions
	for _, mp := range mps {

		log.LogDebugf("checkAndCreateMpLearnersByPolicy: mp[%v], mpPolicy[%v]", mp.PartitionID, vol.mpPolicy)

		mp.RLock()
		mpRegion := mp.Region
		mpID := mp.PartitionID
		mpPeers := make([]proto.Peer, len(mp.Peers))
		replicas := make([]*MetaReplica, len(mp.Replicas))
		copy(mpPeers, mp.Peers)
		copy(replicas, mp.Replicas)
		mp.RUnlock()

		policy, exists := vol.mpPolicy[mpRegion]
		if !exists || policy == nil || len(policy.Learner) == 0 {
			continue
		}

		regions := vol.allowedRegions
		for _, volRegion := range regions {
			learnerPolicy, exists := policy.Learner[volRegion]
			if !exists || learnerPolicy == nil {
				continue
			}

			rInfo := replicaInfo{}
			for _, peer := range mpPeers {
				if peer.Type != raftProto.PeerLearner || !peer.ManualPromote {
					continue
				}

				region := c.getRegionFromMetaNodeAddr(peer.Addr)
				if region != volRegion {
					continue
				}

				rInfo.peer = peer
				rInfo.region = region

				for _, r := range replicas {
					if r.metaNode.Addr == peer.Addr && r.StoreMode == learnerPolicy.Mode {
						rInfo.storeMode = r.StoreMode
						break
					}
				}

				if rInfo.storeMode == learnerPolicy.Mode {
					break
				}
			}

			if rInfo.storeMode == learnerPolicy.Mode {
				log.LogInfof("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] already has learner in region[%v], addr[%v]",
					vol.Name, mpID, volRegion, rInfo.peer.Addr)
				continue
			}

			param := &selectParam{
				replicaNum:   1,
				rackLevel:    c.getRackAwareLevel(),
				poolId:       proto.UnSpecifiedPoolId,
				region:       volRegion,
				excludeHosts: mp.Hosts,
			}

			if rInfo.peer.Addr != "" {
				if err := c.migrateMetaPartitionByLearner(rInfo.peer.Addr, "", mp, learnerPolicy.Mode, proto.MpManumalLearner); err != nil {
					log.LogErrorf("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] failed to migrate learner in region[%v], addr[%v], err[%v]",
						vol.Name, mpID, volRegion, rInfo.peer.Addr, err)
					continue
				}
				log.LogInfof("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] successfully added learner in region[%v], addr[%v]",
					vol.Name, mpID, volRegion, rInfo.peer.Addr)
				continue
			}

			nodeType := TypeMetaPartition
			if learnerPolicy.Mode == proto.StoreModeRocksDb {
				nodeType = TypeRocksdbPartition
			}

			hosts, _, err := c.getHostFromNormalZone(nodeType, nil, 1, "", param)
			if err != nil {
				log.LogErrorf("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] failed to get metanode from region[%v], err[%v]",
					vol.Name, mpID, volRegion, err)
				continue
			}

			if len(hosts) == 0 {
				log.LogErrorf("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] no available metanode in region[%v]",
					vol.Name, mpID, volRegion)
				continue
			}

			targetAddr := hosts[0]
			if err = c.addMetaReplicaLearner(mp, targetAddr, learnerPolicy.Mode, "", true, proto.MpManumalLearner); err != nil {
				log.LogErrorf("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] failed to migrate learner in region[%v], addr[%v], err[%v]",
					vol.Name, mpID, volRegion, targetAddr, err)
				continue
			}
			log.LogInfof("action[checkAndCreateMpLearnersByPolicy] vol[%v] mp[%v] successfully added learner in region[%v], addr[%v]",
				vol.Name, mpID, volRegion, targetAddr)

		}
	}

}

// getMpRegionPolicyStatus returns the learner distribution status for each region
func (vol *Vol) getMpRegionPolicyStatus(c *Cluster) (statuses []*proto.MpRegionPolicyStatus) {
	statuses = make([]*proto.MpRegionPolicyStatus, 0)

	// Group meta partitions by region
	regionMps := make(map[string][]*MetaPartition)
	vol.mpsLock.RLock()
	for _, mp := range vol.MetaPartitions {
		region := mp.Region
		regionMps[region] = append(regionMps[region], mp)
	}
	vol.mpsLock.RUnlock()

	// Process each region
	for region, mps := range regionMps {
		status := &proto.MpRegionPolicyStatus{
			Region:          region,
			TotalMp:         len(mps),
			LearnerStatuses: make(map[string]*proto.LearnerRegionStatus),
		}

		// Get policy for this region
		vol.volLock.RLock()
		policy, hasPolicy := vol.mpPolicy[region]
		vol.volLock.RUnlock()

		if !hasPolicy || policy == nil || len(policy.Learner) == 0 {
			// No policy for this region, all are remaining
			statuses = append(statuses, status)
			continue
		}

		// Initialize learner statuses for each target region in policy
		for targetRegion := range policy.Learner {
			status.LearnerStatuses[targetRegion] = &proto.LearnerRegionStatus{
				Completed:       0,
				InProgress:      0,
				Remaining:       0,
				InProgressMpIds: make([]uint64, 0),
				RemainingMpIds:  make([]uint64, 0),
			}
		}

		// Count status for each meta partition
		for _, mp := range mps {
			mp.RLock()

			inProgressMps := make(map[string]*proto.RecoverPair)
			for _, rp := range mp.RecoverLearners {
				if rp.RecoverDst != "" && rp.RecoverSrc == "" {
					dstRegion := c.getRegionFromMetaNodeAddr(rp.RecoverDst)
					if dstRegion != "" {
						inProgressMps[dstRegion] = rp
					}
				}
			}

			completedMps := make(map[string]bool)
			for _, peer := range mp.Peers {
				if peer.Type == raftProto.PeerLearner && peer.ManualPromote {
					dstRegion := c.getRegionFromMetaNodeAddr(peer.Addr)
					if _, exists := inProgressMps[dstRegion]; exists {
						continue
					}
					completedMps[dstRegion] = true
				}
			}

			// Check each target region in policy
			for targetRegion, learnerStatus := range status.LearnerStatuses {
				if _, exists := completedMps[targetRegion]; exists {
					learnerStatus.Completed++
				} else if _, exists := inProgressMps[targetRegion]; exists {
					learnerStatus.InProgress++
					learnerStatus.InProgressMpIds = append(learnerStatus.InProgressMpIds, mp.PartitionID)
				} else {
					learnerStatus.Remaining++
					learnerStatus.RemainingMpIds = append(learnerStatus.RemainingMpIds, mp.PartitionID)
				}
			}
			mp.RUnlock()
		}

		statuses = append(statuses, status)
	}

	return statuses
}

func (vol *Vol) checkMpLeaseTimeout(c *Cluster) bool {

	mps := vol.cloneMetaPartitionMap()
	// Check lease apply time vs report time
	threshold := int64(atomic.LoadUint64(&c.cfg.FollowerReadLeaseTime))
	mpTimeoutSec := c.getMetaPartitionTimeoutSec()
	for _, mp := range mps {
		mp.RLock()
		for _, replica := range mp.Replicas {

			manualPromote := false

			for _, peer := range mp.Peers {
				if peer.Addr == replica.Addr && peer.ManualPromote {
					manualPromote = true
					break
				}
			}

			if !manualPromote {
				continue
			}

			timeDiff := replica.ReportTime - replica.LeaseApplyTime
			if timeDiff < 0 {
				timeDiff = -timeDiff
			}
			if timeDiff > threshold || !replica.isActive(mpTimeoutSec) {
				log.LogWarnf("checkMpLeaseTimeout: mp[%v] lease timeout, leaseApplyTime[%v], reportTime[%v]", mp.PartitionID, replica.LeaseApplyTime, replica.ReportTime)
				mp.RUnlock()
				return true
			}

		}
		mp.RUnlock()
	}

	return false
}

// checkMpRegionPolicyCompliance checks if all meta partitions in this volume comply with MpRegionPolicy
// Returns true if all MPs comply, false otherwise
func (vol *Vol) checkMpRegionPolicyCompliance(c *Cluster) bool {
	vol.volLock.RLock()
	hasPolicy := vol.mpPolicy != nil && len(vol.mpPolicy) > 0
	vol.volLock.RUnlock()

	if !hasPolicy {
		// No policy configured, consider as compliant
		return true
	}

	vol.mpsLock.RLock()
	mps := make([]*MetaPartition, 0, len(vol.MetaPartitions))
	for _, mp := range vol.MetaPartitions {
		mps = append(mps, mp)
	}
	vol.mpsLock.RUnlock()

	// Check each meta partition
	for _, mp := range mps {
		mp.RLock()
		mpRegion := mp.Region
		mp.RUnlock()

		// Get policy for this mp's region
		vol.volLock.RLock()
		policy, hasPolicyForRegion := vol.mpPolicy[mpRegion]
		vol.volLock.RUnlock()

		if !hasPolicyForRegion || policy == nil || len(policy.Learner) == 0 {
			// No policy for this region, skip
			continue
		}

		peers := make([]proto.Peer, len(mp.Peers))
		mp.RLock()
		copy(peers, mp.Peers)
		mp.RUnlock()

		for targetRegion := range policy.Learner {
			// Check if this mp has completed or in-progress learner for this target region
			exist := false
			for _, p := range mp.Peers {
				if p.Type == raftProto.PeerLearner && p.ManualPromote {
					dstRegion := c.getRegionFromMetaNodeAddr(p.Addr)
					if dstRegion == targetRegion {
						exist = true
						break
					}
				}
			}
			if !exist {
				log.LogWarnf("checkMpRegionPolicyCompliance: mp[%v] region[%v] no learner for target region[%v]", mp.PartitionID, mpRegion, targetRegion)
				return false
			}
		}
	}

	return true
}
