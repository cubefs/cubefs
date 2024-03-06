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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
)

type VolVarargs struct {
	zoneName                string
	description             string
	capacity                uint64 // GB
	deleteLockTime          int64  // h
	followerRead            bool
	authenticate            bool
	dpSelectorName          string
	dpSelectorParm          string
	coldArgs                *coldVolArgs
	domainId                uint64
	dpReplicaNum            uint8
	enablePosixAcl          bool
	dpReadOnlyWhenVolFull   bool
	enableQuota             bool
	enableTransaction       proto.TxOpMask
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	txOpLimit               int
}

// Vol represents a set of meta partitionMap and data partitionMap
type Vol struct {
	ID                uint64
	Name              string
	Owner             string
	OSSAccessKey      string
	OSSSecretKey      string
	dpReplicaNum      uint8
	mpReplicaNum      uint8
	Status            uint8
	threshold         float32
	dataPartitionSize uint64 // byte
	Capacity          uint64 // GB
	VolType           int

	EbsBlkSize       int
	CacheCapacity    uint64
	CacheAction      int
	CacheThreshold   int
	CacheTTL         int
	CacheHighWater   int
	CacheLowWater    int
	CacheLRUInterval int
	CacheRule        string

	PreloadCacheOn          bool
	NeedToLowerReplica      bool
	FollowerRead            bool
	authenticate            bool
	crossZone               bool
	domainOn                bool
	defaultPriority         bool // old default zone first
	enablePosixAcl          bool
	enableTransaction       proto.TxOpMask
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	txOpLimit               int
	zoneName                string
	MetaPartitions          map[uint64]*MetaPartition `graphql:"-"`
	dataPartitions          *DataPartitionMap
	mpsCache                []byte
	viewCache               []byte
	createDpMutex           sync.RWMutex
	createMpMutex           sync.RWMutex
	createTime              int64
	DeleteLockTime          int64
	description             string
	dpSelectorName          string
	dpSelectorParm          string
	domainId                uint64
	qosManager              *QosCtrlManager
	DpReadOnlyWhenVolFull   bool
	aclMgr                  AclManager
	uidSpaceManager         *UidSpaceManager
	volLock                 sync.RWMutex
	quotaManager            *MasterQuotaManager
	enableQuota             bool
	VersionMgr              *VolVersionManager
	Forbidden               bool
	mpsLock                 *mpsLockManager
	EnableAuditLog          bool
	preloadCapacity         uint64
}

func newVol(ctx context.Context, vv volValue) (vol *Vol) {
	vol = &Vol{ID: vv.ID, Name: vv.Name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}

	if vol.threshold <= 0 {
		vol.threshold = defaultMetaPartitionMemUsageThreshold
	}

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
	vol.initQosManager(ctx, limitQosVal)

	magnifyQosVal := &qosArgs{
		iopsRVal: uint64(vv.IopsRMagnify),
		iopsWVal: uint64(vv.IopsWMagnify),
		flowRVal: uint64(vv.FlowWMagnify),
		flowWVal: uint64(vv.FlowWMagnify),
	}
	vol.qosManager.volUpdateMagnify(ctx, magnifyQosVal)
	vol.DpReadOnlyWhenVolFull = vv.DpReadOnlyWhenVolFull
	vol.mpsLock = newMpsLockManager(vol)
	vol.EnableAuditLog = true
	vol.preloadCapacity = math.MaxUint64 // mark as special value to trigger calculate
	return
}

func newVolFromVolValue(ctx context.Context, vv *volValue) (vol *Vol) {
	vol = newVol(ctx, *vv)
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
	vol.Forbidden = vv.Forbidden
	vol.EnableAuditLog = vv.EnableAuditLog
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
	if log.GetOutputLevel() == log.Ldebug {
		atomic.StoreInt32(&lc.enable, 0)
	}
	return lc
}

func (mpsLock *mpsLockManager) Lock() {
	mpsLock.mpsLock.Lock()
	if log.GetOutputLevel() == log.Ldebug && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.innerLock.Lock()
		mpsLock.onLock = true
		mpsLock.lockTime = time.Now()
		mpsLock.lastEffectStack = fmt.Sprintf("Lock stack %v", string(debug.Stack()))
	}
}

func (mpsLock *mpsLockManager) UnLock() {
	mpsLock.mpsLock.Unlock()
	if log.GetOutputLevel() == log.Ldebug && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.onLock = false
		mpsLock.lockTime = time.Unix(0, 0)
		mpsLock.lastEffectStack = fmt.Sprintf("UnLock stack %v", string(debug.Stack()))
		mpsLock.innerLock.Unlock()
	}
}

func (mpsLock *mpsLockManager) RLock() {
	mpsLock.mpsLock.RLock()
	if log.GetOutputLevel() == log.Ldebug && atomic.LoadInt32(&mpsLock.enable) == 1 {
		mpsLock.innerLock.RLock()
		mpsLock.hang = false
		mpsLock.onLock = true
		mpsLock.lockTime = time.Now()
		mpsLock.lastEffectStack = fmt.Sprintf("RLock stack %v", string(debug.Stack()))
	}
}

func (mpsLock *mpsLockManager) RUnlock() {
	mpsLock.mpsLock.RUnlock()
	if log.GetOutputLevel() == log.Ldebug && atomic.LoadInt32(&mpsLock.enable) == 1 {
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
			if log.GetOutputLevel() != log.Ldebug {
				continue
			}
			if !mpsLock.onLock {
				continue
			}
			tm := time.Now()
			if tm.After(mpsLock.lockTime.Add(expireTime)) {
				log.Warnf("vol %v mpsLock hang more than %v since time %v stack(%v)",
					mpsLock.vol.Name, expireTime, mpsLock.lockTime, mpsLock.lastEffectStack)
				mpsLock.hang = true
			}
		}
	}
}

func (vol *Vol) CheckStrategy(ctx context.Context, c *Cluster) {
	// make sure resume all the processing ver deleting tasks before checking
	if !atomic.CompareAndSwapInt32(&vol.VersionMgr.checkStrategy, 0, 1) {
		return
	}
	span := proto.SpanFromContext(ctx)
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
					span.Infof("wait for %v seconds once after becoming leader to make sure all the ver deleting tasks are resumed",
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
				vol.VersionMgr.checkCreateStrategy(ctx, c)
				vol.VersionMgr.checkDeleteStrategy(ctx, c)
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

func (vol *Vol) getPreloadCapacity(ctx context.Context) uint64 {
	if vol.preloadCapacity != math.MaxUint64 {
		return vol.preloadCapacity
	}
	vol.preloadCapacity = vol.CalculatePreloadCapacity()
	span := proto.SpanFromContext(ctx)
	span.Debugf("[getPreloadCapacity] vol(%v) calculated preload capacity: %v", vol.Name, vol.preloadCapacity)
	return vol.preloadCapacity
}

func (vol *Vol) initQosManager(ctx context.Context, limitArgs *qosArgs) {
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
		}
		go vol.qosManager.serverFactorLimitMap[arrType[i]].dispatch(ctx)
	}
}

func (vol *Vol) refreshOSSSecure() (key, secret string) {
	vol.OSSAccessKey = util.RandomString(16, util.Numeric|util.LowerLetter|util.UpperLetter)
	vol.OSSSecretKey = util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	return vol.OSSAccessKey, vol.OSSSecretKey
}

func (vol *Vol) addMetaPartition(ctx context.Context, mp *MetaPartition) {
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

func (vol *Vol) getRWMetaPartitionNum(ctx context.Context) (num uint64, isHeartBeatDone bool) {
	span := proto.SpanFromContext(ctx)
	if time.Now().Unix()-vol.createTime <= defaultMetaPartitionTimeOutSec {
		span.Infof("The vol[%v] is being created.", vol.Name)
		return num, false
	}
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		if !mp.heartBeatDone {
			span.Infof("The mp[%v] of vol[%v] is not done", mp.PartitionID, vol.Name)
			return num, false
		}
		if mp.Status == proto.ReadWrite {
			num++
		} else {
			span.Warnf("The mp[%v] of vol[%v] is not RW", mp.PartitionID, vol.Name)
		}
	}
	return num, true
}

func (vol *Vol) getDataPartitionsView(ctx context.Context) (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(ctx, false, 0, vol.VolType)
}

func (vol *Vol) getDataPartitionViewCompress(ctx context.Context) (body []byte, err error) {
	return vol.dataPartitions.updateCompressCache(ctx, false, 0, vol.VolType)
}

func (vol *Vol) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return vol.dataPartitions.get(partitionID)
}

func (vol *Vol) addMetaPartitions(ctx context.Context, c *Cluster, count int) (err error) {
	// add extra meta partitions at a time
	var (
		start uint64
		end   uint64
	)

	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()
	span := proto.SpanFromContext(ctx)
	// update End of the maxMetaPartition range
	maxPartitionId := vol.maxPartitionID()
	rearMetaPartition := vol.MetaPartitions[maxPartitionId]
	oldEnd := rearMetaPartition.End
	end = rearMetaPartition.MaxInodeID + gConfig.MetaPartitionInodeIdStep

	if err = rearMetaPartition.canSplit(ctx, end, gConfig.MetaPartitionInodeIdStep, false); err != nil {
		return err
	}

	rearMetaPartition.End = end
	if err = c.syncUpdateMetaPartition(ctx, rearMetaPartition); err != nil {
		rearMetaPartition.End = oldEnd
		span.Errorf("action[addMetaPartitions] split partition partitionID[%v] err[%v]", rearMetaPartition.PartitionID, err)
		return
	}

	// create new meta partitions
	for i := 0; i < count; i++ {
		start = end + 1
		end = start + gConfig.MetaPartitionInodeIdStep

		if end > (defaultMaxMetaPartitionInodeID - gConfig.MetaPartitionInodeIdStep) {
			end = defaultMaxMetaPartitionInodeID
			span.Warnf("action[addMetaPartitions] vol[%v] add too many meta partition ,partition range overflow ! ", vol.Name)
		}

		if i == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}

		if err = vol.createMetaPartition(ctx, c, start, end); err != nil {
			span.Errorf("action[addMetaPartitions] vol[%v] add meta partition err[%v]", vol.Name, err)
			break
		}

		if end == defaultMaxMetaPartitionInodeID {
			break
		}
	}

	return
}

func (vol *Vol) initMetaPartitions(ctx context.Context, c *Cluster, count int) (err error) {
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
	span := proto.SpanFromContext(ctx)
	vol.createMpMutex.Lock()
	for index := 0; index < count; index++ {
		if index != 0 {
			start = end + 1
		}
		end = gConfig.MetaPartitionInodeIdStep * uint64(index+1)
		if index == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}
		if err = vol.createMetaPartition(ctx, c, start, end); err != nil {
			span.Errorf("action[initMetaPartitions] vol[%v] init meta partition err[%v]", vol.Name, err)
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

func (vol *Vol) initDataPartitions(ctx context.Context, c *Cluster, dpCount int) (err error) {
	if dpCount == 0 {
		dpCount = defaultInitDataPartitionCnt
	}
	// initialize k data partitionMap at a time
	err = c.batchCreateDataPartition(ctx, vol, dpCount, true)
	return
}

func (vol *Vol) checkDataPartitions(ctx context.Context, c *Cluster) (cnt int) {
	if vol.getDataPartitionsCount() == 0 && vol.Status != proto.VolStatusMarkDelete && proto.IsHot(vol.VolType) {
		c.batchCreateDataPartition(ctx, vol, 1, false)
	}

	shouldDpInhibitWriteByVolFull := vol.shouldInhibitWriteBySpaceFull()
	totalPreloadCapacity := uint64(0)
	span := proto.SpanFromContext(ctx)
	partitions := vol.dataPartitions.clonePartitions()
	for _, dp := range partitions {

		if proto.IsPreLoadDp(dp.PartitionType) {
			now := time.Now().Unix()
			if now > dp.PartitionTTL {
				span.Warnf("[checkDataPartitions] dp(%d) is deleted because of ttl expired, now(%d), ttl(%d)", dp.PartitionID, now, dp.PartitionTTL)
				vol.deleteDataPartition(ctx, c, dp)
				continue
			}

			startTime := dp.dataNodeStartTime()
			if now-dp.createTime > 600 && dp.used == 0 && now-startTime > 600 {
				span.Warnf("[checkDataPartitions] dp(%d) is deleted because of clear, now(%d), create(%d), start(%d)",
					dp.PartitionID, now, dp.createTime, startTime)
				vol.deleteDataPartition(ctx, c, dp)
				continue
			}

			totalPreloadCapacity += dp.total / util.GB
		}

		dp.checkReplicaStatus(ctx, c.cfg.DataPartitionTimeOutSec)
		dp.checkStatus(ctx, c.Name, true, c.cfg.DataPartitionTimeOutSec, c, shouldDpInhibitWriteByVolFull, vol.Forbidden)
		dp.checkLeader(ctx, c.Name, c.cfg.DataPartitionTimeOutSec)
		dp.checkMissingReplicas(ctx, c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		dp.checkReplicaNum(ctx, c, vol)

		if time.Now().Unix()-vol.createTime < defaultIntervalToCheckHeartbeat*3 && !vol.Forbidden {
			dp.setReadWrite()
		}

		if dp.Status == proto.ReadWrite {
			cnt++
		}

		dp.checkDiskError(ctx, c.Name, c.leaderInfo.addr)

		dp.checkReplicationTask(ctx, c.Name, vol.dataPartitionSize)
	}

	if overSoldFactor > 0 {
		totalPreloadCapacity = uint64(float32(totalPreloadCapacity) / overSoldFactor)
	}
	vol.preloadCapacity = totalPreloadCapacity
	if vol.preloadCapacity != 0 {
		span.Debugf("[checkDataPartitions] vol(%v) totalPreloadCapacity(%v GB), overSoldFactor(%v)",
			vol.Name, totalPreloadCapacity, overSoldFactor)
	}

	return
}

func (vol *Vol) loadDataPartition(ctx context.Context, c *Cluster) {
	span := proto.SpanFromContext(ctx)
	partitions, startIndex := vol.dataPartitions.getDataPartitionsToBeChecked(c.cfg.PeriodToLoadALLDataPartitions)
	if len(partitions) == 0 {
		return
	}
	c.waitForResponseToLoadDataPartition(ctx, partitions)
	msg := fmt.Sprintf("action[loadDataPartition] vol[%v],checkStartIndex:%v checkCount:%v",
		vol.Name, startIndex, len(partitions))
	span.Info(msg)
}

func (vol *Vol) releaseDataPartitions(ctx context.Context, releaseCount int, afterLoadSeconds int64) {
	span := proto.SpanFromContext(ctx)
	partitions, startIndex := vol.dataPartitions.getDataPartitionsToBeReleased(releaseCount, afterLoadSeconds)
	if len(partitions) == 0 {
		return
	}
	vol.dataPartitions.freeMemOccupiedByDataPartitions(ctx, partitions)
	msg := fmt.Sprintf("action[freeMemOccupiedByDataPartitions] vol[%v] release data partition start:%v releaseCount:%v",
		vol.Name, startIndex, len(partitions))
	span.Info(msg)
}

func (vol *Vol) tryUpdateDpReplicaNum(ctx context.Context, c *Cluster, partition *DataPartition) (err error) {
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

	if err = c.syncUpdateDataPartition(ctx, partition); err != nil {
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

func (vol *Vol) checkReplicaNum(ctx context.Context, c *Cluster) {
	if !vol.NeedToLowerReplica {
		return
	}
	var err error
	if proto.IsCold(vol.VolType) {
		return
	}
	span := proto.SpanFromContext(ctx)
	dps := vol.cloneDataPartitionMap()
	cnt := 0
	for _, dp := range dps {
		host := dp.getToBeDecommissionHost(ctx, int(vol.dpReplicaNum))
		if host == "" {
			continue
		}
		if err = dp.removeOneReplicaByHost(ctx, c, host, vol.dpReplicaNum == dp.ReplicaNum); err != nil {
			if dp.isSpecialReplicaCnt() && len(dp.Hosts) > 1 {
				span.Warnf("action[checkReplicaNum] removeOneReplicaByHost host [%v],vol[%v],err[%v]", host, vol.Name, err)
				continue
			}
			span.Errorf("action[checkReplicaNum] removeOneReplicaByHost host [%v],vol[%v],err[%v]", host, vol.Name, err)
			continue
		}
		cnt++
		if cnt > 100 {
			return
		}
	}
	vol.NeedToLowerReplica = false
}

func (vol *Vol) checkMetaPartitions(ctx context.Context, c *Cluster) {
	var tasks []*proto.AdminTask
	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	maxPartitionID := vol.maxPartitionID()
	mps := vol.cloneMetaPartitionMap()
	var (
		doSplit bool
		err     error
	)
	span := proto.SpanFromContext(ctx)
	for _, mp := range mps {
		doSplit = mp.checkStatus(ctx, c.Name, true, int(vol.mpReplicaNum), maxPartitionID, metaPartitionInodeIdStep, vol.Forbidden)
		if doSplit && !c.cfg.DisableAutoCreate {
			nextStart := mp.MaxInodeID + metaPartitionInodeIdStep
			span.Infof(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits start[%v] maxinodeid:[%v] default step:[%v],nextStart[%v]",
				c.Name, vol.Name, mp.PartitionID, mp.Start, mp.MaxInodeID, metaPartitionInodeIdStep, nextStart))
			if err = vol.splitMetaPartition(ctx, c, mp, nextStart, metaPartitionInodeIdStep, false); err != nil {
				Warn(ctx, c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]", c.Name, vol.Name, mp.PartitionID, err))
			}
		}

		mp.checkLeader(c.Name)
		mp.checkReplicaNum(ctx, c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(ctx, c, maxPartitionID)
		mp.reportMissingReplicas(ctx, c.Name, c.leaderInfo.addr, defaultMetaPartitionTimeOutSec, defaultIntervalToAlarmMissingMetaPartition)
		tasks = append(tasks, mp.replicaCreationTasks(ctx, c.Name, vol.Name)...)
	}
	c.addMetaNodeTasks(ctx, tasks)
	vol.checkSplitMetaPartition(ctx, c, metaPartitionInodeIdStep)
}

func (vol *Vol) checkSplitMetaPartition(ctx context.Context, c *Cluster, metaPartitionInodeStep uint64) {
	maxPartitionID := vol.maxPartitionID()
	maxMP, err := vol.metaPartition(maxPartitionID)
	if err != nil {
		return
	}
	span := proto.SpanFromContext(ctx)
	// Any of the following conditions will trigger max mp split
	// 1. The memory of the metanode which max mp belongs to reaches the threshold
	// 2. The number of inodes managed by max mp reaches the threshold(0.75)
	// 3. The number of RW mp is less than 3
	maxMPInodeUsedRatio := float64(maxMP.MaxInodeID-maxMP.Start) / float64(metaPartitionInodeStep)
	RWMPNum, isHeartBeatDone := vol.getRWMetaPartitionNum(ctx)
	if !isHeartBeatDone {
		span.Infof("Not all volume[%s] mp heartbeat is done, skip mp split", vol.Name)
		return
	}
	if maxMP.memUsedReachThreshold(ctx, c.Name, vol.Name) || RWMPNum < lowerLimitRWMetaPartition ||
		maxMPInodeUsedRatio > metaPartitionInodeUsageThreshold {
		end := maxMP.MaxInodeID + metaPartitionInodeStep/4
		if RWMPNum < lowerLimitRWMetaPartition {
			end = maxMP.MaxInodeID + metaPartitionInodeStep
		}
		if err := vol.splitMetaPartition(ctx, c, maxMP, end, metaPartitionInodeStep, true); err != nil {
			msg := fmt.Sprintf("action[checkSplitMetaPartition],split meta maxMP[%v] failed,err[%v]\n",
				maxMP.PartitionID, err)
			Warn(ctx, c.Name, msg)
		}
		span.Infof("volume[%v] split MaxMP[%v], MaxInodeID[%d] Start[%d] RWMPNum[%d] maxMPInodeUsedRatio[%.2f]",
			vol.Name, maxPartitionID, maxMP.MaxInodeID, maxMP.Start, RWMPNum, maxMPInodeUsedRatio)
	}
	return
}

func (mp *MetaPartition) memUsedReachThreshold(ctx context.Context, clusterName, volName string) bool {
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
	if readonlyReplica.metaNode.isWritable() {
		msg := fmt.Sprintf("action[checkSplitMetaPartition] vol[%v],max meta parition[%v] status is readonly\n",
			volName, mp.PartitionID)
		Warn(ctx, clusterName, msg)
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

func (vol *Vol) setMpRdOnly() {
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

func (vol *Vol) setDpRdOnly() {
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

func (vol *Vol) autoDeleteDp(ctx context.Context, c *Cluster) {
	if vol.dataPartitions == nil {
		return
	}

	maxSize := overSoldCap(vol.CacheCapacity * util.GB)
	maxCnt := maxSize / vol.dataPartitionSize

	if maxSize%vol.dataPartitionSize != 0 {
		maxCnt++
	}
	span := proto.SpanFromContext(ctx)
	partitions := vol.dataPartitions.clonePartitions()
	for _, dp := range partitions {
		if !proto.IsCacheDp(dp.PartitionType) {
			continue
		}

		if maxCnt > 0 {
			maxCnt--
			continue
		}

		span.Infof("[autoDeleteDp] start delete dp, id[%d]", dp.PartitionID)
		vol.deleteDataPartition(ctx, c, dp)
	}
}

func (vol *Vol) checkAutoDataPartitionCreation(ctx context.Context, c *Cluster) {
	span := proto.SpanFromContext(ctx)
	defer func() {
		if r := recover(); r != nil {
			span.Warnf("checkAutoDataPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(ctx, fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoDataPartitionCreation occurred panic")
		}
	}()

	if ok, _ := vol.needCreateDataPartition(ctx); !ok {
		return
	}

	vol.setStatus(proto.VolStatusNormal)
	span.Infof("action[autoCreateDataPartitions] vol[%v] before autoCreateDataPartitions", vol.Name)
	if !c.DisableAutoAllocate && !vol.Forbidden {
		vol.autoCreateDataPartitions(ctx, c)
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

	return false
}

func (vol *Vol) needCreateDataPartition(ctx context.Context) (ok bool, err error) {
	ok = false
	if vol.status() == proto.VolStatusMarkDelete {
		err = proto.ErrVolNotExists
		return
	}

	if vol.capacity() == 0 {
		err = proto.ErrVolNoAvailableSpace
		return
	}

	if proto.IsHot(vol.VolType) {
		if vol.shouldInhibitWriteBySpaceFull() {
			vol.setAllDataPartitionsToReadOnly(ctx)
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

func (vol *Vol) autoCreateDataPartitions(ctx context.Context, c *Cluster) {
	if time.Since(vol.dataPartitions.lastAutoCreateTime) < time.Minute {
		return
	}
	span := proto.SpanFromContext(ctx)
	if c.cfg.DisableAutoCreate {
		// if disable auto create, once alloc size is over capacity, not allow to create new dp
		allocSize := uint64(len(vol.dataPartitions.partitions)) * vol.dataPartitionSize
		totalSize := vol.capacity() * util.GB
		if allocSize > totalSize {
			return
		}

		if vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions {
			c.batchCreateDataPartition(ctx, vol, minNumOfRWDataPartitions, false)
			span.Warnf("autoCreateDataPartitions: readWrite less than 10, alloc new 10 partitions, vol %s", vol.Name)
		}

		return
	}

	if proto.IsCold(vol.VolType) {

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
			span.Infof("action[autoCreateDataPartitions] (%s) no need to create again, alloc [%d], max [%d]", vol.Name, allocSize, maxSize)
			return
		}

		count := (maxSize-allocSize-1)/vol.dataPartitionSize + 1
		span.Infof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(ctx, vol, int(count), false)
		return
	}

	if (vol.Capacity > 200000 && vol.dataPartitions.readableAndWritableCnt < 200) || vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions {
		vol.dataPartitions.lastAutoCreateTime = time.Now()
		count := vol.calculateExpansionNum()
		span.Infof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(ctx, vol, count, false)
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

func (vol *Vol) setAllDataPartitionsToReadOnly(ctx context.Context) {
	vol.dataPartitions.setAllDataPartitionsToReadOnly(ctx)
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

func (vol *Vol) sendViewCacheToFollower(ctx context.Context, c *Cluster) {
	var err error
	span := proto.SpanFromContext(ctx)
	span.Infof("action[asyncSendPartitionsToFollower]")

	metadata := new(RaftCmd)
	metadata.Op = opSyncDataPartitionsView
	metadata.K = vol.Name
	metadata.V = vol.dataPartitions.getDataPartitionResponseCache()

	if err = c.submit(ctx, metadata); err != nil {
		span.Errorf("action[asyncSendPartitionsToFollower] error [%v]", err)
	}
	span.Infof("action[asyncSendPartitionsToFollower] finished")
}

func (vol *Vol) ebsUsedSpace() uint64 {
	size := uint64(0)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	for _, pt := range vol.MetaPartitions {
		size += pt.dataSize()
	}

	return size
}

func (vol *Vol) updateViewCache(ctx context.Context, c *Cluster) {
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.createTime, vol.CacheTTL, vol.VolType, vol.DeleteLockTime)
	view.SetOwner(vol.Owner)
	view.SetOSSSecure(vol.OSSAccessKey, vol.OSSSecretKey)
	mpViews := vol.getMetaPartitionsView()
	view.MetaPartitions = mpViews
	mpViewsReply := newSuccessHTTPReply(mpViews)
	mpsBody, err := json.Marshal(mpViewsReply)
	span := proto.SpanFromContext(ctx)
	if err != nil {
		span.Errorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setMpsCache(mpsBody)
	// dpResps := vol.dataPartitions.getDataPartitionsView(0)
	// view.DataPartitions = dpResps
	view.DomainOn = vol.domainOn
	viewReply := newSuccessHTTPReply(view)
	body, err := json.Marshal(viewReply)
	if err != nil {
		span.Errorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
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

func (vol *Vol) deleteDataPartition(ctx context.Context, c *Cluster, dp *DataPartition) {
	var addrs []string
	for _, replica := range dp.Replicas {
		addrs = append(addrs, replica.Addr)
	}
	span := proto.SpanFromContext(ctx)
	for _, addr := range addrs {
		if err := vol.deleteDataPartitionFromDataNode(ctx, c, dp.createTaskToDeleteDataPartition(addr)); err != nil {
			span.Errorf("[deleteDataPartitionFromDataNode] delete data replica from datanode fail, id %d, err %s", dp.PartitionID, err.Error())
		}
	}

	vol.dataPartitions.del(dp)

	err := c.syncDeleteDataPartition(ctx, dp)
	if err != nil {
		span.Errorf("[deleteDataPartition] delete data partition from store fail, [%d], err: %s", dp.PartitionID, err.Error())
		return
	}

	span.Infof("[deleteDataPartition] delete data partition success, [%d]", dp.PartitionID)
}

// Periodically check the volume's status.
// If an volume is marked as deleted, then generate corresponding delete task (meta partition or data partition)
// If all the meta partition and data partition of this volume have been deleted, then delete this volume.
func (vol *Vol) checkStatus(ctx context.Context, c *Cluster) {
	if !atomic.CompareAndSwapInt32(&vol.VersionMgr.checkStatus, 0, 1) {
		return
	}
	span := proto.SpanFromContext(ctx)
	defer func() {
		atomic.StoreInt32(&vol.VersionMgr.checkStatus, 0)
		if r := recover(); r != nil {
			span.Warnf("checkStatus occurred panic,err[%v]", r)
			WarnBySpecialKey(ctx, fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkStatus occurred panic")
		}
	}()
	vol.updateViewCache(ctx, c)
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	if vol.Status != proto.VolStatusMarkDelete {
		return
	}
	span.Infof("action[volCheckStatus] vol[%v],status[%v]", vol.Name, vol.Status)
	metaTasks := vol.getTasksToDeleteMetaPartitions(ctx)
	dataTasks := vol.getTasksToDeleteDataPartitions(ctx)

	if len(metaTasks) == 0 && len(dataTasks) == 0 {
		vol.deleteVolFromStore(ctx, c)
	}
	go func() {
		for _, metaTask := range metaTasks {
			vol.deleteMetaPartitionFromMetaNode(ctx, c, metaTask)
		}

		for _, dataTask := range dataTasks {
			vol.deleteDataPartitionFromDataNode(ctx, c, dataTask)
		}
	}()

	return
}

func (vol *Vol) deleteMetaPartitionFromMetaNode(ctx context.Context, c *Cluster, task *proto.AdminTask) {
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
	span := proto.SpanFromContext(ctx)
	if err != nil {
		span.Warnf("deleteMetaPartitionFromMetaNode (%s) maybe alread been deleted", task.ToString())
		return
	}

	_, err = metaNode.Sender.syncSendAdminTask(ctx, task)
	if err != nil {
		span.Errorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
		return
	}
	mp.Lock()
	mp.removeReplicaByAddr(metaNode.Addr)
	mp.removeMissingReplica(metaNode.Addr)
	mp.Unlock()
	return
}

func (vol *Vol) deleteDataPartitionFromDataNode(ctx context.Context, c *Cluster, task *proto.AdminTask) (err error) {
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
	span := proto.SpanFromContext(ctx)
	if !ok {
		span.Warnf("deleteDataPartitionFromDataNode task(%s) maybe already executed", task.ToString())
		return
	}

	_, err = dataNode.TaskManager.syncSendAdminTask(ctx, task)
	if err != nil {
		span.Errorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}

	dp.Lock()
	dp.removeReplicaByAddr(ctx, dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)
	if err = dp.update(ctx, "deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		dp.Unlock()
		return
	}
	dp.Unlock()

	return
}

func (vol *Vol) deleteVolFromStore(ctx context.Context, c *Cluster) (err error) {
	span := proto.SpanFromContext(ctx)
	span.Warnf("deleteVolFromStore vol %v", vol.Name)
	if err = c.syncDeleteVol(ctx, vol); err != nil {
		return
	}

	// delete the metadata of the meta and data partitionMap first
	vol.deleteDataPartitionsFromStore(ctx, c)
	vol.deleteMetaPartitionsFromStore(ctx, c)
	// then delete the volume
	c.deleteVol(vol.Name)
	c.volStatInfo.Delete(vol.Name)

	c.DelBucketLifecycle(ctx, vol.Name)
	return
}

func (vol *Vol) deleteMetaPartitionsFromStore(ctx context.Context, c *Cluster) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		c.syncDeleteMetaPartition(ctx, mp)
	}
	return
}

func (vol *Vol) deleteDataPartitionsFromStore(ctx context.Context, c *Cluster) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitions {
		c.syncDeleteDataPartition(ctx, dp)
	}
}

func (vol *Vol) getTasksToDeleteMetaPartitions(ctx context.Context) (tasks []*proto.AdminTask) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	tasks = make([]*proto.AdminTask, 0)
	span := proto.SpanFromContext(ctx)
	for _, mp := range vol.MetaPartitions {
		span.Debugf("get delete task from vol(%s) mp(%d)", vol.Name, mp.PartitionID)
		for _, replica := range mp.Replicas {
			span.Debugf("get delete task from vol(%s) mp(%d),replica(%v)", vol.Name, mp.PartitionID, replica.Addr)
			tasks = append(tasks, replica.createTaskToDeleteReplica(mp.PartitionID))
		}
	}
	return
}

func (vol *Vol) getTasksToDeleteDataPartitions(ctx context.Context) (tasks []*proto.AdminTask) {
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
	vol.volLock.RLock()
	count = len(vol.dataPartitions.partitionMap)
	vol.volLock.RUnlock()
	return
}

func (vol *Vol) String() string {
	return fmt.Sprintf("name[%v],dpNum[%v],mpNum[%v],cap[%v],status[%v]",
		vol.Name, vol.dpReplicaNum, vol.mpReplicaNum, vol.Capacity, vol.Status)
}

func (vol *Vol) doSplitMetaPartition(ctx context.Context, c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool) (nextMp *MetaPartition, err error) {
	mp.Lock()
	defer mp.Unlock()
	span := proto.SpanFromContext(ctx)
	if err = mp.canSplit(ctx, end, metaPartitionInodeIdStep, ignoreNoLeader); err != nil {
		return
	}

	span.Warnf("action[splitMetaPartition],partition[%v],start[%v],end[%v],new end[%v]", mp.PartitionID, mp.Start, mp.End, end)
	cmdMap := make(map[string]*RaftCmd, 0)
	oldEnd := mp.End
	mp.End = end

	updateMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncUpdateMetaPartition, mp)
	if err != nil {
		return
	}

	cmdMap[updateMpRaftCmd.K] = updateMpRaftCmd
	if nextMp, err = vol.doCreateMetaPartition(ctx, c, mp.End+1, defaultMaxMetaPartitionInodeID); err != nil {
		Warn(ctx, c.Name, fmt.Sprintf("action[updateEnd] clusterID[%v] partitionID[%v] create meta partition err[%v]",
			c.Name, mp.PartitionID, err))
		span.Errorf("action[updateEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
		return
	}

	addMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncAddMetaPartition, nextMp)
	if err != nil {
		return
	}

	cmdMap[addMpRaftCmd.K] = addMpRaftCmd
	if err = c.syncBatchCommitCmd(ctx, cmdMap); err != nil {
		mp.End = oldEnd
		return nil, errors.NewError(err)
	}

	mp.updateInodeIDRangeForAllReplicas()
	mp.addUpdateMetaReplicaTask(ctx, c)
	return
}

func (vol *Vol) splitMetaPartition(ctx context.Context, c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64, ignoreNoLeader bool) (err error) {
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

	nextMp, err := vol.doSplitMetaPartition(ctx, c, mp, end, metaPartitionInodeIdStep, ignoreNoLeader)
	if err != nil {
		return
	}

	vol.addMetaPartition(ctx, nextMp)
	span := proto.SpanFromContext(ctx)
	span.Warnf("action[splitMetaPartition],next partition[%v],start[%v],end[%v]", nextMp.PartitionID, nextMp.Start, nextMp.End)
	return
}

func (vol *Vol) createMetaPartition(ctx context.Context, c *Cluster, start, end uint64) (err error) {
	var mp *MetaPartition
	if mp, err = vol.doCreateMetaPartition(ctx, c, start, end); err != nil {
		return
	}
	if err = c.syncAddMetaPartition(ctx, mp); err != nil {
		return errors.NewError(err)
	}
	vol.addMetaPartition(ctx, mp)
	return
}

func (vol *Vol) doCreateMetaPartition(ctx context.Context, c *Cluster, start, end uint64) (mp *MetaPartition, err error) {
	var (
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
		wg          sync.WaitGroup
	)

	errChannel := make(chan error, vol.mpReplicaNum)
	span := proto.SpanFromContext(ctx)
	if c.isFaultDomain(ctx, vol) {
		if hosts, peers, err = c.getHostFromDomainZone(ctx, vol.domainId, TypeMetaPartition, vol.mpReplicaNum); err != nil {
			span.Errorf("action[doCreateMetaPartition] getHostFromDomainZone err[%v]", err)
			return nil, errors.NewError(err)
		}
	} else {
		var excludeZone []string
		zoneNum := c.decideZoneNum(vol.crossZone)

		if hosts, peers, err = c.getHostFromNormalZone(ctx, TypeMetaPartition, excludeZone, nil, nil, int(vol.mpReplicaNum), zoneNum, vol.zoneName); err != nil {
			span.Errorf("action[doCreateMetaPartition] getHostFromNormalZone err[%v]", err)
			return nil, errors.NewError(err)
		}

	}

	span.Infof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(ctx); err != nil {
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
			if err = c.syncCreateMetaPartitionToMetaNode(ctx, host, mp); err != nil {
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
				c.addMetaNodeTasks(ctx, tasks)
			}(host)
		}
		wg.Wait()
		return nil, errors.NewError(err)
	default:
		mp.Status = proto.ReadWrite
	}
	span.Infof("action[doCreateMetaPartition] success,volName[%v],partition[%v],start[%v],end[%v]", vol.Name, partitionID, start, end)
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

	if proto.IsCold(vol.VolType) {
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
}

func getVolVarargs(vol *Vol) *VolVarargs {
	args := &coldVolArgs{
		objBlockSize:     vol.EbsBlkSize,
		cacheCap:         vol.CacheCapacity,
		cacheAction:      vol.CacheAction,
		cacheThreshold:   vol.CacheThreshold,
		cacheTtl:         vol.CacheTTL,
		cacheHighWater:   vol.CacheHighWater,
		cacheLowWater:    vol.CacheLowWater,
		cacheLRUInterval: vol.CacheLRUInterval,
		cacheRule:        vol.CacheRule,
	}

	return &VolVarargs{
		zoneName:                vol.zoneName,
		description:             vol.description,
		capacity:                vol.Capacity,
		deleteLockTime:          vol.DeleteLockTime,
		followerRead:            vol.FollowerRead,
		authenticate:            vol.authenticate,
		dpSelectorName:          vol.dpSelectorName,
		dpSelectorParm:          vol.dpSelectorParm,
		enablePosixAcl:          vol.enablePosixAcl,
		enableQuota:             vol.enableQuota,
		dpReplicaNum:            vol.dpReplicaNum,
		enableTransaction:       vol.enableTransaction,
		txTimeout:               vol.txTimeout,
		txConflictRetryNum:      vol.txConflictRetryNum,
		txConflictRetryInterval: vol.txConflictRetryInterval,
		txOpLimit:               vol.txOpLimit,
		coldArgs:                args,
		dpReadOnlyWhenVolFull:   vol.DpReadOnlyWhenVolFull,
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

func (vol *Vol) loadQuotaManager(ctx context.Context, c *Cluster) (err error) {
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
	span := proto.SpanFromContext(ctx)
	for _, value := range result {
		quotaInfo := &proto.QuotaInfo{}

		if err = json.Unmarshal(value, quotaInfo); err != nil {
			span.Errorf("loadQuotaManager Unmarshal fail err [%v]", err)
			return err
		}
		span.Debugf("loadQuotaManager info [%v]", quotaInfo)
		if vol.Name != quotaInfo.VolName {
			panic(fmt.Sprintf("vol name do not match vol name [%v], quotaInfo vol name [%v]", vol.Name, quotaInfo.VolName))
		}
		vol.quotaManager.IdQuotaInfoMap[quotaInfo.QuotaId] = quotaInfo
	}

	return err
}
