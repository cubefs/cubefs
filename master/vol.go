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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type VolVarargs struct {
	zoneName       string
	description    string
	capacity       uint64 //GB
	followerRead   bool
	authenticate   bool
	dpSelectorName string
	dpSelectorParm string
	coldArgs       *coldVolArgs
	domainId       uint64
	dpReplicaNum   uint8
	enablePosixAcl bool
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

	PreloadCacheOn     bool
	NeedToLowerReplica bool
	FollowerRead       bool
	authenticate       bool
	crossZone          bool
	domainOn           bool
	defaultPriority    bool // old default zone first
	enablePosixAcl     bool
	zoneName           string
	MetaPartitions     map[uint64]*MetaPartition `graphql:"-"`
	mpsLock            sync.RWMutex
	dataPartitions     *DataPartitionMap
	mpsCache           []byte
	viewCache          []byte
	createDpMutex      sync.RWMutex
	createMpMutex      sync.RWMutex
	createTime         int64
	description        string
	dpSelectorName     string
	dpSelectorParm     string
	domainId           uint64
	qosManager         *QosCtrlManager

	volLock sync.RWMutex
}

func newVol(vv volValue) (vol *Vol) {

	vol = &Vol{ID: vv.ID, Name: vv.Name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}

	if vol.threshold <= 0 {
		vol.threshold = defaultMetaPartitionMemUsageThreshold
	}

	vol.dataPartitions = newDataPartitionMap(vv.Name)
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
	vol.description = vv.Description
	vol.defaultPriority = vv.DefaultPriority
	vol.domainId = vv.DomainId
	vol.enablePosixAcl = vv.EnablePosixAcl

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
	return
}

func newVolFromVolValue(vv *volValue) (vol *Vol) {
	vol = newVol(*vv)
	// overwrite oss secure
	vol.OSSAccessKey, vol.OSSSecretKey = vv.OSSAccessKey, vv.OSSSecretKey
	vol.Status = vv.Status
	vol.dpSelectorName = vv.DpSelectorName
	vol.dpSelectorParm = vv.DpSelectorParm
	return vol
}

func (vol *Vol) getPreloadCapacity() uint64 {
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
			Name:      proto.QosTypeString(arrType[i]),
			Type:      arrType[i],
			Total:     arrLimit[i],
			Buffer:    arrLimit[i],
			requestCh: make(chan interface{}, 10240),
			qosManager: vol.qosManager,
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
	defer vol.mpsLock.Unlock()
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

func (vol *Vol) getDataPartitionsView() (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(false, 0, vol.VolType)
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
	if len(vol.MetaPartitions) != count {
		err = fmt.Errorf("action[initMetaPartitions] vol[%v] init meta partition failed,mpCount[%v],expectCount[%v],err[%v]",
			vol.Name, len(vol.MetaPartitions), count, err)
	}
	return
}

func (vol *Vol) initDataPartitions(c *Cluster) (err error) {
	// initialize k data partitionMap at a time
	err = c.batchCreateDataPartition(vol, defaultInitDataPartitionCnt)
	return
}

func (vol *Vol) checkDataPartitions(c *Cluster) (cnt int) {
	if vol.getDataPartitionsCount() == 0 && vol.Status != markDelete && proto.IsHot(vol.VolType) {
		c.batchCreateDataPartition(vol, 1)
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, dp := range partitions {

		if proto.IsPreLoadDp(dp.PartitionType) {
			now := time.Now().Unix()
			if now > dp.PartitionTTL {
				log.LogWarnf("[checkDataPartitions] dp(%d) is deleted because of ttl expired, now(%d), ttl(%d)", dp.PartitionID, now, dp.PartitionTTL)
				vol.deleteDataPartition(c, dp)
			}

			startTime := dp.dataNodeStartTime()
			if now-dp.createTime > 600 && dp.used == 0 && now-startTime > 600 {
				log.LogWarnf("[checkDataPartitions] dp(%d) is deleted because of clear, now(%d), create(%d), start(%d)",
					dp.PartitionID, now, dp.createTime, startTime)
				vol.deleteDataPartition(c, dp)
			}
		}

		dp.checkReplicaStatus(c.cfg.DataPartitionTimeOutSec)
		dp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec)
		dp.checkLeader(c.cfg.DataPartitionTimeOutSec)
		dp.checkMissingReplicas(c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		dp.checkReplicaNum(c, vol)

		if dp.Status == proto.ReadWrite {
			cnt++
		}

		dp.checkDiskError(c.Name, c.leaderInfo.addr)

		tasks := dp.checkReplicationTask(c.Name, vol.dataPartitionSize)
		if len(tasks) != 0 {
			c.addDataNodeTasks(tasks)
		}
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
	if proto.IsCold(vol.VolType) {
		return
	}

	dps := vol.cloneDataPartitionMap()
	for _, dp := range dps {
		host := dp.getToBeDecommissionHost(int(dp.ReplicaNum))
		if host == "" {
			continue
		}
		if err = dp.removeOneReplicaByHost(c, host); err != nil {
			if dp.isSpecialReplicaCnt() && len(dp.Hosts) > 1 {
				log.LogWarnf("action[checkReplicaNum] removeOneReplicaByHost host [%v],vol[%v],err[%v]", host, vol.Name, err)
				continue
			}
			log.LogErrorf("action[checkReplicaNum] removeOneReplicaByHost host [%v],vol[%v],err[%v]", host, vol.Name, err)
			continue
		}
	}
	vol.NeedToLowerReplica = false
}

func (vol *Vol) checkMetaPartitions(c *Cluster) {
	var tasks []*proto.AdminTask
	vol.checkSplitMetaPartition(c)
	maxPartitionID := vol.maxPartitionID()
	mps := vol.cloneMetaPartitionMap()
	var (
		doSplit bool
		err     error
	)
	for _, mp := range mps {
		doSplit = mp.checkStatus(c.Name, true, int(vol.mpReplicaNum), maxPartitionID)
		if doSplit {
			nextStart := mp.MaxInodeID + defaultMetaPartitionInodeIDStep
			log.LogInfof(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits start[%v] maxinodeid:[%v] default step:[%v],nextStart[%v]",
				c.Name, vol.Name, mp.PartitionID, mp.Start, mp.MaxInodeID, defaultMetaPartitionInodeIDStep, nextStart))
			if err = vol.splitMetaPartition(c, mp, nextStart); err != nil {
				Warn(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]", c.Name, vol.Name, mp.PartitionID, err))
			}
		}

		mp.checkLeader()
		mp.checkReplicaNum(c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(c, maxPartitionID)
		mp.reportMissingReplicas(c.Name, c.leaderInfo.addr, defaultMetaPartitionTimeOutSec, defaultIntervalToAlarmMissingMetaPartition)
		tasks = append(tasks, mp.replicaCreationTasks(c.Name, vol.Name)...)
	}
	c.addMetaNodeTasks(tasks)
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
	if readonlyReplica.metaNode.isWritable() {
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

	if !vol.needCreateDataPartition() {
		return
	}

	vol.setStatus(normal)
	log.LogInfof("action[autoCreateDataPartitions] vol[%v] before autoCreateDataPartitions", vol.Name)
	if !c.DisableAutoAllocate {
		vol.autoCreateDataPartitions(c)
	}
}

func (vol *Vol) needCreateDataPartition() bool {

	if vol.status() == markDelete {
		return false
	}

	if vol.capacity() == 0 {
		return false
	}

	if proto.IsHot(vol.VolType) {
		usedSpace := vol.totalUsedSpace() / util.GB
		if usedSpace >= vol.capacity() {
			vol.setAllDataPartitionsToReadOnly()
			return false
		}
		return true
	}

	// cold
	if vol.CacheAction == proto.NoCache && vol.CacheRule == "" {
		return false
	}

	return true
}

func (vol *Vol) autoCreateDataPartitions(c *Cluster) {

	if time.Since(vol.dataPartitions.lastAutoCreateTime) < time.Minute {
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
			log.LogInfof("action[autoCreateDataPartitions] (%s) no need to create again, alloc [%d], max [%d]", vol.Name, allocSize, maxSize)
			return
		}

		count := (maxSize-allocSize-1)/vol.dataPartitionSize + 1
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(vol, int(count))
		return
	}

	if (vol.Capacity > 200000 && vol.dataPartitions.readableAndWritableCnt < 200) || vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions {
		vol.dataPartitions.lastAutoCreateTime = time.Now()
		count := vol.calculateExpansionNum()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(vol, count)
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
	if proto.IsCold(vol.VolType) {
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
	for _, pt := range vol.MetaPartitions {
		size += pt.dataSize()
	}

	return size
}

type ServerFactorLimit struct {
	Name           string
	Type           uint32
	Total          uint64
	Buffer         uint64 // flowbuffer add with preallocate buffer equal with flowtotal
	CliUsed        uint64
	CliNeed        uint64
	Allocated      uint64
	NeedAfterAlloc uint64
	Magnify        uint32 // for client allocation need magnify
	LimitRate      float32
	requestCh      chan interface{}
	done           chan interface{}
	qosManager     *QosCtrlManager
}

type ClientInfoMgr struct {
	Cli    *proto.ClientReportLimitInfo
	Assign *proto.LimitRsp2Client
	Time   time.Time
	ID     uint64
	host   string
}

type qosRequestArgs struct {
	clientID       uint64
	factorType     uint32
	clientReq      *proto.ClientLimitInfo
	lastClientInfo *proto.ClientLimitInfo
	assignInfo     *proto.ClientLimitInfo
	rsp2Client     *proto.ClientLimitInfo
	wg             *sync.WaitGroup
}

type QosCtrlManager struct {
	cliInfoMgrMap        map[uint64]*ClientInfoMgr     // cientid->client_reportinfo&&assign_limitinfo
	serverFactorLimitMap map[uint32]*ServerFactorLimit // vol qos data for iops w/r and flow w/r
	defaultClientCnt     uint32
	qosEnable            bool
	ClientReqPeriod      uint32
	ClientHitTriggerCnt  uint32
	vol                  *Vol
	sync.RWMutex
}

func (qosManager *QosCtrlManager) volUpdateMagnify(magnifyArgs *qosArgs) {
	defer qosManager.Unlock()
	qosManager.Lock()

	log.LogWarnf("action[volUpdateMagnify] vol %v try set magnify iopsRVal[%v],iopsWVal[%v],flowRVal[%v],flowWVal[%v]",
		qosManager.vol.Name, magnifyArgs.iopsRVal, magnifyArgs.iopsWVal, magnifyArgs.flowRVal, magnifyArgs.flowWVal)

	arrMagnify := [4]uint64{magnifyArgs.iopsRVal, magnifyArgs.iopsWVal, magnifyArgs.flowRVal, magnifyArgs.flowWVal}
	for i := proto.IopsReadType; i <= proto.FlowWriteType; i++ {
		magnify := qosManager.serverFactorLimitMap[i].Magnify
		if uint64(magnify) != arrMagnify[i-1] && arrMagnify[i-1] > 0 {
			qosManager.serverFactorLimitMap[i].Magnify = uint32(arrMagnify[i-1])
			log.LogWarnf("action[volUpdateMagnify] vol %v  after update type [%v] magnify [%v] to [%v]",
				qosManager.vol.Name, proto.QosTypeString(i), magnify, arrMagnify[i-1])
		}
	}
}

func (qosManager *QosCtrlManager) volUpdateLimit(limitArgs *qosArgs) {
	defer qosManager.Unlock()
	qosManager.Lock()

	log.LogWarnf("action[volUpdateLimit] vol %v try set limit iopsrlimit[%v],iopswlimit[%v],flowrlimit[%v],flowwlimit[%v]",
		qosManager.vol.Name, limitArgs.iopsRVal, limitArgs.iopsWVal, limitArgs.flowRVal, limitArgs.flowWVal)

	if limitArgs.iopsWVal != 0 {
		qosManager.serverFactorLimitMap[proto.IopsWriteType].Total = limitArgs.iopsWVal
	}
	if limitArgs.iopsRVal != 0 {
		qosManager.serverFactorLimitMap[proto.IopsReadType].Total = limitArgs.iopsRVal
	}
	if limitArgs.flowWVal != 0 {
		qosManager.serverFactorLimitMap[proto.FlowWriteType].Total = limitArgs.flowWVal
	}
	if limitArgs.flowRVal != 0 {
		qosManager.serverFactorLimitMap[proto.FlowReadType].Total = limitArgs.flowRVal
	}

	for i := proto.IopsReadType; i <= proto.FlowWriteType; i++ {
		limitf := qosManager.serverFactorLimitMap[i]
		log.LogWarnf("action[volUpdateLimit] vol [%v] after set type [%v] [%v,%v,%v,%v]",
			qosManager.vol.Name, proto.QosTypeString(i), limitf.Allocated, limitf.NeedAfterAlloc, limitf.Total, limitf.Buffer)
	}
}

func (qosManager *QosCtrlManager) getQosMagnify(factorTYpe uint32) uint32 {
	return qosManager.serverFactorLimitMap[factorTYpe].Magnify
}

func (qosManager *QosCtrlManager) getQosLimit(factorTYpe uint32) uint64 {
	return qosManager.serverFactorLimitMap[factorTYpe].Total
}

func (qosManager *QosCtrlManager) initClientQosInfo(clientID uint64, host string) (limitRsp2Client *proto.LimitRsp2Client, err error) {

	log.LogInfof("action[initClientQosInfo] vol %v clientID %v host %v", qosManager.vol.Name, clientID, host)
	clientInitInfo := proto.NewClientReportLimitInfo()
	cliCnt := qosManager.defaultClientCnt
	if cliCnt <= proto.QosDefaultClientCnt {
		cliCnt = proto.QosDefaultClientCnt
	}
	if len(qosManager.serverFactorLimitMap) > int(cliCnt) {
		cliCnt = uint32(len(qosManager.serverFactorLimitMap))
	}

	limitRsp2Client = proto.NewLimitRsp2Client()
	limitRsp2Client.ID = clientID
	limitRsp2Client.Enable = qosManager.qosEnable

	factorType := proto.IopsReadType

	defer qosManager.Unlock()
	qosManager.Lock()

	for factorType <= proto.FlowWriteType {
		var initLimit uint64
		serverLimit := qosManager.serverFactorLimitMap[factorType]
		initLimit = serverLimit.Total / uint64(cliCnt)

		if serverLimit.Buffer > initLimit {
			serverLimit.Buffer -= initLimit
			serverLimit.Allocated += initLimit
		} else {
			serverLimit.Buffer = 0
		}

		clientInitInfo.FactorMap[factorType] = &proto.ClientLimitInfo{
			UsedLimit:  initLimit,
			UsedBuffer: 0,
			Used:       0,
			Need:       0,
		}
		cliInfo := clientInitInfo.FactorMap[factorType]
		if factorType == proto.FlowWriteType || factorType == proto.FlowReadType {
			if cliInfo.UsedLimit > 1*util.GB/8 {
				cliInfo.UsedLimit = 1 * util.GB / 8
			}
		} else {
			if cliInfo.UsedLimit > 5000 {
				cliInfo.UsedLimit = 5000
			}
		}

		limitRsp2Client.FactorMap[factorType] = cliInfo
		limitRsp2Client.Magnify[factorType] = serverLimit.Magnify

		log.LogInfof("action[initClientQosInfo] vol [%v] clientID [%v] factorType [%v] init client info and set limitRsp2Client [%v]"+
			"server total[%v] used [%v] buffer [%v]",
			qosManager.vol.Name, clientID, proto.QosTypeString(factorType),
			initLimit, serverLimit.Total, serverLimit.Allocated, serverLimit.Buffer)
		factorType++
	}

	qosManager.cliInfoMgrMap[clientID] = &ClientInfoMgr{
		Cli:    clientInitInfo,
		Assign: limitRsp2Client,
		Time:   time.Now(),
		ID:     clientID,
		host:   host,
	}
	log.LogInfof("action[initClientQosInfo] vol [%v] clientID [%v]", qosManager.vol.Name, clientID)
	return
}

func (serverLimit *ServerFactorLimit) getDstLimit(factorType uint32, used, need uint64) (dstLimit uint64) {
	if factorType == proto.FlowWriteType || factorType == proto.FlowReadType {
		if need > used {
			need = used
		}
		if (need + used) < 10*util.MB/8 {
			dstLimit = uint64(float32(need+used) * 2)
		} else if (need + used) < 50*util.MB/8 {
			dstLimit = uint64(float32(need+used) * 1.5)
		} else if (need + used) < 100*util.MB/8 {
			dstLimit = uint64(float32(need+used) * 1.2)
		} else if (need + used) < 1*util.GB/8 {
			dstLimit = uint64(float32(need+used) * 1.1)
		} else {
			dstLimit = uint64(float32(need+used) + 1*util.GB/8)
		}
	} else {
		if (need + used) < 100 {
			dstLimit = uint64(float32(need+used) * 2)
		} else if (need + used) < 500 {
			dstLimit = uint64(float32(need+used) * 1.5)
		} else if (need + used) < 1000 {
			dstLimit = uint64(float32(need+used) * 1.2)
		} else if (need + used) < 5000 {
			dstLimit = uint64(float32(need+used) * 1.2)
		} else {
			dstLimit = uint64(float32(need+used) + 1000)
		}
	}
	return
}

func (serverLimit *ServerFactorLimit) dispatch() {
	log.LogInfof("action[dispatch] type [%v] start!", serverLimit.Type)
	for {
		select {
		case request := <-serverLimit.requestCh:
			serverLimit.updateLimitFactor(request)
		case <-serverLimit.done:
			log.LogErrorf("done ServerFactorLimit type (%v)", serverLimit.Type)
			return
		}
	}
}

// handle client request and rsp with much more if buffer is enough according rules of allocate
func (s *ServerFactorLimit) updateLimitFactor(req interface{}) {

	request := req.(*qosRequestArgs)
	clientID := request.clientID
	factorType := request.factorType
	clientReq := request.clientReq
	assignInfo := request.assignInfo
	rsp2Client := request.rsp2Client
	lastClientInfo := request.lastClientInfo

	log.LogInfof("action[updateLimitFactor] vol [%v] clientID [%v] type [%v],client report [%v,%v,%v,%v] last client report [%v,%v,%v,%v] periodically cal Assign [%v,%v]",
		s.qosManager.vol.Name, clientID, proto.QosTypeString(factorType),
		clientReq.Used, clientReq.Need, clientReq.UsedLimit, clientReq.UsedBuffer,
		lastClientInfo.Used, lastClientInfo.Need, lastClientInfo.UsedLimit, lastClientInfo.UsedBuffer,
		assignInfo.UsedLimit, assignInfo.UsedBuffer)

	rsp2Client.UsedLimit = assignInfo.UsedLimit
	rsp2Client.UsedBuffer = assignInfo.UsedBuffer

	// flow limit and buffer not enough,client need more
	if (clientReq.Need + clientReq.Used) > (assignInfo.UsedLimit + assignInfo.UsedBuffer) {
		log.LogInfof("action[updateLimitFactor] vol [%v] clientID [%v] type [%v], need [%v] used [%v], used limit [%v]",
			s.qosManager.vol.Name, clientID, proto.QosTypeString(factorType), clientReq.Need, clientReq.Used, clientReq.UsedLimit)

		dstLimit := s.getDstLimit(factorType, clientReq.Used, clientReq.Need)

		// Assign already  allocated the buffer for client
		if dstLimit > assignInfo.UsedLimit+assignInfo.UsedBuffer {
			additionBuffer := dstLimit - assignInfo.UsedLimit - assignInfo.UsedBuffer
			// if buffer is available then balance must not effect, try use buffer as possible as can
			if s.Buffer > 0 {
				log.LogDebugf("action[updateLimitFactor] vol [%v] clientID [%v] type [%v] client need more buffer [%v] serverlimit buffer [%v] used [%v]",
					s.qosManager.vol.Name, clientID, proto.QosTypeString(factorType),
					additionBuffer, s.Buffer, s.Allocated)

				// calc dst buffer for client to expand
				// ignore the case of s.used be zero.  used should large then 0 because dstLimit isn't zero and be part of s.used
				dstUsedBuffer := uint64(float32(dstLimit*s.Buffer/s.Allocated) * 0.5)
				if dstUsedBuffer > dstLimit {
					dstUsedBuffer = dstLimit
				}
				if assignInfo.UsedBuffer < dstUsedBuffer {
					additionBuffer = dstUsedBuffer - assignInfo.UsedBuffer
					if additionBuffer > s.Buffer {
						rsp2Client.UsedBuffer += s.Buffer
						assignInfo.UsedBuffer = rsp2Client.UsedBuffer
						s.Allocated += s.Buffer
						s.Buffer = 0
					} else {
						rsp2Client.UsedBuffer = dstUsedBuffer
						assignInfo.UsedBuffer = dstUsedBuffer
						s.Buffer -= additionBuffer
						s.Allocated += additionBuffer
					}
				}
			}
		}
	}
	log.LogInfof("action[updateLimitFactor] vol [%v] [clientID [%v] type [%v] rsp2Client.UsedLimit [%v], UsedBuffer [%v]",
		s.qosManager.vol.Name, clientID, proto.QosTypeString(factorType), rsp2Client.UsedLimit, rsp2Client.UsedBuffer)
	request.wg.Done()
}

func (qosManager *QosCtrlManager) init(cluster *Cluster, host string) (limit *proto.LimitRsp2Client, err error) {
	log.LogInfof("action[qosManage.init] vol [%v] host %v", qosManager.vol.Name, host)
	var id uint64
	if id, err = cluster.idAlloc.allocateCommonID(); err == nil {
		return qosManager.initClientQosInfo(id, host)
	}
	return
}

func (qosManager *QosCtrlManager) HandleClientQosReq(reqClientInfo *proto.ClientReportLimitInfo, clientID uint64) (limitRsp *proto.LimitRsp2Client, err error) {
	log.LogInfof("action[HandleClientQosReq] vol [%v] reqClientInfo from [%v], enable [%v]",
		qosManager.vol.Name, clientID, qosManager.qosEnable)

	qosManager.RLock()
	clientInfo, lastExist := qosManager.cliInfoMgrMap[clientID]
	if !lastExist || reqClientInfo == nil {
		qosManager.RUnlock()
		log.LogWarnf("action[HandleClientQosReq] vol [%v] id [%v] addr [%v] not exist", qosManager.vol.Name, clientID, reqClientInfo.Host)
		return qosManager.initClientQosInfo(clientID, reqClientInfo.Host)
	}
	qosManager.RUnlock()

	limitRsp = proto.NewLimitRsp2Client()
	limitRsp.Enable = qosManager.qosEnable
	limitRsp.ID = reqClientInfo.ID
	limitRsp.ReqPeriod = qosManager.ClientReqPeriod
	limitRsp.HitTriggerCnt = uint8(qosManager.ClientHitTriggerCnt)

	if !qosManager.qosEnable {
		clientInfo.Cli = reqClientInfo
		limitRsp.FactorMap = reqClientInfo.FactorMap
		clientInfo.Assign = limitRsp
		clientInfo.Time = time.Now()
		for i := proto.IopsReadType; i <= proto.FlowWriteType; i++ {
			log.LogInfof("action[HandleClientQosReq] vol [%v] [%v,%v,%v,%v]", qosManager.vol.Name,
				reqClientInfo.FactorMap[i].Used,
				reqClientInfo.FactorMap[i].Need,
				reqClientInfo.FactorMap[i].UsedLimit,
				reqClientInfo.FactorMap[i].UsedBuffer)
		}
		return
	}

	index := 0
	wg := &sync.WaitGroup{}
	wg.Add(len(reqClientInfo.FactorMap))
	for factorType, clientFactor := range reqClientInfo.FactorMap {
		limitRsp.FactorMap[factorType] = &proto.ClientLimitInfo{}
		serverLimit := qosManager.serverFactorLimitMap[factorType]
		limitRsp.Magnify[factorType] = serverLimit.Magnify

		request := &qosRequestArgs{
			clientID:       clientID,
			factorType:     factorType,
			clientReq:      clientFactor,
			lastClientInfo: clientInfo.Cli.FactorMap[factorType],
			assignInfo:     clientInfo.Assign.FactorMap[factorType],
			rsp2Client:     limitRsp.FactorMap[factorType],
			wg:             wg,
		}
		serverLimit.requestCh <- request
		index++
	}
	wg.Wait()

	clientInfo.Cli = reqClientInfo
	clientInfo.Assign = limitRsp
	clientInfo.Time = time.Now()

	return
}
func (qosManager *QosCtrlManager) updateServerLimitByClientsInfo(factorType uint32) {
	var (
		cliSum                      proto.ClientLimitInfo
		nextStageNeed, nextStageUse uint64
	)
	qosManager.RLock()

	// get sum of data from all clients reports
	for host, cliInfo := range qosManager.cliInfoMgrMap {
		cliFactor := cliInfo.Cli.FactorMap[factorType]
		cliSum.Used += cliFactor.Used
		cliSum.Need += cliFactor.Need
		cliSum.UsedLimit += cliFactor.UsedLimit
		cliSum.UsedBuffer += cliFactor.UsedBuffer
		log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] host [%v] type [%v] used [%v] need [%v] limit [%v] buffer [%v]",
			qosManager.vol.Name, host, proto.QosTypeString(factorType),
			cliFactor.Used, cliFactor.Need, cliFactor.UsedLimit, cliFactor.UsedBuffer)
	}

	log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] type [%v] all clisum used:[%v] need:[%v] limit:[%v] buffer:[%v]",
		qosManager.vol.Name, proto.QosTypeString(factorType), cliSum.Used, cliSum.Need, cliSum.UsedLimit, cliSum.UsedBuffer)

	serverLimit := qosManager.serverFactorLimitMap[factorType]
	serverLimit.CliUsed = cliSum.Used
	serverLimit.CliNeed = cliSum.Need
	qosManager.RUnlock()

	if cliSum.Used > serverLimit.Total {
		log.LogWarnf("action[updateServerLimitByClientsInfo] vol [%v] type [%v] sum of all clients flow use [%v]  larger than volume totoal [%v]",
			qosManager.vol.Name, proto.QosTypeString(factorType), cliSum.Used, serverLimit.Total)
	}

	log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] type [%v] last epoch serverLimit total:[%v] used:[%v] need:[%v] buffer:[%v] limitrate:[%v]",
		qosManager.vol.Name, proto.QosTypeString(factorType), serverLimit.Total, serverLimit.Allocated, serverLimit.NeedAfterAlloc, serverLimit.Buffer, serverLimit.LimitRate)

	serverLimit.Buffer = 0
	nextStageUse = cliSum.Used
	nextStageNeed = cliSum.Need
	if serverLimit.Total >= nextStageUse {
		serverLimit.Buffer = serverLimit.Total - nextStageUse
		log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] reset server buffer [%v] all clients nextStageUse [%v]",
			qosManager.vol.Name, serverLimit.Buffer, nextStageUse)
		if nextStageNeed > serverLimit.Buffer {
			nextStageNeed -= serverLimit.Buffer
			nextStageUse += serverLimit.Buffer
			serverLimit.Buffer = 0
			log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] reset server buffer [%v] all clients nextStageNeed [%v] too nuch",
				qosManager.vol.Name, serverLimit.Buffer, nextStageNeed)
		} else {
			serverLimit.Buffer -= nextStageNeed
			log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] reset server buffer [%v] all clients nextStageNeed [%v]",
				qosManager.vol.Name, serverLimit.Buffer, nextStageNeed)
			nextStageUse += nextStageNeed
			nextStageNeed = 0
		}
	} else { // usage large than limitation
		log.LogInfof("action[updateServerLimitByClientsInfo] vol[%v] type [%v] clients needs [%v] plus overuse [%v],get nextStageNeed [%v]",
			qosManager.vol.Name, proto.QosTypeString(factorType), nextStageNeed, nextStageUse-serverLimit.Total,
			nextStageNeed+nextStageUse-serverLimit.Total)
		nextStageNeed += nextStageUse - serverLimit.Total
		nextStageUse = serverLimit.Total
	}

	serverLimit.Allocated = nextStageUse
	serverLimit.NeedAfterAlloc = nextStageNeed

	log.LogInfof("action[updateServerLimitByClientsInfo] vol [%v] type [%v] after cal get next stage need [%v] and used [%v], buffer [%v]",
		qosManager.vol.Name, proto.QosTypeString(factorType), nextStageNeed, nextStageUse, serverLimit.Buffer)
	// get the limitRate,additionFlowNeed should be zero if total used can increase
	serverLimit.LimitRate = 0
	if serverLimit.NeedAfterAlloc > 0 {
		serverLimit.LimitRate = float32(serverLimit.NeedAfterAlloc) / float32(serverLimit.Allocated+serverLimit.NeedAfterAlloc)
	}

	return
}

func (qosManager *QosCtrlManager) assignClientsNewQos(factorType uint32) {
	qosManager.RLock()
	serverLimit := qosManager.serverFactorLimitMap[factorType]
	var bufferAllocated uint64
	// recalculate client Assign limit and buffer
	for host, cliInfoMgr := range qosManager.cliInfoMgrMap {
		cliInfo := cliInfoMgr.Cli.FactorMap[factorType]
		assignInfo := cliInfoMgr.Assign.FactorMap[factorType]

		if cliInfo.Used+cliInfoMgr.Cli.FactorMap[factorType].Need == 0 {
			assignInfo.UsedLimit = 0
			assignInfo.UsedBuffer = 0
		} else {
			assignInfo.UsedLimit = uint64(float32(cliInfo.Used+cliInfo.Need) * (1 - serverLimit.LimitRate))
			if serverLimit.Allocated != 0 {
				assignInfo.UsedBuffer = uint64(float32(serverLimit.Buffer*assignInfo.UsedLimit/serverLimit.Allocated) * 0.5)
			}

			log.LogDebugf("action[assignClientsNewQos] Assign host [%v] limit [%v] buffer [%v]",
				host, assignInfo.UsedLimit, assignInfo.UsedBuffer)

			// buffer left may be quit large and we should not used up and doen't mean if buffer large than used limit line
			if assignInfo.UsedBuffer > assignInfo.UsedLimit {
				assignInfo.UsedBuffer = assignInfo.UsedLimit
			}
		}

		bufferAllocated += assignInfo.UsedBuffer
		log.LogInfof("action[assignClientsNewQos] vol [%v] host [%v] type [%v] assignInfo used limit [%v], used buffer [%v]",
			qosManager.vol.Name, host, proto.QosTypeString(factorType), assignInfo.UsedLimit, assignInfo.UsedBuffer)
	}

	qosManager.RUnlock()

	if serverLimit.Buffer > bufferAllocated {
		serverLimit.Buffer -= bufferAllocated
	} else {
		serverLimit.Buffer = 0
		log.LogWarnf("action[assignClientsNewQos] vol [%v] type [%v] clients buffer [%v] and server buffer used up trigger flow limit overall",
			qosManager.vol.Name, proto.QosTypeString(factorType), bufferAllocated)
	}

	log.LogInfof("action[assignClientsNewQos] vol [%v]  type [%v] serverLimit buffer:[%v] used:[%v] need:[%v] total:[%v]",
		qosManager.vol.Name, proto.QosTypeString(factorType),
		serverLimit.Buffer, serverLimit.Allocated, serverLimit.NeedAfterAlloc, serverLimit.Total)
}

func (vol *Vol) checkQos() {
	vol.qosManager.Lock()
	// check expire client and delete from map
	tTime := time.Now()
	for id, cli := range vol.qosManager.cliInfoMgrMap {
		if cli.Time.Add(20 * time.Second).Before(tTime) {
			log.LogWarnf("action[checkQos] vol [%v] Id [%v] addr [%v] be delete in case of long time no request",
				vol.Name, id, cli.host)
			delete(vol.qosManager.cliInfoMgrMap, id)
		}
	}

	if !vol.qosManager.qosEnable {
		vol.qosManager.Unlock()
		return
	}
	vol.qosManager.Unlock()

	// periodically updateServerLimitByClientsInfo and get assigned limit info for all clients
	// with last report info from client and qos control info
	for factorType := proto.IopsReadType; factorType <= proto.FlowWriteType; factorType++ {
		// calc all clients and get real used and need value , used value should less then total
		vol.qosManager.updateServerLimitByClientsInfo(factorType)
		// update client assign info by result above
		vol.qosManager.assignClientsNewQos(factorType)

		serverLimit := vol.qosManager.serverFactorLimitMap[factorType]
		log.LogInfof("action[UpdateAllQosInfo] vol name [%v] type [%v] after updateServerLimitByClientsInfo get limitRate:[%v] server total [%v] used [%v] need [%v] buffer [%v]",
			vol.Name, proto.QosTypeString(factorType), serverLimit.LimitRate, serverLimit.Total, serverLimit.Allocated, serverLimit.NeedAfterAlloc, serverLimit.Buffer)
	}
}

func (vol *Vol) getQosStatus() interface{} {

	type qosStatus struct {
		ServerFactorLimitMap map[uint32]*ServerFactorLimit // vol qos data for iops w/r and flow w/r
		DefaultClientCnt     uint32
		InitClientCnt        uint32
		QosEnable            bool
		ClientReqPeriod      uint32
		ClientHitTriggerCnt  uint32
	}

	return &qosStatus{
		ServerFactorLimitMap: vol.qosManager.serverFactorLimitMap,
		QosEnable:            vol.qosManager.qosEnable,
		ClientReqPeriod:      vol.qosManager.ClientReqPeriod,
		ClientHitTriggerCnt:  vol.qosManager.ClientHitTriggerCnt,
	}
}

func (vol *Vol) getClientLimitInfo(id uint64, ip string) (interface{}, error) {
	log.LogInfof("action[getClientLimitInfo] vol [%v] id [%v] ip [%v]", vol.Name, id, ip)
	vol.qosManager.RLock()
	defer vol.qosManager.RUnlock()

	if id > 0 {
		if info, ok := vol.qosManager.cliInfoMgrMap[id]; ok {
			if len(ip) > 0 && util.GetIp(info.host) != ip {
				return nil, fmt.Errorf("ip info [%v] not equal with request [%v]", info.host, ip)
			}
			return info, nil
		}
	} else {
		if len(ip) != 0 {
			var resp []*ClientInfoMgr
			for _, info := range vol.qosManager.cliInfoMgrMap {
				// http connection port  from client will change time by time,so ignore port here
				if util.GetIp(info.host) == ip {
					resp = append(resp, info)
				}
			}
			if len(resp) > 0 {
				return resp, nil
			}
		} else {
			return vol.qosManager.cliInfoMgrMap, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (vol *Vol) volQosEnable(c *Cluster, enable bool) error {
	log.LogWarnf("action[qosEnable] vol %v, set qos enable [%v], qosmgr[%v]", vol.Name, enable, vol.qosManager)
	vol.qosManager.qosEnable = enable
	return c.syncUpdateVol(vol)
}

func (vol *Vol) updateClientParam(c *Cluster, period, triggerCnt uint64) error {
	vol.qosManager.ClientHitTriggerCnt = uint32(triggerCnt)
	vol.qosManager.ClientReqPeriod = uint32(period)
	return c.syncUpdateVol(vol)
}

func (vol *Vol) volQosUpdateMagnify(c *Cluster, magnifyArgs *qosArgs) error {
	vol.qosManager.volUpdateMagnify(magnifyArgs)
	return c.syncUpdateVol(vol)
}

func (vol *Vol) volQosUpdateLimit(c *Cluster, limitArgs *qosArgs) error {
	vol.qosManager.volUpdateLimit(limitArgs)
	return c.syncUpdateVol(vol)
}

func (vol *Vol) updateViewCache(c *Cluster) {
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.createTime, vol.CacheTTL, vol.VolType)
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
	dpResps := vol.dataPartitions.getDataPartitionsView(0)
	view.DataPartitions = dpResps
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
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	mpViews = make([]*proto.MetaPartitionView, 0)
	for _, mp := range vol.MetaPartitions {
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

		if err := vol.deleteDataPartitionFromDataNode(c, dp.createTaskToDeleteDataPartition(addr)); err != nil {
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
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkStatus occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkStatus occurred panic")
		}
	}()
	vol.updateViewCache(c)
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	if vol.Status != markDelete {
		return
	}
	log.LogInfof("action[volCheckStatus] vol[%v],status[%v]", vol.Name, vol.Status)
	metaTasks := vol.getTasksToDeleteMetaPartitions()
	dataTasks := vol.getTasksToDeleteDataPartitions()

	if len(metaTasks) == 0 && len(dataTasks) == 0 {
		vol.deleteVolFromStore(c)
	}
	go func() {
		for _, metaTask := range metaTasks {
			vol.deleteMetaPartitionFromMetaNode(c, metaTask)
		}

		for _, dataTask := range dataTasks {
			vol.deleteDataPartitionFromDataNode(c, dataTask)
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
		return
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
		return
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

	if err = c.syncDeleteVol(vol); err != nil {
		return
	}

	// delete the metadata of the meta and data partitionMap first
	vol.deleteDataPartitionsFromStore(c)
	vol.deleteMetaPartitionsFromStore(c)
	// then delete the volume
	c.deleteVol(vol.Name)
	c.volStatInfo.Delete(vol.Name)
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
	vol.volLock.RLock()
	count = len(vol.dataPartitions.partitionMap)
	vol.volLock.RUnlock()
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
	log.LogWarnf("action[splitMetaPartition],next partition[%v],start[%v],end[%v]", nextMp.PartitionID, nextMp.Start, nextMp.End)
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
		wg          sync.WaitGroup
	)

	errChannel := make(chan error, vol.mpReplicaNum)

	if c.isFaultDomain(vol) {
		if hosts, peers, err = c.getHostFromDomainZone(vol.domainId, TypeMetaPartition, vol.mpReplicaNum); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromDomainZone err[%v]", err)
			return nil, errors.NewError(err)
		}

	} else {
		var excludeZone []string
		zoneNum := c.decideZoneNum(vol.crossZone)

		if hosts, peers, err = c.getHostFromNormalZone(TypeMetaPartition, excludeZone, nil, nil, int(vol.mpReplicaNum), zoneNum, vol.zoneName); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromNormalZone err[%v]", err)
			return nil, errors.NewError(err)
		}

	}

	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return nil, errors.NewError(err)
	}

	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, vol.Name, vol.ID)
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
	log.LogInfof("action[doCreateMetaPartition] success,volName[%v],partition[%v]", vol.Name, partitionID)
	return
}

func setVolFromArgs(args *VolVarargs, vol *Vol) {
	vol.zoneName = args.zoneName
	vol.Capacity = args.capacity
	vol.FollowerRead = args.followerRead
	vol.authenticate = args.authenticate
	vol.enablePosixAcl = args.enablePosixAcl

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
		zoneName:       vol.zoneName,
		description:    vol.description,
		capacity:       vol.Capacity,
		followerRead:   vol.FollowerRead,
		authenticate:   vol.authenticate,
		dpSelectorName: vol.dpSelectorName,
		dpSelectorParm: vol.dpSelectorParm,
		enablePosixAcl: vol.enablePosixAcl,

		coldArgs: args,
	}
}
