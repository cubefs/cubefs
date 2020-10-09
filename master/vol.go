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

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

// Vol represents a set of meta partitionMap and data partitionMap
type Vol struct {
	ID                 uint64
	Name               string
	Owner              string
	OSSAccessKey       string
	OSSSecretKey       string
	dpReplicaNum       uint8
	mpReplicaNum       uint8
	Status             uint8
	threshold          float32
	dataPartitionSize  uint64
	Capacity           uint64 // GB
	NeedToLowerReplica bool
	FollowerRead       bool
	authenticate       bool
	crossZone          bool
	zoneName           string
	enableToken        bool
	tokens             map[string]*proto.Token
	tokensLock         sync.RWMutex
	MetaPartitions     map[uint64]*MetaPartition `graphql:"-"`
	mpStoreType        proto.StoreType           //metaPartition store type
	mpsLock            sync.RWMutex
	dataPartitions     *DataPartitionMap
	mpsCache           []byte
	viewCache          []byte
	createDpMutex      sync.RWMutex
	createMpMutex      sync.RWMutex
	createTime         int64
	description        string
	sync.RWMutex
}

type createVolArg struct {
	name         string
	owner        string
	zoneName     string
	description  string
	mpCount      int
	dpReplicaNum int
	mpReplicaNum int
	mpStoreType  proto.StoreType
	size         uint64
	capacity     uint64
	followerRead bool
	authenticate bool
	crossZone    bool
	enableToken  bool
}

func newVol(id uint64, createTime int64, arg *createVolArg) (vol *Vol) {
	vol = &Vol{ID: id, Name: arg.name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}
	vol.dataPartitions = newDataPartitionMap(arg.name)
	dpReplicaNum := arg.dpReplicaNum
	if dpReplicaNum < defaultReplicaNum {
		dpReplicaNum = defaultReplicaNum
	}
	vol.dpReplicaNum = uint8(dpReplicaNum)
	vol.threshold = defaultMetaPartitionMemUsageThreshold
	mpReplicaNum := arg.mpReplicaNum
	if mpReplicaNum < defaultReplicaNum {
		mpReplicaNum = defaultReplicaNum
	}
	vol.mpReplicaNum = uint8(mpReplicaNum)
	vol.Owner = arg.owner
	dpSize := arg.size
	if dpSize < util.GB {
		dpSize = util.DefaultDataPartitionSize
	}
	vol.dataPartitionSize = dpSize
	vol.Capacity = arg.capacity
	vol.FollowerRead = arg.followerRead
	vol.authenticate = arg.authenticate
	vol.crossZone = arg.crossZone
	vol.zoneName = arg.zoneName
	vol.viewCache = make([]byte, 0)
	vol.mpsCache = make([]byte, 0)
	vol.mpStoreType = arg.mpStoreType
	vol.createTime = createTime
	vol.enableToken = arg.enableToken
	vol.tokens = make(map[string]*proto.Token, 0)
	vol.description = arg.description
	return
}

func newVolFromVolValue(vv *volValue) (vol *Vol) {
	arg := &createVolArg{
		name:         vv.Name,
		owner:        vv.Owner,
		zoneName:     vv.ZoneName,
		size:         vv.DataPartitionSize,
		capacity:     vv.Capacity,
		dpReplicaNum: int(vv.DpReplicaNum),
		mpReplicaNum: int(vv.ReplicaNum),
		followerRead: vv.FollowerRead,
		authenticate: vv.Authenticate,
		crossZone:    vv.CrossZone,
		enableToken:  vv.EnableToken,
		description:  vv.Description,
		mpStoreType:  vv.MpStoreType,
	}
	vol = newVol(vv.ID, vv.CreateTime, arg)
	// overwrite oss secure
	vol.OSSAccessKey, vol.OSSSecretKey = vv.OSSAccessKey, vv.OSSSecretKey
	vol.Status = vv.Status
	return vol
}

func (vol *Vol) refreshOSSSecure() (key, secret string) {
	vol.OSSAccessKey = util.RandomString(16, util.Numeric|util.LowerLetter|util.UpperLetter)
	vol.OSSSecretKey = util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
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

func (vol *Vol) getDataPartitionsView() (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(false, 0)
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
	if vol.getDataPartitionsCount() == 0 && vol.Status != markDelete {
		c.batchCreateDataPartition(vol, 1)
	}
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitionMap {
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
	dps := vol.cloneDataPartitionMap()
	for _, dp := range dps {
		host := dp.getToBeDecommissionHost(int(vol.dpReplicaNum))
		if host == "" {
			continue
		}
		if err = dp.removeOneReplicaByHost(c, host); err != nil {
			log.LogErrorf("action[checkReplicaNum],vol[%v],err[%v]", vol.Name, err)
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
			var nextStart uint64
			if mp.MaxInodeID <= 0 {
				nextStart = mp.Start + defaultMetaPartitionInodeIDStep
			} else {
				nextStart = mp.MaxInodeID + defaultMetaPartitionInodeIDStep
			}
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

func (vol *Vol) checkAutoDataPartitionCreation(c *Cluster) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkAutoDataPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoDataPartitionCreation occurred panic")
		}
	}()
	if vol.status() == markDelete {
		return
	}
	if vol.capacity() == 0 {
		return
	}
	usedSpace := vol.totalUsedSpace() / util.GB
	if usedSpace >= vol.capacity() {
		vol.setAllDataPartitionsToReadOnly()
		return
	}
	vol.setStatus(normal)

	if vol.status() == normal && !c.DisableAutoAllocate {
		vol.autoCreateDataPartitions(c)
	}
}

func (vol *Vol) autoCreateDataPartitions(c *Cluster) {
	if (vol.Capacity > 200000 && vol.dataPartitions.readableAndWritableCnt < 200) || vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions {
		count := vol.calculateExpansionNum()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(vol, count)
	}
}

// Calculate the expansion number (the number of data partitions to be allocated to the given volume)
func (vol *Vol) calculateExpansionNum() (count int) {
	c := float64(vol.Capacity) * float64(volExpansionRatio) * float64(util.GB) / float64(util.DefaultDataPartitionSize)
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
	return vol.dataPartitions.totalUsedSpace()
}

func (vol *Vol) updateViewCache(c *Cluster) {
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.createTime)
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
	_, err = metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", mp.VolName, mp.PartitionID, err)
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
	if hosts, peers, err = c.chooseTargetMetaHosts("", nil, nil, int(vol.mpReplicaNum), vol.crossZone, vol.zoneName, vol.mpStoreType); err != nil {
		log.LogErrorf("action[doCreateMetaPartition] chooseTargetMetaHosts err[%v]", err)
		return nil, errors.NewError(err)
	}
	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return nil, errors.NewError(err)
	}
	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, vol.Name, vol.ID, vol.mpStoreType)
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
