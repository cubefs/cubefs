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
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/google/uuid"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/datanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// Cluster stores all the cluster-level information.
type Cluster struct {
	Name                         string
	CreateTime                   int64
	vols                         map[string]*Vol
	dataNodes                    sync.Map
	metaNodes                    sync.Map
	volMutex                     sync.RWMutex // volume mutex
	createVolMutex               sync.RWMutex // create volume mutex
	mnMutex                      sync.RWMutex // meta node mutex
	dnMutex                      sync.RWMutex // data node mutex
	badPartitionMutex            sync.RWMutex // BadDataPartitionIds and BadMetaPartitionIds operate mutex
	leaderInfo                   *LeaderInfo
	cfg                          *clusterConfig
	retainLogs                   uint64
	idAlloc                      *IDAllocator
	t                            *topology
	dataNodeStatInfo             *nodeStatInfo
	metaNodeStatInfo             *nodeStatInfo
	zoneStatInfos                map[string]*proto.ZoneStat
	volStatInfo                  sync.Map
	domainManager                *DomainManager
	BadDataPartitionIds          *sync.Map
	BadMetaPartitionIds          *sync.Map
	DisableAutoAllocate          bool
	FaultDomain                  bool
	needFaultDomain              bool // FaultDomain is true and normal zone aleady used up
	fsm                          *MetadataFsm
	partition                    raftstore.Partition
	MasterSecretKey              []byte
	lastZoneIdxForNode           int
	zoneIdxMux                   sync.Mutex //
	zoneList                     []string
	followerReadManager          *followerReadManager
	diskQosEnable                bool
	QosAcceptLimit               *rate.Limiter
	apiLimiter                   *ApiLimiter
	DecommissionDisks            sync.Map
	DecommissionLimit            uint64
	checkAutoCreateDataPartition bool
	masterClient                 *masterSDK.MasterClient
	checkDataReplicasEnable      bool
	fileStatsEnable              bool
	clusterUuid                  string
	clusterUuidEnable            bool
}

type followerReadManager struct {
	volDataPartitionsView map[string][]byte
	status                map[string]bool
	lastUpdateTick        map[string]time.Time
	needCheck             bool
	c                     *Cluster
	rwMutex               sync.RWMutex
}

func newFollowerReadManager(c *Cluster) (mgr *followerReadManager) {
	mgr = new(followerReadManager)
	mgr.volDataPartitionsView = make(map[string][]byte)
	mgr.status = make(map[string]bool)
	mgr.lastUpdateTick = make(map[string]time.Time)
	mgr.c = c
	return
}

func (mgr *followerReadManager) reSet() {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	mgr.volDataPartitionsView = make(map[string][]byte)
	mgr.status = make(map[string]bool)
}

func (mgr *followerReadManager) getVolumeDpView() {
	var (
		err      error
		volNames []string
	)
	if err, volNames = mgr.c.loadVolsName(); err != nil {
		panic(err)
	}
	if mgr.c.masterClient.Leader() == "" {
		log.LogDebugf("followerReadManager.getVolumeDpView but master leader not ready")
		return
	}
	for _, name := range volNames {
		log.LogDebugf("followerReadManager.getVolumeDpView %v", name)
		if view, err := mgr.c.masterClient.ClientAPI().GetDataPartitions(name); err == nil {
			mgr.updateVolViewFromLeader(name, view)
		}
	}
}

func (mgr *followerReadManager) checkStatus() {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	timeNow := time.Now()
	for volNm, lastTime := range mgr.lastUpdateTick {
		if lastTime.Before(timeNow.Add(-time.Second * 30)) {
			mgr.status[volNm] = false
			log.LogInfof("action[checkStatus] volume %v expired last time %v, now %v", volNm, lastTime, timeNow)
		}
	}
}

func (mgr *followerReadManager) updateVolViewFromLeader(key string, view *proto.DataPartitionsView) {
	log.LogDebugf("followerReadManager.updateVolViewFromLeader key %v", key)
	if !mgr.checkViewContent(key, view, true) {
		log.LogErrorf("updateVolViewFromLeader. checkViewContent failed")
		return
	}
	log.LogDebugf("action[updateVolViewFromLeader] volume %v be updated, value len %v", key, len(view.DataPartitions))

	reply := newSuccessHTTPReply(view)
	if body, err := json.Marshal(reply); err != nil {
		log.LogErrorf("action[updateDpResponseCache] marshal error %v", err)
		return
	} else {
		mgr.rwMutex.Lock()
		defer mgr.rwMutex.Unlock()
		mgr.volDataPartitionsView[key] = body
	}

	mgr.status[key] = true
	mgr.lastUpdateTick[key] = time.Now()
}

func (mgr *followerReadManager) checkViewContent(volName string, view *proto.DataPartitionsView, isUpdate bool) (ok bool) {
	if !isUpdate && !mgr.needCheck {
		return true
	}
	log.LogDebugf("volName %v do check content", volName)

	if len(view.DataPartitions) == 0 {
		log.LogErrorf("checkViewContent. get nil partitions volName %v", volName)
		return false
	}
	for i := 0; i < len(view.DataPartitions); i++ {
		dp := view.DataPartitions[i]
		if len(dp.Hosts) == 0 {
			log.LogErrorf("checkViewContent. dp id %v, leader %v, status %v", dp.PartitionID, dp.LeaderAddr, dp.Status)
			ok = false
		}
	}
	log.LogDebugf("checkViewContent. volName %v dp cnt %v check pass", volName, len(view.DataPartitions))
	return true
}

func (mgr *followerReadManager) getVolViewAsFollower(key string) (value []byte, ok bool) {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()
	ok = true
	value, _ = mgr.volDataPartitionsView[key]
	log.LogDebugf("getVolViewAsFollower. volume %v return!", key)
	return
}

func (mgr *followerReadManager) IsVolViewReady(volName string) bool {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	return mgr.status[volName]
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition, cfg *clusterConfig) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.vols = make(map[string]*Vol, 0)
	c.cfg = cfg
	c.t = newTopology()
	c.BadDataPartitionIds = new(sync.Map)
	c.BadMetaPartitionIds = new(sync.Map)
	c.dataNodeStatInfo = new(nodeStatInfo)
	c.metaNodeStatInfo = new(nodeStatInfo)
	c.FaultDomain = cfg.faultDomain
	c.zoneStatInfos = make(map[string]*proto.ZoneStat)
	c.followerReadManager = newFollowerReadManager(c)
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
	c.domainManager = newDomainManager(c)
	c.QosAcceptLimit = rate.NewLimiter(rate.Limit(c.cfg.QosMasterAcceptLimit), proto.QosDefaultBurst)
	c.apiLimiter = newApiLimiter()
	c.DecommissionLimit = defaultDecommissionParallelLimit
	c.checkAutoCreateDataPartition = false
	c.masterClient = masterSDK.NewMasterClient(nil, false)
	return
}

func (c *Cluster) scheduleTask() {
	c.scheduleToCheckDataPartitions()
	c.scheduleToLoadDataPartitions()
	c.scheduleToCheckReleaseDataPartitions()
	c.scheduleToCheckHeartbeat()
	c.scheduleToCheckMetaPartitions()
	c.scheduleToUpdateStatInfo()
	c.scheduleToManageDp()
	c.scheduleToCheckVolStatus()
	c.scheduleToCheckVolQos()
	c.scheduleToCheckDiskRecoveryProgress()
	c.scheduleToCheckMetaPartitionRecoveryProgress()
	c.scheduleToLoadMetaPartitions()
	c.scheduleToReduceReplicaNum()
	c.scheduleToCheckNodeSetGrpManagerStatus()
	c.scheduleToCheckFollowerReadCache()
	c.scheduleToCheckDecommissionDataNode()
	c.scheduleToCheckDecommissionDisk()
	c.scheduleToCheckDataReplicas()
}

func (c *Cluster) masterAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) tryToChangeLeaderByHost() error {
	return c.partition.TryToLeader(1)
}

func (c *Cluster) scheduleToUpdateStatInfo() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.updateStatInfo()
			}
			time.Sleep(2 * time.Minute)
		}
	}()

}

func (c *Cluster) addNodeSetGrp(ns *nodeSet, load bool) (err error) {
	log.LogWarnf("addNodeSetGrp nodeSet id[%v] zonename[%v] load[%v] grpManager init[%v]",

		ns.ID, ns.zoneName, load, c.domainManager.init)
	if c.domainManager.init {
		err = c.domainManager.putNodeSet(ns, load)
		c.putZoneDomain(false)
	}
	return
}

const (
	TypeMetaPartition uint32 = 0x01
	TypeDataPartition uint32 = 0x02
)

func (c *Cluster) getHostFromDomainZone(domainId uint64, createType uint32, replicaNum uint8) (hosts []string, peers []proto.Peer, err error) {
	hosts, peers, err = c.domainManager.getHostFromNodeSetGrp(domainId, replicaNum, createType)
	return
}

func (c *Cluster) scheduleToManageDp() {
	go func() {
		// check volumes after switching leader two minutes
		time.Sleep(2 * time.Minute)
		c.checkAutoCreateDataPartition = true
	}()

	// schedule delete dataPartition
	go func() {

		time.Sleep(2 * time.Minute)

		for {

			if c.partition != nil && c.partition.IsRaftLeader() {

				vols := c.copyVols()

				for _, vol := range vols {

					if proto.IsHot(vol.VolType) {
						continue
					}

					vol.autoDeleteDp(c)
				}
			}

			time.Sleep(2 * time.Minute)
		}
	}()
}

func (c *Cluster) scheduleToCheckDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkDataPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition))
		}
	}()
}

func (c *Cluster) scheduleToCheckVolStatus() {
	go func() {
		//check vols after switching leader two minutes
		for {
			if c.partition.IsRaftLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					vol.checkStatus(c)
				}
			}
			time.Sleep(time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition))
		}
	}()
}
func (c *Cluster) scheduleToCheckFollowerReadCache() {
	go func() {
		for {
			if !c.partition.IsRaftLeader() {
				c.followerReadManager.getVolumeDpView()
				c.followerReadManager.checkStatus()
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func (c *Cluster) scheduleToCheckVolQos() {
	go func() {
		//check vols after switching leader two minutes
		for {
			if c.partition.IsRaftLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					vol.checkQos()
				}
			}
			// time.Sleep(time.Second * time.Duration(c.cfg.IntervalToCheckQos))
			time.Sleep(time.Duration(float32(time.Second) * 0.5))
		}
	}()
}

func (c *Cluster) scheduleToCheckNodeSetGrpManagerStatus() {
	go func() {
		for {
			if c.FaultDomain == false || !c.partition.IsRaftLeader() {
				time.Sleep(time.Minute)
				continue
			}
			c.domainManager.checkAllGrpState()
			c.domainManager.checkExcludeZoneState()
			time.Sleep(5 * time.Second)
		}
	}()
}

func (c *Cluster) scheduleToLoadDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.doLoadDataPartitions()
			}
			time.Sleep(time.Second * 5)
		}
	}()
}

// Check the replica status of each data partition.
func (c *Cluster) checkDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDataPartitions occurred panic")
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		readWrites := vol.checkDataPartitions(c)
		vol.dataPartitions.setReadWriteDataPartitions(readWrites, c.Name)
		vol.dataPartitions.updateResponseCache(true, 0, vol.VolType)
		msg := fmt.Sprintf("action[checkDataPartitions],vol[%v] can readWrite partitions:%v  ",
			vol.Name, vol.dataPartitions.readableAndWritableCnt)
		log.LogInfo(msg)

		if c.checkAutoCreateDataPartition {
			vol.checkAutoDataPartitionCreation(c)
		}
	}
}

func (c *Cluster) doLoadDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("doLoadDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"doLoadDataPartitions occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		vol.loadDataPartition(c)
	}
}

func (c *Cluster) scheduleToCheckReleaseDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.releaseDataPartitionAfterLoad()
			}
			time.Sleep(time.Second * defaultIntervalToFreeDataPartition)
		}
	}()
}

// Release the memory used for loading the data partition.
func (c *Cluster) releaseDataPartitionAfterLoad() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("releaseDataPartitionAfterLoad occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"releaseDataPartitionAfterLoad occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		vol.releaseDataPartitions(c.cfg.numberOfDataPartitionsToFree, c.cfg.secondsToFreeDataPartitionAfterLoad)
	}
}

func (c *Cluster) scheduleToCheckHeartbeat() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkLeaderAddr()
				c.checkDataNodeHeartbeat()
				// update load factor
				setOverSoldFactor(c.cfg.ClusterLoadFactor)
			}
			time.Sleep(time.Second * defaultIntervalToCheckHeartbeat)
		}
	}()

	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkMetaNodeHeartbeat()
			}
			time.Sleep(time.Second * defaultIntervalToCheckHeartbeat)
		}
	}()
}

func (c *Cluster) checkLeaderAddr() {
	leaderID, _ := c.partition.LeaderTerm()
	c.leaderInfo.addr = AddrDatabase[leaderID]
}

func (c *Cluster) checkDataNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.dataNodes.Range(func(addr, dataNode interface{}) bool {
		node := dataNode.(*DataNode)
		node.checkLiveness()
		task := node.createHeartbeatTask(c.masterAddr(), c.diskQosEnable)
		tasks = append(tasks, task)
		return true
	})
	c.addDataNodeTasks(tasks)
}

func (c *Cluster) checkMetaNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.metaNodes.Range(func(addr, metaNode interface{}) bool {
		node := metaNode.(*MetaNode)
		node.checkHeartbeat()
		task := node.createHeartbeatTask(c.masterAddr(), c.fileStatsEnable)
		hbReq := task.Request.(*proto.HeartBeatRequest)
		for _, vol := range c.vols {
			if vol.FollowerRead {
				hbReq.FLReadVols = append(hbReq.FLReadVols, vol.Name)
			}
		}
		tasks = append(tasks, task)
		return true
	})
	c.addMetaNodeTasks(tasks)
}

func (c *Cluster) scheduleToCheckMetaPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkMetaPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.IntervalToCheckDataPartition))
		}
	}()
}

func (c *Cluster) checkMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMetaPartitions occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		vol.checkMetaPartitions(c)
	}
}

func (c *Cluster) scheduleToReduceReplicaNum() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkVolReduceReplicaNum()
			}
			time.Sleep(5 * time.Minute)
		}
	}()
}

func (c *Cluster) checkVolReduceReplicaNum() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkVolReduceReplicaNum occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkVolReduceReplicaNum occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		vol.checkReplicaNum(c)
	}
}

func (c *Cluster) getInvalidIDNodes() (nodes []*InvalidNodeView) {
	metaNodes := c.getNotConsistentIDMetaNodes()
	nodes = append(nodes, metaNodes...)
	dataNodes := c.getNotConsistentIDDataNodes()
	nodes = append(nodes, dataNodes...)
	return
}

func (c *Cluster) scheduleToCheckDataReplicas() {
	go func() {
		for {
			if c.checkDataReplicasEnable {
				if c.partition != nil && c.partition.IsRaftLeader() {
					c.checkDataReplicas()
				}
			}
			time.Sleep(1 * time.Minute)
		}
	}()
}

func (c *Cluster) checkDataReplicas() {
	lackReplicaDataPartitions, _ := c.checkLackReplicaAndHostDataPartitions()
	if len(lackReplicaDataPartitions) == 0 {
		return
	}

	successCnt := 0
	for _, dp := range lackReplicaDataPartitions {
		if success, _ := c.autoAddDataReplica(dp); success {
			successCnt += 1
		}
	}
	failCnt := len(lackReplicaDataPartitions) - successCnt
	log.LogInfof("action[checkDataReplicas] autoAddDataReplica successCnt[%v], failedCnt[%v]", successCnt, failCnt)
}

func (c *Cluster) getNotConsistentIDMetaNodes() (metaNodes []*InvalidNodeView) {
	metaNodes = make([]*InvalidNodeView, 0)
	c.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		notConsistent, oldID := c.hasNotConsistentIDMetaPartitions(metanode)
		if notConsistent {
			metaNodes = append(metaNodes, &InvalidNodeView{Addr: metanode.Addr, ID: metanode.ID, OldID: oldID, NodeType: "meta"})
		}
		return true
	})
	return
}

func (c *Cluster) hasNotConsistentIDMetaPartitions(metanode *MetaNode) (notConsistent bool, oldID uint64) {
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			for _, peer := range mp.Peers {
				if peer.Addr == metanode.Addr && peer.ID != metanode.ID {
					vol.mpsLock.RUnlock()
					return true, peer.ID
				}
			}
		}
		vol.mpsLock.RUnlock()
	}
	return
}

func (c *Cluster) getNotConsistentIDDataNodes() (dataNodes []*InvalidNodeView) {
	dataNodes = make([]*InvalidNodeView, 0)
	c.dataNodes.Range(func(key, value interface{}) bool {
		datanode, ok := value.(*DataNode)
		if !ok {
			return true
		}
		notConsistent, oldID := c.hasNotConsistentIDDataPartitions(datanode)
		if notConsistent {
			dataNodes = append(dataNodes, &InvalidNodeView{Addr: datanode.Addr, ID: datanode.ID, OldID: oldID, NodeType: "data"})
		}
		return true
	})
	return
}

func (c *Cluster) hasNotConsistentIDDataPartitions(datanode *DataNode) (notConsistent bool, oldID uint64) {
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.dataPartitions.partitions {
			for _, peer := range mp.Peers {
				if peer.Addr == datanode.Addr && peer.ID != datanode.ID {
					return true, peer.ID
				}
			}
		}
	}
	return
}

func (c *Cluster) updateDataNodeBaseInfo(nodeAddr string, id uint64) (err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	value, ok := c.dataNodes.Load(nodeAddr)
	if !ok {
		err = fmt.Errorf("node %v is not exist", nodeAddr)
		return
	}
	dataNode := value.(*DataNode)
	if dataNode.ID == id {
		return
	}

	if err = c.syncDeleteDataNode(dataNode); err != nil {
		return
	}
	dataNode.ID = id
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		return
	}
	//partitions := c.getAllMetaPartitionsByMetaNode(nodeAddr)
	return
}

func (c *Cluster) updateMetaNodeBaseInfo(nodeAddr string, id uint64) (err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	value, ok := c.metaNodes.Load(nodeAddr)
	if !ok {
		err = fmt.Errorf("node %v is not exist", nodeAddr)
		return
	}
	metaNode := value.(*MetaNode)
	if metaNode.ID == id {
		return
	}
	if err = c.syncDeleteMetaNode(metaNode); err != nil {
		return
	}
	metaNode.ID = id
	if err = c.syncUpdateMetaNode(metaNode); err != nil {
		return
	}
	//partitions := c.getAllMetaPartitionsByMetaNode(nodeAddr)
	return
}

func (c *Cluster) addMetaNode(nodeAddr, zoneName string, nodesetId uint64) (id uint64, err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()

	var metaNode *MetaNode
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		if nodesetId > 0 && nodesetId != metaNode.ID {
			return metaNode.ID, fmt.Errorf("addr already in nodeset [%v]", nodeAddr)
		}
		return metaNode.ID, nil
	}

	metaNode = newMetaNode(nodeAddr, zoneName, c.Name)
	zone, err := c.t.getZone(zoneName)
	if err != nil {
		zone = c.t.putZoneIfAbsent(newZone(zoneName))
	}

	var ns *nodeSet
	if nodesetId > 0 {
		if ns, err = zone.getNodeSet(nodesetId); err != nil {
			return nodesetId, err
		}
	} else {
		ns = zone.getAvailNodeSetForMetaNode()
		if ns == nil {
			if ns, err = zone.createNodeSet(c); err != nil {
				goto errHandler
			}
		}
	}

	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	metaNode.ID = id
	metaNode.NodeSetID = ns.ID
	log.LogInfof("action[addMetaNode] metanode id[%v] zonename [%v] add meta node to nodesetid[%v]", id, zoneName, ns.ID)
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errHandler
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		goto errHandler
	}
	c.t.putMetaNode(metaNode)
	// nodeset be avaliable first time can be put into nodesetGrp

	c.addNodeSetGrp(ns, false)
	c.metaNodes.Store(nodeAddr, metaNode)
	log.LogInfof("action[addMetaNode],clusterID[%v] metaNodeAddr:%v,nodeSetId[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.Capacity)
	return
errHandler:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] metaNodeAddr:%v err:%v ",
		c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) addDataNode(nodeAddr, zoneName string, nodesetId uint64) (id uint64, err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	var dataNode *DataNode
	if node, ok := c.dataNodes.Load(nodeAddr); ok {
		dataNode = node.(*DataNode)
		if nodesetId > 0 && nodesetId != dataNode.NodeSetID {
			return dataNode.ID, fmt.Errorf("addr already in nodeset [%v]", nodeAddr)
		}
		return dataNode.ID, nil
	}

	dataNode = newDataNode(nodeAddr, zoneName, c.Name)
	zone, err := c.t.getZone(zoneName)
	if err != nil {
		zone = c.t.putZoneIfAbsent(newZone(zoneName))
	}
	var ns *nodeSet
	if nodesetId > 0 {
		if ns, err = zone.getNodeSet(nodesetId); err != nil {
			return nodesetId, err
		}
	} else {
		ns = zone.getAvailNodeSetForDataNode()
		if ns == nil {
			if ns, err = zone.createNodeSet(c); err != nil {
				goto errHandler
			}
		}
	}
	// allocate dataNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	dataNode.ID = id
	dataNode.NodeSetID = ns.ID
	log.LogInfof("action[addDataNode] datanode id[%v] zonename [%v] add node to nodesetid[%v]", id, zoneName, ns.ID)
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errHandler
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		goto errHandler
	}
	c.t.putDataNode(dataNode)
	// nodeset be avaliable first time can be put into nodesetGrp

	c.addNodeSetGrp(ns, false)

	c.dataNodes.Store(nodeAddr, dataNode)
	log.LogInfof("action[addDataNode],clusterID[%v] dataNodeAddr:%v,nodeSetId[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.Capacity)
	return
errHandler:
	err = fmt.Errorf("action[addDataNode],clusterID[%v] dataNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) checkCorruptDataPartitions() (inactiveDataNodes []string, corruptPartitions []*DataPartition, err error) {

	inactiveDataNodes = make([]string, 0)
	corruptPartitions = make([]*DataPartition, 0)

	vols := c.copyVols()
	for _, vol := range vols {
		if vol.Status == markDelete || !proto.IsHot(vol.VolType) {
			continue
		}
		for _, dp := range vol.dataPartitions.partitions {
			if dp.getLeaderAddr() == "" {
				corruptPartitions = append(corruptPartitions, dp)
			}
		}
	}

	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		if !dataNode.isActive {
			inactiveDataNodes = append(inactiveDataNodes, dataNode.Addr)
		}

		return true
	})

	log.LogInfof("clusterID[%v] inactiveDataNodes:%v  corruptPartitions count:[%v]",
		c.Name, inactiveDataNodes, len(corruptPartitions))
	return
}

func (c *Cluster) checkLackReplicaAndHostDataPartitions() (lackReplicaDataPartitions []*DataPartition, err error) {
	lackReplicaDataPartitions = make([]*DataPartition, 0)
	vols := c.copyVols()
	for _, vol := range vols {
		var dps *DataPartitionMap
		dps = vol.dataPartitions
		for _, dp := range dps.partitions {
			if dp.ReplicaNum > uint8(len(dp.Hosts)) && len(dp.Hosts) == len(dp.Replicas) && dp.IsDecommissionInitial() {
				lackReplicaDataPartitions = append(lackReplicaDataPartitions, dp)
			}
		}
	}
	log.LogInfof("clusterID[%v] checkLackReplicaAndHostDataPartitions count:[%v]", c.Name, len(lackReplicaDataPartitions))
	return
}

func (c *Cluster) checkLackReplicaDataPartitions() (lackReplicaDataPartitions []*DataPartition, err error) {
	lackReplicaDataPartitions = make([]*DataPartition, 0)
	vols := c.copyVols()
	for _, vol := range vols {
		var dps *DataPartitionMap
		dps = vol.dataPartitions
		for _, dp := range dps.partitions {
			if dp.ReplicaNum > uint8(len(dp.Hosts)) {
				lackReplicaDataPartitions = append(lackReplicaDataPartitions, dp)
			}
		}
	}
	log.LogInfof("clusterID[%v] lackReplicaDataPartitions count:[%v]", c.Name, len(lackReplicaDataPartitions))
	return
}

func (c *Cluster) checkReplicaOfDataPartitions() (lackReplicaDPs []*DataPartition, unavailableReplicaDPs []*DataPartition, err error) {
	lackReplicaDPs = make([]*DataPartition, 0)
	unavailableReplicaDPs = make([]*DataPartition, 0)
	vols := c.copyVols()
	for _, vol := range vols {
		var dps *DataPartitionMap
		dps = vol.dataPartitions
		for _, dp := range dps.partitions {
			if dp.ReplicaNum > uint8(len(dp.Hosts)) {
				lackReplicaDPs = append(lackReplicaDPs, dp)
			}

			for _, replica := range dp.Replicas {
				if replica.Status == proto.Unavailable {
					unavailableReplicaDPs = append(unavailableReplicaDPs, dp)
					break
				}
			}

		}
	}
	log.LogInfof("clusterID[%v] lackReplicaDataPartitions count:[%v], unavailableReplicaDataPartitions count:[%v]",
		c.Name, len(lackReplicaDPs), len(unavailableReplicaDPs))
	return
}

func (c *Cluster) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	vols := c.copyVols()

	for _, vol := range vols {
		if dp, err = vol.getDataPartitionByID(partitionID); err == nil {
			return
		}
	}

	err = dataPartitionNotFound(partitionID)
	return
}

func (c *Cluster) getMetaPartitionByID(id uint64) (mp *MetaPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		if mp, err = vol.metaPartition(id); err == nil {
			return
		}
	}
	err = metaPartitionNotFound(id)
	return
}

func (c *Cluster) putVol(vol *Vol) {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	if _, ok := c.vols[vol.Name]; !ok {
		c.vols[vol.Name] = vol
	}
}

func (c *Cluster) getVol(volName string) (vol *Vol, err error) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	vol, ok := c.vols[volName]
	if !ok {
		err = proto.ErrVolNotExists
	}
	return
}

func (c *Cluster) deleteVol(name string) {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	delete(c.vols, name)
	return
}

func (c *Cluster) markDeleteVol(name, authKey string, force bool) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
	)

	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[markDeleteVol] err[%v]", err)
		return proto.ErrVolNotExists
	}

	if proto.IsCold(vol.VolType) && vol.totalUsedSpace() > 0 && !force {
		return fmt.Errorf("ec-vol can't be deleted if ec used size not equal 0, now(%d)", vol.totalUsedSpace())
	}

	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}

	vol.Status = markDelete
	if err = c.syncUpdateVol(vol); err != nil {
		vol.Status = normal
		return proto.ErrPersistenceByRaft
	}

	return
}

func (c *Cluster) batchCreatePreLoadDataPartition(vol *Vol, preload *DataPartitionPreLoad) (err error, dps []*DataPartition) {
	if proto.IsHot(vol.VolType) {
		return fmt.Errorf("vol type is not warm"), nil
	}

	total := overSoldCap(uint64(preload.preloadCacheCapacity))
	reqCreateCount := (total-1)/(util.DefaultDataPartitionSize/util.GB) + 1

	for i := 0; i < int(reqCreateCount); i++ {
		log.LogInfof("create preload data partition (%v) total (%v)", i, reqCreateCount)

		var dp *DataPartition
		if dp, err = c.createDataPartition(vol.Name, preload); err != nil {
			log.LogErrorf("create preload data partition fail: volume(%v) err(%v)", vol.Name, err)
			return err, nil
		}

		dps = append(dps, dp)
	}

	return
}

func (c *Cluster) batchCreateDataPartition(vol *Vol, reqCount int, init bool) (err error) {
	if !init {
		if _, err = vol.needCreateDataPartition(); err != nil {
			log.LogWarnf("action[batchCreateDataPartition] create data partition failed, err[%v]", err)
			return
		}
	}
	for i := 0; i < reqCount; i++ {
		if c.DisableAutoAllocate {
			log.LogWarn("disable auto allocate dataPartition")
			return fmt.Errorf("cluster is disable auto allocate dataPartition")
		}

		if _, err = c.createDataPartition(vol.Name, nil); err != nil {
			log.LogErrorf("action[batchCreateDataPartition] after create [%v] data partition,occurred error,err[%v]", i, err)
			break
		}
	}
	return
}

func (c *Cluster) isFaultDomain(vol *Vol) bool {
	var specifyZoneNeedDomain bool
	if c.FaultDomain && !vol.crossZone && !c.needFaultDomain {
		if value, ok := c.t.zoneMap.Load(vol.zoneName); ok {
			if value.(*Zone).status == unavailableZone {
				specifyZoneNeedDomain = true
			}
		}
	}
	log.LogInfof("action[isFaultDomain] vol [%v] zoname [%v] FaultDomain[%v] need fault domain[%v] vol crosszone[%v] default[%v] specifyZoneNeedDomain[%v] domainOn[%v]",
		vol.Name, vol.zoneName, c.FaultDomain, c.needFaultDomain, vol.crossZone, vol.defaultPriority, specifyZoneNeedDomain, vol.domainOn)
	domainOn := c.FaultDomain &&
		(vol.domainOn ||
			(!vol.crossZone && c.needFaultDomain) || specifyZoneNeedDomain ||
			(vol.crossZone && (!vol.defaultPriority ||
				(vol.defaultPriority && (c.needFaultDomain || len(c.t.domainExcludeZones) <= 1)))))
	if !vol.domainOn && domainOn {
		vol.domainOn = domainOn
		vol.updateViewCache(c)
		c.syncUpdateVol(vol)
		log.LogInfof("action[isFaultDomain] vol [%v] set domainOn", vol.Name)
	}
	return vol.domainOn
}

// Synchronously create a data partition.
// 1. Choose one of the available data nodes.
// 2. Assign it a partition ID.
// 3. Communicate with the data node to synchronously create a data partition.
// - If succeeded, replicate the data through raft and persist it to RocksDB.
// - Otherwise, throw errors

func (c *Cluster) createDataPartition(volName string, preload *DataPartitionPreLoad) (dp *DataPartition, err error) {
	log.LogInfof("action[createDataPartition] preload [%v]", preload)
	var (
		vol          *Vol
		partitionID  uint64
		targetHosts  []string
		targetPeers  []proto.Peer
		wg           sync.WaitGroup
		isPreload    bool
		partitionTTL int64
	)

	vol = c.vols[volName]

	dpReplicaNum := vol.dpReplicaNum
	zoneName := vol.zoneName

	if preload != nil {
		dpReplicaNum = uint8(preload.preloadReplicaNum)
		zoneName = preload.preloadZoneName
		isPreload = true
		partitionTTL = int64(preload.PreloadCacheTTL)*util.OneDaySec() + time.Now().Unix()
	}

	if vol, err = c.getVol(volName); err != nil {
		return
	}

	vol.createDpMutex.Lock()
	defer vol.createDpMutex.Unlock()

	errChannel := make(chan error, dpReplicaNum)

	if c.isFaultDomain(vol) {
		if targetHosts, targetPeers, err = c.getHostFromDomainZone(vol.domainId, TypeDataPartition, dpReplicaNum); err != nil {
			goto errHandler
		}
	} else {
		zoneNum := c.decideZoneNum(vol.crossZone)
		if targetHosts, targetPeers, err = c.getHostFromNormalZone(TypeDataPartition, nil, nil, nil,
			int(dpReplicaNum), zoneNum, zoneName); err != nil {
			goto errHandler
		}
	}

	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errHandler
	}

	dp = newDataPartition(partitionID, dpReplicaNum, volName, vol.ID, proto.GetDpType(vol.VolType, isPreload), partitionTTL)
	dp.Hosts = targetHosts
	dp.Peers = targetPeers

	log.LogInfof("action[createDataPartition] partitionID [%v] get host [%v]", partitionID, targetHosts)

	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()

			var diskPath string

			if diskPath, err = c.syncCreateDataPartitionToDataNode(host, vol.dataPartitionSize,
				dp, dp.Peers, dp.Hosts, proto.NormalCreateDataPartition, dp.PartitionType); err != nil {
				errChannel <- err
				return
			}

			dp.Lock()
			defer dp.Unlock()
			if err = dp.afterCreation(host, diskPath, c); err != nil {
				errChannel <- err
			}
		}(host)
	}

	wg.Wait()

	select {
	case err = <-errChannel:
		for _, host := range targetHosts {
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				_, err := dp.getReplica(host)
				if err != nil {
					return
				}
				task := dp.createTaskToDeleteDataPartition(host)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.addDataNodeTasks(tasks)
			}(host)
		}
		wg.Wait()
		goto errHandler
	default:
		dp.total = vol.dataPartitionSize
		dp.Status = proto.Unavailable
	}

	if err = c.syncAddDataPartition(dp); err != nil {
		goto errHandler
	}

	vol.dataPartitions.put(dp)
	log.LogInfof("action[createDataPartition] success,volName[%v],partitionId[%v], count[%d]", volName, partitionID, len(vol.dataPartitions.partitions))
	return

errHandler:
	err = fmt.Errorf("action[createDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, volName, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) syncCreateDataPartitionToDataNode(host string, size uint64, dp *DataPartition,
	peers []proto.Peer, hosts []string, createType int, partitionType int) (diskPath string, err error) {
	log.LogInfof("action[syncCreateDataPartitionToDataNode] dp [%v] createtype[%v], partitionType[%v]", dp.PartitionID, createType, partitionType)
	dataNode, err := c.dataNode(host)
	if err != nil {
		return
	}
	task := dp.createTaskToCreateDataPartition(host, size, peers, hosts, createType, partitionType, dataNode.getDecommissionedDisks())
	var resp *proto.Packet
	if resp, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return string(resp.Data), nil
}

func (c *Cluster) syncCreateMetaPartitionToMetaNode(host string, mp *MetaPartition) (err error) {
	hosts := make([]string, 0)
	hosts = append(hosts, host)
	tasks := mp.buildNewMetaPartitionTasks(hosts, mp.Peers, mp.volName)
	metaNode, err := c.metaNode(host)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(tasks[0]); err != nil {
		return
	}
	return
}

// decideZoneNum
// if vol is not cross zone, return 1
// if vol enable cross zone and the zone number of cluster less than defaultReplicaNum return 2
// otherwise, return defaultReplicaNum
func (c *Cluster) decideZoneNum(crossZone bool) (zoneNum int) {
	if !crossZone {
		return 1
	}

	var zoneLen int
	if c.FaultDomain {
		zoneLen = len(c.t.domainExcludeZones)
	} else {
		zoneLen = c.t.zoneLen()
	}

	if zoneLen < defaultReplicaNum {
		zoneNum = 2
	} else {
		zoneNum = defaultReplicaNum
	}

	return zoneNum
}

func (c *Cluster) chooseZone2Plus1(zones []*Zone, excludeNodeSets []uint64, excludeHosts []string,
	nodeType uint32, replicaNum int) (hosts []string, peers []proto.Peer, err error) {

	if replicaNum < 2 || replicaNum > 3 {
		return nil, nil, fmt.Errorf("action[chooseZone2Plus1] replicaNum [%v]", replicaNum)
	}

	zoneList := make([]*Zone, 2)
	if zones[0].getSpaceLeft(nodeType) < zones[1].getSpaceLeft(nodeType) {
		zoneList[0] = zones[0]
		zoneList[1] = zones[1]
	} else {
		zoneList[0] = zones[1]
		zoneList[1] = zones[0]
	}

	for i := 2; i < len(zones); i++ {
		spaceLeft := zones[i].getSpaceLeft(nodeType)
		if spaceLeft > zoneList[0].getSpaceLeft(nodeType) {
			if spaceLeft > zoneList[1].getSpaceLeft(nodeType) {
				zoneList[1] = zones[i]
			} else {
				zoneList[0] = zones[i]
			}
		}
	}
	log.LogInfof("action[chooseZone2Plus1] type [%v] after check,zone0 [%v] left [%v] zone1 [%v] left [%v]",
		nodeType, zoneList[0].name, zoneList[0].getSpaceLeft(nodeType), zoneList[1].name, zoneList[1].getSpaceLeft(nodeType))

	num := 1
	for _, zone := range zoneList {
		selectedHosts, selectedPeers, e := zone.getAvailNodeHosts(nodeType, excludeNodeSets, excludeHosts, num)
		if e != nil {
			log.LogErrorf("action[getHostFromNormalZone] error [%v]", e)
			return nil, nil, e
		}

		hosts = append(hosts, selectedHosts...)
		peers = append(peers, selectedPeers...)
		log.LogInfof("action[chooseZone2Plus1] zone [%v] left [%v] get hosts[%v]",
			zone.name, zone.getSpaceLeft(nodeType), selectedHosts)

		num = replicaNum - num
	}
	log.LogInfof("action[chooseZone2Plus1] finally get hosts[%v]", hosts)

	return hosts, peers, nil
}

func (c *Cluster) chooseZoneNormal(zones []*Zone, excludeNodeSets []uint64, excludeHosts []string,
	nodeType uint32, replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	log.LogInfof("action[chooseZoneNormal] zones[%s] nodeType[%d] replicaNum[%d]", printZonesName(zones), nodeType, replicaNum)

	c.zoneIdxMux.Lock()
	defer c.zoneIdxMux.Unlock()

	for i := 0; i < replicaNum; i++ {
		zone := zones[c.lastZoneIdxForNode]
		c.lastZoneIdxForNode = (c.lastZoneIdxForNode + 1) % len(zones)
		selectedHosts, selectedPeers, err := zone.getAvailNodeHosts(nodeType, excludeNodeSets, excludeHosts, 1)
		if err != nil {
			log.LogErrorf("action[chooseZoneNormal] error [%v]", err)
			return nil, nil, err
		}

		hosts = append(hosts, selectedHosts...)
		peers = append(peers, selectedPeers...)
	}

	return
}

func (c *Cluster) getHostFromNormalZone(nodeType uint32, excludeZones []string, excludeNodeSets []uint64,
	excludeHosts []string, replicaNum int,
	zoneNum int, specifiedZone string) (hosts []string, peers []proto.Peer, err error) {

	var zones []*Zone
	zones = make([]*Zone, 0)
	if replicaNum <= zoneNum {
		zoneNum = replicaNum
	}
	// when creating vol,user specified a zone,we reset zoneNum to 1,to be created partition with specified zone,
	//if specified zone is not writable,we choose a zone randomly
	if specifiedZone != "" {
		if err = c.checkNormalZoneName(specifiedZone); err != nil {
			Warn(c.Name, fmt.Sprintf("cluster[%v],specified zone[%v]is found", c.Name, specifiedZone))
			return
		}
		zoneList := strings.Split(specifiedZone, ",")
		for i := 0; i < len(zoneList); i++ {
			var zone *Zone
			if zone, err = c.t.getZone(zoneList[i]); err != nil {
				Warn(c.Name, fmt.Sprintf("cluster[%v],specified zone[%v]is found", c.Name, specifiedZone))
				return
			}
			zones = append(zones, zone)
		}
	} else {
		if nodeType == TypeDataPartition {
			if zones, err = c.t.allocZonesForDataNode(zoneNum, replicaNum, excludeZones); err != nil {
				return
			}
		} else {
			if zones, err = c.t.allocZonesForMetaNode(zoneNum, replicaNum, excludeZones); err != nil {
				return
			}
		}
	}

	if len(zones) == 1 {
		log.LogInfof("action[getHostFromNormalZone] zones [%v]", zones[0].name)
		if hosts, peers, err = zones[0].getAvailNodeHosts(nodeType, excludeNodeSets, excludeHosts, replicaNum); err != nil {
			log.LogErrorf("action[getHostFromNormalZone],err[%v]", err)
			return
		}
		goto result
	}

	hosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if excludeHosts == nil {
		excludeHosts = make([]string, 0)
	}

	if c.cfg.DefaultNormalZoneCnt == defaultNormalCrossZoneCnt && len(zones) >= defaultNormalCrossZoneCnt {
		if hosts, peers, err = c.chooseZoneNormal(zones, excludeNodeSets, excludeHosts, nodeType, replicaNum); err != nil {
			return
		}
	} else {
		if hosts, peers, err = c.chooseZone2Plus1(zones, excludeNodeSets, excludeHosts, nodeType, replicaNum); err != nil {
			return
		}
	}

result:
	log.LogInfof("action[getHostFromNormalZone] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNum, len(zones), hosts)
	if len(hosts) != replicaNum {
		log.LogErrorf("action[getHostFromNormalZone] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNum, len(zones), hosts)
		return nil, nil, errors.Trace(proto.ErrNoDataNodeToCreateDataPartition, "hosts len[%v],replicaNum[%v],zoneNum[%v],selectedZones[%v]",
			len(hosts), replicaNum, zoneNum, len(zones))
	}

	return
}

func (c *Cluster) dataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		err = errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
		return
	}

	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) metaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		err = errors.Trace(metaNodeNotFound(addr), "%v not found", addr)
		return
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) getAllDataPartitionByDataNode(addr string) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			for _, host := range dp.Hosts {
				if host == addr {
					partitions = append(partitions, dp)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getAllMetaPartitionByMetaNode(addr string) (partitions []*MetaPartition) {
	partitions = make([]*MetaPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			for _, host := range mp.Hosts {
				if host == addr {
					partitions = append(partitions, mp)
					break
				}
			}
		}
		vol.mpsLock.RUnlock()
	}

	return
}

func (c *Cluster) getAllDataPartitionIDByDatanode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			for _, host := range dp.Hosts {
				if host == addr {
					partitionIDs = append(partitionIDs, dp.PartitionID)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getAllMetaPartitionIDByMetaNode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			vol.mpsLock.RLock()
			for _, host := range mp.Hosts {
				if host == addr {
					partitionIDs = append(partitionIDs, mp.PartitionID)
					break
				}
			}
			vol.mpsLock.RUnlock()
		}
	}

	return
}

func (c *Cluster) getAllMetaPartitionsByMetaNode(addr string) (partitions []*MetaPartition) {
	partitions = make([]*MetaPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			vol.mpsLock.RLock()
			for _, host := range mp.Hosts {
				if host == addr {
					partitions = append(partitions, mp)
					break
				}
			}
			vol.mpsLock.RUnlock()
		}
	}
	return
}

func (c *Cluster) decommissionDataNodeCancel(dataNode *DataNode) (err error) {
	if dataNode.GetDecommissionStatus() != DecommissionRunning &&
		dataNode.GetDecommissionStatus() != markDecommission {
		err = fmt.Errorf("action[decommissionDataNodeCancel] dataNode[%v] status[%v] donot support cancel",
			dataNode.Addr, dataNode.GetDecommissionStatus())
		return
	}
	dataNode.SetDecommissionStatus(DecommissionStop)
	//may cause progress confused for new allocated dp
	dataNode.ToBeOffline = false
	//reset DecommissionDpTotal because new dp will be allocated on this node
	dataNode.DecommissionDpTotal = 0
	dataNode.DecommissionCompleteTime = time.Now().Unix()
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		log.LogErrorf("action[decommissionDataNodeCancel] dataNode[%v] sync update failed[ %v]",
			dataNode.Addr, err.Error())
		return err
	}
	partitions := c.getAllDecommissionDataPartitionByDataNodeAndTerm(dataNode.Addr, dataNode.DecommissionTerm)
	for _, dp := range partitions {
		dp.StopDecommission()
		c.syncUpdateDataPartition(dp)
	}
	log.LogDebugf("action[decommissionDataNodeCancel] dataNode[%v] cancel decommission, offline %v",
		dataNode.Addr, dataNode.ToBeOffline)
	return
}

func (c *Cluster) decommissionDiskCancel(disk *DecommissionDisk) (err error) {
	if disk.GetDecommissionStatus() != DecommissionRunning &&
		disk.GetDecommissionStatus() != markDecommission {
		err = fmt.Errorf("action[decommissionDiskCancel] dataNode[%v] disk[%s] status[%v] donot support cancel",
			disk.SrcAddr, disk.SrcAddr, disk.GetDecommissionStatus())
		return
	}
	disk.SetDecommissionStatus(DecommissionStop)
	disk.DecommissionDpTotal = 0
	if err = c.syncUpdateDecommissionDisk(disk); err != nil {
		log.LogErrorf("action[decommissionDiskCancel] dataNode[%v] disk[%s] sync update failed[ %v]",
			disk.SrcAddr, disk.SrcAddr, err.Error())
		return err
	}
	partitions := c.getAllDecommissionDataPartitionByDiskAndTerm(disk.SrcAddr, disk.DiskPath, disk.DecommissionTerm)
	for _, dp := range partitions {
		dp.StopDecommission()
		c.syncUpdateDataPartition(dp)
	}
	log.LogDebugf("action[decommissionDiskCancel] dataNode[%v] disk[%s] cancel decommission",
		disk.SrcAddr, disk.SrcAddr)
	return
}

func (c *Cluster) migrateDataNode(srcAddr, targetAddr string, raftForce bool, limit int) (err error) {
	msg := fmt.Sprintf("action[migrateDataNode], src(%s) migrate to target(%s) raftForcs(%v) limit(%v)",
		srcAddr, targetAddr, raftForce, limit)
	log.LogWarn(msg)

	srcNode, err := c.dataNode(srcAddr)
	if err != nil {
		return
	}

	if srcNode.GetDecommissionStatus() == markDecommission || srcNode.GetDecommissionStatus() == DecommissionRunning {
		err = fmt.Errorf("migrate src(%v) is still on working, please wait,check or cancel if abnormal", srcAddr)
		log.LogWarnf("action[migrateDataNode] %v", err)
		return
	}
	srcNode.markDecommission(targetAddr, raftForce, limit)
	c.syncUpdateDataNode(srcNode)
	log.LogInfof("action[migrateDataNode] %v return now", srcAddr)
	return
}

func (c *Cluster) decommissionDataNode(dataNode *DataNode, force bool) (err error) {
	return c.migrateDataNode(dataNode.Addr, "", false, 0)
}

func (c *Cluster) delDataNodeFromCache(dataNode *DataNode) {
	c.dataNodes.Delete(dataNode.Addr)
	c.t.deleteDataNode(dataNode)
	go dataNode.clean()
}

func (c *Cluster) delDecommissionDiskFromCache(dd *DecommissionDisk) {
	c.DecommissionDisks.Delete(dd.GenerateKey())
}

func (c *Cluster) decommissionSingleDp(dp *DataPartition, newAddr, offlineAddr string) (err error) {
	var dataNode *DataNode
	times := 0
	decommContinue := false

	ticker := time.NewTicker(time.Second * time.Duration(60))
	defer func() {
		ticker.Stop()
	}()

	if dp.SingleDecommissionStatus == datanode.DecommsionEnter || dp.SingleDecommissionStatus == datanode.DecommsionWaitAddRes {
		dp.SingleDecommissionStatus = datanode.DecommsionWaitAddRes
		dp.SingleDecommissionAddr = newAddr
		c.syncUpdateDataPartition(dp)

		if err = c.addDataReplica(dp, newAddr); err != nil {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v addDataReplica fail err %v", dp.PartitionID, err)
			goto ERR
		}
		log.LogWarnf("action[decommissionSingleDp] dp %v start wait add replica %v", dp.PartitionID, newAddr)

		for {
			select {
			case decommContinue = <-dp.singleDecommissionChan:
				if !decommContinue {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v addDataReplica get result decommContinue false", dp.PartitionID)
					dp.SetDecommissionStatus(DecommissionStop)
					goto ERR
				}
			case <-ticker.C:
				err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica result addr %v timeout %v times", dp.PartitionID, newAddr, times)
				log.LogWarnf("%v", err)
				if !c.partition.IsRaftLeader() {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica result addr %v master leader changed", dp.PartitionID, newAddr)
					goto ERR
				}
				times++
				if times == 60 {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica addr %v  timeout: 1hour", dp.PartitionID, newAddr)
					goto ERR
				}
			}
			if decommContinue == true {
				break
			}
		}

	}
	times = 0
	if dp.SingleDecommissionStatus == datanode.DecommsionWaitAddResFin {
		newAddr = dp.SingleDecommissionAddr
		if !c.partition.IsRaftLeader() {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica result addr %v master leader changed", dp.PartitionID, newAddr)
			goto ERR
		}
		if dataNode, err = c.dataNode(newAddr); err != nil {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v get offlineAddr %v err %v", dp.PartitionID, newAddr, err)
			log.LogErrorf("%v", err)
		}
		times = 0
		for {
			if dp.getLeaderAddr() == newAddr {
				err = nil
				break
			}
			log.LogInfof("action[decommissionSingleDp] dp %v try tryToChangeLeader addr %v", dp.PartitionID, newAddr)
			if err = dp.tryToChangeLeader(c, dataNode); err != nil {
				log.LogInfof("action[decommissionSingleDp] dp %v ChangeLeader to addr %v err %v", dp.PartitionID, newAddr, err)
			}

			select {
			case <-ticker.C:
				log.LogInfof("action[decommissionSingleDp] dp %v tryToChangeLeader addr %v again times %v", dp.PartitionID, newAddr, times)
				if !c.partition.IsRaftLeader() {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait tryToChangeLeader  addr %v master leader changed", dp.PartitionID, newAddr)
					goto ERR
				}
				times++
				if times == 60 {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v wait addDataReplica addr %v  timeout: 1hour", dp.PartitionID, newAddr)
					goto ERR
				}
			case decommContinue = <-dp.singleDecommissionChan:
				if !decommContinue {
					err = fmt.Errorf("action[decommissionSingleDp] dp %v tryToChangeLeader get result decommContinue false", dp.PartitionID)
					dp.SetDecommissionStatus(DecommissionStop)
					goto ERR
				}
			}
		}
		if !c.partition.IsRaftLeader() {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v wait tryToChangeLeader addr %v master leader changed", dp.PartitionID, newAddr)
			goto ERR
		}

		if dp.getLeaderAddr() != newAddr {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v  change leader failed", dp.PartitionID)
			goto ERR
		}
		log.LogInfof("action[decommissionSingleDp] dp %v try removeDataReplica %v", dp.PartitionID, offlineAddr)
		dp.SingleDecommissionStatus = datanode.DecommsionRemoveOld
		dp.SingleDecommissionAddr = offlineAddr
		c.syncUpdateDataPartition(dp)
	}

	if dp.SingleDecommissionStatus == datanode.DecommsionRemoveOld {
		if err = c.removeDataReplica(dp, offlineAddr, false, false); err != nil {
			err = fmt.Errorf("action[decommissionSingleDp] dp %v err %v", dp.PartitionID, err)
			goto ERR
		}
		log.LogInfof("action[decommissionSingleDp] dp %v success", dp.PartitionID)
		return
	}
ERR:
	log.LogErrorf("%v", err)
	return err
}

func (c *Cluster) autoAddDataReplica(dp *DataPartition) (success bool, err error) {
	var (
		targetHosts []string
		newAddr     string
		vol         *Vol
		zone        *Zone
		ns          *nodeSet
	)
	success = false

	dp.RLock()

	// not support
	if dp.isSpecialReplicaCnt() {
		dp.RUnlock()
		return
	}

	dp.RUnlock()

	// not support
	if !proto.IsNormalDp(dp.PartitionType) {
		return
	}

	var ok bool
	if vol, ok = c.vols[dp.VolName]; !ok {
		log.LogWarnf("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] vol not exist, PersistenceHosts:[%v]",
			c.Name, dp.VolName, dp.PartitionID, dp.Hosts)
		return
	}

	// not support
	if c.isFaultDomain(vol) {
		return
	}

	if vol.crossZone {
		zones := dp.getZones()
		if targetHosts, _, err = c.getHostFromNormalZone(TypeDataPartition, zones, nil, dp.Hosts, 1, 1, ""); err != nil {
			goto errHandler
		}
	} else {
		if zone, err = c.t.getZone(vol.zoneName); err != nil {
			log.LogWarnf("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] zone not exist, PersistenceHosts:[%v]",
				c.Name, dp.VolName, dp.PartitionID, dp.Hosts)
			return
		}
		nodeSets := dp.getNodeSets()
		if len(nodeSets) != 1 {
			log.LogWarnf("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] the number of nodeSets is not one, PersistenceHosts:[%v]",
				c.Name, dp.VolName, dp.PartitionID, dp.Hosts)
			return
		}
		if ns, err = zone.getNodeSet(nodeSets[0]); err != nil {
			goto errHandler
		}
		if targetHosts, _, err = ns.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
			goto errHandler
		}
	}

	newAddr = targetHosts[0]
	if err = c.addDataReplica(dp, newAddr); err != nil {
		goto errHandler
	}

	dp.Status = proto.ReadOnly
	dp.isRecover = true
	c.putBadDataPartitionIDs(nil, newAddr, dp.PartitionID)

	dp.RLock()
	c.syncUpdateDataPartition(dp)
	dp.RUnlock()

	log.LogInfof("action[autoAddDataReplica] clusterID[%v] vol[%v] partitionID[%v] auto add data replica success, newReplicaHost[%v], PersistenceHosts:[%v]",
		c.Name, dp.VolName, dp.PartitionID, newAddr, dp.Hosts)
	success = true
	return

errHandler:
	if err != nil {
		err = fmt.Errorf("clusterID[%v] vol[%v] partitionID[%v], err[%v]", c.Name, dp.VolName, dp.PartitionID, err)
		log.LogErrorf("action[autoAddDataReplica] err %v", err)
	}
	return
}

// Decommission a data partition.
// 1. Check if we can decommission a data partition. In the following cases, we are not allowed to do so:
// - (a) a replica is not in the latest host list;
// - (b) there is already a replica been taken offline;
// - (c) the remaining number of replicas is less than the majority
// 2. Choose a new data node.
// 3. synchronized decommission data partition
// 4. synchronized create a new data partition
// 5. Set the data partition as readOnly.
// 6. persistent the new host list
func (c *Cluster) migrateDataPartition(srcAddr, targetAddr string, dp *DataPartition, raftForce bool, errMsg string) (err error) {
	var (
		targetHosts     []string
		newAddr         string
		msg             string
		dataNode        *DataNode
		zone            *Zone
		replica         *DataReplica
		ns              *nodeSet
		excludeNodeSets []uint64
		zones           []string
	)
	log.LogDebugf("[migrateDataPartition] src %v target %v raftForce %v", srcAddr, targetAddr, raftForce)
	dp.RLock()
	if ok := dp.hasHost(srcAddr); !ok {
		dp.RUnlock()
		return
	}
	if dp.isSpecialReplicaCnt() {
		if dp.SingleDecommissionStatus >= datanode.DecommsionEnter {
			err = fmt.Errorf("volume [%v] dp [%v] is on decommission", dp.VolName, dp.PartitionID)
			log.LogErrorf("action[decommissionDataPartition][%v] ", err)
			dp.RUnlock()
			return
		}
		dp.SingleDecommissionStatus = datanode.DecommsionEnter
	}

	replica, _ = dp.getReplica(srcAddr)
	dp.RUnlock()

	// delete if not normal data partition
	if !proto.IsNormalDp(dp.PartitionType) {
		c.vols[dp.VolName].deleteDataPartition(c, dp)
		return
	}

	if err = c.validateDecommissionDataPartition(dp, srcAddr); err != nil {
		goto errHandler
	}

	if dataNode, err = c.dataNode(srcAddr); err != nil {
		goto errHandler
	}

	if dataNode.ZoneName == "" {
		err = fmt.Errorf("dataNode[%v] zone is nil", dataNode.Addr)
		goto errHandler
	}

	if zone, err = c.t.getZone(dataNode.ZoneName); err != nil {
		goto errHandler
	}

	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		goto errHandler
	}

	if targetAddr != "" {
		targetHosts = []string{targetAddr}
	} else if targetHosts, _, err = ns.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
		if _, ok := c.vols[dp.VolName]; !ok {
			log.LogWarnf("clusterID[%v] partitionID:%v  on node:%v offline failed,PersistenceHosts:[%v]",
				c.Name, dp.PartitionID, srcAddr, dp.Hosts)
			goto errHandler
		}
		if c.isFaultDomain(c.vols[dp.VolName]) {
			log.LogErrorf("clusterID[%v] partitionID:%v  on node:%v is banlance zone,PersistenceHosts:[%v]",
				c.Name, dp.PartitionID, srcAddr, dp.Hosts)
			goto errHandler
		}
		// select data nodes from the other node set in same zone
		excludeNodeSets = append(excludeNodeSets, ns.ID)
		if targetHosts, _, err = zone.getAvailNodeHosts(TypeDataPartition, excludeNodeSets, dp.Hosts, 1); err != nil {
			// select data nodes from the other zone
			zones = dp.getLiveZones(srcAddr)
			var excludeZone []string
			if len(zones) == 0 {
				excludeZone = append(excludeZone, zone.name)
			} else {
				excludeZone = append(excludeZone, zones[0])
			}
			if targetHosts, _, err = c.getHostFromNormalZone(TypeDataPartition, excludeZone, excludeNodeSets, dp.Hosts, 1, 1, ""); err != nil {
				goto errHandler
			}
		}
	}

	newAddr = targetHosts[0]
	err = c.updateDataNodeSize(newAddr, dp)
	if err != nil {
		log.LogErrorf("action[migrateDataPartition] target addr can't be writable, add %s %s", newAddr, err.Error())
		return
	}

	defer func() {
		if err != nil {
			c.returnDataSize(newAddr, dp)
		}
	}()

	// if special replica wait for
	if dp.ReplicaNum == 1 || (dp.ReplicaNum == 2 && (dp.ReplicaNum == c.vols[dp.VolName].dpReplicaNum) && !raftForce) {
		dp.Status = proto.ReadOnly
		dp.isRecover = true
		c.putBadDataPartitionIDs(replica, srcAddr, dp.PartitionID)

		if err = c.decommissionSingleDp(dp, newAddr, srcAddr); err != nil {
			goto errHandler
		}
	} else {
		if err = c.removeDataReplica(dp, srcAddr, false, raftForce); err != nil {
			goto errHandler
		}
		if err = c.addDataReplica(dp, newAddr); err != nil {
			goto errHandler
		}

		dp.Status = proto.ReadOnly
		dp.isRecover = true
		c.putBadDataPartitionIDs(replica, srcAddr, dp.PartitionID)
	}
	log.LogDebugf("[migrateDataPartition] src %v target %v raftForce %v", srcAddr, targetAddr, raftForce)
	dp.RLock()
	c.syncUpdateDataPartition(dp)
	dp.RUnlock()

	log.LogWarnf("clusterID[%v] partitionID:%v  on node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, dp.PartitionID, srcAddr, newAddr, dp.Hosts)
	dp.SingleDecommissionStatus = 0
	return

errHandler:
	if dp.isSpecialReplicaCnt() {
		if dp.SingleDecommissionStatus == datanode.DecommsionEnter {
			dp.SingleDecommissionStatus = 0
		}
	}
	msg = fmt.Sprintf(errMsg+" clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, srcAddr, newAddr, err, dp.Hosts)

	if err != nil {
		Warn(c.Name, msg)
		err = fmt.Errorf("vol[%v],partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		log.LogErrorf("actin[decommissionDataPartition] err %v", err)
	}

	return

}

// Decommission a data partition.
// 1. Check if we can decommission a data partition. In the following cases, we are not allowed to do so:
// - (a) a replica is not in the latest host list;
// - (b) there is already a replica been taken offline;
// - (c) the remaining number of replicas is less than the majority
// 2. Choose a new data node.
// 3. synchronized decommission data partition
// 4. synchronized create a new data partition
// 5. Set the data partition as readOnly.
// 6. persistent the new host list
func (c *Cluster) decommissionDataPartition(offlineAddr string, dp *DataPartition, raftForce bool, errMsg string) (err error) {
	return c.migrateDataPartition(offlineAddr, "", dp, raftForce, errMsg)
}

func (c *Cluster) validateDecommissionDataPartition(dp *DataPartition, offlineAddr string) (err error) {
	dp.RLock()
	defer dp.RUnlock()
	var vol *Vol
	if vol, err = c.getVol(dp.VolName); err != nil {
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}

	if err = dp.hasMissingOneReplica(offlineAddr, int(vol.dpReplicaNum)); err != nil {
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}

	// if the partition can be offline or not
	if err = dp.canBeOffLine(offlineAddr); err != nil {
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}

	if dp.isRecover && !dp.activeUsedSimilar() {
		err = fmt.Errorf("vol[%v],data partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, dp.PartitionID, offlineAddr)
		log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v err %v", dp.VolName, dp.PartitionID, err)
		return
	}
	log.LogInfof("action[validateDecommissionDataPartition] dp vol %v dp %v looks fine!", dp.VolName, dp.PartitionID)
	return
}

func (c *Cluster) addDataReplica(dp *DataPartition, addr string) (err error) {
	log.LogDebugf("[addDataReplica] addDataReplica %v", addr)
	defer func() {
		if err != nil {
			log.LogErrorf("action[addDataReplica],vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		} else {
			log.LogInfof("action[addDataReplica]  dp %v add replica dst addr %v success!", dp.PartitionID, addr)
		}
	}()

	log.LogInfof("action[addDataReplica]  dp %v try add replica dst addr %v try add raft member", dp.PartitionID, addr)

	dp.addReplicaMutex.Lock()
	defer dp.addReplicaMutex.Unlock()

	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}

	addPeer := proto.Peer{ID: dataNode.ID, Addr: addr}

	if !proto.IsNormalDp(dp.PartitionType) {
		return fmt.Errorf("[%d] is not normal dp, not support add or delete replica", dp.PartitionID)
	}

	log.LogInfof("action[addDataReplica] dp %v dst addr %v try add raft member, node id %v", dp.PartitionID, addr, dataNode.ID)
	if err = c.addDataPartitionRaftMember(dp, addPeer); err != nil {
		log.LogInfof("action[addDataReplica] dp %v addr %v try add raft member err [%v]", dp.PartitionID, addr, err)
		return
	}
	log.LogInfof("action[addDataReplica] dp %v addr %v try create data replica", dp.PartitionID, addr)
	if err = c.createDataReplica(dp, addPeer); err != nil {
		log.LogInfof("action[addDataReplica] dp %v addr %v createDataReplica err [%v]", dp.PartitionID, addr, err)
		return
	}

	return
}

// update datanode size with to replica size
func (c *Cluster) updateDataNodeSize(addr string, dp *DataPartition) error {
	leaderSize := dp.Replicas[0].Used
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return err
	}

	dataNode.Lock()
	defer dataNode.Unlock()

	if dataNode.AvailableSpace < 10*util.GB {
		return fmt.Errorf("new datanode %s is not writable %d", addr, dataNode.AvailableSpace)
	}

	dataNode.LastUpdateTime = time.Now()
	if dataNode.AvailableSpace < leaderSize {
		dataNode.AvailableSpace = 0
		return nil
	}

	dataNode.AvailableSpace -= leaderSize

	return nil
}

func (c *Cluster) returnDataSize(addr string, dp *DataPartition) {
	leaderSize := dp.Replicas[0].Used
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}

	dataNode.Lock()
	defer dataNode.Unlock()
	log.LogWarnf("returnDataSize after error, addr %s, ava %d, leader %d", addr, dataNode.AvailableSpace, leaderSize)

	dataNode.LastUpdateTime = time.Now()
	dataNode.AvailableSpace += leaderSize

}

func (c *Cluster) buildAddDataPartitionRaftMemberTaskAndSyncSendTask(dp *DataPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	log.LogInfof("action[buildAddDataPartitionRaftMemberTaskAndSyncSendTask] add peer [%v] start", addPeer)
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}
		if err != nil {
			log.LogErrorf("vol[%v],data partition[%v],resultCode[%v],err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
		} else {
			log.LogWarnf("vol[%v],data partition[%v],resultCode[%v],err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
		}
	}()
	task, err := dp.createTaskToAddRaftMember(addPeer, leaderAddr)
	if err != nil {
		return
	}
	leaderDataNode, err := c.dataNode(leaderAddr)
	if err != nil {
		return
	}
	if resp, err = leaderDataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	log.LogInfof("action[buildAddDataPartitionRaftMemberTaskAndSyncSendTask] add peer [%v] finished", addPeer)
	return
}

func (c *Cluster) addDataPartitionRaftMember(dp *DataPartition, addPeer proto.Peer) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)

	if leaderAddr, candidateAddrs, err = dp.prepareAddRaftMember(addPeer); err != nil {
		//maybe add success during last decommission
		if dp.DecommissionRetry > 0 {
			err = nil
			return
		}
		return
	}

	//send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		if leaderAddr == "" && len(candidateAddrs) < int(dp.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		_, err = c.buildAddDataPartitionRaftMemberTaskAndSyncSendTask(dp, addPeer, host)
		if err == nil {
			break
		}
		if index < len(candidateAddrs)-1 {
			time.Sleep(retrySendSyncTaskInternal)
		}
	}

	if err != nil {
		return
	}
	dp.Lock()
	defer dp.Unlock()
	newHosts := make([]string, 0, len(dp.Hosts)+1)
	newPeers := make([]proto.Peer, 0, len(dp.Peers)+1)
	newHosts = append(dp.Hosts, addPeer.Addr)
	newPeers = append(dp.Peers, addPeer)

	log.LogInfof("action[addDataPartitionRaftMember] try host [%v] to [%v] peers [%v] to [%v]",
		dp.Hosts, newHosts, dp.Peers, newPeers)
	if err = dp.update("addDataPartitionRaftMember", dp.VolName, newPeers, newHosts, c); err != nil {
		return
	}
	return
}

func (c *Cluster) createDataReplica(dp *DataPartition, addPeer proto.Peer) (err error) {
	vol, err := c.getVol(dp.VolName)
	if err != nil {
		return
	}

	dp.RLock()
	hosts := make([]string, len(dp.Hosts))
	copy(hosts, dp.Hosts)
	peers := make([]proto.Peer, len(dp.Peers))
	copy(peers, dp.Peers)
	dp.RUnlock()

	diskPath, err := c.syncCreateDataPartitionToDataNode(addPeer.Addr, vol.dataPartitionSize,
		dp, peers, hosts, proto.DecommissionedCreateDataPartition, dp.PartitionType)
	if err != nil {
		return
	}

	dp.Lock()
	defer dp.Unlock()

	if err = dp.afterCreation(addPeer.Addr, diskPath, c); err != nil {
		return
	}

	if err = dp.update("createDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		return
	}

	return
}

func (c *Cluster) removeDataReplica(dp *DataPartition, addr string, validate bool, raftForceDel bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[removeDataReplica],vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		}
	}()
	log.LogInfof("action[removeDataReplica]  dp %v try remove replica  addr [%v]", dp.PartitionID, addr)
	// validate be set true only in api call
	if validate && !raftForceDel {
		if err = c.validateDecommissionDataPartition(dp, addr); err != nil {
			return
		}
	}

	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}

	if !proto.IsNormalDp(dp.PartitionType) {
		return fmt.Errorf("[%d] is not normal dp, not support add or delete replica", dp.PartitionID)
	}
	removePeer := proto.Peer{ID: dataNode.ID, Addr: addr}
	if err = c.removeDataPartitionRaftMember(dp, removePeer, raftForceDel); err != nil {
		return
	}

	if err = c.removeHostMember(dp, removePeer); err != nil {
		return
	}

	if err = c.deleteDataReplica(dp, dataNode); err != nil {
		return
	}

	leaderAddr := dp.getLeaderAddrWithLock()
	if leaderAddr != addr {
		return
	}

	if dataNode, err = c.dataNode(dp.Hosts[0]); err != nil {
		return
	}

	if err = dp.tryToChangeLeader(c, dataNode); err != nil {
		return
	}

	return
}

func (c *Cluster) isRecovering(dp *DataPartition, addr string) (isRecover bool) {
	var key string
	dp.RLock()
	defer dp.RUnlock()
	replica, _ := dp.getReplica(addr)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}

	c.badPartitionMutex.RLock()
	defer c.badPartitionMutex.RUnlock()

	var badPartitionIDs []uint64
	badPartitions, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		badPartitionIDs = badPartitions.([]uint64)
	}
	for _, id := range badPartitionIDs {
		if id == dp.PartitionID {
			isRecover = true
		}
	}
	return
}

func (c *Cluster) removeHostMember(dp *DataPartition, removePeer proto.Peer) (err error) {
	newHosts := make([]string, 0, len(dp.Hosts)-1)
	for _, host := range dp.Hosts {
		if host == removePeer.Addr {
			continue
		}
		newHosts = append(newHosts, host)
	}
	newPeers := make([]proto.Peer, 0, len(dp.Peers)-1)
	for _, peer := range dp.Peers {
		if peer.ID == removePeer.ID && peer.Addr == removePeer.Addr {
			continue
		}
		newPeers = append(newPeers, peer)
	}

	dp.Lock()
	defer dp.Unlock()
	if err = dp.update("removeDataPartitionRaftMember", dp.VolName, newPeers, newHosts, c); err != nil {
		return
	}
	return
}

func (c *Cluster) removeDataPartitionRaftMember(dp *DataPartition, removePeer proto.Peer, force bool) (err error) {
	dp.offlineMutex.Lock()
	defer dp.offlineMutex.Unlock()
	defer func() {
		if err1 := c.updateDataPartitionOfflinePeerIDWithLock(dp, 0); err1 != nil {
			err = errors.Trace(err, "updateDataPartitionOfflinePeerIDWithLock failed, err[%v]", err1)
		}
	}()
	if err = c.updateDataPartitionOfflinePeerIDWithLock(dp, removePeer.ID); err != nil {
		log.LogErrorf("action[removeDataPartitionRaftMember] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}
	return dp.createTaskToRemoveRaftMember(c, removePeer, force)
}

// call from remove raft member
func (c *Cluster) updateDataPartitionOfflinePeerIDWithLock(dp *DataPartition, peerID uint64) (err error) {
	dp.Lock()
	defer dp.Unlock()
	dp.OfflinePeerID = peerID
	if err = dp.update("updateDataPartitionOfflinePeerIDWithLock", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		return
	}
	return
}
func (c *Cluster) deleteDataReplica(dp *DataPartition, dataNode *DataNode) (err error) {

	dp.Lock()
	// in case dataNode is unreachable,update meta first.
	dp.removeReplicaByAddr(dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)

	if err = dp.update("deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		dp.Unlock()
		return
	}

	task := dp.createTaskToDeleteDataPartition(dataNode.Addr)
	dp.Unlock()

	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
	}

	return nil
}

func (c *Cluster) putBadMetaPartitions(addr string, partitionID uint64) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	newBadPartitionIDs := make([]uint64, 0)
	badPartitionIDs, ok := c.BadMetaPartitionIds.Load(addr)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadMetaPartitionIds.Store(addr, newBadPartitionIDs)
}

func (c *Cluster) getBadMetaPartitionsView() (bmpvs []badPartitionView) {
	c.badPartitionMutex.RLock()
	defer c.badPartitionMutex.RUnlock()

	bmpvs = make([]badPartitionView, 0)
	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badPartitionIds}
		bmpvs = append(bmpvs, bpv)
		return true
	})
	return
}

func (c *Cluster) putBadDataPartitionIDs(replica *DataReplica, addr string, partitionID uint64) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	var key string
	newBadPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
}

func (c *Cluster) putBadDataPartitionIDsByDiskPath(disk, addr string, partitionID uint64) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	var key string
	newBadPartitionIDs := make([]uint64, 0)
	key = fmt.Sprintf("%s:%s", addr, disk)

	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	if in(partitionID, newBadPartitionIDs) {
		return
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
}

func in(target uint64, strArray []uint64) bool {
	for _, element := range strArray {
		if target == element {
			return true
		}
	}
	return false
}
func (c *Cluster) getBadDataPartitionsView() (bpvs []badPartitionView) {
	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	bpvs = make([]badPartitionView, 0)
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badDataPartitionIds}
		bpvs = append(bpvs, bpv)
		return true
	})
	return
}

func (c *Cluster) migrateMetaNode(srcAddr, targetAddr string, limit int) (err error) {
	var toBeOfflineMps []*MetaPartition

	msg := fmt.Sprintf("action[migrateMetaNode],clusterID[%v] migrate from node[%v] to [%s] begin", c.Name, srcAddr, targetAddr)
	log.LogWarn(msg)

	metaNode, err := c.metaNode(srcAddr)
	if err != nil {
		return err
	}

	metaNode.MigrateLock.Lock()
	defer metaNode.MigrateLock.Unlock()

	partitions := c.getAllMetaPartitionByMetaNode(srcAddr)
	if targetAddr != "" {
		toBeOfflineMps = make([]*MetaPartition, 0)
		for _, mp := range partitions {
			if contains(mp.Hosts, targetAddr) {
				continue
			}

			toBeOfflineMps = append(toBeOfflineMps, mp)
		}
	} else {
		toBeOfflineMps = partitions
	}

	if len(toBeOfflineMps) <= 0 && len(partitions) != 0 {
		return fmt.Errorf("migrateMataNode no partition can migrate from [%s] to [%s] limit [%v]", srcAddr, targetAddr, limit)
	}

	if limit <= 0 && targetAddr == "" { // default all mps
		limit = len(toBeOfflineMps)
	} else if limit <= 0 {
		limit = defaultMigrateMpCnt
	}

	if limit > len(toBeOfflineMps) {
		limit = len(toBeOfflineMps)
	}

	var wg sync.WaitGroup
	metaNode.ToBeOffline = true
	metaNode.MaxMemAvailWeight = 1
	errChannel := make(chan error, limit)

	defer func() {
		metaNode.ToBeOffline = false
		close(errChannel)
	}()

	for idx := 0; idx < limit; idx++ {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			if err1 := c.migrateMetaPartition(srcAddr, targetAddr, mp); err1 != nil {
				errChannel <- err1
			}
		}(toBeOfflineMps[idx])
	}

	wg.Wait()
	select {
	case err = <-errChannel:
		log.LogErrorf("action[migrateMetaNode] clusterID[%v] migrate node[%s] to [%s] faild, err(%s)",
			c.Name, srcAddr, targetAddr, err.Error())
		return
	default:
	}

	if limit < len(partitions) {
		log.LogWarnf("action[migrateMetaNode] clusterID[%v] migrate from [%s] to [%s] cnt[%d] success",
			c.Name, srcAddr, targetAddr, limit)
		return
	}

	if err = c.syncDeleteMetaNode(metaNode); err != nil {
		msg = fmt.Sprintf("action[migrateMetaNode], clusterID[%v] node[%v] synDelMetaNode failed,err[%s]",
			c.Name, srcAddr, err.Error())
		Warn(c.Name, msg)
		return
	}

	c.deleteMetaNodeFromCache(metaNode)
	msg = fmt.Sprintf("action[migrateMetaNode],clusterID[%v] migrate from node[%v] to node(%s) success", c.Name, srcAddr, targetAddr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) decommissionMetaNode(metaNode *MetaNode) (err error) {
	return c.migrateMetaNode(metaNode.Addr, "", 0)
}

func (c *Cluster) deleteMetaNodeFromCache(metaNode *MetaNode) {
	c.metaNodes.Delete(metaNode.Addr)
	c.t.deleteMetaNode(metaNode)
	go metaNode.clean()
}

func (c *Cluster) updateVol(name, authKey string, newArgs *VolVarargs) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
		volUsedSpace  uint64
		oldArgs       *VolVarargs
	)

	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[updateVol] err[%v]", err)
		err = proto.ErrVolNotExists
		goto errHandler
	}

	if vol.status() == markDelete {
		log.LogErrorf("action[updateVol] vol is already deleted, name(%s)", name)
		err = proto.ErrVolNotExists
		goto errHandler
	}

	vol.volLock.Lock()
	defer vol.volLock.Unlock()

	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}

	volUsedSpace = vol.totalUsedSpace()
	if float64(newArgs.capacity*util.GB) < float64(volUsedSpace)*1.01 && newArgs.capacity != vol.Capacity {
		err = fmt.Errorf("capacity[%v] has to be 1 percent larger than the used space[%v]", newArgs.capacity,
			volUsedSpace/util.GB)
		goto errHandler
	}

	log.LogInfof("[checkZoneName] name [%s], zone [%s]", name, newArgs.zoneName)
	if newArgs.zoneName, err = c.checkZoneName(name, vol.crossZone, vol.defaultPriority, newArgs.zoneName, vol.domainId); err != nil {
		goto errHandler
	}

	if newArgs.coldArgs.cacheCap >= newArgs.capacity {
		err = fmt.Errorf("capacity must be large than cache capacity, newCap(%d), newCacheCap(%d)", newArgs.capacity, newArgs.coldArgs.cacheCap)
		goto errHandler
	}

	oldArgs = getVolVarargs(vol)
	setVolFromArgs(newArgs, vol)
	if err = c.syncUpdateVol(vol); err != nil {
		setVolFromArgs(oldArgs, vol)
		log.LogErrorf("action[updateVol] vol[%v] err[%v]", name, err)
		err = proto.ErrPersistenceByRaft
		goto errHandler
	}

	return

errHandler:
	err = fmt.Errorf("action[updateVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) checkNormalZoneName(zoneName string) (err error) {
	var zones []string
	if c.needFaultDomain {
		zones = c.t.domainExcludeZones
	} else {
		zones = c.t.getZoneNameList()
	}

	zoneList := strings.Split(zoneName, ",")
	for i := 0; i < len(zoneList); i++ {
		var isZone bool
		for j := 0; j < len(zones); j++ {
			if zoneList[i] == zones[j] {
				isZone = true
				break
			}
		}

		if !isZone {
			return fmt.Errorf("action[checkZoneName] the zonename[%s] not found", zoneList[i])
		}
	}
	return
}

func (c *Cluster) checkZoneName(name string,
	crossZone bool,
	defaultPriority bool,
	zoneName string,
	domainId uint64) (newZoneName string, err error) {

	zoneList := strings.Split(zoneName, ",")
	newZoneName = zoneName

	if crossZone {
		if newZoneName != "" {
			if len(zoneList) == 1 {
				return newZoneName, fmt.Errorf("action[checkZoneName] vol use specified single zoneName conflit with cross zone flag")
			} else {
				if err = c.checkNormalZoneName(newZoneName); err != nil {
					return newZoneName, err
				}
			}
		}
		if c.FaultDomain {
			if newZoneName != "" {
				if !defaultPriority || domainId > 0 {
					return newZoneName, fmt.Errorf("action[checkZoneName] vol need FaultDomain but set zone name")
				}
			} else {
				if domainId > 0 {
					if _, ok := c.domainManager.domainId2IndexMap[domainId]; !ok {
						return newZoneName, fmt.Errorf("action[checkZoneName] cluster can't find oomainId [%v]", domainId)
					}
				}
			}
		} else {
			if c.t.zoneLen() <= 1 {
				return newZoneName, fmt.Errorf("action[checkZoneName] cluster has one zone,can't cross zone")
			}
		}
	} else { // cross zone disable means not use domain at the time vol be created
		if newZoneName == "" {
			if !c.needFaultDomain {
				if _, err = c.t.getZone(DefaultZoneName); err != nil {
					return newZoneName, fmt.Errorf("action[checkZoneName] the vol is not cross zone and didn't set zone name,but there's no default zone")
				}
				log.LogInfof("action[checkZoneName] vol [%v] use default zone", name)
				newZoneName = DefaultZoneName
			}
		} else {
			if len(zoneList) > 1 {
				return newZoneName, fmt.Errorf("action[checkZoneName] vol specified zoneName need cross zone")
			}

			if err = c.checkNormalZoneName(newZoneName); err != nil {
				return newZoneName, err
			}
		}
	}
	return
}

// Create a new volume.
// By default we create 3 meta partitions and 10 data partitions during initialization.
func (c *Cluster) createVol(req *createVolReq) (vol *Vol, err error) {
	if c.DisableAutoAllocate {
		log.LogWarn("the cluster is frozen")
		return nil, fmt.Errorf("the cluster is frozen, can not create volume")
	}

	var (
		readWriteDataPartitions int
	)

	if req.zoneName, err = c.checkZoneName(req.name, req.crossZone, req.normalZonesFirst, req.zoneName, req.domainId); err != nil {
		return
	}

	if vol, err = c.doCreateVol(req); err != nil {
		goto errHandler
	}

	if err = vol.initMetaPartitions(c, req.mpCount); err != nil {

		vol.Status = markDelete
		if e := vol.deleteVolFromStore(c); e != nil {
			log.LogErrorf("action[createVol] failed,vol[%v] err[%v]", vol.Name, e)
		}

		c.deleteVol(req.name)

		err = fmt.Errorf("action[createVol] initMetaPartitions failed,err[%v]", err)
		goto errHandler
	}

	if vol.CacheCapacity > 0 || (proto.IsHot(vol.VolType) && vol.Capacity > 0) {
		for retryCount := 0; readWriteDataPartitions < defaultInitMetaPartitionCount && retryCount < 3; retryCount++ {
			err = vol.initDataPartitions(c)
			if err != nil {
				log.LogError("init dataPartition error", err.Error(), retryCount, len(vol.dataPartitions.partitionMap))
			}

			readWriteDataPartitions = len(vol.dataPartitions.partitionMap)
		}

		if len(vol.dataPartitions.partitionMap) < defaultInitMetaPartitionCount {
			err = fmt.Errorf("action[createVol]  initDataPartitions failed, less than %d", defaultInitMetaPartitionCount)
			goto errHandler
		}
	}

	vol.dataPartitions.readableAndWritableCnt = readWriteDataPartitions
	vol.updateViewCache(c)
	log.LogInfof("action[createVol] vol[%v],readableAndWritableCnt[%v]", req.name, readWriteDataPartitions)
	return

errHandler:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, req.name, err)
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) doCreateVol(req *createVolReq) (vol *Vol, err error) {
	c.createVolMutex.Lock()
	defer c.createVolMutex.Unlock()

	var createTime = time.Now().Unix() // record unix seconds of volume create time
	var dataPartitionSize uint64

	if req.size*util.GB == 0 {
		dataPartitionSize = util.DefaultDataPartitionSize
	} else {
		dataPartitionSize = uint64(req.size) * util.GB
	}

	vv := volValue{
		Name:              req.name,
		Owner:             req.owner,
		ZoneName:          req.zoneName,
		DataPartitionSize: dataPartitionSize,
		Capacity:          uint64(req.capacity),
		DpReplicaNum:      uint8(req.dpReplicaNum),
		ReplicaNum:        defaultReplicaNum,
		FollowerRead:      req.followerRead,
		Authenticate:      req.authenticate,
		CrossZone:         req.crossZone,
		DefaultPriority:   req.normalZonesFirst,
		DomainId:          req.domainId,
		CreateTime:        createTime,
		Description:       req.description,
		EnablePosixAcl:    req.enablePosixAcl,

		VolType:          req.volType,
		EbsBlkSize:       req.coldArgs.objBlockSize,
		CacheCapacity:    req.coldArgs.cacheCap,
		CacheAction:      req.coldArgs.cacheAction,
		CacheThreshold:   req.coldArgs.cacheThreshold,
		CacheTTL:         req.coldArgs.cacheTtl,
		CacheHighWater:   req.coldArgs.cacheHighWater,
		CacheLowWater:    req.coldArgs.cacheLowWater,
		CacheLRUInterval: req.coldArgs.cacheLRUInterval,
		CacheRule:        req.coldArgs.cacheRule,

		VolQosEnable: req.qosLimitArgs.qosEnable,
		IopsRLimit:   req.qosLimitArgs.iopsRVal,
		IopsWLimit:   req.qosLimitArgs.iopsWVal,
		FlowRlimit:   req.qosLimitArgs.flowRVal,
		FlowWlimit:   req.qosLimitArgs.flowWVal,

		DpReadOnlyWhenVolFull: req.DpReadOnlyWhenVolFull,
	}

	log.LogInfof("[doCreateVol] volView, %v", vv)

	if _, err = c.getVol(req.name); err == nil {
		err = proto.ErrDuplicateVol
		goto errHandler
	}

	vv.ID, err = c.idAlloc.allocateCommonID()
	if err != nil {
		goto errHandler
	}

	vol = newVol(vv)
	log.LogInfof("[doCreateVol] vol, %v", vol)

	// refresh oss secure
	vol.refreshOSSSecure()

	if err = c.syncAddVol(vol); err != nil {
		goto errHandler
	}

	c.putVol(vol)

	return

errHandler:
	err = fmt.Errorf("action[doCreateVol], clusterID[%v] name:%v, err:%v ", c.Name, req.name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

// Update the upper bound of the inode ids in a meta partition.
func (c *Cluster) updateInodeIDRange(volName string, start uint64) (err error) {

	var (
		maxPartitionID uint64
		vol            *Vol
		partition      *MetaPartition
	)

	if vol, err = c.getVol(volName); err != nil {
		log.LogErrorf("action[updateInodeIDRange]  vol [%v] not found", volName)
		return proto.ErrVolNotExists
	}

	maxPartitionID = vol.maxPartitionID()
	if partition, err = vol.metaPartition(maxPartitionID); err != nil {
		log.LogErrorf("action[updateInodeIDRange]  mp[%v] not found", maxPartitionID)
		return proto.ErrMetaPartitionNotExists
	}

	adjustStart := start
	if adjustStart < partition.Start {
		adjustStart = partition.Start
	}

	if adjustStart < partition.MaxInodeID {
		adjustStart = partition.MaxInodeID
	}

	adjustStart = adjustStart + defaultMetaPartitionInodeIDStep
	log.LogWarnf("vol[%v],maxMp[%v],start[%v],adjustStart[%v]", volName, maxPartitionID, start, adjustStart)
	if err = vol.splitMetaPartition(c, partition, adjustStart); err != nil {
		log.LogErrorf("action[updateInodeIDRange]  mp[%v] err[%v]", partition.PartitionID, err)
	}
	return
}

func (c *Cluster) dataNodeCount() (len int) {
	c.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) metaNodeCount() (len int) {
	c.metaNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) allDataNodes() (dataNodes []proto.NodeView) {
	dataNodes = make([]proto.NodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodes = append(dataNodes, proto.NodeView{Addr: dataNode.Addr, DomainAddr: dataNode.DomainAddr,
			Status: dataNode.isActive, ID: dataNode.ID, IsWritable: dataNode.isWriteAble()})
		return true
	})
	return
}

func (c *Cluster) allMetaNodes() (metaNodes []proto.NodeView) {
	metaNodes = make([]proto.NodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, proto.NodeView{ID: metaNode.ID, Addr: metaNode.Addr, DomainAddr: metaNode.DomainAddr,
			Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
		return true
	})
	return
}

func (c *Cluster) allVolNames() (vols []string) {
	vols = make([]string, 0)
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for name := range c.vols {
		vols = append(vols, name)
	}
	return
}

func (c *Cluster) copyVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol, 0)
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()

	for name, vol := range c.vols {
		vols[name] = vol
	}

	return
}

// Return all the volumes except the ones that have been marked to be deleted.
func (c *Cluster) allVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol, 0)
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for name, vol := range c.vols {
		if vol.Status == normal {
			vols[name] = vol
		}
	}
	return
}

func (c *Cluster) getDataPartitionCount() (count int) {
	c.volMutex.RLock()
	defer c.volMutex.RUnlock()
	for _, vol := range c.vols {
		count = count + len(vol.dataPartitions.partitions)
	}
	return
}

func (c *Cluster) getMetaPartitionCount() (count int) {
	vols := c.copyVols()
	for _, vol := range vols {
		vol.mpsLock.RLock()
		count = count + len(vol.MetaPartitions)
		vol.mpsLock.RUnlock()
	}
	return count
}

func (c *Cluster) setClusterInfo(quota uint32) (err error) {
	oldLimit := c.cfg.DirChildrenNumLimit
	atomic.StoreUint32(&c.cfg.DirChildrenNumLimit, quota)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterInfo] err[%v]", err)
		atomic.StoreUint32(&c.cfg.DirChildrenNumLimit, oldLimit)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMetaNodeThreshold(threshold float32) (err error) {
	oldThreshold := c.cfg.MetaNodeThreshold
	c.cfg.MetaNodeThreshold = threshold
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeThreshold] err[%v]", err)
		c.cfg.MetaNodeThreshold = oldThreshold
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMetaNodeDeleteBatchCount(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.MetaNodeDeleteBatchCount)
	atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeDeleteBatchCount] err[%v]", err)
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClusterLoadFactor(factor float32) (err error) {
	oldVal := c.cfg.ClusterLoadFactor
	c.cfg.ClusterLoadFactor = factor
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterLoadFactorErr] err[%v]", err)
		c.cfg.ClusterLoadFactor = oldVal
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeDeleteLimitRate(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DataNodeDeleteLimitRate)
	atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeDeleteLimitRate] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeAutoRepairLimitRate(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.DataNodeAutoRepairLimitRate)
	atomic.StoreUint64(&c.cfg.DataNodeAutoRepairLimitRate, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeAutoRepairLimitRate] err[%v]", err)
		atomic.StoreUint64(&c.cfg.DataNodeAutoRepairLimitRate, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMetaNodeDeleteWorkerSleepMs(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs)
	atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeDeleteWorkerSleepMs] err[%v]", err)
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMaxDpCntLimit(val uint64) (err error) {
	oldVal := atomic.LoadUint64(&c.cfg.MaxDpCntLimit)
	atomic.StoreUint64(&c.cfg.MaxDpCntLimit, val)
	maxDpCntOneNode = uint32(val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[MaxDpCntLimit] err[%v]", err)
		atomic.StoreUint64(&c.cfg.MaxDpCntLimit, oldVal)
		maxDpCntOneNode = uint32(oldVal)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClusterCreateTime(createTime int64) (err error) {
	oldVal := c.CreateTime
	c.CreateTime = createTime
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterCreateTime] err[%v]", err)
		c.CreateTime = oldVal
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDisableAutoAllocate(disableAutoAllocate bool) (err error) {
	oldFlag := c.DisableAutoAllocate
	c.DisableAutoAllocate = disableAutoAllocate
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDisableAutoAllocate] err[%v]", err)
		c.DisableAutoAllocate = oldFlag
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) clearVols() {
	c.volMutex.Lock()
	defer c.volMutex.Unlock()
	c.vols = make(map[string]*Vol, 0)
}

func (c *Cluster) clearTopology() {
	c.t.clear()
}

func (c *Cluster) clearDataNodes() {
	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		c.dataNodes.Delete(key)
		dataNode.clean()
		return true
	})
}

func (c *Cluster) clearMetaNodes() {
	c.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		c.metaNodes.Delete(key)
		metaNode.clean()
		return true
	})
}

func (c *Cluster) scheduleToCheckDecommissionDataNode() {
	go func() {
		for {
			if c.partition.IsRaftLeader() {
				c.checkDecommissionDataNode()
			}
			time.Sleep(10 * time.Second)
		}
	}()
}

func (c *Cluster) checkDecommissionDataNode() {
	//decommission datanode mark
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNode.updateDecommissionStatus(c, false)
		if dataNode.GetDecommissionStatus() == markDecommission {
			c.TryDecommissionDataNode(dataNode)
		} else if dataNode.GetDecommissionStatus() == DecommissionSuccess {
			partitions := c.getAllDataPartitionByDataNode(dataNode.Addr)
			//if only decommission part of data partitions, do not remove the datanode
			if len(partitions) != 0 {
				if time.Now().Sub(time.Unix(dataNode.DecommissionCompleteTime, 0)) > (20 * time.Minute) {
					dataNode.resetDecommissionStatus()
				}
				return true
			}
			if err := c.syncDeleteDataNode(dataNode); err != nil {
				msg := fmt.Sprintf("action[checkDecommissionDataNode],clusterID[%v] Node[%v] syncDeleteDataNode failed,err[%v]",
					c.Name, dataNode.Addr, err)
				log.LogWarnf("%s", msg)
			} else {
				log.LogWarnf("action[checkDecommissionDataNode] del dataNode %v", dataNode.Addr)
				c.delDataNodeFromCache(dataNode)
			}
		}
		return true
	})
}

func (c *Cluster) TryDecommissionDataNode(dataNode *DataNode) {
	var (
		toBeOffLinePartitions   []*DataPartition
		toBeOffLinePartitionIds []uint64
		err                     error
		zone                    *Zone
		ns                      *nodeSet
	)
	log.LogDebugf("action[TryDecommissionDataNode] dataNode [%s]", dataNode.Addr)
	dataNode.MigrateLock.Lock()
	defer func() {
		dataNode.MigrateLock.Unlock()
		if err != nil {
			dataNode.DecommissionRetry++
			log.LogDebugf("action[TryDecommissionDataNode] dataNode [%s] retry %v", dataNode.Addr, dataNode.DecommissionRetry)
		}
		c.syncUpdateDataNode(dataNode)
	}()
	//may allocate new dp when dataNode cancel decommission before
	partitions := c.getAllDataPartitionByDataNode(dataNode.Addr)
	if dataNode.DecommissionDstAddr != "" {
		for _, dp := range partitions {
			// two replica can't exist on same node
			if dp.hasHost(dataNode.DecommissionDstAddr) {
				continue
			}
			toBeOffLinePartitions = append(toBeOffLinePartitions, dp)
		}
	} else {
		toBeOffLinePartitions = partitions
	}

	if len(toBeOffLinePartitions) <= 0 && len(partitions) != 0 {
		err = fmt.Errorf("DecommissionDataNode no partition can migrate from [%s] to [%s] for replica address conflict",
			dataNode.Addr, dataNode.DecommissionDstAddr)
		log.LogWarnf("action[TryDecommissionDataNode] %v", err.Error())
		return
	}

	//check decommission dp last time
	oldPartitions := c.getAllDecommissionDataPartitionByDataNode(dataNode.Addr)

	if len(oldPartitions) != 0 {
		toBeOffLinePartitions = mergeDataPartitionArr(toBeOffLinePartitions, oldPartitions)
	}

	if zone, err = c.t.getZone(dataNode.ZoneName); err != nil {
		log.LogWarnf("action[TryDecommissionDataNode] find dataNode[%s] zone failed[%v]",
			dataNode.Addr, err.Error())
		return
	}
	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		log.LogWarnf("action[TryDecommissionDataNode] find dataNode[%s] nodeset[%v] failed[%v]",
			dataNode.Addr, dataNode.NodeSetID, err.Error())
		return
	}

	if dataNode.DecommissionLimit == 0 || dataNode.DecommissionLimit > len(toBeOffLinePartitions) {
		dataNode.DecommissionDpTotal = len(toBeOffLinePartitions)
	} else {
		dataNode.DecommissionDpTotal = dataNode.DecommissionLimit
		toBeOffLinePartitions = toBeOffLinePartitions[:dataNode.DecommissionLimit]
	}
	if dataNode.DecommissionDpTotal == 0 {
		dataNode.markDecommissionSuccess()
		return
	}
	//put all dp to nodeset's decommission list
	for _, dp := range toBeOffLinePartitions {
		dp.MarkDecommissionStatus(dataNode.Addr, dataNode.DecommissionDstAddr, "",
			dataNode.DecommissionRaftForce, dataNode.DecommissionTerm)
		c.syncUpdateDataPartition(dp)
		ns.AddToDecommissionDataPartitionList(dp)
		toBeOffLinePartitionIds = append(toBeOffLinePartitionIds, dp.PartitionID)
	}
	dataNode.SetDecommissionStatus(DecommissionRunning)
	//avoid alloc dp on this node
	dataNode.ToBeOffline = true
	log.LogInfof("action[TryDecommissionDataNode] mark dataNode[%s] dp %v markDecommission,"+
		"ToBeOffline %v DecommissionDpTotal %v raftForce %v term %v to dst %v",
		dataNode.Addr, toBeOffLinePartitionIds, dataNode.ToBeOffline, dataNode.DecommissionDpTotal,
		dataNode.DecommissionRaftForce, dataNode.DecommissionTerm, dataNode.DecommissionDstAddr)
}

func (c *Cluster) migrateDisk(nodeAddr, diskPath string, raftForce bool, limit int, diskDisable bool) (err error) {
	var disk *DecommissionDisk
	key := fmt.Sprintf("%s_%s", nodeAddr, diskPath)

	if value, ok := c.DecommissionDisks.Load(key); ok {
		disk = value.(*DecommissionDisk)
		if disk.GetDecommissionStatus() == markDecommission || disk.GetDecommissionStatus() == DecommissionRunning {
			err = fmt.Errorf("migrate src(%v) diskPath(%v)s still on working, please wait,check or cancel if abnormal",
				nodeAddr, diskPath)
			log.LogWarnf("action[migrateDisk] %v", err)
			return
		}
	} else {
		disk = &DecommissionDisk{
			SrcAddr:     nodeAddr,
			DiskPath:    diskPath,
			DiskDisable: diskDisable,
		}
		c.DecommissionDisks.Store(disk.GenerateKey(), disk)
	}
	//disk should be decommission all the dp
	disk.markDecommission(raftForce, limit)
	if err = c.syncAddDecommissionDisk(disk); err != nil {
		err = fmt.Errorf("action[addDecommissionDisk],clusterID[%v] dataNodeAddr:%v diskPath:%v err:%v ",
			c.Name, nodeAddr, diskPath, err.Error())
		Warn(c.Name, err.Error())
		return
	}
	log.LogInfof("action[addDecommissionDisk],clusterID[%v] dataNodeAddr:%v,diskPath[%v] err:%v",
		c.Name, nodeAddr, diskPath, err)
	return
}

func (c *Cluster) scheduleToCheckDecommissionDisk() {
	go func() {
		for {
			if c.partition.IsRaftLeader() {
				c.checkDecommissionDisk()
			}
			time.Sleep(10 * time.Second)
		}
	}()
}

func (c *Cluster) checkDecommissionDisk() {
	//decommission disk mark
	c.DecommissionDisks.Range(func(key, value interface{}) bool {
		disk := value.(*DecommissionDisk)
		disk.updateDecommissionStatus(c, false)
		if disk.GetDecommissionStatus() == markDecommission {
			c.TryDecommissionDisk(disk)
		} else if disk.GetDecommissionStatus() == DecommissionSuccess {
			if err := c.syncDeleteDecommissionDisk(disk); err != nil {
				msg := fmt.Sprintf("action[checkDecommissionDisk],clusterID[%v] node[%v] disk[%v],"+
					"syncDeleteDecommissionDisk failed,err[%v]",
					c.Name, disk.SrcAddr, disk.DiskPath, err)
				log.LogWarnf("%s", msg)
			} else {
				c.delDecommissionDiskFromCache(disk)
				if node, err := c.dataNode(disk.SrcAddr); err != nil {
					log.LogWarnf("action[checkDecommissionDisk] cannot find dataNode[%s]", disk.SrcAddr)
				} else {
					if disk.DiskDisable {
						c.deleteAndSyncDecommissionedDisk(node, disk.DiskPath)
					}
				}
			}
		}
		return true
	})
}

func (c *Cluster) TryDecommissionDisk(disk *DecommissionDisk) {
	var (
		node            *DataNode
		err             error
		badPartitionIds []uint64
		badPartitions   []*DataPartition
		rstMsg          string
		zone            *Zone
		ns              *nodeSet
	)
	defer func() {
		if err != nil {
			disk.DecommissionRetry++
		}
		c.syncUpdateDecommissionDisk(disk)
	}()
	if node, err = c.dataNode(disk.SrcAddr); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] cannot find dataNode[%s]", disk.SrcAddr)
		disk.SetDecommissionStatus(DecommissionFail)
		return
	}
	badPartitions = node.badPartitions(disk.DiskPath, c)
	if len(badPartitions) == 0 {
		log.LogInfof("action[TryDecommissionDisk] receive decommissionDisk node[%v] "+
			"no any partitions on disk[%v],offline successfully",
			node.Addr, disk.DiskPath)
		disk.SetDecommissionStatus(DecommissionSuccess)
		disk.DecommissionDpTotal = 0
		if disk.DiskDisable {
			c.addAndSyncDecommissionedDisk(node, disk.DiskPath)
		}
		return
	}
	//check decommission dp last time
	lastBadPartitions := c.getAllDecommissionDataPartitionByDisk(disk.SrcAddr, disk.DiskPath)
	badPartitions = mergeDataPartitionArr(badPartitions, lastBadPartitions)
	if disk.DecommissionLimit == 0 || disk.DecommissionLimit > len(badPartitions) {
		disk.DecommissionDpTotal = len(badPartitions)
	} else {
		disk.DecommissionDpTotal = disk.DecommissionLimit
		badPartitions = badPartitions[:disk.DecommissionLimit]
	}

	if zone, err = c.t.getZone(node.ZoneName); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] find datanode[%s] zone failed[%v]",
			node.Addr, err.Error())
		disk.SetDecommissionStatus(DecommissionFail)
		return
	}
	if ns, err = zone.getNodeSet(node.NodeSetID); err != nil {
		log.LogWarnf("action[TryDecommissionDisk] find datanode[%s] nodeset[%v] failed[%v]",
			node.Addr, node.NodeSetID, err.Error())
		disk.SetDecommissionStatus(DecommissionFail)
		return
	}

	for _, dp := range badPartitions {
		dp.MarkDecommissionStatus(node.Addr, "", disk.DiskPath, disk.DecommissionRaftForce, disk.DecommissionTerm)
		c.syncUpdateDataPartition(dp)
		ns.AddToDecommissionDataPartitionList(dp)
		badPartitionIds = append(badPartitionIds, dp.PartitionID)
	}
	disk.SetDecommissionStatus(DecommissionRunning)
	if disk.DiskDisable {
		c.addAndSyncDecommissionedDisk(node, disk.DiskPath)
	}
	rstMsg = fmt.Sprintf("receive decommissionDisk node[%v] disk[%v],badPartitionIds %v,raftForce %v"+
		"DecommissionDpTotal %v term %v has offline successfully",
		node.Addr, disk.DiskPath, badPartitionIds, disk.DecommissionRaftForce, disk.DecommissionDpTotal, disk.DecommissionTerm)
	log.LogInfof("action[TryDecommissionDisk] %s", rstMsg)
}

func (c *Cluster) getAllDecommissionDataPartitionByDataNodeAndTerm(addr string, term uint64) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr && dp.DecommissionTerm == term {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func (c *Cluster) getAllDecommissionDataPartitionByDataNode(addr string) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func (c *Cluster) getAllDecommissionDataPartitionByDiskAndTerm(addr, disk string, term uint64) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr && dp.DecommissionSrcDiskPath == disk && dp.DecommissionTerm == term {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func (c *Cluster) getAllDecommissionDataPartitionByDisk(addr, disk string) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionSrcAddr == addr && dp.DecommissionSrcDiskPath == disk {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

func mergeDataPartitionArr(newDps, oldDps []*DataPartition) []*DataPartition {
	ret := make([]*DataPartition, 0)
	tempMap := make(map[uint64]bool)
	for _, v := range newDps {
		ret = append(ret, v)
		tempMap[v.PartitionID] = true
	}
	for _, v := range oldDps {
		if !tempMap[v.PartitionID] {
			ret = append(ret, v)
			tempMap[v.PartitionID] = true
		}
	}
	return ret
}

func (c *Cluster) generateClusterUuid() (err error) {
	cid := "CID-" + uuid.NewString()
	c.clusterUuid = cid
	if err := c.syncPutCluster(); err != nil {
		c.clusterUuid = ""
		return errors.NewErrorf(fmt.Sprintf("syncPutCluster failed %v", err.Error()))

	}
	return
}
