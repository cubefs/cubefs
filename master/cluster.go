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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

// Cluster stores all the cluster-level information.
type Cluster struct {
	Name                       string
	vols                       map[string]*Vol
	dataNodes                  sync.Map
	metaNodes                  sync.Map
	dpMutex                    sync.Mutex   // data partition mutex
	volMutex                   sync.RWMutex // volume mutex
	createVolMutex             sync.RWMutex // create volume mutex
	mnMutex                    sync.RWMutex // meta node mutex
	dnMutex                    sync.RWMutex // data node mutex
	leaderInfo                 *LeaderInfo
	cfg                        *clusterConfig
	retainLogs                 uint64
	dnFixTinyDeleteRecordLimit uint64
	idAlloc                    *IDAllocator
	t                          *topology
	dataNodeStatInfo           *nodeStatInfo
	metaNodeStatInfo           *nodeStatInfo
	zoneStatInfos              map[string]*proto.ZoneStat
	volStatInfo                sync.Map
	BadDataPartitionIds        *sync.Map
	BadMetaPartitionIds        *sync.Map
	MigratedMetaPartitionIds   *sync.Map
	MigratedDataPartitionIds   *sync.Map
	DisableAutoAllocate        bool
	AutoMergeNodeSet           bool
	fsm                        *MetadataFsm
	partition                  raftstore.Partition
	MasterSecretKey            []byte
	lastMasterZoneForDataNode  string
	lastMasterZoneForMetaNode  string
	lastPermutationsForZone    uint8
	dpRepairChan               chan *RepairTask
	mpRepairChan               chan *RepairTask
	DataNodeBadDisks           *sync.Map
	MetaVersionRequirements    uint32
}
type (
	RepairType uint8
)

const (
	BalanceMetaZone RepairType = iota
	BalanceDataZone
)

type RepairTask struct {
	RType       RepairType
	Pid         uint64
	OfflineAddr string
}
type ChooseDataHostFunc func(c *Cluster, offlineAddr string, dp *DataPartition, excludeNodeSets []uint64, destZoneName string, validOfflineAddr bool) (oldAddr, newAddr string, err error)

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition, cfg *clusterConfig) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.vols = make(map[string]*Vol, 0)
	c.cfg = cfg
	c.t = newTopology()
	c.BadDataPartitionIds = new(sync.Map)
	c.BadMetaPartitionIds = new(sync.Map)
	c.MigratedDataPartitionIds = new(sync.Map)
	c.MigratedMetaPartitionIds = new(sync.Map)
	c.DataNodeBadDisks = new(sync.Map)
	c.dataNodeStatInfo = new(nodeStatInfo)
	c.metaNodeStatInfo = new(nodeStatInfo)
	c.zoneStatInfos = make(map[string]*proto.ZoneStat)
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
	c.initDpRepairChan()
	c.initMpRepairChan()
	return
}

func (c *Cluster) scheduleTask() {
	c.scheduleToCheckDataPartitions()
	c.scheduleToLoadDataPartitions()
	c.scheduleToCheckReleaseDataPartitions()
	c.scheduleToCheckHeartbeat()
	c.scheduleToCheckMetaPartitions()
	c.scheduleToUpdateStatInfo()
	c.scheduleToCheckAutoDataPartitionCreation()
	c.scheduleToCheckVolStatus()
	c.scheduleToCheckDiskRecoveryProgress()
	c.scheduleToCheckMetaPartitionRecoveryProgress()
	c.scheduleToLoadMetaPartitions()
	c.scheduleToReduceReplicaNum()
	c.scheduleToRepairMultiZoneMetaPartitions()
	c.scheduleToRepairMultiZoneDataPartitions()
	c.scheduleToMergeZoneNodeset()
	c.scheduleToCheckAutoMetaPartitionCreation()

}

func (c *Cluster) masterAddr() (addr string) {
	return c.leaderInfo.addr
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

func (c *Cluster) scheduleToCheckAutoDataPartitionCreation() {
	go func() {

		// check volumes after switching leader two minutes
		time.Sleep(2 * time.Minute)
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					vol.checkAutoDataPartitionCreation(c)
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

// Check the replica status of each data partition.
func (c *Cluster) checkDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDataPartitions occurred panic")
		}
	}()

	allBadDisks := make([]map[string][]string, 0)
	vols := c.allVols()
	for _, vol := range vols {
		readWrites, dataNodeBadDisksOfVol := vol.checkDataPartitions(c)
		allBadDisks = append(allBadDisks, dataNodeBadDisksOfVol)
		vol.dataPartitions.setReadWriteDataPartitions(readWrites, c.Name)
		vol.dataPartitions.updateResponseCache(true, 0)
		msg := fmt.Sprintf("action[checkDataPartitions],vol[%v] can readWrite partitions:%v  ", vol.Name, vol.dataPartitions.readableAndWritableCnt)
		log.LogInfo(msg)
	}
	c.updateDataNodeBadDisks(allBadDisks)
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
		task := node.createHeartbeatTask(c.masterAddr())
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
		task := node.createHeartbeatTask(c.masterAddr())
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
		writableMpCount := vol.checkMetaPartitions(c)
		vol.setWritableMpCount(int64(writableMpCount))
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
func (c *Cluster) repairDataPartition(wg sync.WaitGroup) {
	for i := 0; i < cap(c.dpRepairChan); i++ {
		select {
		case task := <-c.dpRepairChan:
			wg.Add(1)
			go func(c *Cluster, task *RepairTask) {
				var err error
				defer func() {
					wg.Done()
					if err != nil {
						log.LogErrorf("ClusterID[%v], Action[repairDataPartition], err[%v]", c.Name, err)
					}
				}()
				var dp *DataPartition
				if dp, err = c.getDataPartitionByID(task.Pid); err != nil {
					return
				}
				switch task.RType {
				case BalanceDataZone:
					if err = c.decommissionDataPartition("", dp, getTargetAddressForBalanceDataPartitionZone, balanceDataPartitionZoneErr, "", "", false); err != nil {
						return
					}
					Warn(c.Name, fmt.Sprintf("action[repairDataPartition] clusterID[%v] vol[%v] data partition[%v] "+
						"Repair success, type[%v]", c.Name, dp.VolName, dp.PartitionID, task.RType))
				default:
					err = fmt.Errorf("action[repairDataPartition] unknown repair task type")
					return
				}
			}(c, task)
		default:
			time.Sleep(time.Second * 2)
		}
	}
}

func (c *Cluster) repairMetaPartition(wg sync.WaitGroup) {
	for i := 0; i < cap(c.mpRepairChan); i++ {
		select {
		case task := <-c.mpRepairChan:
			wg.Add(1)
			go func(c *Cluster, task *RepairTask) {
				var err error
				defer func() {
					wg.Done()
					if err != nil {
						log.LogErrorf("ClusterID[%v], Action[repairMetaPartition], err[%v]", c.Name, err)
					}
				}()
				var mp *MetaPartition
				if mp, err = c.getMetaPartitionByID(task.Pid); err != nil {
					return
				}
				switch task.RType {
				case BalanceMetaZone:
					if err = c.decommissionMetaPartition("", mp, getTargetAddressForRepairMetaZone, "", false, 0); err != nil {
						return
					}
					Warn(c.Name, fmt.Sprintf("action[repairMetaPartition] clusterID[%v] vol[%v] meta partition[%v] "+
						"Repair success, task type[%v]", c.Name, mp.volName, mp.PartitionID, task.RType))
				default:
					err = fmt.Errorf("action[repairMetaPartition] unknown repair task type")
					return
				}
			}(c, task)
		default:
			time.Sleep(time.Second * 2)
		}
	}
}

func (c *Cluster) dataPartitionInRecovering() (num int) {
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		num = num + len(badDataPartitionIds)
		return true
	})

	return
}

func (c *Cluster) metaPartitionInRecovering() (num int) {
	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		num = num + len(badMetaPartitionIds)
		return true
	})
	return
}
func (c *Cluster) scheduleToRepairMultiZoneMetaPartitions() {
	//consumer
	go func() {
		for {
			var wg sync.WaitGroup
			c.repairMetaPartition(wg)
			wg.Wait()
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
	//producer
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() && !c.t.isSingleZone() {
				c.checkVolRepairMetaPartitions()
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkVolRepairMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkVolRepairMetaPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkVolRepairMetaPartitions occurred panic")
		}
	}()
	var mpInRecover uint64
	if c.DisableAutoAllocate || c.cfg.MetaPartitionsRecoverPoolSize == defaultRecoverPoolSize {
		return
	}
	mpInRecover = uint64(c.metaPartitionInRecovering())
	if int32(mpInRecover) > c.cfg.MetaPartitionsRecoverPoolSize {
		log.LogInfof("action[checkVolRepairMetaPartitions] clusterID[%v]Recover pool is full, recover partition[%v], pool size[%v]", c.Name, mpInRecover, c.cfg.MetaPartitionsRecoverPoolSize)
		return
	}
	vols := c.allVols()
	for _, vol := range vols {
		if !vol.autoRepair {
			continue
		}
		if isValid, _ := c.isValidZone(vol.zoneName); !isValid {
			log.LogWarnf("checkVolRepairMetaPartitions, vol[%v], zoneName[%v] not valid, skip repair", vol.Name, vol.zoneName)
			continue
		}
		vol.checkRepairMetaPartitions(c)
	}
}

func (c *Cluster) scheduleToRepairMultiZoneDataPartitions() {
	//consumer
	go func() {
		for {
			var wg sync.WaitGroup
			c.repairDataPartition(wg)
			wg.Wait()
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
	//producer
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() && !c.t.isSingleZone() {
				c.checkVolRepairDataPartitions()
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkVolRepairDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkVolRepairDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkVolRepairDataPartitions occurred panic")
		}
	}()
	var dpInRecover int
	if c.DisableAutoAllocate || c.cfg.DataPartitionsRecoverPoolSize == defaultRecoverPoolSize {
		return
	}
	dpInRecover = c.dataPartitionInRecovering()
	if int32(dpInRecover) >= c.cfg.DataPartitionsRecoverPoolSize {
		log.LogInfof("action[checkVolRepairDataPartitions] clusterID[%v] Recover pool is full, recover partition[%v], pool size[%v]", c.Name, dpInRecover, c.cfg.DataPartitionsRecoverPoolSize)
		return
	}

	vols := c.allVols()
	for _, vol := range vols {
		if !vol.autoRepair {
			continue
		}
		if isValid, _ := c.isValidZone(vol.zoneName); !isValid {
			log.LogWarnf("checkVolRepairDataPartitions, vol[%v], zoneName[%v] not valid, skip repair", vol.Name, vol.zoneName)
			continue
		}
		vol.checkRepairDataPartitions(c)
	}
}

func (c *Cluster) updateMetaNodeBaseInfo(nodeAddr string, id uint64) (err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	value, ok := c.metaNodes.Load(nodeAddr)
	if !ok {
		err = fmt.Errorf("node %v is not exist", nodeAddr)
	}
	metaNode := value.(*MetaNode)
	if metaNode.ID == id {
		return
	}

	metaNode.ID = id
	if err = c.syncUpdateMetaNode(metaNode); err != nil {
		return
	}
	//partitions := c.getAllMetaPartitionsByMetaNode(nodeAddr)
	return
}

func (c *Cluster) addMetaNode(nodeAddr, zoneName string, version uint32) (id uint64, err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	var metaNode *MetaNode
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		return metaNode.ID, nil
	}
	metaNode = newMetaNode(nodeAddr, zoneName, c.Name, version)
	zone, err := c.t.getZone(zoneName)
	if err != nil {
		zone = c.t.putZoneIfAbsent(newZone(zoneName))
	}
	ns := zone.getAvailNodeSetForMetaNode()
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			goto errHandler
		}
	}
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	metaNode.ID = id
	metaNode.NodeSetID = ns.ID
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errHandler
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		goto errHandler
	}
	c.t.putMetaNode(metaNode)
	c.metaNodes.Store(nodeAddr, metaNode)

	if c.needCheckMetaNodeVersion() {
		if version < atomic.LoadUint32(&c.MetaVersionRequirements) {
			//todo : need alarm: meta node need upgrade
		}
	}
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

func (c *Cluster) addDataNode(nodeAddr, zoneName string) (id uint64, err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	var dataNode *DataNode
	if node, ok := c.dataNodes.Load(nodeAddr); ok {
		dataNode = node.(*DataNode)
		return dataNode.ID, nil
	}

	dataNode = newDataNode(nodeAddr, zoneName, c.Name)
	zone, err := c.t.getZone(zoneName)
	if err != nil {
		zone = c.t.putZoneIfAbsent(newZone(zoneName))
	}
	ns := zone.getAvailNodeSetForDataNode()
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			goto errHandler
		}
	}
	// allocate dataNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	dataNode.ID = id
	dataNode.NodeSetID = ns.ID
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errHandler
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		goto errHandler
	}
	c.t.putDataNode(dataNode)
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
	partitionMap := make(map[uint64]uint8)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		if !dataNode.isActive {
			inactiveDataNodes = append(inactiveDataNodes, dataNode.Addr)
		}
		return true
	})
	for _, addr := range inactiveDataNodes {
		partitions := c.getAllDataPartitionByDataNode(addr)
		for _, partition := range partitions {
			partitionMap[partition.PartitionID] = partitionMap[partition.PartitionID] + 1
		}
	}

	for partitionID, badNum := range partitionMap {
		var partition *DataPartition
		if partition, err = c.getDataPartitionByID(partitionID); err != nil {
			return
		}
		if badNum > partition.ReplicaNum/2 && badNum != partition.ReplicaNum {
			corruptPartitions = append(corruptPartitions, partition)
		}
	}
	log.LogInfof("clusterID[%v] inactiveDataNodes:%v  corruptPartitions count:[%v]",
		c.Name, inactiveDataNodes, len(corruptPartitions))
	return
}

func (c *Cluster) checkLackReplicaDataPartitions() (lackReplicaDataPartitions []*DataPartition, err error) {
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

// check corrupt partitions related to this data node
func (c *Cluster) checkCorruptDataNode(dataNode *DataNode) (corruptPartitions []*DataPartition, panicHostsList [][]string, err error) {
	var (
		dataPartitions []*DataPartition
		dn             *DataNode
		corruptPids    []uint64
	)
	corruptPartitions = make([]*DataPartition, 0)
	panicHostsList = make([][]string, 0)
	dataNode.RLock()
	defer dataNode.RUnlock()
	dataPartitions = c.getAllDataPartitionByDataNode(dataNode.Addr)
	for _, partition := range dataPartitions {
		panicHosts := make([]string, 0)
		for _, host := range partition.Hosts {
			if dn, err = c.dataNode(host); err != nil {
				return
			}
			if !dn.isActive {
				panicHosts = append(panicHosts, host)
			}
		}
		if len(panicHosts) > int(partition.ReplicaNum/2) && len(panicHosts) != int(partition.ReplicaNum) {
			corruptPartitions = append(corruptPartitions, partition)
			panicHostsList = append(panicHostsList, panicHosts)
			corruptPids = append(corruptPids, partition.PartitionID)
		}
	}
	log.LogInfof("action[checkCorruptDataNode],clusterID[%v] dataNodeAddr:[%v], corrupt partitions%v",
		c.Name, dataNode.Addr, corruptPids)
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

func (c *Cluster) markDeleteVol(name, authKey string) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
	)
	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[markDeleteVol] err[%v]", err)
		return proto.ErrVolNotExists
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

func (c *Cluster) getVolCrossRegionHAType(volName string) (volCrossRegionHAType proto.CrossRegionHAType, err error) {
	vol, err := c.getVol(volName)
	if err != nil {
		return
	}
	volCrossRegionHAType = vol.CrossRegionHAType
	return
}

func (c *Cluster) batchCreateDataPartition(vol *Vol, reqCount int) (err error) {
	for i := 0; i < reqCount; i++ {
		if c.DisableAutoAllocate {
			return
		}
		if _, err = c.createDataPartition(vol.Name); err != nil {
			log.LogErrorf("action[batchCreateDataPartition] after create [%v] data partition,occurred error,err[%v]", i, err)
			break
		}
	}
	return
}

// Synchronously create a data partition.
// 1. Choose one of the available data nodes.
// 2. Assign it a partition ID.
// 3. Communicate with the data node to synchronously create a data partition.
// - If succeeded, replicate the data through raft and persist it to RocksDB.
// - Otherwise, throw errors
func (c *Cluster) createDataPartition(volName string) (dp *DataPartition, err error) {
	var (
		vol         *Vol
		partitionID uint64
		targetHosts []string
		targetPeers []proto.Peer
		wg          sync.WaitGroup
	)

	if vol, err = c.getVol(volName); err != nil {
		return
	}
	vol.createDpMutex.Lock()
	defer vol.createDpMutex.Unlock()
	errChannel := make(chan error, vol.dpReplicaNum+vol.dpLearnerNum)
	if IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
		if targetHosts, targetPeers, err = c.chooseTargetDataNodesForCreateQuorumDataPartition(int(vol.dpReplicaNum), vol.zoneName); err != nil {
			goto errHandler
		}
	} else {
		if targetHosts, targetPeers, err = c.chooseTargetDataNodes(nil, nil, nil, int(vol.dpReplicaNum), vol.zoneName, false); err != nil {
			goto errHandler
		}
		// vol.dpLearnerNum is 0 now. if it will be used in the feature, should choose new learner replica
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errHandler
	}
	dp = newDataPartition(partitionID, vol.dpReplicaNum, volName, vol.ID)
	dp.Hosts = targetHosts
	dp.Peers = targetPeers
	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			var diskPath string
			if diskPath, err = c.syncCreateDataPartitionToDataNode(host, vol.dataPartitionSize, dp, dp.Peers, dp.Hosts, dp.Learners, proto.NormalCreateDataPartition, vol.CrossRegionHAType); err != nil {
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
		dp.total = util.DefaultDataPartitionSize
		dp.Status = proto.ReadWrite
	}
	if err = c.syncAddDataPartition(dp); err != nil {
		goto errHandler
	}
	vol.dataPartitions.put(dp)
	log.LogInfof("action[createDataPartition] success, volName[%v], partitionId[%v]", volName, partitionID)
	return
errHandler:
	err = fmt.Errorf("action[createDataPartition], clusterID[%v] vol[%v] Err:%v ", c.Name, volName, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) syncCreateDataPartitionToDataNode(host string, size uint64, dp *DataPartition, peers []proto.Peer,
	hosts []string, learners []proto.Learner, createType int, volumeHAType proto.CrossRegionHAType) (
	diskPath string, err error) {
	task := dp.createTaskToCreateDataPartition(host, size, peers, hosts, learners, createType, volumeHAType)
	dataNode, err := c.dataNode(host)
	if err != nil {
		return
	}
	var resp *proto.Packet
	if resp, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return string(resp.Data), nil
}

func (c *Cluster) syncCreateMetaPartitionToMetaNode(host string, mp *MetaPartition, storeMode proto.StoreMode, trashDays uint32) (err error) {
	hosts := make([]string, 0)
	hosts = append(hosts, host)
	tasks := mp.buildNewMetaPartitionTasks(hosts, mp.Peers, mp.volName, storeMode, trashDays)
	metaNode, err := c.metaNode(host)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(tasks[0]); err != nil {
		return
	}
	return
}

func (c *Cluster) isValidZone(zoneName string) (isValid bool, err error) {
	isValid = true
	if zoneName == "" {
		isValid = false
		return
	}
	zoneList := strings.Split(zoneName, ",")
	for _, name := range zoneList {
		if _, err = c.t.getZone(name); err != nil {
			isValid = false
			return
		}
	}
	return
}

//valid zone name
//if zone name duplicate, return error
//if vol enable cross zone and the zone number of cluster less than defaultReplicaNum return error
func (c *Cluster) validZone(zoneName string, replicaNum int) (err error) {
	var crossZone bool
	if zoneName == "" {
		err = fmt.Errorf("zone name empty")
		return
	}

	zoneList := strings.Split(zoneName, ",")
	sort.Strings(zoneList)
	if len(zoneList) > 1 {
		crossZone = true
	}
	if crossZone && c.t.zoneLen() <= 1 {
		return fmt.Errorf("cluster has one zone,can't cross zone")
	}
	for _, name := range zoneList {
		if _, err = c.t.getZone(name); err != nil {
			return
		}
	}
	if len(zoneList) == 1 {
		return
	}
	if len(zoneList) > replicaNum {
		err = fmt.Errorf("can not specify zone number[%v] more than replica number[%v]", len(zoneList), replicaNum)
	}
	//if length of zoneList more than 1, there should not be duplicate zone names
	for i := 0; i < len(zoneList)-1; i++ {
		if zoneList[i] == zoneList[i+1] {
			err = fmt.Errorf("duplicate zone:[%v]", zoneList[i])
			return
		}
	}
	return
}

func (c *Cluster) validCrossRegionHA(volZoneName string) (err error) {
	if volZoneName == "" {
		return fmt.Errorf("zone name is empty")
	}
	zoneList := strings.Split(volZoneName, ",")
	zoneNameMap := make(map[string]bool, len(zoneList))
	for _, name := range zoneList {
		zoneNameMap[name] = true
	}
	if len(zoneNameMap) <= 1 {
		return fmt.Errorf("can not cross region, onle one zones:%v", zoneList)
	}
	masterRegionZoneName, slaveRegionZone, err := c.getMasterAndSlaveRegionZoneName(volZoneName)
	if err != nil {
		return
	}
	if len(masterRegionZoneName) < 2 || len(slaveRegionZone) == 0 {
		return fmt.Errorf("there must be at least two master and one slave region zone for cross region vol")
	}
	return
}

func (c *Cluster) chooseTargetDataNodes(excludeZones []string, excludeNodeSets []uint64, excludeHosts []string, replicaNum int, zoneName string, isStrict bool) (hosts []string, peers []proto.Peer, err error) {

	var (
		zones []*Zone
	)
	allocateZoneMap := make(map[*Zone][]string, 0)
	hasAllocateNum := 0
	hosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if excludeZones == nil {
		excludeZones = make([]string, 0)
	}
	if excludeHosts == nil {
		excludeHosts = make([]string, 0)
	}

	zoneList := strings.Split(zoneName, ",")
	if zones, err = c.t.allocZonesForDataNode(c.Name, zoneName, replicaNum, excludeZones, isStrict); err != nil {
		return
	}

	if len(zones) == 1 && len(zoneList) == 1 {
		if hosts, peers, err = zones[0].getAvailDataNodeHosts(excludeNodeSets, excludeHosts, replicaNum); err != nil {
			log.LogErrorf("action[chooseTargetDataNodes],err[%v]", err)
			return
		}
		goto result
	}
	// Different from the meta partition whose replicas fully fills the 3 zones,
	// each data partition just fills 2 zones to decrease data transfer across zones.
	// Loop through the 3-zones permutation according to the lastPermutationsForZone
	// to choose 2 zones for each partition.
	//   e.g.[zone0, zone0, zone1] -> [zone1, zone1, zone2] -> [zone2, zone2, zone0]
	//    -> [zone1, zone1, zone0] -> [zone2, zone2, zone1] -> [zone0, zone0, zone2]
	// If [zone0, zone1] is chosen for a partition with 3 replicas, 2 replicas will be allocated to zone0,
	// the rest one will be allocated to zone1.
	if len(zones) == 2 {
		switch c.lastPermutationsForZone % 2 {
		case 0:
			zones = append(make([]*Zone, 0), zones[0], zones[1])
			c.lastPermutationsForZone = (c.lastPermutationsForZone + 1) % 6
		default:
			zones = append(make([]*Zone, 0), zones[1], zones[0])
			c.lastPermutationsForZone = (c.lastPermutationsForZone + 1) % 6
		}
	}
	if len(zones) == 3 {
		switch c.lastPermutationsForZone < 3 {
		case true:
			index := c.lastPermutationsForZone
			zones = append(make([]*Zone, 0), zones[index], zones[index], zones[(index+1)%3])
			c.lastPermutationsForZone = (c.lastPermutationsForZone + 1) % 6
		default:
			index := c.lastPermutationsForZone - 3
			zones = append(make([]*Zone, 0), zones[(index+1)%3], zones[(index+1)%3], zones[index])
			c.lastPermutationsForZone = (c.lastPermutationsForZone + 1) % 6
		}
	}
	for hasAllocateNum < replicaNum {
		localExcludeHosts := excludeHosts
		for _, zone := range zones {
			localExcludeHosts = append(localExcludeHosts, allocateZoneMap[zone]...)
			selectedHosts, selectedPeers, e := zone.getAvailDataNodeHosts(excludeNodeSets, localExcludeHosts, 1)
			if e != nil {
				return nil, nil, errors.NewError(e)
			}
			hosts = append(hosts, selectedHosts...)
			peers = append(peers, selectedPeers...)
			allocateZoneMap[zone] = append(allocateZoneMap[zone], selectedHosts...)
			hasAllocateNum = hasAllocateNum + 1
			if hasAllocateNum == replicaNum {
				break
			}
		}
	}
	goto result
result:
	log.LogInfof("action[chooseTargetDataNodes] replicaNum[%v],zoneName[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneName, len(zones), hosts)
	if len(hosts) != replicaNum {
		log.LogErrorf("action[chooseTargetDataNodes] replicaNum[%v],zoneName[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneName, len(zones), hosts)
		return nil, nil, errors.Trace(proto.ErrNoDataNodeToCreateDataPartition, "hosts len[%v],replicaNum[%v],zoneName[%v],selectedZones[%v]",
			len(hosts), replicaNum, zoneName, len(zones))
	}
	return
}
func (c *Cluster) chooseTargetDataNodesForDecommission(excludeZone string, dp *DataPartition, excludeHosts []string, replicaNum int, zoneName string) (hosts []string, peers []proto.Peer, err error) {
	var zones []*Zone
	var targetZone *Zone
	zones = make([]*Zone, 0)
	zoneList := strings.Split(zoneName, ",")
	for _, z := range zoneList {
		var zone *Zone
		if zone, err = c.t.getZone(z); err != nil {
			return
		}
		zones = append(zones, zone)
	}
	//if not cross zone, choose a zone from all zones
	if len(zoneList) <= 1 {
		zones = c.t.getAllZones()
	}
	demandWriteNodes := 1
	candidateZones := make([]*Zone, 0)
	for _, z := range zones {
		if z.status == unavailableZone {
			continue
		}
		if excludeZone == z.name {
			continue
		}
		if z.canWriteForDataNode(uint8(demandWriteNodes)) {
			candidateZones = append(candidateZones, z)
		}
	}
	//must have a candidate zone
	if len(candidateZones) < 1 {
		log.LogError(fmt.Sprintf("action[allocZonesForDataNode],there are no candidateZones, demandWriteNodes[%v], err:%v",
			demandWriteNodes, proto.ErrNoZoneToCreateDataPartition))
		return nil, nil, proto.ErrNoZoneToCreateDataPartition
	}
	//choose target zone for single zone partition
	if len(zoneList) == 1 {
		for index, zone := range candidateZones {
			if c.lastMasterZoneForDataNode == "" {
				targetZone = zone
				c.lastMasterZoneForDataNode = targetZone.name
				break
			}
			if zone.name == c.lastMasterZoneForDataNode {
				if index == len(candidateZones)-1 {
					targetZone = candidateZones[0]
				} else {
					targetZone = candidateZones[index+1]
				}
				c.lastMasterZoneForDataNode = targetZone.name
				break
			}
		}
		if targetZone == nil {
			targetZone = candidateZones[0]
			c.lastMasterZoneForDataNode = targetZone.name
		}
	}
	//choose target zone for cross zone partition
	if len(zoneList) > 1 {
		var curZonesMap map[string]uint8
		if curZonesMap, err = dp.getDataZoneMap(c); err != nil {
			return
		}
		//avoid change from 2 zones to 1 zone after decommission
		if len(curZonesMap) == 2 && curZonesMap[excludeZone] == 1 {
			for k := range curZonesMap {
				if k == excludeZone {
					continue
				}
				for _, z := range candidateZones {
					if z.name == k {
						continue
					}
					targetZone = z
				}
			}
		} else {
			targetZone = candidateZones[0]
		}
	}
	if targetZone == nil {
		err = fmt.Errorf("no candidate zones available")
		return
	}
	hosts, peers, err = targetZone.getAvailDataNodeHosts(nil, excludeHosts, 1)
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
		for _, mp := range vol.MetaPartitions {
			for _, host := range mp.Hosts {
				if host == addr {
					partitions = append(partitions, mp)
					break
				}
			}
		}
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
			for _, host := range mp.Hosts {
				if host == addr {
					partitionIDs = append(partitionIDs, mp.PartitionID)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getAllMetaPartitionsByMetaNode(addr string) (partitions []*MetaPartition) {
	partitions = make([]*MetaPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			for _, host := range mp.Hosts {
				if host == addr {
					partitions = append(partitions, mp)
					break
				}
			}
		}
	}
	return
}

func (c *Cluster) decommissionDataNode(dataNode *DataNode, destZoneName string, strictFlag bool) (err error) {
	msg := fmt.Sprintf("action[decommissionDataNode], Node[%v],strictMode[%v] OffLine", dataNode.Addr, strictFlag)
	log.LogWarn(msg)
	var wg sync.WaitGroup
	dataNode.ToBeOffline = true
	dataNode.AvailableSpace = 1
	partitions := c.getAllDataPartitionByDataNode(dataNode.Addr)
	errChannel := make(chan error, len(partitions))
	defer func() {
		if err != nil {
			dataNode.ToBeOffline = false
		}
		close(errChannel)
	}()
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer wg.Done()
			if err1 := c.decommissionDataPartition(dataNode.Addr, dp, getTargetAddressForDataPartitionDecommission, dataNodeOfflineErr, destZoneName, "", strictFlag); err1 != nil {
				errChannel <- err1
			}
		}(dp)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return
	default:
	}
	if err = c.syncDeleteDataNode(dataNode); err != nil {
		msg = fmt.Sprintf("action[decommissionDataNode],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, dataNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delDataNodeFromCache(dataNode)
	msg = fmt.Sprintf("action[decommissionDataNode],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) delDataNodeFromCache(dataNode *DataNode) {
	c.dataNodes.Delete(dataNode.Addr)
	c.t.deleteDataNode(dataNode)
	go dataNode.clean()
}

// Decommission a data partition.In strict mode, only if the size of the replica is equal,
// or the number of files is equal, the recovery is considered complete. when it is triggered by migrated dataNode,
// the strict mode is true,otherwise is false.
// 1. Check if we can decommission a data partition. In the following cases, we are not allowed to do so:
// - (a) a replica is not in the latest host list;
// - (b) there is already a replica been taken offline;
// - (c) the remaining number of replicas is less than the majority
// 2. Choose a new data node.
// 3. synchronized decommission data partition
// 4. synchronized create a new data partition
// 5. Set the data partition as readOnly.
// 6. persistent the new host list
func (c *Cluster) decommissionDataPartition(offlineAddr string, dp *DataPartition, chooseDataHostFunc ChooseDataHostFunc, errMsg, destZoneName string, destAddr string, strictMode bool) (err error) {
	var (
		oldAddr         string
		addAddr         string
		dpReplica       *DataReplica
		excludeNodeSets []uint64
		msg             string
		isLearner       bool
		pmConfig        *proto.PromoteConfig
	)
	dp.offlineMutex.Lock()
	defer dp.offlineMutex.Unlock()
	excludeNodeSets = make([]uint64, 0)
	if destAddr != "" {
		if err = c.validateDecommissionDataPartition(dp, offlineAddr); err != nil {
			goto errHandler
		}
		if _, err = c.dataNode(offlineAddr); err != nil {
			goto errHandler
		}
		if contains(dp.Hosts, destAddr) {
			err = fmt.Errorf("destinationAddr[%v] must be a new data node addr,oldHosts[%v]", destAddr, dp.Hosts)
			goto errHandler
		}
		if _, err = c.dataNode(destAddr); err != nil {
			goto errHandler
		}
		oldAddr = offlineAddr
		addAddr = destAddr
	} else {
		if oldAddr, addAddr, err = chooseDataHostFunc(c, offlineAddr, dp, excludeNodeSets, destZoneName, true); err != nil {
			goto errHandler
		}
	}
	if isLearner, pmConfig, err = c.removeDataReplica(dp, oldAddr, false, strictMode); err != nil {
		goto errHandler
	}
	if isLearner {
		if err = c.addDataReplicaLearner(dp, addAddr, pmConfig.AutoProm, pmConfig.PromThreshold); err != nil {
			goto errHandler
		}
	} else {
		if err = c.addDataReplica(dp, addAddr, false); err != nil {
			goto errHandler
		}
	}
	dp.Lock()
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	c.syncUpdateDataPartition(dp)
	dp.Unlock()
	dpReplica, _ = dp.getReplica(oldAddr)
	if strictMode {
		c.putMigratedDataPartitionIDs(dpReplica, oldAddr, dp.PartitionID)
	} else {
		c.putBadDataPartitionIDs(dpReplica, oldAddr, dp.PartitionID)
	}
	// update latest version of replica member info to all partition replica member
	go c.syncDataPartitionReplicasToDataNode(dp)

	return
errHandler:
	msg = errMsg + fmt.Sprintf("clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, oldAddr, addAddr, err, dp.Hosts)
	if err != nil {
		Warn(c.Name, msg)
		err = fmt.Errorf("vol[%v],partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
	}
	return
}
func (partition *DataPartition) RepairZone(vol *Vol, c *Cluster) (err error) {
	var (
		zoneList             []string
		masterRegionZoneName []string
		isNeedBalance        bool
	)
	isCrossRegionHATypeQuorumVol := IsCrossRegionHATypeQuorum(vol.CrossRegionHAType)
	if isCrossRegionHATypeQuorumVol {
		if masterRegionZoneName, _, err = c.getMasterAndSlaveRegionZoneName(vol.zoneName); err != nil {
			return
		}
	}
	partition.RLock()
	defer partition.RUnlock()
	var isValidZone bool
	if isValidZone, err = c.isValidZone(vol.zoneName); err != nil {
		return
	}
	if !isValidZone {
		log.LogWarnf("action[RepairZone], vol[%v], zoneName[%v], partitionId[%v] dpReplicaNum[%v] can not be automatically repaired", vol.Name, vol.zoneName, partition.PartitionID, vol.dpReplicaNum)
		return
	}
	rps := partition.liveReplicas(defaultDataPartitionTimeOutSec)
	if len(rps) < int(vol.dpReplicaNum) {
		log.LogWarnf("action[RepairZone], vol[%v], zoneName[%v], partitionId[%v] live Replicas [%v] less than dpReplicaNum[%v], can not be automatically repaired",
			vol.Name, vol.zoneName, partition.PartitionID, len(rps), vol.dpReplicaNum)
		return
	}
	zoneList = strings.Split(vol.zoneName, ",")
	if isCrossRegionHATypeQuorumVol {
		zoneList = masterRegionZoneName
	}
	if len(partition.Replicas) != int(vol.dpReplicaNum+vol.dpLearnerNum) {
		log.LogWarnf("action[RepairZone], vol[%v], zoneName[%v], partitionId[%v] data replica length[%v] not equal to dpReplicaNum[%v]",
			vol.Name, vol.zoneName, partition.PartitionID, len(partition.Replicas), vol.dpReplicaNum)
		return
	}
	if partition.isRecover {
		log.LogWarnf("action[RepairZone], vol[%v], zoneName[%v], data partition[%v] is recovering", vol.Name, vol.zoneName, partition.PartitionID)
		return
	}
	var dpInRecover int
	dpInRecover = c.dataPartitionInRecovering()
	if int32(dpInRecover) >= c.cfg.DataPartitionsRecoverPoolSize {
		log.LogWarnf("action[repairDataPartition] clusterID[%v] Recover pool is full, recover partition[%v], pool size[%v]", c.Name, dpInRecover, c.cfg.DataPartitionsRecoverPoolSize)
		return
	}
	if isNeedBalance, err = partition.needToRebalanceZone(c, zoneList, vol.CrossRegionHAType); err != nil {
		return
	}
	if !isNeedBalance {
		return
	}
	if err = c.sendRepairDataPartitionTask(partition, BalanceDataZone); err != nil {
		return
	}
	return
}

var getTargetAddressForDataPartitionDecommission = func(c *Cluster, offlineAddr string, dp *DataPartition, excludeNodeSets []uint64, destZoneName string, validOfflineAddr bool) (oldAddr, newAddr string, err error) {
	var (
		dataNode    *DataNode
		zone        *Zone
		zones       []string
		ns          *nodeSet
		excludeZone string
		targetHosts []string
		vol         *Vol
	)
	if validOfflineAddr {
		if err = c.validateDecommissionDataPartition(dp, offlineAddr); err != nil {
			return
		}
	}
	if dataNode, err = c.dataNode(offlineAddr); err != nil {
		return
	}
	if vol, err = c.getVol(dp.VolName); err != nil {
		return
	}
	if destZoneName != "" {
		if zone, err = c.t.getZone(destZoneName); err != nil {
			return
		}
		if targetHosts, _, err = zone.getAvailDataNodeHosts(excludeNodeSets, dp.Hosts, 1); err != nil {
			return
		}
	} else {
		if dataNode.ZoneName == "" {
			err = fmt.Errorf("dataNode[%v] zone is nil", dataNode.Addr)
			return
		}
		if zone, err = c.t.getZone(dataNode.ZoneName); err != nil {
			return
		}
		if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
			return
		}
		if targetHosts, _, err = ns.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
			// select data nodes from the other node set in same zone
			excludeNodeSets = append(excludeNodeSets, ns.ID)
			if targetHosts, _, err = zone.getAvailDataNodeHosts(excludeNodeSets, dp.Hosts, 1); err != nil {
				if IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
					//select data nodes from the other zones in the same region type
					targetHosts, _, err = c.chooseTargetDataNodesFromSameRegionTypeOfOfflineReplica(zone.regionName, vol.zoneName, 1, excludeNodeSets, dp.Hosts)
					if err != nil {
						return
					}
					newAddr = targetHosts[0]
					oldAddr = offlineAddr
					return
				}
				// select data nodes from the other zone
				zones = dp.getLiveZones(dataNode.Addr)
				if len(zones) == 0 {
					excludeZone = zone.name
				} else {
					excludeZone = zones[0]
				}
				if targetHosts, _, err = c.chooseTargetDataNodes([]string{excludeZone}, excludeNodeSets, dp.Hosts, 1, vol.zoneName, false); err != nil {
					return
				}
			}
		}
	}
	newAddr = targetHosts[0]
	oldAddr = offlineAddr
	return
}

func (c *Cluster) resetDataPartition(dp *DataPartition, panicHosts []string) (err error) {
	var msg string
	if err = c.forceRemoveDataReplica(dp, panicHosts); err != nil {
		goto errHandler
	}
	if len(panicHosts) == 0 {
		return
	}
	//record each badAddress, and update the badAddress by a new address in the same zone(nodeSet)
	for _, address := range panicHosts {
		c.putBadDataPartitionIDs(nil, address, dp.PartitionID)
	}
	dp.Lock()
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	dp.PanicHosts = panicHosts
	c.syncUpdateDataPartition(dp)
	dp.Unlock()
	log.LogWarnf("clusterID[%v] partitionID:%v  panicHosts:%v reset success,PersistenceHosts:[%v]",
		c.Name, dp.PartitionID, panicHosts, dp.Hosts)
	return

errHandler:
	msg = fmt.Sprintf(" clusterID[%v] partitionID:%v  badHosts:%v  "+
		"Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, panicHosts, err, dp.Hosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) validateDecommissionDataPartition(dp *DataPartition, offlineAddr string) (err error) {
	dp.RLock()
	defer dp.RUnlock()
	if ok := dp.hasHost(offlineAddr); !ok {
		err = fmt.Errorf("offline address:[%v] is not in data partition hosts:%v", offlineAddr, dp.Hosts)
		return
	}

	var vol *Vol
	if vol, err = c.getVol(dp.VolName); err != nil {
		return
	}

	replicaNum := vol.dpReplicaNum
	if vol.DPConvertMode == proto.IncreaseReplicaNum && vol.dpReplicaNum == maxQuorumVolDataPartitionReplicaNum {
		replicaNum = dp.ReplicaNum
		if replicaNum < defaultReplicaNum {
			replicaNum = defaultReplicaNum
		}
	}
	if err = dp.hasMissingOneReplica(offlineAddr, int(replicaNum)); err != nil {
		return
	}

	// if the partition can be offline or not
	if err = dp.canBeOffLine(offlineAddr); err != nil {
		return
	}

	if dp.isRecover && !dp.isLatestReplica(offlineAddr) {
		err = fmt.Errorf("vol[%v],data partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, dp.PartitionID, offlineAddr)
		return
	}
	return
}

func (c *Cluster) addDataReplica(dp *DataPartition, addr string, isNeedIncreaseDPReplicaNum bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addDataReplica],vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		}
	}()
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}
	addPeer := proto.Peer{ID: dataNode.ID, Addr: addr}
	// Todo: What if adding raft member success but creating replica failed?
	if err = c.addDataPartitionRaftMember(dp, addPeer, isNeedIncreaseDPReplicaNum); err != nil {
		return
	}

	if err = c.createDataReplica(dp, addPeer); err != nil {
		return
	}
	return
}

func (c *Cluster) buildAddDataPartitionRaftMemberTaskAndSyncSendTask(dp *DataPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
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
	return
}

func (c *Cluster) addDataPartitionRaftMember(dp *DataPartition, addPeer proto.Peer, isNeedIncreaseDPReplicaNum bool) (err error) {
	var (
		leaderAddr           string
		candidateAddrs       []string
		volCrossRegionHAType proto.CrossRegionHAType
		volDPReplicaNum      uint8
		oldDPReplicaNum      uint8
		isReplicaNumChanged  bool
	)
	vol, err := c.getVol(dp.VolName)
	if err != nil {
		return
	}
	volCrossRegionHAType = vol.CrossRegionHAType
	volDPReplicaNum = vol.dpReplicaNum
	if err != nil {
		return
	}
	if leaderAddr, candidateAddrs, err = dp.prepareAddRaftMember(addPeer); err != nil {
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
	newHosts := make([]string, 0, len(dp.Hosts)+1)
	newPeers := make([]proto.Peer, 0, len(dp.Peers)+1)
	if IsCrossRegionHATypeQuorum(volCrossRegionHAType) {
		if newHosts, _, err = dp.getNewHostsWithAddedPeer(c, addPeer.Addr); err != nil {
			dp.Unlock()
			return
		}
	} else {
		newHosts = append(dp.Hosts, addPeer.Addr)
	}
	newPeers = append(dp.Peers, addPeer)

	oldDPReplicaNum = dp.ReplicaNum
	if isNeedIncreaseDPReplicaNum {
		newReplicaNum := uint8(len(newHosts))
		if dp.ReplicaNum < newReplicaNum && newReplicaNum <= volDPReplicaNum {
			isReplicaNumChanged = true
			dp.ReplicaNum = newReplicaNum
		}
	}
	if err = dp.update("addDataPartitionRaftMember", dp.VolName, newPeers, newHosts, dp.Learners, c); err != nil {
		if isReplicaNumChanged {
			dp.ReplicaNum = oldDPReplicaNum
		}
		dp.Unlock()
		return
	}
	dp.Unlock()
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
	learners := make([]proto.Learner, len(dp.Learners))
	copy(learners, dp.Learners)
	dp.RUnlock()
	diskPath, err := c.syncCreateDataPartitionToDataNode(addPeer.Addr, vol.dataPartitionSize, dp, peers, hosts, learners, proto.DecommissionedCreateDataPartition, vol.CrossRegionHAType)
	if err != nil {
		return
	}
	dp.Lock()
	defer dp.Unlock()
	if err = dp.afterCreation(addPeer.Addr, diskPath, c); err != nil {
		return
	}
	if err = dp.update("createDataReplica", dp.VolName, dp.Peers, dp.Hosts, dp.Learners, c); err != nil {
		return
	}
	return
}

func (c *Cluster) removeDataReplica(dp *DataPartition, addr string, validate, migrationMode bool) (isLearner bool, pmConfig *proto.PromoteConfig, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[removeDataReplica],vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		}
	}()
	if validate == true {
		if err = c.validateDecommissionDataPartition(dp, addr); err != nil {
			return
		}
	}
	ok := c.isRecovering(dp, addr) && !dp.isLatestReplica(addr)
	if ok {
		err = fmt.Errorf("vol[%v],data partition[%v] can't decommision until it has recovered", dp.VolName, dp.PartitionID)
		return
	}
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}
	removePeer := proto.Peer{ID: dataNode.ID, Addr: addr}

	if isLearner, pmConfig, err = c.removeDataPartitionRaftMember(dp, removePeer, migrationMode); err != nil {
		return
	}
	if err = c.deleteDataReplica(dp, dataNode, migrationMode); err != nil {
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
func (c *Cluster) forceRemoveDataReplica(dp *DataPartition, panicHosts []string) (err error) {
	dp.RLock()
	defer func() {
		if err != nil {
			log.LogErrorf("action[forceRemoveDataReplica],vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		}
	}()
	//Only after reset peers succeed in remote datanode, the meta data can be updated
	newPeers := make([]proto.Peer, 0, len(dp.Peers)-len(panicHosts))
	for _, host := range dp.Hosts {
		if contains(panicHosts, host) {
			continue
		}
		var dataNode *DataNode
		dataNode, err = c.dataNode(host)
		if err != nil {
			dp.RUnlock()
			return
		}
		peer := proto.Peer{
			ID:   dataNode.ID,
			Addr: host,
		}
		newPeers = append(newPeers, peer)
	}
	dp.RUnlock()
	for _, peer := range newPeers {
		if err = c.resetDataPartitionRaftMember(dp, newPeers, peer.Addr); err != nil {
			return
		}
	}
	/*	//try to change leader after  reset raft members
		if dataNode, err = c.dataNode(dp.Hosts[0]); err != nil {
			log.LogErrorf("action[forceRemoveDataReplica],new peers[%v], old peers[%v], err[%v]", newPeers, dp.Peers, err)
			return
		}
		if dataNode != nil {
			if err = dp.tryToChangeLeader(c, dataNode); err != nil {
				log.LogErrorf("action[forceRemoveDataReplica],new peers[%v], old peers[%v], err[%v]", newPeers, dp.Peers, err)
			}
		}*/

	for _, addr := range panicHosts {
		var newDataPartitions = make([]uint64, 0)
		var dataNode *DataNode
		if dataNode, err = c.dataNode(addr); err != nil {
			log.LogErrorf("action[forceRemoveDataReplica],new peers[%v], old peers[%v], err[%v]", newPeers, dp.Peers, err)
			continue
		}
		//try to delete the excess replica
		if err = c.deleteDataReplica(dp, dataNode, false); err != nil {
			log.LogErrorf("action[forceRemoveDataReplica],new peers[%v], old peers[%v], err[%v]", newPeers, dp.Peers, err)
			continue
		}
		for _, pid := range dataNode.PersistenceDataPartitions {
			if pid != dp.PartitionID {
				newDataPartitions = append(newDataPartitions, pid)
			}
		}
		log.LogInfof("action[forceRemoveDataReplica], node addr[%v], old partition ids[%v], new partition ids[%v]", addr, dataNode.PersistenceDataPartitions, newDataPartitions)
		dataNode.Lock()
		dataNode.PersistenceDataPartitions = newDataPartitions
		dataNode.Unlock()
		_ = c.removeDataPartitionRaftOnly(dp, proto.Peer{ID: dataNode.ID, Addr: addr})
	}

	log.LogInfof("action[forceRemoveDataReplica],new peers[%v], old peers[%v], err[%v]", newPeers, dp.Peers, err)
	return
}

//the order of newPeers must be consistent with former
func (c *Cluster) resetDataPartitionRaftMember(dp *DataPartition, newPeers []proto.Peer, host string) (err error) {
	dp.Lock()
	defer dp.Unlock()
	task, err := dp.createTaskToResetRaftMembers(newPeers, host)
	if err != nil {
		return
	}
	var dataNode *DataNode
	if dataNode, err = c.dataNode(host); err != nil {
		return
	}
	if _, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		log.LogErrorf("action[resetDataPartitionRaftMember] vol[%v], data partition[%v], newPeer[%v], err[%v]", dp.VolName, dp.PartitionID, newPeers, err)
		return
	}

	newHosts := make([]string, 0)
	for _, peer := range newPeers {
		newHosts = append(newHosts, peer.Addr)
	}
	if err = dp.update("resetDataPartitionRaftMember", dp.VolName, newPeers, newHosts, dp.Learners, c); err != nil {
		return
	}
	log.LogInfof("action[resetDataPartitionRaftMember] vol[%v], data partition[%v], newPeers[%v], err[%v]", dp.VolName, dp.PartitionID, newPeers, err)
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

func (c *Cluster) removeDataPartitionRaftMember(dp *DataPartition, removePeer proto.Peer, migrationMode bool) (isLearner bool, pmConfig *proto.PromoteConfig, err error) {
	defer func() {
		if err1 := c.updateDataPartitionOfflinePeerIDWithLock(dp, 0); err1 != nil {
			err = errors.Trace(err, "updateDataPartitionOfflinePeerIDWithLock failed, err[%v]", err1)
		}
	}()
	if err = c.updateDataPartitionOfflinePeerIDWithLock(dp, removePeer.ID); err != nil {
		log.LogErrorf("action[removeDataPartitionRaftMember] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}
	task, err := dp.createTaskToRemoveRaftMember(removePeer)
	if err != nil {
		return
	}
	task.ReserveResource = migrationMode
	leaderAddr := dp.getLeaderAddr()
	leaderDataNode, err := c.dataNode(leaderAddr)
	if _, err = leaderDataNode.TaskManager.syncSendAdminTask(task); err != nil {
		log.LogErrorf("action[removeDataPartitionRaftMember] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}
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
	newLearners := make([]proto.Learner, 0)
	for _, learner := range dp.Learners {
		if learner.ID == removePeer.ID && learner.Addr == removePeer.Addr {
			isLearner = true
			pmConfig = learner.PmConfig
			continue
		}
		newLearners = append(newLearners, learner)
	}
	dp.Lock()
	if err = dp.update("removeDataPartitionRaftMember", dp.VolName, newPeers, newHosts, newLearners, c); err != nil {
		dp.Unlock()
		return
	}
	dp.Unlock()
	return
}

//This function is used to remove the redundant raft member or just apply an excess raft log, without any changement to the fsm
func (c *Cluster) removeDataPartitionRaftOnly(dp *DataPartition, removePeer proto.Peer) (err error) {
	dp.Lock()
	defer dp.Unlock()
	task := dp.createTaskToRemoveRaftOnly(removePeer)
	var leaderDataNode *DataNode
	for _, host := range dp.Hosts {
		if leaderDataNode, err = c.dataNode(host); err != nil {
			return
		}
		task.OperatorAddr = host
		_, err = leaderDataNode.TaskManager.syncSendAdminTask(task)
		if err == nil {
			break
		}
		time.Sleep(retrySendSyncTaskInternal)
	}
	if err != nil {
		log.LogErrorf("action[removeDataPartitionRaftOnly] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
	}
	return
}

func (c *Cluster) updateDataPartitionOfflinePeerIDWithLock(dp *DataPartition, peerID uint64) (err error) {
	dp.Lock()
	defer dp.Unlock()
	dp.OfflinePeerID = peerID
	if err = dp.update("updateDataPartitionOfflinePeerIDWithLock", dp.VolName, dp.Peers, dp.Hosts, dp.Learners, c); err != nil {
		return
	}
	return
}

func (c *Cluster) deleteDataReplica(dp *DataPartition, dataNode *DataNode, migrationMode bool) (err error) {
	dp.Lock()
	// in case dataNode is unreachable,update meta first.
	dp.removeReplicaByAddr(dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)
	if err = dp.update("deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, dp.Learners, c); err != nil {
		dp.Unlock()
		return
	}
	task := dp.createTaskToDeleteDataPartition(dataNode.Addr)
	dp.Unlock()
	if migrationMode {
		return
	}
	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
	}
	return nil
}

func (c *Cluster) addDataReplicaLearner(dp *DataPartition, addr string, autoProm bool, threshold uint8) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addDataReplicaLearner], vol[%v], data partition[%v], err[%v]", dp.VolName, dp.PartitionID, err)
		}
	}()
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}
	addPeer := proto.Peer{ID: dataNode.ID, Addr: addr}
	addLearner := proto.Learner{ID: dataNode.ID, Addr: addr, PmConfig: &proto.PromoteConfig{AutoProm: autoProm, PromThreshold: threshold}}
	if err = c.addDataPartitionRaftLearner(dp, addPeer, addLearner); err != nil {
		return
	}

	if err = c.createDataReplica(dp, addPeer); err != nil {
		return
	}
	return
}

func (c *Cluster) addDataPartitionRaftLearner(dp *DataPartition, addPeer proto.Peer, addLearner proto.Learner) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)
	volCrossRegionHAType, err := c.getVolCrossRegionHAType(dp.VolName)
	if err != nil {
		return
	}
	if leaderAddr, candidateAddrs, err = dp.prepareAddRaftMember(addPeer); err != nil {
		return
	}
	//send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		if leaderAddr == "" && len(candidateAddrs) < int(dp.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		_, err = c.buildAddDataPartitionRaftLearnerTaskAndSyncSendTask(dp, addLearner, host)
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
	newHosts := make([]string, 0, len(dp.Hosts)+1)
	newPeers := make([]proto.Peer, 0, len(dp.Peers)+1)
	newLearners := make([]proto.Learner, 0, len(dp.Learners)+1)
	if IsCrossRegionHATypeQuorum(volCrossRegionHAType) {
		if newHosts, _, err = dp.getNewHostsWithAddedPeer(c, addPeer.Addr); err != nil {
			dp.Unlock()
			return
		}
	} else {
		newHosts = append(dp.Hosts, addLearner.Addr)
	}
	newPeers = append(dp.Peers, addPeer)
	newLearners = append(dp.Learners, addLearner)
	if err = dp.update("addDataPartitionRaftLearner", dp.VolName, newPeers, newHosts, newLearners, c); err != nil {
		dp.Unlock()
		return
	}
	dp.Unlock()
	return
}

func (c *Cluster) buildAddDataPartitionRaftLearnerTaskAndSyncSendTask(dp *DataPartition, addLearner proto.Learner, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}
		if err != nil {
			log.LogErrorf("buildAddDataPartitionRaftLearnerTaskAndSyncSendTask: vol[%v], data partition[%v],resultCode[%v],err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
		} else {
			log.LogWarnf("buildAddDataPartitionRaftLearnerTaskAndSyncSendTask: vol[%v], data partition[%v],resultCode[%v],err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
		}
	}()
	task, err := dp.createTaskToAddRaftLearner(addLearner, leaderAddr)
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
	return
}

func (c *Cluster) promoteDataReplicaLearner(partition *DataPartition, addr string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[promoteDataReplicaLearner], vol[%v], data partition[%v], err[%v]", partition.VolName, partition.PartitionID, err)
		}
	}()
	partition.Lock()
	defer partition.Unlock()
	if !contains(partition.Hosts, addr) {
		err = fmt.Errorf("vol[%v], dp[%v] has not contain host[%v]", partition.VolName, partition.PartitionID, addr)
		return
	}
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return
	}
	isLearnerExist := false
	promoteLearner := proto.Learner{ID: dataNode.ID, Addr: addr}
	for _, learner := range partition.Learners {
		if learner.ID == dataNode.ID {
			isLearnerExist = true
			promoteLearner.PmConfig = learner.PmConfig
			break
		}
	}
	if !isLearnerExist {
		err = fmt.Errorf("vol[%v], dp[%v] has not contain learner[%v]", partition.VolName, partition.PartitionID, addr)
		return
	}
	if err = c.promoteDataPartitionRaftLearner(partition, promoteLearner); err != nil {
		return
	}
	newLearners := make([]proto.Learner, 0)
	for _, learner := range partition.Learners {
		if learner.ID == promoteLearner.ID {
			continue
		}
		newLearners = append(newLearners, learner)
	}
	if err = partition.update("promoteDataReplicaLearner", partition.VolName, partition.Peers, partition.Hosts, newLearners, c); err != nil {
		return
	}
	return
}

func (c *Cluster) promoteDataPartitionRaftLearner(partition *DataPartition, addLearner proto.Learner) (err error) {

	var (
		candidateAddrs []string
		leaderAddr     string
	)
	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderAddr = partition.getLeaderAddr()
	if contains(partition.Hosts, leaderAddr) {
		candidateAddrs = append(candidateAddrs, leaderAddr)
	} else {
		leaderAddr = ""
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	//send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		//wait for a new leader
		if leaderAddr == "" && len(candidateAddrs) < int(partition.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		_, err = c.buildPromoteDataPartitionRaftLearnerTaskAndSyncSend(partition, addLearner, host)
		if err == nil {
			break
		}
		if index < len(candidateAddrs)-1 {
			time.Sleep(retrySendSyncTaskInternal)
		}
	}
	return
}

func (c *Cluster) buildPromoteDataPartitionRaftLearnerTaskAndSyncSend(dp *DataPartition, promoteLearner proto.Learner, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}
		log.LogErrorf("action[promoteDataRaftLearnerAndSend], vol[%v], data partition[%v], resultCode[%v], err[%v]", dp.VolName, dp.PartitionID, resultCode, err)
	}()
	t, err := dp.createTaskToPromoteRaftLearner(promoteLearner, leaderAddr)
	if err != nil {
		return
	}
	leaderDataNode, err := c.dataNode(leaderAddr)
	if err != nil {
		return
	}
	if resp, err = leaderDataNode.TaskManager.syncSendAdminTask(t); err != nil {
		return
	}
	return
}

func (c *Cluster) putBadMetaPartitions(addr string, partitionID uint64) {
	newBadPartitionIDs := make([]uint64, 0)
	badPartitionIDs, ok := c.BadMetaPartitionIds.Load(addr)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadMetaPartitionIds.Store(addr, newBadPartitionIDs)
}

func (c *Cluster) putBadDataPartitionIDs(replica *DataReplica, addr string, partitionID uint64) {
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

func (c *Cluster) decommissionMetaNode(metaNode *MetaNode, strictMode bool) (err error) {
	msg := fmt.Sprintf("action[decommissionMetaNode],clusterID[%v] Node[%v] begin", c.Name, metaNode.Addr)
	log.LogWarn(msg)
	var wg sync.WaitGroup
	metaNode.ToBeOffline = true
	metaNode.MaxMemAvailWeight = 1
	partitions := c.getAllMetaPartitionByMetaNode(metaNode.Addr)
	errChannel := make(chan error, len(partitions))
	defer func() {
		metaNode.ToBeOffline = false
		close(errChannel)
	}()
	for _, mp := range partitions {
		wg.Add(1)
		go func(mp *MetaPartition) {
			defer wg.Done()
			if err1 := c.decommissionMetaPartition(metaNode.Addr, mp, getTargetAddressForMetaPartitionDecommission, "", strictMode, 0); err1 != nil {
				errChannel <- err1
			}
		}(mp)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return
	default:
	}
	if err = c.syncDeleteMetaNode(metaNode); err != nil {
		msg = fmt.Sprintf("action[decommissionMetaNode],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, metaNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.deleteMetaNodeFromCache(metaNode)
	msg = fmt.Sprintf("action[decommissionMetaNode],clusterID[%v] Node[%v] OffLine success", c.Name, metaNode.Addr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) deleteMetaNodeFromCache(metaNode *MetaNode) {
	c.metaNodes.Delete(metaNode.Addr)
	c.t.deleteMetaNode(metaNode)
	go metaNode.clean()
}

func (c *Cluster) volStMachineWithOutLock(vol *Vol, oldState, newState proto.VolConvertState) (err error) {

	err = fmt.Errorf("convert vol state: %v --> %v failed", oldState.Str(), newState.Str())
	switch oldState {
	case proto.VolConvertStInit:
		if newState == proto.VolConvertStPrePared {
			err = nil
		}
	case proto.VolConvertStPrePared:
		if newState == proto.VolConvertStRunning || newState == proto.VolConvertStStopped {
			err = nil
		}
	case proto.VolConvertStRunning:
		if newState == proto.VolConvertStStopped || newState == proto.VolConvertStFinished {
			err = nil
		}
	case proto.VolConvertStStopped:
		if newState == proto.VolConvertStRunning || newState == proto.VolConvertStPrePared {
			err = nil
		}
	case proto.VolConvertStFinished:
		if newState == proto.VolConvertStPrePared {
			err = nil
		}
	default:
	}

	if err == nil {
		vol.convertState = newState
	}

	return
}

func (c *Cluster) setVolConvertTaskState(name, authKey string, newState proto.VolConvertState) (err error) {
	var (
		vol           *Vol
		oldState      proto.VolConvertState
		serverAuthKey string
	)

	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[setVolConvertTaskState] err[%v]", err)
		err = proto.ErrVolNotExists
		err = fmt.Errorf("action[setVolConvertTaskState], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
		goto errHandler
	}

	vol.Lock()
	defer vol.Unlock()
	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}

	oldState = vol.convertState
	if oldState == newState {
		return nil
	}

	if err = c.volStMachineWithOutLock(vol, oldState, newState); err != nil {
		goto errHandler
	}

	if err = c.syncUpdateVol(vol); err != nil {
		vol.convertState = oldState
		log.LogErrorf("action[setVolConvertTaskState] vol[%v] err[%v]", name, err)
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

func (c *Cluster) updateVol(name, authKey, zoneName, description string, capacity uint64, replicaNum, mpReplicaNum uint8,
	followerRead, authenticate, enableToken, autoRepair, forceROW bool, dpSelectorName, dpSelectorParm string,
	ossBucketPolicy proto.BucketAccessPolicy, crossRegionHAType proto.CrossRegionHAType, dpWriteableThreshold float64,
	remainingDays uint32, storeMode proto.StoreMode, layout proto.MetaPartitionLayout, extentCacheExpireSec int64) (err error) {
	var (
		vol           *Vol
		volBack       *Vol
		serverAuthKey string
		zoneList      []string
	)
	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[updateVol] err[%v]", err)
		err = proto.ErrVolNotExists
		goto errHandler
	}
	vol.Lock()
	defer vol.Unlock()
	volBack = vol.backupConfig()
	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}
	if capacity < vol.Capacity {
		err = fmt.Errorf("capacity[%v] less than old capacity[%v]", capacity, vol.Capacity)
		goto errHandler
	}
	if replicaNum == 0 || mpReplicaNum == 0 {
		err = fmt.Errorf("new replicaNum[%v] or replicaNum[%v] can not be 0", replicaNum, mpReplicaNum)
		goto errHandler
	}

	if IsCrossRegionHATypeQuorum(crossRegionHAType) {
		if replicaNum != maxQuorumVolDataPartitionReplicaNum {
			err = fmt.Errorf("for cross region quorum vol, dp replicaNum should be 5")
			goto errHandler
		}
		if mpReplicaNum != defaultQuorumMetaPartitionMasterRegionCount {
			err = fmt.Errorf("for cross region quorum vol, mpReplicaNum should be 3")
			goto errHandler
		}
		vol.mpLearnerNum = defaultQuorumMetaPartitionLearnerReplicaNum
	}
	if !IsCrossRegionHATypeQuorum(crossRegionHAType) && replicaNum > vol.dpReplicaNum {
		err = fmt.Errorf("don't support new replicaNum[%v] larger than old dpReplicaNum[%v] for crossRegionHAType[%s]",
			replicaNum, vol.dpReplicaNum, crossRegionHAType)
		goto errHandler
	}
	if enableToken == true && len(vol.tokens) == 0 {
		if err = c.createToken(vol, proto.ReadOnlyToken); err != nil {
			goto errHandler
		}
		if err = c.createToken(vol, proto.ReadWriteToken); err != nil {
			goto errHandler
		}
	}
	if zoneName != "" {
		if err = c.validZone(zoneName, int(replicaNum)); err != nil {
			goto errHandler
		}
		if err = c.validZone(zoneName, int(vol.mpReplicaNum+vol.mpLearnerNum)); err != nil {
			goto errHandler
		}
		vol.zoneName = zoneName
	}
	zoneList = strings.Split(vol.zoneName, ",")
	if len(zoneList) > 1 {
		vol.crossZone = true
	} else {
		vol.crossZone = false
	}
	if extentCacheExpireSec == 0 {
		extentCacheExpireSec = defaultExtentCacheExpireSec
	}

	if vol.MpLayout != layout {
		if err = c.volStMachineWithOutLock(vol, vol.convertState, proto.VolConvertStPrePared); err != nil {
			err = fmt.Errorf("set meta layout must stop convert task, err:%v", err.Error())
			goto errHandler
		}
		vol.MpLayout = layout
	}

	vol.Capacity = capacity
	vol.FollowerRead = followerRead
	vol.ForceROW = forceROW
	vol.ExtentCacheExpireSec = extentCacheExpireSec
	vol.authenticate = authenticate
	vol.enableToken = enableToken
	vol.autoRepair = autoRepair
	vol.CrossRegionHAType = crossRegionHAType
	if description != "" {
		vol.description = description
	}
	//for normal vol, only reduced dp replica num is supported
	if replicaNum != 0 && (replicaNum < vol.dpReplicaNum || IsCrossRegionHATypeQuorum(crossRegionHAType)) {
		if replicaNum > vol.dpReplicaNum {
			vol.DPConvertMode = proto.IncreaseReplicaNum
		}
		vol.dpReplicaNum = replicaNum
	}
	// only can increase mp replica num
	if mpReplicaNum > vol.mpReplicaNum {
		vol.mpReplicaNum = mpReplicaNum
		vol.MPConvertMode = proto.IncreaseReplicaNum
	}

	if remainingDays > maxTrashRemainingDays {
		remainingDays = maxTrashRemainingDays
	}
	vol.dpSelectorName = dpSelectorName
	vol.dpSelectorParm = dpSelectorParm
	vol.OSSBucketPolicy = ossBucketPolicy
	vol.trashRemainingDays = remainingDays
	vol.DefaultStoreMode = storeMode
	vol.dpWriteableThreshold = dpWriteableThreshold

	if err = c.syncUpdateVol(vol); err != nil {
		log.LogErrorf("action[updateVol] vol[%v] err[%v]", name, err)
		err = proto.ErrPersistenceByRaft
		goto errHandler
	}
	return
errHandler:
	vol.rollbackConfig(volBack)
	err = fmt.Errorf("action[updateVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

// Create a new volume.
// By default we create 3 meta partitions and 10 data partitions during initialization.
func (c *Cluster) createVol(name, owner, zoneName, description string, mpCount, dpReplicaNum, mpReplicaNum, size, capacity, trashDays int,
	followerRead, authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW bool,
	crossRegionHAType proto.CrossRegionHAType, dpWriteableThreshold float64,
	storeMode proto.StoreMode, mpLayout proto.MetaPartitionLayout) (vol *Vol, err error) {
	var (
		dataPartitionSize       uint64
		readWriteDataPartitions int
		mpLearnerNum            uint8
	)
	if size == 0 {
		dataPartitionSize = util.DefaultDataPartitionSize
	} else {
		dataPartitionSize = uint64(size) * util.GB
	}
	if zoneName == "" {
		zoneName = DefaultZoneName
	}
	if IsCrossRegionHATypeQuorum(crossRegionHAType) {
		if dpReplicaNum != maxQuorumVolDataPartitionReplicaNum {
			err = fmt.Errorf("for cross region quorum vol, dp replicaNum should be 5")
			goto errHandler
		}
		if mpReplicaNum != defaultQuorumMetaPartitionMasterRegionCount {
			err = fmt.Errorf("for cross region quorum vol, mpReplicaNum should be 3")
			goto errHandler
		}
		mpLearnerNum = defaultQuorumMetaPartitionLearnerReplicaNum
		if err = c.validCrossRegionHA(zoneName); err != nil {
			goto errHandler
		}
	}
	if err = c.validZone(zoneName, dpReplicaNum); err != nil {
		goto errHandler
	}
	if err = c.validZone(zoneName, mpReplicaNum+int(mpLearnerNum)); err != nil {
		goto errHandler
	}
	if trashDays == 0 {
		trashDays = defaultTrashRemainingDays
	}
	if vol, err = c.doCreateVol(name, owner, zoneName, description, dataPartitionSize, uint64(capacity), dpReplicaNum, mpReplicaNum, trashDays,
		followerRead, authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW, crossRegionHAType, 0, mpLearnerNum, dpWriteableThreshold,
		storeMode, proto.VolConvertStInit, mpLayout); err != nil {
		goto errHandler
	}
	if err = vol.initMetaPartitions(c, mpCount); err != nil {
		vol.Status = markDelete
		if e := vol.deleteVolFromStore(c); e != nil {
			log.LogErrorf("action[createVol] failed,vol[%v] err[%v]", vol.Name, e)
		}
		c.deleteVol(name)
		err = fmt.Errorf("action[createVol] initMetaPartitions failed,err[%v]", err)
		goto errHandler
	}
	for retryCount := 0; readWriteDataPartitions < defaultInitDataPartitionCnt && retryCount < 3; retryCount++ {
		_ = vol.initDataPartitions(c)
		readWriteDataPartitions = len(vol.dataPartitions.partitionMap)
	}
	vol.dataPartitions.readableAndWritableCnt = readWriteDataPartitions
	vol.updateViewCache(c)
	log.LogInfof("action[createVol] vol[%v],readableAndWritableCnt[%v]", name, readWriteDataPartitions)
	return

errHandler:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err)
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) doCreateVol(name, owner, zoneName, description string, dpSize, capacity uint64, dpReplicaNum, mpReplicaNum, trashDays int,
	followerRead, authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW bool,
	crossRegionHAType proto.CrossRegionHAType, dpLearnerNum, mpLearnerNum uint8, dpWriteableThreshold float64,
	storeMode proto.StoreMode, convertSt proto.VolConvertState, mpLayout proto.MetaPartitionLayout) (vol *Vol, err error) {
	var id uint64
	c.createVolMutex.Lock()
	defer c.createVolMutex.Unlock()
	var createTime = time.Now().Unix() // record unix seconds of volume create time
	if _, err = c.getVol(name); err == nil {
		err = proto.ErrDuplicateVol
		goto errHandler
	}
	id, err = c.idAlloc.allocateCommonID()
	if err != nil {
		goto errHandler
	}
	vol = newVol(id, name, owner, zoneName, dpSize, capacity, uint8(dpReplicaNum), uint8(mpReplicaNum), followerRead,
		authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW, createTime, description, "", "",
		crossRegionHAType, dpLearnerNum, mpLearnerNum, dpWriteableThreshold, uint32(trashDays), storeMode, convertSt, mpLayout)

	// refresh oss secure
	vol.refreshOSSSecure()
	if err = c.syncAddVol(vol); err != nil {
		goto errHandler
	}
	c.putVol(vol)
	if enableToken {
		if err = c.createToken(vol, proto.ReadOnlyToken); err != nil {
			goto errHandler
		}
		if err = c.createToken(vol, proto.ReadWriteToken); err != nil {
			goto errHandler
		}
	}
	return
errHandler:
	err = fmt.Errorf("action[doCreateVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
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

// Choose the target hosts from the available zones and meta nodes.
func (c *Cluster) chooseTargetMetaHosts(excludeZone string, excludeNodeSets []uint64, excludeHosts []string, replicaNum int, zoneName string, isStrict bool) (hosts []string, peers []proto.Peer, err error) {
	var (
		zones []*Zone
	)
	allocateZoneMap := make(map[*Zone][]string, 0)
	hasAllocateNum := 0
	excludeZones := make([]string, 0)
	hosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if excludeHosts == nil {
		excludeHosts = make([]string, 0)
	}
	if excludeZone != "" {
		excludeZones = append(excludeZones, excludeZone)
	}
	if zones, err = c.t.allocZonesForMetaNode(c.Name, zoneName, replicaNum, excludeZones, isStrict); err != nil {
		return
	}
	zoneList := strings.Split(zoneName, ",")
	if len(zones) == 1 && len(zoneList) == 1 {
		if hosts, peers, err = zones[0].getAvailMetaNodeHosts(excludeNodeSets, excludeHosts, replicaNum); err != nil {
			log.LogErrorf("action[chooseTargetMetaNodes],err[%v]", err)
			return
		}
		goto result
	}
	if len(zones) == 2 {
		switch c.lastPermutationsForZone % 2 {
		case 0:
			zones = append(make([]*Zone, 0), zones[0], zones[1])
			c.lastPermutationsForZone = (c.lastPermutationsForZone + 1) % 6
		default:
			zones = append(make([]*Zone, 0), zones[1], zones[0])
			c.lastPermutationsForZone = (c.lastPermutationsForZone + 1) % 6
		}
	}
	for hasAllocateNum < replicaNum {
		localExcludeHosts := excludeHosts
		for _, zone := range zones {
			localExcludeHosts = append(localExcludeHosts, allocateZoneMap[zone]...)
			selectedHosts, selectedPeers, e := zone.getAvailMetaNodeHosts(excludeNodeSets, localExcludeHosts, 1)
			if e != nil {
				return nil, nil, errors.NewError(e)
			}
			hosts = append(hosts, selectedHosts...)
			peers = append(peers, selectedPeers...)
			allocateZoneMap[zone] = append(allocateZoneMap[zone], selectedHosts...)
			hasAllocateNum = hasAllocateNum + 1
			if hasAllocateNum == replicaNum {
				break
			}
		}
	}
	goto result
result:
	log.LogInfof("action[chooseTargetMetaHosts] replicaNum[%v],zoneName[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneName, zones, hosts)
	if len(hosts) != replicaNum {
		return nil, nil, errors.Trace(proto.ErrNoMetaNodeToCreateMetaPartition, "hosts len[%v],replicaNum[%v]", len(hosts), replicaNum)
	}
	return
}

func (c *Cluster) chooseTargetMetaHostForDecommission(excludeZone string, mp *MetaPartition, excludeHosts []string, replicaNum int, zoneName string) (hosts []string, peers []proto.Peer, err error) {
	var zones []*Zone
	var targetZone *Zone
	zones = make([]*Zone, 0)
	zoneList := strings.Split(zoneName, ",")
	for _, z := range zoneList {
		var zone *Zone
		if zone, err = c.t.getZone(z); err != nil {
			return
		}
		zones = append(zones, zone)

	}
	//if not cross zone, choose a zone from all zones
	if len(zoneList) == 1 {
		zones = c.t.getAllZones()
	}
	demandWriteNodes := 1
	candidateZones := make([]*Zone, 0)
	for _, z := range zones {
		if z.status == unavailableZone {
			continue
		}
		if excludeZone == z.name {
			continue
		}
		if z.canWriteForMetaNode(uint8(demandWriteNodes)) {
			candidateZones = append(candidateZones, z)
		}
	}
	//must have a candidate zone
	if len(candidateZones) < 1 {
		log.LogError(fmt.Sprintf("action[allocZonesForMetaNode],there are no candidateZones, demandWriteNodes[%v], err:%v",
			demandWriteNodes, proto.ErrNoZoneToCreateMetaPartition))
		return nil, nil, proto.ErrNoZoneToCreateMetaPartition
	}
	if len(zoneList) == 1 {
		for index, zone := range candidateZones {
			if c.lastMasterZoneForMetaNode == "" {
				targetZone = zone
				c.lastMasterZoneForMetaNode = targetZone.name
				break
			}
			if zone.name == c.lastMasterZoneForMetaNode {
				if index == len(candidateZones)-1 {
					targetZone = candidateZones[0]
				} else {
					targetZone = candidateZones[index+1]
				}
				c.lastMasterZoneForMetaNode = targetZone.name
				break
			}
		}
		if targetZone == nil {
			targetZone = candidateZones[0]
			c.lastMasterZoneForMetaNode = targetZone.name
		}
	}
	if len(zoneList) > 1 {
		var curZonesMap map[string]uint8
		if curZonesMap, err = mp.getMetaZoneMap(c); err != nil {
			return
		}
		//avoid change from 2 zones to 1 zone after decommission
		if len(curZonesMap) == 2 && curZonesMap[excludeZone] == 1 {
			for k := range curZonesMap {
				if k == excludeZone {
					continue
				}
				for _, z := range candidateZones {
					if z.name == k {
						continue
					}
					targetZone = z
				}
			}
		} else {
			targetZone = candidateZones[0]
		}
	}
	if targetZone == nil {
		err = fmt.Errorf("no candidate zones available")
		return
	}
	hosts, peers, err = targetZone.getAvailMetaNodeHosts(nil, excludeHosts, 1)
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
		dataNodes = append(dataNodes, proto.NodeView{Addr: dataNode.Addr, Status: dataNode.isActive, ID: dataNode.ID, IsWritable: dataNode.isWriteAble()})
		return true
	})
	return
}

func (c *Cluster) allMetaNodes() (metaNodes []proto.NodeView) {
	metaNodes = make([]proto.NodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, proto.NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
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
		count = count + len(vol.MetaPartitions)
	}
	return count
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

func (c *Cluster) setClusterConfig(params map[string]interface{}) (err error) {
	oldDeleteBatchCount := atomic.LoadUint64(&c.cfg.MetaNodeDeleteBatchCount)
	if val, ok := params[nodeDeleteBatchCountKey]; ok {
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, val.(uint64))
	}
	oldDeleteLimitRate := atomic.LoadUint64(&c.cfg.DataNodeDeleteLimitRate)
	if val, ok := params[nodeMarkDeleteRateKey]; ok {
		atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, val.(uint64))
	}
	oldRepairTaskCount := atomic.LoadUint64(&c.cfg.DataNodeRepairTaskCount)
	if val, ok := params[dataNodeRepairTaskCountKey]; ok {
		atomic.StoreUint64(&c.cfg.DataNodeRepairTaskCount, val.(uint64))
	}
	oldMetaNodeReqRateLimit := atomic.LoadUint64(&c.cfg.MetaNodeReqRateLimit)
	if val, ok := params[metaNodeReqRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", metaNodeReqRateKey, minRateLimit)
			return
		}
		atomic.StoreUint64(&c.cfg.MetaNodeReqRateLimit, v)
	}
	oldDeleteWorkerSleepMs := atomic.LoadUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs)
	if val, ok := params[nodeDeleteWorkerSleepMs]; ok {
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, val.(uint64))
	}
	oldDpRecoverPoolSize := atomic.LoadInt32(&c.cfg.DataPartitionsRecoverPoolSize)
	if val, ok := params[dpRecoverPoolSizeKey]; ok {
		atomic.StoreInt32(&c.cfg.DataPartitionsRecoverPoolSize, int32(val.(int64)))
	}
	oldMpRecoverPoolSize := atomic.LoadInt32(&c.cfg.MetaPartitionsRecoverPoolSize)
	if val, ok := params[mpRecoverPoolSizeKey]; ok {
		atomic.StoreInt32(&c.cfg.MetaPartitionsRecoverPoolSize, int32(val.(int64)))
	}
	oldExtentMergeSleepMs := atomic.LoadUint64(&c.cfg.ExtentMergeSleepMs)
	if val, ok := params[extentMergeSleepMsKey]; ok {
		atomic.StoreUint64(&c.cfg.ExtentMergeSleepMs, val.(uint64))
	}

	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClusterConfig] err[%v]", err)
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, oldDeleteBatchCount)
		atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, oldDeleteLimitRate)
		atomic.StoreUint64(&c.cfg.DataNodeRepairTaskCount, oldRepairTaskCount)
		atomic.StoreUint64(&c.cfg.MetaNodeReqRateLimit, oldMetaNodeReqRateLimit)
		atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, oldDeleteWorkerSleepMs)
		atomic.StoreInt32(&c.cfg.DataPartitionsRecoverPoolSize, oldDpRecoverPoolSize)
		atomic.StoreInt32(&c.cfg.MetaPartitionsRecoverPoolSize, oldMpRecoverPoolSize)
		atomic.StoreUint64(&c.cfg.ExtentMergeSleepMs, oldExtentMergeSleepMs)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeReqRateLimit(val uint64, zone string) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	oldVal, ok := c.cfg.DataNodeReqZoneRateLimitMap[zone]
	if val > 0 {
		c.cfg.DataNodeReqZoneRateLimitMap[zone] = val
	} else {
		delete(c.cfg.DataNodeReqZoneRateLimitMap, zone)
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeReqRateLimit] err[%v]", err)
		if ok {
			c.cfg.DataNodeReqZoneRateLimitMap[zone] = oldVal
		} else {
			delete(c.cfg.DataNodeReqZoneRateLimitMap, zone)
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeReqOpRateLimit(val uint64, zone string, op uint8) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	opMap, ok := c.cfg.DataNodeReqZoneOpRateLimitMap[zone]
	var oldVal uint64
	if ok {
		oldVal, ok = opMap[op]
	} else {
		opMap = make(map[uint8]uint64)
		c.cfg.DataNodeReqZoneOpRateLimitMap[zone] = opMap
	}
	if val > 0 {
		opMap[op] = val
	} else {
		delete(opMap, op)
		if len(opMap) == 0 {
			delete(c.cfg.DataNodeReqZoneOpRateLimitMap, zone)
		}
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeReqOpRateLimit] err[%v]", err)
		if ok {
			c.cfg.DataNodeReqZoneOpRateLimitMap[zone][op] = oldVal
		} else {
			delete(opMap, op)
			if len(opMap) == 0 {
				delete(c.cfg.DataNodeReqZoneOpRateLimitMap, zone)
			}
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeReqVolOpRateLimit(val uint64, zone string, vol string, op uint8) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	volOpMap, ok := c.cfg.DataNodeReqZoneVolOpRateLimitMap[zone]
	if !ok {
		volOpMap = make(map[string]map[uint8]uint64)
		c.cfg.DataNodeReqZoneVolOpRateLimitMap[zone] = volOpMap
	}
	opMap, ok := volOpMap[vol]
	var oldVal uint64
	if ok {
		oldVal, ok = opMap[op]
	} else {
		opMap = make(map[uint8]uint64)
		volOpMap[vol] = opMap
	}
	if val > 0 {
		opMap[op] = val
	} else {
		delete(opMap, op)
		if len(opMap) == 0 {
			delete(volOpMap, vol)
			if len(volOpMap) == 0 {
				delete(c.cfg.DataNodeReqZoneVolOpRateLimitMap, zone)
			}
		}
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeReqVolOpRateLimit] err[%v]", err)
		if ok {
			c.cfg.DataNodeReqZoneVolOpRateLimitMap[zone][vol][op] = oldVal
		} else {
			delete(opMap, op)
			if len(opMap) == 0 {
				delete(volOpMap, vol)
				if len(volOpMap) == 0 {
					delete(c.cfg.DataNodeReqZoneVolOpRateLimitMap, zone)
				}
			}
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeReqVolPartRateLimit(val uint64, vol string) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	oldVal, ok := c.cfg.DataNodeReqVolPartRateLimitMap[vol]
	if val > 0 {
		c.cfg.DataNodeReqVolPartRateLimitMap[vol] = val
	} else {
		delete(c.cfg.DataNodeReqVolPartRateLimitMap, vol)
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeReqVolPartRateLimit] err[%v]", err)
		if ok {
			c.cfg.DataNodeReqVolPartRateLimitMap[vol] = oldVal
		} else {
			delete(c.cfg.DataNodeReqVolPartRateLimitMap, vol)
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setDataNodeReqVolOpPartRateLimit(val uint64, vol string, op uint8) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	opMap, ok := c.cfg.DataNodeReqVolOpPartRateLimitMap[vol]
	var oldVal uint64
	if ok {
		oldVal, ok = opMap[op]
	} else {
		opMap = make(map[uint8]uint64)
		c.cfg.DataNodeReqVolOpPartRateLimitMap[vol] = opMap
	}
	if val > 0 {
		opMap[op] = val
	} else {
		delete(opMap, op)
		if len(opMap) == 0 {
			delete(c.cfg.DataNodeReqVolOpPartRateLimitMap, vol)
		}
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDataNodeReqVolOpPartRateLimit] err[%v]", err)
		if ok {
			c.cfg.DataNodeReqVolOpPartRateLimitMap[vol][op] = oldVal
		} else {
			delete(opMap, op)
			if len(opMap) == 0 {
				delete(c.cfg.DataNodeReqVolOpPartRateLimitMap, vol)
			}
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setMetaNodeReqOpRateLimit(val uint64, op uint8) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	oldVal, ok := c.cfg.MetaNodeReqOpRateLimitMap[op]
	if val > 0 {
		c.cfg.MetaNodeReqOpRateLimitMap[op] = val
	} else {
		delete(c.cfg.MetaNodeReqOpRateLimitMap, op)
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setMetaNodeReqVolPartRateLimit] err[%v]", err)
		if ok {
			c.cfg.MetaNodeReqOpRateLimitMap[op] = oldVal
		} else {
			delete(c.cfg.MetaNodeReqOpRateLimitMap, op)
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClientReadVolRateLimit(val uint64, vol string) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	oldVal, ok := c.cfg.ClientReadVolRateLimitMap[vol]
	if val > 0 {
		c.cfg.ClientReadVolRateLimitMap[vol] = val
	} else {
		delete(c.cfg.ClientReadVolRateLimitMap, vol)
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClientReadVolRateLimit] err[%v]", err)
		if ok {
			c.cfg.ClientReadVolRateLimitMap[vol] = oldVal
		} else {
			delete(c.cfg.ClientReadVolRateLimitMap, vol)
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClientWriteVolRateLimit(val uint64, vol string) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	oldVal, ok := c.cfg.ClientWriteVolRateLimitMap[vol]
	if val > 0 {
		c.cfg.ClientWriteVolRateLimitMap[vol] = val
	} else {
		delete(c.cfg.ClientWriteVolRateLimitMap, vol)
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClientWriteVolRateLimit] err[%v]", err)
		if ok {
			c.cfg.ClientWriteVolRateLimitMap[vol] = oldVal
		} else {
			delete(c.cfg.ClientWriteVolRateLimitMap, vol)
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setClientVolOpRateLimit(val int64, vol string, op uint8) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()

	var (
		oldVal  int64
		opExist bool
	)
	opMap, volExist := c.cfg.ClientVolOpRateLimitMap[vol]
	if !volExist {
		opMap = make(map[uint8]int64)
		c.cfg.ClientVolOpRateLimitMap[vol] = opMap
	} else {
		oldVal, opExist = opMap[op]
	}

	opMap[op] = val
	if val < 0 {
		delete(opMap, op)
	}
	log.LogDebugf("setClientVolOpRateLimit: vol(%v) op(%v) val(%v)", vol, op, val)
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setClientVolOpRateLimit] err[%v]", err)
		c.cfg.ClientVolOpRateLimitMap[vol][op] = oldVal
		if !volExist || !opExist {
			delete(c.cfg.ClientVolOpRateLimitMap[vol], op)
		}
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setFixTinyDeleteRecord(limitRate uint64) (err error) {
	oldDeleteRecordLimit := c.dnFixTinyDeleteRecordLimit
	c.dnFixTinyDeleteRecordLimit = limitRate
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setDnFixTinyDeleteRecordLimit] err[%v]", err)
		c.dnFixTinyDeleteRecordLimit = oldDeleteRecordLimit
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) setExtentMergeIno(ino string, vol string) (err error) {
	c.cfg.reqRateLimitMapMutex.Lock()
	defer c.cfg.reqRateLimitMapMutex.Unlock()
	oldVal, ok := c.cfg.ExtentMergeIno[vol]
	if ino != "-1" {
		var inodes []uint64
		inodeSlice := strings.Split(ino, ",")
		for _, inode := range inodeSlice {
			ino, errAtoi := strconv.Atoi(inode)
			if errAtoi != nil {
				err = fmt.Errorf("invalid inode: %s", inode)
				return
			}
			inodes = append(inodes, uint64(ino))
		}
		c.cfg.ExtentMergeIno[vol] = inodes
	} else {
		delete(c.cfg.ExtentMergeIno, vol)
	}
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setExtentMergeIno] err[%v]", err)
		if ok {
			c.cfg.ExtentMergeIno[vol] = oldVal
		} else {
			delete(c.cfg.ExtentMergeIno, vol)
		}
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

func (c *Cluster) setDataNodeToOfflineState(startID, endID uint64, state bool, zoneName string) {
	c.dataNodes.Range(func(key, value interface{}) bool {
		node, ok := value.(*DataNode)
		if !ok {
			return true
		}
		if node.ID < startID || node.ID > endID {
			return true
		}
		if node.ZoneName != zoneName {
			return true
		}
		node.Lock()
		node.ToBeMigrated = state
		node.Unlock()
		return true
	})
}

func (c *Cluster) setMetaNodeToOfflineState(startID, endID uint64, state bool, zoneName string) {
	c.metaNodes.Range(func(key, value interface{}) bool {
		node, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		if node.ID < startID || node.ID > endID {
			return true
		}
		if node.ZoneName != zoneName {
			return true
		}
		node.Lock()
		node.ToBeMigrated = state
		node.Unlock()
		return true
	})
}

func (c *Cluster) initDpRepairChan() {
	var chanCapacity int32
	chanCapacity = c.cfg.DataPartitionsRecoverPoolSize
	if chanCapacity > maxDataPartitionsRecoverPoolSize {
		chanCapacity = maxDataPartitionsRecoverPoolSize
	}
	if chanCapacity < 1 {
		chanCapacity = 1
	}
	c.dpRepairChan = make(chan *RepairTask, chanCapacity)
}

func (c *Cluster) initMpRepairChan() {
	var chanCapacity int32
	chanCapacity = c.cfg.MetaPartitionsRecoverPoolSize
	if chanCapacity > maxMetaPartitionsRecoverPoolSize {
		chanCapacity = maxMetaPartitionsRecoverPoolSize
	}
	if chanCapacity < 1 {
		chanCapacity = 1
	}
	c.mpRepairChan = make(chan *RepairTask, chanCapacity)
}

func (c *Cluster) sendRepairMetaPartitionTask(mp *MetaPartition, rType RepairType) (err error) {
	var repairTask *RepairTask
	repairTask = &RepairTask{
		RType: rType,
		Pid:   mp.PartitionID,
	}
	select {
	case c.mpRepairChan <- repairTask:
		Warn(c.Name, fmt.Sprintf("action[sendRepairMetaPartitionTask] clusterID[%v] vol[%v] meta partition[%v] "+
			"task type[%v]", c.Name, mp.volName, mp.PartitionID, rType))
	default:
		Warn(c.Name, fmt.Sprintf("action[sendRepairMetaPartitionTask] clusterID[%v] vol[%v] meta partition[%v] "+
			"task type[%v], mpRepairChan has been full", c.Name, mp.volName, mp.PartitionID, rType))
	}
	return
}

func (c *Cluster) sendRepairDataPartitionTask(dp *DataPartition, rType RepairType) (err error) {
	var repairTask *RepairTask
	repairTask = &RepairTask{
		RType: rType,
		Pid:   dp.PartitionID,
	}
	select {
	case c.dpRepairChan <- repairTask:
		Warn(c.Name, fmt.Sprintf("action[sendRepairDataPartitionTask] clusterID[%v] vol[%v] data partition[%v] "+
			"task type[%v]", c.Name, dp.VolName, dp.PartitionID, rType))
	default:
		Warn(c.Name, fmt.Sprintf("action[sendRepairDataPartitionTask] clusterID[%v] vol[%v] data partition[%v] "+
			"task type[%v], chanLength[%v], chanCapacity[%v], dpRepairChan has been full", c.Name, dp.VolName, dp.PartitionID, rType, len(c.dpRepairChan),
			cap(c.dpRepairChan)))
	}
	return
}

// send latest replica infos to DataNode
func (c *Cluster) syncDataPartitionReplicasToDataNode(dp *DataPartition) {
	wg := sync.WaitGroup{}
	for _, host := range dp.Hosts {
		wg.Add(1)
		go func(target string, dp *DataPartition) {
			defer wg.Done()
			var err error
			var targetDataNode *DataNode
			//addDataNodeTasks
			if targetDataNode, err = c.dataNode(target); err != nil {
				log.LogWarnf("syncDataPartitionReplicasToDataNode: get data node [address: %v] failed: %v", target, err)
				return
			}
			task := dp.createTaskToSyncDataPartitionReplicas(target)
			if _, err = targetDataNode.TaskManager.syncSendAdminTask(task); err != nil {
				log.LogWarnf("syncDataPartitionReplicasToDataNode: send task [address: %v, partitionID: %v] failed: %v",
					target, dp.PartitionID, err)
			}
			return
		}(host, dp)
	}
	wg.Wait()
}

func (c *Cluster) scheduleToMergeZoneNodeset() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.AutoMergeNodeSet {
					c.checkMergeZoneNodeset()
				}
			}
			time.Sleep(24 * time.Hour)
		}
	}()
}

func (c *Cluster) checkMergeZoneNodeset() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMergeZoneNodeset occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMergeZoneNodeset occurred panic")
		}
	}()
	zones := c.t.getAllZones()
	for _, zone := range zones {
		zone.mergeNodeSetForMetaNode(c)
		zone.mergeNodeSetForDataNode(c)
	}
}

func (c *Cluster) updateDataNodeBadDisks(allBadDisks []map[string][]string) {
	//map[string][]string, key: addr,value: diskPaths
	datanodeBadDisks := new(sync.Map)
	for _, dataNodeBadDisks := range allBadDisks {
		for addr, badDisks := range dataNodeBadDisks {
			diskPaths := make([]string, 0)
			for _, diskPath := range badDisks {
				if contains(diskPaths, diskPath) {
					continue
				}
				diskPaths = append(diskPaths, diskPath)
			}
			datanodeBadDisks.Store(addr, diskPaths)
		}
	}
	c.DataNodeBadDisks = datanodeBadDisks
}

func (c *Cluster) getDataNodeBadDisks() (allBadDisks []proto.DataNodeBadDisksView) {
	allBadDisks = make([]proto.DataNodeBadDisksView, 0)
	c.DataNodeBadDisks.Range(func(key, value interface{}) bool {
		addr, ok := key.(string)
		if !ok {
			return true
		}
		disks, ok := value.([]string)
		if !ok {
			return true
		}
		badDiskNode := proto.DataNodeBadDisksView{Addr: addr, BadDiskPath: disks}
		allBadDisks = append(allBadDisks, badDiskNode)
		return true
	})
	return
}

func (c *Cluster) handleDataNodeValidateCRCReport(dpCrcInfo *proto.DataPartitionExtentCrcInfo) {
	if dpCrcInfo == nil {
		return
	}
	warnMsg := new(strings.Builder)
	replicaCrcDetail := new(strings.Builder)
	for _, extentCrcInfo := range dpCrcInfo.ExtentCrcInfos {
		warnMsg.Reset()
		replicaCrcDetail.Reset()
		for crc, locAddrs := range extentCrcInfo.CrcLocAddrMap {
			replicaCrcDetail.WriteString(fmt.Sprintf(" crc:%v count:%v addr:%v,", crc, len(locAddrs), locAddrs))
		}
		warnMsg.WriteString(fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v File:%v ",
			c.Name, dpCrcInfo.PartitionID, extentCrcInfo.FileID))
		// the replicas are different from each other
		if extentCrcInfo.ExtentNum == len(extentCrcInfo.CrcLocAddrMap) {
			warnMsg.WriteString("crc different between all node.")
			warnMsg.WriteString(replicaCrcDetail.String())
			Warn(c.Name, warnMsg.String())
			continue
		}

		// most of the replicas are same
		var maxNumCrc uint64
		var maxNum int
		for crc, extentInfos := range extentCrcInfo.CrcLocAddrMap {
			if maxNum < len(extentInfos) {
				maxNum = len(extentInfos)
				maxNumCrc = crc
			}
		}
		for crc, locAddrs := range extentCrcInfo.CrcLocAddrMap {
			if crc != maxNumCrc {
				warnMsg.WriteString(fmt.Sprintf("badCrc On addr:%v detail:", locAddrs))
				warnMsg.WriteString(replicaCrcDetail.String())
				Warn(c.Name, warnMsg.String())
			}
		}
	}
}

func (c *Cluster) updateRegion(regionName string, regionType proto.RegionType) (err error) {
	region, err := c.t.getRegion(regionName)
	if err != nil {
		return
	}
	oldRegionType := region.RegionType

	region.RegionType = regionType
	if err = c.syncUpdateRegion(region); err != nil {
		region.RegionType = oldRegionType
		log.LogErrorf("action[updateRegion] region[%v] err[%v]", region.Name, err)
		return
	}
	return
}

func IsCrossRegionHATypeQuorum(crossRegionHAType proto.CrossRegionHAType) bool {
	return crossRegionHAType == proto.CrossRegionHATypeQuorum
}

func (c *Cluster) setZoneRegion(zoneName, newRegionName string) (err error) {
	var zone *Zone
	if zone, err = c.t.getZone(zoneName); err != nil {
		return
	}
	targetRegion, err := c.t.getRegion(newRegionName)
	if err != nil {
		return
	}
	c.t.regionMap.Range(func(_, value interface{}) bool {
		region, ok := value.(*Region)
		if !ok || region == nil {
			return true
		}
		if err = region.deleteZone(zoneName, c); err != nil {
			return false
		}
		return true
	})

	//oldRegionName := zone.regionName
	zone.regionName = newRegionName
	if err = targetRegion.addZone(zoneName, c); err != nil {
		zone.regionName = ""
		return
	}
	return
}

func (c *Cluster) chooseTargetMetaHostsForCreateQuorumMetaPartition(replicaNum, learnerReplicaNum int, zoneName string) (hosts []string, peers []proto.Peer,
	learners []proto.Learner, err error) {
	masterRegionZoneName, slaveRegionZoneName, err := c.getMasterAndSlaveRegionZoneName(zoneName)
	if err != nil {
		return
	}
	learners = make([]proto.Learner, 0)
	masterRegionHosts, masterRegionPeers, err := c.chooseTargetMetaHosts("", nil, nil,
		replicaNum, strings.Join(masterRegionZoneName, ","), true)
	if err != nil {
		err = fmt.Errorf("choose master region hosts failed, err:%v", err)
		return
	}
	hosts = append(hosts, masterRegionHosts...)
	peers = append(peers, masterRegionPeers...)

	slaveReplicaNum := learnerReplicaNum
	if slaveReplicaNum <= 0 {
		return
	}
	slaveRegionHosts, slaveRegionPeers, err := c.chooseTargetMetaHosts("", nil, nil,
		slaveReplicaNum, strings.Join(slaveRegionZoneName, ","), true)
	if err != nil {
		err = fmt.Errorf("choose slave region hosts failed, err:%v", err)
		return
	}
	hosts = append(hosts, slaveRegionHosts...)
	peers = append(peers, slaveRegionPeers...)
	for _, slaveRegionHost := range slaveRegionHosts {
		var metaNode *MetaNode
		if metaNode, err = c.metaNode(slaveRegionHost); err != nil {
			return
		}
		addLearner := proto.Learner{ID: metaNode.ID, Addr: slaveRegionHost, PmConfig: &proto.PromoteConfig{AutoProm: false, PromThreshold: 100}}
		learners = append(learners, addLearner)
	}
	return
}

func (c *Cluster) chooseTargetDataNodesForCreateQuorumDataPartition(replicaNum int, zoneName string) (hosts []string, peers []proto.Peer, err error) {
	masterRegionZoneName, slaveRegionZoneName, err := c.getMasterAndSlaveRegionZoneName(zoneName)
	if err != nil {
		return
	}
	masterRegionHosts, masterRegionPeers, err := c.chooseTargetDataNodes(nil, nil, nil,
		defaultQuorumDataPartitionMasterRegionCount, strings.Join(masterRegionZoneName, ","), true)
	if err != nil {
		err = fmt.Errorf("choose master region hosts failed, err:%v", err)
		return
	}
	hosts = append(hosts, masterRegionHosts...)
	peers = append(peers, masterRegionPeers...)

	slaveReplicaNum := replicaNum - defaultQuorumDataPartitionMasterRegionCount
	if slaveReplicaNum <= 0 {
		return
	}
	slaveRegionHosts, slaveRegionPeers, err := c.chooseTargetDataNodes(nil, nil, nil,
		slaveReplicaNum, convertSliceToVolZoneName(slaveRegionZoneName), true)
	if err != nil {
		err = fmt.Errorf("choose slave region hosts failed, err:%v", err)
		return
	}
	hosts = append(hosts, slaveRegionHosts...)
	peers = append(peers, slaveRegionPeers...)
	return
}

func (c *Cluster) getMasterAndSlaveRegionZoneName(volZoneName string) (masterRegionZoneName, slaveRegionZoneName []string, err error) {
	var (
		region *Region
	)
	masterRegionZoneName = make([]string, 0)
	slaveRegionZoneName = make([]string, 0)

	zoneList := strings.Split(volZoneName, ",")
	for _, zoneName := range zoneList {
		if region, err = c.getRegionOfZoneName(zoneName); err != nil {
			return
		}
		switch region.RegionType {
		case proto.MasterRegion:
			if !contains(masterRegionZoneName, zoneName) {
				masterRegionZoneName = append(masterRegionZoneName, zoneName)
			}
		case proto.SlaveRegion:
			if !contains(slaveRegionZoneName, zoneName) {
				slaveRegionZoneName = append(slaveRegionZoneName, zoneName)
			}
		default:
			err = fmt.Errorf("action[getMasterAndSlaveRegionZoneName] zoneName:%v, region:%v type:%v is wrong", zoneName, region, region.RegionType)
			return
		}
	}
	log.LogDebugf("action[getMasterAndSlaveRegionZoneName] volZoneName:%v masterRegionZoneName:%v slaveRegionZoneName:%v",
		volZoneName, masterRegionZoneName, slaveRegionZoneName)
	return
}

func (c *Cluster) getRegionOfZoneName(zoneName string) (region *Region, err error) {
	zone, err := c.t.getZone(zoneName)
	if err != nil {
		return
	}
	if region, err = c.t.getRegion(zone.regionName); err != nil {
		err = fmt.Errorf("action[getRegionOfZoneName] zone:%v region:%v err:%v", zoneName, zone.regionName, err)
		return
	}
	return
}

func (c *Cluster) chooseTargetDataNodesFromSameRegionTypeOfOfflineReplica(offlineReplicaRegionName, volZoneName string, replicaNum int,
	excludeNodeSets []uint64, dpHosts []string) (hosts []string, peers []proto.Peer, err error) {
	var targetZoneNames string
	region, err := c.t.getRegion(offlineReplicaRegionName)
	if err != nil {
		return
	}
	masterRegionZoneName, slaveRegionZoneName, err := c.getMasterAndSlaveRegionZoneName(volZoneName)
	if err != nil {
		return
	}
	switch region.RegionType {
	case proto.MasterRegion:
		targetZoneNames = convertSliceToVolZoneName(masterRegionZoneName)
	case proto.SlaveRegion:
		targetZoneNames = convertSliceToVolZoneName(slaveRegionZoneName)
	default:
		err = fmt.Errorf("action[chooseTargetDataNodesFromSameRegionTypeOfOfflineReplica] offline replica region name:%v, region:%v type:%v is wrong",
			offlineReplicaRegionName, region.Name, region.RegionType)
		return
	}
	if hosts, peers, err = c.chooseTargetDataNodes(nil, excludeNodeSets, dpHosts, replicaNum, targetZoneNames, true); err != nil {
		return
	}
	return
}

func (c *Cluster) chooseTargetMetaNodesFromSameRegionTypeOfOfflineReplica(offlineReplicaRegionName, volZoneName string, replicaNum int,
	excludeNodeSets []uint64, oldHosts []string) (hosts []string, peers []proto.Peer, err error) {
	var targetZoneNames string
	region, err := c.t.getRegion(offlineReplicaRegionName)
	if err != nil {
		return
	}
	masterRegionZoneName, slaveRegionZoneName, err := c.getMasterAndSlaveRegionZoneName(volZoneName)
	if err != nil {
		return
	}
	switch region.RegionType {
	case proto.MasterRegion:
		targetZoneNames = convertSliceToVolZoneName(masterRegionZoneName)
	case proto.SlaveRegion:
		targetZoneNames = convertSliceToVolZoneName(slaveRegionZoneName)
	default:
		err = fmt.Errorf("action[chooseTargetMetaNodesFromSameRegionTypeOfOfflineReplica] offline replica region name:%v, region:%v type:%v is wrong",
			offlineReplicaRegionName, region.Name, region.RegionType)
		return
	}
	if hosts, peers, err = c.chooseTargetMetaHosts("", excludeNodeSets, oldHosts, replicaNum, targetZoneNames, true); err != nil {
		return
	}
	return
}

func convertSliceToVolZoneName(zoneNames []string) string {
	return strings.Join(zoneNames, commaSeparator)
}

func (c *Cluster) getDataNodeRegionType(dataNodeAddr string) (regionType proto.RegionType, err error) {
	dataNode, err := c.dataNode(dataNodeAddr)
	if err != nil {
		return
	}
	region, err := c.getRegionOfZoneName(dataNode.ZoneName)
	if err != nil {
		return
	}
	regionType = region.RegionType
	return
}

func (c *Cluster) getMetaNodeRegionType(metaNodeAddr string) (regionType proto.RegionType, err error) {
	metaNode, err := c.metaNode(metaNodeAddr)
	if err != nil {
		return
	}
	region, err := c.getRegionOfZoneName(metaNode.ZoneName)
	if err != nil {
		return
	}
	regionType = region.RegionType
	return
}

// it will not change the order of given addrs
func (c *Cluster) getMasterAndSlaveRegionAddrsFromDataNodeAddrs(dataNodeAddrs []string) (masterRegionAddrs, slaveRegionAddrs []string, err error) {
	masterRegionAddrs = make([]string, 0)
	slaveRegionAddrs = make([]string, 0)
	for _, dataNodeAddr := range dataNodeAddrs {
		hostRegionType, err1 := c.getDataNodeRegionType(dataNodeAddr)
		if err1 != nil {
			err = fmt.Errorf("action[getMasterAndSlaveRegionAddrsFromDataNodeAddrs] dataNodeAddr:%v err:%v", dataNodeAddr, err1)
			return
		}
		switch hostRegionType {
		case proto.MasterRegion:
			masterRegionAddrs = append(masterRegionAddrs, dataNodeAddr)
		case proto.SlaveRegion:
			slaveRegionAddrs = append(slaveRegionAddrs, dataNodeAddr)
		default:
			err = fmt.Errorf("action[getMasterAndSlaveRegionAddrsFromDataNodeAddrs] dataNodeAddr:%v, region type:%v is wrong", dataNodeAddr, hostRegionType)
			return
		}
	}
	return
}

// it will not change the order of given addrs
func (c *Cluster) getMasterAndSlaveRegionAddrsFromMetaNodeAddrs(metaNodeAddrs []string) (masterRegionAddrs, slaveRegionAddrs []string, err error) {
	masterRegionAddrs = make([]string, 0)
	slaveRegionAddrs = make([]string, 0)
	for _, metaNodeAddr := range metaNodeAddrs {
		hostRegionType, err1 := c.getMetaNodeRegionType(metaNodeAddr)
		if err1 != nil {
			err = fmt.Errorf("action[getMasterAndSlaveRegionAddrsFromMetaNodeAddrs] metaNodeAddr:%v err:%v", metaNodeAddr, err1)
			return
		}
		switch hostRegionType {
		case proto.MasterRegion:
			masterRegionAddrs = append(masterRegionAddrs, metaNodeAddr)
		case proto.SlaveRegion:
			slaveRegionAddrs = append(slaveRegionAddrs, metaNodeAddr)
		default:
			err = fmt.Errorf("action[getMasterAndSlaveRegionAddrsFromMetaNodeAddrs] metaNodeAddr:%v, region type:%v is wrong", metaNodeAddr, hostRegionType)
			return
		}
	}
	return
}

func isAutoChooseAddrForQuorumVol(addReplicaType proto.AddReplicaType) bool {
	return addReplicaType == proto.AutoChooseAddrForQuorumVol
}

func (c *Cluster) updateVolDataPartitionConvertMode(volName string, convertMode proto.ConvertMode) (err error) {
	var (
		vol            *Vol
		oldConvertMode proto.ConvertMode
	)
	if vol, err = c.getVol(volName); err != nil {
		return
	}
	oldConvertMode = vol.DPConvertMode

	vol.DPConvertMode = convertMode
	if err = c.syncUpdateVol(vol); err != nil {
		vol.DPConvertMode = oldConvertMode
		log.LogErrorf("action[updateVolDataPartitionConvertMode] vol[%v] err[%v]", volName, err)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) updateVolMetaPartitionConvertMode(volName string, convertMode proto.ConvertMode) (err error) {
	var (
		vol            *Vol
		oldConvertMode proto.ConvertMode
	)
	if vol, err = c.getVol(volName); err != nil {
		return
	}
	oldConvertMode = vol.MPConvertMode

	vol.MPConvertMode = convertMode
	if err = c.syncUpdateVol(vol); err != nil {
		vol.MPConvertMode = oldConvertMode
		log.LogErrorf("action[updateVolMetaPartitionConvertMode] vol[%v] err[%v]", volName, err)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) chooseTargetMetaNodeForCrossRegionQuorumVol(mp *MetaPartition) (addr string, totalReplicaNum int, err error) {
	hosts := make([]string, 0)
	var targetZoneNames string
	vol, err := c.getVol(mp.volName)
	if err != nil {
		return
	}
	if !IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
		err = fmt.Errorf("can only auto add replica for quorum vol,vol type:%s", vol.CrossRegionHAType)
		return
	}
	totalReplicaNum = int(vol.mpReplicaNum + vol.mpLearnerNum)
	if !mp.canAddReplicaForCrossRegionQuorumVol(totalReplicaNum) {
		err = fmt.Errorf("partition:%v is recovering or replica is full, "+
			"can not add replica for cross region quorum vol", mp.PartitionID)
		return
	}

	volMasterRegionZoneName, _, err := c.getMasterAndSlaveRegionZoneName(vol.zoneName)
	if err != nil {
		return
	}
	masterRegionHosts, _, err := c.getMasterAndSlaveRegionAddrsFromMetaNodeAddrs(mp.Hosts)
	if err != nil {
		return
	}
	if len(masterRegionHosts) < int(vol.mpReplicaNum) {
		targetZoneNames = convertSliceToVolZoneName(volMasterRegionZoneName)
		if len(volMasterRegionZoneName) > 1 {
			//if the vol need cross zone, mp master region replica must in different zone, so just choose a new zone
			curMasterRegionZonesMap := make(map[string]uint8)
			if curMasterRegionZonesMap, err = mp.getMetaMasterRegionZoneMap(c); err != nil {
				return
			}

			newMasterRegionZone := make([]string, 0)
			for _, masterRegionZone := range volMasterRegionZoneName {
				if _, ok := curMasterRegionZonesMap[masterRegionZone]; ok {
					continue
				}
				newMasterRegionZone = append(newMasterRegionZone, masterRegionZone)
			}
			if len(newMasterRegionZone) > 0 {
				targetZoneNames = convertSliceToVolZoneName(newMasterRegionZone)
			}
		}
	} else {
		err = fmt.Errorf("partition[%v] replicaNum[%v] masterRegionHosts:%v do not need add replica",
			mp.PartitionID, vol.mpReplicaNum, masterRegionHosts)
		return
	}
	if hosts, _, err = c.chooseTargetMetaHosts("", nil, mp.Hosts, 1, targetZoneNames, true); err != nil {
		return
	}
	addr = hosts[0]
	return
}

func (c *Cluster) chooseTargetMetaNodeForCrossRegionQuorumVolOfLearnerReplica(mp *MetaPartition) (addr string, totalReplicaNum int, err error) {
	hosts := make([]string, 0)
	var targetZoneNames string
	vol, err := c.getVol(mp.volName)
	if err != nil {
		return
	}
	if !IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
		err = fmt.Errorf("can only auto add replica for quorum vol,vol type:%s", vol.CrossRegionHAType)
		return
	}
	totalReplicaNum = int(vol.mpReplicaNum + vol.mpLearnerNum)
	if !mp.canAddReplicaForCrossRegionQuorumVol(totalReplicaNum) {
		err = fmt.Errorf("partition:%v is recovering or replica is full, "+
			"can not add replica for cross region quorum vol", mp.PartitionID)
		return
	}

	_, volSlaveRegionZoneName, err := c.getMasterAndSlaveRegionZoneName(vol.zoneName)
	if err != nil {
		return
	}
	_, slaveRegionHosts, err := c.getMasterAndSlaveRegionAddrsFromMetaNodeAddrs(mp.Hosts)
	if err != nil {
		return
	}
	learnerReplicaNum := int(vol.mpLearnerNum)
	if len(slaveRegionHosts) < learnerReplicaNum {
		targetZoneNames = convertSliceToVolZoneName(volSlaveRegionZoneName)
	} else {
		err = fmt.Errorf("partition[%v] learnerReplicaNum[%v] slaveRegionHosts:%v do not need add replica",
			mp.PartitionID, learnerReplicaNum, slaveRegionHosts)
		return
	}
	if hosts, _, err = c.chooseTargetMetaHosts("", nil, mp.Hosts, 1, targetZoneNames, true); err != nil {
		return
	}
	addr = hosts[0]
	return
}

func (c *Cluster) updateVolMinWritableMPAndDPNum(volName string, minRwMPNum, minRwDPNum int) (err error) {
	var (
		vol                 *Vol
		oldMinWritableMPNum int
		oldMinWritableDPNum int
		maxWritableDPNum    int
	)
	if vol, err = c.getVol(volName); err != nil {
		return
	}
	if vol.Capacity < defaultLowCapacityVol {
		maxWritableDPNum = defaultLowCapacityVolMaxWritableDPNum
	} else {
		maxWritableDPNum = defaultVolMaxWritableDPNum
	}
	if minRwMPNum > defaultVolMaxWritableMPNum || minRwDPNum > maxWritableDPNum {
		return fmt.Errorf("vol minRwMPNum(%v) or minRwDPNum(%v) big than maxWritableMPNum(%v) or maxWritableDPNum(%v)",
			minRwMPNum, minRwDPNum, defaultVolMaxWritableMPNum, maxWritableDPNum)
	}
	oldMinWritableMPNum = vol.MinWritableMPNum
	oldMinWritableDPNum = vol.MinWritableDPNum

	vol.MinWritableMPNum = minRwMPNum
	vol.MinWritableDPNum = minRwDPNum
	if err = c.syncUpdateVol(vol); err != nil {
		vol.MinWritableMPNum = oldMinWritableMPNum
		vol.MinWritableDPNum = oldMinWritableDPNum
		log.LogErrorf("action[updateVolMinWritableMPAndDPNum] vol[%v] err[%v]", volName, err)
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}
