// Copyright 2018 The Containerfs Authors.
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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"sync"
	"time"
)

//Cluster 集群管理
type Cluster struct {
	Name                string
	vols                map[string]*Vol
	dataNodes           sync.Map
	metaNodes           sync.Map
	createDpLock        sync.Mutex
	volsLock            sync.RWMutex
	mnLock              sync.RWMutex
	dnLock              sync.RWMutex
	leaderInfo          *LeaderInfo
	cfg                 *clusterConfig
	retainLogs          uint64
	idAlloc             *IDAllocator
	t                   *topology
	compactStatus       bool
	dataNodeSpace       *dataNodeSpaceStat
	metaNodeSpace       *metaNodeSpaceStat
	volSpaceStat        sync.Map
	BadDataPartitionIds *sync.Map
	DisableAutoAlloc    bool
	fsm                 *MetadataFsm
	partition           raftstore.Partition
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition, cfg *clusterConfig) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.vols = make(map[string]*Vol, 0)
	c.cfg = cfg
	c.t = newTopology()
	c.BadDataPartitionIds = new(sync.Map)
	c.dataNodeSpace = new(dataNodeSpaceStat)
	c.metaNodeSpace = new(metaNodeSpaceStat)
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)

	return
}

func (c *Cluster) scheduleTask() {
	c.startCheckDataPartitions()
	c.startCheckBackendLoadDataPartitions()
	c.startCheckReleaseDataPartitions()
	c.startCheckHeartbeat()
	c.startCheckMetaPartitions()
	c.startCheckAvailSpace()
	c.startCheckCreateDataPartitions()
	c.startCheckVolStatus()
	c.startCheckBadDiskRecovery()
}

func (c *Cluster) getMasterAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) startCheckAvailSpace() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.checkAvailSpace()
			}
			time.Sleep(time.Second * defaultCheckHeartbeatIntervalSeconds)
		}
	}()

}

func (c *Cluster) startCheckCreateDataPartitions() {
	go func() {
		//check vols after switching leader two minutes
		time.Sleep(2 * time.Minute)
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.checkCreateDataPartitions()
			}
			time.Sleep(2 * time.Minute)
		}
	}()
}

func (c *Cluster) startCheckDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.checkDataPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) checkCreateDataPartitions() {
	vols := c.copyVols()
	for _, vol := range vols {
		vol.checkNeedAutoCreateDataPartitions(c)
	}
}

func (c *Cluster) startCheckVolStatus() {
	go func() {
		//check vols after switching leader two minutes
		for {
			if c.partition.IsLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					vol.checkStatus(c)
				}
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) checkDataPartitions() {
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		readWrites := vol.checkDataPartitions(c)
		vol.dataPartitions.setReadWriteDataPartitions(readWrites, c.Name)
		vol.dataPartitions.updateDataPartitionResponseCache(true, 0)
		msg := fmt.Sprintf("action[checkDataPartitions],vol[%v] can readWrite dataPartitions:%v  ", vol.Name, vol.dataPartitions.readWriteDataPartitions)
		log.LogInfo(msg)
	}
}

func (c *Cluster) startCheckBackendLoadDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.backendLoadDataPartitions()
			}
			time.Sleep(time.Second)
		}
	}()
}

func (c *Cluster) backendLoadDataPartitions() {
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		vol.loadDataPartition(c)
	}
}

func (c *Cluster) startCheckReleaseDataPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.releaseDataPartitionAfterLoad()
			}
			time.Sleep(time.Second * defaultReleaseDataPartitionInternalSeconds)
		}
	}()
}

func (c *Cluster) releaseDataPartitionAfterLoad() {
	vols := c.copyVols()
	for _, vol := range vols {
		vol.releaseDataPartitions(c.cfg.everyReleaseDataPartitionCount, c.cfg.releaseDataPartitionAfterLoadSeconds)
	}
}

func (c *Cluster) startCheckHeartbeat() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.checkLeaderAddr()
				c.checkDataNodeHeartbeat()
			}
			time.Sleep(time.Second * defaultCheckHeartbeatIntervalSeconds)
		}
	}()

	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.checkMetaNodeHeartbeat()
			}
			time.Sleep(time.Second * defaultCheckHeartbeatIntervalSeconds)
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
		node.checkHeartBeat()
		task := node.generateHeartbeatTask(c.getMasterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.putDataNodeTasks(tasks)
}

func (c *Cluster) checkMetaNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.metaNodes.Range(func(addr, metaNode interface{}) bool {
		node := metaNode.(*MetaNode)
		node.checkHeartbeat()
		task := node.generateHeartbeatTask(c.getMasterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.putMetaNodeTasks(tasks)
}

func (c *Cluster) startCheckMetaPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				c.checkMetaPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) checkMetaPartitions() {
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		vol.checkMetaPartitions(c)
	}
}

func (c *Cluster) addMetaNode(nodeAddr string) (id uint64, err error) {
	c.mnLock.Lock()
	defer c.mnLock.Unlock()
	var metaNode *MetaNode
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		return metaNode.ID, nil
	}
	metaNode = newMetaNode(nodeAddr, c.Name)
	ns := c.t.getAvailNodeSetForMetaNode()
	if ns == nil {
		if ns, err = c.createNodeSet(); err != nil {
			goto errDeal
		}
	}
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errDeal
	}
	metaNode.ID = id
	metaNode.NodeSetID = ns.ID
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errDeal
	}
	ns.increaseMetaNodeLen()
	if err = c.syncUpdateNodeSet(ns); err != nil {
		ns.decreaseMetaNodeLen()
		goto errDeal
	}
	c.metaNodes.Store(nodeAddr, metaNode)
	log.LogInfof("action[addMetaNode],clusterID[%v] metaNodeAddr:%v,nodeSetId[%v],dLen[%v],mLen[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.dataNodeLen, ns.metaNodeLen, ns.Capacity)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] metaNodeAddr:%v err:%v ",
		c.Name, nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createNodeSet() (ns *nodeSet, err error) {
	var id uint64
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		return
	}
	ns = newNodeSet(id, c.cfg.nodeSetCapacity)
	if err = c.syncAddNodeSet(ns); err != nil {
		return
	}
	c.t.putNodeSet(ns)
	return
}

func (c *Cluster) addDataNode(nodeAddr string) (id uint64, err error) {
	c.dnLock.Lock()
	defer c.dnLock.Unlock()
	var dataNode *DataNode
	if node, ok := c.dataNodes.Load(nodeAddr); ok {
		dataNode = node.(*DataNode)
		return dataNode.ID, nil
	}

	dataNode = newDataNode(nodeAddr, c.Name)
	ns := c.t.getAvailNodeSetForDataNode()
	if ns == nil {
		if ns, err = c.createNodeSet(); err != nil {
			goto errDeal
		}
	}
	//allocate dataNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errDeal
	}
	dataNode.ID = id
	dataNode.NodeSetID = ns.ID
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errDeal
	}
	ns.increaseDataNodeLen()
	if err = c.syncUpdateNodeSet(ns); err != nil {
		ns.decreaseDataNodeLen()
		goto errDeal
	}
	c.dataNodes.Store(nodeAddr, dataNode)
	log.LogInfof("action[addDataNode],clusterID[%v] dataNodeAddr:%v,nodeSetId[%v],dLen[%v],mLen[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.dataNodeLen, ns.metaNodeLen, ns.Capacity)
	return
errDeal:
	err = fmt.Errorf("action[addDataNode],clusterID[%v] dataNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
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
		if mp, err = vol.getMetaPartition(id); err == nil {
			return
		}
	}
	err = metaPartitionNotFound(id)
	return
}

func (c *Cluster) putVol(vol *Vol) {
	c.volsLock.Lock()
	defer c.volsLock.Unlock()
	if _, ok := c.vols[vol.Name]; !ok {
		c.vols[vol.Name] = vol
	}
}

func (c *Cluster) getVol(volName string) (vol *Vol, err error) {
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	vol, ok := c.vols[volName]
	if !ok {
		err = errors.Annotatef(volNotFound(volName), "%v not found", volName)
	}
	return
}

func (c *Cluster) deleteVol(name string) {
	c.volsLock.Lock()
	defer c.volsLock.Unlock()
	delete(c.vols, name)
	return
}

func (c *Cluster) markDeleteVol(name string) (err error) {
	var vol *Vol
	if vol, err = c.getVol(name); err != nil {
		return
	}
	vol.Status = volMarkDelete
	if err = c.syncUpdateVol(vol); err != nil {
		vol.Status = volNormal
		return
	}
	return
}

func (c *Cluster) createDataPartition(volName string) (dp *DataPartition, err error) {
	var (
		vol         *Vol
		partitionID uint64
		targetHosts []string
		targetPeers []proto.Peer
		wg          sync.WaitGroup
	)
	c.createDpLock.Lock()
	defer c.createDpLock.Unlock()
	if vol, err = c.getVol(volName); err != nil {
		return
	}
	errChannel := make(chan error, vol.dpReplicaNum)
	if targetHosts, targetPeers, err = c.chooseTargetDataHosts(int(vol.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errDeal
	}
	dp = newDataPartition(partitionID, vol.dpReplicaNum, volName, vol.ID, vol.RandomWrite)
	dp.PersistenceHosts = targetHosts
	dp.Peers = targetPeers
	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			if err = c.syncCreateDataPartitionToDataNode(host, vol.dataPartitionSize, dp); err != nil {
				errChannel <- err
				return
			}
			dp.Lock()
			defer dp.Unlock()
			if err = dp.createDataPartitionSuccessTriggerOperator(host, c); err != nil {
				errChannel <- err
			}
		}(host)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		goto errDeal
	default:
		dp.Status = proto.ReadWrite
	}
	if err = c.syncAddDataPartition(dp); err != nil {
		goto errDeal
	}
	vol.dataPartitions.putDataPartition(dp)
	log.LogInfof("action[createDataPartition] success,volName[%v],partitionId[%v]", volName, partitionID)
	return
errDeal:
	err = fmt.Errorf("action[createDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, volName, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) syncCreateDataPartitionToDataNode(host string, size uint64, dp *DataPartition) (err error) {
	task := dp.generateCreateTask(host, size)
	dataNode, err := c.getDataNode(host)
	if err != nil {
		return
	}
	conn, err := dataNode.Sender.connPool.GetConnect(dataNode.Addr)
	if err != nil {
		return
	}
	if err = dataNode.Sender.syncCreatePartition(task, conn); err != nil {
		return
	}
	dataNode.Sender.connPool.PutConnect(conn, false)
	return
}

func (c *Cluster) syncCreateMetaPartitionToMetaNode(host string, mp *MetaPartition) (err error) {
	hosts := make([]string, 0)
	hosts = append(hosts, host)
	tasks := mp.generateCreateMetaPartitionTasks(hosts, mp.Peers, mp.volName)
	metaNode, err := c.getMetaNode(host)
	if err != nil {
		return
	}
	conn, err := metaNode.Sender.connPool.GetConnect(metaNode.Addr)
	if err != nil {
		return
	}
	if err = metaNode.Sender.syncCreatePartition(tasks[0], conn); err != nil {
		return
	}
	metaNode.Sender.connPool.PutConnect(conn, false)
	return
}

func (c *Cluster) chooseTargetDataHosts(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr  []string
		addrs       []string
		racks       []*Rack
		rack        *Rack
		masterPeers []proto.Peer
		slavePeers  []proto.Peer
	)
	hosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	ns, err := c.t.allocNodeSetForDataNode(uint8(replicaNum))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if ns.isSingleRack() {
		var newHosts []string
		if rack, err = ns.getRack(ns.racks[0]); err != nil {
			return nil, nil, errors.Trace(err)
		}
		if newHosts, peers, err = rack.getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, nil, errors.Trace(err)
		}
		hosts = newHosts
		return
	}

	if racks, err = ns.allocRacks(replicaNum, nil); err != nil {
		return nil, nil, errors.Trace(err)
	}

	if len(racks) == 2 {
		masterRack := racks[0]
		slaveRack := racks[1]
		masterReplicaNum := replicaNum/2 + 1
		slaveReplicaNum := replicaNum - masterReplicaNum
		if masterAddr, masterPeers, err = masterRack.getAvailDataNodeHosts(hosts, masterReplicaNum); err != nil {
			return nil, nil, errors.Trace(err)
		}
		hosts = append(hosts, masterAddr...)
		peers = append(peers, masterPeers...)
		if addrs, slavePeers, err = slaveRack.getAvailDataNodeHosts(hosts, slaveReplicaNum); err != nil {
			return nil, nil, errors.Trace(err)
		}
		hosts = append(hosts, addrs...)
		peers = append(peers, slavePeers...)
	} else if len(racks) == replicaNum {
		for index := 0; index < replicaNum; index++ {
			rack := racks[index]
			var selectPeers []proto.Peer
			if addrs, selectPeers, err = rack.getAvailDataNodeHosts(hosts, 1); err != nil {
				return nil, nil, errors.Trace(err)
			}
			hosts = append(hosts, addrs...)
			peers = append(peers, selectPeers...)
		}
	}
	if len(hosts) != replicaNum {
		return nil, nil, errNoAnyDataNodeForCreateDataPartition
	}
	return
}

func (c *Cluster) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		err = errors.Annotatef(dataNodeNotFound(addr), "%v not found", addr)
		return
	}
	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) getMetaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		err = errors.Annotatef(metaNodeNotFound(addr), "%v not found", addr)
		return
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) dataNodeOffLine(dataNode *DataNode) (err error) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.Addr)
	log.LogWarn(msg)
	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.dataPartitions {
			if err = c.dataPartitionOffline(dataNode.Addr, vol.Name, dp, dataNodeOfflineInfo); err != nil {
				return
			}
		}
	}
	if err = c.syncDeleteDataNode(dataNode); err != nil {
		msg = fmt.Sprintf("action[dataNodeOffLine],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, dataNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delDataNodeFromCache(dataNode)
	msg = fmt.Sprintf("action[dataNodeOffLine],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) delDataNodeFromCache(dataNode *DataNode) {
	c.dataNodes.Delete(dataNode.Addr)
	c.t.deleteDataNode(dataNode)
	go dataNode.clean()
}

func (c *Cluster) dataPartitionOffline(offlineAddr, volName string, dp *DataPartition, errMsg string) (err error) {
	var (
		newHosts   []string
		newAddr    string
		newPeers   []proto.Peer
		msg        string
		tasks      []*proto.AdminTask
		task       *proto.AdminTask
		dataNode   *DataNode
		rack       *Rack
		vol        *Vol
		removePeer proto.Peer
	)
	dp.Lock()
	defer dp.Unlock()
	if ok := dp.isInPersistenceHosts(offlineAddr); !ok {
		return
	}

	if vol, err = c.getVol(volName); err != nil {
		goto errDeal
	}

	if err = dp.hasMissOne(int(vol.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if err = dp.canOffLine(offlineAddr); err != nil {
		goto errDeal
	}

	if dataNode, err = c.getDataNode(offlineAddr); err != nil {
		goto errDeal
	}

	if dataNode.RackName == "" {
		return
	}
	if rack, err = c.t.getRack(dataNode); err != nil {
		goto errDeal
	}
	if newHosts, newPeers, err = rack.getAvailDataNodeHosts(dp.PersistenceHosts, 1); err != nil {
		//select dataNode of other nodeSet
		if newHosts, newPeers, err = c.chooseTargetDataHosts(1); err != nil {
			goto errDeal
		}
	}
	newAddr = newHosts[0]
	for _, replica := range dp.Replicas {
		if replica.Addr == offlineAddr {
			removePeer = proto.Peer{ID: replica.dataNode.ID, Addr: replica.Addr}
		} else {
			newPeers = append(newPeers, proto.Peer{ID: replica.dataNode.ID, Addr: replica.Addr})
		}
	}

	if task, err = dp.generateOfflineTask(removePeer, newPeers[0]); err != nil {
		goto errDeal
	}
	dp.generatorOffLineLog(offlineAddr)

	if err = dp.updateForOffline(offlineAddr, newAddr, volName, newPeers, c); err != nil {
		goto errDeal
	}
	dp.offLineInMem(offlineAddr)
	dp.checkAndRemoveMissReplica(offlineAddr)
	tasks = make([]*proto.AdminTask, 0)
	tasks = append(tasks, task)
	c.putDataNodeTasks(tasks)
	if err = c.syncCreateDataPartitionToDataNode(newAddr, vol.dataPartitionSize, dp); err != nil {
		goto errDeal
	}
	if err = dp.createDataPartitionSuccessTriggerOperator(newAddr, c); err != nil {
		goto errDeal
	}
	dp.Status = proto.ReadOnly
	log.LogWarnf("clusterID[%v] partitionID:%v  on Node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, dp.PartitionID, offlineAddr, newAddr, dp.PersistenceHosts)
	return
errDeal:
	msg = fmt.Sprintf(errMsg+" clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, offlineAddr, newAddr, err, dp.PersistenceHosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) metaNodeOffLine(metaNode *MetaNode) {
	msg := fmt.Sprintf("action[metaNodeOffLine],clusterID[%v] Node[%v] OffLine", c.Name, metaNode.Addr)
	log.LogWarn(msg)

	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			c.metaPartitionOffline(vol.Name, metaNode.Addr, mp.PartitionID)
		}
	}
	if err := c.syncDeleteMetaNode(metaNode); err != nil {
		msg = fmt.Sprintf("action[metaNodeOffLine],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, metaNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delMetaNodeFromCache(metaNode)
	msg = fmt.Sprintf("action[metaNodeOffLine],clusterID[%v] Node[%v] OffLine success", c.Name, metaNode.Addr)
	Warn(c.Name, msg)
}

func (c *Cluster) delMetaNodeFromCache(metaNode *MetaNode) {
	c.metaNodes.Delete(metaNode.Addr)
	c.t.deleteMetaNode(metaNode)
	go metaNode.clean()
}

func (c *Cluster) updateVol(name string, capacity int) (err error) {
	var vol *Vol
	if vol, err = c.getVol(name); err != nil {
		goto errDeal
	}
	if uint64(capacity) < vol.Capacity {
		err = fmt.Errorf("capacity[%v] less than old capacity[%v]", capacity, vol.Capacity)
		goto errDeal
	}
	vol.setCapacity(uint64(capacity))
	if err = c.syncUpdateVol(vol); err != nil {
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[updateVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createVol(name string, replicaNum uint8, randomWrite bool, size, capacity int) (err error) {
	var (
		vol                     *Vol
		dataPartitionSize       uint64
		readWriteDataPartitions int
	)
	if size == 0 {
		dataPartitionSize = util.DefaultDataPartitionSize
	} else {
		dataPartitionSize = uint64(size) * util.GB
	}
	if err = c.createVolInternal(name, replicaNum, randomWrite, dataPartitionSize, uint64(capacity)); err != nil {
		goto errDeal
	}

	if vol, err = c.getVol(name); err != nil {
		goto errDeal
	}

	if err = c.createMetaPartition(name, 0, defaultMaxMetaPartitionInodeID); err != nil {
		vol.Status = volMarkDelete
		c.syncDeleteVol(vol)
		c.deleteVol(name)
		goto errDeal
	}
	for retryCount := 0; readWriteDataPartitions < defaultInitDataPartitions && retryCount < 3; retryCount++ {
		vol.initDataPartitions(c)
		readWriteDataPartitions = vol.checkDataPartitionStatus(c)
	}
	vol.dataPartitions.readWriteDataPartitions = readWriteDataPartitions
	log.LogInfof("action[createVol] vol[%v],readWriteDataPartitions[%v]", name, readWriteDataPartitions)
	return

	return
errDeal:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createVolInternal(name string, replicaNum uint8, randomWrite bool, dpSize, capacity uint64) (err error) {
	var (
		id  uint64
		vol *Vol
	)
	if _, err = c.getVol(name); err == nil {
		err = hasExist(name)
		goto errDeal
	}

	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errDeal
	}
	vol = newVol(id, name, replicaNum, randomWrite, dpSize, capacity)
	if err = c.syncAddVol(vol); err != nil {
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[createVolInternal], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createMetaPartitionForManual(volName string, start uint64) (err error) {

	var (
		maxPartitionID uint64
		vol            *Vol
		partition      *MetaPartition
	)

	if vol, err = c.getVol(volName); err != nil {
		return errors.Annotatef(err, "get vol [%v] err", volName)
	}
	maxPartitionID = vol.getMaxPartitionID()
	if partition, err = vol.getMetaPartition(maxPartitionID); err != nil {
		return errors.Annotatef(err, "get meta partition [%v] err", maxPartitionID)
	}
	if start < partition.MaxNodeID {
		err = errors.Errorf("next meta partition start must be larger than %v", partition.MaxNodeID)
		return
	}
	if _, err := partition.getLeaderMetaReplica(); err != nil {
		return errors.Annotate(err, "can't execute")
	}
	partition.Lock()
	defer partition.Unlock()
	partition.updateEnd(c, start)
	return
}

func (c *Cluster) createMetaPartition(volName string, start, end uint64) (err error) {
	var (
		vol         *Vol
		mp          *MetaPartition
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
		wg          sync.WaitGroup
	)
	if vol, err = c.getVol(volName); err != nil {
		log.LogWarnf("action[createMetaPartition] get vol [%v] err", volName)
		return
	}
	errChannel := make(chan error, vol.mpReplicaNum)

	if hosts, peers, err = c.chooseTargetMetaHosts(int(vol.mpReplicaNum)); err != nil {
		return errors.Trace(err)
	}
	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return errors.Trace(err)
	}
	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, volName, vol.ID)
	mp.setPersistenceHosts(hosts)
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
			if err = mp.createPartitionSuccessTriggerOperator(host, c); err != nil {
				errChannel <- err
			}
		}(host)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return errors.Trace(err)
	default:
		mp.Status = proto.ReadWrite
	}
	if err = c.syncAddMetaPartition(mp); err != nil {
		return errors.Trace(err)
	}
	vol.addMetaPartition(mp)
	log.LogInfof("action[createMetaPartition] success,volName[%v],partition[%v]", volName, partitionID)
	return
}

func (c *Cluster) hasEnoughWritableMetaHosts(replicaNum int, setID uint64) bool {
	ns, err := c.t.getNodeSet(setID)
	if err != nil {
		log.LogErrorf("nodeSet[%v] not exist", setID)
		return false
	}
	maxTotal := ns.getMetaNodeMaxTotal()
	excludeHosts := make([]string, 0)
	nodeTabs, _ := ns.getAvailCarryMetaNodeTab(maxTotal, excludeHosts)
	if nodeTabs != nil && len(nodeTabs) >= replicaNum {
		return true
	}
	return false
}

func (c *Cluster) chooseTargetMetaHosts(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr []string
		slaveAddrs []string
		masterPeer []proto.Peer
		slavePeers []proto.Peer
		ns         *nodeSet
	)
	if ns, err = c.t.allocNodeSetForMetaNode(uint8(replicaNum)); err != nil {
		return nil, nil, errors.Trace(err)
	}

	hosts = make([]string, 0)
	if masterAddr, masterPeer, err = ns.getAvailMetaNodeHosts(hosts, 1); err != nil {
		return nil, nil, errors.Trace(err)
	}
	peers = append(peers, masterPeer...)
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	if slaveAddrs, slavePeers, err = ns.getAvailMetaNodeHosts(hosts, otherReplica); err != nil {
		return nil, nil, errors.Trace(err)
	}
	hosts = append(hosts, slaveAddrs...)
	peers = append(peers, slavePeers...)
	if len(hosts) != replicaNum {
		return nil, nil, errNoAnyMetaNodeForCreateMetaPartition
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

func (c *Cluster) getAllDataNodes() (dataNodes []DataNodeView) {
	dataNodes = make([]DataNodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodes = append(dataNodes, DataNodeView{Addr: dataNode.Addr, Status: dataNode.isActive, ID: dataNode.ID})
		return true
	})
	return
}

func (c *Cluster) getLiveDataNodesRate() (rate float32) {
	dataNodes := make([]DataNodeView, 0)
	liveDataNodes := make([]DataNodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		view := DataNodeView{Addr: dataNode.Addr, Status: dataNode.isActive}
		dataNodes = append(dataNodes, view)
		if dataNode.isActive && time.Since(dataNode.ReportTime) < time.Second*time.Duration(2*defaultCheckHeartbeatIntervalSeconds) {
			liveDataNodes = append(liveDataNodes, view)
		}
		return true
	})
	return float32(len(liveDataNodes)) / float32(len(dataNodes))
}

func (c *Cluster) getLiveMetaNodesRate() (rate float32) {
	metaNodes := make([]MetaNodeView, 0)
	liveMetaNodes := make([]MetaNodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		view := MetaNodeView{Addr: metaNode.Addr, Status: metaNode.IsActive, ID: metaNode.ID}
		metaNodes = append(metaNodes, view)
		if metaNode.IsActive && time.Since(metaNode.ReportTime) < time.Second*time.Duration(2*defaultCheckHeartbeatIntervalSeconds) {
			liveMetaNodes = append(liveMetaNodes, view)
		}
		return true
	})
	return float32(len(liveMetaNodes)) / float32(len(metaNodes))
}

func (c *Cluster) getAllMetaNodes() (metaNodes []MetaNodeView) {
	metaNodes = make([]MetaNodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, MetaNodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive})
		return true
	})
	return
}

func (c *Cluster) getAllVols() (vols []string) {
	vols = make([]string, 0)
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for name := range c.vols {
		vols = append(vols, name)
	}
	return
}

func (c *Cluster) copyVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol, 0)
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for name, vol := range c.vols {
		vols[name] = vol
	}
	return
}

func (c *Cluster) getAllNormalVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol, 0)
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for name, vol := range c.vols {
		if vol.Status == volNormal {
			vols[name] = vol
		}
	}
	return
}

func (c *Cluster) getDataPartitionCapacity(vol *Vol) (count int) {
	var totalCount uint64
	c.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		totalCount = totalCount + dataNode.Total/vol.dataPartitionSize
		return true
	})
	count = int(totalCount / uint64(vol.dpReplicaNum))
	return
}

func (c *Cluster) getDataPartitionCount() (count int) {
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for _, vol := range c.vols {
		count = count + len(vol.dataPartitions.dataPartitions)
	}
	return
}

func (c *Cluster) syncCompactStatus(status bool) (err error) {
	oldCompactStatus := c.compactStatus
	c.compactStatus = status
	if err = c.syncPutCluster(); err != nil {
		c.compactStatus = oldCompactStatus
		log.LogErrorf("action[syncCompactStatus] failed,err:%v", err)
		return
	}
	return
}
