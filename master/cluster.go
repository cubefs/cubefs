// Copyright 2018 The ChuBao Authors.
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

type Cluster struct {
	Name          string
	vols          map[string]*Vol
	dataNodes     sync.Map
	metaNodes     sync.Map
	createDpLock  sync.Mutex
	volsLock      sync.RWMutex
	leaderInfo    *LeaderInfo
	cfg           *ClusterConfig
	fsm           *MetadataFsm
	partition     raftstore.Partition
	retainLogs    uint64
	idAlloc       *IDAllocator
	t             *Topology
	compactStatus bool
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.vols = make(map[string]*Vol, 0)
	c.cfg = NewClusterConfig()
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
	c.t = NewTopology()
	c.startCheckDataPartitions()
	c.startCheckBackendLoadDataPartitions()
	c.startCheckReleaseDataPartitions()
	c.startCheckHeartbeat()
	c.startCheckMetaPartitions()
	c.startCheckAvailSpace()
	c.startCheckVols()
	return
}

func (c *Cluster) getMasterAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) startCheckAvailSpace() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkAvailSpace()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()

}

func (c *Cluster) checkAvailSpace() {
	c.checkDataNodeAvailSpace()
	c.checkVolAvailSpace()
}

func (c *Cluster) checkVolAvailSpace() {
	vols := c.copyVols()
	for _, vol := range vols {
		used, total := vol.statSpace()
		if total <= 0 {
			continue
		}
		useRate := float64(used) / float64(total)
		if useRate > SpaceAvailRate {
			Warn(c.Name, fmt.Sprintf("clusterId[%v] vol[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please allocate dataPartition",
				c.Name, vol.Name, useRate, used, total))
		}
	}
}

func (c *Cluster) checkDataNodeAvailSpace() {
	var (
		total uint64
		used  uint64
	)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		total = total + dataNode.Total
		used = used + dataNode.Used
		return true
	})
	if total <= 0 {
		return
	}
	useRate := float64(used) / float64(total)
	if useRate > SpaceAvailRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add dataNode",
			c.Name, useRate, used, total))
	}
}

func (c *Cluster) startCheckVols() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkVols()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) startCheckDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkDataPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) checkVols() {
	vols := c.copyVols()
	for _, vol := range vols {
		vol.checkStatus(c)
	}
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
			if c.partition.IsLeader() {
				c.backendLoadDataPartitions()
			}
			time.Sleep(time.Second)
		}
	}()
}

func (c *Cluster) backendLoadDataPartitions() {
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		vol.LoadDataPartition(c)
	}
}

func (c *Cluster) startCheckReleaseDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.releaseDataPartitionAfterLoad()
			}
			time.Sleep(time.Second * DefaultReleaseDataPartitionInternalSeconds)
		}
	}()
}

func (c *Cluster) releaseDataPartitionAfterLoad() {
	vols := c.copyVols()
	for _, vol := range vols {
		vol.ReleaseDataPartitionsAfterLoad(c.cfg.everyReleaseDataPartitionCount, c.cfg.releaseDataPartitionAfterLoadSeconds)
	}
}

func (c *Cluster) startCheckHeartbeat() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkLeaderAddr()
				c.checkDataNodeHeartbeat()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()

	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkMetaNodeHeartbeat()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()
}

func (c *Cluster) checkLeaderAddr() {
	leaderId, _ := c.partition.LeaderTerm()
	c.leaderInfo.addr = AddrDatabase[leaderId]
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
			if c.partition.IsLeader() {
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
	var (
		metaNode *MetaNode
	)
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		return metaNode.ID, nil
	}
	metaNode = NewMetaNode(nodeAddr, c.Name)

	if id, err = c.idAlloc.allocateMetaNodeID(); err != nil {
		goto errDeal
	}
	metaNode.ID = id
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errDeal
	}
	c.metaNodes.Store(nodeAddr, metaNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] metaNodeAddr:%v err:%v ",
		c.Name, nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) addDataNode(nodeAddr string) (err error) {
	var dataNode *DataNode
	if _, ok := c.dataNodes.Load(nodeAddr); ok {
		return
	}

	dataNode = NewDataNode(nodeAddr, c.Name)
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errDeal
	}
	c.dataNodes.Store(nodeAddr, dataNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] dataNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return err
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
		err = errors.Annotatef(VolNotFound, "%v not found", volName)
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
	vol.Status = VolMarkDelete
	if err = c.syncUpdateVol(vol); err != nil {
		vol.Status = VolNormal
		return
	}
	return
}

func (c *Cluster) createDataPartition(volName, partitionType string) (dp *DataPartition, err error) {
	var (
		vol         *Vol
		partitionID uint64
		tasks       []*proto.AdminTask
		targetHosts []string
	)
	c.createDpLock.Lock()
	defer c.createDpLock.Unlock()
	if vol, err = c.getVol(volName); err != nil {
		goto errDeal
	}
	if targetHosts, err = c.ChooseTargetDataHosts(int(vol.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errDeal
	}
	dp = newDataPartition(partitionID, vol.dpReplicaNum, partitionType, volName)
	dp.PersistenceHosts = targetHosts
	if err = c.syncAddDataPartition(volName, dp); err != nil {
		goto errDeal
	}
	tasks = dp.GenerateCreateTasks()
	c.putDataNodeTasks(tasks)
	vol.dataPartitions.putDataPartition(dp)

	return
errDeal:
	err = fmt.Errorf("action[createDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, volName, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) ChooseTargetDataHosts(replicaNum int) (hosts []string, err error) {
	var (
		masterAddr []string
		addrs      []string
		racks      []*Rack
		rack       *Rack
	)
	hosts = make([]string, 0)
	if c.t.isSingleRack() {
		var newHosts []string
		if rack, err = c.t.getRack(c.t.racks[0]); err != nil {
			return nil, errors.Trace(err)
		}
		if newHosts, err = rack.getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = newHosts
		return
	}

	if racks, err = c.t.allocRacks(replicaNum, nil); err != nil {
		return nil, errors.Trace(err)
	}

	if len(racks) == 2 {
		masterRack := racks[0]
		slaveRack := racks[1]
		masterReplicaNum := replicaNum/2 + 1
		slaveReplicaNum := replicaNum - masterReplicaNum
		if masterAddr, err = masterRack.getAvailDataNodeHosts(hosts, masterReplicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = append(hosts, masterAddr...)
		if addrs, err = slaveRack.getAvailDataNodeHosts(hosts, slaveReplicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = append(hosts, addrs...)
	} else if len(racks) == replicaNum {
		for index := 0; index < replicaNum; index++ {
			rack := racks[index]
			if addrs, err = rack.getAvailDataNodeHosts(hosts, 1); err != nil {
				return nil, errors.Trace(err)
			}
			hosts = append(hosts, addrs...)
		}
	}
	if len(hosts) != replicaNum {
		return nil, NoAnyDataNodeForCreateDataPartition
	}
	return
}

func (c *Cluster) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		err = errors.Annotatef(DataNodeNotFound, "%v not found", addr)
		return
	}
	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) getMetaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		err = errors.Annotatef(MetaNodeNotFound, "%v not found", addr)
		return
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) dataNodeOffLine(dataNode *DataNode) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.Addr)
	log.LogWarn(msg)

	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.dataPartitions {
			c.dataPartitionOffline(dataNode.Addr, vol.Name, dp, DataNodeOfflineInfo)
		}
	}
	if err := c.syncDeleteDataNode(dataNode); err != nil {
		msg = fmt.Sprintf("action[dataNodeOffLine],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, dataNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delDataNodeFromCache(dataNode)
	msg = fmt.Sprintf("action[dataNodeOffLine],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
}

func (c *Cluster) delDataNodeFromCache(dataNode *DataNode) {
	c.dataNodes.Delete(dataNode.Addr)
	go dataNode.clean()
}

func (c *Cluster) dataPartitionOffline(offlineAddr, volName string, dp *DataPartition, errMsg string) {
	var (
		newHosts []string
		newAddr  string
		msg      string
		tasks    []*proto.AdminTask
		task     *proto.AdminTask
		err      error
		dataNode *DataNode
		rack     *Rack
		vol      *Vol
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
	dp.generatorOffLineLog(offlineAddr)

	if dataNode, err = c.getDataNode(offlineAddr); err != nil {
		goto errDeal
	}

	if dataNode.RackName == "" {
		return
	}
	if rack, err = c.t.getRack(dataNode.RackName); err != nil {
		goto errDeal
	}
	if newHosts, err = rack.getAvailDataNodeHosts(dp.PersistenceHosts, 1); err != nil {
		goto errDeal
	}
	newAddr = newHosts[0]
	if err = dp.updateForOffline(offlineAddr, newAddr, volName, c); err != nil {
		goto errDeal
	}
	dp.offLineInMem(offlineAddr)
	dp.checkAndRemoveMissReplica(offlineAddr)

	task = dp.GenerateDeleteTask(offlineAddr)
	tasks = make([]*proto.AdminTask, 0)
	tasks = append(tasks, task)
	c.putDataNodeTasks(tasks)
	goto errDeal
errDeal:
	msg = fmt.Sprintf(errMsg+" clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, offlineAddr, newAddr, err, dp.PersistenceHosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	log.LogWarn(msg)
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
	go metaNode.clean()
}

func (c *Cluster) createVol(name, volType string, replicaNum uint8) (err error) {
	var vol *Vol
	if vol, err = c.createVolInternal(name, volType, replicaNum); err != nil {
		goto errDeal
	}

	if vol.VolType == proto.TinyPartition {
		return
	}

	if err = c.CreateMetaPartition(name, 0, DefaultMaxMetaPartitionInodeID); err != nil {
		c.deleteVol(name)
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createVolInternal(name, volType string, replicaNum uint8) (vol *Vol, err error) {
	if _, err = c.getVol(name); err == nil {
		err = hasExist(name)
		goto errDeal
	}
	vol = NewVol(name, volType, replicaNum)
	if err = c.syncAddVol(vol); err != nil {
		goto errDeal
	}
	c.putVol(vol)
	return
errDeal:
	err = fmt.Errorf("action[createVolInternal], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) CreateMetaPartitionForManual(volName string, start uint64) (err error) {

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
	partition.UpdateEnd(c, start)
	return
}

func (c *Cluster) CreateMetaPartition(volName string, start, end uint64) (err error) {
	var (
		vol         *Vol
		mp          *MetaPartition
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
	)
	if vol, err = c.getVol(volName); err != nil {
		return errors.Annotatef(err, "get vol [%v] err", volName)
	}

	if hosts, peers, err = c.ChooseTargetMetaHosts(int(vol.mpReplicaNum)); err != nil {
		return errors.Trace(err)
	}
	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return errors.Trace(err)
	}
	mp = NewMetaPartition(partitionID, start, end, vol.mpReplicaNum, volName)
	mp.setPersistenceHosts(hosts)
	mp.setPeers(peers)
	if err = c.syncAddMetaPartition(volName, mp); err != nil {
		return errors.Trace(err)
	}
	vol.AddMetaPartition(mp)
	c.putMetaNodeTasks(mp.generateCreateMetaPartitionTasks(nil, mp.Peers, volName))
	return
}

func (c *Cluster) hasEnoughWritableMetaHosts(replicaNum int) bool {
	maxTotal := c.GetMetaNodeMaxTotal()
	excludeHosts := make([]string, 0)
	nodeTabs, _ := c.GetAvailCarryMetaNodeTab(maxTotal, excludeHosts)
	if nodeTabs != nil && len(nodeTabs) >= replicaNum {
		return true
	}
	return false
}

func (c *Cluster) ChooseTargetMetaHosts(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr []string
		slaveAddrs []string
		masterPeer []proto.Peer
		slavePeers []proto.Peer
	)
	hosts = make([]string, 0)
	if masterAddr, masterPeer, err = c.getAvailMetaNodeHosts(hosts, 1); err != nil {
		return nil, nil, errors.Trace(err)
	}
	peers = append(peers, masterPeer...)
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	if slaveAddrs, slavePeers, err = c.getAvailMetaNodeHosts(hosts, otherReplica); err != nil {
		return nil, nil, errors.Trace(err)
	}
	hosts = append(hosts, slaveAddrs...)
	peers = append(peers, slavePeers...)
	if len(hosts) != replicaNum {
		return nil, nil, NoAnyMetaNodeForCreateMetaPartition
	}
	return
}

func (c *Cluster) DataNodeCount() (len int) {

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
		dataNodes = append(dataNodes, DataNodeView{Addr: dataNode.Addr, Status: dataNode.isActive})
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
		if dataNode.isActive && time.Since(dataNode.ReportTime) < time.Second*time.Duration(2*DefaultCheckHeartbeatIntervalSeconds) {
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
		if metaNode.IsActive && time.Since(metaNode.ReportTime) < time.Second*time.Duration(2*DefaultCheckHeartbeatIntervalSeconds) {
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
		if vol.Status == VolNormal {
			vols[name] = vol
		}
	}
	return
}

func (c *Cluster) getDataPartitionCapacity(vol *Vol) (count int) {
	var totalCount uint64
	c.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		totalCount = totalCount + dataNode.Total/util.DefaultDataPartitionSize
		return true
	})
	count = int(totalCount / uint64(vol.dpReplicaNum))
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
