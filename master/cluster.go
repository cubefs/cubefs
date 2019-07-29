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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
	"time"
)

// Cluster stores all the cluster-level information.
type Cluster struct {
	Name                string
	vols                map[string]*Vol
	dataNodes           sync.Map
	metaNodes           sync.Map
	dpMutex             sync.Mutex   // data partition mutex
	volMutex            sync.RWMutex // volume mutex
	createVolMutex      sync.RWMutex // create volume mutex
	mnMutex             sync.RWMutex // meta node mutex
	dnMutex             sync.RWMutex // data node mutex
	leaderInfo          *LeaderInfo
	cfg                 *clusterConfig
	retainLogs          uint64
	idAlloc             *IDAllocator
	t                   *topology
	dataNodeStatInfo    *nodeStatInfo
	metaNodeStatInfo    *nodeStatInfo
	volStatInfo         sync.Map
	BadDataPartitionIds *sync.Map
	DisableAutoAllocate bool
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
	c.dataNodeStatInfo = new(nodeStatInfo)
	c.metaNodeStatInfo = new(nodeStatInfo)
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
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
	c.startCheckLoadMetaPartitions()
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
			time.Sleep(time.Second * defaultIntervalToCheckHeartbeat)
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

	vols := c.allVols()
	for _, vol := range vols {
		readWrites := vol.checkDataPartitions(c)
		vol.dataPartitions.setReadWriteDataPartitions(readWrites, c.Name)
		vol.dataPartitions.updateResponseCache(true, 0)
		msg := fmt.Sprintf("action[checkDataPartitions],vol[%v] can readWrite partitions:%v  ", vol.Name, vol.dataPartitions.readableAndWritableCnt)
		log.LogInfo(msg)
	}
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
		vol.checkMetaPartitions(c)
	}
}

func (c *Cluster) addMetaNode(nodeAddr string) (id uint64, err error) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	var metaNode *MetaNode
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		return metaNode.ID, nil
	}
	metaNode = newMetaNode(nodeAddr, c.Name)
	ns := c.t.getAvailNodeSetForMetaNode()
	if ns == nil {
		if ns, err = c.createNodeSet(); err != nil {
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
	ns.increaseMetaNodeLen()
	if err = c.syncUpdateNodeSet(ns); err != nil {
		ns.decreaseMetaNodeLen()
		goto errHandler
	}
	c.metaNodes.Store(nodeAddr, metaNode)
	log.LogInfof("action[addMetaNode],clusterID[%v] metaNodeAddr:%v,nodeSetId[%v],dLen[%v],mLen[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.dataNodeLen, ns.metaNodeLen, ns.Capacity)
	return
errHandler:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] metaNodeAddr:%v err:%v ",
		c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
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
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	var dataNode *DataNode
	if node, ok := c.dataNodes.Load(nodeAddr); ok {
		dataNode = node.(*DataNode)
		return dataNode.ID, nil
	}

	dataNode = newDataNode(nodeAddr, c.Name)
	ns := c.t.getAvailNodeSetForDataNode()
	if ns == nil {
		if ns, err = c.createNodeSet(); err != nil {
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
	ns.increaseDataNodeLen()
	if err = c.syncUpdateNodeSet(ns); err != nil {
		ns.decreaseDataNodeLen()
		goto errHandler
	}
	c.dataNodes.Store(nodeAddr, dataNode)
	log.LogInfof("action[addDataNode],clusterID[%v] dataNodeAddr:%v,nodeSetId[%v],dLen[%v],mLen[%v],capacity[%v]",
		c.Name, nodeAddr, ns.ID, ns.dataNodeLen, ns.metaNodeLen, ns.Capacity)
	return
errHandler:
	err = fmt.Errorf("action[addDataNode],clusterID[%v] dataNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
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
	errChannel := make(chan error, vol.dpReplicaNum)
	if targetHosts, targetPeers, err = c.chooseTargetDataNodes(int(vol.dpReplicaNum)); err != nil {
		goto errHandler
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
			if diskPath, err = c.syncCreateDataPartitionToDataNode(host, vol.dataPartitionSize, dp, dp.Peers, dp.Hosts); err != nil {
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
		dp.Status = proto.ReadWrite
	}
	if err = c.syncAddDataPartition(dp); err != nil {
		goto errHandler
	}
	vol.dataPartitions.put(dp)
	log.LogInfof("action[createDataPartition] success,volName[%v],partitionId[%v]", volName, partitionID)
	return
errHandler:
	err = fmt.Errorf("action[createDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, volName, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) syncCreateDataPartitionToDataNode(host string, size uint64, dp *DataPartition, peers []proto.Peer, hosts []string) (diskPath string, err error) {
	task := dp.createTaskToCreateDataPartition(host, size, peers, hosts)
	dataNode, err := c.dataNode(host)
	if err != nil {
		return
	}
	var replicaDiskPath []byte
	if replicaDiskPath, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return string(replicaDiskPath), nil
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

func (c *Cluster) chooseTargetDataNodes(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
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
		return nil, nil, errors.NewError(err)
	}
	if ns.isSingleRack() {
		var newHosts []string
		if rack, err = ns.getRack(ns.racks[0]); err != nil {
			return nil, nil, errors.NewError(err)
		}
		if newHosts, peers, err = rack.getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		hosts = newHosts
		return
	}

	if racks, err = ns.allocRacks(replicaNum, nil); err != nil {
		return nil, nil, errors.NewError(err)
	}
	if len(racks) == replicaNum {
		for index := 0; index < replicaNum; index++ {
			rack := racks[index]
			var selectPeers []proto.Peer
			if addrs, selectPeers, err = rack.getAvailDataNodeHosts(hosts, 1); err != nil {
				return nil, nil, errors.NewError(err)
			}
			hosts = append(hosts, addrs...)
			peers = append(peers, selectPeers...)
		}
		return
	}
	// the number of racks less than replica number,We're only dealing with one rack and two racks
	if len(racks) == 1 {
		var newHosts []string
		if rack, err = ns.getRack(ns.racks[0]); err != nil {
			return nil, nil, errors.NewError(err)
		}
		if newHosts, peers, err = rack.getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		hosts = newHosts
		return
	} else if len(racks) >= 2 {
		masterRack := racks[0]
		slaveRack := racks[1]
		masterReplicaNum := replicaNum/2 + 1
		slaveReplicaNum := replicaNum - masterReplicaNum
		if masterAddr, masterPeers, err = masterRack.getAvailDataNodeHosts(hosts, masterReplicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		hosts = append(hosts, masterAddr...)
		peers = append(peers, masterPeers...)
		if addrs, slavePeers, err = slaveRack.getAvailDataNodeHosts(hosts, slaveReplicaNum); err != nil {
			return nil, nil, errors.NewError(err)
		}
		hosts = append(hosts, addrs...)
		peers = append(peers, slavePeers...)
	}
	if len(hosts) != replicaNum {
		return nil, nil, proto.ErrNoDataNodeToCreateDataPartition
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

func (c *Cluster) dataNodeOffLine(dataNode *DataNode) (err error) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.Addr)
	log.LogWarn(msg)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if err = c.decommissionDataPartition(dataNode.Addr, dp, dataNodeOfflineErr); err != nil {
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
func (c *Cluster) decommissionDataPartition(offlineAddr string, dp *DataPartition, errMsg string) (err error) {
	var (
		targetHosts []string
		newHosts    []string
		newAddr     string
		newPeers    []proto.Peer
		msg         string
		dataNode    *DataNode
		rack        *Rack
		vol         *Vol
		removePeer  proto.Peer
		replica     *DataReplica
		diskPath    string
	)
	dp.Lock()
	defer dp.Unlock()
	if ok := dp.hasHost(offlineAddr); !ok {
		return
	}

	if vol, err = c.getVol(dp.VolName); err != nil {
		goto errHandler
	}

	if err = dp.hasMissingOneReplica(int(vol.dpReplicaNum)); err != nil {
		goto errHandler
	}

	// if the partition can be offline or not
	if err = dp.canBeOffLine(offlineAddr); err != nil {
		goto errHandler
	}

	if dataNode, err = c.dataNode(offlineAddr); err != nil {
		goto errHandler
	}

	if dataNode.RackName == "" {
		return
	}
	if rack, err = c.t.getRack(dataNode); err != nil {
		goto errHandler
	}
	if targetHosts, newPeers, err = rack.getAvailDataNodeHosts(dp.Hosts, 1); err != nil {
		// select data nodes from the node set
		if targetHosts, newPeers, err = c.chooseTargetDataNodes(1); err != nil {
			goto errHandler
		}
	}
	newAddr = targetHosts[0]
	newHosts = make([]string, 0)
	for _, host := range dp.Hosts {
		if dataNode, err = c.dataNode(host); err != nil {
			goto errHandler
		}
		if host == offlineAddr {
			removePeer = proto.Peer{ID: dataNode.ID, Addr: host}
			continue
		}
		newHosts = append(newHosts, host)
		newPeers = append(newPeers, proto.Peer{ID: dataNode.ID, Addr: host})
	}
	newHosts = append(newHosts, newAddr)
	if err = c.syncDecommissionDataPartition(dp, offlineAddr, removePeer, newPeers[0]); err != nil {
		goto errHandler
	}

	if diskPath, err = c.syncCreateDataPartitionToDataNode(newAddr, vol.dataPartitionSize, dp, newPeers, newHosts); err != nil {
		goto errHandler
	}
	if err = dp.afterCreation(newAddr, diskPath, c); err != nil {
		goto errHandler
	}
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	replica, _ = dp.getReplica(offlineAddr)
	c.putBadDataPartitionIDs(replica, offlineAddr, dp.PartitionID)
	dp.removeReplicaByAddr(offlineAddr)
	dp.checkAndRemoveMissReplica(offlineAddr)
	if err = dp.updateForOffline(offlineAddr, newAddr, dp.VolName, newPeers, newHosts, c); err != nil {
		goto errHandler
	}
	log.LogWarnf("clusterID[%v] partitionID:%v  on Node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, dp.PartitionID, offlineAddr, newAddr, dp.Hosts)
	return
errHandler:
	msg = fmt.Sprintf(errMsg + " clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, offlineAddr, newAddr, err, dp.Hosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) syncDecommissionDataPartition(dp *DataPartition, addr string, removePeer, targetPeer proto.Peer) (err error) {
	var (
		task     *proto.AdminTask
		dataNode *DataNode
	)
	if task, err = dp.createTaskToDecommissionDataPartition(removePeer, targetPeer); err != nil {
		return
	}
	dp.logDecommissionedDataPartition(addr)
	leaderAddr := dp.getLeaderAddr()
	if leaderAddr == "" {
		err = proto.ErrNoLeader
		return
	}

	if dataNode, err = c.dataNode(leaderAddr); err != nil {
		return
	}

	if _, err = dataNode.TaskManager.syncSendAdminTask(task); err != nil {
		return nil
	}
	return
}

func (c *Cluster) putBadDataPartitionIDs(replica *DataReplica, offlineAddr string, partitionID uint64) {
	var key string
	newBadPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", offlineAddr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", offlineAddr, "")
	}
	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
}

func (c *Cluster) decommissionMetaNode(metaNode *MetaNode) {
	msg := fmt.Sprintf("action[decommissionMetaNode],clusterID[%v] Node[%v] OffLine", c.Name, metaNode.Addr)
	log.LogWarn(msg)

	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			// err is not handled here.
			c.decommissionMetaPartition(metaNode.Addr, mp)
		}
	}
	if err := c.syncDeleteMetaNode(metaNode); err != nil {
		msg = fmt.Sprintf("action[decommissionMetaNode],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, metaNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.deleteMetaNodeFromCache(metaNode)
	msg = fmt.Sprintf("action[decommissionMetaNode],clusterID[%v] Node[%v] OffLine success", c.Name, metaNode.Addr)
	Warn(c.Name, msg)
}

func (c *Cluster) deleteMetaNodeFromCache(metaNode *MetaNode) {
	c.metaNodes.Delete(metaNode.Addr)
	c.t.deleteMetaNode(metaNode)
	go metaNode.clean()
}

func (c *Cluster) updateVol(name, authKey string, capacity int) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
	)
	if vol, err = c.getVol(name); err != nil {
		log.LogErrorf("action[updateVol] err[%v]", err)
		err = proto.ErrVolNotExists
		goto errHandler
	}
	serverAuthKey = vol.Owner
	if !matchKey(serverAuthKey, authKey) {
		return proto.ErrVolAuthKeyNotMatch
	}
	if uint64(capacity) < vol.Capacity {
		err = fmt.Errorf("capacity[%v] less than old capacity[%v]", capacity, vol.Capacity)
		goto errHandler
	}
	vol.setCapacity(uint64(capacity))
	if err = c.syncUpdateVol(vol); err != nil {
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

// Create a new volume.
// By default we create 3 meta partitions and 10 data partitions during initialization.
func (c *Cluster) createVol(name, owner string, mpCount, size, capacity int) (vol *Vol, err error) {
	var (
		dataPartitionSize       uint64
		readWriteDataPartitions int
	)
	if size == 0 {
		dataPartitionSize = util.DefaultDataPartitionSize
	} else {
		dataPartitionSize = uint64(size) * util.GB
	}
	if vol, err = c.doCreateVol(name, owner, dataPartitionSize, uint64(capacity)); err != nil {
		goto errHandler
	}
	if err = vol.initMetaPartitions(c, mpCount); err != nil {
		vol.Status = markDelete
		if err = c.syncDeleteVol(vol); err != nil {
			log.LogErrorf("action[createVol] failed,vol[%v] err[%v]", vol.Name, err)
		}
		c.deleteVol(name)
		err = fmt.Errorf("action[createVol] initMetaPartitions failed")
		goto errHandler
	}
	for retryCount := 0; readWriteDataPartitions < defaultInitDataPartitionCnt && retryCount < 3; retryCount++ {
		vol.initDataPartitions(c)
		readWriteDataPartitions = len(vol.dataPartitions.partitionMap)
	}
	vol.dataPartitions.readableAndWritableCnt = readWriteDataPartitions
	log.LogInfof("action[createVol] vol[%v],readableAndWritableCnt[%v]", name, readWriteDataPartitions)
	return

errHandler:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err)
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) doCreateVol(name, owner string, dpSize, capacity uint64) (vol *Vol, err error) {
	var id uint64
	c.createVolMutex.Lock()
	defer c.createVolMutex.Unlock()
	if _, err = c.getVol(name); err == nil {
		err = proto.ErrDuplicateVol
		goto errHandler
	}
	id, err = c.idAlloc.allocateCommonID()
	if err != nil {
		goto errHandler
	}
	vol = newVol(id, name, owner, dpSize, capacity)
	if err = c.syncAddVol(vol); err != nil {
		goto errHandler
	}
	c.putVol(vol)
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
	if err = vol.splitMetaPartition(c, partition, start); err != nil {
		log.LogErrorf("action[updateInodeIDRange]  mp[%v] err[%v]", partition.PartitionID, err)
	}
	return
}

// Choose the target hosts from the available node sets and meta nodes.
func (c *Cluster) chooseTargetMetaHosts(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr []string
		slaveAddrs []string
		masterPeer []proto.Peer
		slavePeers []proto.Peer
		ns         *nodeSet
	)
	if ns, err = c.t.allocNodeSetForMetaNode(uint8(replicaNum)); err != nil {
		return nil, nil, errors.NewError(err)
	}

	hosts = make([]string, 0)
	if masterAddr, masterPeer, err = ns.getAvailMetaNodeHosts(hosts, 1); err != nil {
		return nil, nil, errors.NewError(err)
	}
	peers = append(peers, masterPeer...)
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	if slaveAddrs, slavePeers, err = ns.getAvailMetaNodeHosts(hosts, otherReplica); err != nil {
		return nil, nil, errors.NewError(err)
	}
	hosts = append(hosts, slaveAddrs...)
	peers = append(peers, slavePeers...)
	if len(hosts) != replicaNum {
		return nil, nil, proto.ErrNoMetaNodeToCreateMetaPartition
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

func (c *Cluster) allDataNodes() (dataNodes []NodeView) {
	dataNodes = make([]NodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodes = append(dataNodes, NodeView{Addr: dataNode.Addr, Status: dataNode.isActive, ID: dataNode.ID, IsWritable: dataNode.isWriteAble()})
		return true
	})
	return
}

func (c *Cluster) allMetaNodes() (metaNodes []NodeView) {
	metaNodes = make([]NodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
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
