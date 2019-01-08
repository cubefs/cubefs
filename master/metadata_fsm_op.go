// Copyright 2018 The Container File System Authors.
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
	"github.com/juju/errors"
	bsProto "github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

/* We defines several "values" such as ClusterValue, MetaPartitionValue, DataPartitionValue, VolValue, DataNodeValue,
   NodeSetValue, and MetaNodeValue here. Those are the value objects that will be marshaled as a byte array to
   transferred over the network. */

type ClusterValue struct {
	Name          string
}

func newClusterValue(c *Cluster) (cv *ClusterValue) {
	cv = &ClusterValue{
		Name:          c.Name,
	}
	return cv
}

type MetaPartitionValue struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	VolID       uint64
	ReplicaNum  uint8
	Status      int8
	VolName     string
	Hosts       string
	Peers       []bsProto.Peer
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *MetaPartitionValue) {
	mpv = &MetaPartitionValue{
		PartitionID: mp.PartitionID,
		Start:       mp.Start,
		End:         mp.End,
		VolID:       mp.volID,
		ReplicaNum:  mp.ReplicaNum,
		Status:      mp.Status,
		VolName:     mp.volName,
		Hosts:       mp.hostsToString(),
		Peers:       mp.Peers,
	}
	return
}

type DataPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Hosts         string
	Peers         []bsProto.Peer
	IsRandomWrite bool
	Status        int8
	VolID         uint64
	VolName       string
}

func newDataPartitionValue(dp *DataPartition) (dpv *DataPartitionValue) {
	dpv = &DataPartitionValue{
		PartitionID:   dp.PartitionID,
		ReplicaNum:    dp.ReplicaNum,
		Hosts:         dp.hostsToString(),
		Peers:         dp.Peers,
		IsRandomWrite: dp.IsRandomWrite,
		Status:        dp.Status,
		VolID:         dp.VolID,
		VolName:       dp.VolName,
	}
	return
}

type VolValue struct {
	ID                uint64
	Name              string
	ReplicaNum        uint8
	Status            uint8
	RandomWrite       bool
	DataPartitionSize uint64
	Capacity          uint64
}

func newVolValue(vol *Vol) (vv *VolValue) {
	vv = &VolValue{
		ID:                vol.ID,
		Name:              vol.Name,
		ReplicaNum:        vol.mpReplicaNum,
		Status:            vol.Status,
		RandomWrite:       vol.IsRandomWrite,
		DataPartitionSize: vol.dataPartitionSize,
		Capacity:          vol.Capacity,
	}
	return
}

type DataNodeValue struct {
	ID        uint64
	NodeSetID uint64
	Addr      string
}

func newDataNodeValue(dataNode *DataNode) *DataNodeValue {
	return &DataNodeValue{
		ID:        dataNode.ID,
		NodeSetID: dataNode.NodeSetID,
		Addr:      dataNode.Addr,
	}
}

type MetaNodeValue struct {
	ID        uint64
	NodeSetID uint64
	Addr      string
}

func newMetaNodeValue(metaNode *MetaNode) *MetaNodeValue {
	return &MetaNodeValue{
		ID:        metaNode.ID,
		NodeSetID: metaNode.NodeSetID,
		Addr:      metaNode.Addr,
	}
}

type NodeSetValue struct {
	ID          uint64
	Capacity    int
	MetaNodeLen int
	DataNodeLen int
}

func newNodeSetValue(nset *nodeSet) (nsv *NodeSetValue) {
	nsv = &NodeSetValue{
		ID:          nset.ID,
		Capacity:    nset.Capacity,
		MetaNodeLen: nset.metaNodeLen,
		DataNodeLen: nset.dataNodeLen,
	}
	return
}

// RaftCmd defines the Raft commands.
type RaftCmd struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

// Marshal converts the RaftCmd to a byte array.
func (m *RaftCmd) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts the byte array to a RaftCmd.
func (m *RaftCmd) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

func (m *RaftCmd) setOpType() {
	keyArr := strings.Split(m.K, keySeparator)
	if len(keyArr) < 2 {
		log.LogWarnf("action[setOpType] invalid length[%v]", keyArr)
		return
	}
	switch keyArr[1] {
	case metaNodeAcronym:
		m.Op = opSyncAddMetaNode
	case dataNodeAcronym:
		m.Op = opSyncAddDataNode
	case dataPartitionAcronym:
		m.Op = opSyncAddDataPartition
	case metaPartitionAcronym:
		m.Op = opSyncAddMetaPartition
	case volAcronym:
		m.Op = opSyncAddVol
	case clusterAcronym:
		m.Op = opSyncPutCluster
	case nodeSetAcronym:
		m.Op = opSyncAddNodeSet
	case maxDataPartitionIDKey:
		m.Op = opSyncAllocDataPartitionID
	case maxMetaPartitionIDKey:
		m.Op = opSyncAllocMetaPartitionID
	case maxCommonIDKey:
		m.Op = opSyncAllocCommonID
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}

//key=#c#name
func (c *Cluster) syncPutCluster() (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opSyncPutCluster
	metadata.K = clusterPrefix + c.Name
	cv := newClusterValue(c)
	metadata.V, err = json.Marshal(cv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

// key=#s#id
func (c *Cluster) syncAddNodeSet(nset *nodeSet) (err error) {
	return c.putNodeSetInfo(opSyncAddNodeSet, nset)
}

func (c *Cluster) syncUpdateNodeSet(nset *nodeSet) (err error) {
	return c.putNodeSetInfo(opSyncUpdateNodeSet, nset)
}

func (c *Cluster) putNodeSetInfo(opType uint32, nset *nodeSet) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = nodeSetPrefix + strconv.FormatUint(nset.ID, 10)
	nsv := newNodeSetValue(nset)
	metadata.V, err = json.Marshal(nsv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

// key=#dp#volID#partitionID,value=json.Marshal(DataPartitionValue)
func (c *Cluster) syncAddDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(opSyncAddDataPartition, dp)
}

func (c *Cluster) syncUpdateDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(opSyncUpdateDataPartition, dp)
}

func (c *Cluster) syncDeleteDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(opSyncDeleteDataPartition, dp)
}

func (c *Cluster) putDataPartitionInfo(opType uint32, dp *DataPartition) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = dataPartitionPrefix + strconv.FormatUint(dp.VolID, 10) + keySeparator + strconv.FormatUint(dp.PartitionID, 10)
	dpv := newDataPartitionValue(dp)
	metadata.V, err = json.Marshal(dpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *RaftCmd) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = c.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[metadata_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

//key=#vol#volID,value=json.Marshal(vv)
func (c *Cluster) syncAddVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(opSyncAddVol, vol)
}

func (c *Cluster) syncUpdateVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(opSyncUpdateVol, vol)
}

func (c *Cluster) syncDeleteVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(opSyncDeleteVol, vol)
}

func (c *Cluster) syncPutVolInfo(opType uint32, vol *Vol) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = volPrefix + strconv.FormatUint(vol.ID, 10)
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

// key=#mp#volID#metaPartitionID,value=json.Marshal(MetaPartitionValue)
func (c *Cluster) syncAddMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(opSyncAddMetaPartition, mp)
}

func (c *Cluster) syncUpdateMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(opSyncUpdateMetaPartition, mp)
}

func (c *Cluster) syncDeleteMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(opSyncDeleteMetaPartition, mp)
}

func (c *Cluster) putMetaPartitionInfo(opType uint32, mp *MetaPartition) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	partitionID := strconv.FormatUint(mp.PartitionID, 10)
	metadata.K = metaPartitionPrefix + strconv.FormatUint(mp.volID, 10) + keySeparator + partitionID
	mpv := newMetaPartitionValue(mp)
	if metadata.V, err = json.Marshal(mpv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

// key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(opSyncAddMetaNode, metaNode)
}

func (c *Cluster) syncDeleteMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(opSyncDeleteMetaNode, metaNode)
}

func (c *Cluster) syncPutMetaNode(opType uint32, metaNode *MetaNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = metaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + keySeparator + metaNode.Addr
	mnv := newMetaNodeValue(metaNode)
	metadata.V, err = json.Marshal(mnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

// key=#dn#id#Addr,value = json.Marshal(dnv)
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(opSyncAddDataNode, dataNode)
}

func (c *Cluster) syncDeleteDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(opSyncDeleteDataNode, dataNode)
}

func (c *Cluster) syncPutDataNodeInfo(opType uint32, dataNode *DataNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = dataNodePrefix + strconv.FormatUint(dataNode.ID, 10) + keySeparator + dataNode.Addr
	dnv := newDataNodeValue(dataNode)
	metadata.V, err = json.Marshal(dnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) addRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfAddNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[addRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) removeRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfRemoveNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[removeRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) handleApply(cmd *RaftCmd) (err error) {
	if cmd == nil {
		return fmt.Errorf("metadata can't be null")
	}
	switch cmd.Op {

	case opSyncPutCluster:
		c.applyPutCluster(cmd)
	case opSyncAddNodeSet:
		c.applyAddNodeSet(cmd)
	case opSyncUpdateNodeSet:
		c.applyUpdateNodeSet(cmd)
	case opSyncAddDataNode:
		c.applyAddDataNode(cmd)
	case opSyncDeleteDataNode:
		c.applyDeleteDataNode(cmd)
	case opSyncAddMetaNode:
		err = c.applyAddMetaNode(cmd)
	case opSyncDeleteMetaNode:
		c.applyDeleteMetaNode(cmd)
	case opSyncAddVol:
		c.applyAddVol(cmd)
	case opSyncUpdateVol:
		c.applyUpdateVol(cmd)
	case opSyncDeleteVol:
		c.applyDeleteVol(cmd)
	case opSyncAddMetaPartition:
		c.applyAddMetaPartition(cmd)
	case opSyncUpdateMetaPartition:
		c.applyUpdateMetaPartition(cmd)
	case opSyncAddDataPartition:
		c.applyAddDataPartition(cmd)
	case opSyncUpdateDataPartition:
		c.applyUpdateDataPartition(cmd)
	case opSyncAllocCommonID:
		id, err1 := strconv.ParseUint(string(cmd.V), 10, 64)
		if err1 != nil {
			return err1
		}
		c.idAlloc.setCommonID(id)
	case opSyncAllocDataPartitionID:
		id, err1 := strconv.ParseUint(string(cmd.V), 10, 64)
		if err1 != nil {
			return err1
		}
		c.idAlloc.setDataPartitionID(id)
	case opSyncAllocMetaPartitionID:
		id, err1 := strconv.ParseUint(string(cmd.V), 10, 64)
		if err1 != nil {
			return err1
		}
		c.idAlloc.setMetaPartitionID(id)
	}
	log.LogInfof("action[handleApply] success,cmd.K[%v],cmd.V[%v]", cmd.K, string(cmd.V))
	curIndex := c.fsm.applied
	if curIndex > 0 && curIndex%c.retainLogs == 0 && c.partition != nil {
		c.partition.Truncate(curIndex)
	}
	return
}

func (c *Cluster) applyPutCluster(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyPutCluster] cmd:%v", cmd.K)
	cv := &ClusterValue{}
	if err = json.Unmarshal(cmd.V, cv); err != nil {
		log.LogErrorf("action[applyPutCluster],err:%v", err.Error())
		return
	}
	return
}

func (c *Cluster) applyAddNodeSet(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyAddNodeSet] cmd:%v", cmd.K)
	nsv := &NodeSetValue{}
	if err = json.Unmarshal(cmd.V, nsv); err != nil {
		log.LogErrorf("action[applyAddNodeSet],err:%v", err.Error())
		return
	}
	ns := newNodeSet(nsv.ID, c.cfg.nodeSetCapacity)
	c.t.putNodeSet(ns)
	return
}

func (c *Cluster) applyUpdateNodeSet(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyUpdateNodeSet] cmd:%v", cmd.K)
	nsv := &NodeSetValue{}
	if err = json.Unmarshal(cmd.V, nsv); err != nil {
		log.LogErrorf("action[applyUpdateNodeSet],err:%v", err.Error())
		return
	}
	ns, err := c.t.getNodeSet(nsv.ID)
	if err != nil {
		log.LogErrorf("action[applyUpdateNodeSet],err:%v", err.Error())
		return
	}
	ns.Lock()
	ns.dataNodeLen = nsv.DataNodeLen
	ns.metaNodeLen = nsv.MetaNodeLen
	ns.Unlock()
	return
}

func (c *Cluster) applyAddDataNode(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyAddDataNode] cmd:%v", cmd.K)
	dnv := &DataNodeValue{}
	if err = json.Unmarshal(cmd.V, dnv); err != nil {
		log.LogErrorf("action[applyAddDataNode],err:%v", err.Error())
		return
	}
	dataNode := newDataNode(dnv.Addr, c.Name)
	dataNode.ID = dnv.ID
	dataNode.NodeSetID = dnv.NodeSetID
	c.dataNodes.Store(dataNode.Addr, dataNode)
	return
}

func (c *Cluster) applyDeleteDataNode(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyDeleteDataNode] cmd:%v", cmd.K)
	dnv := &DataNodeValue{}
	if err = json.Unmarshal(cmd.V, dnv); err != nil {
		log.LogErrorf("action[applyDeleteDataNode],err:%v", err.Error())
		return
	}
	if value, ok := c.dataNodes.Load(dnv.ID); ok {
		dataNode := value.(*DataNode)
		c.delDataNodeFromCache(dataNode)
	}
	return
}

func (c *Cluster) applyAddMetaNode(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyAddMetaNode] cmd:%v", cmd.K)
	mnv := &MetaNodeValue{}
	if err = json.Unmarshal(cmd.V, mnv); err != nil {
		log.LogErrorf("action[applyAddMetaNode],err:%v", err.Error())
		return
	}
	if _, err = c.metaNode(mnv.Addr); err != nil {
		metaNode := newMetaNode(mnv.Addr, c.Name)
		metaNode.ID = mnv.ID
		metaNode.NodeSetID = mnv.NodeSetID
		c.metaNodes.Store(metaNode.Addr, metaNode)
	}
	return nil
}

func (c *Cluster) applyDeleteMetaNode(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyDeleteMetaNode] cmd:%v", cmd.K)
	mnv := &MetaNodeValue{}
	if err = json.Unmarshal(cmd.V, mnv); err != nil {
		log.LogErrorf("action[applyDeleteMetaNode],err:%v", err.Error())
		return
	}
	if value, ok := c.metaNodes.Load(mnv.ID); ok {
		metaNode := value.(*MetaNode)
		c.deleteMetaNodeFromCache(metaNode)
	}
	return
}

func (c *Cluster) applyAddVol(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyAddVol] cmd:%v", cmd.K)
	vv := &VolValue{}
	if err = json.Unmarshal(cmd.V, vv); err != nil {
		log.LogError(fmt.Sprintf("action[applyAddVol] failed,err:%v", err))
		return
	}
	vol := newVol(vv.ID, vv.Name, vv.ReplicaNum, vv.RandomWrite, vv.DataPartitionSize, vv.Capacity)
	c.putVol(vol)
	return
}

func (c *Cluster) applyUpdateVol(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyUpdateVol] cmd:%v", cmd.K)
	vv := &VolValue{}
	if err = json.Unmarshal(cmd.V, vv); err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateVol] failed,err:%v", err))
		return
	}
	vol, err := c.getVol(vv.Name)
	if err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateVol] failed,err:%v", err))
		return
	}
	vol.setStatus(vv.Status)
	vol.setCapacity(vv.Capacity)
	return
}

func (c *Cluster) applyDeleteVol(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyDeleteVol] cmd:%v", cmd.K)
	vv := &VolValue{}
	if err = json.Unmarshal(cmd.V, vv); err != nil {
		log.LogError(fmt.Sprintf("action[applyDeleteVol] failed,err:%v", err))
		return
	}
	c.deleteVol(vv.Name)
	return
}

func (c *Cluster) applyAddMetaPartition(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyAddMetaPartition] cmd:%v", cmd.K)
	mpv := &MetaPartitionValue{}
	if err = json.Unmarshal(cmd.V, mpv); err != nil {
		log.LogError(fmt.Sprintf("action[applyAddMetaPartition] failed,err:%v", err))
		return
	}
	mp := newMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, mpv.ReplicaNum, mpv.VolName, mpv.VolID)
	mp.Peers = mpv.Peers
	mp.Hosts = strings.Split(mpv.Hosts, underlineSeparator)
	mp.Status = mpv.Status
	vol, err := c.getVol(mpv.VolName)
	if err != nil {
		log.LogErrorf("action[applyAddMetaPartition] failed,err:%v", err)
		return
	}
	vol.addMetaPartition(mp)
	return
}

func (c *Cluster) applyUpdateMetaPartition(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyUpdateMetaPartition] cmd:%v", cmd.K)
	mpv := &MetaPartitionValue{}
	if err = json.Unmarshal(cmd.V, mpv); err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateMetaPartition] failed,err:%v", err))
		return
	}
	vol, err := c.getVol(mpv.VolName)
	if err != nil {
		log.LogErrorf("action[applyUpdateDataPartition] failed,err:%v", err)
		return
	}
	mp, err := vol.metaPartition(mpv.PartitionID)
	if err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateMetaPartition] failed,err:%v", err))
		return
	}
	mp.updateMetricByRaft(mpv)
	return
}

func (c *Cluster) applyAddDataPartition(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyAddDataPartition] cmd:%v", cmd.K)
	dpv := &DataPartitionValue{}
	if err = json.Unmarshal(cmd.V, dpv); err != nil {
		log.LogError(fmt.Sprintf("action[applyAddDataPartition] failed,err:%v", err))
		return
	}
	vol, err := c.getVol(dpv.VolName)
	if err != nil {
		log.LogErrorf("action[applyAddDataPartition] failed,err:%v", err)
		return
	}
	dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, vol.Name, vol.ID, vol.IsRandomWrite)
	dp.Hosts = strings.Split(dpv.Hosts, underlineSeparator)
	dp.Peers = dpv.Peers
	dp.Status = dpv.Status
	vol.dataPartitions.put(dp)
	return
}

func (c *Cluster) applyUpdateDataPartition(cmd *RaftCmd) (err error) {
	log.LogInfof("action[applyUpdateDataPartition] cmd:%v", cmd.K)
	dpv := &DataPartitionValue{}
	if err = json.Unmarshal(cmd.V, dpv); err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateDataPartition] failed,err:%v", err))
		return
	}
	vol, err := c.getVol(dpv.VolName)
	if err != nil {
		log.LogErrorf("action[applyUpdateDataPartition] failed,err:%v", err)
		return
	}
	if _, err = vol.getDataPartitionByID(dpv.PartitionID); err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateDataPartition] failed,err:%v", err))
		return
	}
	dp, err := vol.getDataPartitionByID(dpv.PartitionID)
	if err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateDataPartition] failed,err:%v", err))
		return
	}
	dp.Hosts = strings.Split(dpv.Hosts, underlineSeparator)
	dp.Peers = dpv.Peers
	return
}

func (c *Cluster) loadNodeSets() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(nodeSetPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadNodeSets],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		nsv := &NodeSetValue{}
		if err = json.Unmarshal(value, nsv); err != nil {
			log.LogErrorf("action[loadNodeSets], unmarshal err:%v", err.Error())
			return err
		}
		ns := newNodeSet(nsv.ID, c.cfg.nodeSetCapacity)
		ns.metaNodeLen = nsv.MetaNodeLen
		ns.dataNodeLen = nsv.DataNodeLen
		c.t.putNodeSet(ns)
		log.LogInfof("action[loadNodeSets], nsId[%v]", ns.ID)
	}
	return
}

func (c *Cluster) loadDataNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(dataNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadDataNodes],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		dnv := &DataNodeValue{}
		if err = json.Unmarshal(value, dnv); err != nil {
			err = fmt.Errorf("action[loadDataNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		dataNode := newDataNode(dnv.Addr, c.Name)
		dataNode.ID = dnv.ID
		dataNode.NodeSetID = dnv.NodeSetID
		c.dataNodes.Store(dataNode.Addr, dataNode)
		log.LogInfof("action[loadDataNodes],dataNode[%v]", dataNode.Addr)
	}
	return
}

func (c *Cluster) loadMetaNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(metaNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadMetaNodes],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		mnv := &MetaNodeValue{}
		if err = json.Unmarshal(value, mnv); err != nil {
			err = fmt.Errorf("action[loadMetaNodes],unmarshal err:%v", err.Error())
			return err
		}
		metaNode := newMetaNode(mnv.Addr, c.Name)
		metaNode.ID = mnv.ID
		metaNode.NodeSetID = mnv.NodeSetID
		c.metaNodes.Store(metaNode.Addr, metaNode)
		log.LogInfof("action[loadMetaNodes],metaNode[%v]", metaNode.Addr)
	}
	return
}

func (c *Cluster) loadVols() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(volPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadVols],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		vv := &VolValue{}
		if err = json.Unmarshal(value, vv); err != nil {
			err = fmt.Errorf("action[loadVols],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol := newVol(vv.ID, vv.Name, vv.ReplicaNum, vv.RandomWrite, vv.DataPartitionSize, vv.Capacity)
		vol.Status = vv.Status
		c.putVol(vol)
		log.LogInfof("action[loadVols],vol[%v]", vol)
	}
	return
}

func (c *Cluster) loadMetaPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(metaPartitionPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadMetaPartitions],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		mpv := &MetaPartitionValue{}
		if err = json.Unmarshal(value, mpv); err != nil {
			err = fmt.Errorf("action[loadMetaPartitions],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol, err1 := c.getVol(mpv.VolName)
		if err1 != nil {
			log.LogErrorf("action[loadMetaPartitions] err:%v", err1.Error())
			continue
		}
		mp := newMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, vol.mpReplicaNum, vol.Name, mpv.VolID)
		mp.setHosts(strings.Split(mpv.Hosts, underlineSeparator))
		mp.setPeers(mpv.Peers)
		vol.addMetaPartition(mp)
		log.LogInfof("action[loadMetaPartitions],vol[%v],mp[%v]", vol.Name, mp.PartitionID)
	}
	return
}

func (c *Cluster) loadDataPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(dataPartitionPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadDataPartitions],err:%v", err.Error())
		return err
	}
	for _, value := range result {

		dpv := &DataPartitionValue{}
		if err = json.Unmarshal(value, dpv); err != nil {
			err = fmt.Errorf("action[loadDataPartitions],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol, err1 := c.getVol(dpv.VolName)
		if err1 != nil {
			log.LogErrorf("action[loadDataPartitions] err:%v", err1.Error())
			continue
		}
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.VolName, dpv.VolID, dpv.IsRandomWrite)
		dp.Hosts = strings.Split(dpv.Hosts, underlineSeparator)
		dp.Peers = dpv.Peers
		vol.dataPartitions.put(dp)
		log.LogInfof("action[loadDataPartitions],vol[%v],dp[%v]", vol.Name, dp.PartitionID)
	}
	return
}
