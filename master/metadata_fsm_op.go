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
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	bsProto "github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

type ClusterValue struct {
	Name          string
	CompactStatus bool
}

func newClusterValue(c *Cluster) (cv *ClusterValue) {
	cv = &ClusterValue{
		Name:          c.Name,
		CompactStatus: c.compactStatus,
	}
	return cv
}

type MetaPartitionValue struct {
	PartitionID uint64
	ReplicaNum  uint8
	Start       uint64
	End         uint64
	VolID       uint64
	VolName     string
	Hosts       string
	Peers       []bsProto.Peer
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *MetaPartitionValue) {
	mpv = &MetaPartitionValue{
		PartitionID: mp.PartitionID,
		ReplicaNum:  mp.ReplicaNum,
		Start:       mp.Start,
		End:         mp.End,
		VolID:       mp.volID,
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
	PartitionType string
	RandomWrite   bool
	VolID         uint64
	VolName       string
}

func newDataPartitionValue(dp *DataPartition) (dpv *DataPartitionValue) {
	dpv = &DataPartitionValue{
		PartitionID:   dp.PartitionID,
		ReplicaNum:    dp.ReplicaNum,
		Hosts:         dp.HostsToString(),
		Peers:         dp.Peers,
		PartitionType: dp.PartitionType,
		RandomWrite:   dp.RandomWrite,
		VolID:         dp.VolID,
		VolName:       dp.VolName,
	}
	return
}

type VolValue struct {
	Id                uint64
	Name              string
	ReplicaNum        uint8
	Status            uint8
	RandomWrite       bool
	DataPartitionSize uint64
	Capacity          uint64
}

func newVolValue(vol *Vol) (vv *VolValue) {
	vv = &VolValue{
		Id:                vol.Id,
		Name:              vol.Name,
		ReplicaNum:        vol.mpReplicaNum,
		Status:            vol.Status,
		RandomWrite:       vol.RandomWrite,
		DataPartitionSize: vol.dataPartitionSize,
		Capacity:          vol.Capacity,
	}
	return
}

type DataNodeValue struct {
	Id        uint64
	NodeSetId uint64
	Addr      string
}

func newDataNodeValue(dataNode *DataNode) *DataNodeValue {
	return &DataNodeValue{
		Id:        dataNode.Id,
		NodeSetId: dataNode.NodeSetId,
		Addr:      dataNode.Addr,
	}
}

type MetaNodeValue struct {
	Id        uint64
	NodeSetId uint64
	Addr      string
}

func newMetaNodeValue(metaNode *MetaNode) *MetaNodeValue {
	return &MetaNodeValue{
		Id:        metaNode.ID,
		NodeSetId: metaNode.NodeSetId,
		Addr:      metaNode.Addr,
	}
}

type NodeSetValue struct {
	Id          uint64
	Capacity    int
	MetaNodeLen int
	DataNodeLen int
}

func newNodeSetValue(nset *NodeSet) (nsv *NodeSetValue) {
	nsv = &NodeSetValue{
		Id:          nset.Id,
		Capacity:    nset.Capacity,
		MetaNodeLen: nset.metaNodeLen,
		DataNodeLen: nset.dataNodeLen,
	}
	return
}

type RaftCmdData struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func (m *RaftCmdData) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *RaftCmdData) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

//send data to follower by snapshot,must set operate type to each add type
func (m *RaftCmdData) setOpType() {
	keyArr := strings.Split(m.K, KeySeparator)
	if len(keyArr) < 2 {
		log.LogWarnf("action[setOpType] invalid length[%v]", keyArr)
		return
	}
	switch keyArr[1] {
	case MetaNodeAcronym:
		m.Op = OpSyncAddMetaNode
	case DataNodeAcronym:
		m.Op = OpSyncAddDataNode
	case DataPartitionAcronym:
		m.Op = OpSyncAddDataPartition
	case MetaPartitionAcronym:
		m.Op = OpSyncAddMetaPartition
	case VolAcronym:
		m.Op = OpSyncAddVol
	case ClusterAcronym:
		m.Op = OpSyncPutCluster
	case NodeSetAcronym:
		m.Op = OpSyncAddNodeSet
	case MaxDataPartitionIDKey:
		m.Op = OpSyncAllocDataPartitionID
	case MaxMetaPartitionIDKey:
		m.Op = OpSyncAllocMetaPartitionID
	case MaxCommonIDKey:
		m.Op = OpSyncAllocCommonID
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}

//key=#c#name
func (c *Cluster) syncPutCluster() (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = OpSyncPutCluster
	metadata.K = ClusterPrefix + c.Name
	cv := newClusterValue(c)
	metadata.V, err = json.Marshal(cv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

//key=#s#id
func (c *Cluster) syncAddNodeSet(nset *NodeSet) (err error) {
	return c.putNodeSetInfo(OpSyncAddNodeSet, nset)
}

func (c *Cluster) syncUpdateNodeSet(nset *NodeSet) (err error) {
	return c.putNodeSetInfo(OpSyncUpdateNodeSet, nset)
}

func (c *Cluster) putNodeSetInfo(opType uint32, nset *NodeSet) (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = opType
	metadata.K = NodeSetPrefix + strconv.FormatUint(nset.Id, 10)
	nsv := newNodeSetValue(nset)
	metadata.V, err = json.Marshal(nsv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

//key=#dp#volID#partitionID,value=json.Marshal(DataPartitionValue)
func (c *Cluster) syncAddDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncAddDataPartition, dp)
}

func (c *Cluster) syncUpdateDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncUpdateDataPartition, dp)
}

func (c *Cluster) syncDeleteDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncDeleteDataPartition, dp)
}

func (c *Cluster) putDataPartitionInfo(opType uint32, dp *DataPartition) (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = opType
	metadata.K = DataPartitionPrefix + strconv.FormatUint(dp.VolID, 10) + KeySeparator + strconv.FormatUint(dp.PartitionID, 10)
	dpv := newDataPartitionValue(dp)
	metadata.V, err = json.Marshal(dpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *RaftCmdData) (err error) {
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
	return c.syncPutVolInfo(OpSyncAddVol, vol)
}

func (c *Cluster) syncUpdateVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(OpSyncUpdateVol, vol)
}

func (c *Cluster) syncDeleteVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(OpSyncDeleteVol, vol)
}

func (c *Cluster) syncPutVolInfo(opType uint32, vol *Vol) (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = opType
	metadata.K = VolPrefix + strconv.FormatUint(vol.Id, 10)
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

////key=#mp#volID#metaPartitionID,value=json.Marshal(MetaPartitionValue)
func (c *Cluster) syncAddMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncAddMetaPartition, mp)
}

func (c *Cluster) syncUpdateMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncUpdateMetaPartition, mp)
}

func (c *Cluster) syncDeleteMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncDeleteMetaPartition, mp)
}

func (c *Cluster) putMetaPartitionInfo(opType uint32, mp *MetaPartition) (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = opType
	partitionID := strconv.FormatUint(mp.PartitionID, 10)
	metadata.K = MetaPartitionPrefix + strconv.FormatUint(mp.volID, 10) + KeySeparator + partitionID
	mpv := newMetaPartitionValue(mp)
	if metadata.V, err = json.Marshal(mpv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

//key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(OpSyncAddMetaNode, metaNode)
}

func (c *Cluster) syncDeleteMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(OpSyncDeleteMetaNode, metaNode)
}

func (c *Cluster) syncPutMetaNode(opType uint32, metaNode *MetaNode) (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = opType
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	mnv := newMetaNodeValue(metaNode)
	metadata.V, err = json.Marshal(mnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

//key=#dn#id#Addr,value = json.Marshal(dnv)
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(OpSyncAddDataNode, dataNode)
}

func (c *Cluster) syncDeleteDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(OpSyncDeleteDataNode, dataNode)
}

func (c *Cluster) syncPutDataNodeInfo(opType uint32, dataNode *DataNode) (err error) {
	metadata := new(RaftCmdData)
	metadata.Op = opType
	metadata.K = DataNodePrefix + strconv.FormatUint(dataNode.Id, 10) + KeySeparator + dataNode.Addr
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

func (c *Cluster) handleApply(cmd *RaftCmdData) (err error) {
	if cmd == nil {
		return fmt.Errorf("metadata can't be null")
	}
	if c == nil || c.fsm == nil {
		return fmt.Errorf("cluster has not init")
	}
	switch cmd.Op {
	case OpSyncPutCluster:
		c.applyPutCluster(cmd)
	case OpSyncAddNodeSet:
		c.applyAddNodeSet(cmd)
	case OpSyncUpdateNodeSet:
		c.applyUpdateNodeSet(cmd)
	case OpSyncAddDataNode:
		c.applyAddDataNode(cmd)
	case OpSyncDeleteDataNode:
		c.applyDeleteDataNode(cmd)
	case OpSyncAddMetaNode:
		err = c.applyAddMetaNode(cmd)
	case OpSyncDeleteMetaNode:
		c.applyDeleteMetaNode(cmd)
	case OpSyncAddVol:
		c.applyAddVol(cmd)
	case OpSyncUpdateVol:
		c.applyUpdateVol(cmd)
	case OpSyncDeleteVol:
		c.applyDeleteVol(cmd)
	case OpSyncAddMetaPartition:
		c.applyAddMetaPartition(cmd)
	case OpSyncUpdateMetaPartition:
		c.applyUpdateMetaPartition(cmd)
	case OpSyncAddDataPartition:
		c.applyAddDataPartition(cmd)
	case OpSyncUpdateDataPartition:
		c.applyUpdateDataPartition(cmd)
	case OpSyncAllocCommonID:
		id, err1 := strconv.ParseUint(string(cmd.V), 10, 64)
		if err1 != nil {
			return err1
		}
		c.idAlloc.setCommonID(id)
	case OpSyncAllocDataPartitionID:
		id, err1 := strconv.ParseUint(string(cmd.V), 10, 64)
		if err1 != nil {
			return err1
		}
		c.idAlloc.setDataPartitionID(id)
	case OpSyncAllocMetaPartitionID:
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

func (c *Cluster) applyPutCluster(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyPutCluster] cmd:%v", cmd.K)
	cv := &ClusterValue{}
	if err = json.Unmarshal(cmd.V, cv); err != nil {
		log.LogErrorf("action[applyPutCluster],err:%v", err.Error())
		return
	}
	c.compactStatus = cv.CompactStatus
	return
}

func (c *Cluster) applyAddNodeSet(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyAddNodeSet] cmd:%v", cmd.K)
	nsv := &NodeSetValue{}
	if err = json.Unmarshal(cmd.V, nsv); err != nil {
		log.LogErrorf("action[applyAddNodeSet],err:%v", err.Error())
		return
	}
	ns := newNodeSet(nsv.Id, DefaultNodeSetCapacity)
	c.t.putNodeSet(ns)
	return
}

func (c *Cluster) applyUpdateNodeSet(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyUpdateNodeSet] cmd:%v", cmd.K)
	nsv := &NodeSetValue{}
	if err = json.Unmarshal(cmd.V, nsv); err != nil {
		log.LogErrorf("action[applyUpdateNodeSet],err:%v", err.Error())
		return
	}
	ns, err := c.t.getNodeSet(nsv.Id)
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

func (c *Cluster) applyAddDataNode(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyAddDataNode] cmd:%v", cmd.K)
	dnv := &DataNodeValue{}
	if err = json.Unmarshal(cmd.V, dnv); err != nil {
		log.LogErrorf("action[applyAddDataNode],err:%v", err.Error())
		return
	}
	dataNode := NewDataNode(dnv.Addr, c.Name)
	dataNode.Id = dnv.Id
	dataNode.NodeSetId = dnv.NodeSetId
	c.dataNodes.Store(dataNode.Addr, dataNode)
	return
}

func (c *Cluster) applyDeleteDataNode(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyDeleteDataNode] cmd:%v", cmd.K)
	dnv := &DataNodeValue{}
	if err = json.Unmarshal(cmd.V, dnv); err != nil {
		log.LogErrorf("action[applyDeleteDataNode],err:%v", err.Error())
		return
	}
	if value, ok := c.dataNodes.Load(dnv.Id); ok {
		dataNode := value.(*DataNode)
		c.delDataNodeFromCache(dataNode)
	}
	return
}

func (c *Cluster) applyAddMetaNode(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyAddMetaNode] cmd:%v", cmd.K)
	mnv := &MetaNodeValue{}
	if err = json.Unmarshal(cmd.V, mnv); err != nil {
		log.LogErrorf("action[applyAddMetaNode],err:%v", err.Error())
		return
	}
	if _, err = c.getMetaNode(mnv.Addr); err != nil {
		metaNode := NewMetaNode(mnv.Addr, c.Name)
		metaNode.ID = mnv.Id
		metaNode.NodeSetId = mnv.NodeSetId
		c.metaNodes.Store(metaNode.Addr, metaNode)
	}
	return nil
}

func (c *Cluster) applyDeleteMetaNode(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyDeleteMetaNode] cmd:%v", cmd.K)
	mnv := &MetaNodeValue{}
	if err = json.Unmarshal(cmd.V, mnv); err != nil {
		log.LogErrorf("action[applyDeleteMetaNode],err:%v", err.Error())
		return
	}
	if value, ok := c.metaNodes.Load(mnv.Id); ok {
		metaNode := value.(*MetaNode)
		c.delMetaNodeFromCache(metaNode)
	}
	return
}

func (c *Cluster) applyAddVol(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyAddVol] cmd:%v", cmd.K)
	vv := &VolValue{}
	if err = json.Unmarshal(cmd.V, vv); err != nil {
		log.LogError(fmt.Sprintf("action[applyAddVol] failed,err:%v", err))
		return
	}
	vol := NewVol(vv.Id, vv.Name, vv.ReplicaNum, vv.RandomWrite, vv.DataPartitionSize, vv.Capacity)
	c.putVol(vol)
	return
}

func (c *Cluster) applyUpdateVol(cmd *RaftCmdData) (err error) {
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

func (c *Cluster) applyDeleteVol(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyDeleteVol] cmd:%v", cmd.K)
	vv := &VolValue{}
	if err = json.Unmarshal(cmd.V, vv); err != nil {
		log.LogError(fmt.Sprintf("action[applyDeleteVol] failed,err:%v", err))
		return
	}
	c.deleteVol(vv.Name)
	return
}

func (c *Cluster) applyAddMetaPartition(cmd *RaftCmdData) (err error) {
	log.LogInfof("action[applyAddMetaPartition] cmd:%v", cmd.K)
	mpv := &MetaPartitionValue{}
	if err = json.Unmarshal(cmd.V, mpv); err != nil {
		log.LogError(fmt.Sprintf("action[applyAddMetaPartition] failed,err:%v", err))
		return
	}
	mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, mpv.ReplicaNum, mpv.VolName, mpv.VolID)
	mp.Peers = mpv.Peers
	mp.PersistenceHosts = strings.Split(mpv.Hosts, UnderlineSeparator)
	vol, err := c.getVol(mpv.VolName)
	if err != nil {
		log.LogErrorf("action[applyAddMetaPartition] failed,err:%v", err)
		return
	}
	vol.AddMetaPartitionByRaft(mp)
	return
}

func (c *Cluster) applyUpdateMetaPartition(cmd *RaftCmdData) (err error) {
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
	mp, err := vol.getMetaPartition(mpv.PartitionID)
	if err != nil {
		log.LogError(fmt.Sprintf("action[applyUpdateMetaPartition] failed,err:%v", err))
		return
	}
	mp.updateMetricByRaft(mpv)
	return
}

func (c *Cluster) applyAddDataPartition(cmd *RaftCmdData) (err error) {
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
	dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, vol.Name, vol.Id, vol.RandomWrite)
	dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
	dp.Peers = dpv.Peers
	vol.dataPartitions.putDataPartition(dp)
	return
}

func (c *Cluster) applyUpdateDataPartition(cmd *RaftCmdData) (err error) {
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
	dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
	dp.Peers = dpv.Peers
	vol.dataPartitions.putDataPartition(dp)
	return
}

func (c *Cluster) loadCompactStatus() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(ClusterPrefix))
	if err != nil {
		return errors.Annotatef(err, "action[loadCompactStatus] failed,err:%v", err)
	}
	for key := range result {
		log.LogInfof("action[loadCompactStatus] cluster[%v] key[%v]", c.Name, key)
		keys := strings.Split(key, KeySeparator)
		var status bool
		status, err = strconv.ParseBool(keys[3])
		if err != nil {
			return errors.Annotatef(err, "action[loadCompactStatus] failed,err:%v", err)
		}
		c.compactStatus = status
		log.LogInfof("action[loadCompactStatus] cluster[%v] status[%v]", c.Name, c.compactStatus)
	}
	return
}

func (c *Cluster) loadNodeSets() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(NodeSetPrefix))
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
		ns := newNodeSet(nsv.Id, DefaultNodeSetCapacity)
		ns.metaNodeLen = nsv.MetaNodeLen
		ns.dataNodeLen = nsv.DataNodeLen
		c.t.putNodeSet(ns)
		log.LogInfof("action[loadNodeSets], nsId[%v]", ns.Id)
	}
	return
}

func (c *Cluster) loadDataNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(DataNodePrefix))
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
		dataNode := NewDataNode(dnv.Addr, c.Name)
		dataNode.Id = dnv.Id
		dataNode.NodeSetId = dnv.NodeSetId
		c.dataNodes.Store(dataNode.Addr, dataNode)
		log.LogInfof("action[loadDataNodes],dataNode[%v]", dataNode.Addr)
	}
	return
}

func (c *Cluster) decodeNodeSetKey(key string) (setId uint64, err error) {
	keys := strings.Split(key, KeySeparator)
	setId, err = strconv.ParseUint(keys[2], 10, 64)
	return
}

func (c *Cluster) decodeMetaNodeKey(key string) (nodeID uint64, addr string, err error) {
	keys := strings.Split(key, KeySeparator)
	addr = keys[3]
	nodeID, err = strconv.ParseUint(keys[2], 10, 64)
	return
}

func (c *Cluster) loadMetaNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(MetaNodePrefix))
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
		metaNode := NewMetaNode(mnv.Addr, c.Name)
		metaNode.ID = mnv.Id
		metaNode.NodeSetId = mnv.NodeSetId
		c.metaNodes.Store(metaNode.Addr, metaNode)
		log.LogInfof("action[loadMetaNodes],metaNode[%v]", metaNode.Addr)
	}
	return
}

func (c *Cluster) loadVols() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(VolPrefix))
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
		vol := NewVol(vv.Id, vv.Name, vv.ReplicaNum, vv.RandomWrite, vv.DataPartitionSize, vv.Capacity)
		vol.Status = vv.Status
		c.putVol(vol)
		log.LogInfof("action[loadVols],vol[%v]", vol)
	}
	return
}

func (c *Cluster) loadMetaPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(MetaPartitionPrefix))
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
			// if vol not found,record log and continue
			log.LogErrorf("action[loadMetaPartitions] err:%v", err1.Error())
			continue
		}
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, vol.mpReplicaNum, vol.Name, mpv.VolID)
		mp.setPersistenceHosts(strings.Split(mpv.Hosts, UnderlineSeparator))
		mp.setPeers(mpv.Peers)
		vol.AddMetaPartition(mp)
		log.LogInfof("action[loadMetaPartitions],vol[%v],mp[%v]", vol.Name, mp.PartitionID)
	}
	return
}

func (c *Cluster) loadDataPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(DataPartitionPrefix))
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
			// if vol not found,record log and continue
			log.LogErrorf("action[loadDataPartitions] err:%v", err1.Error())
			continue
		}
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.VolName, dpv.VolID, dpv.RandomWrite)
		dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
		dp.Peers = dpv.Peers
		vol.dataPartitions.putDataPartition(dp)
		log.LogInfof("action[loadDataPartitions],vol[%v],dp[%v]", vol.Name, dp.PartitionID)
	}
	return
}
