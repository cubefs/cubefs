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
	"encoding/json"
	"fmt"
	bsProto "github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	OpSyncAddMetaNode          uint32 = 0x01
	OpSyncAddDataNode          uint32 = 0x02
	OpSyncAddDataPartition     uint32 = 0x03
	OpSyncAddVol               uint32 = 0x04
	OpSyncAddMetaPartition     uint32 = 0x05
	OpSyncUpdateDataPartition  uint32 = 0x06
	OpSyncUpdateMetaPartition  uint32 = 0x07
	OpSyncDeleteDataNode       uint32 = 0x08
	OpSyncDeleteMetaNode       uint32 = 0x09
	OpSyncAllocDataPartitionID uint32 = 0x0A
	OpSyncAllocMetaPartitionID uint32 = 0x0B
	OpSyncAllocMetaNodeID      uint32 = 0x0C
	OpSyncPutCluster           uint32 = 0x0D
	OpSyncUpdateVol            uint32 = 0x0E
	OpSyncDeleteVol            uint32 = 0x0F
)

const (
	KeySeparator         = "#"
	MetaNodeAcronym      = "mn"
	DataNodeAcronym      = "dn"
	DataPartitionAcronym = "dp"
	MetaPartitionAcronym = "mp"
	VolAcronym           = "vol"
	ClusterAcronym       = "c"
	MetaNodePrefix       = KeySeparator + MetaNodeAcronym + KeySeparator
	DataNodePrefix       = KeySeparator + DataNodeAcronym + KeySeparator
	DataPartitionPrefix  = KeySeparator + DataPartitionAcronym + KeySeparator
	VolPrefix            = KeySeparator + VolAcronym + KeySeparator
	MetaPartitionPrefix  = KeySeparator + MetaPartitionAcronym + KeySeparator
	ClusterPrefix        = KeySeparator + ClusterAcronym + KeySeparator
)

type MetaPartitionValue struct {
	PartitionID uint64
	ReplicaNum  uint8
	Start       uint64
	End         uint64
	Hosts       string
	Peers       []bsProto.Peer
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *MetaPartitionValue) {
	mpv = &MetaPartitionValue{
		PartitionID: mp.PartitionID,
		ReplicaNum:  mp.ReplicaNum,
		Start:       mp.Start,
		End:         mp.End,
		Hosts:       mp.hostsToString(),
		Peers:       mp.Peers,
	}
	return
}

type DataPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Hosts         string
	PartitionType string
}

func newDataPartitionValue(dp *DataPartition) (dpv *DataPartitionValue) {
	dpv = &DataPartitionValue{
		PartitionID:   dp.PartitionID,
		ReplicaNum:    dp.ReplicaNum,
		Hosts:         dp.HostsToString(),
		PartitionType: dp.PartitionType,
	}
	return
}

type VolValue struct {
	VolType    string
	ReplicaNum uint8
	Status     uint8
}

func newVolValue(vol *Vol) (vv *VolValue) {
	vv = &VolValue{
		VolType:    vol.VolType,
		ReplicaNum: vol.dpReplicaNum,
		Status:     vol.Status,
	}
	return
}

type Metadata struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func (m *Metadata) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Metadata) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

func (m *Metadata) setOpType() {
	keyArr := strings.Split(m.K, KeySeparator)

	switch keyArr[0] {
	case MaxDataPartitionIDKey:
		m.Op = OpSyncAllocDataPartitionID
		return
	case MaxMetaPartitionIDKey:
		m.Op = OpSyncAllocMetaPartitionID
		return
	case MaxMetaNodeIDKey:
		m.Op = OpSyncAllocMetaNodeID
		return
	}
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
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}

func (c *Cluster) syncPutCluster() (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncPutCluster
	metadata.K = ClusterPrefix + c.Name + KeySeparator + strconv.FormatBool(c.compactStatus)
	return c.submit(metadata)
}

//key=#vg#volName#partitionID,value=json.Marshal(DataPartitionValue)
func (c *Cluster) syncAddDataPartition(volName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncAddDataPartition, volName, dp)
}

func (c *Cluster) syncUpdateDataPartition(volName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncUpdateDataPartition, volName, dp)
}

func (c *Cluster) putDataPartitionInfo(opType uint32, volName string, dp *DataPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	metadata.K = DataPartitionPrefix + volName + KeySeparator + strconv.FormatUint(dp.PartitionID, 10)
	dpv := newDataPartitionValue(dp)
	metadata.V, err = json.Marshal(dpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *Metadata) (err error) {
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

//key=#vol#volName,value=json.Marshal(vv)
func (c *Cluster) syncAddVol(vol *Vol) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddVol
	metadata.K = VolPrefix + vol.Name
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncUpdateVol(vol *Vol) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncUpdateVol
	metadata.K = VolPrefix + vol.Name
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteVol(vol *Vol) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteVol
	metadata.K = VolPrefix + vol.Name
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

////key=#mp#volName#metaPartitionID,value=json.Marshal(MetaPartitionValue)
func (c *Cluster) syncAddMetaPartition(volName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncAddMetaPartition, volName, mp)
}

func (c *Cluster) syncUpdateMetaPartition(volName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncUpdateMetaPartition, volName, mp)
}

func (c *Cluster) putMetaPartitionInfo(opType uint32, volName string, mp *MetaPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	partitionID := strconv.FormatUint(mp.PartitionID, 10)
	metadata.K = MetaPartitionPrefix + volName + KeySeparator + partitionID
	mpv := newMetaPartitionValue(mp)
	if metadata.V, err = json.Marshal(mpv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

//key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	return c.submit(metadata)
}

//key=#dn#httpAddr,value = nil
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddDataNode
	metadata.K = DataNodePrefix + dataNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteDataNode
	metadata.K = DataNodePrefix + dataNode.Addr
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

func (c *Cluster) handleApply(cmd *Metadata) (err error) {
	log.LogInfof("action[handleApply] cmd:%v", cmd.K)
	if cmd == nil {
		return fmt.Errorf("metadata can't be null")
	}
	curIndex := c.fsm.applied
	if curIndex > 0 && curIndex%c.retainLogs == 0 {
		c.partition.Truncate(curIndex)
	}
	if c.partition.IsLeader() {
		return
	}
	switch cmd.Op {
	case OpSyncAddDataNode:
		c.applyAddDataNode(cmd)
	case OpSyncAddMetaNode:
		err = c.applyAddMetaNode(cmd)
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
	case OpSyncDeleteMetaNode:
		c.applyDeleteMetaNode(cmd)
	case OpSyncDeleteDataNode:
		c.applyDeleteDataNode(cmd)
	case OpSyncPutCluster:
		c.applyPutCluster(cmd)
	case OpSyncAllocMetaNodeID:
		c.idAlloc.increaseMetaNodeID()
	case OpSyncAllocDataPartitionID:
		c.idAlloc.increaseDataPartitionID()
	case OpSyncAllocMetaPartitionID:
		c.idAlloc.increaseMetaPartitionID()
	}
	return
}

func (c *Cluster) applyPutCluster(cmd *Metadata) {
	log.LogInfof("action[applyPutCluster] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] != ClusterAcronym {
		return
	}
	status, err := strconv.ParseBool(keys[3])
	if err != nil {
		return
	}
	c.compactStatus = status
}

func (c *Cluster) applyDeleteDataNode(cmd *Metadata) {
	log.LogInfof("action[applyDeleteDataNode] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] != DataNodeAcronym {
		return
	}
	if value, ok := c.dataNodes.Load(keys[2]); ok {
		dataNode := value.(*DataNode)
		c.delDataNodeFromCache(dataNode)
	}
}

func (c *Cluster) applyDeleteMetaNode(cmd *Metadata) {
	log.LogInfof("action[applyDeleteMetaNode] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] != MetaNodeAcronym {
		return
	}
	if value, ok := c.metaNodes.Load(keys[3]); ok {
		metaNode := value.(*MetaNode)
		c.delMetaNodeFromCache(metaNode)
	}
}

func (c *Cluster) applyAddDataNode(cmd *Metadata) {
	log.LogInfof("action[applyAddDataNode] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)

	if keys[1] == DataNodeAcronym {
		dataNode := NewDataNode(keys[2], c.Name)
		c.dataNodes.Store(dataNode.Addr, dataNode)
	}
}

func (c *Cluster) applyAddMetaNode(cmd *Metadata) (err error) {
	log.LogInfof("action[applyAddMetaNode] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	var (
		id uint64
	)
	if keys[1] == MetaNodeAcronym {
		addr := keys[3]
		if _, err = c.getMetaNode(addr); err != nil {
			metaNode := NewMetaNode(addr, c.Name)
			if id, err = strconv.ParseUint(keys[2], 10, 64); err != nil {
				return
			}
			metaNode.ID = id
			c.metaNodes.Store(metaNode.Addr, metaNode)
		}
	}
	return nil
}

func (c *Cluster) applyAddVol(cmd *Metadata) {
	log.LogInfof("action[applyAddVol] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == VolAcronym {

		vv := &VolValue{}
		if err := json.Unmarshal(cmd.V, vv); err != nil {
			log.LogError(fmt.Sprintf("action[applyAddVol] failed,err:%v", err))
			return
		}
		vol := NewVol(keys[2], vv.VolType, vv.ReplicaNum)
		c.putVol(vol)
	}
}

func (c *Cluster) applyUpdateVol(cmd *Metadata) {
	log.LogInfof("action[applyUpdateVol] cmd:%v", cmd.K)
	var (
		vol *Vol
		err error
	)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == VolAcronym {

		vv := &VolValue{}
		if err = json.Unmarshal(cmd.V, vv); err != nil {
			log.LogError(fmt.Sprintf("action[applyUpdateVol] failed,err:%v", err))
			return
		}
		if vol, err = c.getVol(keys[2]); err != nil {
			log.LogError(fmt.Sprintf("action[applyUpdateVol] failed,err:%v", err))
			return
		}
		vol.setStatus(vv.Status)
	}
}

func (c *Cluster) applyDeleteVol(cmd *Metadata) {
	log.LogInfof("action[applyDeleteVol] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == VolAcronym {
		c.deleteVol(keys[2])
	}
}

func (c *Cluster) applyAddMetaPartition(cmd *Metadata) {
	log.LogInfof("action[applyAddMetaPartition] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == MetaPartitionAcronym {
		mpv := &MetaPartitionValue{}
		if err := json.Unmarshal(cmd.V, mpv); err != nil {
			log.LogError(fmt.Sprintf("action[applyAddMetaPartition] failed,err:%v", err))
			return
		}
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, mpv.ReplicaNum, keys[2])
		mp.Lock()
		mp.Peers = mpv.Peers
		mp.PersistenceHosts = strings.Split(mpv.Hosts, UnderlineSeparator)
		mp.Unlock()
		vol, _ := c.getVol(keys[2])
		vol.AddMetaPartitionByRaft(mp)
	}
}

func (c *Cluster) applyUpdateMetaPartition(cmd *Metadata) {
	log.LogInfof("action[applyUpdateMetaPartition] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == MetaPartitionAcronym {
		mpv := &MetaPartitionValue{}
		if err := json.Unmarshal(cmd.V, mpv); err != nil {
			log.LogError(fmt.Sprintf("action[applyUpdateMetaPartition] failed,err:%v", err))
			return
		}
		vol, _ := c.getVol(keys[2])
		mp, err := vol.getMetaPartition(mpv.PartitionID)
		if err != nil {
			log.LogError(fmt.Sprintf("action[applyUpdateMetaPartition] failed,err:%v", err))
			return
		}
		mp.updateMetricByRaft(mpv)
	}
}

func (c *Cluster) applyAddDataPartition(cmd *Metadata) {
	log.LogInfof("action[applyAddDataPartition] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == DataPartitionAcronym {
		dpv := &DataPartitionValue{}
		json.Unmarshal(cmd.V, dpv)
		vol, _ := c.getVol(keys[2])
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.PartitionType, vol.Name)
		dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
		vol.dataPartitions.putDataPartitionByRaft(dp)
	}
}

func (c *Cluster) applyUpdateDataPartition(cmd *Metadata) {
	log.LogInfof("action[applyUpdateDataPartition] cmd:%v", cmd.K)
	keys := strings.Split(cmd.K, KeySeparator)
	if keys[1] == DataPartitionAcronym {
		dpv := &DataPartitionValue{}
		json.Unmarshal(cmd.V, dpv)
		vol, _ := c.getVol(keys[2])
		if _, err := vol.getDataPartitionByID(dpv.PartitionID); err != nil {
			log.LogError(fmt.Sprintf("action[applyUpdateDataPartition] failed,err:%v", err))
			return
		}
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.PartitionType, vol.Name)
		dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
		vol.dataPartitions.putDataPartitionByRaft(dp)
	}
}

func (c *Cluster) decodeDataPartitionKey(key string) (acronym, volName string) {
	return c.decodeAcronymAndNsName(key)
}

func (c *Cluster) decodeMetaPartitionKey(key string) (acronym, volName string) {
	return c.decodeAcronymAndNsName(key)
}

func (c *Cluster) decodeVolKey(key string) (acronym, volName string, err error) {
	arr := strings.Split(key, KeySeparator)
	acronym = arr[1]
	volName = arr[2]
	return
}

func (c *Cluster) decodeAcronymAndNsName(key string) (acronym, volName string) {
	arr := strings.Split(key, KeySeparator)
	acronym = arr[1]
	volName = arr[2]
	return
}

func (c *Cluster) loadCompactStatus() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(ClusterPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		keys := strings.Split(string(encodedKey.Data()), KeySeparator)
		var status bool
		status, err = strconv.ParseBool(keys[3])
		if err != nil {
			return errors.Annotatef(err, "action[loadCompactStatus] failed,err:%v", err)
		}
		c.compactStatus = status
		encodedKey.Free()
	}
	return
}

func (c *Cluster) loadDataNodes() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(DataNodePrefix)
	it.Seek(prefixKey)

	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		keys := strings.Split(string(encodedKey.Data()), KeySeparator)
		dataNode := NewDataNode(keys[2], c.Name)
		c.dataNodes.Store(dataNode.Addr, dataNode)
		encodedKey.Free()
	}
	return
}

func (c *Cluster) decodeMetaNodeKey(key string) (nodeID uint64, addr string, err error) {
	keys := strings.Split(key, KeySeparator)
	addr = keys[3]
	nodeID, err = strconv.ParseUint(keys[2], 10, 64)
	return
}

func (c *Cluster) loadMetaNodes() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(MetaNodePrefix)
	it.Seek(prefixKey)

	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		nodeID, addr, err1 := c.decodeMetaNodeKey(string(encodedKey.Data()))
		if err1 != nil {
			err = fmt.Errorf("action[loadMetaNodes] err:%v", err1.Error())
			return err
		}
		metaNode := NewMetaNode(addr, c.Name)
		metaNode.ID = nodeID
		c.metaNodes.Store(addr, metaNode)
		encodedKey.Free()
	}
	return
}

func (c *Cluster) loadVols() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(VolPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, volName, err1 := c.decodeVolKey(string(encodedKey.Data()))
		if err1 != nil {
			err = fmt.Errorf("action[loadVols] err:%v", err1.Error())
			return err
		}
		vv := &VolValue{}
		if err = json.Unmarshal(encodedValue.Data(), vv); err != nil {
			err = fmt.Errorf("action[loadVols],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		vol := NewVol(volName, vv.VolType, vv.ReplicaNum)
		vol.Status = vv.Status
		c.putVol(vol)
		encodedKey.Free()
	}
	return
}

func (c *Cluster) loadMetaPartitions() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(MetaPartitionPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, volName := c.decodeMetaPartitionKey(string(encodedKey.Data()))
		vol, err1 := c.getVol(volName)
		if err1 != nil {
			// if vol not found,record log and continue
			err = fmt.Errorf("action[loadMetaPartitions] err:%v", err1.Error())
			continue
		}
		mpv := &MetaPartitionValue{}
		if err = json.Unmarshal(encodedValue.Data(), mpv); err != nil {
			err = fmt.Errorf("action[decodeMetaPartitionValue],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, vol.mpReplicaNum, volName)
		mp.Lock()
		mp.setPersistenceHosts(strings.Split(mpv.Hosts, UnderlineSeparator))
		mp.setPeers(mpv.Peers)
		mp.Unlock()
		vol.AddMetaPartition(mp)
		encodedKey.Free()
		encodedValue.Free()
	}
	return
}

func (c *Cluster) loadDataPartitions() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(DataPartitionPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, volName := c.decodeDataPartitionKey(string(encodedKey.Data()))
		vol, err1 := c.getVol(volName)
		if err1 != nil {
			// if vol not found,record log and continue
			err = fmt.Errorf("action[loadDataPartitions] err:%v", err1.Error())
			continue
		}
		dpv := &DataPartitionValue{}
		if err = json.Unmarshal(encodedValue.Data(), dpv); err != nil {
			err = fmt.Errorf("action[decodeDataPartitionValue],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.PartitionType, volName)
		dp.Lock()
		dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
		dp.Unlock()
		vol.dataPartitions.putDataPartition(dp)
		encodedKey.Free()
		encodedValue.Free()
	}
	return
}
