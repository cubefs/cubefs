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
	"net/http"
	"strconv"

	"bytes"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"regexp"
	"strings"
)

// ClusterView provides the view of a cluster.
type ClusterView struct {
	Name               string
	LeaderAddr         string
	DisableAutoAlloc   bool
	MetaNodeThreshold  float32
	Applied            uint64
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	MaxMetaPartitionID uint64
	DataNodeStatInfo   *nodeStatInfo
	MetaNodeStatInfo   *nodeStatInfo
	VolStatInfo        []*volStatInfo
	BadPartitionIDs    []badPartitionView
	MetaNodes          []NodeView
	DataNodes          []NodeView
}

// VolStatView provides the view of the volume.
type VolStatView struct {
	Name      string
	Total     uint64 `json:"TotalGB"`
	Used      uint64 `json:"UsedGB"`
	Increased uint64 `json:"IncreasedGB"`
}

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	NodeSet map[uint64]*nodeSetView
}

type nodeSetView struct {
	Racks     []*RackView
	MetaNodes []NodeView
}

func newNodeSetView() *nodeSetView {
	return &nodeSetView{Racks: make([]*RackView, 0), MetaNodes: make([]NodeView, 0)}
}

//RackView define the view of rack
type RackView struct {
	Name      string
	DataNodes []NodeView
}

func newRackView() *RackView {
	return &RackView{DataNodes: make([]NodeView, 0)}
}

type badPartitionView struct {
	DiskPath     string
	PartitionIDs []uint64
}

// Set the threshold of the memory usage on each meta node.
// If the memory usage reaches this threshold, then all the mata partition will be marked as readOnly.
func (m *Server) setMetaNodeThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
	if threshold, err = parseAndExtractThreshold(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setMetaNodeThreshold(float32(threshold)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set threshold to %v successfully", threshold)))
}

// Turn on or off the automatic allocation of the data partitions.
// If DisableAutoAllocate == off, then we WILL NOT automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
//
// If DisableAutoAllocate == on, then we WILL automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
func (m *Server) setupAutoAllocation(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	if status, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setDisableAutoAllocate(status); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set DisableAutoAllocate to %v successfully", status)))
}

// View the topology of the cluster.
func (m *Server) getTopology(w http.ResponseWriter, r *http.Request) {
	tv := &TopologyView{
		NodeSet: make(map[uint64]*nodeSetView, 0),
	}
	for _, ns := range m.cluster.t.nodeSetMap {
		nsView := newNodeSetView()
		tv.NodeSet[ns.ID] = nsView
		ns.metaNodes.Range(func(key, value interface{}) bool {
			metaNode := value.(*MetaNode)
			nsView.MetaNodes = append(nsView.MetaNodes, NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
			return true
		})
		for _, rack := range ns.rackMap {
			rv := newRackView()
			rv.Name = rack.name
			nsView.Racks = append(nsView.Racks, rv)
			rack.dataNodes.Range(func(key, value interface{}) bool {
				dataNode := value.(*DataNode)
				rv.DataNodes = append(rv.DataNodes, NodeView{ID: dataNode.ID, Addr: dataNode.Addr, Status: dataNode.isActive, IsWritable: dataNode.isWriteAble()})
				return true
			})
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(tv))
}

func (m *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	cv := &ClusterView{
		Name:               m.cluster.Name,
		LeaderAddr:         m.leaderInfo.addr,
		DisableAutoAlloc:   m.cluster.DisableAutoAllocate,
		MetaNodeThreshold:  m.cluster.cfg.MetaNodeThreshold,
		Applied:            m.fsm.applied,
		MaxDataPartitionID: m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:      m.cluster.idAlloc.commonID,
		MaxMetaPartitionID: m.cluster.idAlloc.metaPartitionID,
		MetaNodes:          make([]NodeView, 0),
		DataNodes:          make([]NodeView, 0),
		VolStatInfo:        make([]*volStatInfo, 0),
		BadPartitionIDs:    make([]badPartitionView, 0),
	}

	vols := m.cluster.allVolNames()
	cv.MetaNodes = m.cluster.allMetaNodes()
	cv.DataNodes = m.cluster.allDataNodes()
	cv.DataNodeStatInfo = m.cluster.dataNodeStatInfo
	cv.MetaNodeStatInfo = m.cluster.metaNodeStatInfo
	for _, name := range vols {
		stat, ok := m.cluster.volStatInfo.Load(name)
		if !ok {
			cv.VolStatInfo = append(cv.VolStatInfo, newVolStatInfo(name, 0, 0, "0.0001"))
			continue
		}
		cv.VolStatInfo = append(cv.VolStatInfo, stat.(*volStatInfo))
	}
	m.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{DiskPath: path, PartitionIDs: badDataPartitionIds}
		cv.BadPartitionIDs = append(cv.BadPartitionIDs, bpv)
		return true
	})
	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *Server) getIPAddr(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{Cluster: m.cluster.Name, Ip: strings.Split(r.RemoteAddr, ":")[0]}
	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		start   uint64
		err     error
	)

	if volName, start, err = validateRequestToCreateMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.updateInodeIDRange(volName, start); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprint("create meta partition successfully")))
}

func (m *Server) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                     string
		volName                    string
		vol                        *Vol
		reqCreateCount             int
		lastTotalDataPartitions    int
		clusterTotalDataPartitions int
		err                        error
	)

	if reqCreateCount, volName, err = parseRequestToCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	lastTotalDataPartitions = len(vol.dataPartitions.partitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()
	for i := 0; i < reqCreateCount; i++ {
		if _, err = m.cluster.createDataPartition(volName); err != nil {
			break
		}
	}

	rstMsg = fmt.Sprintf(" createDataPartition succeeeds. "+
		"clusterLastTotalDataPartitions[%v],vol[%v] has %v data partitions previously and %v data partitions now",
		clusterTotalDataPartitions, volName, lastTotalDataPartitions, len(vol.dataPartitions.partitions))
	if err != nil {
		rstMsg = fmt.Sprintf("%v \n err[%v]", rstMsg, err.Error())
	}
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) getDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		err         error
	)
	if partitionID, err = parseRequestToGetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(dp))
}

// Load the data partition.
func (m *Server) loadDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, err = parseRequestToLoadDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	m.cluster.loadDataPartition(dp)
	msg = fmt.Sprintf(proto.AdminLoadDataPartition+"partitionID :%v  load data partition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Decommission a data partition. This usually happens when disk error has been reported.
// This function needs to be called manually by the admin.
func (m *Server) decommissionDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		err         error
	)

	if addr, partitionID, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if err = m.cluster.decommissionDataPartition(addr, dp, handleDataPartitionOfflineErr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		err     error
		msg     string
	)

	if name, authKey, err = parseRequestToDeleteVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.markDeleteVol(name, authKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("delete vol[%v] successfully,from[%v]", name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) updateVol(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		authKey  string
		err      error
		msg      string
		capacity int
	)
	if name, authKey, capacity, err = parseRequestToUpdateVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.updateVol(name, authKey, capacity); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		owner    string
		err      error
		msg      string
		size     int
		mpCount  int
		capacity int
		vol      *Vol
	)

	if name, owner, mpCount, size, capacity, err = parseRequestToCreateVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.createVol(name, owner, mpCount, size, capacity); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("create vol[%v] successfully, has allocate [%v] data partitions", name, len(vol.dataPartitions.partitions))
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) getVolSimpleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		name    string
		vol     *Vol
		volView *proto.SimpleVolView
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	volView = newSimpleView(vol)
	sendOkReply(w, r, newSuccessHTTPReply(volView))
}

func newSimpleView(vol *Vol) *proto.SimpleVolView {
	return &proto.SimpleVolView{
		ID:           vol.ID,
		Name:         vol.Name,
		Owner:        vol.Owner,
		DpReplicaNum: vol.dpReplicaNum,
		MpReplicaNum: vol.mpReplicaNum,
		Status:       vol.Status,
		Capacity:     vol.Capacity,
		RwDpCnt:      vol.dataPartitions.readableAndWritableCnt,
		MpCnt:        len(vol.MetaPartitions),
		DpCnt:        len(vol.dataPartitions.partitionMap),
	}
}

func (m *Server) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if id, err = m.cluster.addDataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		dataNode *DataNode
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(dataNode))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) dataNodeOffline(w http.ResponseWriter, r *http.Request) {
	var (
		node        *DataNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.dataNodeOffLine(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission data node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a disk. This will decommission all the data partitions on this disk.
func (m *Server) decommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		node                  *DataNode
		rstMsg                string
		offLineAddr, diskPath string
		err                   error
		badPartitionIds       []uint64
	)

	if offLineAddr, diskPath, err = parseRequestToDecommissionNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	badPartitionIds = node.badPartitionIDs(diskPath)
	if len(badPartitionIds) == 0 {
		err = fmt.Errorf("node[%v] disk[%v] does not have any data partition", node.Addr, diskPath)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("recive decommissionDisk node[%v] disk[%v], badPartitionIds[%v] has offline successfully",
		node.Addr, diskPath, badPartitionIds)
	if err = m.cluster.decommissionDisk(node, diskPath, badPartitionIds); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	Warn(m.clusterName, rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// handle tasks such as heartbeat，loadDataPartition，deleteDataPartition, etc.
func (m *Server) handleDataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleDataNodeTaskResponse(tr.OperatorAddr, tr)
}

func (m *Server) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if id, err = m.cluster.addMetaNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		metaNode *MetaNode
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(metaNode))
}

func (m *Server) decommissionMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		nodeAddr    string
		mp          *MetaPartition
		msg         string
		err         error
	)
	if nodeAddr, partitionID, err = parseRequestToDecommissionMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if err = m.cluster.decommissionMetaPartition(nodeAddr, mp); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf(proto.AdminLoadMetaPartition+" partitionID :%v  decommissionMetaPartition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) loadMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, err = parseRequestToLoadMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp)
	msg = fmt.Sprintf(proto.AdminLoadMetaPartition+" partitionID :%v Load successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) decommissionMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	m.cluster.decommissionMetaNode(metaNode)
	rstMsg = fmt.Sprintf("decommissionMetaNode metaNode [%v] has offline successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) handleMetaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleMetaNodeTaskResponse(tr.OperatorAddr, tr)
}

// Dynamically add a raft node (replica) for the master.
// By using this function, there is no need to stop all the master services. Adding a new raft node is performed online.
func (m *Server) addRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successfully \n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Dynamically remove a master node. Similar to addRaftNode, this operation is performed online.
func (m *Server) removeRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successfully\n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Parse the request that adds/deletes a raft node.
func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var idStr string
	if idStr = r.FormValue(idKey); idStr == "" {
		err = keyNotFound(idKey)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		return
	}

	if arr := strings.Split(host, colonSplit); len(arr) < 2 {
		err = unmatchedKey(addrKey)
		return
	}
	return
}

func parseAndExtractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractNodeAddr(r)
}

func parseRequestToDecommissionNode(r *http.Request) (nodeAddr, diskPath string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	return
}

func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(body)))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseRequestToGetVol(r *http.Request) (name, authKey string, err error) {
	return parseVolNameAndAuthKey(r)
}

func parseVolNameAndAuthKey(r *http.Request) (name, authKey string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	return

}

func parseRequestToDeleteVol(r *http.Request) (name, authKey string, err error) {
	return parseVolNameAndAuthKey(r)

}

func parseRequestToUpdateVol(r *http.Request) (name, authKey string, capacity int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	if capacityStr := r.FormValue(volCapacityKey); capacityStr != "" {
		if capacity, err = strconv.Atoi(capacityStr); err != nil {
			err = unmatchedKey(volCapacityKey)
		}
	} else {
		err = keyNotFound(volCapacityKey)
	}
	return
}

func parseRequestToCreateVol(r *http.Request) (name, owner string, mpCount, size, capacity int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if owner = r.FormValue(volOwnerKey); owner == "" {
		err = keyNotFound(volOwnerKey)
		return
	}

	if mpCountStr := r.FormValue(metaPartitionCountKey); mpCountStr != "" {
		if mpCount, err = strconv.Atoi(mpCountStr); err != nil {
			mpCount = defaultInitMetaPartitionCount
		}
	}

	if sizeStr := r.FormValue(dataPartitionSizeKey); sizeStr != "" {
		if size, err = strconv.Atoi(sizeStr); err != nil {
			err = unmatchedKey(dataPartitionSizeKey)
		}
	}

	if capacityStr := r.FormValue(volCapacityKey); capacityStr == "" {
		err = keyNotFound(volCapacityKey)
		return
	} else if capacity, err = strconv.Atoi(capacityStr); err != nil {
		err = unmatchedKey(volCapacityKey)
	}
	return
}

func parseRequestToCreateDataPartition(r *http.Request) (count int, name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if countStr := r.FormValue(countKey); countStr == "" {
		err = keyNotFound(countKey)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = unmatchedKey(countKey)
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	return
}

func parseRequestToGetDataPartition(r *http.Request) (ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractDataPartitionID(r)
}

func parseRequestToLoadDataPartition(r *http.Request) (ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	return
}

func extractDataPartitionID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseRequestToDecommissionDataPartition(r *http.Request) (nodeAddr string, ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func extractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(addrKey); nodeAddr == "" {
		err = keyNotFound(addrKey)
		return
	}
	return
}

func extractDiskPath(r *http.Request) (diskPath string, err error) {
	if diskPath = r.FormValue(diskPathKey); diskPath == "" {
		err = keyNotFound(diskPathKey)
		return
	}
	return
}

func parseRequestToLoadMetaPartition(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func parseRequestToDecommissionMetaPartition(r *http.Request) (nodeAddr string, partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func parseAndExtractStatus(r *http.Request) (status bool, err error) {

	if err = r.ParseForm(); err != nil {
		return
	}
	return extractStatus(r)
}

func extractStatus(r *http.Request) (status bool, err error) {
	var value string
	if value = r.FormValue(enableKey); value == "" {
		err = keyNotFound(enableKey)
		return
	}
	if status, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func parseAndExtractThreshold(r *http.Request) (threshold float64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(thresholdKey); value == "" {
		err = keyNotFound(thresholdKey)
		return
	}
	if threshold, err = strconv.ParseFloat(value, 64); err != nil {
		return
	}
	return
}

func validateRequestToCreateMetaPartition(r *http.Request) (volName string, start uint64, err error) {
	if volName, err = extractName(r); err != nil {
		return
	}

	var value string
	if value = r.FormValue(startKey); value == "" {
		err = keyNotFound(startKey)
		return
	}
	start, err = strconv.ParseUint(value, 10, 64)
	return
}

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	if err == nil {
		return newSuccessHTTPReply("")
	}
	code, ok := proto.Err2CodeMap[err]
	if ok {
		return &proto.HTTPReply{Code: code, Msg: err.Error()}
	}
	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

func sendReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	switch httpReply.Data.(type) {
	case *DataPartition:
		dp := httpReply.Data.(*DataPartition)
		dp.RLock()
		defer dp.RUnlock()
	case *MetaPartition:
		mp := httpReply.Data.(*MetaPartition)
		mp.RLock()
		defer mp.RUnlock()
	case *MetaNode:
		mn := httpReply.Data.(*MetaNode)
		mn.RLock()
		defer mn.RUnlock()
	case *DataNode:
		dn := httpReply.Data.(*DataNode)
		dn.RLock()
		defer dn.RUnlock()
	}
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	if err := sendReply(w, r, httpReply); err == nil {
		log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	}
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	sendReply(w, r, httpReply)
}

// Obtain all the data partitions in a volume.
func (m *Server) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		name string
		vol  *Vol
		err  error
	)
	dps := proto.NewDataPartitionsView()
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if body, err = vol.getDataPartitionsView(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = json.Unmarshal(body, dps); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	sendOkReply(w, r, newSuccessHTTPReply(dps))
}

func (m *Server) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		name    string
		authKey string
		vol     *Vol
		volView *proto.VolView
	)
	if name, authKey, err = parseRequestToGetVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if !matchKey(vol.Owner, authKey) {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolAuthKeyNotMatch))
		return
	}
	if volView, err = m.getVolView(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(volView))
}

// Obtain the volume information such as total capacity and used space, etc.
func (m *Server) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(volStat(vol)))
}

func (m *Server) getVolView(vol *Vol) (view *proto.VolView, err error) {
	view = proto.NewVolView(vol.Name, vol.Status)
	setMetaPartitions(vol, view)
	setDataPartitions(vol, view)
	return
}
func setDataPartitions(vol *Vol, view *proto.VolView) (err error) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	view.DataPartitions = vol.dataPartitions.getDataPartitionsView(0)
	return
}
func setMetaPartitions(vol *Vol, view *proto.VolView) (err error) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		view.MetaPartitions = append(view.MetaPartitions, getMetaPartitionView(mp))
	}
	return
}

func volStat(vol *Vol) (stat *proto.VolStatInfo) {
	stat = new(proto.VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.totalUsedSpace()
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *proto.MetaPartitionView) {
	mpView = proto.NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.Lock()
	defer mp.Unlock()
	for _, metaReplica := range mp.Replicas {
		mpView.Members = append(mpView.Members, metaReplica.Addr)
		if metaReplica.IsLeader {
			mpView.LeaderAddr = metaReplica.Addr
		}
	}
	return
}

func (m *Server) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		partitionID uint64
		mp          *MetaPartition
	)
	if partitionID, err = parseAndExtractPartitionInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(mp))
}

func parseAndExtractPartitionInfo(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func extractMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractAuthKey(r *http.Request) (authKey string, err error) {
	if authKey = r.FormValue(volAuthKey); authKey == "" {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseAndExtractName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractName(r)
}

func extractName(r *http.Request) (name string, err error) {
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		return
	}

	pattern := "^[a-zA-Z0-9_-]{3,256}$"
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}

	if !reg.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}
