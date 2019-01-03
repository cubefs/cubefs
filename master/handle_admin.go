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


/* TODO is handle_admin.go a good name ? */
/* TODO double check the messages */

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"bytes"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"io/ioutil"
	"strings"
)

// ClusterView provides the view of a cluster.
type ClusterView struct {
	Name               string
	LeaderAddr         string
	CompactStatus      bool
	DisableAutoAlloc   bool
	Applied            uint64  // TODO what is Applied?
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	MaxMetaPartitionID uint64
	DataNodeStatInfo       *nodeStatInfo
	MetaNodeStatInfo       *nodeStatInfo
	VolStatInfo            []*volStatInfo
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
	Addr   string
	Status bool
	ID     uint64
}

//// TODO why not call them NodeView ?
//// NodeView provides the view of the data node
//type DataNodeView struct {
//	Addr   string
//	Status bool
//	ID     uint64
//}
//
//// NodeView provides the view of the meta node
//type MetaNodeView struct {
//	ID     uint64
//	Addr   string
//	Status bool
//}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	DataNodes []NodeView
	MetaNodes []NodeView
	NodeSet   []uint64
}

type badPartitionView struct {
	DiskPath     string
	PartitionIDs []uint64
}

// TODO explain "该阈值并不会被持久化且同步到其它master节点"
// Set the threshold of the memory usage on each meta node.
// If the memory usage reaches this threshold, them all the mata partition will be marked as readOnly.
func (m *Server) setMetaNodeThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
	if threshold, err = parseAndExtractThreshold(r); err != nil {
		goto errHandler
	}
	m.cluster.cfg.MetaNodeThreshold = float32(threshold)
	m.sendOkReply(w, r, fmt.Sprintf("set threshold to %v successfully", threshold))
	return
errHandler:
	logMsg := newLogMsg("setMetaNodeThreshold", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Turn on or off the automatic allocation of the data partitions.
// If AutoAllocationSwitch == off, then we WILL NOT automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
//
// If AutoAllocationSwitch == on, then we WILL automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
func (m *Server) setupAutoAllocation(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	if status, err = parseAndExtractStatus(r); err != nil {
		goto errHandler
	}
	m.cluster.AutoAllocationSwitch = status
	io.WriteString(w, fmt.Sprintf("set AutoAllocationSwitch to %v successfully", status))
	return
errHandler:
	logMsg := newLogMsg("setupAutoAllocation", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// View the topology of the cluster.
func (m *Server) getTopology(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)
	tv := &TopologyView{
		DataNodes: make([]NodeView, 0),
		MetaNodes: make([]NodeView, 0),
		NodeSet:   make([]uint64, 0),
	}
	m.cluster.t.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*topoMetaNode)
		tv.MetaNodes = append(tv.MetaNodes, NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive})
		return true
	})
	m.cluster.t.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*topoDataNode)
		tv.DataNodes = append(tv.DataNodes, NodeView{ID: dataNode.ID, Addr: dataNode.Addr, Status: dataNode.isActive})
		return true
	})
	for _, ns := range m.cluster.t.nodeSetMap {
		tv.NodeSet = append(tv.NodeSet, ns.ID)
	}
	if body, err = json.Marshal(tv); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, string(body))
	return

errHandler:
	logMsg := newLogMsg("getCluster", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)
	cv := &ClusterView{
		Name:               m.cluster.Name,
		LeaderAddr:         m.leaderInfo.addr,
		CompactStatus:      m.cluster.compactStatus,
		DisableAutoAlloc:   m.cluster.AutoAllocationSwitch,
		Applied:            m.fsm.applied,
		MaxDataPartitionID: m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:      m.cluster.idAlloc.commonID,
		MaxMetaPartitionID: m.cluster.idAlloc.metaPartitionID,
		MetaNodes:          make([]NodeView, 0),
		DataNodes:          make([]NodeView, 0),
		VolStatInfo:            make([]*volStatInfo, 0),
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

	if body, err = json.Marshal(cv); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, string(body))
	return

errHandler:
	logMsg := newLogMsg("getCluster", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) getIPAddr(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{Cluster: m.cluster.Name, Ip: strings.Split(r.RemoteAddr, ":")[0]}
	cInfoBytes, err := json.Marshal(cInfo)
	if err != nil {
		goto errHandler
	}
	w.Write(cInfoBytes)
	return
errHandler:
	rstMsg := newLogMsg("getIPAddr", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, rstMsg, err)
	return
}


// TODO see the following example of inline. This looks better than several layers of wrappers.
// TODO explain how the meta partition is created here
func (m *Server) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		value string
		start   uint64
		rstMsg  string
		err     error
	)

	if volName, err = extractName(r); err != nil {
		goto errHandler
	}

	if value = r.FormValue(startkey); value == "" {
		err = keyNotFound(startkey)
		goto errHandler
	}

	if start, err = strconv.ParseUint(value, 10, 64); err != nil {
		goto errHandler
	}

	//if volName, start, err = validateRequestToCreateMetaPartition(r); err != nil {
	//
	//}

	// TODO explain
	if err = m.cluster.updateUpperBoundOfInodeIds(volName, start); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, fmt.Sprint("create meta partition successfully"))
	return
errHandler:
	rstMsg = newLogMsg("createMetaPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, rstMsg, err)
	return
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
		goto errHandler
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errHandler
	}
	lastTotalDataPartitions = len(vol.dataPartitions.partitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()
	for i := 0; i < reqCreateCount; i++ {
		if _, err = m.cluster.createDataPartition(volName); err != nil {
			break
		}
	}

	rstMsg = fmt.Sprintf(" createDataPartition succeeeds. " +
		"clusterLastTotalDataPartitions[%v],vol[%v] has %v data partitionMap previously and %v data partitionMap now",
		clusterTotalDataPartitions, volName, lastTotalDataPartitions, len(vol.dataPartitions.partitions))
	m.sendOkReply(w, r, rstMsg)
	return
errHandler:
	rstMsg = newLogMsg("createDataPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, rstMsg, err)
	return
}

func (m *Server) getDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		dp          *DataPartition
		partitionID uint64
		err         error
	)
	if partitionID, err = parseRequestToGetDataPartition(r); err != nil {
		goto errHandler
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		goto errHandler
	}
	if body, err = dp.toJSON(); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, string(body))
	return
errHandler:
	logMsg := newLogMsg("getDataPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Load the data partition.
func (m *Server) loadDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		vol         *Vol
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, volName, err = parseRequestToLoadDataPartition(r); err != nil {
		goto errHandler
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errHandler
	}
	if dp, err = vol.getDataPartitionByID(partitionID); err != nil {
		goto errHandler
	}

	m.cluster.loadDataPartition(dp)
	msg = fmt.Sprintf(adminLoadDataPartition+"partitionID :%v  load data partition successfully", partitionID)
	m.sendOkReply(w, r, msg)
	return
errHandler:
	logMsg := newLogMsg(adminLoadDataPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Decommission a data partition. This usually happens when disk error has been reported.
// This function needs to be called manually by the admin.
func (m *Server) decommissionDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		vol         *Vol
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		err         error
	)

	if addr, partitionID, volName, err = parseRequestToDecommissionDataPartition(r); err != nil {
		goto errHandler
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errHandler
	}
	if dp, err = vol.getDataPartitionByID(partitionID); err != nil {
		goto errHandler
	}
	if err = m.cluster.decommissionDataPartition(addr, volName, dp, handleDataPartitionOfflineErr); err != nil {
		goto errHandler
	}
	rstMsg = fmt.Sprintf(adminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	m.sendOkReply(w, r, rstMsg)
	return
errHandler:
	logMsg := newLogMsg(adminDecommissionDataPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
		msg  string
	)

	if name, err = parseRequestToDeleteVol(r); err != nil {
		goto errHandler
	}
	if err = m.cluster.markDeleteVol(name); err != nil {
		goto errHandler
	}
	msg = fmt.Sprintf("delete vol[%v] successfully,from[%v]", name, r.RemoteAddr)
	log.LogWarn(msg)
	m.sendOkReply(w, r, msg)
	return

errHandler:
	logMsg := newLogMsg("markDeleteVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) updateVol(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		err      error
		msg      string
		capacity int
	)
	if name, capacity, err = parseRequestToUpdateVol(r); err != nil {
		goto errHandler
	}
	if err = m.cluster.updateVol(name, capacity); err != nil {
		goto errHandler
	}
	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	m.sendOkReply(w, r, msg)
	return
errHandler:
	logMsg := newLogMsg("updateVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		name        string
		err         error
		msg         string
		randomWrite bool
		replicaNum  int
		size        int
		capacity    int
		vol         *Vol
	)

	if name, replicaNum, randomWrite, size, capacity, err = parseRequestToCreateVol(r); err != nil {
		goto errHandler
	}
	if err = m.cluster.createVol(name, uint8(replicaNum), randomWrite, size, capacity); err != nil {
		goto errHandler
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		goto errHandler
	}
	msg = fmt.Sprintf("create vol[%v] successfully, has allocate [%v] data partitionMap", name, len(vol.dataPartitions.partitions))
	m.sendOkReply(w, r, msg)
	return

errHandler:
	logMsg := newLogMsg("createVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		goto errHandler
	}

	if id, err = m.cluster.addDataNode(nodeAddr); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, fmt.Sprintf("%v", id))
	return
errHandler:
	logMsg := newLogMsg("addDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) getDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		dataNode *DataNode
		body     []byte
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		goto errHandler
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	if body, err = dataNode.toJSON(); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, string(body))
	return
errHandler:
	logMsg := newLogMsg("dataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
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
		goto errHandler
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		goto errHandler
	}
	if err = m.cluster.dataNodeOffLine(node); err != nil {
		goto errHandler
	}
	rstMsg = fmt.Sprintf("decommission data node [%v] successfully", offLineAddr)
	m.sendOkReply(w, r, rstMsg)
	return
errHandler:
	logMsg := newLogMsg("decommissionDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
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
		goto errHandler
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		goto errHandler
	}
	badPartitionIds = node.getBadDiskPartitions(diskPath)
	if len(badPartitionIds) == 0 {
		err = fmt.Errorf("node[%v] disk[%v] does not have any data partition", node.Addr, diskPath)
		goto errHandler
	}
	rstMsg = fmt.Sprintf("recive decommissionDisk node[%v] disk[%v], badPartitionIds[%v] has offline successfully",
		node.Addr, diskPath, badPartitionIds)
	m.cluster.BadDataPartitionIds.Store(fmt.Sprintf("%s:%s", offLineAddr, diskPath), badPartitionIds)
	if err = m.cluster.diskOffLine(node, diskPath, badPartitionIds); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, rstMsg)
	Warn(m.clusterName, rstMsg)
	return
errHandler:
	logMsg := newLogMsg("decommissionDisk", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Returns the response of tasks such as heartbeat，loadDataPartition，deleteDataPartition, etc.
func (m *Server) getDataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		dataNode *DataNode
		code     = http.StatusOK
		tr       *proto.AdminTask
		err      error
	)

	if tr, err = parseRequestToGetTaskResponse(r); err != nil {
		code = http.StatusBadRequest
		goto errHandler
	}
	io.WriteString(w, fmt.Sprintf("%v", http.StatusOK))
	if dataNode, err = m.cluster.dataNode(tr.OperatorAddr); err != nil {
		code = http.StatusInternalServerError
		goto errHandler
	}

	m.cluster.handleDataNodeTaskResponse(dataNode.Addr, tr)

	return

errHandler:
	logMsg := newLogMsg("getDataNodeTaskResponse", r.RemoteAddr, err.Error(),
		http.StatusBadRequest)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func (m *Server) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		goto errHandler
	}

	if id, err = m.cluster.addMetaNode(nodeAddr); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, fmt.Sprintf("%v", id))
	return
errHandler:
	logMsg := newLogMsg("addMetaNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

//func parseAddNodeRequest(r *http.Request) (nodeAddr string, err error) {
//	r.ParseForm()
//	return extractNodeAddr(r)
//}

//func parseAddMetaNodePara(r *http.Request) (nodeAddr string, err error) {
//	r.ParseForm()
//	return extractNodeAddr(r)
//}
//
//func parseAddDataNodePara(r *http.Request) (nodeAddr string, err error) {
//	r.ParseForm()
//	return extractNodeAddr(r)
//}

func (m *Server) getMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		metaNode *MetaNode
		body     []byte
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		goto errHandler
	}

	if metaNode, err = m.cluster.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	if body, err = metaNode.toJSON(); err != nil {
		goto errHandler
	}
	m.sendOkReply(w, r, string(body))
	return
errHandler:
	logMsg := newLogMsg("dataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) decommissionMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID       uint64
		volName, nodeAddr string
		msg               string
		err               error
	)
	if volName, nodeAddr, partitionID, err = parseRequestToDecommissionMetaPartition(r); err != nil {
		goto errHandler
	}

	if err = m.cluster.metaPartitionOffline(volName, nodeAddr, partitionID); err != nil {
		goto errHandler
	}
	msg = fmt.Sprintf(adminLoadMetaPartition+" partitionID :%v  decommissionMetaPartition successfully", partitionID)
	m.sendOkReply(w, r, msg)
	return
errHandler:
	logMsg := newLogMsg(adminDecommissionMetaPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) loadMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		vol         *Vol
		msg         string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, volName, err = parseRequestToLoadMetaPartition(r); err != nil {
		goto errHandler
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errHandler
	}
	if mp, err = vol.metaPartition(partitionID); err != nil {
		goto errHandler
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp)
	msg = fmt.Sprintf(adminLoadMetaPartition+" partitionID :%v Load successfully", partitionID)
	m.sendOkReply(w, r, msg)
	return
errHandler:
	logMsg := newLogMsg(adminLoadMetaPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) decommissionMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		goto errHandler
	}

	if metaNode, err = m.cluster.metaNode(offLineAddr); err != nil {
		goto errHandler
	}
	m.cluster.metaNodeOffLine(metaNode)
	rstMsg = fmt.Sprintf("decommissionMetaNode metaNode [%v] has offline successfully", offLineAddr)
	m.sendOkReply(w, r, rstMsg)
	return
errHandler:
	logMsg := newLogMsg("decommissionMetaNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Server) getMetaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode *MetaNode
		code     = http.StatusOK
		tr       *proto.AdminTask
		err      error
	)

	if tr, err = parseRequestToGetTaskResponse(r); err != nil {
		code = http.StatusBadRequest
		goto errHandler
	}

	io.WriteString(w, fmt.Sprintf("%v", http.StatusOK))

	if metaNode, err = m.cluster.metaNode(tr.OperatorAddr); err != nil {
		code = http.StatusInternalServerError
		goto errHandler
	}
	m.cluster.dealMetaNodeTaskResponse(metaNode.Addr, tr)
	return

errHandler:
	logMsg := newLogMsg("getMetaNodeTaskResponse", r.RemoteAddr, err.Error(),
		http.StatusBadRequest)
	HandleError(logMsg, err, code, w)
	return
}

// Dynamically add a raft node (replica) for the master.
// By using this function, there is no need to stop all the master services. Adding a new raft node is performed online.
func (m *Server) addRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
	if err != nil {
		goto errHandler
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		goto errHandler
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successfully \n", id, addr)
	m.sendOkReply(w, r, msg)
	return
errHandler:
	logMsg := newLogMsg("add raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Dynamically remove a master node. Similar to addRaftNode, this operation is performed online.
func (m *Server) removeRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
	if err != nil {
		goto errHandler
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		goto errHandler
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successfully\n", id, addr)
	m.sendOkReply(w, r, msg)
	return
errHandler:
	logMsg := newLogMsg("remove raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

// Parse the request that adds/deletes a raft node.
func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	r.ParseForm()
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
	r.ParseForm()
	return extractNodeAddr(r)
}

//func parseGetMetaNodePara(r *http.Request) (nodeAddr string, err error) {
//	r.ParseForm()
//	return extractNodeAddr(r)
//}
//
//func parseGetDataNodePara(r *http.Request) (nodeAddr string, err error) {
//	r.ParseForm()
//	return extractNodeAddr(r)
//}
//
//func parseDataNodeOfflinePara(r *http.Request) (nodeAddr string, err error) {
//	r.ParseForm()
//	return extractNodeAddr(r)
//}

func parseRequestToDecommissionNode(r *http.Request) (nodeAddr, diskPath string, err error) {
	r.ParseForm()
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	return
}


func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	r.ParseForm()

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(body)))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseRequestToDeleteVol(r *http.Request) (name string, err error) {
	r.ParseForm()
	return extractName(r)
}

func parseRequestToUpdateVol(r *http.Request) (name string, capacity int, err error) {
	r.ParseForm()
	if name, err = extractName(r); err != nil {
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

func parseRequestToCreateVol(r *http.Request) (name string, replicaNum int, randomWrite bool, size, capacity int, err error) {
	r.ParseForm()
	var randomWriteValue string
	if name, err = extractName(r); err != nil {
		return
	}
	if replicaStr := r.FormValue(replicasKey); replicaStr == "" {
		err = keyNotFound(replicasKey)
		return
	} else if replicaNum, err = strconv.Atoi(replicaStr); err != nil || replicaNum < 2 {
		err = unmatchedKey(replicasKey)
	}

	if randomWriteValue = r.FormValue(randomWriteKey); randomWriteValue == "" {
		err = keyNotFound(randomWriteKey)
		return
	}

	if randomWrite, err = strconv.ParseBool(randomWriteValue); err != nil {
		return
	}

	if sizeStr := r.FormValue(dataPartitionSizeKey); sizeStr != "" {
		if size, err = strconv.Atoi(sizeStr); err != nil {
			err = unmatchedKey(dataPartitionSizeKey)
		}
	}

	if capacityStr := r.FormValue(volCapacityKey); capacityStr != "" {
		if capacity, err = strconv.Atoi(capacityStr); err != nil {
			err = unmatchedKey(volCapacityKey)
		}
	} else {
		capacity = defaultVolCapacity
	}
	return
}

func parseRequestToCreateDataPartition(r *http.Request) (count int, name string, err error) {
	r.ParseForm()
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
	r.ParseForm()
	return extractDataPartitionID(r)
}

func parseRequestToLoadDataPartition(r *http.Request) (ID uint64, name string, err error) {
	r.ParseForm()
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
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

func parseRequestToDecommissionDataPartition(r *http.Request) (nodeAddr string, ID uint64, name string, err error) {
	r.ParseForm()
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}

	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
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

func parseRequestToLoadMetaPartition(r *http.Request) (partitionID uint64, volName string, err error) {
	r.ParseForm()
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if volName, err = extractName(r); err != nil {
		return
	}
	return
}

func parseRequestToDecommissionMetaPartition(r *http.Request) (volName, nodeAddr string, partitionID uint64, err error) {
	r.ParseForm()
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if volName, err = extractName(r); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func parseAndExtractStatus(r *http.Request) (status bool, err error) {
	r.ParseForm()  // TODO what if this line returns an error?
	return extractStatus(r)
}

// TODO why not inline the following?
func extractStatus(r *http.Request) (status bool, err error) {
	var value string
	if value = r.FormValue(enablekey); value == "" {
		err = keyNotFound(enablekey)
		return
	}
	if status, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func parseAndExtractThreshold(r *http.Request) (threshold float64, err error) {
	r.ParseForm()
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
	if value = r.FormValue(startkey); value == "" {
		err = keyNotFound(startkey)
		return
	}
	start, err = strconv.ParseUint(value, 10, 64)
	return
}

func (m *Server) sendOkReply(w http.ResponseWriter, r *http.Request, msg string) {
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(msg)))
	w.Write([]byte(msg))
}

func (m *Server) sendErrReply(w http.ResponseWriter, r *http.Request, httpCode int, msg string, err error) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err", r.URL, r.RemoteAddr)
	HandleError(msg, err, httpCode, w)
}
