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
	"io"
	"net/http"
	"strconv"

	"bytes"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"io/ioutil"
	"strings"
)

// ClusterView 集群视图
type ClusterView struct {
	Name               string
	LeaderAddr         string
	CompactStatus      bool
	DisableAutoAlloc   bool
	Applied            uint64
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	MaxMetaPartitionID uint64
	DataNodeStat       *dataNodeSpaceStat
	MetaNodeStat       *metaNodeSpaceStat
	VolStat            []*volSpaceStat
	MetaNodes          []MetaNodeView
	DataNodes          []DataNodeView
}

//VolStatView vol统计视图
type VolStatView struct {
	Name      string
	Total     uint64 `json:"TotalGB"`
	Used      uint64 `json:"UsedGB"`
	Increased uint64 `json:"IncreasedGB"`
}

//DataNodeView 数据节点视图
type DataNodeView struct {
	Addr   string
	Status bool
	ID     uint64
}

//MetaNodeView 元数据节点视图
type MetaNodeView struct {
	ID     uint64
	Addr   string
	Status bool
}

//TopologyView 集群拓扑视图
type TopologyView struct {
	DataNodes []DataNodeView
	MetaNodes []MetaNodeView
	NodeSet   []uint64
}

func (m *Master) setMetaNodeThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
	if threshold, err = parseSetMetaNodeThresholdPara(r); err != nil {
		goto errDeal
	}
	m.cluster.cfg.MetaNodeThreshold = float32(threshold)
	m.sendOkReply(w, r, fmt.Sprintf("set threshold to %v success", threshold))
	return
errDeal:
	logMsg := getReturnMessage("setMetaNodeThreshold", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) setDisableAutoAlloc(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	if status, err = parseDisableAutoAlloc(r); err != nil {
		goto errDeal
	}
	m.cluster.DisableAutoAlloc = status
	io.WriteString(w, fmt.Sprintf("set disableAutoAlloc  to %v success", status))
	return
errDeal:
	logMsg := getReturnMessage("setDisableAutoAlloc", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) getTopology(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)
	tv := &TopologyView{
		DataNodes: make([]DataNodeView, 0),
		MetaNodes: make([]MetaNodeView, 0),
		NodeSet:   make([]uint64, 0),
	}
	m.cluster.t.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*topoMetaNode)
		tv.MetaNodes = append(tv.MetaNodes, MetaNodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive})
		return true
	})
	m.cluster.t.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*topoDataNode)
		tv.DataNodes = append(tv.DataNodes, DataNodeView{ID: dataNode.ID, Addr: dataNode.Addr, Status: dataNode.isActive})
		return true
	})
	for _, ns := range m.cluster.t.nodeSetMap {
		tv.NodeSet = append(tv.NodeSet, ns.ID)
	}
	if body, err = json.Marshal(tv); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, string(body))
	return

errDeal:
	logMsg := getReturnMessage("getCluster", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) getCluster(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)
	cv := &ClusterView{
		Name:               m.cluster.Name,
		LeaderAddr:         m.leaderInfo.addr,
		CompactStatus:      m.cluster.compactStatus,
		DisableAutoAlloc:   m.cluster.DisableAutoAlloc,
		Applied:            m.fsm.applied,
		MaxDataPartitionID: m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:      m.cluster.idAlloc.commonID,
		MaxMetaPartitionID: m.cluster.idAlloc.metaPartitionID,
		MetaNodes:          make([]MetaNodeView, 0),
		DataNodes:          make([]DataNodeView, 0),
		VolStat:            make([]*volSpaceStat, 0),
	}

	vols := m.cluster.getAllVols()
	cv.MetaNodes = m.cluster.getAllMetaNodes()
	cv.DataNodes = m.cluster.getAllDataNodes()
	cv.DataNodeStat = m.cluster.dataNodeSpace
	cv.MetaNodeStat = m.cluster.metaNodeSpace
	for _, name := range vols {
		stat, ok := m.cluster.volSpaceStat.Load(name)
		if !ok {
			cv.VolStat = append(cv.VolStat, newVolSpaceStat(name, 0, 0, "0.0001"))
			continue
		}
		cv.VolStat = append(cv.VolStat, stat.(*volSpaceStat))
	}
	if body, err = json.Marshal(cv); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, string(body))
	return

errDeal:
	logMsg := getReturnMessage("getCluster", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) getIPAndClusterName(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{Cluster: m.cluster.Name, Ip: strings.Split(r.RemoteAddr, ":")[0]}
	cInfoBytes, err := json.Marshal(cInfo)
	if err != nil {
		goto errDeal
	}
	w.Write(cInfoBytes)
	return
errDeal:
	rstMsg := getReturnMessage("getIPAndClusterName", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, rstMsg, err)
	return
}

func (m *Master) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		start   uint64
		rstMsg  string
		err     error
	)

	if volName, start, err = parseCreateMetaPartitionPara(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.createMetaPartitionForManual(volName, start); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, fmt.Sprint("createMetaPartition request seccess"))
	return
errDeal:
	rstMsg = getReturnMessage("createMetaPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, rstMsg, err)
	return
}

func (m *Master) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                     string
		volName                    string
		vol                        *Vol
		reqCreateCount             int
		lastTotalDataPartitions    int
		clusterTotalDataPartitions int
		err                        error
	)

	if reqCreateCount, volName, err = parseCreateDataPartitionPara(r); err != nil {
		goto errDeal
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errDeal
	}
	lastTotalDataPartitions = len(vol.dataPartitions.dataPartitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()
	for i := 0; i < reqCreateCount; i++ {
		if _, err = m.cluster.createDataPartition(volName); err != nil {
			break
		}
	}
	rstMsg = fmt.Sprintf(" createDataPartition success. clusterLastTotalDataPartitions[%v],vol[%v] has %v data partitions last,%v data partitions now",
		clusterTotalDataPartitions, volName, lastTotalDataPartitions, len(vol.dataPartitions.dataPartitions))
	m.sendOkReply(w, r, rstMsg)
	return
errDeal:
	rstMsg = getReturnMessage("createDataPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, rstMsg, err)
	return
}

func (m *Master) getDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		dp          *DataPartition
		partitionID uint64
		err         error
	)
	if partitionID, err = parseDataPartitionID(r); err != nil {
		goto errDeal
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		goto errDeal
	}
	if body, err = dp.toJSON(); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, string(body))
	return
errDeal:
	logMsg := getReturnMessage("getDataPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) loadDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		vol         *Vol
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, volName, err = parseDataPartitionIDAndVol(r); err != nil {
		goto errDeal
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errDeal
	}
	if dp, err = vol.getDataPartitionByID(partitionID); err != nil {
		goto errDeal
	}

	m.cluster.loadDataPartitionAndCheckResponse(dp)
	msg = fmt.Sprintf(adminLoadDataPartition+"partitionID :%v  load data partition success", partitionID)
	m.sendOkReply(w, r, msg)
	return
errDeal:
	logMsg := getReturnMessage(adminLoadDataPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) dataPartitionOffline(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		vol         *Vol
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		err         error
	)

	if addr, partitionID, volName, err = parseDataPartitionOfflinePara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errDeal
	}
	if dp, err = vol.getDataPartitionByID(partitionID); err != nil {
		goto errDeal
	}
	if err = m.cluster.dataPartitionOffline(addr, volName, dp, handleDataPartitionOfflineErr); err != nil {
		goto errDeal
	}
	rstMsg = fmt.Sprintf(adminDataPartitionOffline+" dataPartitionID :%v  on node:%v  has offline success", partitionID, addr)
	m.sendOkReply(w, r, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage(adminDataPartitionOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
		msg  string
	)

	if name, err = parseDeleteVolPara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.markDeleteVol(name); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("delete vol[%v] successed,from[%v]", name, r.RemoteAddr)
	log.LogWarn(msg)
	m.sendOkReply(w, r, msg)
	return

errDeal:
	logMsg := getReturnMessage("markDeleteVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) updateVol(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		err      error
		msg      string
		capacity int
	)
	if name, capacity, err = parseUpdateVolPara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.updateVol(name, capacity); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("update vol[%v] successed\n", name)
	m.sendOkReply(w, r, msg)
	return
errDeal:
	logMsg := getReturnMessage("updateVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) createVol(w http.ResponseWriter, r *http.Request) {
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

	if name, replicaNum, randomWrite, size, capacity, err = parseCreateVolPara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.createVol(name, uint8(replicaNum), randomWrite, size, capacity); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("create vol[%v] success,has allocate [%v] data partitions", name, len(vol.dataPartitions.dataPartitions))
	m.sendOkReply(w, r, msg)
	return

errDeal:
	logMsg := getReturnMessage("createVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAddDataNodePara(r); err != nil {
		goto errDeal
	}

	if id, err = m.cluster.addDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, fmt.Sprintf("%v", id))
	return
errDeal:
	logMsg := getReturnMessage("addDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) getDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		dataNode *DataNode
		body     []byte
		err      error
	)
	if nodeAddr, err = parseGetDataNodePara(r); err != nil {
		goto errDeal
	}

	if dataNode, err = m.cluster.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	if body, err = dataNode.toJSON(); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, string(body))
	return
errDeal:
	logMsg := getReturnMessage("getDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) dataNodeOffline(w http.ResponseWriter, r *http.Request) {
	var (
		node        *DataNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseDataNodeOfflinePara(r); err != nil {
		goto errDeal
	}

	if node, err = m.cluster.getDataNode(offLineAddr); err != nil {
		goto errDeal
	}
	if err = m.cluster.dataNodeOffLine(node); err != nil {
		goto errDeal
	}
	rstMsg = fmt.Sprintf("dataNodeOffline node [%v] has offline SUCCESS", offLineAddr)
	m.sendOkReply(w, r, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("dataNodeOffline", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) diskOffline(w http.ResponseWriter, r *http.Request) {
	var (
		node                  *DataNode
		rstMsg                string
		offLineAddr, diskPath string
		err                   error
		badPartitionIds       []uint64
	)

	if offLineAddr, diskPath, err = parseDiskOfflinePara(r); err != nil {
		goto errDeal
	}

	if node, err = m.cluster.getDataNode(offLineAddr); err != nil {
		goto errDeal
	}
	badPartitionIds = node.getBadDiskPartitions(diskPath)
	if len(badPartitionIds) == 0 {
		err = fmt.Errorf("node[%v] disk[%v] no any datapartition", node.Addr, diskPath)
		goto errDeal
	}
	rstMsg = fmt.Sprintf("recive diskOffline node[%v] disk[%v],badPartitionIds[%v]  has offline  success",
		node.Addr, diskPath, badPartitionIds)
	m.cluster.BadDataPartitionIds.Store(fmt.Sprintf("%s:%s", offLineAddr, diskPath), badPartitionIds)
	if err = m.cluster.diskOffLine(node, diskPath, badPartitionIds); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, rstMsg)
	Warn(m.clusterName, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("diskOffLine", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) dataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		dataNode *DataNode
		code     = http.StatusOK
		tr       *proto.AdminTask
		err      error
	)

	if tr, err = parseTaskResponse(r); err != nil {
		code = http.StatusBadRequest
		goto errDeal
	}
	io.WriteString(w, fmt.Sprintf("%v", http.StatusOK))
	if dataNode, err = m.cluster.getDataNode(tr.OperatorAddr); err != nil {
		code = http.StatusInternalServerError
		goto errDeal
	}

	m.cluster.dealDataNodeTaskResponse(dataNode.Addr, tr)

	return

errDeal:
	logMsg := getReturnMessage("dataNodeTaskResponse", r.RemoteAddr, err.Error(),
		http.StatusBadRequest)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func (m *Master) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAddMetaNodePara(r); err != nil {
		goto errDeal
	}

	if id, err = m.cluster.addMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, fmt.Sprintf("%v", id))
	return
errDeal:
	logMsg := getReturnMessage("addMetaNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func parseAddMetaNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseAddDataNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func (m *Master) getMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		metaNode *MetaNode
		body     []byte
		err      error
	)
	if nodeAddr, err = parseGetMetaNodePara(r); err != nil {
		goto errDeal
	}

	if metaNode, err = m.cluster.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if body, err = metaNode.toJSON(); err != nil {
		goto errDeal
	}
	m.sendOkReply(w, r, string(body))
	return
errDeal:
	logMsg := getReturnMessage("getDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) metaPartitionOffline(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID       uint64
		volName, nodeAddr string
		msg               string
		err               error
	)
	if volName, nodeAddr, partitionID, err = parseMetaPartitionOffline(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.metaPartitionOffline(volName, nodeAddr, partitionID); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf(adminLoadMetaPartition+" partitionID :%v  metaPartitionOffline success", partitionID)
	m.sendOkReply(w, r, msg)
	return
errDeal:
	logMsg := getReturnMessage(adminMetaPartitionOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) loadMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		vol         *Vol
		msg         string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, volName, err = parsePartitionIDAndVol(r); err != nil {
		goto errDeal
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errDeal
	}
	if mp, err = vol.getMetaPartition(partitionID); err != nil {
		goto errDeal
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp)
	msg = fmt.Sprintf(adminLoadMetaPartition+" partitionID :%v  Load success", partitionID)
	m.sendOkReply(w, r, msg)
	return
errDeal:
	logMsg := getReturnMessage(adminLoadMetaPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) metaNodeOffline(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseDataNodeOfflinePara(r); err != nil {
		goto errDeal
	}

	if metaNode, err = m.cluster.getMetaNode(offLineAddr); err != nil {
		goto errDeal
	}
	m.cluster.metaNodeOffLine(metaNode)
	rstMsg = fmt.Sprintf("metaNodeOffline metaNode [%v] has offline SUCCESS", offLineAddr)
	m.sendOkReply(w, r, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("metaNodeOffline", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) metaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode *MetaNode
		code     = http.StatusOK
		tr       *proto.AdminTask
		err      error
	)

	if tr, err = parseTaskResponse(r); err != nil {
		code = http.StatusBadRequest
		goto errDeal
	}

	io.WriteString(w, fmt.Sprintf("%v", http.StatusOK))

	if metaNode, err = m.cluster.getMetaNode(tr.OperatorAddr); err != nil {
		code = http.StatusInternalServerError
		goto errDeal
	}
	m.cluster.dealMetaNodeTaskResponse(metaNode.Addr, tr)
	return

errDeal:
	logMsg := getReturnMessage("metaNodeTaskResponse", r.RemoteAddr, err.Error(),
		http.StatusBadRequest)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) handleAddRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		goto errDeal
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successed \n", id, addr)
	m.sendOkReply(w, r, msg)
	return
errDeal:
	logMsg := getReturnMessage("add raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func (m *Master) handleRemoveRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRaftNodePara(r)
	if err != nil {
		goto errDeal
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successed\n", id, addr)
	m.sendOkReply(w, r, msg)
	return
errDeal:
	logMsg := getReturnMessage("remove raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	m.sendErrReply(w, r, http.StatusBadRequest, logMsg, err)
	return
}

func parseRaftNodePara(r *http.Request) (id uint64, host string, err error) {
	r.ParseForm()
	var idStr string
	if idStr = r.FormValue(paraID); idStr == "" {
		err = paraNotFound(paraID)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(paraNodeAddr); host == "" {
		err = paraNotFound(paraNodeAddr)
		return
	}

	if arr := strings.Split(host, colonSplit); len(arr) < 2 {
		err = paraUnmatch(paraNodeAddr)
		return
	}
	return
}

func parseGetMetaNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseGetDataNodePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseDataNodeOfflinePara(r *http.Request) (nodeAddr string, err error) {
	r.ParseForm()
	return checkNodeAddr(r)
}

func parseDiskOfflinePara(r *http.Request) (nodeAddr, diskPath string, err error) {
	r.ParseForm()
	nodeAddr, err = checkNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = checkDiskPath(r)
	return
}

func parseTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
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

func parseDeleteVolPara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkVolPara(r)
}

func parseUpdateVolPara(r *http.Request) (name string, capacity int, err error) {
	r.ParseForm()
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if capacityStr := r.FormValue(paraVolCapacity); capacityStr != "" {
		if capacity, err = strconv.Atoi(capacityStr); err != nil {
			err = paraUnmatch(paraVolCapacity)
		}
	} else {
		err = paraNotFound(paraVolCapacity)
	}
	return
}

func parseCreateVolPara(r *http.Request) (name string, replicaNum int, randomWrite bool, size, capacity int, err error) {
	r.ParseForm()
	var randomWriteValue string
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if replicaStr := r.FormValue(paraReplicas); replicaStr == "" {
		err = paraNotFound(paraReplicas)
		return
	} else if replicaNum, err = strconv.Atoi(replicaStr); err != nil || replicaNum < 2 {
		err = paraUnmatch(paraReplicas)
	}

	if randomWriteValue = r.FormValue(paraRandomWrite); randomWriteValue == "" {
		err = paraNotFound(paraRandomWrite)
		return
	}

	if randomWrite, err = strconv.ParseBool(randomWriteValue); err != nil {
		return
	}

	if sizeStr := r.FormValue(paraDataPartitionSize); sizeStr != "" {
		if size, err = strconv.Atoi(sizeStr); err != nil {
			err = paraUnmatch(paraDataPartitionSize)
		}
	}

	if capacityStr := r.FormValue(paraVolCapacity); capacityStr != "" {
		if capacity, err = strconv.Atoi(capacityStr); err != nil {
			err = paraUnmatch(paraVolCapacity)
		}
	} else {
		capacity = defaultVolCapacity
	}
	return
}

func parseCreateDataPartitionPara(r *http.Request) (count int, name string, err error) {
	r.ParseForm()
	if countStr := r.FormValue(paraCount); countStr == "" {
		err = paraNotFound(paraCount)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = paraUnmatch(paraCount)
		return
	}
	if name, err = checkVolPara(r); err != nil {
		return
	}
	return
}

func parseDataPartitionID(r *http.Request) (ID uint64, err error) {
	r.ParseForm()
	return checkDataPartitionID(r)
}

func parseDataPartitionIDAndVol(r *http.Request) (ID uint64, name string, err error) {
	r.ParseForm()
	if ID, err = checkDataPartitionID(r); err != nil {
		return
	}
	if name, err = checkVolPara(r); err != nil {
		return
	}
	return
}

func checkDataPartitionID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(paraID); value == "" {
		err = paraNotFound(paraID)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseDataPartitionOfflinePara(r *http.Request) (nodeAddr string, ID uint64, name string, err error) {
	r.ParseForm()
	if ID, err = checkDataPartitionID(r); err != nil {
		return
	}
	if nodeAddr, err = checkNodeAddr(r); err != nil {
		return
	}

	if name, err = checkVolPara(r); err != nil {
		return
	}
	return
}

func checkNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(paraNodeAddr); nodeAddr == "" {
		err = paraNotFound(paraNodeAddr)
		return
	}
	return
}

func checkDiskPath(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(paraDiskPath); nodeAddr == "" {
		err = paraNotFound(paraDiskPath)
		return
	}
	return
}

func parsePartitionIDAndVol(r *http.Request) (partitionID uint64, volName string, err error) {
	r.ParseForm()
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	if volName, err = checkVolPara(r); err != nil {
		return
	}
	return
}

func parseMetaPartitionOffline(r *http.Request) (volName, nodeAddr string, partitionID uint64, err error) {
	r.ParseForm()
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	if volName, err = checkVolPara(r); err != nil {
		return
	}
	if nodeAddr, err = checkNodeAddr(r); err != nil {
		return
	}
	return
}

func parseDisableAutoAlloc(r *http.Request) (status bool, err error) {
	r.ParseForm()
	return checkEnable(r)
}

func checkEnable(r *http.Request) (status bool, err error) {
	var value string
	if value = r.FormValue(paraEnable); value == "" {
		err = paraNotFound(paraEnable)
		return
	}
	if status, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func parseSetMetaNodeThresholdPara(r *http.Request) (threshold float64, err error) {
	r.ParseForm()
	var value string
	if value = r.FormValue(paraThreshold); value == "" {
		err = paraNotFound(paraThreshold)
		return
	}
	if threshold, err = strconv.ParseFloat(value, 64); err != nil {
		return
	}
	return
}

func parseCreateMetaPartitionPara(r *http.Request) (volName string, start uint64, err error) {
	if volName, err = checkVolPara(r); err != nil {
		return
	}

	var value string
	if value = r.FormValue(paraStart); value == "" {
		err = paraNotFound(paraStart)
		return
	}
	start, err = strconv.ParseUint(value, 10, 64)
	return
}

func (m *Master) sendOkReply(w http.ResponseWriter, r *http.Request, msg string) {
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(msg)))
	w.Write([]byte(msg))
}

func (m *Master) sendErrReply(w http.ResponseWriter, r *http.Request, httpCode int, msg string, err error) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err", r.URL, r.RemoteAddr)
	HandleError(msg, err, httpCode, w)
}
