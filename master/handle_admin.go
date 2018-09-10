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
	"io"
	"net/http"
	"strconv"

	"bytes"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"io/ioutil"
	"strings"
)

type ClusterView struct {
	Name               string
	LeaderAddr         string
	CompactStatus      bool
	Applied            uint64
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	MaxMetaPartitionID uint64
	Vols               []string
	MetaNodes          []MetaNodeView
	DataNodes          []DataNodeView
}

type DataNodeView struct {
	Addr   string
	Status bool
}

type MetaNodeView struct {
	ID     uint64
	Addr   string
	Status bool
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
	io.WriteString(w, fmt.Sprintf("set threshold to %v success", threshold))
	return
errDeal:
	logMsg := getReturnMessage("setMetaNodeThreshold", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) setCompactStatus(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	if status, err = parseCompactPara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.syncPutCluster(); err != nil {
		goto errDeal
	}
	io.WriteString(w, fmt.Sprintf("set compact status to %v success", status))
	return
errDeal:
	logMsg := getReturnMessage("setCompactStatus", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) getCompactStatus(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, fmt.Sprintf("%v", m.cluster.compactStatus))
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
		Applied:            m.fsm.applied,
		MaxDataPartitionID: m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:      m.cluster.idAlloc.metaNodeID,
		MaxMetaPartitionID: m.cluster.idAlloc.metaPartitionID,
		Vols:               make([]string, 0),
		MetaNodes:          make([]MetaNodeView, 0),
		DataNodes:          make([]DataNodeView, 0),
	}

	cv.Vols = m.cluster.getAllVols()
	cv.MetaNodes = m.cluster.getAllMetaNodes()
	cv.DataNodes = m.cluster.getAllDataNodes()
	if body, err = json.Marshal(cv); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))
	return

errDeal:
	logMsg := getReturnMessage("getCluster", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) getIpAndClusterName(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{Cluster: m.cluster.Name, Ip: strings.Split(r.RemoteAddr, ":")[0]}
	cInfoBytes, err := json.Marshal(cInfo)
	if err != nil {
		goto errDeal
	}
	w.Write(cInfoBytes)
	return
errDeal:
	rstMsg := getReturnMessage("getIpAndClusterName", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(rstMsg, err, http.StatusBadRequest, w)
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

	if err = m.cluster.CreateMetaPartitionForManual(volName, start); err != nil {
		goto errDeal
	}

	io.WriteString(w, fmt.Sprint("createMetaPartition request seccess"))
	return
errDeal:
	rstMsg = getReturnMessage("createMetaPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(rstMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                  string
		volName                 string
		partitionType           string
		vol                     *Vol
		reqCreateCount          int
		capacity                int
		lastTotalDataPartitions int
		err                     error
	)

	if reqCreateCount, volName, partitionType, err = parseCreateDataPartitionPara(r); err != nil {
		goto errDeal
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		goto errDeal
	}
	capacity = m.cluster.getDataPartitionCapacity(vol)
	lastTotalDataPartitions = len(vol.dataPartitions.dataPartitions)
	for i := 0; i < reqCreateCount; i++ {
		if (reqCreateCount + lastTotalDataPartitions) < len(vol.dataPartitions.dataPartitions) {
			break
		}
		if _, err = m.cluster.createDataPartition(volName, partitionType); err != nil {
			goto errDeal
		}
	}
	rstMsg = fmt.Sprintf(" createDataPartition success. cluster capacity[%v],vol[%v] has %v data partitions last,%v data partitions now",
		capacity, volName, lastTotalDataPartitions, len(vol.dataPartitions.dataPartitions))
	io.WriteString(w, rstMsg)

	return
errDeal:
	rstMsg = getReturnMessage("createDataPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(rstMsg, err, http.StatusBadRequest, w)
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
	if body, err = dp.toJson(); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))

	return
errDeal:
	logMsg := getReturnMessage("getDataPartition", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	msg = fmt.Sprintf(AdminLoadDataPartition+"partitionID :%v  load data partition success", partitionID)
	io.WriteString(w, msg)

	return
errDeal:
	logMsg := getReturnMessage(AdminLoadDataPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	m.cluster.dataPartitionOffline(addr, volName, dp, HandleDataPartitionOfflineErr)
	rstMsg = fmt.Sprintf(AdminDataPartitionOffline+" dataPartitionID :%v  on node:%v  has offline success", partitionID, addr)
	io.WriteString(w, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage(AdminDataPartitionOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	msg = fmt.Sprintf("delete vol[%v] successed\n", name)
	log.LogWarn(msg)
	io.WriteString(w, msg)
	return

errDeal:
	logMsg := getReturnMessage("markDeleteVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		name       string
		err        error
		msg        string
		volType    string
		replicaNum int
	)

	if name, volType, replicaNum, err = parseCreateVolPara(r); err != nil {
		goto errDeal
	}
	if err = m.cluster.createVol(name, volType, uint8(replicaNum)); err != nil {
		goto errDeal
	}
	msg = fmt.Sprintf("create vol[%v] successed\n", name)
	io.WriteString(w, msg)
	return

errDeal:
	logMsg := getReturnMessage("createVol", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		err      error
	)
	if nodeAddr, err = parseAddDataNodePara(r); err != nil {
		goto errDeal
	}

	if err = m.cluster.addDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	io.WriteString(w, fmt.Sprintf("addDataNode %v successed\n", nodeAddr))
	return
errDeal:
	logMsg := getReturnMessage("addDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	if body, err = dataNode.toJson(); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))

	return
errDeal:
	logMsg := getReturnMessage("getDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	m.cluster.dataNodeOffLine(node)
	rstMsg = fmt.Sprintf("dataNodeOffline node [%v] has offline SUCCESS", offLineAddr)
	io.WriteString(w, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("dataNodeOffline", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	HandleError(logMsg, err, code, w)
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
	io.WriteString(w, fmt.Sprintf("%v", id))
	return
errDeal:
	logMsg := getReturnMessage("addMetaNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	if body, err = metaNode.toJson(); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))
	return
errDeal:
	logMsg := getReturnMessage("getDataNode", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	msg = fmt.Sprintf(AdminLoadMetaPartition+" partitionID :%v  metaPartitionOffline success", partitionID)
	io.WriteString(w, msg)
	return
errDeal:
	logMsg := getReturnMessage(AdminMetaPartitionOffline, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	msg = fmt.Sprintf(AdminLoadMetaPartition+" partitionID :%v  Load success", partitionID)
	io.WriteString(w, msg)

	return
errDeal:
	logMsg := getReturnMessage(AdminLoadMetaPartition, r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	io.WriteString(w, rstMsg)
	return
errDeal:
	logMsg := getReturnMessage("metaNodeOffline", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	io.WriteString(w, msg)
	return
errDeal:
	logMsg := getReturnMessage("add raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
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
	io.WriteString(w, msg)
	return
errDeal:
	logMsg := getReturnMessage("remove raft node", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func parseRaftNodePara(r *http.Request) (id uint64, host string, err error) {
	r.ParseForm()
	var idStr string
	if idStr = r.FormValue(ParaId); idStr == "" {
		err = paraNotFound(ParaId)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(ParaNodeAddr); host == "" {
		err = paraNotFound(ParaNodeAddr)
		return
	}

	if arr := strings.Split(host, ColonSplit); len(arr) < 2 {
		err = UnMatchPara
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

func parseCreateVolPara(r *http.Request) (name, volType string, replicaNum int, err error) {
	r.ParseForm()
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if replicaStr := r.FormValue(ParaReplicas); replicaStr == "" {
		err = paraNotFound(ParaReplicas)
		return
	} else if replicaNum, err = strconv.Atoi(replicaStr); err != nil || replicaNum < 2 {
		err = UnMatchPara
	}
	if volType, err = parseDataPartitionType(r); err != nil {
		return
	}
	return
}

func parseCreateDataPartitionPara(r *http.Request) (count int, name, partitionType string, err error) {
	r.ParseForm()
	if countStr := r.FormValue(ParaCount); countStr == "" {
		err = paraNotFound(ParaCount)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = UnMatchPara
		return
	}
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if partitionType, err = parseDataPartitionType(r); err != nil {
		return
	}
	return
}

func parseDataPartitionType(r *http.Request) (partitionType string, err error) {
	if partitionType = r.FormValue(ParaDataPartitionType); partitionType == "" {
		err = paraNotFound(ParaDataPartitionType)
		return
	}

	if !(strings.TrimSpace(partitionType) == proto.ExtentPartition || strings.TrimSpace(partitionType) == proto.TinyPartition) {
		err = InvalidDataPartitionType
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
	if value = r.FormValue(ParaId); value == "" {
		err = paraNotFound(ParaId)
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
	if nodeAddr = r.FormValue(ParaNodeAddr); nodeAddr == "" {
		err = paraNotFound(ParaNodeAddr)
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

func parseCompactPara(r *http.Request) (status bool, err error) {
	r.ParseForm()
	var value string
	if value = r.FormValue(ParaEnable); value == "" {
		err = ParaEnableNotFound
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
	if value = r.FormValue(ParaThreshold); value == "" {
		err = paraNotFound(ParaThreshold)
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
	if value = r.FormValue(ParaStart); value == "" {
		err = paraNotFound(ParaStart)
		return
	}
	start, err = strconv.ParseUint(value, 10, 64)
	return
}
