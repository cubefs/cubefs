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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"net/http"
	"regexp"
	"strconv"
)

//VolStatInfo vol统计信息
type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

//DataPartitionResponse 简单数据分片
type DataPartitionResponse struct {
	PartitionID uint64
	Status      int8
	ReplicaNum  uint8
	Hosts       []string
	RandomWrite bool
	LeaderAddr  string
}

//DataPartitionsView 所有数据分片视图
type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
}

func newDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

//MetaPartitionView 元数据分片视图
type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
}

//VolView vol视图
type VolView struct {
	Name           string
	Status         uint8
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
}

func newVolView(name string, status uint8) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.Status = status
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

func newMetaPartitionView(partitionID, start, end uint64, status int8) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Status = status
	mpView.Members = make([]string, 0)
	return
}

func (m *Master) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		name string
		vol  *Vol
		ok   bool
		err  error
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}

	if body, err = vol.getDataPartitionsView(m.cluster.getLiveDataNodesRate()); err != nil {
		goto errDeal
	}
	m.sendOkReplyForClient(w, r, body)
	return
errDeal:
	logMsg := getReturnMessage("getDataPartitions", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func (m *Master) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}
	if body, err = json.Marshal(m.getVolView(vol)); err != nil {
		goto errDeal
	}
	m.sendOkReplyForClient(w, r, body)
	return
errDeal:
	logMsg := getReturnMessage("getVol", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func (m *Master) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		err  error
		name string
		vol  *Vol
		ok   bool
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}
	if body, err = json.Marshal(volStat(vol)); err != nil {
		goto errDeal
	}
	m.sendOkReplyForClient(w, r, body)
	return
errDeal:
	logMsg := getReturnMessage("getVolStatInfo", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func (m *Master) getVolView(vol *Vol) (view *VolView) {
	view = newVolView(vol.Name, vol.Status)
	setMetaPartitions(vol, view, m.cluster.getLiveMetaNodesRate())
	setDataPartitions(vol, view, m.cluster.getLiveDataNodesRate())
	return
}
func setDataPartitions(vol *Vol, view *VolView, liveRate float32) {
	if liveRate < nodesAliveRate {
		return
	}
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	view.DataPartitions = vol.dataPartitions.getDataPartitionsView(0)
}
func setMetaPartitions(vol *Vol, view *VolView, liveRate float32) {
	if liveRate < nodesAliveRate {
		return
	}
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		view.MetaPartitions = append(view.MetaPartitions, getMetaPartitionView(mp))
	}
}

func volStat(vol *Vol) (stat *VolStatInfo) {
	stat = new(VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.getTotalUsedSpace()
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *MetaPartitionView) {
	mpView = newMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
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

func (m *Master) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		code        = http.StatusBadRequest
		err         error
		name        string
		partitionID uint64
		vol         *Vol
		mp          *MetaPartition
		ok          bool
	)
	if name, partitionID, err = parseGetMetaPartitionPara(r); err != nil {
		goto errDeal
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}
	if mp, ok = vol.MetaPartitions[partitionID]; !ok {
		err = errors.Annotatef(metaPartitionNotFound(partitionID), "%v not found", partitionID)
		code = http.StatusNotFound
		goto errDeal
	}
	if body, err = mp.toJSON(); err != nil {
		goto errDeal
	}
	m.sendOkReplyForClient(w, r, body)
	return
errDeal:
	logMsg := getReturnMessage("getMetaPartition", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func parseGetMetaPartitionPara(r *http.Request) (name string, partitionID uint64, err error) {
	r.ParseForm()
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	return
}

func checkMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(paraID); value == "" {
		err = paraNotFound(paraID)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseGetVolPara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkVolPara(r)
}

func checkVolPara(r *http.Request) (name string, err error) {
	if name = r.FormValue(paraName); name == "" {
		err = paraNotFound(name)
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

func (m *Master) sendOkReplyForClient(w http.ResponseWriter, r *http.Request, msg []byte) {
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	w.Write(msg)
}
