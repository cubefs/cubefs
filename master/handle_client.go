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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"net/http"
	"regexp"
	"strconv"
)

// VolStatInfo defines the statistics related to a volume
type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

// TODO DataPartitionResponse defines the response to a data partition? dn给master的response
type DataPartitionResponse struct {
	PartitionID uint64
	Status      int8
	ReplicaNum  uint8
	Hosts       []string
	RandomWrite bool
	LeaderAddr  string
}

// DataPartitionsView defines the view of a data partition
type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
}

func newDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

// MetaPartitionView defines the view of a meta partition
type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
}

// VolView defines the view of a volume
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

//获取vol下所有的data partition
func (m *Server) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		name string
		vol  *Vol
		ok   bool
		err  error
	)
	if name, err = parseAndExtractName(r); err != nil {
		goto errHandler
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errHandler
	}

	if body, err = vol.getDataPartitionsView(m.cluster.liveDataNodesRate()); err != nil {
		goto errHandler
	}
	m.replyOk(w, r, body)
	return
errHandler:
	logMsg := newLogMsg("getDataPartitions", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

//获取vol下所有的data partition和meta partition
func (m *Server) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseAndExtractName(r); err != nil {
		goto errHandler
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errHandler
	}
	if body, err = json.Marshal(m.getVolView(vol)); err != nil {
		goto errHandler
	}
	m.replyOk(w, r, body)
	return
errHandler:
	logMsg := newLogMsg("getVol", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

//获取vol的总容量和已使用空间信息
func (m *Server) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		err  error
		name string
		vol  *Vol
		ok   bool
	)
	if name, err = parseAndExtractName(r); err != nil {
		goto errHandler
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errHandler
	}
	if body, err = json.Marshal(volStat(vol)); err != nil {
		goto errHandler
	}
	m.replyOk(w, r, body)
	return
errHandler:
	logMsg := newLogMsg("getVolStatInfo", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

func (m *Server) getVolView(vol *Vol) (view *VolView) {
	view = newVolView(vol.Name, vol.Status)
	setMetaPartitions(vol, view, m.cluster.liveMetaNodesRate())
	setDataPartitions(vol, view, m.cluster.liveDataNodesRate())
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
	stat.UsedSize = vol.totalUsedSpace()
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

func (m *Server) getMetaPartition(w http.ResponseWriter, r *http.Request) {
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
	if name, partitionID, err = extractPartitionIdAndName(r); err != nil {
		goto errHandler
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(volNotFound(name), "%v not found", name)
		code = http.StatusNotFound
		goto errHandler
	}
	if mp, ok = vol.MetaPartitions[partitionID]; !ok {
		err = errors.Annotatef(metaPartitionNotFound(partitionID), "%v not found", partitionID)
		code = http.StatusNotFound
		goto errHandler
	}
	if body, err = mp.toJSON(); err != nil {
		goto errHandler
	}
	m.replyOk(w, r, body)
	return
errHandler:
	logMsg := newLogMsg("metaPartition", r.RemoteAddr, err.Error(), code)
	m.sendErrReply(w, r, code, logMsg, err)
	return
}

// TODO find a better name for extractPartitionIdAndName
func extractPartitionIdAndName(r *http.Request) (name string, partitionID uint64, err error) {
	r.ParseForm()
	if name, err = extractName(r); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}


// TODO this function is exactly the same as extractDataPartitionID in handle_admin.go
func extractMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

// TODO find a better name for parseAndExtractName
func parseAndExtractName(r *http.Request) (name string, err error) {
	r.ParseForm()
	return extractName(r)
}

func extractName(r *http.Request) (name string, err error) {
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(name)
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

func (m *Server) replyOk(w http.ResponseWriter, r *http.Request, msg []byte) {
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	w.Write(msg)
}
