// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"net/http"
	"os"
	"path"
	"strconv"

	"bytes"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// APIResponse defines the structure of the response to an HTTP request
type APIResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"`
}

// NewAPIResponse returns a new API response.
func NewAPIResponse(code int, msg string) *APIResponse {
	return &APIResponse{
		Code: code,
		Msg:  msg,
	}
}

// Marshal is a wrapper function of json.Marshal
func (api *APIResponse) Marshal() ([]byte, error) {
	return json.Marshal(api)
}

// register the APIs
func (m *MetaNode) registerAPIHandler() (err error) {
	http.HandleFunc("/getPartitions", m.getPartitionsHandler)
	http.HandleFunc("/getPartitionById", m.getPartitionByIDHandler)
	http.HandleFunc("/getInode", m.getInodeHandler)
	http.HandleFunc("/getExtentsByInode", m.getExtentsByInodeHandler)
	http.HandleFunc("/getEbsExtentsByInode", m.getEbsExtentsByInodeHandler)
	// get all inodes of the partitionID
	http.HandleFunc("/getAllInodes", m.getAllInodesHandler)
	// get dentry information
	http.HandleFunc("/getDentry", m.getDentryHandler)
	http.HandleFunc("/getDirectory", m.getDirectoryHandler)
	http.HandleFunc("/getAllDentry", m.getAllDentriesHandler)
	http.HandleFunc("/getParams", m.getParamsHandler)
	http.HandleFunc("/getSmuxStat", m.getSmuxStatHandler)
	http.HandleFunc("/getRaftStatus", m.getRaftStatusHandler)
	http.HandleFunc("/genClusterVersionFile", m.genClusterVersionFileHandler)
	return
}

func (m *MetaNode) getParamsHandler(w http.ResponseWriter,
	r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	params := make(map[string]interface{})
	params[metaNodeDeleteBatchCountKey] = DeleteBatchCount()
	resp.Data = params
	data, _ := resp.Marshal()
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getPartitionsHandler] response %s", err)
	}
}

func (m *MetaNode) getSmuxStatHandler(w http.ResponseWriter,
	r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	resp.Data = smuxPool.GetStat()
	data, _ := resp.Marshal()
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getSmuxStatHandler] response %s", err)
	}
}

func (m *MetaNode) getPartitionsHandler(w http.ResponseWriter,
	r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	resp.Data = m.metadataManager
	data, _ := resp.Marshal()
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getPartitionsHandler] response %s", err)
	}
}

func (m *MetaNode) getPartitionByIDHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getPartitionByIDHandler] response %s", err)
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	msg := make(map[string]interface{})
	leader, _ := mp.IsLeader()
	msg["leaderAddr"] = leader
	conf := mp.GetBaseConfig()
	msg["partition_id"] = conf.PartitionId
	msg["partition_type"] = conf.PartitionType
	msg["vol_name"] = conf.VolName
	msg["start"] = conf.Start
	msg["end"] = conf.End
	msg["peers"] = conf.Peers
	msg["nodeId"] = conf.NodeId
	msg["cursor"] = conf.Cursor
	resp.Data = msg
	resp.Code = http.StatusOK
	resp.Msg = http.StatusText(http.StatusOK)
}

func (m *MetaNode) getAllInodesHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	defer func() {
		if err != nil {
			msg := fmt.Sprintf("[getAllInodesHandler] err(%v)", err)
			if _, e := w.Write([]byte(msg)); e != nil {
				log.LogErrorf("[getAllInodesHandler] failed to write response: err(%v) msg(%v)", e, msg)
			}
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}
	id, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		return
	}
	mp, err := m.metadataManager.GetPartition(id)
	if err != nil {
		return
	}

	var inode *Inode

	f := func(i BtreeItem) bool {
		var (
			data []byte
			e    error
		)

		if inode != nil {
			if _, e = w.Write([]byte("\n")); e != nil {
				log.LogErrorf("[getAllInodesHandler] failed to write response: %v", e)
				return false
			}
		}

		inode = i.(*Inode)
		if data, e = inode.MarshalToJSON(); e != nil {
			log.LogErrorf("[getAllInodesHandler] failed to marshal to json: %v", e)
			return false
		}

		if _, e = w.Write(data); e != nil {
			log.LogErrorf("[getAllInodesHandler] failed to write response: %v", e)
			return false
		}

		return true
	}

	mp.GetInodeTree().Ascend(f)
}

func (m *MetaNode) getInodeHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getInodeHandler] response %s", err)
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	id, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &InodeGetReq{
		PartitionID: pid,
		Inode:       id,
	}
	p := &Packet{}
	err = mp.InodeGet(req, p)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
	}
	return
}

func (m *MetaNode) getRaftStatusHandler(w http.ResponseWriter, r *http.Request) {
	const (
		paramRaftID = "id"
	)

	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getRaftStatusHandler] response %s", err)
		}
	}()

	raftID, err := strconv.ParseUint(r.FormValue(paramRaftID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramRaftID, err)
		resp.Msg = err.Error()
		resp.Code = http.StatusBadRequest
		return
	}

	raftStatus := m.raftStore.RaftStatus(raftID)
	resp.Data = raftStatus
}

func (m *MetaNode) getEbsExtentsByInodeHandler(w http.ResponseWriter,
	r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getEbsExtentsByInodeHandler] response %s", err)
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	id, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &proto.GetExtentsRequest{
		PartitionID: pid,
		Inode:       id,
	}
	p := &Packet{}
	if err = mp.ObjExtentsList(req, p); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
	}
	return
}

func (m *MetaNode) getExtentsByInodeHandler(w http.ResponseWriter,
	r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getExtentsByInodeHandler] response %s", err)
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	id, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &proto.GetExtentsRequest{
		PartitionID: pid,
		Inode:       id,
	}
	p := &Packet{}
	if err = mp.ExtentsList(req, p); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
	}
	return
}

func (m *MetaNode) getDentryHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getDentryHandler] response %s", err)
		}
	}()
	var (
		pid  uint64
		pIno uint64
		err  error
	)
	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err == nil {
		pIno, err = strconv.ParseUint(r.FormValue("parentIno"), 10, 64)
	}
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &LookupReq{
		PartitionID: pid,
		ParentID:    pIno,
		Name:        name,
	}
	p := &Packet{}
	if err = mp.Lookup(req, p); err != nil {
		resp.Code = http.StatusSeeOther
		resp.Msg = err.Error()
		return
	}

	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
	}
	return

}

func (m *MetaNode) getAllDentriesHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusSeeOther, "")
	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllDentriesHandler] response %s", err)
			}
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	buff := bytes.NewBufferString(`{"code": 200, "msg": "OK", "data":[`)
	if _, err := w.Write(buff.Bytes()); err != nil {
		return
	}
	buff.Reset()
	var (
		val       []byte
		delimiter = []byte{',', '\n'}
		isFirst   = true
	)
	mp.GetDentryTree().Ascend(func(i BtreeItem) bool {
		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false
			}
		} else {
			isFirst = false
		}
		val, err = json.Marshal(i)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return false
		}
		if _, err = w.Write(val); err != nil {
			return false
		}
		return true
	})
	shouldSkip = true
	buff.WriteString(`]}`)
	if _, err = w.Write(buff.Bytes()); err != nil {
		log.LogErrorf("[getAllDentriesHandler] response %s", err)
	}
	return
}

func (m *MetaNode) getDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getDirectoryHandler] response %s", err)
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	pIno, err := strconv.ParseUint(r.FormValue("parentIno"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := ReadDirReq{
		ParentID: pIno,
	}
	p := &Packet{}
	if err = mp.ReadDir(&req, p); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
	}
	return
}

func (m *MetaNode) genClusterVersionFileHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusOK, "Generate cluster version file success")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[genClusterVersionFileHandler] response %s", err)
		}
	}()
	paths := make([]string, 0)
	paths = append(paths, m.metadataDir, m.raftDir)
	for _, p := range paths {
		if _, err := os.Stat(path.Join(p, config.ClusterVersionFile)); err == nil || os.IsExist(err) {
			resp.Code = http.StatusCreated
			resp.Msg = "Cluster version file already exists in " + p
			return
		}
	}
	for _, p := range paths {
		if err := config.CheckOrStoreClusterUuid(p, m.clusterUuid, true); err != nil {
			resp.Code = http.StatusInternalServerError
			resp.Msg = "Failed to create cluster version file in " + p
			return
		}
	}
	return
}
