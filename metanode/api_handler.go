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

package metanode

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
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
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("MetaNode")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc("/getPartitions", m.getPartitionsHandler)
	http.HandleFunc("/getPartitionById", m.getPartitionByIDHandler)
	http.HandleFunc("/getInode", m.getInodeHandler)
	http.HandleFunc("/getExtentsByInode", m.getExtentsByInodeHandler)
	// get all inodes of the partitionID
	http.HandleFunc("/getAllInodes", m.getAllInodesHandler)
	// get dentry information
	http.HandleFunc("/getDentry", m.getDentryHandler)
	http.HandleFunc("/getDirectory", m.getDirectoryHandler)
	http.HandleFunc("/getAllDentry", m.getAllDentriesHandler)
	http.HandleFunc("/getParams", m.getParamsHandler)
	http.HandleFunc("/getDiskStat", m.getDiskStatHandler)
	http.HandleFunc("/stat/info", m.getStatInfo)

	http.HandleFunc("/cursorReset", m.cursorReset)
	http.HandleFunc("/getAllInodeId", m.getAllInodeId)
	http.HandleFunc("/getSnapshotCrc", m.getSnapshotCrc)

	http.HandleFunc("/resetPeer", m.resetPeer)
	http.HandleFunc("/removePeer", m.removePeerInRaftLog)
	http.HandleFunc("/getAllDeleteExtents", m.getAllDeleteEkHandler)

	return
}

func (m *MetaNode) getDiskStatHandler(w http.ResponseWriter,
	r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	resp.Data = m.getDiskStat()
	data, _ := resp.Marshal()
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getPartitionsHandler] response %s", err)
	}
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
	msg["peers"] = conf.Peers
	msg["learners"] = conf.Learners
	msg["nodeId"] = conf.NodeId
	msg["cursor"] = conf.Cursor
	msg["inode_count"] = mp.GetInodeTree().Len()
	msg["dentry_count"] = mp.GetDentryTree().Len()
	msg["multipart_count"] = mp.(*metaPartition).multipartTree.Len()
	msg["extend_count"] = mp.(*metaPartition).extendTree.Len()
	msg["free_list_count"] = mp.(*metaPartition).freeList.Len()
	msg["trash_days"] = mp.(*metaPartition).config.TrashRemainingDays
	msg["cursor"] = mp.GetCursor()
	_, msg["leader"] = mp.IsLeader()
	msg["apply_id"] = mp.GetAppliedID()
	msg["raft_status"] = m.raftStore.RaftStatus(pid)
	resp.Data = msg
	resp.Code = http.StatusOK
	resp.Msg = http.StatusText(http.StatusOK)
}

func (m *MetaNode) getAllDeleteEkHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")

	if err := r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllDeleteEkHandler] response %s", err)
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
	if _, err = w.Write(buff.Bytes()); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	snap :=	mp.(*metaPartition).db.OpenSnap()
	defer mp.(*metaPartition).db.ReleaseSnap(snap)

	buff.Reset()
	var (
		val       []byte
		delimiter = []byte{',', '\n'}
		isFirst   = true
		stKey	  []byte
		endKey    []byte
	)

	stKey = make([]byte, 1)
	endKey = make([]byte, 1)
	stKey[0] = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	mp.(*metaPartition).db.RangeWithSnap(stKey, endKey, snap, func(k, v []byte)(bool, error) {
		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, nil
			}
		} else {
			isFirst = false
		}

		ek := &proto.ExtentKey{}
		ek.UnmarshalDbKey(k[8:])
		val, err = json.Marshal(ek)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return false, nil
		}
		if _, err = w.Write(val); err != nil {
			return false, nil
		}

		return true, nil
	})

	shouldSkip = true
	buff.WriteString(`]}`)
	if _, err = w.Write(buff.Bytes()); err != nil {
		log.LogErrorf("[getAllInodesHandler] response %s", err)
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
	}
	return
}

func (m *MetaNode) getAllInodesHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")

	if err := r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllInodeHandler] response %s", err)
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
	if _, err = w.Write(buff.Bytes()); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	buff.Reset()
	var (
		val       []byte
		delimiter = []byte{',', '\n'}
		isFirst   = true
	)

	inodeType := uint32(0)
	mode, err := strconv.ParseUint(r.FormValue("mode"), 10, 64)
	if err == nil {
		inodeType = uint32(mode)
	}

	stTime, err := strconv.ParseInt(r.FormValue("start"), 10, 64)
	if err != nil {
		stTime = 0
	}

	endTime, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		endTime = math.MaxInt64
	}

	mp.GetInodeTree().Ascend(func(i BtreeItem) bool {
		inode := i.(*Inode)
		if inodeType != 0 && inode.Type != inodeType {
			return true
		}

		if inode.ModifyTime < stTime || inode.ModifyTime > endTime {
			return true
		}

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
		log.LogErrorf("[getAllInodesHandler] response %s", err)
	}
	return

}

// param need pid:partition id and vol:volume name
func (m *MetaNode) cursorReset(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[cursorReset] response %s", err)
		}
	}()

	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	ino, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
	if err != nil {
		ino = 0
		err = nil
	}

	force := false
	forceStr := r.FormValue("force")
	if forceStr != "" {
		force = true
	}
	log.LogInfof("Mp[%d] recv reset cursor, ino:%d, force:%v", pid, ino, force)

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	if _, ok := mp.IsLeader(); !ok {
		resp.Code = http.StatusInternalServerError
		resp.Msg = "this node is not leader, can not execute this op"
		return
	}

	req := &proto.CursorResetRequest{
		PartitionId: pid,
		Inode:       ino,
		Force:       force,
	}

	cursor, err := mp.(*metaPartition).CursorReset(r.Context(), req)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		log.LogInfof("Mp[%d] recv reset cursor failed, cursor:%d, err:%s", pid, cursor, err.Error())
		return
	}

	respInfo := &proto.CursorResetResponse{
		PartitionId: mp.GetBaseConfig().PartitionId,
		Start:       mp.GetBaseConfig().Start,
		End:         mp.GetBaseConfig().End,
		Cursor:      cursor,
	}

	log.LogInfof("Mp[%d] recv reset cursor success, cursor:%d", pid, cursor)
	resp.Code = http.StatusOK
	resp.Msg = "Ok"
	resp.Data = respInfo
	return
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
	p := NewPacket(r.Context())
	err = mp.InodeGet(req, p, proto.OpInodeGetCurVersion)
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
	p := NewPacket(r.Context())
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
	p := NewPacket(r.Context())
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
	p := NewPacket(r.Context())
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

func (m *MetaNode) getStatInfo(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getStatInfoHandler] response %s", err)
		}
	}()
	//get process stat info
	cpuUsageList, maxCPUUsage := m.processStatInfo.GetProcessCPUStatInfo()
	memoryUsedGBList, maxMemoryUsedGB, maxMemoryUsage := m.processStatInfo.GetProcessMemoryStatInfo()
	//get disk info
	disks := m.getDiskStat()
	diskList := make([]interface{}, 0, len(disks))
	for _, disk := range disks {
		diskInfo := &struct {
			Path          string  `json:"path"`
			TotalTB       float64 `json:"totalTB"`
			UsedGB        float64 `json:"usedGB"`
			UsedRatio     float64 `json:"usedRatio"`
			ReservedSpace uint    `json:"reservedSpaceGB"`
		}{
			Path:      disk.Path,
			TotalTB:   util.FixedPoint(disk.Total/util.TB, 1),
			UsedGB:    util.FixedPoint(disk.Used/util.GB, 1),
			UsedRatio: util.FixedPoint(disk.Used/disk.Total, 1),
		}
		diskList = append(diskList, diskInfo)
	}
	msg := map[string]interface{}{
		"type":             "metaNode",
		"zone":             m.zoneName,
		"versionInfo":      proto.MakeVersion("MetaNode"),
		"statTime":         m.processStatInfo.ProcessStartTime,
		"cpuUsageList":     cpuUsageList,
		"maxCPUUsage":      maxCPUUsage,
		"cpuCoreNumber":    util.GetCPUCoreNumber(),
		"memoryUsedGBList": memoryUsedGBList,
		"maxMemoryUsedGB":  maxMemoryUsedGB,
		"maxMemoryUsage":   maxMemoryUsage,
		"diskInfo":         diskList,
	}
	resp.Data = msg
	resp.Code = http.StatusOK
	resp.Msg = http.StatusText(http.StatusOK)
}

func (m *MetaNode) getAllInodeId(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[cursorReset] response %s", err)
		}
	}()

	if err := r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	inodeType := uint32(0)
	mode, err := strconv.ParseUint(r.FormValue("mode"), 10, 64)
	if err == nil {
		inodeType = uint32(mode)
	}

	stTime, err := strconv.ParseInt(r.FormValue("start"), 10, 64)
	if err != nil {
		stTime = 0
	}

	endTime, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		endTime = math.MaxInt64
	}

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	inosRsp := &proto.MpAllInodesId{Count: 0, Inodes: make([]uint64, 0)}
	mp.GetInodeTree().Ascend(func(i BtreeItem) bool {
		inode := i.(*Inode)

		if inodeType != 0 && inode.Type != inodeType {
			return true
		}

		if inode.ModifyTime < stTime || inode.ModifyTime > endTime {
			return true
		}

		inosRsp.Count++
		inosRsp.Inodes = append(inosRsp.Inodes, inode.Inode)
		return true
	})

	resp.Code = http.StatusOK
	resp.Msg = "OK"
	resp.Data = inosRsp
	return
}

func (m *MetaNode) getSnapshotCrc(w http.ResponseWriter, r *http.Request){
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[snapshotCheckSum] response %s", err)
		}
	}()
	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	if err := r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	rootDir := fmt.Sprintf("%s/partition_%v",m.metadataDir, pid)
	crcFilePathSuffix := "snapshotCrc"
	filepath := fmt.Sprintf("%s/%s", rootDir, crcFilePathSuffix)
	file, err := os.Open(filepath)
	if err != nil{
		log.LogErrorf("open snapshotCrc file failed, err: %v",err)
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	defer file.Close()
	reader := bufio.NewReader(file)
	buf := make([]byte, 128)
	n, err := reader.Read(buf)
	if err != nil && err != io.EOF{
		log.LogErrorf("read snapshotCrc file failed, err: %v",err)
	}
	result := &proto.SnapshotCrdResponse{
		LastSnapshotStr:     string(buf[:n]),
		LocalAddr: m.localAddr,
	}
	resp.Code = http.StatusOK
	resp.Msg = "OK"
	resp.Data = result
	return
}

func (m *MetaNode) resetPeer(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[resetPeer] response %s", err)
		}
	}()

	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	idsStr := strings.Split(r.FormValue("PeerId"), ",")
	ids := make([]uint64, 0)
	for _, idStr := range idsStr {
		peerID, err := strconv.ParseUint(idStr, 10, 64)
		ids = append(ids, peerID)
		if err != nil {
			resp.Msg = err.Error()
			return
		}
	}

	if len(ids) == 0 {
		resp.Msg = "No reset peer id"
		return
	}

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	log.LogWarnf("Mp[%d] recv reset peer[%v] cmd", mp.GetBaseConfig().PartitionId, ids)
	if err = mp.ResetMemberInter(ids); err != nil {
		log.LogWarnf("Mp[%d] recv reset peer[%v] cmd failed:%s", mp.GetBaseConfig().PartitionId, ids, err.Error())
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	log.LogWarnf("Mp[%d] recv reset peer[%v] cmd success", mp.GetBaseConfig().PartitionId, ids)
	resp.Code = http.StatusOK
	resp.Msg = "OK"
	return
}

func (m *MetaNode) removePeerInRaftLog(w http.ResponseWriter, r *http.Request) {
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

	peerID, err := strconv.ParseUint(r.FormValue("PeerId"), 10, 64)
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

	log.LogWarnf("Mp[%d] recv remove peer[%v] cmd", mp.GetBaseConfig().PartitionId, peerID)
	if err = mp.RemoveMemberOnlyRaft(peerID); err != nil {
		log.LogWarnf("Mp[%d] recv remove peer[%v] cmd failed:%s", mp.GetBaseConfig().PartitionId, peerID, err.Error())
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	log.LogWarnf("Mp[%d] recv remove peer[%v] cmd success", mp.GetBaseConfig().PartitionId, peerID)

	resp.Code = http.StatusOK
	resp.Msg = "OK"
	return
}