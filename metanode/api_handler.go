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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/cpu"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/unit"
	"hash/crc32"
	"io"
	"io/fs"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
		version.Version = MetaNodeLatestVersion
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
	http.HandleFunc("/cleanDeleteExtents", m.cleanDeleteEkHandler)

	http.HandleFunc("/getMetaDataCrcSum", m.getMetaDataCrcSum)
	http.HandleFunc("/getInodesCrcSum", m.getAllInodesCrcSum)
	http.HandleFunc("/getDelInodesCrcSum", m.getAllDeletedInodesCrcSum)

	http.HandleFunc("/startPartition", m.startPartition)
	http.HandleFunc("/stopPartition", m.stopPartition)
	http.HandleFunc("/reloadPartition", m.reloadPartition)

	http.HandleFunc("/cleanExpiredPartitions", m.cleanExpiredPartitions)

	http.HandleFunc("/getAllDeletedInodes", m.getAllDeletedInodesHandler)
	http.HandleFunc("/getAllDeletedDentry", m.getAllDeletedDentriesHandler)
	http.HandleFunc("/getAllDeletedInodeId", m.getAllDeletedInodeIdHandler)
	http.HandleFunc("/getExtentsByDelIno", m.getExtentsByDeletedInodeHandler)
	http.HandleFunc("/getAllInodeIdWithDeleted", m.getAllInodeIdWithDeletedHandler)

	http.HandleFunc("/tryToLeader", m.tryToLeader)

	http.HandleFunc("/pushInodeToFreeList", m.pushInodeToFreeList)

	http.HandleFunc("/getInodeWithMarkDelete", m.getInodeWithMarkDeleteHandler)

	http.HandleFunc("/enableRocksDBSync", m.enableRocksDBSync)
	http.HandleFunc("/reopenDb", m.reopenRocksDb)
	http.HandleFunc("/setDelEKRecordFilesMaxTotalMB", m.setDelEKRecordFilesMaxTotalSize)
	http.HandleFunc("/removeOldDelEkRecordFile", m.removeOldDelEKRecordFile)
	http.HandleFunc("/setRaftStorageParam", m.setRaftStorageParam)
	http.HandleFunc("/getDeletedDentrys", m.getDeletedDentrysByParentInoHandler)
	http.HandleFunc("/getInodeInuse", m.getInodeInuse)
	http.HandleFunc("/getBitInuse", m.getBitInuse)
	http.HandleFunc("/getInoAllocatorInfo", m.getInodeAllocatorStat)
	http.HandleFunc("/setSkipStep", m.setSkipStep)
	http.HandleFunc("/checkFreeList", m.checkFreelist)
	return
}

func (m *MetaNode) getDiskStatHandler(w http.ResponseWriter,
	r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	resp.Data = m.getDisks()
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
	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()
	msg := make(map[string]interface{})
	leader, _ := mp.IsLeader()
	msg["leaderAddr"] = leader
	conf := mp.GetBaseConfig()
	msg["peers"] = conf.Peers
	msg["learners"] = conf.Learners
	msg["nodeId"] = conf.NodeId
	msg["cursor"] = conf.Cursor
	msg["inode_count"] = snap.Count(InodeType)
	msg["dentry_count"] = snap.Count(DentryType)
	msg["multipart_count"] = snap.Count(MultipartType)
	msg["extend_count"] = snap.Count(ExtendType)
	msg["deleted_inode_count"] = snap.Count(DelInodeType)
	msg["deleted_dentry_count"] = snap.Count(DelDentryType)
	msg["free_list_count"] = mp.(*metaPartition).freeList.Len()
	msg["trash_days"] = mp.(*metaPartition).config.TrashRemainingDays
	msg["cursor"] = mp.GetCursor()
	_, msg["leader"] = mp.IsLeader()
	msg["apply_id"] = mp.GetAppliedID()
	msg["raft_status"] = m.raftStore.RaftStatus(pid)
	msg["trash_first_upd_time"] = mp.(*metaPartition).trashExpiresFirstUpdateTime
	msg["trash_clean_interval_min"] = mp.(*metaPartition).getTrashCleanInterval() / time.Minute
	msg["batch_del_inode_cnt"] = mp.(*metaPartition).GetBatchDelInodeCnt()
	msg["del_inode_interval_ms"] = mp.(*metaPartition).GetDelInodeInterval()
	raftPartition := mp.(*metaPartition).raftPartition
	if raftPartition != nil {
		msg["raft_log_size"] = raftPartition.GetWALFileSize()
		msg["raft_log_cap"] = raftPartition.GetWALFileCacheCapacity()
	}
	msg["status"] = mp.(*metaPartition).status
	msg["virtual_mps"] = mp.(*metaPartition).getVirtualMetaPartitionsInfo()
	msg["cleanTrashItemMaxDurationEachTime"] = mp.(*metaPartition).getCleanTrashItemMaxDurationEachTime()
	msg["cleanTrashItemMaxCountEachTime"] = mp.(*metaPartition).getCleanTrashItemMaxCountEachTime()
	msg["skipStep"] = mp.(*metaPartition).getCursorSkipStep()
	msg["now"] = time.Now()
	resp.Data = msg
	resp.Code = http.StatusOK
	resp.Msg = http.StatusText(http.StatusOK)
}

func (m *MetaNode) getAllDeleteEkHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")
	shouldSkip := false
	prefixTable := ExtentDelTable
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllDeleteEkHandler] response %s", err)
			}
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
	version, _ := strconv.ParseUint(r.FormValue("version"), 10, 64)
	switch version {
	case 1:
		prefixTable = ExtentDelTableV1
		break
	default:
		//0 is latest version
		prefixTable = ExtentDelTable
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	buff := bytes.NewBufferString(`{"data":[`)
	if _, err = w.Write(buff.Bytes()); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	snap := mp.(*metaPartition).db.OpenSnap()
	defer mp.(*metaPartition).db.ReleaseSnap(snap)

	buff.Reset()
	var (
		val       []byte
		delimiter = []byte{',', '\n'}
		isFirst   = true
		stKey     []byte
		endKey    []byte
	)

	stKey = make([]byte, 1)
	endKey = make([]byte, 1)
	stKey[0] = byte(prefixTable)
	endKey[0] = byte(prefixTable + 1)
	err = mp.(*metaPartition).db.RangeWithSnap(stKey, endKey, snap, func(k, v []byte) (bool, error) {
		ek := &proto.ExtentKey{}
		switch prefixTable {
		case ExtentDelTableV1:
			ek.PartitionId = binary.BigEndian.Uint64(k[8:16])
			ek.ExtentId = binary.BigEndian.Uint64(k[16:24])
			ek.ExtentOffset = uint64(binary.BigEndian.Uint32(k[24:28]))
			ek.Size = binary.BigEndian.Uint32(k[28:32])
		case ExtentDelTable:
			err = ek.UnmarshalDbKey(k[8:])
		default:
			break
		}

		if err != nil {
			return false, err
		}
		delEkInfo := &struct {
			proto.ExtentKey
			DelTime uint64
		}{
			proto.ExtentKey{
				FileOffset:   ek.FileOffset,
				PartitionId:  ek.PartitionId,
				ExtentId:     ek.ExtentId,
				ExtentOffset: ek.ExtentOffset,
				Size: ek.Size,
				CRC: ek.CRC,
			},
			getDateInKey(k),
		}

		val, err = json.Marshal(delEkInfo)
		if err != nil {
			return false, err
		}

		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}

		if _, err = w.Write(val); err != nil {
			return false, err
		}

		return true, nil
	})
	shouldSkip = true
	if err != nil {
		buff.WriteString(fmt.Sprintf(`], "code": %v, "msg": "%s"}`, http.StatusInternalServerError, err.Error()))
	} else {
		buff.WriteString(`], "code": 200, "msg": "OK"}`)
	}
	if _, err = w.Write(buff.Bytes()); err != nil {
		log.LogErrorf("[getAllDeleteEkHandler] response %s", err)
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
	}
	return
}

func (m *MetaNode) cleanDeleteEkHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getAllDeleteEkHandler] response %s", err)
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
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	snap := mp.(*metaPartition).db.OpenSnap()
	defer mp.(*metaPartition).db.ReleaseSnap(snap)

	var (
		stKey  []byte
		endKey []byte
	)

	stKey = make([]byte, 1)
	endKey = make([]byte, 1)
	stKey[0] = byte(ExtentDelTableV1)
	endKey[0] = byte(ExtentDelTableV1 + 1)
	delHandle, err := mp.(*metaPartition).db.CreateBatchHandler()
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	err = mp.(*metaPartition).db.RangeWithSnap(stKey, endKey, snap, func(k, v []byte) (bool, error) {
		if k[0] != byte(ExtentDelTableV1) {
			return false, nil
		}
		mp.(*metaPartition).db.DelItemToBatch(delHandle, k)
		return true, nil
	})
	if err = mp.(*metaPartition).db.CommitBatchAndRelease(delHandle); err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	return
}

func (m *MetaNode) getAllInodesHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")
	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllInodeHandler] response %s", err)
			}
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
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	buff := bytes.NewBufferString(`{"data":[`)
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

	err = snap.Range(InodeType, func(item interface{}) (bool, error) {
		inode := item.(*Inode)
		if inodeType != 0 && inode.Type != inodeType {
			return true, nil
		}

		if inode.ModifyTime < stTime || inode.ModifyTime > endTime {
			return true, nil
		}

		val, err = json.Marshal(inode)
		if err != nil {
			return false, err
		}

		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}

		if _, err = w.Write(val); err != nil {
			return false, err
		}
		return true, nil
	})
	shouldSkip = true
	if err != nil {
		buff.WriteString(fmt.Sprintf(`], "code": %v, "msg": "%s"}`, http.StatusInternalServerError, err.Error()))
	} else {
		buff.WriteString(`], "code": 200, "msg": "OK"}`)
	}
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

	cursorResetType, err := ParseCursorResetMode(r.FormValue("resetType"))
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	if cursorResetType == SubCursor {
		resp.Msg = fmt.Sprintf("not suport sub")
		return
	}

	newCursor, _ := strconv.ParseUint(r.FormValue("newCursor"), 10, 64)

	force, _ := strconv.ParseBool(r.FormValue("force"))

	log.LogInfof("Mp[%d] recv reset cursor, type:%s, ino:%d, force:%v", pid, cursorResetType, newCursor, force)

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
		PartitionId:     pid,
		NewCursor:       newCursor,
		Force:           force,
		CursorResetType: int(cursorResetType),
	}

	err = mp.(*metaPartition).CursorReset(r.Context(), req)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		log.LogInfof("Mp[%d] recv reset cursor failed, cursor:%d, err:%s", pid, mp.GetBaseConfig().Cursor, err.Error())
		return
	}

	respInfo := &proto.CursorResetResponse{
		PartitionId: mp.GetBaseConfig().PartitionId,
		Start:       mp.GetBaseConfig().Start,
		End:         mp.GetBaseConfig().End,
		Cursor:      mp.GetBaseConfig().Cursor,
	}

	log.LogInfof("Mp[%d] recv reset cursor success, cursor:%d", pid, mp.GetBaseConfig().Cursor)
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

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	buff := bytes.NewBufferString(`{"data":[`)
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
	err = snap.Range(DentryType, func(item interface{}) (bool, error) {
		val, err = json.Marshal(item.(*Dentry))
		if err != nil {
			return false, err
		}

		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}
		if _, err = w.Write(val); err != nil {
			return false, err
		}
		return true, nil
	})
	shouldSkip = true
	if err != nil {
		buff.WriteString(fmt.Sprintf(`], "code": %v, "msg": "%s"}`, http.StatusInternalServerError, err.Error()))
	} else {
		buff.WriteString(`], "code": 200, "msg": "OK"}`)
	}
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

	if m.processStatInfo == nil {
		resp.Msg = "meta node is initializing"
		return
	}
	//get process stat info
	cpuUsageList, maxCPUUsage := m.processStatInfo.GetProcessCPUStatInfo()
	_, memoryUsedGBList, maxMemoryUsedGB, maxMemoryUsage := m.processStatInfo.GetProcessMemoryStatInfo()
	nodeCfg := getGlobalConfNodeInfo()
	//get disk info
	disks := m.getDisks()
	diskList := make([]interface{}, 0, len(disks))
	for _, disk := range disks {
		diskInfo := &struct {
			Path          string  `json:"path"`
			TotalTB       float64 `json:"totalTB"`
			UsedGB        float64 `json:"usedGB"`
			UsedRatio     float64 `json:"usedRatio"`
			ReservedSpace uint64  `json:"reservedSpaceGB"`
			Status        int8    `json:"status"`
		}{
			Path:          disk.Path,
			TotalTB:       unit.FixedPoint(disk.Total/unit.TB, 1),
			UsedGB:        unit.FixedPoint(disk.Used/unit.GB, 1),
			UsedRatio:     unit.FixedPoint(disk.Used/disk.Total, 3),
			ReservedSpace: disk.ReservedSpace / unit.GB,
			Status:        disk.Status,
		}
		diskList = append(diskList, diskInfo)
	}
	msg := map[string]interface{}{
		"type":                              "metaNode",
		"zone":                              m.zoneName,
		"versionInfo":                       proto.MakeVersion("MetaNode"),
		"statTime":                          m.processStatInfo.ProcessStartTime,
		"cpuUsageList":                      cpuUsageList,
		"maxCPUUsage":                       maxCPUUsage,
		"cpuCoreNumber":                     cpu.GetCPUCoreNumber(),
		"memoryUsedGBList":                  memoryUsedGBList,
		"maxMemoryUsedGB":                   maxMemoryUsedGB,
		"maxMemoryUsage":                    maxMemoryUsage,
		"diskInfo":                          diskList,
		"raftLogSizeFromMaster":             nodeCfg.raftLogSizeFromMaster,
		"raftLogCapFromMaster":              nodeCfg.raftLogCapFromMaster,
		"raftLogSizeFromLoc":                nodeCfg.raftLogSizeFromLoc,
		"raftLogCapFromLoc":                 nodeCfg.raftLogCapFromLoc,
		"trashCleanInterval":                nodeCfg.trashCleanInterval,
		"delEKRecordFileMaxMB":              DeleteEKRecordFilesMaxTotalSize.Load() / unit.MB,
		"raftWALSyncEnableState":            m.raftStore.IsSyncWALOnUnstable(),
		"reuseMPInodeCountThreshold":        m.getReuseMPInodeCountThreshold(),
		"reuseMPDentryCountThreshold":       m.getReuseMPDentryCountThreshold(),
		"mpMaxInodeCount":                   m.getMetaPartitionMaxInodeCount(),
		"mpMaxDentryCount":                  m.getMetaPartitionMaxDentryCount(),
		"cleanTrashItemMaxDurationEachTime": nodeInfo.CleanTrashItemMaxDurationEachTime,
		"cleanTrashItemMaxCountEachTime":    nodeInfo.CleanTrashItemMaxCountEachTime,
		"skipStep":                          atomic.LoadUint64(&nodeInfo.cursorAddStep),
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

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	inosRsp := &proto.MpAllInodesId{Count: 0, Inodes: make([]uint64, 0)}
	err = snap.Range(InodeType, func(item interface{}) (bool, error) {
		inode := item.(*Inode)
		if inodeType != 0 && inode.Type != inodeType {
			return true, nil
		}

		if inode.ModifyTime < stTime || inode.ModifyTime > endTime {
			return true, nil
		}

		inosRsp.Count++
		inosRsp.Inodes = append(inosRsp.Inodes, inode.Inode)
		return true, nil
	})

	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	resp.Code = http.StatusOK
	resp.Msg = "OK"
	resp.Data = inosRsp
	return
}

func (m *MetaNode) getSnapshotCrc(w http.ResponseWriter, r *http.Request) {
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
	rootDir := fmt.Sprintf("%s/partition_%v", m.metadataDir, pid)
	crcFilePathSuffix := "snapshotCrc"
	filepath := fmt.Sprintf("%s/%s", rootDir, crcFilePathSuffix)
	file, err := os.Open(filepath)
	if err != nil {
		log.LogErrorf("open snapshotCrc file failed, err: %v", err)
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	defer file.Close()
	reader := bufio.NewReader(file)
	buf := make([]byte, 128)
	n, err := reader.Read(buf)
	if err != nil && err != io.EOF {
		log.LogErrorf("read snapshotCrc file failed, err: %v", err)
	}
	result := &proto.SnapshotCrdResponse{
		LastSnapshotStr: string(buf[:n]),
		LocalAddr:       m.localAddr,
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

func (m *MetaNode) getMetaDataCrcSum(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		pid    uint64
		mp     MetaPartition
		crcSum uint32
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getMetaDataCrcSum] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	snap := mp.GetSnapShot()
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = "meta partition snapshot is nil"
		return
	}
	defer snap.Close()

	var (
		cntSet    = make([]uint64, 0)
		crcSumSet = make([]uint32, 0)
	)
	for t := DentryType; t < MaxType; t++ {
		crcSum, err = snap.CrcSum(t)
		if err != nil {
			resp.Code = http.StatusInternalServerError
			resp.Msg = fmt.Sprintf("%s crc sum error:%v", t.String(), err)
			return
		}
		cntSet = append(cntSet, snap.Count(t))
		crcSumSet = append(crcSumSet, crcSum)
	}
	resp.Data = &proto.MetaDataCRCSumInfo{
		PartitionID: pid,
		ApplyID:     snap.ApplyID(),
		CntSet:      cntSet,
		CRCSumSet:   crcSumSet,
	}
	return
}

func (m *MetaNode) getAllInodesCrcSum(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
		mp  MetaPartition
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllInodesCrcSum] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	snap := mp.GetSnapShot()
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = "meta partition snapshot is nil"
		return
	}
	defer snap.Close()

	var (
		inodeCnt  = snap.Count(InodeType)
		crcSumSet = make([]uint32, 0, inodeCnt)
		inodes    = make([]uint64, 0, inodeCnt)
		crc       = crc32.NewIEEE()
	)
	err = snap.Range(InodeType, func(item interface{}) (bool, error) {
		inode := item.(*Inode)
		inode.AccessTime = 0
		var inodeBinary []byte
		inodeBinary, err = inode.MarshalV2()
		if err != nil {
			return false, err
		}
		inodes = append(inodes, inode.Inode)
		crcSumSet = append(crcSumSet, crc32.ChecksumIEEE(inodeBinary[0:]))
		if _, err = crc.Write(inodeBinary); err != nil {
			return false, fmt.Errorf("crc sum write failed:%v", err)
		}
		return true, nil
	})
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	resp.Data = &proto.InodesCRCSumInfo{
		PartitionID:     pid,
		ApplyID:         snap.ApplyID(),
		AllInodesCRCSum: crc.Sum32(),
		InodesID:        inodes,
		CRCSumSet:       crcSumSet,
	}
	return
}

func (m *MetaNode) cleanExpiredPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		err           error
		reservedDays  uint64
		cleanTotalCnt int
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[cleanExpiredPartitions] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if reservedDays, err = strconv.ParseUint(r.FormValue("Days"), 10, 64); err != nil {
		reservedDays = 1
	}

	if reservedDays <= 0 {
		reservedDays = 1
	}

	expiredCheck := (time.Now().Add(-24 * time.Hour * time.Duration(reservedDays))).Unix()

	cleanDirs := []string{m.metadataDir, m.raftDir}
	cleanFailedDirs := make([]string, 0, len(cleanDirs))
	for _, cleanDir := range cleanDirs {
		var fileInfoList []fs.FileInfo
		fileInfoList, err = ioutil.ReadDir(cleanDir)
		if err != nil {
			cleanFailedDirs = append(cleanFailedDirs, cleanDir)
			log.LogErrorf("[cleanExpiredPartitions] read %s failed:%v", cleanDir, err)
			continue
		}
		//meta data:expired_partition_7320_1634280431; raft data:expired_25496_1662606658
		for _, fileInfo := range fileInfoList {
			if !fileInfo.IsDir() {
				continue
			}
			if !strings.HasPrefix(fileInfo.Name(), ExpiredPartitionPrefix) {
				continue
			}
			mpInfo := strings.Split(fileInfo.Name(), "_")
			if len(mpInfo) == 0 {
				continue
			}
			delTime := int64(0)
			if delTime, err = strconv.ParseInt(mpInfo[len(mpInfo)-1], 10, 64); err != nil {
				continue
			}

			if delTime > expiredCheck {
				continue
			}
			cleanTotalCnt++
			os.RemoveAll(path.Join(cleanDir, fileInfo.Name()))
		}
	}
	resp.Data = &struct {
		FailedDirs    []string `json:"failedDir"`
		CleanTotalCnt int      `json:"cleanCnt"`
	}{
		FailedDirs:    cleanFailedDirs,
		CleanTotalCnt: cleanTotalCnt,
	}
	return
}

func (m *MetaNode) stopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllInodesCrcSum] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	//m.metadataManager.Start()
	err = m.metadataManager.StopPartition(pid)
	if err != nil {
		resp.Msg = err.Error()
	}
}

func (m *MetaNode) startPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllInodesCrcSum] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	err = m.metadataManager.StartPartition(pid)
	if err != nil {
		resp.Msg = err.Error()
	}
}

func (m *MetaNode) reloadPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllInodesCrcSum] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	err = m.metadataManager.ReloadPartition(pid)
	if err != nil {
		resp.Msg = err.Error()
	}
}

func (m *MetaNode) getAllDeletedInodesHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")
	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllDeletedInodesHandler] response %s", err)
			}
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
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	buff := bytes.NewBufferString(`{"data":[`)
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

	err = snap.Range(DelInodeType, func(item interface{}) (bool, error) {
		delInode := item.(*DeletedINode)
		if inodeType != 0 && delInode.Inode.Type != inodeType {
			return true, nil
		}

		if delInode.Timestamp < stTime || delInode.Timestamp > endTime {
			return true, nil
		}

		val, err = json.Marshal(delInode)
		if err != nil {
			return false, err
		}

		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}

		if _, err = w.Write(val); err != nil {
			return false, err
		}
		return true, nil
	})
	shouldSkip = true
	if err != nil {
		buff.WriteString(fmt.Sprintf(`], "code": %v, "msg": "%s"}`, http.StatusInternalServerError, err.Error()))
	} else {
		buff.WriteString(`], "code": 200, "msg": "OK"}`)
	}
	if _, err = w.Write(buff.Bytes()); err != nil {
		log.LogErrorf("[getAllDeletedInodesHandler] response %s", err)
	}
	return
}

func (m *MetaNode) getAllDeletedDentriesHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusSeeOther, "")
	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllDeletedDentriesHandler] response %s", err)
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

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	buff := bytes.NewBufferString(`{"data":[`)
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
	err = snap.Range(DelDentryType, func(item interface{}) (bool, error) {
		delDentry := item.(*DeletedDentry)
		val, err = json.Marshal(delDentry)
		if err != nil {
			return false, err
		}

		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}
		if _, err = w.Write(val); err != nil {
			return false, err
		}
		return true, nil
	})
	shouldSkip = true
	if err != nil {
		buff.WriteString(fmt.Sprintf(`], "code": %v, "msg": "%s"}`, http.StatusInternalServerError, err.Error()))
	} else {
		buff.WriteString(`], "code": 200, "msg": "OK"}`)
	}
	if _, err = w.Write(buff.Bytes()); err != nil {
		log.LogErrorf("[getAllDeletedDentriesHandler] response %s", err)
	}
	return
}

func (m *MetaNode) getAllDeletedInodeIdHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getAllDeletedInodeId] response %s", err)
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

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	inosRsp := &proto.MpAllInodesId{Count: 0, DelInodes: make([]uint64, 0)}
	err = snap.Range(DelInodeType, func(item interface{}) (bool, error) {
		delInode := item.(*DeletedINode)
		if inodeType != 0 && delInode.Type != inodeType {
			return true, nil
		}

		if delInode.Timestamp < stTime || delInode.Timestamp > endTime {
			return true, nil
		}

		inosRsp.Count++
		inosRsp.DelInodes = append(inosRsp.DelInodes, delInode.Inode.Inode)
		return true, nil
	})

	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	resp.Code = http.StatusOK
	resp.Msg = "OK"
	resp.Data = inosRsp
	return
}

func (m *MetaNode) getAllInodeIdWithDeletedHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusSeeOther, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getAllInodeIdWithDeleted] response %s", err)
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

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	snap := NewSnapshot(mp.(*metaPartition))
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	inosRsp := &proto.MpAllInodesId{Count: 0, Inodes: make([]uint64, 0), DelInodes: make([]uint64, 0)}
	err = snap.Range(InodeType, func(item interface{}) (bool, error) {
		ino := item.(*Inode)
		inosRsp.Count++
		inosRsp.Inodes = append(inosRsp.Inodes, ino.Inode)
		return true, nil
	})
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	err = snap.Range(DelInodeType, func(item interface{}) (bool, error) {
		delInode := item.(*DeletedINode)
		inosRsp.Count++
		inosRsp.DelInodes = append(inosRsp.DelInodes, delInode.Inode.Inode)
		return true, nil
	})

	resp.Code = http.StatusOK
	resp.Msg = "OK"
	resp.Data = inosRsp
	return
}

func (m *MetaNode) getExtentsByDeletedInodeHandler(w http.ResponseWriter,
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

	srcIno, delIno, _, err := mp.(*metaPartition).getDeletedInode(id)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	ino := srcIno
	if ino == nil && delIno != nil {
		ino = delIno.buildInode()
	}

	if ino != nil {
		inodeExtentsInfo := &proto.GetExtentsResponse{}
		inodeExtentsInfo.Generation = ino.Generation
		inodeExtentsInfo.Size = ino.Size
		ino.Extents.Range(func(ek proto.ExtentKey) bool {
			inodeExtentsInfo.Extents = append(inodeExtentsInfo.Extents, ek)
			return true
		})
		var data []byte
		data, err = json.Marshal(inodeExtentsInfo)
		if err != nil {
			resp.Code = http.StatusInternalServerError
			resp.Msg = err.Error()
			return
		}
		resp.Data = json.RawMessage(data)
	}

	resp.Code = http.StatusSeeOther
	resp.Msg = "OK"
	return
}

func (m *MetaNode) getAllDeletedInodesCrcSum(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
		mp  MetaPartition
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllDeletedInodesCrcSum] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	snap := mp.GetSnapShot()
	if snap == nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = "meta partition snapshot is nil"
		return
	}
	defer snap.Close()
	var (
		inodeCnt  = snap.Count(InodeType)
		crcSumSet = make([]uint32, 0, inodeCnt)
		delInodes = make([]uint64, 0, inodeCnt)
		crc       = crc32.NewIEEE()
	)
	err = snap.Range(DelInodeType, func(item interface{}) (bool, error) {
		delIno := item.(*DeletedINode)
		delIno.Inode.AccessTime = 0
		var binary []byte
		binary, err = delIno.Marshal()
		if err != nil {
			return false, err
		}
		delInodes = append(delInodes, delIno.Inode.Inode)
		crcSumSet = append(crcSumSet, crc32.ChecksumIEEE(binary[0:]))
		if _, err = crc.Write(binary); err != nil {
			return false, fmt.Errorf("crc sum write failed:%v", err)
		}
		return true, nil
	})
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	resp.Data = &proto.InodesCRCSumInfo{
		PartitionID:     pid,
		ApplyID:         snap.ApplyID(),
		AllInodesCRCSum: crc.Sum32(),
		InodesID:        delInodes,
		CRCSumSet:       crcSumSet,
	}
	return
}

func (m *MetaNode) tryToLeader(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
		mp  MetaPartition
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[tryToLeader] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	if pid != mp.GetBaseConfig().PartitionId {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("Pid:%d is not equal mp:%d", pid, mp.GetBaseConfig().PartitionId)
		return
	}

	_, ok := mp.IsLeader()
	if ok {
		//alread is leader, do nothing
		return
	}

	err = mp.TryToLeader(pid)
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	return
}

func (m *MetaNode) pushInodeToFreeList(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		pid    uint64
		mp     MetaPartition
		inodes []uint64
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[pushInodeToFreeList] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	inodesStr := r.FormValue("inos")
	if inodesStr == "" {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("inos is needed")
		return
	}
	inodeStrArr := strings.Split(inodesStr, ",")
	if len(inodesStr) == 0 {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("inos is needed")
		return
	}

	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	if pid != mp.GetBaseConfig().PartitionId {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("Pid:%d is not equal mp:%d", pid, mp.GetBaseConfig().PartitionId)
		return
	}

	inodes = make([]uint64, 0, len(inodesStr))
	for _, inodeStr := range inodeStrArr {
		var inode uint64
		if inode, err = strconv.ParseUint(inodeStr, 10, 64); err != nil {
			resp.Code = http.StatusBadRequest
			resp.Msg = fmt.Sprintf("%s parse failed:%v", inodeStr, err)
			return
		}
		inodes = append(inodes, inode)
	}

	for _, inode := range inodes {
		mp.(*metaPartition).freeList.PushFront(inode)
	}
	return
}

func (m *MetaNode) getInodeWithMarkDeleteHandler(w http.ResponseWriter, r *http.Request) {
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
	needEKInfo, _ := strconv.ParseBool(r.FormValue("needEK"))
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	var (
		delIno    *DeletedINode
		expired   bool
		timestamp int64
	)
	ino, err := mp.(*metaPartition).inodeTree.Get(id)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	delIno, err = mp.(*metaPartition).inodeDeletedTree.Get(id)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

	if ino == nil && delIno == nil {
		resp.Code = http.StatusNotFound
		return
	}

	if ino == nil {
		ino = delIno.buildInode()
		expired = delIno.IsExpired
		timestamp = delIno.Timestamp
	}

	ino.RLock()
	defer ino.RUnlock()

	inodeInfo := &proto.InodeInfoWithEK{
		Inode:      ino.Inode,
		Mode:       ino.Type,
		Nlink:      ino.NLink,
		Size:       ino.Size,
		Uid:        ino.Uid,
		Gid:        ino.Gid,
		Generation: ino.Generation,
		ModifyTime: time.Unix(ino.CreateTime, 0),
		CreateTime: time.Unix(ino.AccessTime, 0),
		AccessTime: time.Unix(ino.ModifyTime, 0),
		Flag:       ino.Flag,
		Timestamp:  timestamp,
		IsExpired:  expired,
	}

	if length := len(ino.LinkTarget); length > 0 {
		inodeInfo.Target = make([]byte, length)
		copy(inodeInfo.Target, ino.LinkTarget)
	}

	if needEKInfo {
		inodeInfo.Extents = make([]proto.ExtentKey, 0, ino.Extents.Len())
		ino.Extents.Range(func(ek proto.ExtentKey) bool {
			inodeInfo.Extents = append(inodeInfo.Extents, ek)
			return true
		})
	}

	resp.Code = http.StatusOK
	resp.Msg = "OK"
	resp.Data = inodeInfo
	return
}

func (m *MetaNode) enableRocksDBSync(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
		mp  MetaPartition
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllDeletedInodesCrcSum] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	enable, _ := strconv.ParseBool(r.FormValue("enable"))
	mp.(*metaPartition).db.SyncFlag = enable
	return
}

func (m *MetaNode) reopenRocksDb(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
		mp  MetaPartition
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[getAllDeletedInodesCrcSum] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if pid, err = strconv.ParseUint(r.FormValue("pid"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if mp, err = m.metadataManager.GetPartition(pid); err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	err = mp.(*metaPartition).db.CloseDb()
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = "close db failed " + err.Error()
		return
	}

	mConf := mp.GetBaseConfig()

	err = mp.(*metaPartition).db.ReOpenDb(mp.(*metaPartition).getRocksDbRootDir(), mConf.RocksWalFileSize, mConf.RocksWalMemSize,
		mConf.RocksLogFileSize, mConf.RocksLogReversedTime, mConf.RocksLogReVersedCnt, mConf.RocksWalTTL)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = "reopen db failed " + err.Error()
		return
	}
	return
}

func (m *MetaNode) setDelEKRecordFilesMaxTotalSize(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		maxTotalMB uint64
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[setDelEKRecordFilesMaxTotalSize] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if maxTotalMB, err = strconv.ParseUint(r.FormValue("maxTotalMB"), 10, 64); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	if maxTotalMB < 0 {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("maxTotalMB value invalid:%v", maxTotalMB)
		return
	}
	atomic.StoreUint64(&nodeInfo.delEKFileLocalMaxMB, maxTotalMB*unit.MB)
	if maxTotalMB == 0 {
		DeleteEKRecordFilesMaxTotalSize.Store(defDeleteEKRecordFilesMaxTotalSize*unit.MB)
		return
	}
	DeleteEKRecordFilesMaxTotalSize.Store(maxTotalMB*unit.MB)
	return
}

func (m *MetaNode) removeOldDelEKRecordFile(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		pid uint64
		mp  MetaPartition
	)
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[removeOldDelEKRecordFile] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	pidStr := r.FormValue("pid")
	if pidStr != "" {
		if pid, err = strconv.ParseUint(pidStr, 10, 64); err != nil {
			resp.Code = http.StatusBadRequest
			resp.Msg = err.Error()
			return
		}
		if mp, err = m.metadataManager.GetPartition(pid); err != nil {
			resp.Code = http.StatusNotFound
			resp.Msg = err.Error()
			return
		}
		mp.(*metaPartition).removeOldDeleteEKRecordFile(delExtentKeyList, prefixDelExtentKeyListBackup, true)
		mp.(*metaPartition).removeOldDeleteEKRecordFile(InodeDelExtentKeyList, PrefixInodeDelExtentKeyListBackup, true)
		return
	}

	pidArray := make([]uint64, 0)
	//get all pid
	m.metadataManager.(*metadataManager).Range(func(i uint64, p MetaPartition) bool {
		pidArray = append(pidArray, i)
		return true
	})

	for _, id := range pidArray {
		partition, err := m.metadataManager.GetPartition(id)
		if err != nil {
			continue
		}
		partition.(*metaPartition).removeOldDeleteEKRecordFile(delExtentKeyList, prefixDelExtentKeyListBackup, true)
		partition.(*metaPartition).removeOldDeleteEKRecordFile(InodeDelExtentKeyList, PrefixInodeDelExtentKeyListBackup, true)
	}
	return
}

func (m *MetaNode) setRaftStorageParam(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = w.Write(data); err != nil {
			log.LogErrorf("[removeOldDelEKRecordFile] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	logSizeStr := r.FormValue("logSize")
	logSize, err:= strconv.Atoi(logSizeStr)
	if err != nil {
	logSize = 0
	}

	logCapStr := r.FormValue("logCap")
	logCap, err:= strconv.Atoi(logCapStr)
	if err != nil {
	logCap = 0
	}

	m.updateRaftParamFromLocal(logSize, logCap)
	return
}

func (m *MetaNode) getDeletedDentrysByParentInoHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getDeletedDentrysByParentInoHandler] response %s", err)
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

	parentIno, err := strconv.ParseUint(r.FormValue("pIno"), 10, 64)
	if err != nil || parentIno == 0 {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	batchNum, err := strconv.ParseInt(r.FormValue("batch"), 10, 16)
	if err != nil || batchNum > proto.ReadDeletedDirBatchNum {
		batchNum = proto.ReadDeletedDirBatchNum
	}
	prev := r.FormValue("prev")
	startTime, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
	if err != nil {
		startTime = 0
	}
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	var (
		count int64 = 0
		ddentrys    = make([]*DeletedDentry, 0)
	)
	prefix := newPrimaryDeletedDentry(parentIno, "", 0, 0)
	start := newPrimaryDeletedDentry(parentIno, prev, startTime, 0)
	end := newPrimaryDeletedDentry(parentIno+1, "", 0, 0)
	err = mp.(*metaPartition).dentryDeletedTree.RangeWithPrefix(prefix, start, end, func(dd *DeletedDentry) (bool, error) {
		count++
		ddentrys = append(ddentrys, dd)
		if count == batchNum {
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
	} else {
		resp.Data = ddentrys
	}
	return
}

func (m *MetaNode) getInodeInuse(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getBitInuse] response %s", err)
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

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	virtualMP := mp.(*metaPartition).getVirtualMetaPartitionByID(pid)
	if virtualMP.InodeIDAlloter == nil || virtualMP.InodeIDAlloter.GetStatus() == allocatorStatusUnavailable {
		resp.Msg = fmt.Sprintf("allocator disable")
		return
	}

	inoInuse := virtualMP.InodeIDAlloter.GetUsedInos()
	resp.Data = &struct {
		Cnt      int      `json:"count"`
		InoInuse []uint64 `json:"inodeInuse"`
	}{
		Cnt:      len(inoInuse),
		InoInuse: inoInuse,
	}
	return
}

func (m *MetaNode) getBitInuse(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getBitInuse] response %s", err)
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

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	virtualMP := mp.(*metaPartition).getVirtualMetaPartitionByID(pid)
	if virtualMP.InodeIDAlloter == nil || virtualMP.InodeIDAlloter.GetStatus() == allocatorStatusUnavailable {
		resp.Msg = fmt.Sprintf("allocator disable")
		return
	}

	inoInuseBitMap := virtualMP.InodeIDAlloter.GetUsedInosBitMap()
	resp.Data = &struct {
		InoInuseBitMap []uint64 `json:"inodeInuseBitMap"`
	}{
		InoInuseBitMap: inoInuseBitMap,
	}
	return
}

func (m *MetaNode) getInodeAllocatorStat(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getInodeAllocatorStat] response %s", err)
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

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	virtualMP := mp.(*metaPartition).getVirtualMetaPartitionByID(pid)
	if virtualMP.InodeIDAlloter == nil || virtualMP.InodeIDAlloter.GetStatus() == allocatorStatusUnavailable {
		resp.Msg = fmt.Sprintf("allocator disable")
		return
	}

	resp.Data = virtualMP.InodeIDAlloter
	return
}

func (m *MetaNode) setSkipStep(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setSkipStep] response %s", err)
		}
	}()

	skipStep, err := strconv.ParseUint(r.FormValue("skipStep"), 10, 64)
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}

	if skipStep < 0 {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("skipStep(%v) invalid", skipStep)
		return
	}

	m.updateSkipStep(skipStep)
	return
}

func (m *MetaNode) checkFreelist(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[checkFreelist] response %s", err)
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
	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	if mp.(*metaPartition).HasRocksDBStore() {
		return
	}
	applyID := mp.(*metaPartition).applyID
	//del inode snap
	delInodeTree := &DeletedInodeBTree{(mp.(*metaPartition).inodeDeletedTree).(*DeletedInodeBTree).GetTree()}
	//free inodes
	freeInodes := make([]uint64, 0, mp.(*metaPartition).freeList.Len())
	mp.(*metaPartition).freeList.Range(func(ino uint64) bool {
		freeInodes = append(freeInodes, ino)
		return true
	})
	unexpectFreeInodes := make([]uint64, 0)
	for _, ino := range freeInodes {
		delInode, _ := delInodeTree.RefGet(ino)
		if delInode == nil || !delInode.IsExpired {
			unexpectFreeInodes = append(unexpectFreeInodes, ino)
		}
	}
	if len(unexpectFreeInodes) == 0 {
		return
	}
	log.LogInfof("checkFreelist, partitionID(%v) applyID(%v) has unexpect free inodes, count:%v", pid, applyID, len(unexpectFreeInodes))
	resp.Data = &struct {
		Count              int      `json:"count"`
		UnExpectFreeInodes []uint64 `json:"unExpectFreeInodes"`
	}{
		Count:              len(unexpectFreeInodes),
		UnExpectFreeInodes: unexpectFreeInodes,
	}
}