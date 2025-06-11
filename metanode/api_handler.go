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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	defaultGOGCLowerLimit = 30
	defaultGOGCUpperLimit = 100
)

var parseArgs = common.ParseArguments

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
	http.HandleFunc("/getLeaderPartitions", m.getLeaderPartitionsHandler)
	http.HandleFunc("/getInode", m.getInodeHandler)
	http.HandleFunc("/getSplitKey", m.getSplitKeyHandler)
	http.HandleFunc("/getExtentsByInode", m.getExtentsByInodeHandler)
	http.HandleFunc("/getEbsExtentsByInode", m.getEbsExtentsByInodeHandler)
	// get all inodes of the partitionID
	http.HandleFunc("/getAllInodes", m.getAllInodesHandler)
	// get dentry information
	http.HandleFunc("/getDentry", m.getDentryHandler)
	http.HandleFunc("/getDirectory", m.getDirectoryHandler)
	http.HandleFunc("/getAllDentry", m.getAllDentriesHandler)
	http.HandleFunc("/getAllTxInfo", m.getAllTxHandler)
	http.HandleFunc("/getParams", m.getParamsHandler)
	http.HandleFunc("/getSmuxStat", m.getSmuxStatHandler)
	http.HandleFunc("/getRaftStatus", m.getRaftStatusHandler)
	http.HandleFunc("/genClusterVersionFile", m.genClusterVersionFileHandler)
	http.HandleFunc("/getInodeSnapshot", m.getInodeSnapshotHandler)
	http.HandleFunc("/getDentrySnapshot", m.getDentrySnapshotHandler)
	// get tx information
	http.HandleFunc("/getTx", m.getTxHandler)
	http.HandleFunc("/getInodeAccessTime", m.getInodeAccessTimeHandler)
	// for hybrid cloud debug
	http.HandleFunc("/getInodeWithExtentKey", m.getInodeWithExtentKeyHandler)
	// http.HandleFunc("/setInodeCreateTime", m.setInodeCreateTimeHandler)
	// http.HandleFunc("/deleteMigrateExtentKey", m.deleteMigrateExtentKeyHandler)
	// http.HandleFunc("/updateExtentKeyAfterMigration", m.updateExtentKeyAfterMigrationHandler)
	http.HandleFunc("/getRaftPeers", m.getRaftPeersHandler)
	http.HandleFunc("/setGOGC", m.setGOGCHandler)
	http.HandleFunc("/getGOGC", m.getGOGCHandler)
	http.HandleFunc("/reloadMp", m.reloadMpHandler)
	return
}

func (m *MetaNode) getParamsHandler(w http.ResponseWriter,
	r *http.Request,
) {
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
	r *http.Request,
) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	resp.Data = smuxPool.GetStat()
	data, _ := resp.Marshal()
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getSmuxStatHandler] response %s", err)
	}
}

func (m *MetaNode) getPartitionsHandler(w http.ResponseWriter,
	r *http.Request,
) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	resp.Data = m.metadataManager
	data, _ := resp.Marshal()
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getPartitionsHandler] response %s", err)
	}
}

func (m *MetaNode) getPartitionByIDHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getPartitionByIDHandler] response %s", err)
		}
	}()
	var pid common.Uint
	if err := parseArgs(r, pid.PID()); err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	msg := make(map[string]interface{})
	leader, _ := mp.IsLeader()
	_, leaderTerm := mp.LeaderTerm()
	msg["leaderAddr"] = leader
	msg["leader_term"] = leaderTerm
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

func (m *MetaNode) getLeaderPartitionsHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	mps := m.metadataManager.GetLeaderPartitions()
	resp.Data = mps
	data, err := resp.Marshal()
	if err != nil {
		log.LogErrorf("json marshal error:%v", err)
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	if _, err := w.Write(data); err != nil {
		log.LogErrorf("[getPartitionsHandler] response %s", err)
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
	}
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

	var pid common.Uint
	if err = parseArgs(r, pid.PID()); err != nil {
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		return
	}
	verSeq, err := m.getRealVerSeq(w, r)
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

		inode, _ = i.(*Inode).getInoByVer(verSeq, false)
		if inode == nil {
			return true
		}
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

func (m *MetaNode) getSplitKeyHandler(w http.ResponseWriter, r *http.Request) {
	log.LogDebugf("getSplitKeyHandler")
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getSplitKeyHandler] response %s", err)
		}
	}()
	var pid, ino common.Uint
	var verAll common.Bool
	if err := parseArgs(r, pid.PID(), ino.Ino(),
		verAll.Key("verAll").OmitEmpty().OmitError()); err != nil {
		resp.Msg = err.Error()
		return
	}

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	log.LogDebugf("getSplitKeyHandler")
	req := &InodeGetSplitReq{
		PartitionID: pid.V,
		Inode:       ino.V,
		VerSeq:      verSeq,
		VerAll:      verAll.V,
	}
	log.LogDebugf("getSplitKeyHandler")
	p := &Packet{}
	err = mp.InodeGetSplitEk(req, p)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	log.LogDebugf("getSplitKeyHandler")
	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
		log.LogDebugf("getSplitKeyHandler data %v", resp.Data)
	} else {
		log.LogDebugf("getSplitKeyHandler")
	}
}

func (m *MetaNode) getInodeHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getInodeHandler] response %s", err)
		}
	}()
	var pid, ino common.Uint
	var verAll common.Bool
	if err := parseArgs(r, pid.PID(), ino.Ino(),
		verAll.Key("verAll").OmitEmpty().OmitError()); err != nil {
		resp.Msg = err.Error()
		return
	}

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &InodeGetReq{
		PartitionID: pid.V,
		Inode:       ino.V,
		VerSeq:      verSeq,
		VerAll:      verAll.V,
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
	if len(p.Data) == 0 {
		return
	}
	inodeResp := &proto.InodeGetResponse{}
	err = json.Unmarshal(p.Data, inodeResp)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	p = &Packet{}
	err = mp.InodeGetAccessTime(req, p)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	persistAtResp := &proto.InodeGetAccessTimeResponse{}
	err = json.Unmarshal(p.Data, persistAtResp)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}
	inodeResp.Info.PersistAccessTime = persistAtResp.Info.AccessTime
	resp.Data = inodeResp.Info
}

func (m *MetaNode) getRaftStatusHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getRaftStatusHandler] response %s", err)
		}
	}()
	var raftID common.Uint
	if err := parseArgs(r, raftID.ID()); err != nil {
		resp.Msg = err.Error()
		resp.Code = http.StatusBadRequest
		return
	}
	raftStatus := m.raftStore.RaftStatus(raftID.V)
	resp.Data = raftStatus
}

func (m *MetaNode) getEbsExtentsByInodeHandler(w http.ResponseWriter,
	r *http.Request,
) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getEbsExtentsByInodeHandler] response %s", err)
		}
	}()
	var pid, ino common.Uint
	if err := parseArgs(r, pid.PID(), ino.Ino()); err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &proto.GetExtentsRequest{
		PartitionID: pid.V,
		Inode:       ino.V,
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
}

func (m *MetaNode) getExtentsByInodeHandler(w http.ResponseWriter,
	r *http.Request,
) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getExtentsByInodeHandler] response %s", err)
		}
	}()
	var pid, ino common.Uint
	var verAll common.Bool
	if err := parseArgs(r, pid.PID(), ino.Ino(),
		verAll.Key("verAll").OmitEmpty().OmitError()); err != nil {
		resp.Msg = err.Error()
		return
	}

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	req := &proto.GetExtentsRequest{
		PartitionID: pid.V,
		Inode:       ino.V,
		VerSeq:      uint64(verSeq),
		VerAll:      verAll.V,
	}
	p := &Packet{}
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	p.ReqID = proto.GenerateRequestID()
	p.Opcode = proto.OpMetaExtentsList
	p.PartitionID = pid.V
	err = p.MarshalData(req)
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = err.Error()
		return
	}

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
}

func (m *MetaNode) getDentryHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getDentryHandler] response %s", err)
		}
	}()
	var pid, pIno common.Uint
	var verAll common.Bool
	if err := parseArgs(r, pid.PID(), pIno.ParentIno(),
		verAll.Key("verAll").OmitEmpty().OmitError()); err != nil {
		resp.Msg = err.Error()
		return
	}
	name := r.FormValue("name")

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &LookupReq{
		PartitionID: pid.V,
		ParentID:    pIno.V,
		Name:        name,
		VerSeq:      verSeq,
		VerAll:      verAll.V,
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
}

func (m *MetaNode) getTxHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getTxHandler] response %s", err)
		}
	}()
	var pid common.Uint
	var txid common.String
	if err := parseArgs(r, pid.PID(), txid.Key("txId")); err != nil {
		resp.Msg = err.Error()
		return
	}

	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &proto.TxGetInfoRequest{
		Pid:  pid.V,
		TxID: txid.V,
	}
	p := &Packet{}
	if err = mp.TxGetInfo(req, p); err != nil {
		resp.Code = http.StatusSeeOther
		resp.Msg = err.Error()
		return
	}

	resp.Code = http.StatusSeeOther
	resp.Msg = p.GetResultMsg()
	if len(p.Data) > 0 {
		resp.Data = json.RawMessage(p.Data)
	}
}

func (m *MetaNode) getRealVerSeq(w http.ResponseWriter, r *http.Request) (verSeq uint64, err error) {
	var seq common.Uint
	err = parseArgs(r, seq.Key("verSeq").OmitEmpty().OnValue(func() error {
		verSeq = seq.V
		if verSeq == 0 {
			verSeq = math.MaxUint64
		}
		return nil
	}))
	return
}

func (m *MetaNode) getAllDentriesHandler(w http.ResponseWriter, r *http.Request) {
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
	var pid common.Uint
	if err := parseArgs(r, pid.PID()); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
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
		den, _ := i.(*Dentry).getDentryFromVerList(verSeq, false)
		if den == nil || den.isDeleted() {
			return true
		}

		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false
			}
		} else {
			isFirst = false
		}
		val, err = json.Marshal(den)
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
}

func (m *MetaNode) getAllTxHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, "")
	shouldSkip := false
	defer func() {
		if !shouldSkip {
			data, _ := resp.Marshal()
			if _, err := w.Write(data); err != nil {
				log.LogErrorf("[getAllTxHandler] response %s", err)
			}
		}
	}()
	var pid common.Uint
	if err := parseArgs(r, pid.PID()); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
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

	f := func(i BtreeItem) bool {
		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false
			}
		} else {
			isFirst = false
		}

		if ino, ok := i.(*TxRollbackInode); ok {
			_, err = w.Write([]byte(ino.ToString()))
			return err == nil
		}
		if den, ok := i.(*TxRollbackDentry); ok {
			_, err = w.Write([]byte(den.ToString()))
			return err == nil
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
	}

	txTree, rbInoTree, rbDenTree := mp.TxGetTree()
	txTree.Ascend(f)
	rbInoTree.Ascend(f)
	rbDenTree.Ascend(f)

	shouldSkip = true
	buff.WriteString(`]}`)
	if _, err = w.Write(buff.Bytes()); err != nil {
		log.LogErrorf("[getAllTxHandler] response %s", err)
	}
}

func (m *MetaNode) getDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getDirectoryHandler] response %s", err)
		}
	}()
	var pid, pIno common.Uint
	if err := parseArgs(r, pid.PID(), pIno.ParentIno()); err != nil {
		resp.Msg = err.Error()
		return
	}

	p1, err := strconv.ParseUint(r.FormValue("parentIno"), 10, 64)
	if err != nil {
		resp.Msg = err.Error()
		return
	}
	pid.V = p1

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := ReadDirReq{
		ParentID: pIno.V,
		VerSeq:   verSeq,
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
}

func (m *MetaNode) getInodeSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	m.getSnapshotHandler(w, r, inodeFile)
}

func (m *MetaNode) getDentrySnapshotHandler(w http.ResponseWriter, r *http.Request) {
	m.getSnapshotHandler(w, r, dentryFile)
}

func (m *MetaNode) getSnapshotHandler(w http.ResponseWriter, r *http.Request, file string) {
	var err error
	defer func() {
		if err != nil {
			msg := fmt.Sprintf("[getInodeSnapshotHandler] err(%v)", err)
			log.LogErrorf("%s", msg)
			if _, e := w.Write([]byte(msg)); e != nil {
				log.LogErrorf("[getInodeSnapshotHandler] failed to write response: err(%v) msg(%v)", e, msg)
			}
		}
	}()
	var pid common.Uint
	if err = parseArgs(r, pid.PID()); err != nil {
		return
	}
	mp, err := m.metadataManager.GetPartition(pid.V)
	if err != nil {
		return
	}

	filename := path.Join(mp.GetBaseConfig().RootDir, snapshotDir, file)
	if _, err = os.Stat(filename); err != nil {
		err = errors.NewErrorf("[getInodeSnapshotHandler] Stat: %s", err.Error())
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		err = errors.NewErrorf("[getInodeSnapshotHandler] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()

	_, err = io.Copy(w, fp)
	if err != nil {
		err = errors.NewErrorf("[getInodeSnapshotHandler] copy: %s", err.Error())
		return
	}
}

func (m *MetaNode) getInodeAccessTimeHandler(w http.ResponseWriter, r *http.Request) {
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
	err = mp.InodeGetAccessTime(req, p)
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
}

func (m *MetaNode) getInodeWithExtentKeyHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := NewAPIResponse(http.StatusBadRequest, "")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getInodeWithExtentKeyHandler] response %s", err)
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

	verSeq, err := m.getRealVerSeq(w, r)
	if err != nil {
		resp.Msg = err.Error()
		return
	}

	verAll, _ := strconv.ParseBool(r.FormValue("verAll"))

	mp, err := m.metadataManager.GetPartition(pid)
	if err != nil {
		resp.Code = http.StatusNotFound
		resp.Msg = err.Error()
		return
	}
	req := &InodeGetReq{
		PartitionID: pid,
		Inode:       id,
		VerSeq:      verSeq,
		VerAll:      verAll,
	}
	p := &Packet{}
	err = mp.InodeGetWithEk(req, p)
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
}

// used for debug api
// func (m *MetaNode) setInodeCreateTimeHandler(w http.ResponseWriter, r *http.Request) {
// 	var err error

// 	r.ParseForm()
// 	resp := NewAPIResponse(http.StatusBadRequest, "")
// 	defer func() {
// 		data, _ := resp.Marshal()
// 		if _, err := w.Write(data); err != nil {
// 			log.LogErrorf("[setInodeCreateTimeHandler] response %s", err.Error())
// 		}
// 	}()

// 	pid, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
// 	if err != nil {
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	id, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
// 	if err != nil {
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	dateTimeStr := r.FormValue("createTime")

// 	log.LogInfof("[setInodeCreateTimeHandler] mpId(%v) ino(%v), to set createTime: %v",
// 		pid, id, dateTimeStr)

// 	formatStr := "2006-01-02 15:04:05 -0700 MST"
// 	datetime, err := time.Parse(formatStr, dateTimeStr)
// 	if err != nil {
// 		err = fmt.Errorf("failed to parse createTime: %v", err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	}
// 	createTime := datetime.Unix()

// 	mp, err := m.metadataManager.GetPartition(pid)
// 	if err != nil {
// 		resp.Code = http.StatusNotFound
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	if leaderAddr, ok := mp.IsLeader(); !ok {
// 		resp.Code = http.StatusSeeOther
// 		err = fmt.Errorf("not mp leader, leader is %v", leaderAddr)
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	req := &SetCreateTimeRequest{
// 		Inode:      id,
// 		CreateTime: createTime,
// 	}

// 	p := &Packet{}
// 	err = p.MarshalData(req)
// 	if err != nil {
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	if err = mp.SetCreateTime(req, p.Data, p); err != nil {
// 		log.LogErrorf("[setInodeCreateTimeHandler] req: %v, error: %s", req, err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	resp.Code = http.StatusOK
// 	resp.Msg = p.GetResultMsg()

// 	log.LogInfof("[setInodeCreateTimeHandler] mpId(%v) ino(%v), to set createTime: %v(%v)",
// 		pid, id, dateTimeStr, createTime)
// 	return
// }

// func (m *MetaNode) deleteMigrateExtentKeyHandler(w http.ResponseWriter, r *http.Request) {
// 	var err error

// 	r.ParseForm()
// 	resp := NewAPIResponse(http.StatusBadRequest, "")
// 	defer func() {
// 		data, _ := resp.Marshal()
// 		if _, err := w.Write(data); err != nil {
// 			log.LogErrorf("[deleteMigrateExtentKeyHandler] response %s", err.Error())
// 		}
// 	}()

// 	mpId, err := strconv.ParseUint(r.FormValue("pid"), 10, 64)
// 	if err != nil {
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	inoId, err := strconv.ParseUint(r.FormValue("ino"), 10, 64)
// 	if err != nil {
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	log.LogInfof("[deleteMigrateExtentKeyHandler] mpId(%v) ino(%v) run", mpId, inoId)

// 	mp, err := m.metadataManager.GetPartition(mpId)
// 	if err != nil {
// 		log.LogErrorf("[deleteMigrateExtentKeyHandler] mpId(%v) ino(%v), get mp err: %v", mpId, inoId, err.Error())
// 		resp.Code = http.StatusNotFound
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	if leaderAddr, ok := mp.IsLeader(); !ok {
// 		resp.Code = http.StatusSeeOther
// 		err = fmt.Errorf("not mp leader, leader is %v", leaderAddr)
// 		log.LogErrorf("[deleteMigrateExtentKeyHandler] mpId(%v) ino(%v), err: %v", mpId, inoId, err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	req := &DeleteMigrationExtentKeyRequest{
// 		PartitionID: mpId,
// 		Inode:       inoId,
// 	}

// 	p := &Packet{}
// 	p.Opcode = proto.OpDeleteMigrationExtentKey
// 	req.FullPaths = []string{"N/A"}
// 	err = p.MarshalData(req)
// 	if err != nil {
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	remoteInfo := "httpFrom" + r.RemoteAddr
// 	if err = mp.DeleteMigrationExtentKey(req, p, remoteInfo); err != nil {
// 		log.LogErrorf("[deleteMigrateExtentKeyHandler] req: %v, error: %s", req, err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	resp.Code = http.StatusOK
// 	resp.Msg = p.GetResultMsg()

// 	log.LogInfof("[deleteMigrateExtentKeyHandler] mpId(%v) ino(%v) success", mpId, inoId)
// 	return
// }

// func (m *MetaNode) updateExtentKeyAfterMigrationHandler(w http.ResponseWriter, r *http.Request) {
// 	var err error
// 	var bytes []byte

// 	resp := NewAPIResponse(http.StatusBadRequest, "")
// 	defer func() {
// 		data, _ := resp.Marshal()
// 		if _, err := w.Write(data); err != nil {
// 			log.LogErrorf("[updateExtentKeyAfterMigrationHandler] response %s", err.Error())
// 		}
// 	}()

// 	if bytes, err = ioutil.ReadAll(r.Body); err != nil {
// 		resp.Code = http.StatusBadRequest
// 		resp.Msg = err.Error()
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] read request data body err:%s", err)
// 		return
// 	}
// 	req := &proto.UpdateExtentKeyAfterMigrationRequest{}
// 	if err = json.Unmarshal(bytes, req); err != nil {
// 		resp.Code = http.StatusBadRequest
// 		resp.Msg = err.Error()
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] Unmarshal request data body err:%s", err)
// 		return
// 	}
// 	log.LogInfof("[updateExtentKeyAfterMigrationHandler] req: %v", req)

// 	mp, err := m.metadataManager.GetPartition(req.PartitionID)
// 	if err != nil {
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] mpId(%v) ino(%v), get mp err: %v",
// 			req.PartitionID, req.Inode, err.Error())
// 		resp.Code = http.StatusNotFound
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	leaderAddr, isLeader := mp.IsLeader()
// 	if leaderAddr == "" {
// 		resp.Code = http.StatusSeeOther
// 		err = fmt.Errorf("mp(%v) no leader", req.PartitionID)
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] mpId(%v) ino(%v) err: %v",
// 			req.PartitionID, req.Inode, err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	} else if !isLeader {
// 		resp.Code = http.StatusSeeOther
// 		err = fmt.Errorf("not leader, mp(%v) leader is %v", req.PartitionID, leaderAddr)
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] mpId(%v) ino(%v) err: %v",
// 			req.PartitionID, req.Inode, err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	p := &Packet{}
// 	p.Opcode = proto.OpMetaUpdateExtentKeyAfterMigration
// 	req.FullPaths = []string{"N/A"}
// 	err = p.MarshalData(req)
// 	if err != nil {
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] mpId(%v) ino(%v) MarshalData err: %v",
// 			req.PartitionID, req.Inode, err.Error())
// 		resp.Code = http.StatusSeeOther
// 		resp.Msg = "inner error"
// 		return
// 	}

// 	remoteInfo := "httpFrom" + r.RemoteAddr
// 	if err = mp.UpdateExtentKeyAfterMigration(req, p, remoteInfo); err != nil {
// 		log.LogErrorf("[updateExtentKeyAfterMigrationHandler] req: %v, error: %s", req, err.Error())
// 		resp.Msg = err.Error()
// 		return
// 	}

// 	resp.Code = http.StatusOK
// 	resp.Msg = p.GetResultMsg()

// 	log.LogInfof("[updateExtentKeyAfterMigrationHandler] mpId(%v) ino(%v) success", req.PartitionID, req.Inode)
// 	return
// }

func (m *MetaNode) getRaftPeersHandler(w http.ResponseWriter, r *http.Request) {
	const (
		paramRaftID = "id"
	)

	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getRaftPeersHandler] response %s", err)
		}
	}()

	raftID, err := strconv.ParseUint(r.FormValue(paramRaftID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramRaftID, err)
		resp.Msg = err.Error()
		resp.Code = http.StatusBadRequest
		return
	}

	raftPeers := m.raftStore.GetPeers(raftID)
	resp.Data = raftPeers
}

func (m *MetaNode) setGOGCHandler(w http.ResponseWriter, r *http.Request) {
	const (
		paramGOGC = "gogc"
	)
	var (
		gogcValue int
		err       error
	)
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		} else {
			resp.Data = "set GOGC success"
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setGOGCHandler] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}
	gogcValue, err = strconv.Atoi(r.FormValue(paramGOGC))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramGOGC, err)
		return
	}
	if gogcValue < defaultGOGCLowerLimit || gogcValue > defaultGOGCUpperLimit {
		err = fmt.Errorf("gogc must be greater than or equal to %v and less than or equal to %v", defaultGOGCLowerLimit, defaultGOGCUpperLimit)
		return
	}
	if m.metadataManager == nil {
		err = fmt.Errorf("metadataManager is nil")
		return
	}
	m.metadataManager.(*metadataManager).useLocalGOGC = true
	if m.metadataManager.(*metadataManager).gogcValue != gogcValue {
		oldGOGC := m.metadataManager.(*metadataManager).gogcValue
		debug.SetGCPercent(gogcValue)
		m.metadataManager.(*metadataManager).gogcValue = gogcValue
		log.LogWarnf("[setGOGC] change GOGC, old(%v) new(%v)", oldGOGC, gogcValue)
	}
}

func (m *MetaNode) getGOGCHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setGOGCHandler] response %s", err)
		}
	}()
	if m.metadataManager == nil {
		err = fmt.Errorf("metadataManager is nil")
		return
	}
	resp.Data = fmt.Sprintf("gogc value is %v", m.metadataManager.(*metadataManager).gogcValue)
}

func (m *MetaNode) reloadMpHandler(w http.ResponseWriter, r *http.Request) {
	var (
		id  int
		err error
	)
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[reloadMpHandler] response %s", err)
		}
	}()
	if m.metadataManager == nil {
		err = fmt.Errorf("metadataManager is nil")
		return
	}
	if err = r.ParseForm(); err != nil {
		return
	}
	id, err = strconv.Atoi(r.FormValue("id"))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", id, err)
		return
	}
	err = m.metadataManager.ReloadPartition(id)
}
