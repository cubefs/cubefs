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
	"crypto/md5"
	"encoding/hex"
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
	http.HandleFunc("/setQosEnable", m.setQosEnableHandler)
	http.HandleFunc("/setMetaQos", m.setMetaQosHandler)
	http.HandleFunc("/getMetaQos", m.getMetaQosHandler)
	http.HandleFunc("/getRocksdbStats", m.getRocksdbStatsHandler)
	http.HandleFunc("/updateRocksDBConfig", m.updateRocksDBConfigHandler)
	http.HandleFunc("/getRocksDBConfig", m.getRocksDBConfigHandler)
	// Operation rate limiting management interfaces
	http.HandleFunc("/setOpLimit", m.setOpLimitHandler)
	http.HandleFunc("/getOpLimit", m.getOpLimitHandler)
	http.HandleFunc("/rmOpLimit", m.rmOpLimitHandler)
	http.HandleFunc("/getOpList", m.getOpListHandler)
	http.HandleFunc("/getRocksdbProperty", m.getRocksdbPropertyHandler)
	http.HandleFunc("/setRocksdbKeyNumMax", m.setRocksdbKeyNumMaxHandler)
	http.HandleFunc("/compactRocksdb", m.compactRocksdbHandler)
	http.HandleFunc("/calcMpMd5", m.calcMpMd5Handler)
	http.HandleFunc("/setTruncateBlockMax", m.setTruncateBlockMaxHandler)
	http.HandleFunc("/setRocksdbDiskThreshold", m.setRocksdbDiskThresholdHandler)
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
	partition := mp.(*metaPartition)
	snap, err := mp.GetSnapShot()
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()
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
	msg["inode_count"] = snap.Count(InodeType)
	msg["dentry_count"] = snap.Count(DentryType)
	msg["multipart_count"] = snap.Count(MultipartType)
	msg["extend_count"] = snap.Count(ExtendType)
	msg["apply_id"] = partition.GetAppliedID() // mp.GetAppliedID()
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

	f := func(i interface{}) bool {
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

	snap, err := mp.GetSnapShot()
	if err != nil {
		err = fmt.Errorf("can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	err = snap.Range(InodeType, f)
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

	snap, err := mp.GetSnapShot()
	if err != nil {
		resp.Code = http.StatusInternalServerError
		resp.Msg = fmt.Sprintf("Can not get mp[%d] snap shot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer snap.Close()

	err = snap.Range(DentryType, func(i interface{}) bool {
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

	handleTx := func(tx *proto.TransactionInfo) (bool, error) {
		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}
		val, err = json.Marshal(tx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return false, err
		}
		if _, err = w.Write(val); err != nil {
			return false, err
		}
		return true, nil
	}

	handleIno := func(ino *TxRollbackInode) (bool, error) {
		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}
		_, err = w.Write([]byte(ino.ToString()))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	handleDen := func(den *TxRollbackDentry) (bool, error) {
		if !isFirst {
			if _, err = w.Write(delimiter); err != nil {
				return false, err
			}
		} else {
			isFirst = false
		}
		_, err = w.Write([]byte(den.ToString()))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	snap, err := mp.GetSnapShot()
	if err != nil {
		log.LogErrorf("[getAllTxHandler] failed to get mp(%v) snapshot", mp.GetBaseConfig().PartitionId)
		return
	}
	defer mp.ReleaseSnapShot(snap)
	err = snap.Range(TransactionType, func(item interface{}) bool {
		ret, _ := handleTx(item.(*proto.TransactionInfo))
		return ret
	})
	if err != nil {
		log.LogErrorf("[getAllTxHandler] failed to range tx, err(%v)", err)
	}
	err = snap.Range(TransactionRollbackInodeType, func(item interface{}) bool {
		ret, _ := handleIno(item.(*TxRollbackInode))
		return ret
	})
	if err != nil {
		log.LogErrorf("[getAllTxHandler] failed to range rb inode, err(%v)", err)
	}
	err = snap.Range(TransactionRollbackDentryType, func(item interface{}) bool {
		ret, _ := handleDen(item.(*TxRollbackDentry))
		return ret
	})
	if err != nil {
		log.LogErrorf("[getAllTxHandler] failed to range rb dentry, err(%v)", err)
	}

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
			log.LogErrorf("[getGOGCHandler] response %s", err)
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
		id  uint64
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
	id, err = strconv.ParseUint(r.FormValue("id"), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", id, err)
		return
	}
	err = m.metadataManager.ReloadPartition(id)
}

func (m *MetaNode) setQosEnableHandler(w http.ResponseWriter, r *http.Request) {
	const (
		paramEnable = "enable"
	)
	var (
		enable bool
		err    error
	)
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setQosEnalbeHandler] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}
	enable, err = strconv.ParseBool(r.FormValue(paramEnable))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", enable, err)
		return
	}
	m.qosEnable = enable
	log.LogWarnf("[setQosEnable] change qosEnable to %v success", m.qosEnable)
}

func (m *MetaNode) setMetaQosHandler(w http.ResponseWriter, r *http.Request) {
	const (
		paramReadDirIops = "readDirIops"
	)
	var err error

	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setMetaQosHandler] response %s", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		return
	}

	parser := func(key string) (val int, err error, has bool) {
		valStr := r.FormValue(key)
		if valStr == "" {
			return 0, nil, false
		}
		has = true
		val, err = strconv.Atoi(valStr)
		return
	}

	updated := false
	for key, pVal := range map[string]*int{
		paramReadDirIops: &m.readDirIops,
	} {
		val, err, has := parser(key)
		if err != nil {
			return
		}
		if has {
			updated = true
			*pVal = val
		}
	}

	if updated {
		if m.metadataManager == nil {
			err = fmt.Errorf("metadataManager is nil")
			return
		}
		m.metadataManager.UpdateQosLimit()
	}
}

func (m *MetaNode) getMetaQosHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getMetaQosHandler] response %s", err)
		}
	}()

	metaQos := &struct {
		QosEnable   bool `json:"qosEnable"`
		ReadDirIops int  `json:"readDirIops"`
	}{
		QosEnable:   m.qosEnable,
		ReadDirIops: m.readDirIops,
	}

	resp.Data = metaQos
}

func (m *MetaNode) getRocksdbStatsHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getRocksdbStatsHandler] response %s", err)
		}
	}()

	result := make(map[string]string)
	for _, dbPath := range m.rocksDirs {
		db, err := m.rocksdbManager.OpenRocksdb(dbPath, 0)
		if err != nil {
			log.LogErrorf("[getRocksdbStatsHandler] failed to open rocksdb, err(%v)", err)
			continue
		}
		stats := db.GetStatistics()
		result[dbPath] = stats
		m.rocksdbManager.CloseRocksdb(db)
	}

	resp.Data = result
}

func (m *MetaNode) updateRocksDBConfigHandler(w http.ResponseWriter, r *http.Request) {
	var config map[string]string
	var err error

	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[updateRocksDBConfigHandler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	dbDir := r.FormValue("dbDir")
	if dbDir == "" {
		err = fmt.Errorf("dbDir is required")
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		log.LogErrorf("[updateRocksDBConfigHandler] failed to decode request, err(%v)", err)
		return
	}

	if err := m.rocksdbManager.UpdateConfig(dbDir, config); err != nil {
		log.LogErrorf("[updateRocksDBConfigHandler] failed to update write buffer size, err(%v)", err)
		return
	}
}

func (m *MetaNode) getRocksDBConfigHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var data []byte
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getRocksDBConfigHandler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	dbDir := r.FormValue("dbDir")
	if dbDir == "" {
		err = fmt.Errorf("dbDir is required")
		return
	}

	config, err := m.rocksdbManager.GetConfig(dbDir)
	if err != nil {
		log.LogErrorf("[getRocksDBConfigHandler] failed to get rocksdb config, err(%v)", err)
		return
	}

	resp.Data = config

	data, err = resp.Marshal()
	if err != nil {
		log.LogErrorf("[getRocksDBConfigHandler] failed to marshal response, err(%v)", err)
		return
	}
}

// setOpLimitHandler sets operation rate limiting
func (m *MetaNode) setOpLimitHandler(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		limit   uint32
		timeout uint32
		err     error
	)

	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setOpLimitHandler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	if name = r.FormValue("name"); name == "" {
		err = fmt.Errorf("missing name parameter")
		return
	}

	if limitStr := r.FormValue("limit"); limitStr == "" {
		err = fmt.Errorf("missing limit parameter")
		return
	} else {
		if limitVal, parseErr := strconv.ParseUint(limitStr, 10, 32); parseErr != nil {
			err = fmt.Errorf("invalid limit parameter")
			return
		} else {
			limit = uint32(limitVal)
		}
	}

	if timeoutStr := r.FormValue("timeout"); timeoutStr == "" {
		timeout = 0
	} else {
		if timeoutVal, parseErr := strconv.ParseUint(timeoutStr, 10, 32); parseErr != nil {
			err = fmt.Errorf("invalid timeout parameter")
			return
		} else {
			timeout = uint32(timeoutVal)
		}
	}

	if err = m.opLimiter.SetLimiter(name, limit, timeout); err != nil {
		return
	}

	log.LogInfof("set op limit success: name=%v, limit=%v, timeout=%v", name, limit, timeout)
	resp.Msg = fmt.Sprintf("set op limit success: name=%v, limit=%v, timeout=%v", name, limit, timeout)
}

// getOpLimitHandler gets operation rate limiting configuration
func (m *MetaNode) getOpLimitHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getOpLimitHandler] response %s", err)
		}
	}()

	m.opLimiter.m.RLock()
	limiterInfos := make(map[string]*OpLimitInfo)
	for opCode, lInfo := range m.opLimiter.limiterInfos {
		key := fmt.Sprintf("%d_%s", opCode, lInfo.OpName)
		copyInfo := &OpLimitInfo{
			OpName:         lInfo.OpName,
			OpCode:         lInfo.OpCode,
			Limit:          lInfo.Limit,
			LimiterTimeout: lInfo.LimiterTimeout,
		}
		limiterInfos[key] = copyInfo
	}
	m.opLimiter.m.RUnlock()

	meta := map[string]interface{}{
		"count": len(limiterInfos),
		"items": limiterInfos,
	}
	resp.Data = meta
}

// rmOpLimitHandler removes operation rate limiting
func (m *MetaNode) rmOpLimitHandler(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
	)

	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[rmOpLimitHandler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	if name = r.FormValue("name"); name == "" {
		err = fmt.Errorf("missing name parameter")
		return
	}

	if err = m.opLimiter.RmLimiter(name); err != nil {
		return
	}

	log.LogInfof("remove op limit success: name=%v", name)
	resp.Msg = fmt.Sprintf("remove op limit success: name=%v", name)
}

// getOpListHandler gets supported operation list
func (m *MetaNode) getOpListHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getOpListHandler] response %s", err)
		}
	}()

	opList := make(map[string]interface{})
	operations := make([]map[string]interface{}, 0)

	for opName, opCode := range proto.GOpInfo {
		opInfo := map[string]interface{}{
			"name":    opName,
			"op_code": opCode,
		}
		operations = append(operations, opInfo)
	}

	opList["operations"] = operations
	opList["count"] = len(operations)

	resp.Data = opList
}

func (m *MetaNode) getRocksdbPropertyHandler(w http.ResponseWriter, r *http.Request) {
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	var (
		err     error
		db      *RocksdbOperator
		result  string
		request struct {
			DbDir    string `json:"dbDir"`
			Property string `json:"property"`
		}
	)
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[getRocksdbStatsHandler] response %s", err)
		}
	}()

	if err = json.NewDecoder(r.Body).Decode(&request); err != nil {
		log.LogErrorf("[getRocksdbPropertyHandler] failed to decode request, err(%v)", err)
		return
	}

	db, err = m.rocksdbManager.OpenRocksdb(request.DbDir, 0)
	if err != nil {
		log.LogErrorf("[getRocksdbStatsHandler] failed to open rocksdb, err(%v)", err)
		return
	}
	defer m.rocksdbManager.CloseRocksdb(db)
	result, err = db.GetProperty(request.Property)
	if err != nil {
		log.LogErrorf("[getRocksdbStatsHandler] failed to get rocksdb property, err(%v)", err)
		return
	}

	resp.Data = result
}

func (m *MetaNode) setRocksdbKeyNumMaxHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[setRocksdbKeyNumMaxHandler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	if keyNumStr := r.FormValue("num"); keyNumStr == "" {
		err = fmt.Errorf("missing num parameter")
		return
	} else {
		keyNumVal, parseErr := strconv.ParseUint(keyNumStr, 10, 64)
		if parseErr != nil {
			err = fmt.Errorf("invalid num parameter")
			return
		}
		m.rocksdbKeyNumMax = keyNumVal
		log.LogInfof("[setRocksdbKeyNumMaxHandler] set rocksdb key num max success: num=%v", keyNumVal)
		resp.Msg = fmt.Sprintf("set rocksdb key num max success: num=%v", keyNumVal)
		m.CheckRocksdbStatus()
	}
}

func (m *MetaNode) compactRocksdbHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[compactRocksdbHandler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	dbDir := r.FormValue("dbDir")
	if dbDir == "" {
		err = fmt.Errorf("dbDir is required")
		return
	}

	now := time.Now()
	db, err := m.rocksdbManager.OpenRocksdb(dbDir, 0)
	if err != nil {
		log.LogErrorf("[compactRocksdbHandler] failed to open rocksdb, err(%v)", err)
		return
	}
	defer m.rocksdbManager.CloseRocksdb(db)
	err = db.CompactRange(nil, nil)
	if err != nil {
		log.LogErrorf("[compactRocksdbHandler] failed to compact rocksdb, err(%v)", err)
		return
	}

	log.LogInfof("[compactRocksdbHandler] compact rocksdb success: dbDir=%v, cost time=%v", dbDir, time.Since(now))
	resp.Msg = fmt.Sprintf("compact rocksdb success: dbDir=%v, costtime=%v", dbDir, time.Since(now))
}

func (m *MetaNode) calcMpMd5Handler(w http.ResponseWriter, r *http.Request) {
	var err error
	var id uint64
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := resp.Marshal()
		if _, err := w.Write(data); err != nil {
			log.LogErrorf("[calcMpMd5Handler] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	mpIdStr := r.FormValue("id")
	if mpIdStr == "" {
		err = fmt.Errorf("missing id parameter")
		return
	}
	id, err = strconv.ParseUint(mpIdStr, 10, 64)
	if err != nil {
		log.LogErrorf("[calcMpMd5Handler] failed to parse id, err(%v)", err)
		return
	}

	mp, err := m.metadataManager.GetPartition(id)
	if err != nil {
		log.LogErrorf("[calcMpMd5Handler] failed to get partition, err(%v)", err)
		return
	}
	partition := mp.(*metaPartition)
	snap, err := partition.GetSnapShot()
	if err != nil {
		log.LogErrorf("[calcMpMd5Handler] failed to get snap shot, err(%v)", err)
		return
	}
	defer snap.Close()

	h := md5.New()
	count := uint64(0)
	buff := bytes.NewBuffer(make([]byte, 0, 1024))
	err = snap.RangeReuseInode(func(inode *Inode) bool {
		buff.Reset()
		err = writeInodeToBufferForMd5(inode, buff)
		if err != nil {
			log.LogErrorf("[calcMpMd5Handler] failed to write inode, err(%v)", err)
			return false
		}
		_, err = h.Write(buff.Bytes())
		if err != nil {
			log.LogErrorf("[calcMpMd5Handler] failed to write inode, err(%v)", err)
			return false
		}
		count++
		return true
	})
	if err != nil {
		log.LogErrorf("[calcMpMd5Handler] failed to range reuse inode, err(%v)", err)
		return
	}

	md5Str := hex.EncodeToString(h.Sum(nil))

	h.Reset()
	dentryCount := uint64(0)
	err = snap.RangeReuseDentry(func(dentry *Dentry) bool {
		buff.Reset()
		err = WriteDentryToBuffer(dentry, buff)
		if err != nil {
			log.LogErrorf("[calcMpMd5Handler] failed to write dentry, err(%v)", err)
			return false
		}
		_, err = h.Write(buff.Bytes())
		if err != nil {
			log.LogErrorf("[calcMpMd5Handler] failed to write dentry, err(%v)", err)
			return false
		}
		dentryCount++
		return true
	})
	if err != nil {
		log.LogErrorf("[calcMpMd5Handler] failed to range reuse dentry, err(%v)", err)
		return
	}

	dentryMd5Str := hex.EncodeToString(h.Sum(nil))

	resp.Msg = fmt.Sprintf("mp id=%v applyid(%v) mode(%v), inode_count=%v, inode_md5=%v, dentry_count=%v, dentry_md5=%v", id, mp.GetAppliedID(), partition.inodeTree.GetStoreMode(), count, md5Str, dentryCount, dentryMd5Str)
}

func (m *MetaNode) setTruncateBlockMaxHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	mpIdStr := r.FormValue("id")
	if mpIdStr == "" {
		err = fmt.Errorf("missing id parameter")
		return
	}
	id, err := strconv.ParseUint(mpIdStr, 10, 64)
	if err != nil {
		log.LogErrorf("[setTruncateBlockMaxHandler] failed to parse id, err(%v)", err)
		return
	}

	countStr := r.FormValue("count")
	if countStr == "" {
		err = fmt.Errorf("missing count parameter")
		return
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		log.LogErrorf("[setTruncateBlockMaxHandler] failed to parse count, err(%v)", err)
		return
	}

	err = m.raftStore.SetTruncateBlockMax(id, count)
	if err != nil {
		log.LogErrorf("[setTruncateBlockMaxHandler] failed to set partition[%v] truncate block max to %v, err(%v)", id, count, err)
		return
	}

	log.LogInfof("[setTruncateBlockMaxHandler] set truncate block max success: partition=%v, count=%v", id, count)
	resp.Msg = fmt.Sprintf("set truncate block max success: partition=%v, count=%v", id, count)
}

func (m *MetaNode) setRocksdbDiskThresholdHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Msg = err.Error()
			resp.Code = http.StatusBadRequest
		}
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	thresholdStr := r.FormValue("threshold")
	if thresholdStr == "" {
		err = fmt.Errorf("missing threshold parameter")
		return
	}
	threshold, err := strconv.ParseFloat(thresholdStr, 64)
	if err != nil {
		log.LogErrorf("[setRocksdbDiskThresholdHandler] failed to parse threshold, err(%v)", err)
		return
	}

	m.metadataManager.SetRocksdbDiskThreshold(threshold)
	log.LogInfof("[setRocksdbDiskThresholdHandler] set rocksdb disk threshold success: threshold=%v", threshold)
	resp.Msg = fmt.Sprintf("set rocksdb disk threshold success: threshold=%v", threshold)
}
