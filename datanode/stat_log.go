package datanode

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

func (d *DataNode) startStat(cfg *config.Config) {
	logDir := cfg.GetString(ConfigKeyLogDir)
	var err error
	logLeftSpaceLimitRatio := log.DefaultLogLeftSpaceLimitRatio

	if logLeftSpaceLimitRatioStr := cfg.GetString("logLeftSpaceLimitRatio"); logLeftSpaceLimitRatioStr != "" {
		if val, err := strconv.ParseFloat(logLeftSpaceLimitRatioStr, 64); err == nil {
			logLeftSpaceLimitRatio = val
		} else {
			log.LogWarnf("get log limt ratio failed, err %s", err.Error())
		}
	}

	stat.DpStat, err = stat.NewOpLogger(logDir, "dp_op.log", stat.DefaultMaxOps, stat.DefaultDuration, logLeftSpaceLimitRatio)
	if err != nil {
		log.LogErrorf("DpStat init failed.err:%+v", err)
		return
	}
	stat.DiskStat, err = stat.NewOpLogger(logDir, "disk_op.log", stat.DefaultMaxOps, stat.DefaultDuration, logLeftSpaceLimitRatio)
	if err != nil {
		log.LogErrorf("DiskStat init failed.err:%+v", err)
		return
	}
	stat.DpStat.SetSendMaster(true)
	stat.DiskStat.SetSendMaster(true)
	log.LogInfof("startStat")
}

func (d *DataNode) closeStat() {
	stat.DpStat.Close()
	stat.DiskStat.Close()
	log.LogInfof("closeStat")
}

func (d *DataNode) getDiskOpLog() []proto.OpLog {
	return d.getOplogs(stat.DiskStat.GetMasterOps())
}

func (d *DataNode) getDpOpLog() []proto.OpLog {
	return d.getOplogs(stat.DpStat.GetMasterOps())
}

func (d *DataNode) getOplogs(ops []*stat.Operation) []proto.OpLog {
	oplog := make([]proto.OpLog, 0, len(ops))
	for _, op := range ops {
		if op.Op == "" {
			arr := strings.Split(op.Name, "_")
			op.Op = arr[len(arr)-1]
		}
		oplog = append(oplog, proto.OpLog{
			Name:  op.Name,
			Op:    op.Op,
			Count: op.Count,
		})
	}
	return oplog
}

func (s *DataNode) setOpLog(w http.ResponseWriter, r *http.Request) {
	const (
		paramRecordFile = "recordFile"
		paramSendMaster = "sendMaster"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	recordFile, err := strconv.ParseBool(r.FormValue(paramRecordFile))
	log.LogDebugf("action[setOpLog] ingnore:%+v,err:%+v", recordFile, err)
	if err == nil {
		stat.DpStat.SetRecordFile(recordFile)
		stat.DiskStat.SetRecordFile(recordFile)
	}
	sendMaster, err := strconv.ParseBool(r.FormValue(paramSendMaster))
	log.LogDebugf("action[setOpLog] sendMaster:%+v,err:%+v", sendMaster, err)
	if err == nil {
		stat.DpStat.SetSendMaster(sendMaster)
		stat.DiskStat.SetSendMaster(sendMaster)
	}
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) getOpLog(w http.ResponseWriter, r *http.Request) {
	const (
		paramOpType = "opType"
		paramDpId   = "dpId"
		paramOp     = "op"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	opType := r.FormValue(paramOpType)
	op := r.FormValue(paramOp)
	dpId := r.FormValue(paramDpId)
	var oplogs []proto.OpLog
	switch opType {
	case "disk":
		stat.DiskStat.Lock()
		oplogs = s.getOplogs(stat.DiskStat.GetPrevOps())
		stat.DiskStat.Unlock()
	case "dp":
		stat.DpStat.Lock()
		oplogs = s.getOplogs(stat.DpStat.GetPrevOps())
		stat.DpStat.Unlock()
		if dpId != "" {
			oplogs = filterDpId(dpId, oplogs)
		}
	default:
	}
	if op != "" {
		oplogs = filterOp(op, oplogs)
	}
	s.buildSuccessResp(w, oplogs)
}

func filterDpId(dpId string, oplogs []proto.OpLog) []proto.OpLog {
	if dpId == "" {
		return oplogs
	}
	out := make([]proto.OpLog, 0, len(oplogs))
	for _, oplog := range oplogs {
		arr := strings.Split(oplog.Name, "_")
		if len(arr) < 2 {
			continue
		}
		if arr[0] == "dp" && arr[1] == dpId {
			out = append(out, oplog)
		}
	}
	return out
}

func filterOp(op string, oplogs []proto.OpLog) []proto.OpLog {
	if op == "" {
		return oplogs
	}
	out := make([]proto.OpLog, 0, len(oplogs))
	opLower := strings.ToLower(op)
	for _, oplog := range oplogs {
		if !strings.Contains(strings.ToLower(oplog.Op), opLower) {
			continue
		}
		out = append(out, oplog)
	}
	return out
}
