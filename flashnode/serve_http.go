// Copyright 2023 The CubeFS Authors.
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

package flashnode

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

func (f *FlashNode) registerAPIHandler() {
	http.HandleFunc("/stat", f.handleStat)
	http.HandleFunc("/statAll", f.handleStatAll)
	http.HandleFunc("/evictVol", f.handleEvictVolume)
	http.HandleFunc("/evictAll", f.handleEvictAll)
	http.HandleFunc("/inactiveDisk", f.handleInactiveDisk)
	http.HandleFunc("/setWriteDiskQos", f.handleSetWriteDiskQos)
	http.HandleFunc("/setReadDiskQos", f.handleSetReadDiskQos)
	http.HandleFunc("/getDiskQos", f.handleGetDiskQos)
	http.HandleFunc("/scannerControl", f.handleScannerCommand)
	http.HandleFunc("/setWaitForCacheBlock", f.handleSetWaitForCacheBlock)
	http.HandleFunc("/slotStat", f.handleSlotStat)
	http.HandleFunc("/submitTask", f.handleSubmitTask)
}

func (f *FlashNode) handleStat(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeStat{
		NodeLimit:         uint64(f.readLimiter.Limit()),
		CacheStatus:       f.cacheEngine.Status(),
		WaitForCacheBlock: f.waitForCacheBlock,
	})
}

func (f *FlashNode) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = io.ReadAll(r.Body); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	defer r.Body.Close()
	req := proto.FlashManualTask{}
	if err = json.Unmarshal(bytes, &req); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	log.LogInfof("submit mannual task with http arg:%+v", &req)
	start := time.Now()
	req.StartTime = &start
	req.UpdateTime = &start
	if req.Id == "" {
		req.Id = uuid.New().String()
	}
	rootDir := req.GetPathPrefix()
	var tmpDir string
	f.manualScanners.Range(func(k, v interface{}) bool {
		t := v.(*proto.FlashManualTask)
		if t.VolName != req.VolName {
			return true
		}
		tmpDir = t.GetPathPrefix()
		if rootDir != tmpDir {
			return true
		}
		if !proto.ManualTaskDone(t.Status) {
			err = fmt.Errorf("manual task[%v] is running with the same directory", t.Id)
			return false
		}
		return true
	})
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	req.ManualTaskStatistics = &proto.ManualTaskStatistics{
		FlashNode: f.localAddr,
	}
	adminTask := &proto.AdminTask{
		Request: &proto.FlashNodeManualTaskRequest{
			Task:       &req,
			FnNodeAddr: f.localAddr,
		},
		Response: &proto.FlashNodeManualTaskResponse{
			FlashNode: f.localAddr,
		},
	}
	err = f.startTaskScan(adminTask)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	replyOK(w, r, fmt.Sprintf("create flash manual scan task(%v) success", req.Id))
}

func (f *FlashNode) handleStatAll(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeStat{
		NodeLimit:   uint64(f.readLimiter.Limit()),
		CacheStatus: f.cacheEngine.StatusAll(),
	})
}

func (f *FlashNode) handleEvictVolume(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	volume := r.FormValue("volume")
	if volume == "" {
		replyErr(w, r, proto.ErrCodeParamError, "volume name can not be empty", nil)
		return
	}
	replyOK(w, r, f.cacheEngine.EvictCacheByVolume(volume))
}

func (f *FlashNode) handleEvictAll(w http.ResponseWriter, r *http.Request) {
	f.cacheEngine.EvictCacheAll()
	replyOK(w, r, nil)
}

func (f *FlashNode) handleInactiveDisk(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	dataPath := r.FormValue("dataPath")
	if dataPath == "" {
		replyErr(w, r, proto.ErrCodeParamError, "dataPath can not be empty", nil)
		return
	}
	f.cacheEngine.DoInactiveDisk(dataPath)
	replyOK(w, r, nil)
}

func replyOK(w http.ResponseWriter, r *http.Request, data interface{}) {
	replyErr(w, r, proto.ErrCodeSuccess, "OK", data)
}

func replyErr(w http.ResponseWriter, r *http.Request, code int32, msg string, data interface{}) {
	remote := fmt.Sprintf("url(%s) addr(%s)", r.URL.String(), r.RemoteAddr)
	reply := proto.HTTPReply{Code: code, Msg: msg, Data: data}
	replyBytes, err := json.Marshal(reply)
	if err != nil {
		log.LogErrorf("to %s marshal reply[%v] err:[%v]", remote, reply, err)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(replyBytes)))
	if _, err = w.Write(replyBytes); err != nil {
		log.LogErrorf("to %s write reply len[%d] err:[%v]", remote, len(replyBytes), err)
	}
	log.LogInfof("to %s respond", remote)
}

func (f *FlashNode) handleSetWriteDiskQos(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
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
		paramFlow:   &f.diskWriteFlow,
		paramIocc:   &f.diskWriteIocc,
		paramFactor: &f.diskWriteIoFactorFlow,
	} {
		val, err, has := parser(key)
		if err != nil {
			replyErr(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		if has {
			updated = true
			*pVal = val
		}
	}
	if f.diskWriteIoFactorFlow == 0 {
		f.diskWriteIoFactorFlow = _defaultDiskWriteFactor
	}
	if updated {
		f.limitWrite.ResetIOEx(f.diskWriteIocc*len(f.disks), f.diskWriteIoFactorFlow, f.handleReadTimeout)
		f.limitWrite.ResetFlow(f.diskWriteFlow)
	}
	replyOK(w, r, nil)
}

func (f *FlashNode) handleSetReadDiskQos(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
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
		paramFlow:   &f.diskReadFlow,
		paramIocc:   &f.diskReadIocc,
		paramFactor: &f.diskReadIoFactorFlow,
	} {
		val, err, has := parser(key)
		if err != nil {
			replyErr(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		if has {
			updated = true
			*pVal = val
		}
	}
	if f.diskReadIoFactorFlow == 0 {
		f.diskReadIoFactorFlow = _defaultDiskReadFactor
	}
	if updated {
		f.limitRead.ResetIOEx(f.diskReadIocc*len(f.disks), f.diskReadIoFactorFlow, f.handleReadTimeout)
		f.limitRead.ResetFlow(f.diskReadFlow)
	}
	replyOK(w, r, nil)
}

func (f *FlashNode) handleGetDiskQos(w http.ResponseWriter, r *http.Request) {
	writeStatus := proto.FlashNodeLimiterStatus{Status: f.limitWrite.Status(true), DiskNum: len(f.disks), ReadTimeout: f.handleReadTimeout}
	readStatus := proto.FlashNodeLimiterStatus{Status: f.limitRead.Status(true), DiskNum: len(f.disks), ReadTimeout: f.handleReadTimeout}
	info := proto.FlashNodeLimiterStatusInfo{WriteStatus: writeStatus, ReadStatus: readStatus}
	replyOK(w, r, info)
}

func (f *FlashNode) handleScannerCommand(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		msg := fmt.Sprintf("httpServiceScanner ParseForm failed: %v", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	id := r.FormValue("id")
	if id == "" {
		http.Error(w, "invalid task id", http.StatusBadRequest)
		return
	}
	log.LogInfof("receive httpServiceScanner id: %v", id)
	opCode := r.FormValue("opCode")
	if opCode == "" {
		http.Error(w, "invalid task opCode", http.StatusBadRequest)
		return
	}
	log.LogInfof("receive httpServiceScanner opCode: %v", opCode)
	mScanner, ok := f.manualScanners.Load(id)
	if !ok {
		msg := fmt.Sprintf("task id(%v) not exist", id)
		http.Error(w, msg, http.StatusNotFound)
		return
	}
	scanner := mScanner.(*ManualScanner)
	if opCode == "info" {
		replyOK(w, r, scanner.copyResponse())
		return
	}
	scanner.processCommand(opCode)
	w.WriteHeader(http.StatusOK)
}

func (f *FlashNode) handleSetWaitForCacheBlock(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	valStr := r.FormValue("waitForCacheBlock")

	if valStr == "" {
		replyErr(w, r, proto.ErrCodeParamError, "invalid parameter", nil)
		return
	}
	val, err := strconv.ParseBool(valStr)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, "parse  waitForCacheBlock failed", nil)
		return
	}
	f.waitForCacheBlock = val
	replyOK(w, r, nil)
}

func (f *FlashNode) handleSlotStat(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeSlotStat{
		NodeId:   f.nodeID,
		Addr:     f.localAddr,
		SlotStat: f.GetFlashNodeSlotStat(),
	})
}
